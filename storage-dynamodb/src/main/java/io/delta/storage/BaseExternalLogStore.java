/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.storage;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Optional;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import io.delta.storage.internal.FileNameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base {@link LogStore} implementation for cloud stores (e.g. Amazon S3) that do not provide
 * mutual exclusion.
 * <p>
 * This implementation depends on child methods, particularly `putExternalEntry`, to provide
 * the mutual exclusion that the cloud store is lacking.
 */
public abstract class BaseExternalLogStore extends HadoopFileSystemLogStore {
    private static final Logger LOG = LoggerFactory.getLogger(BaseExternalLogStore.class);

    ////////////////////////
    // Public API Methods //
    ////////////////////////

    public BaseExternalLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    /**
     * First checks if there is any incomplete entry in the external store. If so, tries to perform
     * a recovery/fix.
     *
     * Then, performs a normal listFrom user the `super` implementation.
     */
    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) throws IOException {
        final FileSystem fs = path.getFileSystem(hadoopConf);
        final Path resolvedPath = stripUserInfo(fs.makeQualified(path));
        final Path tablePath = getTablePath(resolvedPath);
        final Optional<ExternalCommitEntry> entry = getLatestExternalEntry(tablePath);

        if (entry.isPresent() && !entry.get().complete) {
            fixDeltaLog(fs, entry.get());
        }

        // This is predicated on the storage system providing consistent listing
        // If there was a recovery performed in the `fixDeltaLog` call, then some temp file
        // was just copied into some N.json in the delta log. Because of consistent listing,
        // the `super.listFrom` is guaranteed to see N.json.
        return super.listFrom(path, hadoopConf);
    }

    /**
     * If overwrite=true, then write normally without any interaction with external store.
     * Else, to commit for delta version N:
     * - Step 1: Ensure that N-1.json exists. If not, perform a recovery.
     * - Step 2: PREPARE the commit.
     *      - Write `actions` into temp file T(N)
     *      - Write with mutual exclusion to external store and entry E(N, T(N), complete=false)
     * - Step 3: COMMIT the commit to the delta log.
     *      - Copy T(N) into N.json
     * - Step 4: ACKNOWLEDGE the commit.
     *      - Overwrite entry E in external store and set complete=true
     */
    @Override
    public void write(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        final FileSystem fs = path.getFileSystem(hadoopConf);
        final Path resolvedPath = stripUserInfo(fs.makeQualified(path));

        if (overwrite) {
            writeActions(fs, path, actions);
            return;
        }

        // Step 0: Ensure that N-1.json exists
        final Path tablePath = getTablePath(resolvedPath);
        if (FileNameUtils.isDeltaFile(path)) {
            final long version = FileNameUtils.deltaVersion(path);
            if (version > 0) {
                final long prevVersion = version - 1;
                final Path deltaLogPath = new Path(tablePath, "_delta_log");
                final Path prevPath = FileNameUtils.deltaFile(deltaLogPath, prevVersion);
                final String prevFileName = prevPath.getName();
                final Optional<ExternalCommitEntry> prevEntry = getExternalEntry(
                    tablePath.toString(),
                    prevFileName
                );
                if (prevEntry.isPresent() && !prevEntry.get().complete) {
                    fixDeltaLog(fs, prevEntry.get());
                } else {
                    if (!fs.exists(prevPath)) {
                        throw new java.nio.file.FileSystemException(
                            String.format("previous commit %s doesn't exist", prevPath)
                        );
                    }
                }
            } else {
                final String fileName = path.getName();
                final Optional<ExternalCommitEntry> entry = getExternalEntry(
                    tablePath.toString(),
                    fileName
                );
                if (entry.isPresent()) {
                    if (entry.get().complete && !fs.exists(path)) {
                        throw new java.nio.file.FileSystemException(
                            String.format(
                                "Old entries for table %s still exist in the external store",
                                tablePath
                            )
                        );
                    }
                }
            }
        }

        // Step 2: PREPARE the commit
        final String tempPath = createTemporaryPath(resolvedPath);
        final ExternalCommitEntry entry = new ExternalCommitEntry(
            tablePath,
            resolvedPath.getName(),
            tempPath,
            false, // not complete
            null // commitTime
        );
        writeActions(fs, entry.absoluteTempPath(), actions);
        putExternalEntry(entry, false); // overwrite=false

        try {
            // Step 3: COMMIT the commit to the delta log
            writeCopyTempFile(fs, entry.absoluteTempPath(), resolvedPath);

            // Step 4: ACKNOWLEDGE the commit
            writePutCompleteDbEntry(entry);
        } catch (Throwable e) {
            LOG.info(
                "{}: ignoring recoverable error", e.getClass().getSimpleName(), e
            );
        }
    }

    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) {
        return false;
    }

    /////////////////////////////////////////////////////////////
    // Protected Members (for interaction with external store) //
    /////////////////////////////////////////////////////////////

    /**
     * Write file with actions under a specific path.
     */
    protected void writeActions(
        FileSystem fs,
        Path path,
        Iterator<String> actions
    ) throws IOException {
        LOG.debug("writeActions to: {}", path);
        FSDataOutputStream stream = fs.create(path, true);
        while(actions.hasNext()) {
            byte[] line = String.format("%s\n", actions.next()).getBytes(StandardCharsets.UTF_8);
            stream.write(line);
        }
        stream.close();
    }

    /**
     * Generate temporary path for TransactionLog.
     */
    protected String createTemporaryPath(Path path) {
        String uuid = java.util.UUID.randomUUID().toString();
        return String.format(".tmp/%s.%s", path.getName(), uuid);
    }

    /**
     * Returns the base table path for a given Delta log entry located in
     * e.g. input path of $tablePath/_delta_log/00000N.json would return $tablePath
     */
    protected Path getTablePath(Path path) {
        return path.getParent().getParent();
    }

    /**
     * Write to external store in exclusive way.
     *
     * @throws java.nio.file.FileAlreadyExistsException if path exists in cache and `overwrite` is
     *                                                  false
     */
    abstract protected void putExternalEntry(
        ExternalCommitEntry entry,
        boolean overwrite) throws IOException;

    /**
     * Return external store entry corresponding to delta log file with given `tablePath` and
     * `fileName`, or `Optional.empty()` if it doesn't exist.
     */
    abstract protected Optional<ExternalCommitEntry> getExternalEntry(
        String tablePath,
        String fileName) throws IOException;

    /**
     * Return the latest external store entry corresponding to the delta log for given `tablePath`,
     * or `Optional.empty()` if it doesn't exist.
     *
     * @param tablePath TODO
     */
    abstract protected Optional<ExternalCommitEntry> getLatestExternalEntry(
        Path tablePath) throws IOException;

    //////////////////////////////////////////////////////////
    // Protected Members (for error injection during tests) //
    //////////////////////////////////////////////////////////

    /**
     * Wrapper for `copyFile`, called by the `write` method.
     */
    @VisibleForTesting
    protected void writeCopyTempFile(FileSystem fs, Path src, Path dst) throws IOException {
        copyFile(fs, src, dst);
    }

    /**
     * Wrapper for `putExternalEntry`, called by the `write` method.
     */
    @VisibleForTesting
    protected void writePutCompleteDbEntry(ExternalCommitEntry entry) throws IOException {
        putExternalEntry(entry.asComplete(), true);
    }

    /**
     * Wrapper for `copyFile`, called by the `fixDeltaLog` method.
     */
    @VisibleForTesting
    protected void fixDeltaLogCopyTempFile(FileSystem fs, Path src, Path dst) throws IOException {
        copyFile(fs, src, dst);
    }

    /**
     * Wrapper for `putExternalEntry`, called by the `fixDeltaLog` method.
     */
    @VisibleForTesting
    protected void fixDeltaLogPutCompleteDbEntry(ExternalCommitEntry entry) throws IOException {
        putExternalEntry(entry.asComplete(), true);
    }

    ////////////////////
    // Helper Methods //
    ////////////////////

    /**
     * Method for assuring consistency on filesystem according to the external cache.
     * Method tries to rewrite TransactionLog entry from temporary path if it does not exist.
     */
    private void fixDeltaLog(FileSystem fs, ExternalCommitEntry entry) throws IOException {
        if (entry.complete) {
            return;
        }
        int retry = 0;
        boolean copied = false;
        while (true) {
            LOG.debug("trying to fix: {}", entry.fileName);
            try {
                if (!copied && !fs.exists(entry.absoluteFilePath())) {
                    fixDeltaLogCopyTempFile(fs, entry.absoluteTempPath(), entry.absoluteFilePath());
                    copied = true;
                }
                fixDeltaLogPutCompleteDbEntry(entry);
                LOG.info("fixed {}", entry.fileName);
                return;
            } catch(Throwable e) {
                LOG.info("{}:", e.getClass().getSimpleName(), e);
                if (retry >= 3) {
                    throw e;
                }
            }
            retry += 1;
        }
    }

   /**
    * Copies file within filesystem
    * @param fs reference to [[FileSystem]]
    * @param src path to source file
    * @param dst path to destination file
    */
    private void copyFile(FileSystem fs, Path src, Path dst) throws IOException {
        LOG.debug("copy file: {} -> {}", src, dst);
        FSDataInputStream input_stream = fs.open(src);
        FSDataOutputStream output_stream = fs.create(dst, true);
        try {
            IOUtils.copy(input_stream, output_stream);
            output_stream.close();
        } finally {
            input_stream.close();
        }
    }

    /**
     * Returns path stripped user info.
     */
    private Path stripUserInfo(Path path) {
        final URI uri = path.toUri();

        try {
            final URI newUri = new URI(
                uri.getScheme(),
                null, // userInfo
                uri.getHost(),
                uri.getPort(),
                uri.getPath(),
                uri.getQuery(),
                uri.getFragment()
            );

            return new Path(newUri);
        } catch (URISyntaxException e) {
            // Propagating this URISyntaxException to callers would mean we would have to either
            // include it in the public LogStore.java interface or wrap it in an
            // IllegalArgumentException somewhere else. Instead, catch and wrap it here.
            throw new IllegalArgumentException(e);
        }
    }
}
