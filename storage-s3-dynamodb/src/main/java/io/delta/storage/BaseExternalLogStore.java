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
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import io.delta.storage.internal.FileNameUtils;
import io.delta.storage.internal.PathLock;
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
 *
 * Notation:
 * - N: the target commit version we are writing. e.g. 10 for 0000010.json
 * - N.json: the actual target commit we want to write.
 * - T(N): the temp file path for commit N used during the prepare-commit-acknowledge `write`
 *         algorithm below. We will eventually copy T(N) into N.json
 * - E(N, T(N), complete=true/false): the entry we will atomically commit into the external
 *                                      cache.
 */
public abstract class BaseExternalLogStore extends HadoopFileSystemLogStore {
    private static final Logger LOG = LoggerFactory.getLogger(BaseExternalLogStore.class);

    /**
     * A global path lock to ensure that no two writers/readers are copying a given T(N) into N.json
     * at the same time within the same JVM. This can occur
     * - while a writer is performing a normal write operation AND a reader happens to see an
     *   external entry E(complete=false) and so starts a recovery operation
     * - while two readers see E(complete=false) and so both start a recovery operation
     */
    private static final PathLock pathLock = new PathLock();

    /**
     * The delay, in seconds, after an external entry has been committed to the delta log at which
     * point it is safe to be deleted from the external store.
     *
     * We want a delay long enough such that, after the external entry has been deleted, another
     * write attempt for the SAME delta log commit can FAIL using ONLY the FileSystem's existence
     * check (e.g. `fs.exists(path)`). Recall we assume that the FileSystem does not provide mutual
     * exclusion.
     *
     * We use a value of 1 day.
     *
     * If we choose too small of a value, like 0 seconds, then the following scenario is possible:
     * - t0:  Writers W1 and W2 start writing data files
     * - t1:  W1 begins to try and write into the _delta_log.
     * - t2:  W1 checks if N.json exists in FileSystem. It doesn't.
     * - t3:  W1 writes actions into temp file T1(N)
     * - t4:  W1 writes to external store entry E1(N, complete=false)
     * - t5:  W1 copies (with overwrite=false) T1(N) into N.json.
     * - t6:  W1 overwrites entry in external store E1(N, complete=true, expireTime=now+0)
     * - t7:  E1 is safe to be deleted, and some external store TTL mechanism deletes E1
     * - t8:  W2 begins to try and write into the _delta_log.
     * - t9:  W1 checks if N.json exists in FileSystem, but too little time has transpired between
     *        t5 and t9 that the FileSystem check (fs.exists(path)) returns FALSE.
     *        Note: This isn't possible on S3 (which provides strong consistency) but could be
     *        possible on eventually-consistent systems.
     * - t10: W2 writes actions into temp file T2(N)
     * - t11: W2 writes to external store entry E2(N, complete=false)
     * - t12: W2 successfully copies (with overwrite=false) T2(N) into N.json. FileSystem didn't
     *        provide the necessary mutual exclusion, so the copy succeeded. Thus, DATA LOSS HAS
     *        OCCURRED.
     *
     * By using an expiration delay of 1 day, we ensure one of the steps at t9 or t12 will fail.
     */
    protected static final long DEFAULT_EXTERNAL_ENTRY_EXPIRATION_DELAY_SECONDS =
        TimeUnit.DAYS.toSeconds(1);

    /**
     * Completed external commit entries will be created with a value of
     * NOW_EPOCH_SECONDS + getExpirationDelaySeconds().
     */
    protected long getExpirationDelaySeconds() {
        return DEFAULT_EXTERNAL_ENTRY_EXPIRATION_DELAY_SECONDS;
    }

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
            // Note: `fixDeltaLog` will apply per-JVM mutual exclusion via a lock to help reduce
            // the chance of many reader threads in a single JVM doing duplicate copies of
            // T(N) -> N.json.
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
     * - Step 0: Fail if N.json already exists in FileSystem.
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
        try {
            // Prevent concurrent writers in this JVM from either
            // a) concurrently overwriting N.json if overwrite=true
            // b) both checking if N-1.json exists and performing a "recovery" where they both
            //    copy T(N-1) into N-1.json
            //
            // Note that the mutual exclusion on writing into N.json with overwrite=false from
            // different JVMs (which is the entire point of BaseExternalLogStore) is provided by the
            // external cache, not by this lock, of course.
            //
            // Also note that this lock path (resolvedPath) is for N.json, while the lock path used
            // below in the recovery `fixDeltaLog` path is for N-1.json. Thus, no deadlock.
            pathLock.acquire(resolvedPath);

            if (overwrite) {
                writeActions(fs, path, actions);
                return;
            } else if (fs.exists(path)) {
                // Step 0: Fail if N.json already exists in FileSystem and overwrite=false.
                throw new java.nio.file.FileAlreadyExistsException(path.toString());
            }

            // Step 1: Ensure that N-1.json exists
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
                                String.format("previous commit %s doesn't exist on the file system but does in the external log store", prevPath)
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
                                    "Old entries for table %s still exist in the external log store",
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
                null // no expireTime
            );

            // Step 2.1: Create temp file T(N)
            writeActions(fs, entry.absoluteTempPath(), actions);

            // Step 2.2: Create externals store entry E(N, T(N), complete=false)
            putExternalEntry(entry, false); // overwrite=false

            try {
                // Step 3: COMMIT the commit to the delta log.
                //         Copy T(N) -> N.json with overwrite=false
                writeCopyTempFile(fs, entry.absoluteTempPath(), resolvedPath);

                // Step 4: ACKNOWLEDGE the commit
                writePutCompleteDbEntry(entry);
            } catch (Throwable e) {
                LOG.info(
                    "{}: ignoring recoverable error", e.getClass().getSimpleName(), e
                );
            }
        } catch (java.lang.InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
        } finally {
            pathLock.release(resolvedPath);
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
        while (actions.hasNext()) {
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
        putExternalEntry(entry.asComplete(getExpirationDelaySeconds()), true); // overwrite=true
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
        putExternalEntry(entry.asComplete(getExpirationDelaySeconds()), true); // overwrite=true
    }

    ////////////////////
    // Helper Methods //
    ////////////////////

    /**
     * Method for assuring consistency on filesystem according to the external cache.
     * Method tries to rewrite TransactionLog entry from temporary path if it does not exist.
     *
     * Should never throw a FileAlreadyExistsException.
     * - If we see one when copying the temp file, we can assume the target file N.json already
     *   exists and a concurrent writer has already copied the contents of T(N).
     * - We will never see one when writing to the external cache since overwrite=true.
     */
    private void fixDeltaLog(FileSystem fs, ExternalCommitEntry entry) throws IOException {
        if (entry.complete) {
            return;
        }

        final Path targetPath = entry.absoluteFilePath();
        try {
            pathLock.acquire(targetPath);

            int retry = 0;
            boolean copied = false;
            while (true) {
                LOG.info("trying to fix: {}", entry.fileName);
                try {
                    if (!copied && !fs.exists(targetPath)) {
                        fixDeltaLogCopyTempFile(fs, entry.absoluteTempPath(), targetPath);
                        copied = true;
                    }
                    fixDeltaLogPutCompleteDbEntry(entry);
                    LOG.info("fixed file {}", entry.fileName);
                    return;
                } catch (java.nio.file.FileAlreadyExistsException e) {
                    LOG.info("file {} already copied: {}:",
                        entry.fileName, e.getClass().getSimpleName(), e);
                    copied = true;
                    // Don't return since we still need to mark the DB entry as complete. This will
                    // happen when we execute the main try block on the next while loop iteration
                } catch (Throwable e) {
                    LOG.info("{}:", e.getClass().getSimpleName(), e);
                    if (retry >= 3) {
                        throw e;
                    }
                }
                retry += 1;
            }
        } catch (java.lang.InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
        } finally {
            pathLock.release(targetPath);
        }
    }

   /**
    * Copies file within filesystem.
    *
    * @param fs reference to [[FileSystem]]
    * @param src path to source file
    * @param dst path to destination file
    */
    private void copyFile(FileSystem fs, Path src, Path dst) throws IOException {
        LOG.info("copy file: {} -> {}", src, dst);
        final FSDataInputStream inputStream = fs.open(src);
        try {
            final FSDataOutputStream outputStream = fs.create(dst, false); // overwrite=false
            IOUtils.copy(inputStream, outputStream);

            // We don't close `outputStream` if an exception happens because it may create a partial
            // file.
            outputStream.close();
        } catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
            throw new java.nio.file.FileAlreadyExistsException(dst.toString());
        } finally {
            inputStream.close();
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
