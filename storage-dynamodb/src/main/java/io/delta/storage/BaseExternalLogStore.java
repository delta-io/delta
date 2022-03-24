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

import java.net.URI;
import java.nio.charset.StandardCharsets;
import io.delta.storage.HadoopFileSystemLogStore;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.collections.iterators.FilterIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Used so that we can use an external store child implementation
* to provide the mutual exclusion that the cloud store,
* e.g. s3, is missing.
*/
public abstract class BaseExternalLogStore extends HadoopFileSystemLogStore {
    private static final Logger LOG = LoggerFactory.getLogger(BaseExternalLogStore.class);

    ////////////////////////
    // Public API Methods //
    ////////////////////////

    public BaseExternalLogStore(Configuration hadoopConf) {
        super(hadoopConf);
        deltaFilePattern = Pattern.compile("\\d+\\.json");

    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) throws IOException {
        FileSystem fs = path.getFileSystem(hadoopConf);
        Path resolvedPath;
        try {
            resolvedPath = stripUserInfo(fs.makeQualified(path));
        } catch(java.net.URISyntaxException e) {
            throw new IOException(e);
        };
        Path tablePath = getTablePath(resolvedPath);
        ExternalCommitEntry entry = getLatestExternalEntry(tablePath);
        if(entry != null) {
            fixDeltaLog(fs, entry);
        }
        return super.listFrom(path, hadoopConf);
    }

    @Override
    public void write(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {

        FileSystem fs = path.getFileSystem(hadoopConf);
        Path resolvedPath;
        try {
            resolvedPath = stripUserInfo(fs.makeQualified(path));
        } catch(java.net.URISyntaxException e) {
            throw new IOException(e);
        };

        if(overwrite) {
            writeActions(fs, path, actions);
            return;
        };

        Path tablePath = getTablePath(resolvedPath);
        if(isDeltaFile(path)) {
            long version = deltaVersion(path);
            if(version > 0) {
                long prevVersion = version - 1;
                Path prevPath = deltaFile(tablePath, prevVersion);
                ExternalCommitEntry entry = getExternalEntry(tablePath, prevPath);
                if(entry != null) {
                    fixDeltaLog(fs, entry);
                } else {
                    if(!fs.exists(prevPath)) {
                        throw new java.nio.file.FileSystemException(
                            String.format("previous commit %s doesn't exist", prevPath)
                        );
                    }
                }
            } else {
                ExternalCommitEntry entry = getExternalEntry(tablePath, path);
                if(entry != null) {
                    if(entry.complete && !fs.exists(path)) {
                        throw new java.nio.file.FileSystemException(
                            String.format(
                                "Old entries for %s still exist in the database", tablePath
                            )
                        );
                    }
                }
            }
        }
        String tempPath = createTemporaryPath(resolvedPath);
        ExternalCommitEntry entry = new ExternalCommitEntry(
            tablePath,
            resolvedPath.getName(),
            tempPath,
            false,  // not complete
            null    // commitTime
        );
        writeActions(fs, entry.absoluteTempPath(), actions);
        putExternalEntry(entry, false);

        try {
            writeCopyTempFile(fs, entry.absoluteTempPath(), resolvedPath);
            writePutCompleteDbEntry(entry);
        } catch(Throwable e) {
            LOG.info(String.format(
                "%s: ignoring recoverable error: %s", e.getClass().getSimpleName(), e));
        }
    }

    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) throws IOException {
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
        LOG.debug(String.format("writeActions to: %s", path));
        FSDataOutputStream stream = fs.create(path, true);
        while(actions.hasNext()) {
            byte[] line = String.format("%s\n", actions.next()).getBytes(StandardCharsets.UTF_8);
            stream.write(line);
        }
        stream.close();
    }

    /**
     * Returns path stripped user info.
     */
    protected Path getPathKey(Path resolvedPath) throws java.net.URISyntaxException {
        return stripUserInfo(resolvedPath);
    }

    /**
     * Generate temporary path for TransactionLog.
     */
    protected String createTemporaryPath(Path path) {
        String uuid = java.util.UUID.randomUUID().toString();
        return String.format(".tmp/%s.%s", path.getName(), uuid);
    }

    protected Path getTablePath(Path path) {
        return path.getParent().getParent();
    }

   /*
    * Write to db in exclusive way.
    * Method should throw @java.nio.file.FileAlreadyExistsException if path exists in cache.
    */
    abstract protected void putExternalEntry(
        ExternalCommitEntry entry, boolean overwrite
    ) throws IOException;

    abstract protected ExternalCommitEntry getExternalEntry(
        Path tablePath, Path jsonPath
    ) throws IOException;

    abstract protected ExternalCommitEntry getLatestExternalEntry(
        Path tablePath
    ) throws IOException;

    //////////////////////////////////////////////////////////
    // Protected Members (for error injection during tests) //
    //////////////////////////////////////////////////////////

    /**
     * The following four methods are extracted for testing purposes
     * so we can more easily inject errors and test for failures.
     */

    protected void writeCopyTempFile(FileSystem fs, Path src, Path dst) throws IOException {
        copyFile(fs, src, dst);
    }

    protected void writePutCompleteDbEntry(ExternalCommitEntry entry) throws IOException {
        putExternalEntry(entry.asComplete(), true);
    }

    protected void fixDeltaLogCopyTempFile(FileSystem fs, Path src, Path dst) throws IOException {
        copyFile(fs, src, dst);
    }

    protected void fixDeltaLogPutCompleteDbEntry(ExternalCommitEntry entry) throws IOException {
        putExternalEntry(entry.asComplete(), true);
    }

    ////////////////////
    // Helper Methods //
    ////////////////////

    /**
     * Method for assuring consistency on filesystem according to the external cache.
     * Method tries to rewrite TransactionLog entry from temporary path if it does not exists.
     * Method returns completed [[ExternalCommitEntry]]
     */

    private void fixDeltaLog(FileSystem fs, ExternalCommitEntry entry) throws IOException {
        if(entry.complete) {
            return;
        }
        int retry = 0;
        boolean copied = false;
        while(true) {
            LOG.debug(String.format("trying to fix: %s", entry.fileName));
            try {
                if (!copied && !fs.exists(entry.absoluteJsonPath())) {
                    fixDeltaLogCopyTempFile(fs, entry.absoluteTempPath(), entry.absoluteJsonPath());
                    copied = true;
                }
                fixDeltaLogPutCompleteDbEntry(entry);
                LOG.info(String.format("fixed %s", entry.fileName));
                return;
            } catch(Throwable e) {
                LOG.info(String.format("%s: %s", e.getClass().getSimpleName(), e));
                if(retry >= 3) {
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
        LOG.debug(String.format("copy file: %s -> %s", src, dst));
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
    private Path stripUserInfo(Path path) throws java.net.URISyntaxException {
        URI uri = path.toUri();
        URI newUri = new URI(
            uri.getScheme(),
            null,
            uri.getHost(),
            uri.getPort(),
            uri.getPath(),
            uri.getQuery(),
            uri.getFragment()
        );
        return new Path(newUri);
    }

    // TODO - use java version of FileNames utils?
    private Pattern deltaFilePattern;

    private boolean isDeltaFile(Path path) {
        return deltaFilePattern.matcher(path.getName()).matches();
    }

    private Path deltaFile(Path tablePath, long version) {
        return new Path(tablePath, String.format("_delta_log/%020d.json", version));
    }

    private long deltaVersion(Path path) {
        return Long.parseLong(path.getName().split("\\.")[0]);
    }

}
