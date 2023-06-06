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
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.EnumSet;
import java.util.Iterator;

import io.delta.storage.internal.LogStoreErrors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.CreateFlag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link LogStore} implementation for HDFS, which uses Hadoop {@link FileContext} API's to
 * provide the necessary atomic and durability guarantees:
 * <ol>
 *     <li>Atomic visibility of files: `FileContext.rename` is used write files which is atomic for
 *         HDFS.</li>
 *     <li>Consistent file listing: HDFS file listing is consistent.</li>
 * </ol>
 */
public class HDFSLogStore extends HadoopFileSystemLogStore {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSLogStore.class);
    public static final String NO_ABSTRACT_FILE_SYSTEM_EXCEPTION_MESSAGE = "No AbstractFileSystem";

    public HDFSLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    @Override
    public void write(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        final boolean isLocalFs = path.getFileSystem(hadoopConf) instanceof RawLocalFileSystem;
        if (isLocalFs) {
            // We need to add `synchronized` for RawLocalFileSystem as its rename will not throw an
            // exception when the target file exists. Hence we must make sure `exists + rename` in
            // `writeInternal` for RawLocalFileSystem is atomic in our tests.
            synchronized(this) {
                writeInternal(path, actions, overwrite, hadoopConf);
            }
        } else {
            // rename is atomic and also will fail when the target file exists. Not need to add the
            // extra `synchronized`.
            writeInternal(path, actions, overwrite, hadoopConf);
        }
    }

    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) {
        return true;
    }

    /**
     * @throws IOException if this HDFSLogStore is used to write into a Delta table on a non-HDFS
     *                     storage system.
     * @throws FileAlreadyExistsException if {@code overwrite} is false and the file at {@code path}
     *                                    already exists.
     */
    private void writeInternal(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        final FileContext fc;
        try {
            fc = FileContext.getFileContext(path.toUri(), hadoopConf);
        } catch (IOException e) {
            if (e.getMessage().contains(NO_ABSTRACT_FILE_SYSTEM_EXCEPTION_MESSAGE)) {
                final IOException newException =
                    LogStoreErrors.incorrectLogStoreImplementationException(e);
                LOG.error(newException.getMessage(), newException.getCause());
                throw newException;
            } else {
                throw e;
            }
        }

        if (!overwrite && fc.util().exists(path)) {
            // This is needed for the tests to throw error with local file system
            throw new FileAlreadyExistsException(path.toString());
        }

        final Path tempPath = createTempPath(path);
        boolean streamClosed = false; // This flag is to avoid double close
        boolean renameDone = false; // This flag is to save the delete operation in most cases.
        final FSDataOutputStream stream = fc.create(
            tempPath,
            EnumSet.of(CreateFlag.CREATE),
            Options.CreateOpts.checksumParam(Options.ChecksumOpt.createDisabled())
        );

        try {
            while (actions.hasNext()) {
                stream.write((actions.next() + "\n").getBytes(StandardCharsets.UTF_8));
            }
            stream.close();
            streamClosed = true;
            try {
                final Options.Rename renameOpt =
                    overwrite ? Options.Rename.OVERWRITE : Options.Rename.NONE;
                fc.rename(tempPath, path, renameOpt);
                renameDone = true;
                // TODO: this is a workaround of HADOOP-16255 - remove this when HADOOP-16255 is
                // resolved
                tryRemoveCrcFile(fc, tempPath);
            } catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
                throw new FileAlreadyExistsException(path.toString());
            }
        } finally {
            if (!streamClosed) {
                stream.close();
            }
            if (!renameDone) {
                fc.delete(tempPath, false); // recursive=false
            }
        }

        msyncIfSupported(path, hadoopConf);
    }

    /**
     * Normally when using HDFS with an Observer NameNode setup, there would be read after write
     * consistency within a single process, so the write would be guaranteed to be visible on the
     * next read. However, since we are using the FileContext API for writing (for atomic rename),
     * and the FileSystem API for reading (for more compatibility with various file systems), we
     * are essentially using two separate clients that are not guaranteed to be kept in sync.
     * Therefore we "msync" the FileSystem instance, which is cached across all uses of the same
     * protocol/host combination, to make sure the next read through the HDFSLogStore can see this
     * write.
     * Any underlying FileSystem that is not the DistributedFileSystem will simply throw an
     * UnsupportedOperationException, which can be ignored. Additionally, if an older version of
     * Hadoop is being used that does not include msync, a NoSuchMethodError will be thrown while
     * looking up the method, which can also be safely ignored.
     */
    private void msyncIfSupported(Path path, Configuration hadoopConf) throws IOException {
        try {
            FileSystem fs = path.getFileSystem(hadoopConf);
            Method msync = fs.getClass().getMethod("msync");
            msync.invoke(fs);
        } catch (InterruptedIOException e) {
            throw e;
        } catch (Throwable e) {
            if (e instanceof InterruptedException) {
                // Propagate the interrupt status
                Thread.currentThread().interrupt();
            }
            // We ignore non fatal errors as calling msync is best effort.
        }
    }

    /**
     * @throws IOException if a fatal exception occurs. Will try to ignore most exceptions.
     */
    private void tryRemoveCrcFile(FileContext fc, Path path) throws IOException {
        try {
            final Path checksumFile =
                new Path(path.getParent(), String.format(".%s.crc", path.getName()));

            if (fc.util().exists(checksumFile)) {
                // checksum file exists, deleting it
                fc.delete(checksumFile, true); // recursive=true
            }
        } catch (Throwable e) {
            if (!LogStoreErrors.isNonFatal(e)) {
                throw e;
            }
            // else, ignore - we are removing crc file as "best-effort"
        }
    }
}
