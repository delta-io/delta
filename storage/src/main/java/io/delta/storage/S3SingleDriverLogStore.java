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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.google.common.io.CountingOutputStream;
import io.delta.storage.internal.PathLock;
import io.delta.storage.internal.S3LogStoreUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

/**
 * Single JVM LogStore implementation for S3.
 * <p>
 * We assume the following from S3's {@link FileSystem} implementations:
 * <ul>
 *   <li>File writing on S3 is all-or-nothing, whether overwrite or not.</li>
 *   <li>List-after-write is strongly consistent.</li>
 * </ul>
 * <p>
 * Regarding file creation, this implementation:
 * <ul>
 *   <li>Opens a stream to write to S3 (regardless of the overwrite option).</li>
 *   <li>Failures during stream write may leak resources, but may never result in partial
 *       writes.</li>
 * </ul>
 */
public class S3SingleDriverLogStore extends HadoopFileSystemLogStore {

    /**
     * Enables a faster implementation of listFrom by setting the startAfter parameter in S3 list
     * requests. The feature is enabled by setting the property delta.enableFastS3AListFrom in the
     * Hadoop configuration.
     *
     * This feature requires the Hadoop file system used for S3 paths to be castable to
     * org.apache.hadoop.fs.s3a.S3AFileSystem.
     */
    private final boolean enableFastListFrom
            = initHadoopConf().getBoolean("delta.enableFastS3AListFrom", false);

    ///////////////////////////
    // Static Helper Methods //
    ///////////////////////////

    /**
     * A global path lock to ensure that no concurrent writers writing to the same path in the same
     * JVM.
     */
    private static final PathLock pathLock = new PathLock();

    /////////////////////////////////////////////
    // Constructor and Instance Helper Methods //
    /////////////////////////////////////////////

    public S3SingleDriverLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    private Path resolvePath(FileSystem fs, Path path) {
        return stripUserInfo(fs.makeQualified(path));
    }

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

    /**
     * List files starting from `resolvedPath` (inclusive) in the same directory, which merges
     * the file system list and the cache list when `useCache` is on, otherwise
     * use file system list only.
     */
    private Iterator<FileStatus> listFromInternal(
            FileSystem fs,
            Path resolvedPath) throws IOException {
        final Path parentPath = resolvedPath.getParent();
        if (!fs.exists(parentPath)) {
            throw new FileNotFoundException(
                String.format("No such file or directory: %s", parentPath)
            );
        }

        FileStatus[] statuses;
        if (
            // LocalFileSystem and RawLocalFileSystem checks are needed for tests to pass
            fs instanceof LocalFileSystem || fs instanceof RawLocalFileSystem || !enableFastListFrom
        ) {
            statuses = fs.listStatus(parentPath);
        } else {
            statuses = S3LogStoreUtil.s3ListFromArray(fs, resolvedPath, parentPath);
        }

        return Arrays
            .stream(statuses)
            .filter(s -> s.getPath().getName().compareTo(resolvedPath.getName()) >= 0)
            .sorted(Comparator.comparing(a -> a.getPath().getName()))
            .iterator();
    }

    ////////////////////////
    // Public API Methods //
    ////////////////////////

    @Override
    public void write(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        final FileSystem fs = path.getFileSystem(hadoopConf);
        final Path resolvedPath = resolvePath(fs, path);
        try {
            pathLock.acquire(resolvedPath);
            try {
                if (fs.exists(resolvedPath) && !overwrite) {
                    throw new java.nio.file.FileAlreadyExistsException(
                        resolvedPath.toUri().toString()
                    );
                }

                final CountingOutputStream stream =
                    new CountingOutputStream(fs.create(resolvedPath, overwrite));

                while (actions.hasNext()) {
                    stream.write((actions.next() + "\n").getBytes(StandardCharsets.UTF_8));
                }
                stream.close();
            } catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
                // Convert Hadoop's FileAlreadyExistsException to Java's FileAlreadyExistsException
                throw new java.nio.file.FileAlreadyExistsException(e.getMessage());
            }
        } catch (java.lang.InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
        } finally {
            pathLock.release(resolvedPath);
        }
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) throws IOException {
        final FileSystem fs = path.getFileSystem(hadoopConf);
        final Path resolvedPath = resolvePath(fs, path);
        return listFromInternal(fs, resolvedPath);
    }

    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) {
        return false;
    }
}
