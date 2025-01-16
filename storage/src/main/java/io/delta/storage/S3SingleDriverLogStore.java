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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.io.CountingOutputStream;
import io.delta.storage.internal.FileNameUtils;
import io.delta.storage.internal.PathLock;
import io.delta.storage.internal.S3LogStoreUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

/**
 * Single Spark-driver/JVM LogStore implementation for S3.
 * <p>
 * We assume the following from S3's {@link FileSystem} implementations:
 * <ul>
 *   <li>File writing on S3 is all-or-nothing, whether overwrite or not.</li>
 *   <li>List-after-write can be inconsistent.</li>
 * </ul>
 * <p>
 * Regarding file creation, this implementation:
 * <ul>
 *   <li>Opens a stream to write to S3 (regardless of the overwrite option).</li>
 *   <li>Failures during stream write may leak resources, but may never result in partial
 *       writes.</li>
 * </ul>
 * <p>
 * Regarding directory listing, this implementation:
 * <ul>
 *   <li>returns a list by merging the files listed from S3 and recently-written files from the
 *       cache.</li>
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

    /**
     * A global cache that records the metadata of the files recently written.
     * As list-after-write may be inconsistent on S3, we can use the files in the cache
     * to fix the inconsistent file listing.
     */
    private static final Cache<Path, FileMetadata> writtenPathCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(120, TimeUnit.MINUTES)
            .build();

    /////////////////////////////////////////////
    // Constructor and Instance Helper Methods //
    /////////////////////////////////////////////

    public S3SingleDriverLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    /**
     * Check if the path is an initial version of a Delta log.
     */
    private boolean isInitialVersion(Path path) {
        return FileNameUtils.isDeltaFile(path) && FileNameUtils.deltaVersion(path) == 0L;
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
     * Merge two lists of {@link FileStatus} into a single list ordered by file path name.
     * In case both lists have {@link FileStatus}'s for the same file path, keep the one from
     * `listWithPrecedence` and discard the other from `list`.
     */
    private Iterator<FileStatus> mergeFileLists(
            List<FileStatus> list,
            List<FileStatus> listWithPrecedence) {
        final Map<Path, FileStatus> fileStatusMap = new HashMap<>();

        // insert all elements from `listWithPrecedence` (highest priority)
        // and then insert elements from `list` if and only if that key doesn't already exist
        Stream.concat(listWithPrecedence.stream(), list.stream())
            .forEach(fs -> fileStatusMap.putIfAbsent(fs.getPath(), fs));

        return fileStatusMap
            .values()
            .stream()
            .sorted(Comparator.comparing(a -> a.getPath().getName()))
            .iterator();
    }

    /**
     * List files starting from `resolvedPath` (inclusive) in the same directory.
     */
    private List<FileStatus> listFromCache(
            FileSystem fs,
            Path resolvedPath) {
        final Path pathKey = stripUserInfo(resolvedPath);

        return writtenPathCache
            .asMap()
            .entrySet()
            .stream()
            .filter(e -> {
                final Path path = e.getKey();
                return path.getParent().equals(pathKey.getParent()) &&
                       path.getName().compareTo(pathKey.getName()) >= 0;
            }).map(e -> {
                final Path path = e.getKey();
                final FileMetadata fileMetadata = e.getValue();
                return new FileStatus(
                    fileMetadata.length,
                    false, // isDir
                    1, // block_replication
                    fs.getDefaultBlockSize(path),
                    fileMetadata.modificationTime,
                    path);
            }).collect(Collectors.toList());
    }

    /**
     * List files starting from `resolvedPath` (inclusive) in the same directory, which merges
     * the file system list and the cache list when `useCache` is on, otherwise
     * use file system list only.
     */
    private Iterator<FileStatus> listFromInternal(
            FileSystem fs,
            Path resolvedPath,
            boolean useCache) throws IOException {
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

        final List<FileStatus> listedFromFs = Arrays
            .stream(statuses)
            .filter(s -> s.getPath().getName().compareTo(resolvedPath.getName()) >= 0)
            .collect(Collectors.toList());

        final List<FileStatus> listedFromCache = useCache ?
            listFromCache(fs, resolvedPath) : Collections.emptyList();

        // File statuses listed from file system take precedence
        return mergeFileLists(listedFromCache, listedFromFs);
    }

    /**
     * Check if a path exists. Normally we check both the file system and the cache, but when the
     * path is the first version of a Delta log, we ignore the cache.
     */
    private boolean exists(
            FileSystem fs,
            Path resolvedPath) throws IOException {
        final boolean useCache = !isInitialVersion(resolvedPath);
        final Iterator<FileStatus> iter = listFromInternal(fs, resolvedPath, useCache);
        if (!iter.hasNext()) return false;

        return iter.next().getPath().getName().equals(resolvedPath.getName());
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
                if (exists(fs, resolvedPath) && !overwrite) {
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

                // When a Delta log starts afresh, all cached files in that Delta log become
                // obsolete, so we remove them from the cache.
                if (isInitialVersion(resolvedPath)) {
                    final List<Path> obsoleteFiles = writtenPathCache
                        .asMap()
                        .keySet()
                        .stream()
                        .filter(p -> p.getParent().equals(resolvedPath.getParent()))
                        .collect(Collectors.toList());

                    writtenPathCache.invalidateAll(obsoleteFiles);
                }

                // Cache the information of written files to help fix the inconsistency in future
                // listings
                writtenPathCache.put(
                    resolvedPath,
                    new FileMetadata(stream.getCount(), System.currentTimeMillis())
                );
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
        return listFromInternal(fs, resolvedPath, true); // useCache=true
    }

    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) {
        return false;
    }

    //////////////////
    // Helper Class //
    //////////////////

    /**
     * The file metadata to be stored in the cache.
     */
    private class FileMetadata {
        private long length;
        private long modificationTime;

        public FileMetadata(long length, long modificationTime) {
            this.length = length;
            this.modificationTime = modificationTime;
        }
    }
}
