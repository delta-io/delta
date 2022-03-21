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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.io.CountingOutputStream;
import io.delta.storage.internal.FileNameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Single Spark-driver/JVM LogStore implementation for S3.
 *
 * We assume the following from S3's {@link FileSystem} implementations:
 * - File writing on S3 is all-or-nothing, whether overwrite or not.
 * - List-after-write can be inconsistent.
 *
 * Regarding file creation, this implementation:
 * - Opens a stream to write to S3 (regardless of the overwrite option).
 * - Failures during stream write may leak resources, but may never result in partial writes.
 *
 * Regarding directory listing, this implementation:
 * - returns a list by merging the files listed from S3 and recently-written files from the cache.
 */
public class S3SingleDriverLogStore extends HadoopFileSystemLogStore {

    ///////////////////////////
    // Static Helper Methods //
    ///////////////////////////

    /**
     * A global path lock to ensure that no concurrent writers writing to the same path in the same
     * JVM.
     */
    private static final ConcurrentHashMap<Path, Object> pathLock = new ConcurrentHashMap<>();

    /**
     * A global cache that records the metadata of the files recently written.
     * As list-after-write may be inconsistent on S3, we can use the files in the cache
     * to fix the inconsistent file listing.
     */
    private static final Cache<Path, FileMetadata> writtenPathCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(120, TimeUnit.MINUTES)
            .build();

    /**
     * Release the lock for the path after writing.
     *
     * Note: the caller should resolve the path to make sure we are locking the correct absolute
     * path.
     */
    private static void releasePathLock(Path resolvedPath) {
        final Object lock = pathLock.remove(resolvedPath);
        synchronized(lock) {
            lock.notifyAll();
        }
    }

    /**
     * Acquire a lock for the path before writing.
     *
     * Note: the caller should resolve the path to make sure we are locking the correct absolute
     * path.
     */
    private static void acquirePathLock(Path resolvedPath) throws InterruptedException {
        while (true) {
            final Object lock = pathLock.putIfAbsent(resolvedPath, new Object());
            if (lock == null) {
                return;
            }
            synchronized (lock) {
                while (pathLock.get(resolvedPath) == lock) {
                    lock.wait();
                }
            }
        }
    }

    /**
     * Check if the path is an initial version of a Delta log.
     */
    public static boolean isInitialVersion(Path path) {
        return FileNameUtils.isDeltaFile(path) && FileNameUtils.deltaVersion(path) == 0L;
    }

    /////////////////////////////////////////////
    // Constructor and Instance Helper Methods //
    /////////////////////////////////////////////

    public S3SingleDriverLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    private Path getPathKey(Path resolvedPath) throws URISyntaxException {
        return stripUserInfo(resolvedPath);
    }

    private Path stripUserInfo(Path path) throws URISyntaxException {
        final URI uri = path.toUri();
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
    }

    /**
     * Merge two lists of {@link FileStatus} into a single list ordered by file path name.
     * In case both lists have {@link FileSystem}'s for the same file path, keep the one from
     * `listWithPrecedence` and discard the other from `list`.
     */
    private Iterator<FileStatus> mergeFileLists(
            List<FileStatus> list,
            List<FileStatus> listWithPrecedence) {
        final Map<Path, FileStatus> fileStatusMap = new HashMap<>();

        final Map<Path, FileStatus> lowPriorityMap = list
            .stream()
            .collect(Collectors.toMap(FileStatus::getPath, Function.identity()));

        final Map<Path, FileStatus> highPriorityMap = listWithPrecedence
            .stream()
            .collect(Collectors.toMap(FileStatus::getPath, Function.identity()));

        fileStatusMap.putAll(lowPriorityMap);
        fileStatusMap.putAll(highPriorityMap); // replaces existing entry if conflict

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
            Path resolvedPath) throws URISyntaxException {
        final Path pathKey = getPathKey(resolvedPath);

        return writtenPathCache
            .asMap()
            .entrySet()
            .stream()
            .filter(e -> {
                final Path path = e.getKey();
                return path.getParent() == pathKey.getParent() &&
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

    private Iterator<FileStatus> listFromInternal(
            FileSystem fs,
            Path resolvedPath,
            boolean useCache) throws IOException, URISyntaxException {
        final Path parentPath = resolvedPath.getParent();
        if (!fs.exists(parentPath)) {
            throw new FileNotFoundException(
                String.format("No such file or directory: %s", parentPath)
            );
        }

        final List<FileStatus> listedFromFs = Arrays
            .stream(fs.listStatus(parentPath))
            .filter(s -> s.getPath().getName().compareTo(resolvedPath.getName()) >= 0)
            .collect(Collectors.toList());

        final List<FileStatus> listedFromCache = useCache ?
            listFromCache(fs, resolvedPath) : Collections.emptyList();

        return mergeFileLists(listedFromCache, listedFromFs);
    }

    /**
     * Check if a path exists. Normally we check both the file system and the cache, but when the
     * path is the first version of a Delta log, we ignore the cache.
     */
    private boolean exists(
            FileSystem fs,
            Path resolvedPath) throws IOException, URISyntaxException {
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
        try {
            final FileSystem fs = path.getFileSystem(hadoopConf);
            final Path resolvedPath = stripUserInfo(fs.makeQualified(path));
            final Path lockedPath = getPathKey(resolvedPath);
            acquirePathLock(lockedPath);

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
                        .filter(p -> p.getParent() == lockedPath.getParent())
                        .collect(Collectors.toList());

                    writtenPathCache.invalidateAll(obsoleteFiles);
                }

                // Cache the information of written files to help fix the inconsistency in future
                // listings
                writtenPathCache.put(
                    lockedPath,
                    new FileMetadata(stream.getCount(), System.currentTimeMillis())
                );
            } catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
                // Convert Hadoop's FileAlreadyExistsException to Java's FileAlreadyExistsException
                throw new java.nio.file.FileAlreadyExistsException(e.getMessage());
            } finally {
                releasePathLock(lockedPath);
            }
        } catch (java.net.URISyntaxException e) {
            throw new IOException("S3SingleDriverLogStore: java.net.URISyntaxException", e);
        } catch (java.lang.InterruptedException e) {
            throw new IOException("S3SingleDriverLogStore: java.lang.InterruptedException", e);
        }
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) throws IOException {
        try {
            final FileSystem fs = path.getFileSystem(hadoopConf);
            final Path resolvedPath = stripUserInfo(fs.makeQualified(path));
            return listFromInternal(fs, resolvedPath, true); // useCache=true
        } catch (java.net.URISyntaxException e) {
            throw new IOException("S3SingleDriverLogStore: java.net.URISyntaxException", e);
        }
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
