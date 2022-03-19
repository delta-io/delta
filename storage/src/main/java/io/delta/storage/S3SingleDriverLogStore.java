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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Single Spark-driver/JVM LogStore implementation for S3.
 *
 * We assume the following from S3's [[FileSystem]] implementations:
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

    public S3SingleDriverLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    private FileSystem resolved(Path path, Configuration hadoopConf) throws IOException {
        return path.getFileSystem(hadoopConf);
    }

    private Path getPathKey(Path resolvedPath) throws URISyntaxException {
        return stripUserInfo(resolvedPath);
    }

    private Path stripUserInfo(Path path) throws URISyntaxException {
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

    /**
     * Merge two iterators of [[FileStatus]] into a single iterator ordered by file path name.
     * In case both iterators have [[FileStatus]]s for the same file path, keep the one from
     * `iterWithPrecedence` and discard that from `iter`.
     */
//    private Iterator<FileStatus> mergeFileIterators(
//            Iterator<FileStatus> iter,
//            Iterator<FileStatus> iterWithPrecedence) {
//            iter.forEach()
////                (iter.map(f => (f.getPath, f)).toMap ++ iterWithPrecedence.map(f => (f.getPath, f)))
////                .values
////                .toSeq
////                .sortBy(_.getPath.getName)
////                .iterator
//    }

    /**
     * List files starting from `resolvedPath` (inclusive) in the same directory.
     */
//    private FileStatus listFromCache(FileSystem fs, Path resolvedPath) throws URISyntaxException {
//            final var pathKey = getPathKey(resolvedPath);
//            writtenPathCache.iterator.filter { case (path, _) =>
//            path.getParent == pathKey.getParent() && path.getName >= pathKey.getName }
//      .map { case (path, fileMetadata) =>
//            new FileStatus(
//                    fileMetadata.length,
//                    false,
//                    1,
//                    fs.getDefaultBlockSize(path),
//                    fileMetadata.modificationTime,
//                    path);
//        }
//    }

    /**
     * Check if the path is an initial version of a Delta log.
     */
//    public Boolean isInitialVersion(Path path) {
//        if ( (FileNames.isDeltaFile(path)) & (FileNames.deltaVersion(path) == 0L)) {
//            return true;
//        }
//        return false;
//    }

    @Override
    public void write(
            Path path,
            Iterator<String> actions,
            Boolean overwrite,
            Configuration hadoopConf) throws IOException {
        writeWithRename(path, actions, overwrite, hadoopConf);
    }

    public Boolean isPartialWriteVisible(
            Path path,
            Configuration hadoopConf) {
        return false;
    }

    /**
     * A global path lock to ensure that no concurrent writers writing to the same path in the same
     * JVM.
     */
    private static final ConcurrentHashMap<Path, Object> pathLock = new ConcurrentHashMap<Path, Object>();

    /**
     * A global cache that records the metadata of the files recently written.
     * As list-after-write may be inconsistent on S3, we can use the files in the cache
     * to fix the inconsistent file listing.
     */
    private static final ConcurrentMap<Path, FileMetadata> writtenPathCache = CacheBuilder.newBuilder().expireAfterAccess(120, TimeUnit.MINUTES).<Path,FileMetadata>build().asMap();

    /**
     * Release the lock for the path after writing.
     *
     * Note: the caller should resolve the path to make sure we are locking the correct absolute path.
     */
        private static final void releasePathLock(Path resolvedPath) {
            final Object lock = pathLock.remove(resolvedPath);
            synchronized(lock) {
                lock.notifyAll();
            }
        }

    /**
     * Acquire a lock for the path before writing.
     *
     * Note: the caller should resolve the path to make sure we are locking the correct absolute path.
     */
    private static final void acquirePathLock(Path resolvedPath) throws InterruptedException {
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
     * The file metadata to be stored in the cache.
     */
    class FileMetadata {
        private long length;
        private long modificationTime;
    }
}