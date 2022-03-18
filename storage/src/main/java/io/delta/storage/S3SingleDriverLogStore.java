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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.sql.delta.util.FileNames;
import java.util.concurrent.ConcurrentHashMap;
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

    static class FileMetadata {
        private long length;
        private long modificationTime;
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
//    private static final LoadingCache<String, String> writtenPathCache = CacheBuilder.newBuilder().expireAfterAccess(120, TimeUnit.MINUTES).build(Path, FileMetadata);

    /**
     * Release the lock for the path after writing.
     *
     * Note: the caller should resolve the path to make sure we are locking the correct absolute path.
     */
    //    private static final void releasePathLock(Path resolvedPath) {
    //        var lock = pathLock.remove(resolvedPath);
    //        lock.synchronized {
    //            lock.notifyAll();
    //        }
    //    }

    /**
     * Check if the path is an initial version of a Delta log.
     */
    public Boolean isInitialVersion(Path path) {
        if ( (FileNames.isDeltaFile(path)) & (FileNames.deltaVersion(path) == 0L)) {
            return true;
        }
        return false;
    }

    public S3SingleDriverLogStore(Configuration hadoopConf) {
        super(hadoopConf);
    }

    private Path getPathKey(Path resolvedPath) {
        return stripUserInfo(resolvedPath);
    }

    private Path stripUserInfo(Path path) {
        URI uri = path.toUri();
        URI newUri = new URI(
            uri.getScheme,
            null,
            uri.getHost,
            uri.getPort,
            uri.getPath,
            uri.getQuery,
            uri.getFragment
        );
        return new Path(newUri);
    }

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

}
