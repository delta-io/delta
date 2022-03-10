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

import org.apache.spark.sql.delta.util.FileNames;
import java.net.URI;
import java.util.concurrent.{ConcurrentHashMap,TimeUnit};

import org.apache.spark.sql.delta.util.FileNames;
import com.google.common.cache.CacheBuilder;
import com.google.common.io.CountingOutputStream;
import org.apache.hadoop.conf.Configuration;

import org.apache.spark.SparkConf;

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
public class  S3SingleDriverLogStore extends HadoopFileSystemLogStore {

    private FileSystem resolved(Path path, Configuration hadoopConf) {
        return path.getFileSystem(hadoopConf);
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
        )
        return new Path(newUri);
    }
    
    // Merge two iterators of [[FileStatus]] into a single iterator ordered by file path name.
    // In case both iterators have [[FileStatus]]s for the same file path, keep the one from
    // `iterWithPrecedence` and discard that from `iter`.
    private Iterator<FileStatus> mergeFileIterators(
        Iterator<FileStatus> iter,
        Iterator<FileStatus> iterWithPrecedence) {
        //     (iter.map(f => (f.getPath, f)).toMap ++ iterWithPrecedence.map(f => (f.getPath, f)))
        //   .values
        //   .toSeq
        //   .sortBy(_.getPath.getName)
        //   .iterator
    }
    
    // List files starting from `resolvedPath` (inclusive) in the same directory.
    //   private def listFromCache(fs: FileSystem, resolvedPath: Path) = {
    //     val pathKey = getPathKey(resolvedPath)
    //     writtenPathCache
    //       .asMap()
    //       .asScala
    //       .iterator
    //       .filter { case (path, _) =>
    //         path.getParent == pathKey.getParent() && path.getName >= pathKey.getName }
    //       .map { case (path, fileMetadata) =>
    //         new FileStatus(
    //           fileMetadata.length,
    //           false,
    //           1,
    //           fs.getDefaultBlockSize(path),
    //           fileMetadata.modificationTime,
    //           path)
    //       }
    //   }
    
    // List files starting from `resolvedPath` (inclusive) in the same directory, which merges
    // the file system list and the cache list when `useCache` is on, otherwise
    // use file system list only.
    private Iterator<FileStatus> listFromInternal(
                FileSystem fs, 
                Path resolvedPath, 
                Boolean useCache) throws IOException {

        Path parentPath = resolvedPath.getParent();

        if (!fs.exists(parentPath)) {
            throw new FileNotFoundException("No such file or directory: %s", parentPath);
        }

        // val listedFromFs = fs.listStatus(parentPath).filter(_.getPath.getName >= resolvedPath.getName).iterator
        // val listedFromCache = if (useCache) listFromCache(fs, resolvedPath) else Iterator.empty

        // File statuses listed from file system take precedence
        return mergeFileIterators(listedFromCache, listedFromFs);
    }

    // List files starting from `resolvedPath` (inclusive) in the same directory.
    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration hadoopConf) {
        FileSystem fs = resolve(path);
        Path resolvedPath = stripUserInfo(fs.makeQualified(path));
        return listFromInternal(fs, resolvedPath, true);
    }
    
    private Boolean isInitialVersion(Path path) {
        return FileNames.isDeltaFile(path) && (FileNames.deltaVersion(path) == 0L);
    }
    
    // Check if a path exists. Normally we check both the file system and the cache, but when the
    // path is the first version of a Delta log, we ignore the cache.
    private Boolean exists(FileSystem fs, Path resolvedPath) {
        // Ignore the cache for the first file of a Delta log
        //     listFromInternal(fs, resolvedPath, !isInitialVersion(resolvedPath))
        //       .take(1)
        //       .exists(_.getPath.getName == resolvedPath.getName)
    }
    

        @Override
        public void write(
                Path path, 
                Iterator<String> actions, 
                Boolean overwrite,
                Configuration hadoopConf) throws IOException {
            FileSystem fs = resolve(path);
            Path resolvedPath = stripUserInfo(fs.makeQualified(path));
            Path lockedPath = getPathKey(resolvedPath);
            acquirePathLock(lockedPath);
               //     try {
    //       if (exists(fs, resolvedPath) && !overwrite) {
    //         throw new java.nio.file.FileAlreadyExistsException(resolvedPath.toUri.toString)
    //       }
    //       val stream = new CountingOutputStream(fs.create(resolvedPath, overwrite))
    //       actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
    //       stream.close()
    
    //       // When a Delta log starts afresh, all cached files in that Delta log become obsolete,
    //       // so we remove them from the cache.
    //       if (isInitialVersion(resolvedPath)) {
    //         val obsoleteFiles = writtenPathCache
    //           .asMap()
    //           .asScala
    //           .keys
    //           .filter(_.getParent == lockedPath.getParent())
    //           .asJava
    
    //         writtenPathCache.invalidateAll(obsoleteFiles)
    //       }
    
    //       // Cache the information of written files to help fix the inconsistency in future listings
    //       writtenPathCache.put(lockedPath,
    //         FileMetadata(stream.getCount(), System.currentTimeMillis()))
    //     } catch {
    //       // Convert Hadoop's FileAlreadyExistsException to Java's FileAlreadyExistsException
    //       case e: org.apache.hadoop.fs.FileAlreadyExistsException =>
    //           throw new java.nio.file.FileAlreadyExistsException(e.getMessage)
    //     } finally {
    //       releasePathLock(lockedPath)
    //     }
    }

    // Acquire a lock for the path before writing.
    // Note: the caller should resolve the path to make sure we are locking the correct absolute path.
    private void acquirePathLock(Path resolvedPath) {
        // while (true) {
        //   val lock = pathLock.putIfAbsent(resolvedPath, new Object)
        //   if (lock == null) return
        //   lock.synchronized {
        //     while (pathLock.get(resolvedPath) == lock) {
        //       lock.wait()
        //     }
        //   }
        // }
    }

    @Override
    public Boolean isPartialWriteVisible(Path path) {
        return false;
    }
    
    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration hadoopConf) {
        return false;
    }
    
    @Override
    public void invalidateCache() {
        writtenPathCache.invalidateAll();
    }
}
