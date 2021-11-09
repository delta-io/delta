/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal.storage

import java.io.FileNotFoundException
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._

import com.google.common.cache.CacheBuilder
import com.google.common.io.CountingOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import io.delta.standalone.internal.util.FileNames

private[internal] class S3SingleDriverLogStore(override val initHadoopConf: Configuration)
  extends HadoopFileSystemLogStore(initHadoopConf) {

  import S3SingleDriverLogStore._

  ///////////////////////////////////////////////////////////////////////////
  // Public API Methods
  ///////////////////////////////////////////////////////////////////////////

  override def write(
      path: Path,
      actions: java.util.Iterator[String],
      overwrite: java.lang.Boolean,
      hadoopConf: Configuration): Unit = {
    val (fs, resolvedPath) = resolved(path, hadoopConf)
    val lockedPath = getPathKey(resolvedPath)
    acquirePathLock(lockedPath)
    try {
      if (exists(fs, resolvedPath) && !overwrite) {
        throw new java.nio.file.FileAlreadyExistsException(resolvedPath.toUri.toString)
      }
      val stream = new CountingOutputStream(fs.create(resolvedPath, overwrite))
      actions.asScala.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
      stream.close()

      // When a Delta log starts afresh, all cached files in that Delta log become obsolete,
      // so we remove them from the cache.
      if (isInitialVersion(resolvedPath)) {
        val obsoleteFiles = writtenPathCache
          .asMap()
          .asScala
          .keys
          .filter(_.getParent == lockedPath.getParent())
          .asJava

        writtenPathCache.invalidateAll(obsoleteFiles)
      }

      // Cache the information of written files to help fix the inconsistency in future listings
      writtenPathCache.put(lockedPath,
        FileMetadata(stream.getCount(), System.currentTimeMillis()))
    } catch {
      // Convert Hadoop's FileAlreadyExistsException to Java's FileAlreadyExistsException
      case e: org.apache.hadoop.fs.FileAlreadyExistsException =>
        throw new java.nio.file.FileAlreadyExistsException(e.getMessage)
    } finally {
      releasePathLock(lockedPath)
    }
  }

  override def isPartialWriteVisible(path: Path, hadoopConf: Configuration): java.lang.Boolean = {
    false
  }

  /**
   * List files starting from `resolvedPath` (inclusive) in the same directory.
   */
  override def listFrom(path: Path, hadoopConf: Configuration): java.util.Iterator[FileStatus] = {
    val (fs, resolvedPath) = resolved(path, hadoopConf)
    listFromInternal(fs, resolvedPath).asJava
  }

  ///////////////////////////////////////////////////////////////////////////
  // Helper Methods
  ///////////////////////////////////////////////////////////////////////////

  private def resolved(path: Path, hadoopConf: Configuration): (FileSystem, Path) = {
    val fs = path.getFileSystem(hadoopConf)
    val resolvedPath = stripUserInfo(fs.makeQualified(path))
    (fs, resolvedPath)
  }

  private def getPathKey(resolvedPath: Path): Path = {
    stripUserInfo(resolvedPath)
  }

  private def stripUserInfo(path: Path): Path = {
    val uri = path.toUri
    val newUri = new URI(
      uri.getScheme,
      null,
      uri.getHost,
      uri.getPort,
      uri.getPath,
      uri.getQuery,
      uri.getFragment)
    new Path(newUri)
  }

  /**
   * Merge two iterators of [[FileStatus]] into a single iterator ordered by file path name.
   * In case both iterators have [[FileStatus]]s for the same file path, keep the one from
   * `iterWithPrecedence` and discard that from `iter`.
   */
  private def mergeFileIterators(
      iter: Iterator[FileStatus],
      iterWithPrecedence: Iterator[FileStatus]): Iterator[FileStatus] = {
    (iter.map(f => (f.getPath, f)).toMap ++ iterWithPrecedence.map(f => (f.getPath, f)))
      .values
      .toSeq
      .sortBy(_.getPath.getName)
      .iterator
  }

  /**
   * List files starting from `resolvedPath` (inclusive) in the same directory.
   */
  private def listFromCache(fs: FileSystem, resolvedPath: Path) = {
    val pathKey = getPathKey(resolvedPath)
    writtenPathCache
      .asMap()
      .asScala
      .iterator
      .filter { case (path, _) =>
        path.getParent == pathKey.getParent() && path.getName >= pathKey.getName }
      .map { case (path, fileMetadata) =>
        new FileStatus(
          fileMetadata.length,
          false,
          1,
          fs.getDefaultBlockSize(path),
          fileMetadata.modificationTime,
          path)
      }
  }

  /**
   * List files starting from `resolvedPath` (inclusive) in the same directory, which merges
   * the file system list and the cache list when `useCache` is on, otherwise
   * use file system list only.
   */
  private def listFromInternal(fs: FileSystem, resolvedPath: Path, useCache: Boolean = true) = {
    val parentPath = resolvedPath.getParent
    if (!fs.exists(parentPath)) {
      throw new FileNotFoundException(s"No such file or directory: $parentPath")
    }
    val listedFromFs =
      fs.listStatus(parentPath).filter(_.getPath.getName >= resolvedPath.getName).iterator
    val listedFromCache = if (useCache) listFromCache(fs, resolvedPath) else Iterator.empty

    // File statuses listed from file system take precedence
    mergeFileIterators(listedFromCache, listedFromFs)
  }

  /**
   * Check if the path is an initial version of a Delta log.
   */
  private def isInitialVersion(path: Path): Boolean = {
    FileNames.isDeltaFile(path) && FileNames.deltaVersion(path) == 0L
  }

  /**
   * Check if a path exists. Normally we check both the file system and the cache, but when the
   * path is the first version of a Delta log, we ignore the cache.
   */
  private def exists(fs: FileSystem, resolvedPath: Path): Boolean = {
    // Ignore the cache for the first file of a Delta log
    listFromInternal(fs, resolvedPath, useCache = !isInitialVersion(resolvedPath))
      .take(1)
      .exists(_.getPath.getName == resolvedPath.getName)
  }
}

private object S3SingleDriverLogStore {
  /**
   * A global path lock to ensure that no concurrent writers writing to the same path in the same
   * JVM.
   */
  private val pathLock = new ConcurrentHashMap[Path, AnyRef]()

  /**
   * A global cache that records the metadata of the files recently written.
   * As list-after-write may be inconsistent on S3, we can use the files in the cache
   * to fix the inconsistent file listing.
   */
  private val writtenPathCache =
    CacheBuilder.newBuilder()
      .expireAfterAccess(120, TimeUnit.MINUTES)
      .build[Path, FileMetadata]()

  /**
   * Release the lock for the path after writing.
   *
   * Note: the caller should resolve the path to make sure we are locking the correct absolute path.
   */
  private def releasePathLock(resolvedPath: Path): Unit = {
    val lock = pathLock.remove(resolvedPath)
    lock.synchronized {
      lock.notifyAll()
    }
  }

  /**
   * Acquire a lock for the path before writing.
   *
   * Note: the caller should resolve the path to make sure we are locking the correct absolute path.
   */
  private def acquirePathLock(resolvedPath: Path): Unit = {
    while (true) {
      val lock = pathLock.putIfAbsent(resolvedPath, new Object)
      if (lock == null) return
      lock.synchronized {
        while (pathLock.get(resolvedPath) == lock) {
          lock.wait()
        }
      }
    }
  }
}

/**
 * The file metadata to be stored in the cache.
 */
private case class FileMetadata(length: Long, modificationTime: Long)
