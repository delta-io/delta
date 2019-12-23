/*
 * Copyright 2019 Databricks, Inc.
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

package org.apache.spark.sql.delta.storage

import java.io.FileNotFoundException
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.SparkConf

class LocalLockingFileSystemLogStoreImpl(
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends HDFSLogStoreImpl(sparkConf, hadoopConf) {
  import LocalLockingFileSystemLogStoreImpl._

  private def resolved(path: Path): (FileSystem, Path, Path) = {
    val fs = path.getFileSystem(getActiveHadoopConf)
    val resolvedPath = stripUserInfo(fs.makeQualified(path))
    val lockedPath = stripUserInfo(resolvedPath)
    (fs, resolvedPath, lockedPath)
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

  override def iteratorFrom(path: Path): Iterator[FileStatus] = {
    val (fs, resolvedPath, _) = resolved(path)
    iteratorFromInternal(fs, resolvedPath)
  }

  /**
   * Returns `FileStatus`s starting from `resolvedPath` (inclusive).
   */
  private def iteratorFromInternal(fs: FileSystem, resolvedPath: Path): Iterator[FileStatus] = {
    if (!fs.exists(resolvedPath.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${resolvedPath.getParent}")
    }
    val files = fs.listStatus(resolvedPath.getParent)
    files.filter(_.getPath.getName >= resolvedPath.getName).sortBy(_.getPath.getName).iterator
  }

  private def exists(fs: FileSystem, resolvedPath: Path): Boolean = {
    iteratorFromInternal(fs, resolvedPath)
      .take(1)
      .exists(_.getPath.getName == resolvedPath.getName)
  }

  override def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val (fs, resolvedPath, lockedPath) = resolved(path)
    var stream: FSDataOutputStream = null
    acquirePathLock(lockedPath)
    try {
      if (exists(fs, resolvedPath) && !overwrite) {
        throw new FileAlreadyExistsException(resolvedPath.toUri.toString)
      }
      stream = fs.create(resolvedPath, overwrite)
      actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
      stream.close()
    } catch {
      case e: Throwable =>
        // Convert Hadoop's FileAlreadyExistsException to Java's FileAlreadyExistsException
        if (e.isInstanceOf[org.apache.hadoop.fs.FileAlreadyExistsException]) {
          throw new FileAlreadyExistsException(e.getMessage)
        } else {
          throw e
        }
    } finally {
      releasePathLock(lockedPath)
    }
  }
}

object LocalLockingFileSystemLogStoreImpl {
  /**
   * A global path lock to ensure that no concurrent writers writing to the same path in the same
   * JVM.
   */
  private val pathLock = new ConcurrentHashMap[Path, AnyRef]()

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
