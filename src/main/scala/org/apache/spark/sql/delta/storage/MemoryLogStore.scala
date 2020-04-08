/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.google.common.cache.CacheBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf

import collection.JavaConverters._
import scala.util.control.Breaks._


class MemoryLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
  extends BaseExternalLogStore(sparkConf, hadoopConf) {

  import MemoryLogStore._

  private def releaseLock(logEntryMetadata: LogEntryMetadata) {
    val unlock = pathLock.remove(logEntryMetadata.path)
    unlock.synchronized {
      unlock.notifyAll()
    }
  }

  override protected def cleanCache(p: LogEntryMetadata => Boolean) {
    val keys = writtenPathCache
      .asMap()
      .asScala
      .filter { case (_, entry) => p(entry) }
      .keys
      .asJava

    writtenPathCache.invalidateAll(keys)
  }

  override def invalidateCache(): Unit = {
    writtenPathCache.invalidateAll()
  }

  override protected def listFromCache(
    fs: FileSystem,
    resolvedPath: Path): Iterator[LogEntryMetadata] = {
    val pathKey = getPathKey(resolvedPath)
    writtenPathCache
      .asMap()
      .asScala
      .iterator
      .filter { case (path, _) =>
        path.getParent == pathKey.getParent() && path.getName >= pathKey.getName
      }
      .map {
        case (_, logEntry) => logEntry
      }
  }

  override protected def writeCache(
    fs: FileSystem,
    logEntry: LogEntryMetadata,
    overwrite: Boolean = false): Unit = {

    logDebug(s"WriteExternalCache: ${logEntry.path} (overwrite=$overwrite)")

    if (!overwrite) {
      writeCacheExclusive(fs, logEntry)
      return
    }
    writtenPathCache.put(logEntry.path, logEntry)
  }

  protected def writeCacheExclusive(
    fs: FileSystem,
    logEntry: LogEntryMetadata
  ): Unit = {
    breakable {
      while (true) {
        val lock = pathLock.putIfAbsent(logEntry.path, new Object)
        if (lock == null) break
        lock.synchronized {
          while (pathLock.get(logEntry.path) == lock) {
            lock.wait()
          }
        }
      }
    }

    if (exists(fs, logEntry.path)) {
      releaseLock(logEntry)
      throw new java.nio.file.FileAlreadyExistsException(
        s"TransactionLog exists ${logEntry.path}"
      )
    }

    writtenPathCache.put(logEntry.path, logEntry)
    releaseLock(logEntry)
  }

}

object MemoryLogStore {
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
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(120, TimeUnit.MINUTES)
      .build[Path, LogEntryMetadata]()
}
