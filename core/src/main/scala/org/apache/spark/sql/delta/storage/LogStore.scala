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

package org.apache.spark.sql.delta.storage

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

/**
 * General interface for all critical file system operations required to read and write the
 * [[DeltaLog]]. The correctness of the [[DeltaLog]] is predicated on the atomicity and
 * durability guarantees of the implementation of this interface. Specifically,
 *
 * 1. Atomic visibility of files: Any file written through this store must
 *    be made visible atomically. In other words, this should not generate partial files.
 *
 * 2. Mutual exclusion: Only one writer must be able to create (or rename) a file at the final
 *    destination.
 *
 * 3. Consistent listing: Once a file has been written in a directory, all future listings for
 *    that directory must return that file.
 */
trait LogStore {

  /**
   * Load the given file and return a `Seq` of lines. The line break will be removed from each
   * line. This method will load the entire file into the memory. Call `readAsIterator` if possible
   * as its implementation may be more efficient.
   */
  @deprecated("call the method that asks for a Hadoop Configuration object instead")
  final def read(path: String): Seq[String] = read(new Path(path))

  /**
   * Load the given file and return a `Seq` of lines. The line break will be removed from each
   * line. This method will load the entire file into the memory. Call `readAsIterator` if possible
   * as its implementation may be more efficient.
   */
  @deprecated("call the method that asks for a Hadoop Configuration object instead")
  def read(path: Path): Seq[String]

  /**
   * Load the given file and return a `Seq` of lines. The line break will be removed from each
   * line. This method will load the entire file into the memory. Call `readAsIterator` if possible
   * as its implementation may be more efficient.
   *
   * Note: The default implementation ignores the `hadoopConf` parameter to provide the backward
   * compatibility. Subclasses should override this method and use `hadoopConf` properly to support
   * passing Hadoop file system configurations through DataFrame options.
   */
  def read(path: Path, hadoopConf: Configuration): Seq[String] = read(path)

  /**
   * Load the given file and return an iterator of lines. The line break will be removed from each
   * line. The default implementation calls `read` to load the entire file into the memory.
   * An implementation should provide a more efficient approach if possible. For example, the file
   * content can be loaded on demand.
   */
  @deprecated("call the method that asks for a Hadoop Configuration object instead")
  final def readAsIterator(path: String): ClosableIterator[String] = {
    readAsIterator(new Path(path))
  }

  /**
   * Load the given file and return an iterator of lines. The line break will be removed from each
   * line. The default implementation calls `read` to load the entire file into the memory.
   * An implementation should provide a more efficient approach if possible. For example, the file
   * content can be loaded on demand.
   *
   * Note: the returned [[ClosableIterator]] should be closed when it's no longer used to avoid
   * resource leak.
   */
  @deprecated("call the method that asks for a Hadoop Configuration object instead")
  def readAsIterator(path: Path): ClosableIterator[String] = {
    val iter = read(path).iterator
    new ClosableIterator[String] {

      override def hasNext: Boolean = iter.hasNext

      override def next(): String = iter.next()

      override def close(): Unit = {}
    }
  }

  /**
   * Load the given file and return an iterator of lines. The line break will be removed from each
   * line. The default implementation calls `read` to load the entire file into the memory.
   * An implementation should provide a more efficient approach if possible. For example, the file
   * content can be loaded on demand.
   *
   * Note: the returned [[ClosableIterator]] should be closed when it's no longer used to avoid
   * resource leak.
   *
   * Note: The default implementation ignores the `hadoopConf` parameter to provide the backward
   * compatibility. Subclasses should override this method and use `hadoopConf` properly to support
   * passing Hadoop file system configurations through DataFrame options.
   */
  def readAsIterator(path: Path, hadoopConf: Configuration): ClosableIterator[String] = {
    readAsIterator(path)
  }

  /**
   * Write the given `actions` to the given `path` without overwriting any existing file.
   * Implementation must throw [[java.nio.file.FileAlreadyExistsException]] exception if the file
   * already exists. Furthermore, implementation must ensure that the entire file is made
   * visible atomically, that is, it should not generate partial files.
   */
  @deprecated("call the method that asks for a Hadoop Configuration object instead")
  final def write(path: String, actions: Iterator[String]): Unit = write(new Path(path), actions)

  /**
   * Write the given `actions` to the given `path` with or without overwrite as indicated.
   * Implementation must throw [[java.nio.file.FileAlreadyExistsException]] exception if the file
   * already exists and overwrite = false. Furthermore, implementation must ensure that the
   * entire file is made visible atomically, that is, it should not generate partial files.
   */
  @deprecated("call the method that asks for a Hadoop Configuration object instead")
  def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit

  /**
   * Write the given `actions` to the given `path` with or without overwrite as indicated.
   * Implementation must throw [[java.nio.file.FileAlreadyExistsException]] exception if the file
   * already exists and overwrite = false. Furthermore, implementation must ensure that the
   * entire file is made visible atomically, that is, it should not generate partial files.
   *
   * Note: The default implementation ignores the `hadoopConf` parameter to provide the backward
   * compatibility. Subclasses should override this method and use `hadoopConf` properly to support
   * passing Hadoop file system configurations through DataFrame options.
   */
  def write(
      path: Path,
      actions: Iterator[String],
      overwrite: Boolean,
      hadoopConf: Configuration): Unit = {
    write(path, actions, overwrite)
  }

  /**
   * List the paths in the same directory that are lexicographically greater or equal to
   * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
   */
  @deprecated("call the method that asks for a Hadoop Configuration object instead")
  final def listFrom(path: String): Iterator[FileStatus] =
    listFrom(new Path(path))

  /**
   * List the paths in the same directory that are lexicographically greater or equal to
   * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
   */
  @deprecated("call the method that asks for a Hadoop Configuration object instead")
  def listFrom(path: Path): Iterator[FileStatus]

  /**
   * List the paths in the same directory that are lexicographically greater or equal to
   * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
   *
   * Note: The default implementation ignores the `hadoopConf` parameter to provide the backward
   * compatibility. Subclasses should override this method and use `hadoopConf` properly to support
   * passing Hadoop file system configurations through DataFrame options.
   */
  def listFrom(path: Path, hadoopConf: Configuration): Iterator[FileStatus] = listFrom(path)

  /** Invalidate any caching that the implementation may be using */
  def invalidateCache(): Unit

  /** Resolve the fully qualified path for the given `path`. */
  @deprecated("call the method that asks for a Hadoop Configuration object instead")
  def resolvePathOnPhysicalStorage(path: Path): Path = {
    throw new UnsupportedOperationException()
  }

  /**
   * Resolve the fully qualified path for the given `path`.
   *
   * Note: The default implementation ignores the `hadoopConf` parameter to provide the backward
   * compatibility. Subclasses should override this method and use `hadoopConf` properly to support
   * passing Hadoop file system configurations through DataFrame options.
   */
  def resolvePathOnPhysicalStorage(path: Path, hadoopConf: Configuration): Path = {
    resolvePathOnPhysicalStorage(path)
  }

  /**
   * Whether a partial write is visible when writing to `path`.
   *
   * As this depends on the underlying file system implementations, we require the input of `path`
   * here in order to identify the underlying file system, even though in most cases a log store
   * only deals with one file system.
   *
   * The default value is only provided here for legacy reasons, which will be removed.
   * Any LogStore implementation should override this instead of relying on the default.
   */
  @deprecated("call the method that asks for a Hadoop Configuration object instead")
  def isPartialWriteVisible(path: Path): Boolean = true

  /**
   * Whether a partial write is visible when writing to `path`.
   *
   * As this depends on the underlying file system implementations, we require the input of `path`
   * here in order to identify the underlying file system, even though in most cases a log store
   * only deals with one file system.
   *
   * The default value is only provided here for legacy reasons, which will be removed.
   * Any LogStore implementation should override this instead of relying on the default.
   *
   * Note: The default implementation ignores the `hadoopConf` parameter to provide the backward
   * compatibility. Subclasses should override this method and use `hadoopConf` properly to support
   * passing Hadoop file system configurations through DataFrame options.
   */
  def isPartialWriteVisible(path: Path, hadoopConf: Configuration): Boolean = {
    isPartialWriteVisible(path)
  }
}

object LogStore extends LogStoreProvider
  with Logging {

  def apply(sc: SparkContext): LogStore = {
    apply(sc.getConf, sc.hadoopConfiguration)
  }

  def apply(sparkConf: SparkConf, hadoopConf: Configuration): LogStore = {
    createLogStore(sparkConf, hadoopConf)
  }

  // The conf key for setting the LogStore implementation for `scheme`.
  def logStoreSchemeConfKey(scheme: String): String = s"spark.delta.logStore.${scheme}.impl"

  // Creates a LogStore with the given LogStore class name and configurations.
  def createLogStoreWithClassName(
      className: String,
      sparkConf: SparkConf,
      hadoopConf: Configuration): LogStore = {
    if (className == classOf[DelegatingLogStore].getName) {
      new DelegatingLogStore(hadoopConf)
    } else {
      val logStoreClass = Utils.classForName(className)
      if (classOf[io.delta.storage.LogStore].isAssignableFrom(logStoreClass)) {
        new LogStoreAdaptor(logStoreClass.getConstructor(classOf[Configuration])
          .newInstance(hadoopConf))
      } else {
        logStoreClass.getConstructor(classOf[SparkConf], classOf[Configuration])
          .newInstance(sparkConf, hadoopConf).asInstanceOf[LogStore]
      }
    }
  }
}

trait LogStoreProvider {
  val logStoreClassConfKey: String = "spark.delta.logStore.class"
  val defaultLogStoreClass: String = classOf[DelegatingLogStore].getName

  def createLogStore(spark: SparkSession): LogStore = {
    // TODO: return the singleton.
    val sc = spark.sparkContext
    createLogStore(sc.getConf, sc.hadoopConfiguration)
  }

  def checkLogStoreConfConflicts(sparkConf: SparkConf): Unit = {
    val (classConf, otherConf) = sparkConf.getAllWithPrefix("spark.delta.logStore.")
      .partition(v => v._1 == "class")
    val schemeConf = otherConf.filter(_._1.endsWith(".impl"))
    if (classConf.nonEmpty && schemeConf.nonEmpty) {
      throw DeltaErrors.logStoreConfConflicts(schemeConf)
    }
  }

  def createLogStore(sparkConf: SparkConf, hadoopConf: Configuration): LogStore = {
    checkLogStoreConfConflicts(sparkConf)
    val logStoreClassName = sparkConf.get(logStoreClassConfKey, defaultLogStoreClass)
    LogStore.createLogStoreWithClassName(logStoreClassName, sparkConf, hadoopConf)
  }
}

/**
 * An adaptor from the new public LogStore API to the old private LogStore API. The old LogStore
 * API is still used in most places. Before we move all of them to the new API, adapting from
 * the new API to the old API is a cheap way to ensure that implementations of both APIs work.
 *
 * @param logStoreImpl An implementation of the new public LogStore API.
 */
class LogStoreAdaptor(val logStoreImpl: io.delta.storage.LogStore) extends LogStore {

  private def getHadoopConfiguration(): Configuration = {
    // scalastyle:off deltahadoopconfiguration
    SparkSession.getActiveSession.map(_.sessionState.newHadoopConf())
      .getOrElse(logStoreImpl.initHadoopConf())
    // scalastyle:on deltahadoopconfiguration
  }

  override def read(path: Path): Seq[String] = {
    read(path, getHadoopConfiguration)
  }

  override def read(path: Path, hadoopConf: Configuration): Seq[String] = {
    var iter: io.delta.storage.CloseableIterator[String] = null
    try {
      iter = logStoreImpl.read(path, hadoopConf)
      val contents = iter.asScala.toArray
      contents
    } finally {
      if (iter != null) {
        iter.close
      }
    }
  }

  override def readAsIterator(path: Path): ClosableIterator[String] = {
    readAsIterator(path, getHadoopConfiguration)
  }

  override def readAsIterator(path: Path, hadoopConf: Configuration): ClosableIterator[String] = {
    val iter = logStoreImpl.read(path, hadoopConf)
    new ClosableIterator[String] {
      override def close(): Unit = iter.close
      override def hasNext: Boolean = iter.hasNext
      override def next(): String = iter.next
    }
  }

  override def write(path: Path, actions: Iterator[String], overwrite: Boolean): Unit = {
    write(path, actions, overwrite, getHadoopConfiguration)
  }

  override def write(
      path: Path,
      actions: Iterator[String],
      overwrite: Boolean,
      hadoopConf: Configuration): Unit = {
    logStoreImpl.write(path, actions.asJava, overwrite, hadoopConf)
  }

  override def listFrom(path: Path): Iterator[FileStatus] = {
    listFrom(path, getHadoopConfiguration)
  }

  override def listFrom(path: Path, hadoopConf: Configuration): Iterator[FileStatus] = {
    logStoreImpl.listFrom(path, hadoopConf).asScala
  }

  override def invalidateCache(): Unit = {}

  override def resolvePathOnPhysicalStorage(path: Path): Path = {
    resolvePathOnPhysicalStorage(path, getHadoopConfiguration)
  }

  override def resolvePathOnPhysicalStorage(path: Path, hadoopConf: Configuration): Path = {
    logStoreImpl.resolvePathOnPhysicalStorage(path, hadoopConf)
  }

  override def isPartialWriteVisible(path: Path): Boolean = {
    isPartialWriteVisible(path, getHadoopConfiguration)
  }

  override def isPartialWriteVisible(path: Path, hadoopConf: Configuration): Boolean = {
    logStoreImpl.isPartialWriteVisible(path, hadoopConf)
  }
}
