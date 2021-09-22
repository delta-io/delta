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

package io.delta.standalone.internal.storage

import io.delta.standalone.data.{CloseableIterator => ClosebleIteratorJ}
import io.delta.standalone.storage.{LogStore => LogStoreJ}
import io.delta.standalone.internal.sources.StandaloneHadoopConf

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

/**
 * General interface for all critical file system operations required to read and write the
 * [[io.delta.standalone.DeltaLog]]. The correctness of the [[io.delta.standalone.DeltaLog]] is
 * predicated on the atomicity and durability guarantees of the implementation of this interface.
 * Specifically,
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
private[internal] trait LogStore { // TODO: rename and refactor

  /** Read the given `path` */
  def read(path: String): Seq[String] = read(new Path(path))

  /** Read the given `path` */
  def read(path: Path): Seq[String]

  /**
   * List the paths in the same directory that are lexicographically greater or equal to
   * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
   */
  def listFrom(path: String): Iterator[FileStatus] = listFrom(new Path(path))

  /**
   * List the paths in the same directory that are lexicographically greater or equal to
   * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
   */
  def listFrom(path: Path): Iterator[FileStatus]

  /**
   * Write the given `actions` to the given `path` without overwriting any existing file.
   * Implementation must throw [[java.nio.file.FileAlreadyExistsException]] exception if the file
   * already exists. Furthermore, implementation must ensure that the entire file is made
   * visible atomically, that is, it should not generate partial files.
   */
  final def write(path: String, actions: Iterator[String]): Unit = write(new Path(path), actions)

  /**
   * Write the given `actions` to the given `path` with or without overwrite as indicated.
   * Implementation must throw [[java.nio.file.FileAlreadyExistsException]] exception if the file
   * already exists and overwrite = false. Furthermore, implementation must ensure that the
   * entire file is made visible atomically, that is, it should not generate partial files.
   */
  def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit

  /** Resolve the fully qualified path for the given `path`. */
  def resolvePathOnPhysicalStorage(path: Path): Path = {
    throw new UnsupportedOperationException()
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
  def isPartialWriteVisible(path: Path): Boolean = true
}

private[internal] object LogStore extends LogStoreProvider

private[internal] trait LogStoreProvider {

  val defaultLogStoreClassName = classOf[HDFSLogStore].getName

  def createLogStore(hadoopConf: Configuration): LogStore = {
    val logStoreClassName =
      hadoopConf.get(StandaloneHadoopConf.LOG_STORE_CLASS_KEY, defaultLogStoreClassName)

    // scalastyle:off classforname
    val logStoreClass =
      Class.forName(logStoreClassName, true, Thread.currentThread().getContextClassLoader)
    // scalastyle:on classforname

    if (classOf[LogStoreJ].isAssignableFrom(logStoreClass)) {
      val logStoreImpl = logStoreClass.getConstructor(classOf[Configuration])
        .newInstance(hadoopConf).asInstanceOf[LogStoreJ]
      new LogStoreAdaptor(logStoreImpl, hadoopConf)
    } else {
      logStoreClass.getConstructor(classOf[Configuration]).newInstance(hadoopConf)
        .asInstanceOf[LogStore]
    }
  }
}

/**
 * An adapter from external Java instances of [[LogStoreJ]] to internal Scala instances of
 * [[LogStore]].
 */
private[internal] class LogStoreAdaptor(
    logStoreImpl: LogStoreJ,
    hadoopConf: Configuration) extends LogStore {

  override def read(path: Path): Seq[String] = {
    var iter: ClosebleIteratorJ[String] = null
    try {
      iter = logStoreImpl.read(path, hadoopConf)
      val contents = iter.asScala.toArray
      contents
    } finally {
      if (iter != null) {
        iter.close()
      }
    }
  }

  override def write(path: Path, actions: Iterator[String], overwrite: Boolean): Unit = {
    logStoreImpl.write(path, actions.asJava, overwrite, hadoopConf)
  }

  override def listFrom(path: Path): Iterator[FileStatus] = {
    logStoreImpl.listFrom(path, hadoopConf).asScala
  }

  override def resolvePathOnPhysicalStorage(path: Path): Path = {
    logStoreImpl.resolvePathOnPhysicalStorage(path, hadoopConf)
  }

  override def isPartialWriteVisible(path: Path): Boolean = {
    logStoreImpl.isPartialWriteVisible(path, hadoopConf)
  }
}
