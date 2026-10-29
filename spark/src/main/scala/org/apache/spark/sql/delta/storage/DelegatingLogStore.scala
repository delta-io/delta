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

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils


/**
 * A delegating LogStore used to dynamically resolve LogStore implementation based
 * on the scheme of paths.
 */
class DelegatingLogStore(hadoopConf: Configuration)
  extends LogStore with DeltaLogging {

  private val sparkConf = SparkEnv.get.conf

  // Cache instances, not the capability decision: the same scheme can resolve to filesystems
  // with different capabilities. Access is synchronized because DeltaLog instances are shared.
  private val logStoreMap = mutable.Map.empty[(String, String), LogStore]

  private lazy val defaultLogStore = createLogStore(DelegatingLogStore.defaultHDFSLogStoreClassName)

  // Creates a LogStore with given LogStore class name.
  private def createLogStore(className: String): LogStore = {
    LogStore.createLogStoreWithClassName(className, sparkConf, hadoopConf)
  }

  private def getHadoopConfiguration: Configuration = {
    // Match LogStoreAdaptor's configuration-less operations, including active-session overrides.
    // scalastyle:off deltahadoopconfiguration
    SparkSession.getActiveSession.map(_.sessionState.newHadoopConf()).getOrElse(hadoopConf)
    // scalastyle:on deltahadoopconfiguration
  }

  private def supportsNativeS3(path: Path, conf: Configuration): Boolean = {
    val fs = path.getFileSystem(conf)
    Utils.tryWithSafeFinally {
      io.delta.storage.S3LogStore.isSupported(fs, path)
    } {
      // A cache-disabled lookup creates a client owned by this probe. The selected LogStore
      // resolves its own filesystem, so do not leak this client or close Hadoop's shared client.
      if (conf.getBoolean(s"fs.${path.toUri.getScheme}.impl.disable.cache", false)) {
        fs.close()
      }
    }
  }

  def getDelegate(path: Path): LogStore = getDelegate(path, getHadoopConfiguration)

  def getDelegate(path: Path, conf: Configuration): LogStore = {
    Option(path.toUri.getScheme) match {
      case Some(origScheme) =>
        val scheme = origScheme.toLowerCase(Locale.ROOT)
        val configuredClass = LogStore.getLogStoreConfValue(
          LogStore.logStoreSchemeConfKey(scheme), sparkConf)
        // An explicit override wins without resolving or probing the filesystem.
        val className = configuredClass.getOrElse {
          if (DelegatingLogStore.s3Schemes.contains(scheme) && supportsNativeS3(path, conf)) {
            DelegatingLogStore.nativeS3LogStoreClassName
          } else {
            DelegatingLogStore.getDefaultLogStoreClassName(scheme)
              .getOrElse(DelegatingLogStore.defaultHDFSLogStoreClassName)
          }
        }
        this.synchronized {
          logStoreMap.getOrElseUpdate((scheme, className), {
            val logStore = createLogStore(className)
            if (configuredClass.isEmpty && DelegatingLogStore.s3Schemes.contains(scheme) &&
                className == DelegatingLogStore.defaultS3LogStoreClassName) {
              logWarning(log"Native S3 conditional writes are unavailable for scheme " +
                log"${MDC(DeltaLogKeys.FILE_SYSTEM_SCHEME, scheme)}; using " +
                log"S3SingleDriverLogStore, which cannot coordinate concurrent drivers. " +
                log"Use a supported Hadoop S3A filesystem with conditional creation enabled.")
            }
            val actualLogStoreClassName = logStore match {
              case lsa: LogStoreAdaptor => s"LogStoreAdapter(${lsa.logStoreImpl.getClass.getName})"
              case _ => logStore.getClass.getName
            }
            logInfo(log"LogStore ${MDC(DeltaLogKeys.SYSTEM_CLASS_NAME, actualLogStoreClassName)} " +
              log"is used for scheme ${MDC(DeltaLogKeys.FILE_SYSTEM_SCHEME, scheme)}")
            logStore
          })
        }
      case _ => defaultLogStore
    }
  }

  //////////////////////////
  // Public API Overrides //
  //////////////////////////

  override def read(path: Path): Seq[String] = {
    read(path, getHadoopConfiguration)
  }

  override def read(path: Path, hadoopConf: Configuration): Seq[String] = {
    getDelegate(path, hadoopConf).read(path, hadoopConf)
  }

  override def readAsIterator(path: Path): ClosableIterator[String] = {
    readAsIterator(path, getHadoopConfiguration)
  }

  override def readAsIterator(path: Path, hadoopConf: Configuration): ClosableIterator[String] = {
    getDelegate(path, hadoopConf).readAsIterator(path, hadoopConf)
  }

  override def write(
      path: Path,
      actions: Iterator[String],
      overwrite: Boolean): Unit = {
    write(path, actions, overwrite, getHadoopConfiguration)
  }

  override def write(
      path: Path,
      actions: Iterator[String],
      overwrite: Boolean,
      hadoopConf: Configuration): Unit = {
    getDelegate(path, hadoopConf).write(path, actions, overwrite, hadoopConf)
  }

  override def listFrom(path: Path): Iterator[FileStatus] = {
    listFrom(path, getHadoopConfiguration)
  }

  override def listFrom(path: Path, hadoopConf: Configuration): Iterator[FileStatus] = {
    getDelegate(path, hadoopConf).listFrom(path, hadoopConf)
  }

  override def invalidateCache(): Unit = {
    this.synchronized {
      logStoreMap.foreach { entry =>
        entry._2.invalidateCache()
      }
    }
    defaultLogStore.invalidateCache()
  }

  override def resolvePathOnPhysicalStorage(path: Path): Path = {
    resolvePathOnPhysicalStorage(path, getHadoopConfiguration)
  }

  override def resolvePathOnPhysicalStorage(path: Path, hadoopConf: Configuration): Path = {
    getDelegate(path, hadoopConf).resolvePathOnPhysicalStorage(path, hadoopConf)
  }

  override def isPartialWriteVisible(path: Path): Boolean = {
    isPartialWriteVisible(path, getHadoopConfiguration)
  }

  override def isPartialWriteVisible(path: Path, hadoopConf: Configuration): Boolean = {
    getDelegate(path, hadoopConf).isPartialWriteVisible(path, hadoopConf)
  }
}

object DelegatingLogStore {

  try {
    // load any arbitrary delta-storage class to ensure the dependency has been included
    classOf[io.delta.storage.LogStore]
  } catch {
    case e: NoClassDefFoundError =>
      throw DeltaErrors.missingDeltaStorageJar(e)
  }

  /**
   * Java LogStore (io.delta.storage) implementations are now the default.
   */
  val nativeS3LogStoreClassName = classOf[io.delta.storage.S3LogStore].getName
  // Conservative fallback for S3 filesystems without native-write prerequisites.
  val defaultS3LogStoreClassName = classOf[io.delta.storage.S3SingleDriverLogStore].getName
  val defaultAzureLogStoreClassName = classOf[io.delta.storage.AzureLogStore].getName
  val defaultHDFSLogStoreClassName = classOf[io.delta.storage.HDFSLogStore].getName
  val defaultGCSLogStoreClassName = classOf[io.delta.storage.GCSLogStore].getName

  // Supported schemes with default.
  val s3Schemes = Set("s3", "s3a", "s3n")
  val azureSchemes = Set("abfs", "abfss", "adl", "wasb", "wasbs")
  val gsSchemes = Set("gs")

  // Returns the scheme-only default. getDelegate selects native S3 when capabilities permit.
  // None if we do not have a default for it.
  def getDefaultLogStoreClassName(scheme: String): Option[String] = {
    if (s3Schemes.contains(scheme)) {
      return Some(defaultS3LogStoreClassName)
    } else if (DelegatingLogStore.azureSchemes(scheme: String)) {
      return Some(defaultAzureLogStoreClassName)
    } else if (DelegatingLogStore.gsSchemes(scheme: String)) {
      return Some(defaultGCSLogStoreClassName)
    }
    None
  }
}
