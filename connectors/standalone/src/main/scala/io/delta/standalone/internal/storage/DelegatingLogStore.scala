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

import java.util.Locale

import scala.collection.mutable

import io.delta.storage.{CloseableIterator, LogStore}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import io.delta.standalone.internal.logging.Logging

/**
 * A delegating LogStore used to dynamically resolve LogStore implementation based
 * on the scheme of paths.
 */
class DelegatingLogStore(hadoopConf: Configuration)
  extends LogStore(hadoopConf)
  with Logging {

  // Map scheme to the corresponding LogStore resolved and created. Accesses to this map need
  // synchronization This could be accessed by multiple threads because it is shared through
  // shared DeltaLog instances.
  private val schemeToLogStoreMap = mutable.Map.empty[String, LogStore]

  private lazy val defaultLogStore = createLogStore(DelegatingLogStore.defaultHDFSLogStoreClassName)

  // Creates a LogStore with given LogStore class name.
  private def createLogStore(className: String): LogStore = {
    LogStoreProvider.createLogStoreWithClassName(className, hadoopConf)
  }

  // Create LogStore based on the scheme of `path`.
  private def schemeBasedLogStore(path: Path): LogStore = {
    Option(path.toUri.getScheme) match {
      case Some(origScheme) =>
        val scheme = origScheme.toLowerCase(Locale.ROOT)
        this.synchronized {
          if (schemeToLogStoreMap.contains(scheme)) {
            schemeToLogStoreMap(scheme)
          } else {
            // Resolve LogStore class based on the following order:
            // 1. Scheme conf if set.
            // 2. Defaults for scheme if exists.
            // 3. Default.
            val logStoreClassNameOpt = Option(
              hadoopConf.get(LogStoreProvider.logStoreSchemeConfKey(scheme))
            ).orElse(DelegatingLogStore.getDefaultLogStoreClassName(scheme))

            val logStore = logStoreClassNameOpt.map(createLogStore(_)).getOrElse(defaultLogStore)
            schemeToLogStoreMap += scheme -> logStore
            logInfo(s"LogStore ${logStore.getClass.getName} is used for scheme ${scheme}")
            logStore
          }
        }
      case _ => defaultLogStore
    }
  }

  def getDelegate(path: Path): LogStore = schemeBasedLogStore(path)

  //////////////////////////
  // Public API Overrides //
  //////////////////////////

  override def read(path: Path, hadoopConf: Configuration): CloseableIterator[String] = {
    getDelegate(path).read(path, hadoopConf)
  }

  override def write(
      path: Path,
      actions: java.util.Iterator[String],
      overwrite: java.lang.Boolean,
      hadoopConf: Configuration): Unit = {
    getDelegate(path).write(path, actions, overwrite, hadoopConf)
  }

  override def listFrom(path: Path, hadoopConf: Configuration): java.util.Iterator[FileStatus] = {
    getDelegate(path).listFrom(path, hadoopConf)
  }

  override def resolvePathOnPhysicalStorage(path: Path, hadoopConf: Configuration): Path = {
    getDelegate(path).resolvePathOnPhysicalStorage(path, hadoopConf)
  }

  override def isPartialWriteVisible(path: Path, hadoopConf: Configuration): java.lang.Boolean = {
    getDelegate(path).isPartialWriteVisible(path, hadoopConf)
  }
}

object DelegatingLogStore {

  /**
   * Java LogStore (io.delta.storage) implementations are now the default.
   */
  val defaultS3LogStoreClassName = classOf[io.delta.storage.S3SingleDriverLogStore].getName
  val defaultAzureLogStoreClassName = classOf[io.delta.storage.AzureLogStore].getName
  val defaultHDFSLogStoreClassName = classOf[io.delta.storage.HDFSLogStore].getName
  val defaultGCSLogStoreClassName = classOf[io.delta.storage.GCSLogStore].getName

  // Supported schemes with default.
  val s3Schemes = Set("s3", "s3a", "s3n")
  val azureSchemes = Set("abfs", "abfss", "adl", "wasb", "wasbs")
  val gsSchemes = Set("gs")

  // Returns the default LogStore class name for `scheme`.
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

