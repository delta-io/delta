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

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.SparkEnv


/**
 * A delegating LogStore used to dynamically resolve LogStore implementation based
 * on the scheme of paths.
 */
class DelegatingLogStore(hadoopConf: Configuration)
  extends LogStore with DeltaLogging {

  private val sparkConf = SparkEnv.get.conf

  // Map scheme to the corresponding LogStore resolved and created. Accesses to this map need
  // synchronization This could be accessed by multiple threads because it is shared through
  // shared DeltaLog instances.
  private val schemeToLogStoreMap = mutable.Map.empty[String, LogStore]

  private lazy val defaultLogStore = createLogStore(DelegatingLogStore.defaultHDFSLogStoreClassName)

  // Creates a LogStore with given LogStore class name.
  private def createLogStore(className: String): LogStore = {
    LogStore.createLogStoreWithClassName(className, sparkConf, hadoopConf)
  }

  // Create LogStore based on the scheme of `path`.
  private def schemeBasedLogStore(path: Path): LogStore = {
    val store = Option(path.toUri.getScheme) match {
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
            val logStoreClassNameOpt = sparkConf.getOption(LogStore.logStoreSchemeConfKey(scheme))
              .orElse(DelegatingLogStore.getDefaultLogStoreClassName(scheme))
            val logStore = logStoreClassNameOpt.map(createLogStore(_)).getOrElse(defaultLogStore)
            schemeToLogStoreMap += scheme -> logStore
            logInfo(s"LogStore ${logStore.getClass.getName} is used for scheme ${scheme}")
            logStore
          }
        }
      case _ => defaultLogStore
    }
    store
  }

  def getDelegate(path: Path): LogStore = schemeBasedLogStore(path)

  //////////////////////////
  // Public API Overrides //
  //////////////////////////

  override def read(path: Path): Seq[String] = {
    getDelegate(path).read(path)
  }

  override def readAsIterator(path: Path): ClosableIterator[String] = {
    getDelegate(path).readAsIterator(path)
  }

  override def write(path: Path, actions: Iterator[String], overwrite: Boolean): Unit = {
    getDelegate(path).write(path, actions, overwrite)
  }

  override def listFrom(path: Path): Iterator[FileStatus] = {
    getDelegate(path).listFrom(path)
  }

  override def invalidateCache(): Unit = {
    this.synchronized {
      schemeToLogStoreMap.foreach { entry =>
        entry._2.invalidateCache
      }
    }
    defaultLogStore.invalidateCache
  }

  override def resolvePathOnPhysicalStorage(path: Path): Path = {
    getDelegate(path).resolvePathOnPhysicalStorage(path)
  }

  override def isPartialWriteVisible(path: Path): Boolean = {
    getDelegate(path).isPartialWriteVisible(path)
  }
}

object DelegatingLogStore {

  val defaultS3LogStoreClassName = classOf[S3SingleDriverLogStore].getName
  val defaultAzureLogStoreClassName = classOf[AzureLogStore].getName
  val defaultHDFSLogStoreClassName = classOf[HDFSLogStore].getName

  // Supported schemes with default.
  val s3Schemes = Set("s3", "s3a", "s3n")
  val azureSchemes = Set("abfs", "abfss", "adl", "wasb", "wasbs")

  // Returns the default LogStore class name for `scheme`.
  // None if we do not have a default for it.
  def getDefaultLogStoreClassName(scheme: String): Option[String] = {
    if (s3Schemes.contains(scheme)) {
      return Some(defaultS3LogStoreClassName)
    } else if (DelegatingLogStore.azureSchemes(scheme: String)) {
      return Some(defaultAzureLogStoreClassName)
    }
    None
  }
}
