/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.logstore

import io.delta.storage._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalatest.funsuite.AnyFunSuite

class LogStoreProviderSuite extends AnyFunSuite {

  private val customLogStoreClassName = classOf[UserDefinedLogStore].getName

  val hadoopConf = new Configuration()
  Seq(
    "s3" -> classOf[S3SingleDriverLogStore].getName,
    "s3a" -> classOf[S3SingleDriverLogStore].getName,
    "s3n" -> classOf[S3SingleDriverLogStore].getName,
    "hdfs" -> classOf[HDFSLogStore].getName,
    "file" -> classOf[HDFSLogStore].getName,
    "gs" -> classOf[GCSLogStore].getName,
    "abfss" -> classOf[AzureLogStore].getName,
    "abfs" -> classOf[AzureLogStore].getName,
    "adl" -> classOf[AzureLogStore].getName,
    "wasb" -> classOf[AzureLogStore].getName,
    "wasbs" -> classOf[AzureLogStore].getName
  ).foreach { case (scheme, logStoreClass) =>
    test(s"get the default LogStore for scheme $scheme") {
      val logStore = LogStoreProvider.getLogStore(hadoopConf, scheme)
      assert(logStore.getClass.getName === logStoreClass)
    }
  }

  test("override the default LogStore for a schema") {
    val hadoopConf = new Configuration()
    hadoopConf.set(LogStoreProvider.getLogStoreSchemeConfKey("s3"), customLogStoreClassName)
    val logStore = LogStoreProvider.getLogStore(hadoopConf, "s3")
    assert(logStore.getClass.getName === customLogStoreClassName)
  }

  test("set LogStore config for a custom scheme") {
    val hadoopConf = new Configuration()
    hadoopConf.set(LogStoreProvider.getLogStoreSchemeConfKey("fake"), customLogStoreClassName)
    val logStore = LogStoreProvider.getLogStore(hadoopConf, "fake")
    assert(logStore.getClass.getName === customLogStoreClassName)
  }

  test("set LogStore config to a class that doesn't extend LogStore") {
    val hadoopConf = new Configuration()
    hadoopConf.set(LogStoreProvider.getLogStoreSchemeConfKey("fake"), "java.lang.String")
    val e = intercept[IllegalArgumentException](
      LogStoreProvider.getLogStore(hadoopConf, "fake")
    )
    assert(e.getMessage.contains(
      "Can not instantiate `LogStore` class (from config): %s".format("java.lang.String")))
  }
}

/**
 * Sample user-defined log store implementing [[LogStore]].
 */
class UserDefinedLogStore(override val initHadoopConf: Configuration)
    extends LogStore(initHadoopConf) {

  private val logStoreInternal = new HDFSLogStore(initHadoopConf)

  override def read(path: Path, hadoopConf: Configuration): CloseableIterator[String] = {
    logStoreInternal.read(path, hadoopConf)
  }

  override def write(
      path: Path,
      actions: java.util.Iterator[String],
      overwrite: java.lang.Boolean,
      hadoopConf: Configuration): Unit = {
    logStoreInternal.write(path, actions, overwrite, hadoopConf)
  }

  override def listFrom(path: Path, hadoopConf: Configuration): java.util.Iterator[FileStatus] = {
    logStoreInternal.listFrom(path, hadoopConf)
  }

  override def resolvePathOnPhysicalStorage(path: Path, hadoopConf: Configuration): Path = {
    logStoreInternal.resolvePathOnPhysicalStorage(path, hadoopConf)
  }

  override def isPartialWriteVisible(path: Path, hadoopConf: Configuration): java.lang.Boolean = {
    false
  }
}
