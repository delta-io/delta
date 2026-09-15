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

import java.io.File
import java.net.URI
import java.nio.file.{AccessDeniedException, Files}

import scala.collection.JavaConverters._

import io.delta.storage._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}
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
    "wasbs" -> classOf[AzureLogStore].getName).foreach { case (scheme, logStoreClass) =>
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
      LogStoreProvider.getLogStore(hadoopConf, "fake"))
    assert(e.getMessage.contains(
      "Can not instantiate `LogStore` class (from config): %s".format("java.lang.String")))
  }

  // listFrom must not HEAD the _delta_log directory: a credential scoped to LIST but not HEAD
  // would otherwise fail a listable log.
  Seq(
    ("s3", classOf[S3SingleDriverLogStore]),
    ("headdenyhdfs", classOf[HDFSLogStore])).foreach { case (scheme, storeClass) =>
    test(s"listFrom does not HEAD the log dir ($scheme -> ${storeClass.getSimpleName})") {
      val tableDir = Files.createTempDirectory("logstore-head-probe").toFile
      try {
        val deltaLog = new File(tableDir, "_delta_log")
        assert(deltaLog.mkdir())
        val versions = Seq("00000000000000000000.json", "00000000000000000001.json")
        versions.foreach(n => assert(new File(deltaLog, n).createNewFile()))

        val conf = new Configuration()
        conf.set(s"fs.$scheme.impl", classOf[HeadDenyingLocalFileSystem].getName)
        conf.setBoolean(s"fs.$scheme.impl.disable.cache", true)
        // The HeadDenyingLocalFileSystem reports a fixed scheme, so pin the FS for every tested
        // scheme to it and bind the scheme to the LogStore under test.
        conf.set("fs.headdeny.scheme", scheme)
        conf.set(LogStoreProvider.getLogStoreSchemeConfKey(scheme), storeClass.getName)

        val store = LogStoreProvider.getLogStore(conf, scheme)
        assert(store.getClass.getName === storeClass.getName)

        // path inside _delta_log; its parent (the bare _delta_log dir) is HEAD-denied
        val listingPrefix = new Path(s"$scheme://${deltaLog.getAbsolutePath}/${versions.head}")
        val listed = store.listFrom(listingPrefix, conf).asScala.map(_.getPath.getName).toList
        assert(listed === versions.toList)
      } finally {
        deleteRecursively(tableDir)
      }
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) Option(file.listFiles).foreach(_.foreach(deleteRecursively))
    file.delete()
  }
}

/**
 * A [[RawLocalFileSystem]] that denies a HEAD on the bare `_delta_log` directory with a
 * non-[[java.io.FileNotFoundException]] error, mimicking an S3 403 from a credential scoped to
 * allow LIST but not HEAD on the directory key. Both `exists` (which the LogStore probe calls)
 * and `getFileStatus` are denied for the directory; `listStatus` (and HEADs on the version files
 * inside the directory) are still allowed. So a correct `listFrom` that relies on `listStatus`
 * succeeds, while one that probes the directory with `exists` fails.
 *
 * Note `RawLocalFileSystem` overrides `exists` to use the local file directly rather than
 * `getFileStatus`, so the `exists` override below is what actually trips the probe.
 *
 * Reports a configurable scheme (default `s3`, via `fs.headdeny.scheme`) so it can stand in for
 * different LogStore backends.
 */
class HeadDenyingLocalFileSystem extends RawLocalFileSystem {
  private var uri: URI = _
  private var scheme: String = "s3"

  override def initialize(name: URI, conf: Configuration): Unit = {
    scheme = conf.get("fs.headdeny.scheme", "s3")
    uri = URI.create(scheme + ":///")
    super.initialize(name, conf)
  }

  override def getScheme: String = scheme

  override def getUri: URI = if (uri == null) super.getUri else uri

  private def denyIfLogDir(f: Path): Unit = {
    if (f.getName == "_delta_log") {
      throw new AccessDeniedException(s"$f: simulated 403 Forbidden on HEAD")
    }
  }

  override def exists(f: Path): Boolean = {
    denyIfLogDir(f)
    super.exists(f)
  }

  override def getFileStatus(f: Path): FileStatus = {
    denyIfLogDir(f)
    super.getFileStatus(f)
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
