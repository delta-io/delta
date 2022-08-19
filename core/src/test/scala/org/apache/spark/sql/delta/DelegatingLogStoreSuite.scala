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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.storage.{DelegatingLogStore, LogStore, LogStoreAdaptor}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.LocalSparkSession._
import org.apache.spark.sql.SparkSession

class DelegatingLogStoreSuite
  extends SparkFunSuite {


  private val customLogStoreClassName = classOf[CustomPublicLogStore].getName
  private def fakeSchemeWithNoDefault = "fake"

  private def constructSparkConf(confs: Seq[(String, String)]): SparkConf = {
    val sparkConf = new SparkConf(loadDefaults = false).setMaster("local")
    confs.foreach { case (key, value) => sparkConf.set(key, value) }
    sparkConf
  }

  /**
   * Test DelegatingLogStore by directly creating a DelegatingLogStore and test LogStore
   * resolution based on input `scheme`. This is not an end-to-end test.
   *
   * @param scheme The scheme to be used for testing.
   * @param sparkConf The spark configuration to use.
   * @param expClassName Expected LogStore class name resolved by DelegatingLogStore.
   * @param expAdaptor True if DelegatingLogStore is expected to resolve to LogStore adaptor, for
   *                   which the actual implementation inside will be checked. This happens when
   *                   LogStore is set to subclass of the new [[io.delta.storage.LogStore]] API.
   */
  private def testDelegatingLogStore(
      scheme: String,
      sparkConf: SparkConf,
      expClassName: String,
      expAdaptor: Boolean): Unit = {
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      val sc = spark.sparkContext
      val delegatingLogStore = new DelegatingLogStore(sc.hadoopConfiguration)
      val actualLogStore = delegatingLogStore.getDelegate(
        new Path(s"${scheme}://dummy"))
      if (expAdaptor) {
        assert(actualLogStore.isInstanceOf[LogStoreAdaptor])
        assert(actualLogStore.asInstanceOf[LogStoreAdaptor]
          .logStoreImpl.getClass.getName == expClassName)
      } else {
        assert(actualLogStore.getClass.getName == expClassName)
      }
    }
  }

  /** Test the default LogStore resolution for `scheme` */
  private def testDefaultSchemeResolution(scheme: String, expClassName: String): Unit = {
    testDelegatingLogStore(
      scheme,
      constructSparkConf(Seq.empty), // we set no custom LogStore confs
      expClassName,
      expAdaptor = true // all default implementations are from delta-storage
    )
  }

  /** Test LogStore resolution with a customized scheme conf */
  private def testCustomSchemeResolution(
      scheme: String, className: String, expAdaptor: Boolean): Unit = {
    val sparkPrefixKey = LogStore.logStoreSchemeConfKey(scheme)
    val nonSparkPrefixKey = sparkPrefixKey.stripPrefix("spark.")
    // only set spark-prefixed key
    testDelegatingLogStore(
      scheme,
      constructSparkConf(Seq((sparkPrefixKey, className))),
      className, // we expect our custom-set LogStore class
      expAdaptor
    )
    // only set non-spark-prefixed key
    testDelegatingLogStore(
      scheme,
      constructSparkConf(Seq((nonSparkPrefixKey, className))),
      className, // we expect our custom-set LogStore class
      expAdaptor
    )
    // set both
    testDelegatingLogStore(
      scheme,
      constructSparkConf(Seq((nonSparkPrefixKey, className), (sparkPrefixKey, className))),
      className, // we expect our custom-set LogStore class
      expAdaptor
    )
  }

  test("DelegatingLogStore resolution using default scheme confs") {
    for (scheme <- DelegatingLogStore.s3Schemes) {
      testDefaultSchemeResolution(
        scheme,
        expClassName = DelegatingLogStore.defaultS3LogStoreClassName)
    }
    for (scheme <- DelegatingLogStore.azureSchemes) {
      testDefaultSchemeResolution(
        scheme,
        expClassName = DelegatingLogStore.defaultAzureLogStoreClassName)
    }
    for (scheme <- DelegatingLogStore.gsSchemes) {
      testDefaultSchemeResolution(
        scheme,
        expClassName = DelegatingLogStore.defaultGCSLogStoreClassName)
    }
    testDefaultSchemeResolution(
      scheme = fakeSchemeWithNoDefault,
      expClassName = DelegatingLogStore.defaultHDFSLogStoreClassName)
  }

  test("DelegatingLogStore resolution using customized scheme confs") {
    val allTestSchemes = DelegatingLogStore.s3Schemes ++ DelegatingLogStore.azureSchemes +
      fakeSchemeWithNoDefault
    for (scheme <- allTestSchemes) {
      for (store <- Seq(
        // default (java) classes (in io.delta.storage)
        "io.delta.storage.S3SingleDriverLogStore",
        "io.delta.storage.AzureLogStore",
        "io.delta.storage.HDFSLogStore",
        // deprecated (scala) classes
        classOf[org.apache.spark.sql.delta.storage.S3SingleDriverLogStore].getName,
        classOf[org.apache.spark.sql.delta.storage.AzureLogStore].getName,
        classOf[org.apache.spark.sql.delta.storage.HDFSLogStore].getName,
        customLogStoreClassName)) {

        // we set spark.delta.logStore.${scheme}.impl -> $store
        testCustomSchemeResolution(
          scheme,
          store,
          expAdaptor = store.contains("io.delta.storage") || store == customLogStoreClassName)
      }
    }
  }
}

//////////////////
// Helper Class //
//////////////////

class CustomPublicLogStore(initHadoopConf: Configuration)
  extends io.delta.storage.LogStore(initHadoopConf) {

  private val logStoreInternal = new io.delta.storage.HDFSLogStore(initHadoopConf)

  override def read(
      path: Path,
      hadoopConf: Configuration): io.delta.storage.CloseableIterator[String] = {
    logStoreInternal.read(path, hadoopConf)
  }

  override def write(
      path: Path,
      actions: java.util.Iterator[String],
      overwrite: java.lang.Boolean,
      hadoopConf: Configuration): Unit = {
    logStoreInternal.write(path, actions, overwrite, hadoopConf)
  }

  override def listFrom(
      path: Path,
      hadoopConf: Configuration): java.util.Iterator[FileStatus] = {
    logStoreInternal.listFrom(path, hadoopConf)
  }

  override def resolvePathOnPhysicalStorage(
      path: Path,
      hadoopConf: Configuration): Path = {
    logStoreInternal.resolvePathOnPhysicalStorage(path, hadoopConf)
  }

  override def isPartialWriteVisible(path: Path, hadoopConf: Configuration): java.lang.Boolean = {
    logStoreInternal.isPartialWriteVisible(path, hadoopConf)
  }
}
