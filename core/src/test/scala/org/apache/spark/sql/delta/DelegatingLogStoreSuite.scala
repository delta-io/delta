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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.LocalSparkSession._
import org.apache.spark.sql.SparkSession

class DelegatingLogStoreSuite
  extends SparkFunSuite {


  private val customLogStoreClassName = classOf[CustomPublicLogStore].getName

  private def fakeSchemeWithNoDefault = "fake"

  /**
   * Constructs SparkConf based on options.
   *
   * @param scheme The scheme whose corresponding conf will be set or unset.
   */
  private def constructSparkConf(
      scheme: String,
      classConf: Option[String],
      schemeConf: Option[String]): SparkConf = {
    val sparkConf = new SparkConf().setMaster("local")
    val classConfKey = LogStore.logStoreClassConfKey
    val schemeConfKey = LogStore.logStoreSchemeConfKey(scheme)

    // this will set/unset spark.delta.logStore.class -> $classConf
    classConf match {
      case Some(conf) => sparkConf.set(classConfKey, conf)
      case _ => sparkConf.remove(classConfKey)
    }

    // this will set/unset spark.delta.logStore.${scheme}.impl -> $schemeConf
    schemeConf match {
      case Some(conf) => sparkConf.set(schemeConfKey, conf)
      case _ => sparkConf.remove(schemeConfKey)
    }

    sparkConf
  }

  /**
   * Test DelegatingLogStore by directly creating a DelegatingLogStore and test LogStore
   * resolution based on input `scheme`. This is not an end-to-end test.
   *
   * @param scheme The scheme to be used for testing.
   * @param schemeConf The scheme conf value to be set. If None, scheme conf will be unset.
   * @param expClassName Expected LogStore class name resolved by DelegatingLogStore.
   * @param expAdaptor True if DelegatingLogStore is expected to resolve to LogStore adaptor, for
   *                   which the actual implementation inside will be checked. This happens when
   *                   LogStore is set to subclass of the new [[io.delta.storage.LogStore]] API.
   */
  private def testDelegatingLogStore(
      scheme: String,
      schemeConf: Option[String],
      expClassName: String,
      expAdaptor: Boolean): Unit = {
    val sparkConf = constructSparkConf(scheme, None, schemeConf)
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

  /**
   * Test with class conf set and scheme conf unset using `scheme`.
   */
  private def testLogStoreClassConfNoSchemeConf(scheme: String) {
    val sparkConf = constructSparkConf(scheme, Some(customLogStoreClassName), None)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      assert(LogStore(spark).isInstanceOf[LogStoreAdaptor])
      assert(LogStore(spark).asInstanceOf[LogStoreAdaptor]
        .logStoreImpl.getClass.getName == customLogStoreClassName)
    }
  }

  test("DelegatingLogStore resolution using default scheme confs") {
    for (scheme <- DelegatingLogStore.s3Schemes) {
      testDelegatingLogStore(
        scheme,
        schemeConf = None,
        expClassName = DelegatingLogStore.defaultS3LogStoreClassName,
        expAdaptor = true)
    }
    for (scheme <- DelegatingLogStore.azureSchemes) {
      testDelegatingLogStore(
        scheme,
        schemeConf = None,
        expClassName = DelegatingLogStore.defaultAzureLogStoreClassName,
        expAdaptor = true)
    }
    for (scheme <- DelegatingLogStore.gsSchemes) {
      testDelegatingLogStore(
        scheme,
        schemeConf = None,
        expClassName = DelegatingLogStore.defaultGCSLogStoreClassName,
        expAdaptor = true)
    }
    testDelegatingLogStore(
      scheme = fakeSchemeWithNoDefault,
      schemeConf = None,
      expClassName = DelegatingLogStore.defaultHDFSLogStoreClassName,
      expAdaptor = true)
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
        testDelegatingLogStore(
          scheme,
          schemeConf = Some(store),
          expClassName = store,
          expAdaptor = store.contains("io.delta.storage") || store == customLogStoreClassName)
      }
    }
  }

  test("class-conf = set, scheme has no default, scheme-conf = not set") {
    testLogStoreClassConfNoSchemeConf(fakeSchemeWithNoDefault)
  }

  test("class-conf = set, scheme has no default, scheme-conf = set") {
    val sparkConf = constructSparkConf(fakeSchemeWithNoDefault, Some(customLogStoreClassName),
      Some(DelegatingLogStore.defaultAzureLogStoreClassName))
    val e = intercept[AnalysisException](
      withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
        LogStore(spark)
      }
    )
    assert(e.getMessage.contains(
      "(`spark.delta.logStore.class`) and (`spark.delta.logStore.fake.impl`) " +
        "cannot be set at the same time"))
  }

  test("class-conf = set, scheme has default, scheme-conf = not set") {
    testLogStoreClassConfNoSchemeConf("s3a")
  }

  test("class-conf = set, scheme has default, scheme-conf = set") {
    val sparkConf = constructSparkConf("s3a", Some(customLogStoreClassName),
      Some(DelegatingLogStore.defaultAzureLogStoreClassName))
    val e = intercept[AnalysisException](
      withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
        LogStore(spark)
      }
    )
    assert(e.getMessage.contains(
      "(`spark.delta.logStore.class`) and (`spark.delta.logStore.s3a.impl`) " +
        "cannot be set at the same time"))
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
