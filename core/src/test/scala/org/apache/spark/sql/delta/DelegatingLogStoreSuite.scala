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

import scala.collection.JavaConverters._

import io.delta.storage.DelegatingLogStore
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.delta.storage.{LogStore, LogStoreAdaptor}
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.LocalSparkSession._
import org.apache.spark.sql.SparkSession

class DelegatingLogStoreSuite extends SparkFunSuite {

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
   */
  private def testDelegatingLogStore(
      scheme: String,
      schemeConf: Option[String],
      expClassName: String): Unit = {
    val sparkConf = constructSparkConf(scheme, None, schemeConf)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      // scalastyle:off deltahadoopconfiguration
      val hadoopConf = spark.sessionState.newHadoopConf()
      // scalastyle:on deltahadoopconfiguration

      val actualLogStore = LogStore(spark)
      val delegatedLogStore = new DelegatingLogStore(hadoopConf)
        .getDelegateByScheme(new Path(s"${scheme}://dummy"), hadoopConf)

      assert(actualLogStore.isInstanceOf[LogStoreAdaptor])
      assert(actualLogStore.asInstanceOf[LogStoreAdaptor]
        .logStoreImpl.getClass.getName == classOf[DelegatingLogStore].getName)
      assert(delegatedLogStore.getClass.getName == expClassName)
    }
  }

  /**
   * Test with class conf set and scheme conf unset using `scheme`.
   */
  private def testLogStoreClassConfNoSchemeConf(scheme: String) {
    val sparkConf = constructSparkConf(
      scheme, Some(DelegatingLogStore.DEFAULT_HDFS_LOG_STORE_CLASS_NAME), None)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      assert(LogStore(spark).isInstanceOf[LogStoreAdaptor])
      assert(LogStore(spark).asInstanceOf[LogStoreAdaptor]
        .logStoreImpl.getClass.getName == DelegatingLogStore.DEFAULT_HDFS_LOG_STORE_CLASS_NAME)
    }
  }

  test("DelegatingLogStore resolution using default scheme confs") {
    for (scheme <- DelegatingLogStore.S3_SCHEMES.asScala) {
      testDelegatingLogStore(scheme, None, DelegatingLogStore.DEFAULT_S3_LOG_STORE_CLASS_NAME)
    }
    for (scheme <- DelegatingLogStore.AZURE_SCHEMES.asScala) {
      testDelegatingLogStore(scheme, None, DelegatingLogStore.DEFAULT_AZURE_LOG_STORE_CLASS_NAME)
    }
    testDelegatingLogStore(fakeSchemeWithNoDefault, None,
      DelegatingLogStore.DEFAULT_HDFS_LOG_STORE_CLASS_NAME)
  }

  test("DelegatingLogStore resolution using customized scheme confs") {
    val allTestSchemes = DelegatingLogStore.ALL_SCHEMES.asScala + fakeSchemeWithNoDefault
    for (scheme <- allTestSchemes) {
      for (store <- Seq(
        DelegatingLogStore.DEFAULT_S3_LOG_STORE_CLASS_NAME,
        DelegatingLogStore.DEFAULT_AZURE_LOG_STORE_CLASS_NAME,
        DelegatingLogStore.DEFAULT_HDFS_LOG_STORE_CLASS_NAME)) {

        // we set spark.delta.logStore.${scheme}.impl -> $store
        testDelegatingLogStore(scheme, Some(store), store)
      }
    }
  }

  test("class-conf = set, scheme has no default, scheme-conf = not set") {
    testLogStoreClassConfNoSchemeConf(fakeSchemeWithNoDefault)
  }

  test("class-conf = set, scheme has no default, scheme-conf = set") {
    val classConf = Some(DelegatingLogStore.DEFAULT_HDFS_LOG_STORE_CLASS_NAME)
    val schemeConf = Some(DelegatingLogStore.DEFAULT_AZURE_LOG_STORE_CLASS_NAME)
    val sparkConf = constructSparkConf(fakeSchemeWithNoDefault, classConf, schemeConf)
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
    val classConf = Some(DelegatingLogStore.DEFAULT_HDFS_LOG_STORE_CLASS_NAME)
    val schemeConf = Some(DelegatingLogStore.DEFAULT_AZURE_LOG_STORE_CLASS_NAME)
    val sparkConf = constructSparkConf("s3a", classConf, schemeConf)
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
