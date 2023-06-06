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

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.LocalSparkSession._

class LogStoreProviderSuite extends SparkFunSuite {


  private val customLogStoreClassName = classOf[CustomPublicLogStore].getName
  private def fakeSchemeWithNoDefault = "fake"
  private def withoutSparkPrefix(key: String) = key.stripPrefix("spark.")

  private def constructSparkConf(confs: Seq[(String, String)]): SparkConf = {
    val sparkConf = new SparkConf(loadDefaults = false).setMaster("local")
    confs.foreach { case (key, value) => sparkConf.set(key, value) }
    sparkConf
  }

  /**
   * Test with class conf set and scheme conf unset using `scheme`. Test using class conf key both
   * with and without 'spark.' prefix.
   */
  private def testLogStoreClassConfNoSchemeConf(scheme: String) {
    for (classKeys <- Seq(
      // set only prefixed key
      Seq(LogStore.logStoreClassConfKey),
      // set only non-prefixed key
      Seq(withoutSparkPrefix(LogStore.logStoreClassConfKey)),
      // set both spark-prefixed key and non-spark prefixed key
      Seq(LogStore.logStoreClassConfKey, withoutSparkPrefix(LogStore.logStoreClassConfKey))
    )) {
      val sparkConf = constructSparkConf(classKeys.map((_, customLogStoreClassName)))
      withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
        assert(LogStore(spark).isInstanceOf[LogStoreAdaptor])
        assert(LogStore(spark).asInstanceOf[LogStoreAdaptor]
          .logStoreImpl.getClass.getName == customLogStoreClassName)
      }
    }
  }

  /**
   * Test with class conf set and scheme conf set using `scheme`. This tests
   * checkLogStoreConfConflicts. Test conf keys both with and without 'spark.' prefix.
   */
  private def testLogStoreClassConfAndSchemeConf(scheme: String, classConf: String,
      schemeConf: String) {
    val schemeKey = LogStore.logStoreSchemeConfKey(scheme)
    // we test with both the spark-prefixed and non-prefixed keys
    val schemeConfKeys = Seq(schemeKey, withoutSparkPrefix(schemeKey))
    val classConfKeys = Seq(LogStore.logStoreClassConfKey,
      withoutSparkPrefix(LogStore.logStoreClassConfKey))

    schemeConfKeys.foreach { schemeKey =>
      classConfKeys.foreach { classKey =>
        val sparkConf = constructSparkConf(Seq((schemeKey, schemeConf), (classKey, classConf)))
        val e = intercept[AnalysisException](
          withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
            LogStore(spark)
          }
        )
        assert(e.getMessage.contains(
          s"(`$classKey`) and (`$schemeKey`) cannot be set at the same time"))
      }
    }
  }

  test("class-conf = set, scheme has no default, scheme-conf = not set") {
    testLogStoreClassConfNoSchemeConf(fakeSchemeWithNoDefault)
  }

  test("class-conf = set, scheme has no default, scheme-conf = set") {
    testLogStoreClassConfAndSchemeConf(fakeSchemeWithNoDefault, customLogStoreClassName,
      DelegatingLogStore.defaultAzureLogStoreClassName)
  }

  test("class-conf = set, scheme has default, scheme-conf = not set") {
    testLogStoreClassConfNoSchemeConf("s3a")
  }

  test("class-conf = set, scheme has default, scheme-conf = set") {
    testLogStoreClassConfAndSchemeConf("s3a", customLogStoreClassName,
      DelegatingLogStore.defaultAzureLogStoreClassName)
  }

  test("verifyLogStoreConfs - scheme conf keys ") {
    Seq(
      fakeSchemeWithNoDefault, // scheme with no default
      "s3a" // scheme with default
    ).foreach { scheme =>
      val schemeConfKey = LogStore.logStoreSchemeConfKey(scheme)
      for (confs <- Seq(
        // set only non-prefixed key
        Seq((withoutSparkPrefix(schemeConfKey), customLogStoreClassName)),
        // set only prefixed key
        Seq((schemeConfKey, customLogStoreClassName)),
        // set both spark-prefixed key and non-spark prefixed key to same value
        Seq((withoutSparkPrefix(schemeConfKey), customLogStoreClassName),
          (schemeConfKey, customLogStoreClassName))
      )) {
        val sparkConf = constructSparkConf(confs)
        withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
          // no error is thrown
          LogStore(spark)
        }
      }

      // set both spark-prefixed key and non-spark-prefixed key to inconsistent values
      val sparkConf = constructSparkConf(
        Seq((withoutSparkPrefix(schemeConfKey), customLogStoreClassName),
          (schemeConfKey, DelegatingLogStore.defaultAzureLogStoreClassName)))
      val e = intercept[IllegalArgumentException](
        withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
          LogStore(spark)
        }
      )
      assert(e.getMessage.contains(
        s"(${withoutSparkPrefix(schemeConfKey)} = $customLogStoreClassName, " +
          s"$schemeConfKey = ${DelegatingLogStore.defaultAzureLogStoreClassName}) cannot be set " +
          s"to different values. Please only set one of them, or set them to the same value."
      ))
    }
  }

  test("verifyLogStoreConfs - class conf keys") {
    val classConfKey = LogStore.logStoreClassConfKey
    for (confs <- Seq(
      // set only non-prefixed key
      Seq((withoutSparkPrefix(classConfKey), customLogStoreClassName)),
      // set only prefixed key
      Seq((classConfKey, customLogStoreClassName)),
      // set both spark-prefixed key and non-spark prefixed key to same value
      Seq((withoutSparkPrefix(classConfKey), customLogStoreClassName),
        (classConfKey, customLogStoreClassName))
    )) {
      val sparkConf = constructSparkConf(confs)
      withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
        // no error is thrown
      LogStore(spark)
      }
    }

    // set both spark-prefixed key and non-spark-prefixed key to inconsistent values
    val sparkConf = constructSparkConf(
      Seq((withoutSparkPrefix(classConfKey), customLogStoreClassName),
        (classConfKey, DelegatingLogStore.defaultAzureLogStoreClassName)))
    val e = intercept[IllegalArgumentException](
      withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
        LogStore(spark)
      }
    )
    assert(e.getMessage.contains(
      s"(${withoutSparkPrefix(classConfKey)} = $customLogStoreClassName, " +
        s"$classConfKey = ${DelegatingLogStore.defaultAzureLogStoreClassName})" +
        s" cannot be set to different values. Please only set one of them, or set them to the " +
        s"same value."
    ))
  }
}
