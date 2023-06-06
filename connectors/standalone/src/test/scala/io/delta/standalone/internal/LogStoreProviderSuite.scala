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

package io.delta.standalone.internal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import io.delta.standalone.exceptions.DeltaStandaloneException

import io.delta.standalone.internal.sources.StandaloneHadoopConf
import io.delta.standalone.internal.storage.{DelegatingLogStore, LogStoreProvider}

class LogStoreProviderSuite extends FunSuite {

  private def fakeSchemeWithNoDefault = "fake"
  private val customLogStoreClassName = classOf[UserDefinedLogStore].getName

  private val sparkClassKey = "spark." + LogStoreProvider.logStoreClassConfKey
  private val legacyClassKey = StandaloneHadoopConf.LEGACY_LOG_STORE_CLASS_KEY
  private val normalClassKey = LogStoreProvider.logStoreClassConfKey

  private def sparkPrefixLogStoreSchemeConfKey(scheme: String) =
    "spark." + LogStoreProvider.logStoreSchemeConfKey(scheme)

  private def newHadoopConf(confs: Seq[(String, String)]): Configuration = {
    val hadoopConf = new Configuration()
    confs.foreach{ case (key, value) => hadoopConf.set(key, value)}
    hadoopConf
  }

  private def testClassAndSchemeConfSet(scheme: String, classConf: String, schemeConf: String)
    : Unit = {

    val schemeConfKeys = Seq(LogStoreProvider.logStoreSchemeConfKey(scheme),
      sparkPrefixLogStoreSchemeConfKey(scheme))
    val classConfKeys = Seq(legacyClassKey, sparkClassKey, normalClassKey)
    schemeConfKeys.foreach{ schemeKey =>
      classConfKeys.foreach { classKey =>
        val hadoopConf = newHadoopConf(
          Seq((classKey, classConf), (schemeKey, schemeConf))
        )
        val e = intercept[IllegalArgumentException](
          LogStoreProvider.createLogStore(hadoopConf)
        )
        assert(e.getMessage.contains(
          s"(`$classKey`) and (`$schemeKey`) cannot be set at the same time"
        ))
      }
    }
  }

  test("class-conf = set, scheme has no default, scheme-conf = set") {
    testClassAndSchemeConfSet(fakeSchemeWithNoDefault, customLogStoreClassName,
      DelegatingLogStore.defaultAzureLogStoreClassName
    )
  }

  test("class-conf = set, scheme has default, scheme-conf = set") {
    testClassAndSchemeConfSet("s3a", customLogStoreClassName,
      DelegatingLogStore.defaultAzureLogStoreClassName
    )
  }

  test("normalizeHadoopConf - scheme conf keys") {
    Seq(
      fakeSchemeWithNoDefault, // scheme with no default
      "s3a" // scheme with default
    ).foreach { scheme =>

        for (hadoopConf <- Seq(
          // set only spark-prefixed key
          newHadoopConf(Seq(
            (sparkPrefixLogStoreSchemeConfKey(scheme), customLogStoreClassName)
          )),
          // set both spark-prefixed key and normalized key to same value
          newHadoopConf(Seq(
            (sparkPrefixLogStoreSchemeConfKey(scheme), customLogStoreClassName),
            (LogStoreProvider.logStoreSchemeConfKey(scheme), customLogStoreClassName)
          ))
        )) {
          val logStore =
            LogStoreProvider.createLogStore(hadoopConf).asInstanceOf[DelegatingLogStore]
          assert(logStore.getDelegate(new Path(s"$scheme://dummy")).getClass.getName ==
            customLogStoreClassName)
          // normalized key is set
          assert(hadoopConf.get(LogStoreProvider.logStoreSchemeConfKey(scheme)) ==
            customLogStoreClassName)
        }

        // set both spark-prefixed key and normalized key to inconsistent values
        val hadoopConf = newHadoopConf(Seq(
          (sparkPrefixLogStoreSchemeConfKey(scheme), customLogStoreClassName),
          (LogStoreProvider.logStoreSchemeConfKey(scheme),
            "io.delta.standalone.internal.storage.AzureLogStore")
        ))
        val e = intercept[IllegalArgumentException](
          LogStoreProvider.createLogStore(hadoopConf)
        )
        assert(e.getMessage.contains(
          s"(${sparkPrefixLogStoreSchemeConfKey(scheme)} = $customLogStoreClassName, " +
            s"${LogStoreProvider.logStoreSchemeConfKey(scheme)} = " +
            s"io.delta.standalone.internal.storage.AzureLogStore) cannot be set to different " +
            s"values. Please only set one of them, or set them to the same value."
        ))
      }
  }

  test("normalizeHadoopConf - class conf keys") {
    // combinations of legacy, spark-prefixed and normalized class conf set to same value
    Seq(Some(legacyClassKey), None).foreach { legacyConf =>
      Seq(Some(sparkClassKey), None).foreach { sparkPrefixConf =>
        Seq(Some(normalClassKey), None).foreach { normalConf =>
          if (legacyConf.nonEmpty || sparkPrefixConf.nonEmpty || normalConf.nonEmpty) {
            val hadoopConf = new Configuration()
            legacyConf.foreach(hadoopConf.set(_, customLogStoreClassName))
            sparkPrefixConf.foreach(hadoopConf.set(_, customLogStoreClassName))
            normalConf.foreach(hadoopConf.set(_, customLogStoreClassName))

            assert(LogStoreProvider.createLogStore(hadoopConf).getClass.getName ==
              customLogStoreClassName)
            // normalized key is set
            assert(hadoopConf.get(normalClassKey) == customLogStoreClassName)
          }
        }
      }
    }

    // combinations of legacy, spark-prefixed and normalized class conf set to inconsistent values
    for ((key1, key2) <- Seq(
      (legacyClassKey, sparkClassKey),
      (normalClassKey, legacyClassKey),
      (normalClassKey, sparkClassKey)
    )) {
      val hadoopConf = newHadoopConf(Seq(
        (key1, customLogStoreClassName),
        (key2, "io.delta.standalone.internal.storage.AzureLogStore")
      ))
      val e = intercept[IllegalArgumentException] {
        LogStoreProvider.createLogStore((hadoopConf))
      }
      assert(
        e.getMessage.contains("cannot be set to different values. Please only set one of them, " +
        "or set them to the same value.")
        && e.getMessage.contains(s"$key1 = $customLogStoreClassName")
          &&e.getMessage.contains(s"$key2 = io.delta.standalone.internal.storage.AzureLogStore")

      )
    }
  }

  test("DelegatingLogStore is default") {
    val hadoopConf = new Configuration()
    assert(LogStoreProvider.createLogStore(hadoopConf).getClass.getName
      == "io.delta.standalone.internal.storage.DelegatingLogStore")
  }

  test("Set class Conf to class that doesn't extend LogStore") {
    val hadoopConf = newHadoopConf(
      Seq((LogStoreProvider.logStoreClassConfKey, "io.delta.standalone.DeltaLog")))
    val e = intercept[DeltaStandaloneException](
      LogStoreProvider.createLogStore(hadoopConf)
    )
    assert(e.getMessage.contains(
      "Can't instantiate a LogStore with classname io.delta.standalone.DeltaLog"
    ))
  }

  test("Set (deprecated) scala class with class Conf") {
    val hadoopConf = newHadoopConf(
      Seq((LogStoreProvider.logStoreClassConfKey,
        "io.delta.standalone.internal.storage.AzureLogStore")))
    assert(LogStoreProvider.createLogStore(hadoopConf).getClass.getName
      == "io.delta.standalone.internal.storage.AzureLogStore")
  }

  test("Set delta-storage class with class Conf") {
    val hadoopConf = newHadoopConf(
      Seq((LogStoreProvider.logStoreClassConfKey,
        "io.delta.storage.AzureLogStore")))
    assert(LogStoreProvider.createLogStore(hadoopConf).getClass.getName
      == "io.delta.storage.AzureLogStore")
  }
}
