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
import org.scalatest.FunSuite

import io.delta.standalone.exceptions.DeltaStandaloneException

import io.delta.standalone.internal.storage.{DelegatingLogStore, LogStoreProvider}

class LogStoreProviderSuite extends FunSuite {

  private def fakeSchemeWithNoDefault = "fake"
  private val customLogStoreClassName = classOf[UserDefinedLogStore].getName

  private def newHadoopConf(confs: Seq[(String, String)]): Configuration = {
    val hadoopConf = new Configuration()
    confs.foreach{ case (key, value) => hadoopConf.set(key, value)}
    hadoopConf
  }

  private def testClassAndSchemeConfSet(scheme: String, classConf: String, schemeConf: String)
    : Unit = {

    val classConfKey = LogStoreProvider.logStoreClassConfKey
    val schemeConfKey = LogStoreProvider.logStoreSchemeConfKey(scheme)
    val hadoopConf = newHadoopConf(
      Seq((classConfKey, classConf), (schemeConfKey, schemeConf))
    )

    val e = intercept[IllegalArgumentException](
      LogStoreProvider.createLogStore(hadoopConf)
    )
    assert(e.getMessage.contains(
      "(`io.delta.standalone.LOG_STORE_CLASS_KEY`) and " +
        f"(`${LogStoreProvider.logStoreSchemeConfKey(scheme)}`) cannot be set at the same time"
    ))
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

  test("Set scala class with class Conf") {
    val hadoopConf = newHadoopConf(
      Seq((LogStoreProvider.logStoreClassConfKey,
        "io.delta.standalone.internal.storage.AzureLogStore")))
    assert(LogStoreProvider.createLogStore(hadoopConf).getClass.getName
      == "io.delta.standalone.internal.storage.AzureLogStore")
  }

  // todo: set delta-storage class with class Conf
}
