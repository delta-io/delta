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

import io.delta.standalone.internal.storage.{DelegatingLogStore, LogStoreProvider}

class DelegatingLogStoreSuite extends FunSuite {

  private val customLogStoreClassName = classOf[UserDefinedLogStore].getName

  private def fakeSchemeWithNoDefault = "fake"

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

    val hadoopConf = new Configuration()
    val schemeConfKey = LogStoreProvider.logStoreSchemeConfKey(scheme)
    schemeConf.map(hadoopConf.set(schemeConfKey, _))

    val delegatingLogStore = new DelegatingLogStore(hadoopConf)
    val actualLogStore = delegatingLogStore.getDelegate(
      new Path(s"${scheme}://dummy")
    )
    assert(actualLogStore.getClass.getName == expClassName)
  }

  test("DelegatingLogStore resolution using default scheme confs") {
    for (scheme <- DelegatingLogStore.s3Schemes) {
      testDelegatingLogStore(scheme, None, DelegatingLogStore.defaultS3LogStoreClassName)
    }
    for (scheme <- DelegatingLogStore.azureSchemes) {
      testDelegatingLogStore(scheme, None, DelegatingLogStore.defaultAzureLogStoreClassName)
    }
    for (scheme <- DelegatingLogStore.gsSchemes) {
      testDelegatingLogStore(scheme, None, DelegatingLogStore.defaultGCSLogStoreClassName)
    }
    testDelegatingLogStore(fakeSchemeWithNoDefault, None,
      DelegatingLogStore.defaultHDFSLogStoreClassName)
  }

  test("DelegatingLogStore resolution using customized scheme confs") {
    val allTestSchemes = DelegatingLogStore.s3Schemes ++ DelegatingLogStore.azureSchemes +
      fakeSchemeWithNoDefault
    for (scheme <- allTestSchemes) {
      for (store <- Seq(
        // default (java) classes (in io.delta.storage)
        DelegatingLogStore.defaultS3LogStoreClassName,
        DelegatingLogStore.defaultAzureLogStoreClassName,
        DelegatingLogStore.defaultHDFSLogStoreClassName,
        DelegatingLogStore.defaultGCSLogStoreClassName,
        // deprecated (scala) classes
        "io.delta.standalone.internal.storage.S3SingleDriverLogStore",
        "io.delta.standalone.internal.storage.AzureLogStore",
        "io.delta.standalone.internal.storage.HDFSLogStore",
        customLogStoreClassName)) {
        // we set delta.logStore.${scheme}.impl -> $store
        testDelegatingLogStore(scheme, Some(store), store)
      }
    }
  }
}
