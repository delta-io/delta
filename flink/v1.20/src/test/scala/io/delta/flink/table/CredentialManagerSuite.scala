/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.table

import java.util.function.Supplier

import scala.jdk.CollectionConverters.MapHasAsJava

import io.delta.flink.Conf

import org.scalatest.funsuite.AnyFunSuite

class CredentialManagerSuite extends AnyFunSuite {

  test("get and auto refresh credentials") {
    val supplier = new Supplier[java.util.Map[String, String]] {
      var callCount = 0
      override def get(): java.util.Map[String, String] = {
        val currentTime = System.currentTimeMillis()
        val refreshInterval = Conf.getInstance().getCredentialsRefreshAheadInMs
        val result = Map(
          "authKey" -> ("authValue" + callCount),
          // Refresh after around 100 ms
          CredentialManager.CREDENTIAL_EXPIRATION_KEY ->
            (currentTime + refreshInterval + 100).toString).asJava
        callCount += 1
        result
      }
    }

    val manager = new CredentialManager(
      supplier,
      () => {})

    // Initial values
    val initialResult = manager.getCredentials
    Thread.sleep(150)
    // Refreshed values
    val refreshedResult = manager.getCredentials

    assert(initialResult.get("authKey") == "authValue0")
    assert(refreshedResult.get("authKey") == "authValue1")
  }
}
