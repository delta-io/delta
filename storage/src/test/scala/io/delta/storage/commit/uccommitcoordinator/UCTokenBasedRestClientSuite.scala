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

package io.delta.storage.commit.uccommitcoordinator

import java.util.{Collections, Map => JMap}

import io.unitycatalog.client.auth.TokenProvider
import org.scalatest.funsuite.AnyFunSuite

class UCTokenBasedRestClientSuite extends AnyFunSuite {

  /** Simple TokenProvider implementation for testing */
  class TestTokenProvider(token: String) extends TokenProvider {
    override def accessToken(): String = token
    override def configs(): JMap[String, String] = Collections.emptyMap()
    override def initialize(configs: JMap[String, String]): Unit = {}
  }

  test("constructor requires non-null TokenProvider") {
    val exception = intercept[NullPointerException] {
      new UCTokenBasedRestClient("https://test-uri.com", null)
    }
    assert(exception.getMessage.contains("tokenProvider must not be null"))
  }

  test("constructor accepts valid parameters") {
    val tokenProvider = new TestTokenProvider("test-token")

    val client = new UCTokenBasedRestClient("https://test-uri.com", tokenProvider)
    assert(client != null)
    client.close()
  }

  test("constructor handles URI with trailing slash") {
    val tokenProvider = new TestTokenProvider("test-token")

    val client = new UCTokenBasedRestClient("https://test-uri.com/", tokenProvider)
    assert(client != null)
    client.close()
  }

  test("constructor handles URI without trailing slash") {
    val tokenProvider = new TestTokenProvider("test-token")

    val client = new UCTokenBasedRestClient("https://test-uri.com", tokenProvider)
    assert(client != null)
    client.close()
  }
}
