/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite

class TokenProviderSuite extends AnyFunSuite {

  private val OAUTH_URI = "https://oauth.example.com/token"
  private val CLIENT_ID = "test-client-id"
  private val CLIENT_SECRET = "test-client-secret"

  test("create token provider via configs") {
    // Test with valid token - should create StaticTokenProvider
    val tokenConfigs = Map(
      AuthConfigs.TYPE -> AuthConfigs.STATIC_TYPE_VALUE,
      AuthConfigs.STATIC_TOKEN -> "test-token"
    ).asJava
    val tokenProvider = TokenProvider.create(tokenConfigs)
    assert(tokenProvider.isInstanceOf[StaticTokenProvider])
    assert(tokenProvider.accessToken() == "test-token")

    // Test with complete OAuth config - should create OAuthTokenProvider
    val oauthConfigs = Map(
      AuthConfigs.TYPE -> AuthConfigs.OAUTH_TYPE_VALUE,
      AuthConfigs.OAUTH_URI -> OAUTH_URI,
      AuthConfigs.OAUTH_CLIENT_ID -> CLIENT_ID,
      AuthConfigs.OAUTH_CLIENT_SECRET -> CLIENT_SECRET
    ).asJava
    val oauthProvider = TokenProvider.create(oauthConfigs)
    assert(oauthProvider.isInstanceOf[OAuthTokenProvider])
    val configs = oauthProvider.configs().asScala
    assert(configs.size == 4)
    assert(configs(AuthConfigs.TYPE) == AuthConfigs.OAUTH_TYPE_VALUE)
    assert(configs(AuthConfigs.OAUTH_URI) == OAUTH_URI)
    assert(configs(AuthConfigs.OAUTH_CLIENT_ID) == CLIENT_ID)
    assert(configs(AuthConfigs.OAUTH_CLIENT_SECRET) == CLIENT_SECRET)

    // Test with incomplete OAuth config - should throw
    val incompleteOAuthOptions = Map(
      AuthConfigs.TYPE -> AuthConfigs.OAUTH_TYPE_VALUE,
      AuthConfigs.OAUTH_URI -> OAUTH_URI
    ).asJava
    val exception1 = intercept[IllegalArgumentException] {
      TokenProvider.create(incompleteOAuthOptions)
    }
    assert(exception1.getMessage.contains("Configuration key 'oauth.clientId' is missing or empty"))

    // Test with no valid config - should throw
    val exception2 = intercept[IllegalArgumentException] {
      TokenProvider.create(Map.empty[String, String].asJava)
    }
    assert(exception2.getMessage.contains("Required configuration key 'type' is missing or empty"))

    // Test with no 'type' even both static token and oauth are provided - should throw
    val bothOptions = Map(
      AuthConfigs.STATIC_TOKEN -> "fixed-token",
      AuthConfigs.OAUTH_URI -> OAUTH_URI,
      AuthConfigs.OAUTH_CLIENT_ID -> CLIENT_ID,
      AuthConfigs.OAUTH_CLIENT_SECRET -> CLIENT_SECRET
    ).asJava
    val exception3 = intercept[IllegalArgumentException] {
      TokenProvider.create(bothOptions)
    }
    assert(exception3.getMessage.contains("Required configuration key 'type' is missing or empty"))
  }

  test("static token provider") {
    val configs1 = Map(
      AuthConfigs.TYPE -> AuthConfigs.STATIC_TYPE_VALUE,
      AuthConfigs.STATIC_TOKEN -> "my-token"
    ).asJava
    val provider = TokenProvider.create(configs1)
    assert(provider.accessToken() == "my-token")

    val configs2 = Map(
      AuthConfigs.TYPE -> AuthConfigs.STATIC_TYPE_VALUE,
      AuthConfigs.STATIC_TOKEN -> "consistent"
    ).asJava
    val consistentProvider = TokenProvider.create(configs2)
    assert(consistentProvider.accessToken() == "consistent")
    assert(consistentProvider.accessToken() == "consistent")
    assert(consistentProvider.accessToken() == "consistent")

    val configs3 = Map(
      AuthConfigs.TYPE -> AuthConfigs.STATIC_TYPE_VALUE,
      AuthConfigs.STATIC_TOKEN -> "test-token"
    ).asJava
    val providerWithProperties = TokenProvider.create(configs3)
    val properties = providerWithProperties.configs().asScala
    assert(properties.size == 2)
    assert(properties(AuthConfigs.STATIC_TOKEN) == "test-token")
    assert(properties(AuthConfigs.TYPE) == AuthConfigs.STATIC_TYPE_VALUE)

    val configs4 = Map(
      AuthConfigs.TYPE -> AuthConfigs.STATIC_TYPE_VALUE,
      AuthConfigs.STATIC_TOKEN -> "factory-token"
    ).asJava
    val factoryProvider = TokenProvider.create(configs4)
    assert(factoryProvider != null)
    assert(factoryProvider.accessToken() == "factory-token")
  }

  test("oauth token provider") {
    val configs = Map(
      AuthConfigs.TYPE -> AuthConfigs.OAUTH_TYPE_VALUE,
      AuthConfigs.OAUTH_URI -> OAUTH_URI,
      AuthConfigs.OAUTH_CLIENT_ID -> CLIENT_ID,
      AuthConfigs.OAUTH_CLIENT_SECRET -> CLIENT_SECRET
    ).asJava
    val provider = TokenProvider.create(configs)
    assert(provider != null)
    assert(provider.isInstanceOf[OAuthTokenProvider])

    val providerConfigs = provider.configs().asScala
    assert(providerConfigs.size == 4)
    assert(providerConfigs(AuthConfigs.TYPE) == AuthConfigs.OAUTH_TYPE_VALUE)
    assert(providerConfigs(AuthConfigs.OAUTH_URI) == OAUTH_URI)
    assert(providerConfigs(AuthConfigs.OAUTH_CLIENT_ID) == CLIENT_ID)
    assert(providerConfigs(AuthConfigs.OAUTH_CLIENT_SECRET) == CLIENT_SECRET)
  }

  test("custom token provider") {
    // Test with custom TokenProvider using fully qualified class name
    val customConfigs = Map(
      AuthConfigs.TYPE -> classOf[CustomTestTokenProvider].getName,
      "custom.token" -> "custom-test-token"
    ).asJava

    val customProvider = TokenProvider.create(customConfigs)
    assert(customProvider != null)
    assert(customProvider.isInstanceOf[CustomTestTokenProvider])
    assert(customProvider.accessToken() == "custom-test-token")
    val configs = customProvider.configs().asScala
    assert(configs(AuthConfigs.TYPE) == classOf[CustomTestTokenProvider].getName)
    assert(configs("custom.token") == "custom-test-token")

    // Test that configs() can be used to create a new instance
    val clonedProvider = TokenProvider.create(customProvider.configs())
    assert(clonedProvider.isInstanceOf[CustomTestTokenProvider])
    assert(clonedProvider.accessToken() == "custom-test-token")
  }

  test("custom token provider with invalid class name") {
    // Test with non-existent class name - should throw RuntimeException
    val invalidConfigs = Map(
      AuthConfigs.TYPE -> "com.example.NonExistentTokenProvider"
    ).asJava

    val exception = intercept[RuntimeException] {
      TokenProvider.create(invalidConfigs)
    }
    assert(exception.getMessage.contains("Failed to instantiate custom TokenProvider"))
    assert(exception.getCause.isInstanceOf[ClassNotFoundException])
  }

  test("custom token provider with invalid class") {
    // Test with a class that doesn't implement TokenProvider - should throw RuntimeException
    val invalidConfigs = Map(
      AuthConfigs.TYPE -> classOf[String].getName
    ).asJava

    val exception = intercept[RuntimeException] {
      TokenProvider.create(invalidConfigs)
    }
    assert(exception.getMessage.contains("Failed to instantiate custom TokenProvider"))
  }
}

/**
 * Custom TokenProvider implementation for testing purposes.
 */
class CustomTestTokenProvider extends TokenProvider {
  private var configsMap: java.util.Map[String, String] = _

  override def initialize(configs: java.util.Map[String, String]): Unit = {
    this.configsMap = new java.util.HashMap[String, String](configs)
  }

  override def accessToken(): String = {
    configsMap.get("custom.token")
  }

  override def configs(): java.util.Map[String, String] = {
    new java.util.HashMap[String, String](configsMap)
  }
}
