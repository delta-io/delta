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

package org.apache.spark.sql.delta.coordinatedcommits

import io.delta.storage.commit.uccommitcoordinator.{UCClient, UCCommitCoordinatorClient}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession
import org.mockito.{Mock, Mockito}
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, never, times, verify, when}

class UCCommitCoordinatorBuilderSuite extends SparkFunSuite with SharedSparkSession {

  @Mock
  private val mockFactory: UCClientFactory = mock(classOf[UCClientFactory])

  override def beforeEach(): Unit = {
    super.beforeEach()
    Mockito.reset(mockFactory)
    CommitCoordinatorProvider.clearAllBuilders()
    UCCommitCoordinatorBuilder.ucClientFactory = mockFactory
    UCCommitCoordinatorBuilder.clearCache()
    CommitCoordinatorProvider.registerBuilder(UCCommitCoordinatorBuilder)
  }

  case class CatalogTestConfig(
      name: String,
      uri: Option[String] = None,
      authConfig: Map[String, String] = Map.empty,
      metastoreId: Option[String] = None,
      path: Option[String] = Some("io.unitycatalog.spark.UCSingleCatalog"),
      useLegacyAuth: Boolean = true) {
    // Helper to get the converted auth config (what getCatalogConfigs returns)
    def convertedAuthConfig: Map[String, String] = {
      if (useLegacyAuth && authConfig.contains("token")) {
        Map("type" -> "static", "token" -> authConfig("token"))
      } else {
        authConfig
      }
    }
  }

  def setupCatalogs(configs: CatalogTestConfig*)(testCode: => Unit): Unit = {
    val allConfigs = configs.flatMap { config =>
      val baseConfigs = Seq(
        config.path.map(p => s"spark.sql.catalog.${config.name}" -> p),
        config.uri.map(uri => s"spark.sql.catalog.${config.name}.uri" -> uri)).flatten

      // Add auth configurations based on useLegacyAuth flag
      val authConfig = if (config.useLegacyAuth) {
        // Legacy format: spark.sql.catalog.<name>.token
        config.authConfig.get("token").map { token =>
          s"spark.sql.catalog.${config.name}.token" -> token
        }.toSeq
      } else {
        // New format: spark.sql.catalog.<name>.auth.*
        config.authConfig.map { case (key, value) =>
          s"spark.sql.catalog.${config.name}.auth.$key" -> value
        }
      }

      baseConfigs ++ authConfig
    }

    withSQLConf(allConfigs: _*) {
      configs.foreach { config =>
        (config.uri, config.authConfig.isEmpty, config.metastoreId) match {
          case (Some(uri), false, Some(id)) =>
            // Convert legacy auth config to new format for mocking
            val convertedAuthConfig = if (config.useLegacyAuth) {
              Map("type" -> "static", "token" -> config.authConfig("token"))
            } else {
              config.authConfig
            }
            registerMetastoreId(uri, convertedAuthConfig, id)
          case (Some(uri), false, None) =>
            // Convert legacy auth config to new format for mocking
            val convertedAuthConfig = if (config.useLegacyAuth) {
              Map("type" -> "static", "token" -> config.authConfig("token"))
            } else {
              config.authConfig
            }
            registerMetastoreIdException(
              uri,
              convertedAuthConfig,
              new RuntimeException("Invalid metastore ID"))
          case _ => // Do nothing for incomplete configs
        }
      }
      testCode
    }
  }

  test("build with valid configuration") {
    val expectedMetastoreId = "test-metastore-id"
    val catalog1 = CatalogTestConfig(
      name = "catalog1",
      uri = Some("https://test-uri-1.com"),
      authConfig = Map("token" -> "test-token-1"),
      metastoreId = Some(expectedMetastoreId))
    val catalog2 = CatalogTestConfig(
      name = "catalog2",
      uri = Some("https://test-uri-2.com"),
      authConfig = Map("token" -> "test-token-2"),
      metastoreId = Some("different-metastore-id"))

    setupCatalogs(catalog1, catalog2) {
      val result = getCommitCoordinatorClient(expectedMetastoreId)

      assert(result.isInstanceOf[UCCommitCoordinatorClient])
      verify(mockFactory, times(2)).createUCClient(catalog1.uri.get, catalog1.convertedAuthConfig)
      verify(mockFactory).createUCClient(catalog2.uri.get, catalog2.convertedAuthConfig)
      verify(mockFactory.createUCClient(catalog1.uri.get, catalog1.convertedAuthConfig))
        .getMetastoreId
      verify(mockFactory.createUCClient(catalog2.uri.get, catalog2.convertedAuthConfig))
        .getMetastoreId
      verify(mockFactory.createUCClient(catalog2.uri.get, catalog2.convertedAuthConfig)).close()
      verify(mockFactory.createUCClient(catalog1.uri.get, catalog1.convertedAuthConfig)).close()
    }
  }

  test("build with missing metastore ID") {
    val exception = intercept[IllegalArgumentException] {
      CommitCoordinatorProvider.getCommitCoordinatorClient(
        UCCommitCoordinatorBuilder.getName,
        Map.empty,
        spark)
    }
    assert(exception.getMessage.contains("UC metastore ID not found"))
  }

  test("build with no matching catalog") {
    val metastoreId = "test-metastore-id"
    val catalog = CatalogTestConfig(
      name = "catalog",
      uri = Some("https://test-uri.com"),
      authConfig = Map("token" -> "test-token"),
      metastoreId = Some("different-metastore-id"))

    setupCatalogs(catalog) {
      val exception = intercept[IllegalStateException] {
        getCommitCoordinatorClient(metastoreId)
      }
      assert(exception.getMessage.contains("No matching catalog found"))
      verify(mockFactory).createUCClient(catalog.uri.get, catalog.convertedAuthConfig)
      verify(mockFactory.createUCClient(
        catalog.uri.get,
        catalog.convertedAuthConfig)).getMetastoreId
      verify(mockFactory.createUCClient(catalog.uri.get, catalog.convertedAuthConfig)).close()
    }
  }

  test("build with multiple matching catalogs") {
    val metastoreId = "test-metastore-id"
    val catalog1 = CatalogTestConfig(
      name = "catalog1",
      uri = Some("https://test-uri1.com"),
      authConfig = Map("token" -> "test-token-1"),
      metastoreId = Some(metastoreId))
    val catalog2 = CatalogTestConfig(
      name = "catalog2",
      uri = Some("https://test-uri2.com"),
      authConfig = Map("token" -> "test-token-2"),
      metastoreId = Some(metastoreId))

    setupCatalogs(catalog1, catalog2) {
      val exception = intercept[IllegalStateException] {
        getCommitCoordinatorClient(metastoreId)
      }
      assert(exception.getMessage.contains("Found multiple catalogs"))
      verify(mockFactory).createUCClient(catalog1.uri.get, catalog1.convertedAuthConfig)
      verify(mockFactory).createUCClient(catalog2.uri.get, catalog2.convertedAuthConfig)
      verify(mockFactory.createUCClient(catalog1.uri.get, catalog1.convertedAuthConfig))
        .getMetastoreId
      verify(mockFactory.createUCClient(catalog2.uri.get, catalog2.convertedAuthConfig))
        .getMetastoreId
      verify(mockFactory.createUCClient(catalog1.uri.get, catalog1.convertedAuthConfig)).close()
      verify(mockFactory.createUCClient(catalog2.uri.get, catalog2.convertedAuthConfig)).close()
    }
  }

  test("build with mixed valid and invalid catalog configurations") {
    val expectedMetastoreId = "test-metastore-id"
    val validCatalog = CatalogTestConfig(
      name = "valid-catalog",
      uri = Some("https://valid-uri.com"),
      authConfig = Map("token" -> "valid-token"),
      metastoreId = Some(expectedMetastoreId))
    val invalidCatalog1 = CatalogTestConfig(
      name = "invalid-catalog-1",
      uri = Some("https://invalid-uri.com"),
      authConfig = Map("token" -> "invalid-token"),
      metastoreId = None)
    val invalidCatalog2 = CatalogTestConfig(
      name = "invalid-catalog-2",
      uri = Some("random-uri"),
      authConfig = Map("token" -> "invalid-token"))
    val incompleteCatalog = CatalogTestConfig(
      name = "incomplete-catalog",
      path = None)

    setupCatalogs(validCatalog, invalidCatalog1, invalidCatalog2, incompleteCatalog) {
      val result = getCommitCoordinatorClient(expectedMetastoreId)

      assert(result.isInstanceOf[UCCommitCoordinatorClient])
      verify(mockFactory, times(2)).createUCClient(
        validCatalog.uri.get,
        validCatalog.convertedAuthConfig)
      verify(
        mockFactory.createUCClient(validCatalog.uri.get, validCatalog.convertedAuthConfig),
        times(1)).close()
    }
  }

  test("build caching behavior") {
    val metastoreId = "test-metastore-id"
    val catalog = CatalogTestConfig(
      name = "catalog",
      uri = Some("https://test-uri.com"),
      authConfig = Map("token" -> "test-token"),
      metastoreId = Some(metastoreId))

    setupCatalogs(catalog) {
      val result1 = getCommitCoordinatorClient(metastoreId)
      val result2 = getCommitCoordinatorClient(metastoreId)
      assert(result1 eq result2)
    }
  }

  test("build with multiple catalogs pointing to the same URI, token, and metastore") {
    val metastoreId = "shared-metastore-id"
    val sharedUri = "https://shared-test-uri.com"
    val sharedAuthConfig = Map("token" -> "shared-test-token")
    val catalog1 = CatalogTestConfig(
      name = "catalog1",
      uri = Some(sharedUri),
      authConfig = sharedAuthConfig,
      metastoreId = Some(metastoreId))
    val catalog2 = CatalogTestConfig(
      name = "catalog2",
      uri = Some(sharedUri),
      authConfig = sharedAuthConfig,
      metastoreId = Some(metastoreId))
    val catalog3 = CatalogTestConfig(
      name = "catalog3",
      uri = Some(sharedUri),
      authConfig = sharedAuthConfig,
      metastoreId = Some(metastoreId))

    setupCatalogs(catalog1, catalog2, catalog3) {
      val result = getCommitCoordinatorClient(metastoreId)

      assert(result.isInstanceOf[UCCommitCoordinatorClient])
      // Use converted auth config since all catalogs use legacy format by default
      val convertedAuthConfig = catalog1.convertedAuthConfig
      verify(mockFactory, times(2)).createUCClient(sharedUri, convertedAuthConfig)
      verify(mockFactory.createUCClient(sharedUri, convertedAuthConfig)).getMetastoreId
      verify(mockFactory.createUCClient(sharedUri, convertedAuthConfig)).close()
    }
  }

  test("build with a catalog having invalid path but valid URI and token") {
    val metastoreId = "test-metastore-id"
    val catalog = CatalogTestConfig(
      name = "invalid-path-catalog",
      uri = Some("https://test-uri.com"),
      authConfig = Map("token" -> "test-token"),
      metastoreId = Some(metastoreId),
      path = Some("invalid-catalog-path"))

    setupCatalogs(catalog) {
      assert(UCCommitCoordinatorBuilder.getCatalogConfigs(spark).isEmpty)
      val e = intercept[IllegalStateException] {
        getCommitCoordinatorClient(metastoreId)
      }
      assert(e.getMessage.contains("No matching catalog found"))
      verify(mockFactory, never()).createUCClient(catalog.uri.get, catalog.convertedAuthConfig)
    }
  }

  test("getCatalogConfigs with new auth.* format") {
    val catalogName = "new_catalog"
    val uri = "https://test-uri.com"
    val token = "test-token"
    val catalog = CatalogTestConfig(
      name = catalogName,
      uri = Some(uri),
      authConfig = Map("type" -> "static", "token" -> token),
      useLegacyAuth = false)

    setupCatalogs(catalog) {
      val configs = UCCommitCoordinatorBuilder.getCatalogConfigs(spark)
      assert(configs.length == 1)

      val (name, catalogUri, authConfig) = configs.head
      assert(name == catalogName)
      assert(catalogUri == uri)
      assert(authConfig("type") == "static")
      assert(authConfig("token") == token)
    }
  }

  test("getCatalogConfigs with nested auth.* configurations") {
    val catalogName = "oauth_catalog"
    val uri = "https://test-uri.com"
    val catalog = CatalogTestConfig(
      name = catalogName,
      uri = Some(uri),
      authConfig = Map(
        "type" -> "oauth",
        "oauth.uri" -> "https://oauth.example.com",
        "oauth.clientId" -> "client123",
        "oauth.clientSecret" -> "secret456"),
      useLegacyAuth = false)

    setupCatalogs(catalog) {
      val configs = UCCommitCoordinatorBuilder.getCatalogConfigs(spark)
      assert(configs.length == 1)

      val (name, catalogUri, authConfig) = configs.head
      assert(name == catalogName)
      assert(catalogUri == uri)
      assert(authConfig("type") == "oauth")
      assert(authConfig("oauth.uri") == "https://oauth.example.com")
      assert(authConfig("oauth.clientId") == "client123")
      assert(authConfig("oauth.clientSecret") == "secret456")
    }
  }

  test("getCatalogConfigs skips catalog with no auth configurations") {
    val catalogName = "no_auth_catalog"
    val uri = "https://test-uri.com"
    val catalog = CatalogTestConfig(
      name = catalogName,
      uri = Some(uri))

    setupCatalogs(catalog) {
      val configs = UCCommitCoordinatorBuilder.getCatalogConfigs(spark)
      assert(configs.isEmpty, "Catalog without auth config should be skipped")
    }
  }

  test("getCatalogConfigs prefers new auth.* format over legacy token") {
    val catalogName = "mixed_catalog"
    val uri = "https://test-uri.com"
    val legacyToken = "legacy-token"
    val newToken = "new-token"

    // Manually set up both legacy and new auth configs
    withSQLConf(
      s"spark.sql.catalog.$catalogName" -> "io.unitycatalog.spark.UCSingleCatalog",
      s"spark.sql.catalog.$catalogName.uri" -> uri,
      s"spark.sql.catalog.$catalogName.token" -> legacyToken,
      s"spark.sql.catalog.$catalogName.auth.type" -> "static",
      s"spark.sql.catalog.$catalogName.auth.token" -> newToken) {
      val configs = UCCommitCoordinatorBuilder.getCatalogConfigs(spark)
      assert(configs.length == 1)

      val (name, catalogUri, authConfig) = configs.head
      assert(name == catalogName)
      assert(catalogUri == uri)
      // New format should take precedence
      assert(authConfig("type") == "static")
      assert(authConfig("token") == newToken)
      assert(!authConfig.contains(legacyToken))
    }
  }

  test("getCatalogConfigs handles multiple catalogs with mixed formats") {
    val catalog1 = CatalogTestConfig(
      name = "catalog1",
      uri = Some("https://uri1.com"),
      authConfig = Map("token" -> "token1"),
      useLegacyAuth = true)
    val catalog2 = CatalogTestConfig(
      name = "catalog2",
      uri = Some("https://uri2.com"),
      authConfig = Map("type" -> "static", "token" -> "token2"),
      useLegacyAuth = false)
    val catalog3 = CatalogTestConfig(
      name = "catalog3",
      uri = Some("https://uri3.com"))

    setupCatalogs(catalog1, catalog2, catalog3) {
      val configs = UCCommitCoordinatorBuilder.getCatalogConfigs(spark)
      // Only catalog1 and catalog2 should be included (catalog3 has no auth)
      assert(configs.length == 2)

      val catalog1Config = configs.find(_._1 == "catalog1")
      assert(catalog1Config.isDefined)
      assert(catalog1Config.get._3("type") == "static")
      assert(catalog1Config.get._3("token") == "token1")

      val catalog2Config = configs.find(_._1 == "catalog2")
      assert(catalog2Config.isDefined)
      assert(catalog2Config.get._3("type") == "static")
      assert(catalog2Config.get._3("token") == "token2")
    }
  }

  test("buildForCatalog with legacy token format") {
    val catalogName = "test_catalog"
    val uri = "https://test-uri.com"
    val token = "test-token"
    val catalog = CatalogTestConfig(
      name = catalogName,
      uri = Some(uri),
      authConfig = Map("token" -> token),
      useLegacyAuth = true)

    setupCatalogs(catalog) {
      val result = UCCommitCoordinatorBuilder.buildForCatalog(spark, catalogName)
      assert(result.isInstanceOf[UCCommitCoordinatorClient])

      // Verify that createUCClient was called with the converted auth config
      verify(mockFactory).createUCClient(
        meq(uri),
        any[Map[String, String]]())
    }
  }

  test("buildForCatalog with new auth.* format") {
    val catalogName = "test_catalog"
    val uri = "https://test-uri.com"
    val token = "test-token"
    val catalog = CatalogTestConfig(
      name = catalogName,
      uri = Some(uri),
      authConfig = Map("type" -> "static", "token" -> token),
      useLegacyAuth = false)

    setupCatalogs(catalog) {
      val result = UCCommitCoordinatorBuilder.buildForCatalog(spark, catalogName)
      assert(result.isInstanceOf[UCCommitCoordinatorClient])

      verify(mockFactory).createUCClient(
        meq(uri),
        any[Map[String, String]]())
    }
  }

  test("buildForCatalog with non-existent catalog") {
    val exception = intercept[IllegalArgumentException] {
      UCCommitCoordinatorBuilder.buildForCatalog(spark, "non_existent_catalog")
    }
    assert(exception.getMessage.contains("not found"))
  }

  private def registerMetastoreId(
      uri: String,
      authConfig: Map[String, String],
      metastoreId: String): Unit = {
    val mockClient = org.mockito.Mockito.mock(classOf[UCClient])
    when(mockClient.getMetastoreId).thenReturn(metastoreId)
    when(mockFactory.createUCClient(meq(uri), meq(authConfig))).thenReturn(mockClient)
  }

  private def registerMetastoreIdException(
      uri: String,
      authConfig: Map[String, String],
      exception: Throwable): Unit = {
    val mockClient = org.mockito.Mockito.mock(classOf[UCClient])
    when(mockClient.getMetastoreId).thenThrow(exception)
    when(mockFactory.createUCClient(meq(uri), meq(authConfig))).thenReturn(mockClient)
  }

  private def getCommitCoordinatorClient(metastoreId: String) = {
    CommitCoordinatorProvider.getCommitCoordinatorClient(
      UCCommitCoordinatorBuilder.getName,
      Map(UCCommitCoordinatorClient.UC_METASTORE_ID_KEY -> metastoreId),
      spark)
  }
}
