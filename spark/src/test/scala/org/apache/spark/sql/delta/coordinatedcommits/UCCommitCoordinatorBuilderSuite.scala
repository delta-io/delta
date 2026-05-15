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
import org.mockito.{Mock, Mockito}
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, never, times, verify, when}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

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
    configMap: Map[String, String] = Map.empty,
    metastoreId: Option[String] = None,
    path: Option[String] = Some("io.unitycatalog.spark.UCSingleCatalog")
  ) {
    /**
     * The ucConfig as it would appear after getCatalogConfigs
     * parsing: all sub-keys under spark.sql.catalog.<name>.*
     * with the prefix stripped. Includes `uri` when present.
     */
    def expectedUcConfig: Map[String, String] = {
      val base = configMap
      uri.map(u => base + ("uri" -> u)).getOrElse(base)
    }
  }

  def setupCatalogs(configs: CatalogTestConfig*)(testCode: => Unit): Unit = {
    val allConfigs = configs.flatMap { config =>
      val baseConfigs = Seq(
        config.path.map(p => s"spark.sql.catalog.${config.name}" -> p),
        config.uri.map(uri => s"spark.sql.catalog.${config.name}.uri" -> uri)
      ).flatten

      val additionalConfigs = config.configMap.map { case (key, value) =>
        s"spark.sql.catalog.${config.name}.$key" -> value
      }

      baseConfigs ++ additionalConfigs
    }

    withSQLConf(allConfigs: _*) {
      configs.foreach { config =>
        (config.uri, config.configMap.isEmpty, config.metastoreId) match {
          case (Some(_), false, Some(id)) =>
            registerMetastoreId(config.expectedUcConfig, id)
          case (Some(_), false, None) =>
            registerMetastoreIdException(
              config.expectedUcConfig,
              new RuntimeException("Invalid metastore ID"))
          case _ =>
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
      configMap = Map("type" -> "static", "token" -> "test-token-1"),
      metastoreId = Some(expectedMetastoreId)
    )
    val catalog2 = CatalogTestConfig(
      name = "catalog2",
      uri = Some("https://test-uri-2.com"),
      configMap = Map("type" -> "static", "token" -> "test-token-2"),
      metastoreId = Some("different-metastore-id")
    )

    setupCatalogs(catalog1, catalog2) {
      val result = getCommitCoordinatorClient(expectedMetastoreId)

      assert(result.isInstanceOf[UCCommitCoordinatorClient])
      verify(mockFactory, times(2)).createUCClient(catalog1.expectedUcConfig)
      verify(mockFactory).createUCClient(catalog2.expectedUcConfig)
      verify(mockFactory.createUCClient(catalog1.expectedUcConfig))
        .getMetastoreId
      verify(mockFactory.createUCClient(catalog2.expectedUcConfig))
        .getMetastoreId
      verify(mockFactory.createUCClient(catalog2.expectedUcConfig)).close()
      verify(mockFactory.createUCClient(catalog1.expectedUcConfig)).close()
    }
  }

  test("token based rest client factory default app versions") {
    val defaults = UCTokenBasedRestClientFactory.defaultAppVersions
    assert(defaults("Delta") === io.delta.VERSION)
    assert(defaults("Spark") === org.apache.spark.SPARK_VERSION)
    assert(defaults("Scala") === scala.util.Properties.versionNumberString)
    assert(defaults("Java") === System.getProperty("java.version"))
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
      configMap = Map("type" -> "static", "token" -> "test-token"),
      metastoreId = Some("different-metastore-id")
    )

    setupCatalogs(catalog) {
      val exception = intercept[IllegalStateException] {
        getCommitCoordinatorClient(metastoreId)
      }
      assert(exception.getMessage.contains("No matching catalog found"))
      verify(mockFactory).createUCClient(catalog.expectedUcConfig)
      verify(mockFactory.createUCClient(catalog.expectedUcConfig)).getMetastoreId
      verify(mockFactory.createUCClient(catalog.expectedUcConfig)).close()
    }
  }

  test("build with multiple matching catalogs") {
    val metastoreId = "test-metastore-id"
    val catalog1 = CatalogTestConfig(
      name = "catalog1",
      uri = Some("https://test-uri1.com"),
      configMap = Map("type" -> "static", "token" -> "test-token-1"),
      metastoreId = Some(metastoreId)
    )
    val catalog2 = CatalogTestConfig(
      name = "catalog2",
      uri = Some("https://test-uri2.com"),
      configMap = Map("type" -> "static", "token" -> "test-token-2"),
      metastoreId = Some(metastoreId)
    )

    setupCatalogs(catalog1, catalog2) {
      val exception = intercept[IllegalStateException] {
        getCommitCoordinatorClient(metastoreId)
      }
      assert(exception.getMessage.contains("Found multiple catalogs"))
      verify(mockFactory).createUCClient(catalog1.expectedUcConfig)
      verify(mockFactory).createUCClient(catalog2.expectedUcConfig)
      verify(mockFactory.createUCClient(catalog1.expectedUcConfig))
        .getMetastoreId
      verify(mockFactory.createUCClient(catalog2.expectedUcConfig))
        .getMetastoreId
      verify(mockFactory.createUCClient(catalog1.expectedUcConfig)).close()
      verify(mockFactory.createUCClient(catalog2.expectedUcConfig)).close()
    }
  }

  test("build with mixed valid and invalid catalog configurations") {
    val expectedMetastoreId = "test-metastore-id"
    val validCatalog = CatalogTestConfig(
      name = "valid-catalog",
      uri = Some("https://valid-uri.com"),
      configMap = Map("type" -> "static", "token" -> "valid-token"),
      metastoreId = Some(expectedMetastoreId)
    )
    val invalidCatalog1 = CatalogTestConfig(
      name = "invalid-catalog-1",
      uri = Some("https://invalid-uri.com"),
      configMap = Map("type" -> "static", "token" -> "invalid-token"),
      metastoreId = None
    )
    val invalidCatalog2 = CatalogTestConfig(
      name = "invalid-catalog-2",
      uri = Some("random-uri"),
      configMap = Map("type" -> "static", "token" -> "invalid-token")
    )
    val incompleteCatalog = CatalogTestConfig(
      name = "incomplete-catalog",
      path = None
    )

    setupCatalogs(validCatalog, invalidCatalog1, invalidCatalog2, incompleteCatalog) {
      val result = getCommitCoordinatorClient(expectedMetastoreId)

      assert(result.isInstanceOf[UCCommitCoordinatorClient])
      verify(mockFactory, times(2)).createUCClient(
        validCatalog.expectedUcConfig
      )
      verify(mockFactory.createUCClient(validCatalog.expectedUcConfig),
        times(1)).close()
    }
  }

  test("build caching behavior") {
    val metastoreId = "test-metastore-id"
    val catalog = CatalogTestConfig(
      name = "catalog",
      uri = Some("https://test-uri.com"),
      configMap = Map("type" -> "static", "token" -> "test-token"),
      metastoreId = Some(metastoreId)
    )

    setupCatalogs(catalog) {
      val result1 = getCommitCoordinatorClient(metastoreId)
      val result2 = getCommitCoordinatorClient(metastoreId)
      assert(result1 eq result2)
    }
  }

  test("build with multiple catalogs pointing to the same URI, token, and metastore") {
    val metastoreId = "shared-metastore-id"
    val sharedUri = "https://shared-test-uri.com"
    val sharedConfigMap = Map("type" -> "static", "token" -> "shared-test-token")
    val sharedUcConfig = sharedConfigMap + ("uri" -> sharedUri)
    val catalog1 = CatalogTestConfig(
      name = "catalog1",
      uri = Some(sharedUri),
      configMap = sharedConfigMap,
      metastoreId = Some(metastoreId)
    )
    val catalog2 = CatalogTestConfig(
      name = "catalog2",
      uri = Some(sharedUri),
      configMap = sharedConfigMap,
      metastoreId = Some(metastoreId)
    )
    val catalog3 = CatalogTestConfig(
      name = "catalog3",
      uri = Some(sharedUri),
      configMap = sharedConfigMap,
      metastoreId = Some(metastoreId)
    )

    setupCatalogs(catalog1, catalog2, catalog3) {
      val result = getCommitCoordinatorClient(metastoreId)

      assert(result.isInstanceOf[UCCommitCoordinatorClient])
      verify(mockFactory, times(2)).createUCClient(sharedUcConfig)
      verify(mockFactory.createUCClient(sharedUcConfig)).getMetastoreId
      verify(mockFactory.createUCClient(sharedUcConfig)).close()
    }
  }

  test("build with a catalog having invalid path but valid URI and token") {
    val metastoreId = "test-metastore-id"
    val catalog = CatalogTestConfig(
      name = "invalid-path-catalog",
      uri = Some("https://test-uri.com"),
      configMap = Map("type" -> "static", "token" -> "test-token"),
      metastoreId = Some(metastoreId),
      path = Some("invalid-catalog-path")
    )

    setupCatalogs(catalog) {
      assert(UCCommitCoordinatorBuilder.getCatalogConfigs(spark).isEmpty)
      val e = intercept[IllegalStateException] {
        getCommitCoordinatorClient(metastoreId)
      }
      assert(e.getMessage.contains("No matching catalog found"))
      verify(mockFactory, never()).createUCClient(catalog.expectedUcConfig)
    }
  }

  private def registerMetastoreId(
      ucConfig: Map[String, String],
      metastoreId: String): Unit = {
    val mockClient = org.mockito.Mockito.mock(classOf[UCClient])
    when(mockClient.getMetastoreId).thenReturn(metastoreId)
    when(mockFactory.createUCClient(meq(ucConfig))).thenReturn(mockClient)
  }

  private def registerMetastoreIdException(
      ucConfig: Map[String, String],
      exception: Throwable): Unit = {
    val mockClient = org.mockito.Mockito.mock(classOf[UCClient])
    when(mockClient.getMetastoreId).thenThrow(exception)
    when(mockFactory.createUCClient(meq(ucConfig))).thenReturn(mockClient)
  }

  test("getCatalogConfigs returns all sub-keys") {
    val catalogName = "test_catalog"
    val uri = "https://test-uri.com"
    val token = "test-token"

    withSQLConf(
      s"spark.sql.catalog.$catalogName" -> "io.unitycatalog.spark.UCSingleCatalog",
      s"spark.sql.catalog.$catalogName.uri" -> uri,
      s"spark.sql.catalog.$catalogName.token" -> token
    ) {
      val configs = UCCommitCoordinatorBuilder.getCatalogConfigs(spark)
      assert(configs.length == 1)

      val (name, ucConfig) = configs.head
      assert(name == catalogName)
      assert(ucConfig("uri") == uri)
      assert(ucConfig("token") == token)
    }
  }

  test("getCatalogConfigs with new auth.* format") {
    val catalogName = "new_catalog"
    val uri = "https://test-uri.com"
    val token = "test-token"

    withSQLConf(
      s"spark.sql.catalog.$catalogName" -> "io.unitycatalog.spark.UCSingleCatalog",
      s"spark.sql.catalog.$catalogName.uri" -> uri,
      s"spark.sql.catalog.$catalogName.auth.type" -> "static",
      s"spark.sql.catalog.$catalogName.auth.token" -> token
    ) {
      val configs = UCCommitCoordinatorBuilder.getCatalogConfigs(spark)
      assert(configs.length == 1)

      val (name, ucConfig) = configs.head
      assert(name == catalogName)
      assert(ucConfig("uri") == uri)
      assert(ucConfig("auth.type") == "static")
      assert(ucConfig("auth.token") == token)
    }
  }

  test("getCatalogConfigs with nested auth.* configurations") {
    val catalogName = "oauth_catalog"
    val uri = "https://test-uri.com"

    withSQLConf(
      s"spark.sql.catalog.$catalogName" -> "io.unitycatalog.spark.UCSingleCatalog",
      s"spark.sql.catalog.$catalogName.uri" -> uri,
      s"spark.sql.catalog.$catalogName.auth.type" -> "oauth",
      s"spark.sql.catalog.$catalogName.auth.oauth.uri" -> "https://oauth.example.com",
      s"spark.sql.catalog.$catalogName.auth.oauth.client_id" -> "client123",
      s"spark.sql.catalog.$catalogName.auth.oauth.client_secret" -> "secret456"
    ) {
      val configs = UCCommitCoordinatorBuilder.getCatalogConfigs(spark)
      assert(configs.length == 1)

      val (name, ucConfig) = configs.head
      assert(name == catalogName)
      assert(ucConfig("uri") == uri)
      assert(ucConfig("auth.type") == "oauth")
      assert(ucConfig("auth.oauth.uri") == "https://oauth.example.com")
      assert(ucConfig("auth.oauth.client_id") == "client123")
      assert(ucConfig("auth.oauth.client_secret") == "secret456")
    }
  }

  test("getCatalogConfigs skips catalog without uri") {
    val catalogName = "no_uri_catalog"

    withSQLConf(
      s"spark.sql.catalog.$catalogName" -> "io.unitycatalog.spark.UCSingleCatalog",
      s"spark.sql.catalog.$catalogName.token" -> "some-token"
    ) {
      val configs = UCCommitCoordinatorBuilder.getCatalogConfigs(spark)
      assert(configs.isEmpty, "Catalog without uri should be skipped")
    }
  }

  test("getCatalogConfigs prefers auth.* keys (both present)") {
    val catalogName = "mixed_catalog"
    val uri = "https://test-uri.com"
    val legacyToken = "legacy-token"
    val newToken = "new-token"

    withSQLConf(
      s"spark.sql.catalog.$catalogName" -> "io.unitycatalog.spark.UCSingleCatalog",
      s"spark.sql.catalog.$catalogName.uri" -> uri,
      s"spark.sql.catalog.$catalogName.token" -> legacyToken,
      s"spark.sql.catalog.$catalogName.auth.type" -> "static",
      s"spark.sql.catalog.$catalogName.auth.token" -> newToken
    ) {
      val configs = UCCommitCoordinatorBuilder.getCatalogConfigs(spark)
      assert(configs.length == 1)

      val (_, ucConfig) = configs.head
      assert(ucConfig("uri") == uri)
      assert(ucConfig("auth.type") == "static")
      assert(ucConfig("auth.token") == newToken)
      assert(ucConfig("token") == legacyToken)

      val catalogConfig = UCCatalogConfig(catalogName, ucConfig)
      assert(catalogConfig.authConfig("type") == "static")
      assert(catalogConfig.authConfig("token") == newToken)
    }
  }

  test("getCatalogConfigs handles multiple catalogs with mixed formats") {
    withSQLConf(
      "spark.sql.catalog.catalog1" -> "io.unitycatalog.spark.UCSingleCatalog",
      "spark.sql.catalog.catalog1.uri" -> "https://uri1.com",
      "spark.sql.catalog.catalog1.token" -> "token1",
      "spark.sql.catalog.catalog2" -> "io.unitycatalog.spark.UCSingleCatalog",
      "spark.sql.catalog.catalog2.uri" -> "https://uri2.com",
      "spark.sql.catalog.catalog2.auth.type" -> "static",
      "spark.sql.catalog.catalog2.auth.token" -> "token2",
      "spark.sql.catalog.catalog3" -> "io.unitycatalog.spark.UCSingleCatalog"
    ) {
      val configs = UCCommitCoordinatorBuilder.getCatalogConfigs(spark)
      assert(configs.length == 2)

      val catalog1 = configs.find(_._1 == "catalog1")
      assert(catalog1.isDefined)
      assert(catalog1.get._2("uri") == "https://uri1.com")
      assert(catalog1.get._2("token") == "token1")

      val catalog2 = configs.find(_._1 == "catalog2")
      assert(catalog2.isDefined)
      assert(catalog2.get._2("uri") == "https://uri2.com")
      assert(catalog2.get._2("auth.type") == "static")
      assert(catalog2.get._2("auth.token") == "token2")
    }
  }

  test("extractAuthConfig prefers auth.* over legacy token") {
    val ucConfig = Map(
      "uri" -> "https://test.com",
      "token" -> "legacy-token",
      "auth.type" -> "static",
      "auth.token" -> "new-token"
    )
    val auth = UCTokenBasedRestClientFactory.extractAuthConfig(ucConfig)
    assert(auth("type") == "static")
    assert(auth("token") == "new-token")
  }

  test("extractAuthConfig falls back to legacy token") {
    val ucConfig = Map(
      "uri" -> "https://test.com",
      "token" -> "legacy-token"
    )
    val auth = UCTokenBasedRestClientFactory.extractAuthConfig(ucConfig)
    assert(auth("type") == "static")
    assert(auth("token") == "legacy-token")
  }

  test("buildForCatalog with legacy token format") {
    val catalogName = "test_catalog"
    val uri = "https://test-uri.com"
    val token = "test-token"

    withSQLConf(
      s"spark.sql.catalog.$catalogName" -> "io.unitycatalog.spark.UCSingleCatalog",
      s"spark.sql.catalog.$catalogName.uri" -> uri,
      s"spark.sql.catalog.$catalogName.token" -> token
    ) {
      val result = UCCommitCoordinatorBuilder.buildForCatalog(spark, catalogName)
      assert(result.isInstanceOf[UCCommitCoordinatorClient])

      verify(mockFactory).createUCClient(
        any[Map[String, String]]()
      )
    }
  }

  test("buildForCatalog with new auth.* format") {
    val catalogName = "test_catalog"
    val uri = "https://test-uri.com"
    val token = "test-token"

    withSQLConf(
      s"spark.sql.catalog.$catalogName" -> "io.unitycatalog.spark.UCSingleCatalog",
      s"spark.sql.catalog.$catalogName.uri" -> uri,
      s"spark.sql.catalog.$catalogName.auth.type" -> "static",
      s"spark.sql.catalog.$catalogName.auth.token" -> token
    ) {
      val result = UCCommitCoordinatorBuilder.buildForCatalog(spark, catalogName)
      assert(result.isInstanceOf[UCCommitCoordinatorClient])

      verify(mockFactory).createUCClient(
        any[Map[String, String]]()
      )
    }
  }

  test("extractAppVersions merges defaults with ucConfig entries") {
    val ucConfig = Map(
      "uri" -> "https://test.com",
      "appVersions.Kernel" -> "0.7.0",
      "appVersions.Delta V2 connector" -> "true"
    )
    val versions = UCTokenBasedRestClientFactory.extractAppVersions(ucConfig)
    assert(versions("Delta") === io.delta.VERSION)
    assert(versions("Spark") === org.apache.spark.SPARK_VERSION)
    assert(versions("Kernel") === "0.7.0")
    assert(versions("Delta V2 connector") === "true")
  }

  test("buildForCatalog with non-existent catalog") {
    val exception = intercept[IllegalArgumentException] {
      UCCommitCoordinatorBuilder.buildForCatalog(spark, "non_existent_catalog")
    }
    assert(exception.getMessage.contains("not found"))
  }

  private def getCommitCoordinatorClient(metastoreId: String) = {
    CommitCoordinatorProvider.getCommitCoordinatorClient(
      UCCommitCoordinatorBuilder.getName,
      Map(UCCommitCoordinatorClient.UC_METASTORE_ID_KEY -> metastoreId),
      spark)
  }
}
