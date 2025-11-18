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

import io.delta.storage.commit.uccommitcoordinator.{UCCommitCoordinatorClient, UCTokenProvider}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, when}

class UCCommitCoordinatorBuilderSuite extends SparkFunSuite with SharedSparkSession {

  private var mockFactory: UCClientFactory = _
  private val ucCommitCoordinator = new InMemoryUCCommitCoordinator()

  override def beforeEach(): Unit = {
    super.beforeEach()
    mockFactory = mock(classOf[UCClientFactory])
    CommitCoordinatorProvider.clearAllBuilders()
    UCCommitCoordinatorBuilder.ucClientFactory = mockFactory
    UCCommitCoordinatorBuilder.clearCache()
    CommitCoordinatorProvider.registerBuilder(UCCommitCoordinatorBuilder)
  }

  case class CatalogTestConfig(
      name: String,
      uri: Option[String] = None,
      token: Option[String] = None,
      oauthUri: Option[String] = None,
      oauthClientId: Option[String] = None,
      oauthClientSecret: Option[String] = None,
      metastoreId: Option[String] = None,
      path: Option[String] = Some("io.unitycatalog.spark.UCSingleCatalog"))

  def setupCatalogs(configs: CatalogTestConfig*)(testCode: => Unit): Unit = {
    val allConfigs = configs.flatMap { config =>
      Seq(
        config.path.map(p => s"spark.sql.catalog.${config.name}" -> p),
        config.uri.map(uri => s"spark.sql.catalog.${config.name}.uri" -> uri),
        config.token.map(token => s"spark.sql.catalog.${config.name}.token" -> token),
        config.oauthUri.map(oUri => s"spark.sql.catalog.${config.name}.oauth.uri" -> oUri),
        config.oauthClientId.map(oId => s"spark.sql.catalog.${config.name}.oauth.clientId" -> oId),
        config.oauthClientSecret.map(oSec =>
          s"spark.sql.catalog.${config.name}.oauth.clientSecret" -> oSec)).flatten
    }

    withSQLConf(allConfigs: _*) {
      configs.foreach { config =>
        // Only register mocks for valid configurations that will pass UCClientParams.create
        val isValidUri = config.uri.exists(u => isValidURI(u))
        val isValidOAuthUri = config.oauthUri.forall(u => isValidURI(u))

        if (isValidUri && isValidOAuthUri) {
          (
            config.uri,
            config.token,
            config.oauthUri,
            config.oauthClientId,
            config.oauthClientSecret,
            config.metastoreId) match {
            case (Some(uri), Some(token), _, _, _, Some(id)) =>
              registerMetastoreIdForToken(uri, token, id)
            case (Some(uri), Some(token), _, _, _, None) =>
              registerMetastoreIdExceptionForToken(
                uri,
                token,
                new RuntimeException("Invalid metastore ID"))
            case (Some(uri), _, Some(oUri), Some(oId), Some(oSec), Some(id)) =>
              registerMetastoreIdForOAuth(uri, oUri, oId, oSec, id)
            case (Some(uri), _, Some(oUri), Some(oId), Some(oSec), None) =>
              registerMetastoreIdExceptionForOAuth(
                uri,
                oUri,
                oId,
                oSec,
                new RuntimeException("Invalid metastore ID"))
            case _ => // Do nothing for incomplete configs
          }
        }
      }
      testCode
    }
  }

  private def isValidURI(uri: String): Boolean = {
    try {
      new java.net.URI(uri)
      true
    } catch {
      case _: java.net.URISyntaxException => false
    }
  }

  test("build with valid configuration") {
    val expectedMetastoreId = "test-metastore-id"
    val catalog1 = CatalogTestConfig(
      name = "catalog1",
      uri = Some("https://test-uri-1.com"),
      token = Some("test-token-1"),
      metastoreId = Some(expectedMetastoreId))
    val catalog2 = CatalogTestConfig(
      name = "catalog2",
      uri = Some("https://test-uri-2.com"),
      token = Some("test-token-2"),
      metastoreId = Some("different-metastore-id"))

    setupCatalogs(catalog1, catalog2) {
      val result = getCommitCoordinatorClient(expectedMetastoreId)
      assert(result.isInstanceOf[UCCommitCoordinatorClient])
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
      token = Some("test-token"),
      metastoreId = Some("different-metastore-id"))

    setupCatalogs(catalog) {
      val exception = intercept[IllegalStateException] {
        getCommitCoordinatorClient(metastoreId)
      }
      assert(exception.getMessage.contains("No matching catalog found"))
    }
  }

  test("build with multiple matching catalogs") {
    val metastoreId = "test-metastore-id"
    val catalog1 = CatalogTestConfig(
      name = "catalog1",
      uri = Some("https://test-uri1.com"),
      token = Some("test-token-1"),
      metastoreId = Some(metastoreId))
    val catalog2 = CatalogTestConfig(
      name = "catalog2",
      uri = Some("https://test-uri2.com"),
      token = Some("test-token-2"),
      metastoreId = Some(metastoreId))

    setupCatalogs(catalog1, catalog2) {
      val exception = intercept[IllegalStateException] {
        getCommitCoordinatorClient(metastoreId)
      }
      assert(exception.getMessage.contains("Found multiple catalogs"))
    }
  }

  test("build with mixed valid and invalid catalog configurations") {
    val expectedMetastoreId = "test-metastore-id"
    val validCatalog = CatalogTestConfig(
      name = "valid-catalog",
      uri = Some("https://valid-uri.com"),
      token = Some("valid-token"),
      metastoreId = Some(expectedMetastoreId))
    val invalidCatalog1 = CatalogTestConfig(
      name = "invalid-catalog-1",
      uri = Some("https://invalid-uri.com"),
      token = Some("invalid-token"),
      metastoreId = None)
    val invalidCatalog2 = CatalogTestConfig(
      name = "invalid-catalog-2",
      uri = Some("random-uri"),
      token = Some("invalid-token"))
    val incompleteCatalog = CatalogTestConfig(
      name = "incomplete-catalog",
      path = None)

    setupCatalogs(validCatalog, invalidCatalog1, invalidCatalog2, incompleteCatalog) {
      val result = getCommitCoordinatorClient(expectedMetastoreId)
      assert(result.isInstanceOf[UCCommitCoordinatorClient])
    }
  }

  test("build caching behavior") {
    val metastoreId = "test-metastore-id"
    val catalog = CatalogTestConfig(
      name = "catalog",
      uri = Some("https://test-uri.com"),
      token = Some("test-token"),
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
    val sharedToken = "shared-test-token"
    val catalog1 = CatalogTestConfig(
      name = "catalog1",
      uri = Some(sharedUri),
      token = Some(sharedToken),
      metastoreId = Some(metastoreId))
    val catalog2 = CatalogTestConfig(
      name = "catalog2",
      uri = Some(sharedUri),
      token = Some(sharedToken),
      metastoreId = Some(metastoreId))
    val catalog3 = CatalogTestConfig(
      name = "catalog3",
      uri = Some(sharedUri),
      token = Some(sharedToken),
      metastoreId = Some(metastoreId))

    setupCatalogs(catalog1, catalog2, catalog3) {
      val result = getCommitCoordinatorClient(metastoreId)
      assert(result.isInstanceOf[UCCommitCoordinatorClient])
    }
  }

  test("build with a catalog having invalid path but valid URI and token") {
    val metastoreId = "test-metastore-id"
    val catalog = CatalogTestConfig(
      name = "invalid-path-catalog",
      uri = Some("https://test-uri.com"),
      token = Some("test-token"),
      metastoreId = Some(metastoreId),
      path = Some("invalid-catalog-path"))

    setupCatalogs(catalog) {
      assert(UCCommitCoordinatorBuilder.getCatalogConfigs(spark).isEmpty)
      val e = intercept[IllegalStateException] {
        getCommitCoordinatorClient(metastoreId)
      }
      assert(e.getMessage.contains("No matching catalog found"))
    }
  }

  test("build with OAuth configuration") {
    val metastoreId = "oauth-metastore-id"
    val catalog = CatalogTestConfig(
      name = "oauth-catalog",
      uri = Some("https://test-uri.com"),
      oauthUri = Some("https://oauth-uri.com"),
      oauthClientId = Some("client-id"),
      oauthClientSecret = Some("client-secret"),
      metastoreId = Some(metastoreId))

    setupCatalogs(catalog) {
      val result = getCommitCoordinatorClient(metastoreId)
      assert(result.isInstanceOf[UCCommitCoordinatorClient])
    }
  }

  test("build with mixed token and OAuth catalogs") {
    val metastoreId = "shared-metastore-id"
    val tokenCatalog = CatalogTestConfig(
      name = "token-catalog",
      uri = Some("https://token-uri.com"),
      token = Some("token-123"),
      metastoreId = Some(metastoreId))
    val oauthCatalog = CatalogTestConfig(
      name = "oauth-catalog",
      uri = Some("https://oauth-uri.com"),
      oauthUri = Some("https://oauth-endpoint.com"),
      oauthClientId = Some("client-id"),
      oauthClientSecret = Some("client-secret"),
      metastoreId = Some("different-metastore-id"))

    setupCatalogs(tokenCatalog, oauthCatalog) {
      val result = getCommitCoordinatorClient(metastoreId)
      assert(result.isInstanceOf[UCCommitCoordinatorClient])
    }
  }

  test("build with incomplete OAuth configuration") {
    val catalog1 = CatalogTestConfig(
      name = "incomplete-oauth-1",
      uri = Some("https://test-uri.com"),
      oauthUri = Some("https://oauth-uri.com"),
      oauthClientId = Some("client-id")
      // Missing oauthClientSecret
    )
    val catalog2 = CatalogTestConfig(
      name = "incomplete-oauth-2",
      uri = Some("https://test-uri.com"),
      oauthUri = Some("https://oauth-uri.com")
      // Missing oauthClientId and oauthClientSecret
    )

    setupCatalogs(catalog1, catalog2) {
      assert(UCCommitCoordinatorBuilder.getCatalogConfigs(spark).isEmpty)
    }
  }

  test("build with OAuth configuration and valid URI") {
    val metastoreId = "oauth-valid-metastore-id"
    val catalog = CatalogTestConfig(
      name = "oauth-catalog-valid",
      uri = Some("https://test-uri.com"),
      oauthUri = Some("https://oauth-uri.com"),
      oauthClientId = Some("client-id"),
      oauthClientSecret = Some("client-secret"),
      metastoreId = Some(metastoreId))

    setupCatalogs(catalog) {
      val configs = UCCommitCoordinatorBuilder.getCatalogConfigs(spark)
      assert(configs.nonEmpty)
      assert(configs.head._2.isInstanceOf[UCOAuthClientParams])
    }
  }

  private def registerMetastoreIdForToken(uri: String, token: String, metastoreId: String): Unit = {
    val client = new InMemoryUCClient(metastoreId, ucCommitCoordinator)
    when(mockFactory.createUCClient(uri, token)).thenReturn(client)
  }

  private def registerMetastoreIdExceptionForToken(
      uri: String,
      token: String,
      exception: Throwable): Unit = {
    when(mockFactory.createUCClient(uri, token)).thenThrow(exception)
  }

  private def registerMetastoreIdForOAuth(
      uri: String,
      oauthUri: String,
      oauthClientId: String,
      oauthClientSecret: String,
      metastoreId: String): Unit = {
    val client = new InMemoryUCClient(metastoreId, ucCommitCoordinator)
    when(mockFactory.createUCClient(
      ArgumentMatchers.eq(uri),
      ArgumentMatchers.any(classOf[UCTokenProvider]))).thenReturn(client)
  }

  private def registerMetastoreIdExceptionForOAuth(
      uri: String,
      oauthUri: String,
      oauthClientId: String,
      oauthClientSecret: String,
      exception: Throwable): Unit = {
    when(mockFactory.createUCClient(
      ArgumentMatchers.eq(uri),
      ArgumentMatchers.any(classOf[UCTokenProvider]))).thenThrow(exception)
  }

  private def getCommitCoordinatorClient(metastoreId: String) = {
    CommitCoordinatorProvider.getCommitCoordinatorClient(
      UCCommitCoordinatorBuilder.getName,
      Map(UCCommitCoordinatorClient.UC_METASTORE_ID_KEY -> metastoreId),
      spark)
  }
}
