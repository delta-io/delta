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
    token: Option[String] = None,
    metastoreId: Option[String] = None,
    path: Option[String] = Some("io.unitycatalog.connectors.spark.UCSingleCatalog")
  )

  def setupCatalogs(configs: CatalogTestConfig*)(testCode: => Unit): Unit = {
    val allConfigs = configs.flatMap { config =>
      Seq(
        config.path.map(p => s"spark.sql.catalog.${config.name}" -> p),
        config.uri.map(uri => s"spark.sql.catalog.${config.name}.uri" -> uri),
        config.token.map(token => s"spark.sql.catalog.${config.name}.token" -> token)
      ).flatten
    }

    withSQLConf(allConfigs: _*) {
      configs.foreach { config =>
        (config.uri, config.token, config.metastoreId) match {
          case (Some(uri), Some(token), Some(id)) =>
            registerMetastoreId(uri, token, id)
          case (Some(uri), Some(token), None) =>
            registerMetastoreIdException(uri, token, new RuntimeException("Invalid metastore ID"))
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
      token = Some("test-token-1"),
      metastoreId = Some(expectedMetastoreId)
    )
    val catalog2 = CatalogTestConfig(
      name = "catalog2",
      uri = Some("https://test-uri-2.com"),
      token = Some("test-token-2"),
      metastoreId = Some("different-metastore-id")
    )

    setupCatalogs(catalog1, catalog2) {
      val result = getCommitCoordinatorClient(expectedMetastoreId)

      assert(result.isInstanceOf[UCCommitCoordinatorClient])
      verify(mockFactory, times(2)).createUCClient(catalog1.uri.get, catalog1.token.get)
      verify(mockFactory).createUCClient(catalog2.uri.get, catalog2.token.get)
      verify(mockFactory.createUCClient(catalog1.uri.get, catalog1.token.get))
        .getMetastoreId
      verify(mockFactory.createUCClient(catalog2.uri.get, catalog2.token.get))
        .getMetastoreId
      verify(mockFactory.createUCClient(catalog2.uri.get, catalog2.token.get)).close()
      verify(mockFactory.createUCClient(catalog1.uri.get, catalog1.token.get)).close()
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
      metastoreId = Some("different-metastore-id")
    )

    setupCatalogs(catalog) {
      val exception = intercept[IllegalStateException] {
        getCommitCoordinatorClient(metastoreId)
      }
      assert(exception.getMessage.contains("No matching catalog found"))
      verify(mockFactory).createUCClient(catalog.uri.get, catalog.token.get)
      verify(mockFactory.createUCClient(catalog.uri.get, catalog.token.get)).getMetastoreId
      verify(mockFactory.createUCClient(catalog.uri.get, catalog.token.get)).close()
    }
  }

  test("build with multiple matching catalogs") {
    val metastoreId = "test-metastore-id"
    val catalog1 = CatalogTestConfig(
      name = "catalog1",
      uri = Some("https://test-uri1.com"),
      token = Some("test-token-1"),
      metastoreId = Some(metastoreId)
    )
    val catalog2 = CatalogTestConfig(
      name = "catalog2",
      uri = Some("https://test-uri2.com"),
      token = Some("test-token-2"),
      metastoreId = Some(metastoreId)
    )

    setupCatalogs(catalog1, catalog2) {
      val exception = intercept[IllegalStateException] {
        getCommitCoordinatorClient(metastoreId)
      }
      assert(exception.getMessage.contains("Found multiple catalogs"))
      verify(mockFactory).createUCClient(catalog1.uri.get, catalog1.token.get)
      verify(mockFactory).createUCClient(catalog2.uri.get, catalog2.token.get)
      verify(mockFactory.createUCClient(catalog1.uri.get, catalog1.token.get))
        .getMetastoreId
      verify(mockFactory.createUCClient(catalog2.uri.get, catalog2.token.get))
        .getMetastoreId
      verify(mockFactory.createUCClient(catalog1.uri.get, catalog1.token.get)).close()
      verify(mockFactory.createUCClient(catalog2.uri.get, catalog2.token.get)).close()
    }
  }

  test("build with mixed valid and invalid catalog configurations") {
    val expectedMetastoreId = "test-metastore-id"
    val validCatalog = CatalogTestConfig(
      name = "valid-catalog",
      uri = Some("https://valid-uri.com"),
      token = Some("valid-token"),
      metastoreId = Some(expectedMetastoreId)
    )
    val invalidCatalog1 = CatalogTestConfig(
      name = "invalid-catalog-1",
      uri = Some("https://invalid-uri.com"),
      token = Some("invalid-token"),
      metastoreId = None
    )
    val invalidCatalog2 = CatalogTestConfig(
      name = "invalid-catalog-2",
      uri = Some("random-uri"),
      token = Some("invalid-token")
    )
    val incompleteCatalog = CatalogTestConfig(
      name = "incomplete-catalog",
      path = None
    )

    setupCatalogs(validCatalog, invalidCatalog1, invalidCatalog2, incompleteCatalog) {
      val result = getCommitCoordinatorClient(expectedMetastoreId)

      assert(result.isInstanceOf[UCCommitCoordinatorClient])
      verify(mockFactory, times(2)).createUCClient(
        validCatalog.uri.get,
        validCatalog.token.get
      )
      verify(mockFactory.createUCClient(validCatalog.uri.get, validCatalog.token.get),
        times(1)).close()
    }
  }

  test("build caching behavior") {
    val metastoreId = "test-metastore-id"
    val catalog = CatalogTestConfig(
      name = "catalog",
      uri = Some("https://test-uri.com"),
      token = Some("test-token"),
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
    val sharedToken = "shared-test-token"
    val catalog1 = CatalogTestConfig(
      name = "catalog1",
      uri = Some(sharedUri),
      token = Some(sharedToken),
      metastoreId = Some(metastoreId)
    )
    val catalog2 = CatalogTestConfig(
      name = "catalog2",
      uri = Some(sharedUri),
      token = Some(sharedToken),
      metastoreId = Some(metastoreId)
    )
    val catalog3 = CatalogTestConfig(
      name = "catalog3",
      uri = Some(sharedUri),
      token = Some(sharedToken),
      metastoreId = Some(metastoreId)
    )

    setupCatalogs(catalog1, catalog2, catalog3) {
      val result = getCommitCoordinatorClient(metastoreId)

      assert(result.isInstanceOf[UCCommitCoordinatorClient])
      verify(mockFactory, times(2)).createUCClient(sharedUri, sharedToken)
      verify(mockFactory.createUCClient(sharedUri, sharedToken)).getMetastoreId
      verify(mockFactory.createUCClient(sharedUri, sharedToken)).close()
    }
  }

  test("build with a catalog having invalid path but valid URI and token") {
    val metastoreId = "test-metastore-id"
    val catalog = CatalogTestConfig(
      name = "invalid-path-catalog",
      uri = Some("https://test-uri.com"),
      token = Some("test-token"),
      metastoreId = Some(metastoreId),
      path = Some("invalid-catalog-path")
    )

    setupCatalogs(catalog) {
      assert(UCCommitCoordinatorBuilder.getCatalogConfigs(spark).isEmpty)
      val e = intercept[IllegalStateException] {
        getCommitCoordinatorClient(metastoreId)
      }
      assert(e.getMessage.contains("No matching catalog found"))
      verify(mockFactory, never()).createUCClient(catalog.uri.get, catalog.token.get)
    }
  }

  private def registerMetastoreId(uri: String, token: String, metastoreId: String): Unit = {
    val mockClient = org.mockito.Mockito.mock(classOf[UCClient])
    when(mockClient.getMetastoreId).thenReturn(metastoreId)
    when(mockFactory.createUCClient(uri, token)).thenReturn(mockClient)
  }

  private def registerMetastoreIdException(
      uri: String,
      token: String,
      exception: Throwable): Unit = {
    val mockClient = org.mockito.Mockito.mock(classOf[UCClient])
    when(mockClient.getMetastoreId).thenThrow(exception)
    when(mockFactory.createUCClient(uri, token)).thenReturn(mockClient)
  }

  private def getCommitCoordinatorClient(metastoreId: String) = {
    CommitCoordinatorProvider.getCommitCoordinatorClient(
      UCCommitCoordinatorBuilder.getName,
      Map(UCCommitCoordinatorClient.UC_METASTORE_ID_KEY -> metastoreId),
      spark)
  }
}
