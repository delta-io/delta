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

package org.apache.spark.sql.delta.serverSidePlanning

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import shadedForDelta.org.apache.iceberg.PartitionSpec
import shadedForDelta.org.apache.iceberg.catalog._

/**
 * Tests that the IcebergRESTCatalogPlanningClient calls the tokenSupplier per-request,
 * enabling OAuth token refresh between requests.
 */
class TokenRefreshSuite extends QueryTest with SharedSparkSession {

  private val defaultNamespace = Namespace.of("testDatabase")
  private val defaultSchema = TestSchemas.testSchema
  private val defaultSpec = PartitionSpec.unpartitioned()

  private lazy val server = IcebergRESTServerTestUtils.startServer()
  private lazy val catalog = server.getCatalog()
  private lazy val serverUri = s"http://localhost:${server.getPort}"

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.conf.set(s"spark.sql.catalog.rest_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(s"spark.sql.catalog.rest_catalog.type", "rest")
    spark.conf.set(s"spark.sql.catalog.rest_catalog.uri", serverUri)

    if (catalog.isInstanceOf[SupportsNamespaces]) {
      catalog.asInstanceOf[SupportsNamespaces].createNamespace(defaultNamespace)
    } else {
      throw new IllegalStateException("Catalog does not support namespaces")
    }
  }

  override def afterAll(): Unit = {
    try {
      if (server != null) {
        server.clearCaptured()
        server.stop()
      }
    } finally {
      super.afterAll()
    }
  }

  test("tokenSupplier is called per-request, not just once at construction") {
    withTempTable("tokenRefreshTest") { table =>
      val callCount = new AtomicInteger(0)
      val tokenSupplier: () => String = () => {
        s"token-${callCount.incrementAndGet()}"
      }

      val client = new IcebergRESTCatalogPlanningClient(serverUri, "test_catalog", tokenSupplier)
      try {
        // First planScan triggers lazy httpClient init (/v1/config GET + /plan POST).
        // The interceptor calls tokenSupplier() for each HTTP request.
        client.planScan(defaultNamespace.toString, "tokenRefreshTest")
        val countAfterFirst = callCount.get()

        // The first planScan involves at least 2 HTTP requests: /v1/config and /plan
        assert(countAfterFirst >= 2,
          s"tokenSupplier should be called at least twice during first planScan " +
          s"(once for /v1/config, once for /plan), but was called $countAfterFirst times")

        // Second planScan should invoke tokenSupplier again (at least once for /plan)
        client.planScan(defaultNamespace.toString, "tokenRefreshTest")
        val countAfterSecond = callCount.get()
        assert(countAfterSecond > countAfterFirst,
          s"tokenSupplier should be called again for second planScan (fresh token per-request). " +
          s"Count after first: $countAfterFirst, after second: $countAfterSecond")
      } finally {
        client.close()
      }
    }
  }

  test("tokenSupplier returning different tokens uses latest value per-request") {
    withTempTable("dynamicTokenTest") { table =>
      val tokens = new java.util.concurrent.CopyOnWriteArrayList[String]()
      var currentToken = "initial-token"
      val tokenSupplier: () => String = () => {
        tokens.add(currentToken)
        currentToken
      }

      val client = new IcebergRESTCatalogPlanningClient(serverUri, "test_catalog", tokenSupplier)
      try {
        client.planScan(defaultNamespace.toString, "dynamicTokenTest")

        // Verify initial token was used
        assert(tokens.contains("initial-token"),
          s"Initial token should have been requested. Tokens seen: $tokens")

        // Simulate OAuth token refresh
        currentToken = "refreshed-token"
        tokens.clear()

        client.planScan(defaultNamespace.toString, "dynamicTokenTest")

        // Verify refreshed token was used for second request
        assert(tokens.contains("refreshed-token"),
          s"Refreshed token should have been requested after update. Tokens seen: $tokens")
        assert(!tokens.contains("initial-token"),
          s"Old token should not appear after refresh. Tokens seen: $tokens")
      } finally {
        client.close()
      }
    }
  }

  test("empty and null tokens do not cause errors") {
    Seq(
      ("empty string", () => ""),
      ("null", () => null)
    ).foreach { case (description, supplier) =>
      withTempTable(s"${description.replace(" ", "")}Test") { table =>
        val client = new IcebergRESTCatalogPlanningClient(serverUri, "test_catalog", supplier)
        try {
          val scanPlan = client.planScan(defaultNamespace.toString,
            s"${description.replace(" ", "")}Test")
          assert(scanPlan != null,
            s"[$description] Scan plan should not be null even without auth")
          assert(scanPlan.files != null,
            s"[$description] Scan plan files should not be null")
        } finally {
          client.close()
        }
      }
    }
  }

  private def withTempTable[T](
      tableName: String)(func: shadedForDelta.org.apache.iceberg.Table => T): T = {
    IcebergRESTServerTestUtils.withTempTable(
      catalog, defaultNamespace, tableName,
      defaultSchema, defaultSpec, Some(server)
    )(func)
  }
}
