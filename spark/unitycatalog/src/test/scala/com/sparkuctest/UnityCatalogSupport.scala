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

package com.sparkuctest

import java.io.File
import java.net.ServerSocket
import java.nio.file.Files
import java.util.Properties

import io.unitycatalog.client.ApiClient
import io.unitycatalog.client.api.{CatalogsApi, SchemasApi}
import io.unitycatalog.client.model.{CreateCatalog, CreateSchema}
import io.unitycatalog.server.UnityCatalogServer
import io.unitycatalog.server.utils.ServerProperties

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * A trait that provides Unity Catalog server integration for Delta tests.
 *
 * This trait automatically:
 * - Starts a local Unity Catalog server before all tests
 * - Configures Spark to connect to the UC server
 * - Stops the server and cleans up after all tests
 *
 * The UC server runs with shaded dependencies (via unitycatalog-server-shaded JAR)
 * to avoid conflicts with Spark/Delta dependencies.
 *
 * Usage:
 * {{{
 * class MyUCTest extends QueryTest
 *     with SharedSparkSession
 *     with UnityCatalogSupport {
 *
 *   override protected def sparkConf: SparkConf = {
 *     configureSparkWithUnityCatalog(super.sparkConf)
 *   }
 *
 *   test("my test") {
 *     // Use unityCatalogName to reference the catalog
 *     spark.sql(s"CREATE TABLE ${unityCatalogName}.default.test_table ...")
 *   }
 * }
 * }}}
 */
trait UnityCatalogSupport extends BeforeAndAfterAll with Logging { self: Suite =>

  /**
   * The Unity Catalog server instance. Set during beforeAll().
   */
  private var ucServer: Option[UnityCatalogServer] = None

  /**
   * The port on which the UC server is running.
   */
  private var ucPort: Int = _

  /**
   * The temporary directory for UC server data.
   */
  private var ucTempDir: File = _

  /**
   * The name of the Unity Catalog in Spark's catalog registry.
   * Tests can override this if they need a different catalog name.
   */
  protected def unityCatalogName: String = "unity"

  /**
   * The URI of the Unity Catalog server.
   * Available after beforeAll() is called.
   */
  protected def unityCatalogUri: String = s"http://localhost:$ucPort"

  /**
   * The authentication token for the Unity Catalog server.
   * Currently using a test token; in production scenarios this would be more secure.
   */
  protected def unityCatalogToken: String = "not-a-token"

  /**
   * Creates a Unity Catalog API client configured for this server.
   */
  def createUnityCatalogClient(): ApiClient = {
    val port = unityCatalogUri.split(":")(2).toInt
    val client = new ApiClient()
    client.setScheme("http")
    client.setHost("localhost")
    client.setPort(port)
    client
  }

  /**
   * Finds an available port for the UC server.
   */
  private def findAvailablePort(): Int = {
    val socket = new ServerSocket(0)
    try {
      socket.getLocalPort
    } finally {
      socket.close()
    }
  }

  /**
   * Starts the Unity Catalog server before all tests.
   * IMPORTANT: Starts the server BEFORE calling super.beforeAll() to ensure
   * the server is running when SharedSparkSession creates the SparkSession.
   */
  override def beforeAll(): Unit = {
    // Create temporary directory for UC server data
    ucTempDir = Files.createTempDirectory("unity-catalog-test-").toFile
    ucTempDir.deleteOnExit()

    // Find an available port
    ucPort = findAvailablePort()

    // Set up server properties
    val serverProps = new Properties()
    serverProps.setProperty("server.env", "test")

    // Start UC server with configuration
    // The UnityCatalogServer class comes from the shaded JAR which has no dependency conflicts
    val initServerProperties = new ServerProperties(serverProps)

    val server = UnityCatalogServer.builder()
      .port(ucPort)
      .serverProperties(initServerProperties)
      .build()

    server.start()
    ucServer = Some(server)

    // Poll for server readiness by checking if we can create an API client and query catalogs
    val maxRetries = 30
    val retryDelayMs = 500
    var serverReady = false
    var retries = 0

    while (!serverReady && retries < maxRetries) {
      try {
        val testClient = new ApiClient()
        testClient.setScheme("http")
        testClient.setHost("localhost")
        testClient.setPort(ucPort)
        val catalogsApi = new CatalogsApi(testClient)
        catalogsApi.listCatalogs(null, null) // This will throw if server is not ready
        serverReady = true
      } catch {
        case _: Exception =>
          Thread.sleep(retryDelayMs)
          retries += 1
      }
    }

    if (!serverReady) {
      throw new RuntimeException(
        s"Unity Catalog server did not become ready after ${maxRetries * retryDelayMs}ms")
    }

    // Create the catalog and default schema in the UC server
    val client = new ApiClient()
    // Set scheme, host and port separately (matching TestUtils.createApiClient pattern)
    client.setScheme("http")
    client.setHost("localhost")
    client.setPort(ucPort)

    val catalogsApi = new CatalogsApi(client)
    val schemasApi = new SchemasApi(client)

    // Create catalog
    catalogsApi.createCatalog(
      new CreateCatalog()
        .name(unityCatalogName)
        .comment("Test catalog for Delta Lake integration")
    )

    // Create default schema
    schemasApi.createSchema(
      new CreateSchema()
        .name("default")
        .catalogName(unityCatalogName)
    )

    logInfo(s"Unity Catalog server started and ready at $unityCatalogUri")
    logInfo(s"Created catalog '$unityCatalogName' with schema 'default'")

    // Call super.beforeAll() AFTER starting UC server
    // This ensures the UC server is running when SharedSparkSession creates the SparkSession
    super.beforeAll()
  }

  /**
   * Stops the Unity Catalog server after all tests.
   */
  override def afterAll(): Unit = {
    try {
      ucServer.foreach { server =>
        server.stop()
        logInfo("Unity Catalog server stopped")
      }
      ucServer = None

      // Clean up temporary directory
      if (ucTempDir != null && ucTempDir.exists()) {
        deleteRecursively(ucTempDir)
      }
    } finally {
      super.afterAll()
    }
  }

  /**
   * Configures a SparkConf with Unity Catalog settings.
   *
   * This method should be called in the test's sparkConf override:
   * {{{
   * override protected def sparkConf: SparkConf = {
   *   configureSparkWithUnityCatalog(super.sparkConf)
   * }
   * }}}
   *
   * @param conf The base SparkConf to configure
   * @return The configured SparkConf with Unity Catalog settings
   */
  protected def configureSparkWithUnityCatalog(conf: SparkConf): SparkConf = {
    conf
      .set(s"spark.sql.catalog.$unityCatalogName", "io.unitycatalog.spark.UCSingleCatalog")
      .set(s"spark.sql.catalog.$unityCatalogName.uri", unityCatalogUri)
      .set(s"spark.sql.catalog.$unityCatalogName.token", unityCatalogToken)
  }

  /**
   * Recursively deletes a directory and all its contents.
   */
  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}
