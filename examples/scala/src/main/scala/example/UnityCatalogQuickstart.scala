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

package example

import io.unitycatalog.client.ApiClientBuilder
import io.unitycatalog.client.api.{CatalogsApi, SchemasApi}
import io.unitycatalog.client.auth.TokenProvider
import io.unitycatalog.client.model.{CreateCatalog, CreateSchema}
import io.unitycatalog.server.UnityCatalogServer
import io.unitycatalog.server.utils.ServerProperties
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File
import java.net.ServerSocket
import java.nio.file.Files
import java.util.Properties
import scala.collection.JavaConverters._

/**
 * Example of testing streaming read from UC managed table with OSS UC 
 */
object UnityCatalogQuickstart {

  private val StaticToken = "static-token"

  def main(args: Array[String]): Unit = {
    val serverDir = Files.createTempDirectory("uc-integration-test-").toFile
    val tableDir = Files.createTempDirectory("uc-table-location-").toFile

    val port = {
      val socket = new ServerSocket(0)
      val p = socket.getLocalPort
      socket.close()
      p
    }

    val serverProps = new Properties()
    serverProps.setProperty("server.env", "test")
    serverProps.setProperty("server.managed-table.enabled", "true")
    serverProps.setProperty("storage-root.tables", new File(serverDir, "ucroot").getAbsolutePath)

    val server = UnityCatalogServer.builder()
      .port(port)
      .serverProperties(new ServerProperties(serverProps))
      .build()
    server.start()

    val serverUri = s"http://localhost:$port/"

    try {
      waitForServer(serverUri)
      createCatalogAndSchema(serverUri)
      runDeltaWorkload(serverUri, tableDir)
      println("SUCCESS: Unity Catalog + Delta integration test passed")
    } finally {
      server.stop()
      FileUtils.deleteQuietly(serverDir)
      FileUtils.deleteQuietly(tableDir)
    }
  }

  private def waitForServer(serverUri: String): Unit = {
    var ready = false
    var retries = 0
    while (!ready && retries < 30) {
      try {
        new CatalogsApi(createApiClient(serverUri)).listCatalogs(null, null)
        ready = true
      } catch {
        case _: Exception =>
          Thread.sleep(500)
          retries += 1
      }
    }
    if (!ready) {
      throw new RuntimeException("Unity Catalog server did not become ready within 15 seconds")
    }
  }

  private def createCatalogAndSchema(serverUri: String): Unit = {
    val client = createApiClient(serverUri)
    new CatalogsApi(client).createCatalog(
      new CreateCatalog().name("unity").comment("Integration test catalog"))
    new SchemasApi(client).createSchema(
      new CreateSchema().name("default").catalogName("unity"))
  }

  private def runDeltaWorkload(serverUri: String, tableDir: File): Unit = {
    val spark = SparkSession.builder()
      .appName("UC Delta Integration Test")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog")
      .config("spark.sql.catalog.unity.uri", serverUri)
      .config("spark.sql.catalog.unity.token", StaticToken)
      .getOrCreate()

    try {
      val tablePath = new File(tableDir, "test_table").getAbsolutePath
      val checkpointPath = new File(tableDir, "checkpoint").getAbsolutePath

      spark.sql(
        s"""CREATE TABLE unity.default.test_table (id BIGINT, data STRING)
           |USING DELTA
           |LOCATION '$tablePath'""".stripMargin)

      spark.sql("INSERT INTO unity.default.test_table VALUES (1, 'hello'), (2, 'world')")

      val stream = spark.readStream
        .table("unity.default.test_table")
        .writeStream
        .format("console")
        .option("checkpointLocation", checkpointPath)
        .start()

      stream.awaitTermination(10000)
      stream.stop()

      spark.sql("DROP TABLE IF EXISTS unity.default.test_table")
    } finally {
      spark.stop()
    }
  }

  private def createApiClient(serverUri: String) = {
    ApiClientBuilder.create()
      .uri(serverUri)
      .tokenProvider(
        TokenProvider.create(Map("type" -> "static", "token" -> StaticToken).asJava))
      .build()
  }
}
