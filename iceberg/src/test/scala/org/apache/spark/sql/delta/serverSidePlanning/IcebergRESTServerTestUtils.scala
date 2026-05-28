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

import scala.jdk.CollectionConverters._

import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.spark.sql.{Row, SparkSession}
import shadedForDelta.org.apache.iceberg.{PartitionSpec, Table}
import shadedForDelta.org.apache.iceberg.catalog._
import shadedForDelta.org.apache.iceberg.rest.IcebergRESTServer

/**
 * Shared test utilities for IcebergRESTServer-based tests.
 *
 * Provides helper methods for:
 * - Starting and verifying IcebergRESTServer instances
 * - Managing table lifecycle with guaranteed cleanup
 * - Populating test data with consistent schemas
 */
object IcebergRESTServerTestUtils {

  /**
   * Starts an IcebergRESTServer on a dynamic port and verifies it's reachable.
   *
   * @return Started and verified IcebergRESTServer instance
   * @throws IllegalStateException if server fails to start or become reachable
   */
  def startServer(): IcebergRESTServer = {
    val config = Map(IcebergRESTServer.REST_PORT -> "0").asJava
    val newServer = new IcebergRESTServer(config)
    newServer.start(/* join = */ false)
    if (!isServerReachable(newServer)) {
      throw new IllegalStateException("Failed to start IcebergRESTServer")
    }
    newServer
  }

  /**
   * Checks if an IcebergRESTServer is reachable via HTTP.
   *
   * Makes a GET request to /v1/config endpoint to verify server is responding.
   *
   * @param server The IcebergRESTServer to check
   * @return true if server returns 200 OK, false otherwise
   */
  def isServerReachable(server: IcebergRESTServer): Boolean = {
    val httpHeaders = Map(
      HttpHeaders.ACCEPT -> ContentType.APPLICATION_JSON.getMimeType,
      HttpHeaders.CONTENT_TYPE -> ContentType.APPLICATION_JSON.getMimeType
    ).map { case (k, v) => new BasicHeader(k, v) }.toSeq.asJava

    val httpClient = HttpClientBuilder.create()
      .setDefaultHeaders(httpHeaders)
      .build()

    try {
      val httpGet = new HttpGet(s"http://localhost:${server.getPort}/v1/config")
      val httpResponse = httpClient.execute(httpGet)
      try {
        val statusCode = httpResponse.getStatusLine.getStatusCode
        statusCode == 200
      } finally {
        httpResponse.close()
      }
    } finally {
      httpClient.close()
    }
  }

  /**
   * Executes a function with a temporary table, guaranteeing cleanup.
   *
   * @param catalog The Iceberg catalog to use
   * @param namespace The namespace for the table
   * @param tableName The table name
   * @param schema The table schema
   * @param spec The partition spec
   * @param server Optional server to clear captures after cleanup
   * @param func Function to execute with the created table
   * @return The result of executing func
   */
  def withTempTable[T](
      catalog: Catalog,
      namespace: Namespace,
      tableName: String,
      schema: shadedForDelta.org.apache.iceberg.Schema,
      spec: PartitionSpec,
      server: Option[IcebergRESTServer] = None
  )(func: Table => T): T = {
    val tableId = TableIdentifier.of(namespace, tableName)
    val table = catalog.createTable(tableId, schema, spec)
    try {
      func(table)
    } finally {
      catalog.dropTable(tableId, false)
      server.foreach(_.clearCaptured())
    }
  }

  /**
   * Populates an Iceberg table with test data.
   *
   * Creates 250 rows of test data using TestSchemas.sparkSchema, distributed
   * across 2 partitions to create 2 data files.
   *
   * @param spark The SparkSession to use
   * @param tableName The fully-qualified table name (e.g., "catalog.db.table")
   */
  def populateTestData(spark: SparkSession, tableName: String): Unit = {
    // scalastyle:off sparkimplicits
    import spark.implicits._
    // scalastyle:on sparkimplicits

    val data = spark.sparkContext.parallelize(0 until 250, numSlices = 2)
      .map(i => Row(
        i, // intCol
        i.toLong, // longCol
        i * 10.0, // doubleCol
        i.toFloat, // floatCol
        s"test_$i", // stringCol
        i % 2 == 0, // boolCol
        BigDecimal(i).bigDecimal, // decimalCol
        java.sql.Date.valueOf("2024-01-01"), // dateCol
        java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), // timestampCol
        java.sql.Date.valueOf("2024-01-01"), // localDateCol
        java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), // localDateTimeCol
        java.sql.Timestamp.valueOf("2024-01-01 00:00:00"), // instantCol
        Row(i * 100), // address.intCol (nested struct)
        Row(s"meta_$i"), // metadata.stringCol (nested struct)
        Row(s"child_$i"), // parent.`child.name` (nested struct with dotted field name)
        s"city_$i", // address.city (literal top-level dotted column)
        s"abc_$i" // a.b.c (literal top-level dotted column)
      ))

    spark.createDataFrame(data, TestSchemas.sparkSchema)
      .write
      .format("iceberg")
      .mode("append")
      .save(tableName)
  }
}
