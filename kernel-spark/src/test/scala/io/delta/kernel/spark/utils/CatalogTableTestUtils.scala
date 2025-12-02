/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.spark.utils

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.StructType

/**
 * Helpers for constructing [[CatalogTable]] instances inside Java tests.
 *
 * Spark's [[CatalogTable]] is defined in Scala and its constructor signature shifts between Spark
 * releases. Centralising the construction in Scala keeps the kernel tests insulated from those
 * binary changes and saves Java tests from manually wiring the many optional parameters.
 */
object CatalogTableTestUtils {

  /**
   * Creates a [[CatalogTable]] with configurable options.
   *
   * @param tableName table name (default: "tbl")
   * @param catalogName optional catalog name for the identifier
   * @param properties table properties (default: empty)
   * @param storageProperties storage properties (default: empty)
   * @param locationUri optional storage location URI
   * @param nullStorage if true, sets storage to null (for edge case testing)
   * @param nullStorageProperties if true, sets storage properties to null
   */
  def createCatalogTable(
      tableName: String = "tbl",
      catalogName: Option[String] = None,
      properties: java.util.Map[String, String] = new java.util.HashMap[String, String](),
      storageProperties: java.util.Map[String, String] = new java.util.HashMap[String, String](),
      locationUri: Option[java.net.URI] = None,
      nullStorage: Boolean = false,
      nullStorageProperties: Boolean = false): CatalogTable = {

    val scalaProps = ScalaUtils.toScalaMap(properties)
    val scalaStorageProps =
      if (nullStorageProperties) null else ScalaUtils.toScalaMap(storageProperties)

    val identifier = catalogName match {
      case Some(catalog) =>
        TableIdentifier(tableName, Some("default") /* database */, Some(catalog) /* catalog */ )
      case None => TableIdentifier(tableName)
    }

    val storage = if (nullStorage) {
      null
    } else {
      CatalogStorageFormat(
        locationUri = locationUri,
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = scalaStorageProps),
      schema = new StructType(),
      provider = None,
      partitionColumnNames = Seq.empty,
      bucketSpec = None,
      properties = scalaProps)
  }

  /**
   * Creates a [[CatalogTable]] with a specific catalog name in the identifier.
   * This is needed for testing Unity Catalog integration where the catalog name
   * is used to look up catalog configurations.
   */
  def catalogTableWithCatalogName(
      catalogName: String,
      tableName: String,
      properties: java.util.Map[String, String],
      storageProperties: java.util.Map[String, String],
      locationUri: java.net.URI): CatalogTable = {
    val scalaProps = ScalaUtils.toScalaMap(properties)
    val scalaStorageProps = ScalaUtils.toScalaMap(storageProperties)

    CatalogTable(
      identifier = TableIdentifier(tableName, Some("default"), Some(catalogName)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(
        locationUri = Some(locationUri),
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = scalaStorageProps),
      schema = new StructType(),
      provider = None,
      partitionColumnNames = Seq.empty,
      bucketSpec = None,
      properties = scalaProps)
  }

  /**
   * Creates a [[CatalogTable]] with storage location but no catalog name.
   * Uses the default catalog from the session's catalog manager.
   */
  def catalogTableWithLocation(
      properties: java.util.Map[String, String],
      storageProperties: java.util.Map[String, String],
      locationUri: java.net.URI): CatalogTable = {
    val scalaProps = ScalaUtils.toScalaMap(properties)
    val scalaStorageProps = ScalaUtils.toScalaMap(storageProperties)

    CatalogTable(
      identifier = TableIdentifier("tbl"),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(
        locationUri = Some(locationUri),
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = scalaStorageProps),
      schema = new StructType(),
      provider = None,
      partitionColumnNames = Seq.empty,
      bucketSpec = None,
      properties = scalaProps)
  }

  def catalogTableWithNullStorage(
      properties: java.util.Map[String, String]): CatalogTable = {
    val scalaProps = ScalaUtils.toScalaMap(properties)

    CatalogTable(
      identifier = identifier,
      tableType = CatalogTableType.MANAGED,
      storage = storage,
      schema = new StructType(),
      provider = None,
      partitionColumnNames = Seq.empty,
      bucketSpec = None,
      properties = scalaProps)
  }
}
