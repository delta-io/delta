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

package org.apache.spark.sql.delta.catalog

import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.types.StructType

/**
 * Helper for unified-catalog V2 CREATE TABLE routing.
 *
 * This isolates Spark metastore registration (CatalogTable construction +
 * SessionCatalog.createTable) so the unified Java catalog can call into Kernel for the commit but
 * still register table metadata in the Spark catalog with V1-compatible semantics.
 */
object V2CreateTableHelper {

  /**
   * Build a Spark CatalogTable descriptor and the resolved table path string to commit to.
   *
   * The returned CatalogTable is not registered yet.
   */
  def buildCatalogTableSpec(
      spark: SparkSession,
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      allTableProperties: util.Map[String, String]): (CatalogTable, String) = {
    require(spark != null, "spark is null")
    require(ident != null, "ident is null")
    require(schema != null, "schema is null")
    require(partitions != null, "partitions is null")
    require(allTableProperties != null, "allTableProperties is null")

    val tableProperties = filterTableProperties(allTableProperties)
    val commentOpt = Option(allTableProperties.get(TableCatalog.PROP_COMMENT))

    val locationOpt = Option(allTableProperties.get(TableCatalog.PROP_LOCATION))
    val isManagedLocation =
      Option(allTableProperties.get(TableCatalog.PROP_IS_MANAGED_LOCATION))
        .exists(_.equalsIgnoreCase("true"))

    val db =
      ident.namespace().lastOption.getOrElse(spark.sessionState.catalog.getCurrentDatabase)
    val tableIdent = TableIdentifier(ident.name(), Some(db))

    if (spark.sessionState.catalog.tableExists(tableIdent)) {
      throw new org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException(ident)
    }

    // For managed tables without explicit LOCATION, Spark uses the warehouse default.
    val locUri =
      locationOpt
        .map(CatalogUtils.stringToURI)
        .getOrElse(spark.sessionState.catalog.defaultTablePath(tableIdent))

    val tableType = if (locationOpt.isEmpty || isManagedLocation) {
      CatalogTableType.MANAGED
    } else {
      CatalogTableType.EXTERNAL
    }

    // Metadata-only create: no write options yet (OPTIONS support can be added later).
    val storage = DataSource
      .buildStorageFormatFromOptions(Map.empty)
      .copy(locationUri = Some(locUri))

    val partitionColumns = convertIdentityPartitionColumns(partitions)

    val tableDesc = new CatalogTable(
      identifier = tableIdent,
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = Some(DeltaSourceUtils.ALT_NAME),
      partitionColumnNames = partitionColumns,
      properties = tableProperties,
      comment = commentOpt
    )

    val tablePath = new Path(locUri).toString
    (tableDesc, tablePath)
  }

  /** Register the provided CatalogTable in Spark's SessionCatalog. */
  def registerTable(spark: SparkSession, tableDesc: CatalogTable): Unit = {
    require(spark != null, "spark is null")
    require(tableDesc != null, "tableDesc is null")

    spark.sessionState.catalog.createTable(
      tableDesc,
      ignoreIfExists = false,
      validateLocation = false)
  }

  private def convertIdentityPartitionColumns(partitions: Array[Transform]): Seq[String] = {
    partitions.toSeq.map {
      case IdentityTransform(FieldReference(Seq(col))) => col
      case other =>
        throw new UnsupportedOperationException(
          s"Partitioning by expressions is not supported: ${other.name()}")
    }
  }

  /**
   * Match V1 property filtering (see AbstractDeltaCatalog#createDeltaTable).
   *
   * These keys are Spark catalog properties that shouldn't be persisted as Delta table properties.
   */
  private def filterTableProperties(
      allTableProperties: util.Map[String, String]): Map[String, String] = {
    allTableProperties.asScala
      .filter { case (k, _) =>
        k match {
          case TableCatalog.PROP_LOCATION => false
          case TableCatalog.PROP_PROVIDER => false
          case TableCatalog.PROP_COMMENT => false
          case TableCatalog.PROP_OWNER => false
          case TableCatalog.PROP_EXTERNAL => false
          case "path" => false
          case "option.path" => false
          case _ => true
        }
      }
      .toMap
  }
}

