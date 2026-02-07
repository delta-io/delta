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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.{IdentityTransform, NamedReference, Transform}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.types.StructType

/**
 * Helpers for implementing metadata-only CREATE TABLE in the unified DeltaCatalog STRICT path.
 *
 * This isolates Spark-catalyst table descriptor construction and SessionCatalog registration so the
 * Java catalog routing code stays small and readable.
 */
private[catalog] object V2CreateTableHelper {

  def buildCatalogTableSpec(
      spark: SparkSession,
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): (CatalogTable, String) = {
    val db = ident.namespace().lastOption.getOrElse(spark.sessionState.catalog.getCurrentDatabase)
    val tableIdent = TableIdentifier(ident.name(), Some(db))

    if (spark.sessionState.catalog.tableExists(tableIdent)) {
      throw new TableAlreadyExistsException(ident)
    }

    val locationOpt = Option(properties.get(TableCatalog.PROP_LOCATION))
    val isManagedLocation =
      Option(properties.get(TableCatalog.PROP_IS_MANAGED_LOCATION))
        .exists(_.equalsIgnoreCase("true"))

    val tableType =
      if (locationOpt.isEmpty || isManagedLocation) {
        CatalogTableType.MANAGED
      } else {
        CatalogTableType.EXTERNAL
      }

    val location =
      locationOpt.map(org.apache.spark.sql.catalyst.catalog.CatalogUtils.stringToURI)
        .getOrElse(spark.sessionState.catalog.defaultTablePath(tableIdent))

    val tableProperties = filterTableProperties(properties)
    val commentOpt = Option(properties.get(TableCatalog.PROP_COMMENT))

    val partitionColumnNames = toPartitionColumnNames(partitions)

    val tableDesc = CatalogTable(
      identifier = tableIdent,
      tableType = tableType,
      storage = CatalogStorageFormat.empty.copy(locationUri = Some(location)),
      schema = schema,
      provider = Some(DeltaSourceUtils.ALT_NAME),
      partitionColumnNames = partitionColumnNames,
      properties = tableProperties,
      comment = commentOpt
    )

    (tableDesc, location.toString)
  }

  def registerTable(spark: SparkSession, tableDesc: CatalogTable): Unit = {
    if (tableDesc.tableType == CatalogTableType.MANAGED) {
      // The Kernel commit creates the table directory before we register the table in Spark's
      // catalog, so location validation would fail for managed tables.
      spark.sessionState.catalog.createTable(
        tableDesc,
        ignoreIfExists = false,
        validateLocation = false)
    } else {
      spark.sessionState.catalog.createTable(tableDesc, ignoreIfExists = false)
    }
  }

  private def filterTableProperties(properties: util.Map[String, String]): Map[String, String] = {
    properties.asScala.filterKeys {
      case TableCatalog.PROP_LOCATION => false
      case TableCatalog.PROP_PROVIDER => false
      case TableCatalog.PROP_COMMENT => false
      case TableCatalog.PROP_OWNER => false
      case TableCatalog.PROP_EXTERNAL => false
      case "path" => false
      case "option.path" => false
      case _ => true
    }.toMap
  }

  private def toPartitionColumnNames(partitions: Array[Transform]): Seq[String] = {
    if (partitions == null || partitions.isEmpty) return Nil
    partitions.map {
      case identity: IdentityTransform =>
        val refs: Array[NamedReference] = identity.references()
        if (refs == null || refs.length != 1) {
          throw new IllegalArgumentException(s"Invalid partition transform: ${identity.name}")
        }
        val fieldNames = refs(0).fieldNames()
        if (fieldNames == null || fieldNames.length != 1) {
          throw new UnsupportedOperationException(
            s"Partition columns must be top-level columns: ${refs(0).describe()}")
        }
        fieldNames(0)
      case other =>
        throw new UnsupportedOperationException(
          s"Partitioning by expressions is not supported: ${other.name}")
    }.toSeq
  }
}

