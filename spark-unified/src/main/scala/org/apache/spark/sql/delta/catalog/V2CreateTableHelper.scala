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

import io.delta.spark.internal.v2.catalog.DeltaKernelStagedDDLTable

import java.net.URI
import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.{IdentityTransform, Transform}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters._

/**
 * Scala helpers for Spark-catalyst CatalogTable construction and
 * SessionCatalog registration used by the DSv2 STRICT DDL path.
 *
 * The Java routing code in DeltaCatalog delegates here because
 * CatalogTable uses Scala named/default constructor parameters.
 */
private[delta] object V2CreateTableHelper {

  def buildCatalogTableSpec(
      spark: SparkSession,
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): (CatalogTable, String) = {
    require(spark != null, "spark is null")
    require(ident != null, "ident is null")
    require(schema != null, "schema is null")
    require(partitions != null, "partitions is null")
    require(properties != null, "properties is null")

    val namespace = Option(ident.namespace()).getOrElse(Array.empty[String])
    val db =
      if (namespace.nonEmpty) namespace.last
      else spark.sessionState.catalog.getCurrentDatabase

    val tableIdent = TableIdentifier(ident.name(), Some(db))
    if (spark.sessionState.catalog.tableExists(tableIdent)) {
      throw new TableAlreadyExistsException(ident)
    }

    val location = properties.get(TableCatalog.PROP_LOCATION)
    val isManagedLocation =
      Option(properties.get(TableCatalog.PROP_IS_MANAGED_LOCATION))
        .exists(_.equalsIgnoreCase("true"))

    val tableType =
      if (location == null || location.isEmpty || isManagedLocation) CatalogTableType.MANAGED
      else CatalogTableType.EXTERNAL

    val locationUri: URI =
      if (location != null && location.nonEmpty) CatalogUtils.stringToURI(location)
      else spark.sessionState.catalog.defaultTablePath(tableIdent)

    val tableProperties: Map[String, String] =
      properties.asScala.iterator
        .collect {
          case (k, v)
              if k != null && !DeltaKernelStagedDDLTable.isSparkReservedPropertyKey(k) =>
            k -> v
        }
        .toMap

    val commentOpt = Option(properties.get(TableCatalog.PROP_COMMENT))
    val partitionColumnNames = toPartitionColumnNames(partitions)

    // Provider is always delta for this code path (validated by DeltaCatalog routing).
    val providerOpt = Some(DeltaSourceUtils.ALT_NAME)

    val storage =
      CatalogStorageFormat.empty.copy(locationUri = Some(locationUri))

    val tableDesc = new CatalogTable(
      identifier = tableIdent,
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = providerOpt,
      partitionColumnNames = partitionColumnNames,
      properties = tableProperties,
      comment = commentOpt
    )

    (tableDesc, locationUri.toString)
  }

  def registerTable(spark: SparkSession, tableDesc: CatalogTable): Unit = {
    require(spark != null, "spark is null")
    require(tableDesc != null, "tableDesc is null")

    if (tableDesc.tableType == CatalogTableType.MANAGED) {
      // The Kernel commit creates the table directory before we register the table in Spark's
      // catalog, so location validation would fail for managed tables.
      spark.sessionState.catalog.createTable(
        tableDesc,
        ignoreIfExists = false,
        validateLocation = false)
    } else {
      spark.sessionState.catalog.createTable(
        tableDesc,
        ignoreIfExists = false,
        validateLocation = true)
    }
  }

  private def toPartitionColumnNames(partitions: Array[Transform]): Seq[String] = {
    partitions.toSeq.map {
      case identity: IdentityTransform =>
        val refs = identity.references()
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
    }
  }
}

