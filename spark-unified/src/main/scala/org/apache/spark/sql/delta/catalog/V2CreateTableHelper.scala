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

import io.delta.spark.internal.v2.utils.{PartitionTransformUtils, SparkDDLPropertyUtils}

import java.net.URI
import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters._

/**
 * Scala helpers for Spark-catalyst CatalogTable construction used by the DSv2 STRICT DDL path.
 *
 * The Java routing code in DeltaCatalog delegates here because CatalogTable uses Scala
 * named/default constructor parameters.
 */
private[delta] object V2CreateTableHelper {

  def buildCatalogTableSpec(
      spark: SparkSession,
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): (CatalogTable, String) = {

    // Use the last namespace segment as the database name, matching Spark's V2SessionCatalog
    // behavior. This only works correctly for the session catalog (spark_catalog).
    val namespace = Option(ident.namespace()).getOrElse(Array.empty[String])
    val db =
      if (namespace.nonEmpty) namespace.last
      else spark.sessionState.catalog.getCurrentDatabase

    val tableIdent = TableIdentifier(ident.name(), Some(db))

    // Best-effort early-fail: check the catalog (metastore) before attempting the Kernel commit.
    // This is a TOCTOU check -- another session could create the table between this check and the
    // Kernel commit -- but it prevents the more common failure mode where the Kernel commit
    // succeeds (writing 000.json) and then catalog registration fails because the catalog entry
    // already exists.
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
              if k != null && !SparkDDLPropertyUtils.isSparkReservedPropertyKey(k) =>
            k -> v
        }
        .toMap

    val commentOpt = Option(properties.get(TableCatalog.PROP_COMMENT))
    val partitionColumnNames =
      PartitionTransformUtils.extractPartitionColumnNames(partitions).asScala.toSeq

    // Provider is always delta for this code path (validated by DeltaCatalog routing).
    val providerOpt = Some(DeltaSourceUtils.NAME)

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
}

