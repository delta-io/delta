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

package org.apache.spark.sql.delta.skipping.clustering

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaErrors, Snapshot}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils

import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * A wrapper class that stores a clustering column's physical name parts.
 */
case class ClusteringColumn(physicalName: Seq[String])

object ClusteringColumn {
  /**
   * Note: `logicalName` must be validated to exist in the given `schema`.
   */
  def apply(schema: StructType, logicalName: String): ClusteringColumn = {
    val resolver = SchemaUtils.DELTA_COL_RESOLVER
    // Note that we use AttributeNameParser instead of CatalystSqlParser to account for the case
    // where the column name is a backquoted string with spaces.
    val logicalNameParts = FieldReference(logicalName).fieldNames
    val physicalNameParts = logicalNameParts.foldLeft[(DataType, Seq[String])]((schema, Nil)) {
      (partial, namePart) =>
        val (currStructType, currPhysicalNameSeq) = partial
        val field = currStructType.asInstanceOf[StructType].find(
          field => resolver(field.name, namePart)) match {
          case Some(f) => f
          case None =>
            throw DeltaErrors.columnNotInSchemaException(logicalName, schema)
        }
        (field.dataType, currPhysicalNameSeq :+ DeltaColumnMapping.getPhysicalName(field))
    }._2
    ClusteringColumn(physicalNameParts)
  }
}

/**
 * A wrapper class that stores a clustering column's physical name parts and data type.
 */
case class ClusteringColumnInfo(
    physicalName: Seq[String], dataType: DataType, schema: StructType) {
  lazy val logicalName: String = {
    val reversePhysicalNameParts = physicalName.reverse
    val resolver = SchemaUtils.DELTA_COL_RESOLVER
    val logicalNameParts =
      reversePhysicalNameParts
        .foldRight[(Seq[String], DataType)]((Nil, schema)) {
          (namePart, state) =>
            val (logicalNameParts, parentRawDataType) = state
            val parentDataType = parentRawDataType.asInstanceOf[StructType]
            val nextField =
              parentDataType
                .find(field => resolver(DeltaColumnMapping.getPhysicalName(field), namePart))
                .get
            (nextField.name +: logicalNameParts, nextField.dataType)
        }._1.reverse
    FieldReference(logicalNameParts).toString
  }
}

object ClusteringColumnInfo extends DeltaLogging {
  def apply(schema: StructType, clusteringColumn: ClusteringColumn): ClusteringColumnInfo =
    apply(schema, clusteringColumn.physicalName)

  def apply(schema: StructType, physicalName: Seq[String]): ClusteringColumnInfo = {
    val resolver = SchemaUtils.DELTA_COL_RESOLVER
    val dataType = physicalName.foldLeft[DataType](schema) {
      (currStructType, namePart) =>
        currStructType.asInstanceOf[StructType].find { field =>
          resolver(DeltaColumnMapping.getPhysicalName(field), namePart)
        }.get.dataType
    }
    ClusteringColumnInfo(physicalName, dataType, schema)
  }

  def extractLogicalNames(snapshot: Snapshot): Seq[String] = {
    ClusteredTableUtils.getClusteringColumnsOptional(snapshot).map { clusteringColumns =>
      clusteringColumns.map(ClusteringColumnInfo(snapshot.schema, _).logicalName)
    }.getOrElse(Seq.empty)
  }
}
