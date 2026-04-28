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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.actions.{Action, Metadata, Protocol}
import org.apache.spark.sql.delta.schema.{SchemaUtils, UnsupportedDataTypeInfo}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

object DeltaGeoSpatial {

  /** Returns whether to enable geospatial features that are part of the preview scope. */
  def isPreviewEnabled(spark: SparkSession): Boolean =
    spark.conf.get(DeltaSQLConf.DELTA_GEO_PREVIEW_ENABLED)

  /**
   * Asserts that the given table doesn't contain any geo types if flag is disabled or protocol
   * is not supported.
   */
  def assertTableReadable(conf: SQLConf, protocol: Protocol, metadata: Metadata): Unit = {
    // if metadata contains geo column
    // but either config is disabled on protocol is not supported
    // throw exception
    if (containsGeoColumns(metadata.schema) &&
        !conf.getConf(DeltaSQLConf.DELTA_GEO_PREVIEW_ENABLED)) {
      throw DeltaErrors.geoSpatialNotSupportedException()
    }
  }

  /**
   * Returns whether the protocol version supports the GeoSpatial table feature.
   */
  def isSupported(protocol: Protocol): Boolean =
    protocol.isFeatureSupported(GeoSpatialPreviewTableFeature) ||
      protocol.isFeatureSupported(GeoSpatialTableFeature)

  /**
   * Validates that the given actions are compatible with the geospatial table feature.
   * Will throw an error if the feature is not allowed but the schema contains geospatial data.
   */
  def validateCommitActions(spark: SparkSession, protocol: Protocol, actions: Seq[Action]): Unit = {
    val hasGeoSpatialMetadata = actions.exists {
      case m: Metadata =>
        assertSridSupported(m.schema)
        containsGeoColumns(m.schema)
      case _ => false
    }

    if (hasGeoSpatialMetadata) {
      if (!isPreviewEnabled(spark) || !isSupported(protocol)) {
        throw DeltaErrors.geoSpatialNotSupportedException()
      }
    }
  }

  def isGeoSpatialType(dt: DataType): Boolean = dt match {
    case _: GeometryType | _: GeographyType => true
    case _ => false
  }

  /**
   * Find Geo columns in the table schema.
   */
  def containsGeoColumns(schema: DataType): Boolean = {
    SchemaUtils.typeExistsRecursively(schema)(dt =>
      DeltaGeoSpatial.isGeoSpatialType(dt)
    )
  }

  def assertSridSupported(schema: DataType): Unit = {
    SchemaUtils.typeExistsRecursively(schema) {
      case g: GeometryType if !GeometryType.isSridSupported(g.srid) =>
        throw new AnalysisException(
          "DELTA_GEOSPATIAL_SRID_NOT_SUPPORTED",
          Map("srid" -> g.srid.toString))
        false
      case g: GeographyType if !GeographyType.isSridSupported(g.srid) =>
        throw new AnalysisException(
          "DELTA_GEOSPATIAL_SRID_NOT_SUPPORTED",
          Map("srid" -> g.srid.toString))
        false
      case _ => false
    }
  }

  def findGeoColumnsRecursively(schema: StructType): Option[DataType] = {
    SchemaUtils.findAnyTypeRecursively(schema)(dt =>
      DeltaGeoSpatial.isGeoSpatialType(dt)
    )
  }

  /**
   * Throws not supported for columns with geospatial error if there are geospatial
   * types in the given schema.
   */
  def failIfSchemaHasGeoColumn(schema: StructType, operation: String): Unit = {
    SchemaUtils.findColumnPaths(schema)(isGeoSpatialType) match {
      case columnPaths if columnPaths.nonEmpty =>
        val dataTypeInfo = columnPaths.map(
          column => UnsupportedDataTypeInfo(column._1.toString, column._2))
        throw DeltaErrors.operationNotSupportedForDataTypes(
          operation, dataTypeInfo.head, dataTypeInfo.tail: _*
        )

      case _ =>
    }
  }
}
