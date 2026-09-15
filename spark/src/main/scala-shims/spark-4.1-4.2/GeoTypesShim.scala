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

package org.apache.spark.sql.delta.shims

import org.apache.spark.sql.catalyst.expressions.st.{
  ST_AsBinary, ST_GeogFromWKB, ST_GeomFromWKB, ST_SetSrid, ST_Srid}

import org.apache.spark.sql.types.{DataType, GeographyType, GeometryType}

/**
 * Shim for the Spark GeometryType / GeographyType catalyst types, which were introduced
 * in Spark 4.1 (SPARK-53760). This is the real implementation for Spark 4.1+.
 */
object GeoTypesShim {
  /** Returns true if `dt` is `GeometryType` or `GeographyType`. */
  def isGeoSpatialType(dt: DataType): Boolean = isGeometryType(dt) || isGeographyType(dt)

  /** Returns true if `dt` is `GeometryType`. */
  def isGeometryType(dt: DataType): Boolean = dt match {
    case _: GeometryType => true
    case _ => false
  }

  /** Returns true if `dt` is `GeographyType`. */
  def isGeographyType(dt: DataType): Boolean = dt match {
    case _: GeographyType => true
    case _ => false
  }

  /**
   * If `dt` is a geospatial type with an unsupported SRID, returns `Some(srid)`. Returns
   * `None` for non-geospatial types or geospatial types with supported SRIDs.
   */
  def unsupportedSrid(dt: DataType): Option[Int] = dt match {
    case g: GeometryType if !GeometryType.isSridSupported(g.srid) => Some(g.srid)
    case g: GeographyType if !GeographyType.isSridSupported(g.srid) => Some(g.srid)
    case _ => None
  }

  /**
   * Geospatial Catalyst expression classes whitelisted for user-provided expressions
   * (e.g. generated columns, check constraints).
   */
  val geoExpressions: Set[Class[_]] = Set(
    classOf[ST_AsBinary],
    classOf[ST_GeogFromWKB],
    classOf[ST_GeomFromWKB],
    classOf[ST_SetSrid],
    classOf[ST_Srid])

  /** Geospatial Catalyst type classes (`GeometryType`, `GeographyType`). */
  val geoTypes: Seq[Class[_]] = Seq(classOf[GeometryType], classOf[GeographyType])

  /** Returns the CRS of a `GeometryType`. Only valid when `isGeometryType(dt)` is true. */
  def geometryCrs(dt: DataType): String = dt match {
    case g: GeometryType => g.crs
    case _ => throw new IllegalArgumentException(s"Not a geometry type: $dt")
  }

  /**
   * Returns the CRS and edge interpolation algorithm name (e.g. "SPHERICAL") of a
   * `GeographyType`. Only valid when `isGeographyType(dt)` is true.
   */
  def geographyCrsAndAlgorithm(dt: DataType): (String, String) = dt match {
    case g: GeographyType => (g.crs, g.algorithm.toString)
    case _ => throw new IllegalArgumentException(s"Not a geography type: $dt")
  }
}
