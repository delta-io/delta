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

import org.apache.spark.sql.types.{DataType, GeographyType, GeometryType}

/**
 * Shim for the Spark GeometryType / GeographyType catalyst types, which were introduced
 * in Spark 4.1 (SPARK-53760). This is the real implementation for Spark 4.2.
 */
object GeoTypesShim {
  /** Returns true if `dt` is `GeometryType` or `GeographyType`. */
  def isGeoSpatialType(dt: DataType): Boolean = dt match {
    case _: GeometryType | _: GeographyType => true
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
}
