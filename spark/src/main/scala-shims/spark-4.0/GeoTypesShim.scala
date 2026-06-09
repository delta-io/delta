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

import org.apache.spark.sql.types.DataType

/**
 * Shim for the Spark GeometryType / GeographyType catalyst types, which were introduced
 * in Spark 4.1 (SPARK-53760). On Spark 4.0 these types do not exist, so this shim simply
 * reports that no value is ever a geospatial type. As a result the GeoSpatial table
 * feature is effectively a no-op on Spark 4.0.
 */
object GeoTypesShim {
  /** Returns true if `dt` is `GeometryType` or `GeographyType`. */
  def isGeoSpatialType(dt: DataType): Boolean = false

  /**
   * If `dt` is a geospatial type with an unsupported SRID, returns `Some(srid)`. Returns
   * `None` for non-geospatial types or geospatial types with supported SRIDs.
   */
  def unsupportedSrid(dt: DataType): Option[Int] = None

  /**
   * Geospatial Catalyst expression classes whitelisted for user-provided expressions
   * (e.g. generated columns, check constraints).
   */
  val geoExpressions: Set[Class[_]] = Set.empty[Class[_]]
}
