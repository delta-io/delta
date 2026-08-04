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

import org.apache.spark.sql.types._

/**
 * Spark 4.1 GeoSpatial suite. The shared assertions live in [[DeltaGeoSuiteBase]]
 * (`scala-shims/spark-4.1-4.2`); this subclass adds the 4.1-only coverage.
 *
 * On OSS Spark 4.1 the Parquet writer cannot encode `GeometryType`/`GeographyType`
 * (`UnsupportedDataType GeometryType(...)` from `ParquetWriteSupport.makeWriter`), so the
 * end-to-end DML/CDF/MERGE read-write tests only exist in the Spark 4.2 subclass. Here the
 * CONVERT TO DELTA rejection is covered at the validation-hook level instead.
 */
class DeltaGeoSuite extends DeltaGeoSuiteBase {

  test("CONVERT TO DELTA fails when the schema contains geo types " +
    "(failIfSchemaHasGeoColumn unit check)") {
    // This is the unit-level check that `ConvertToDeltaCommandBase.validateConvert` runs on
    // the parquet table's schema (see `ConvertToDeltaCommand.scala`). The end-to-end SQL
    // version (CREATE TABLE USING parquet AS SELECT ... ST_GeomFromWKB; CONVERT TO DELTA)
    // lives only in the Spark 4.2 shim because OSS Spark 4.1's Parquet writer cannot encode
    // `GeometryType`/`GeographyType` (`UnsupportedDataType GeometryType(...)` from
    // `ParquetWriteSupport.makeWriter`). On 4.1 we test the validation hook directly so the
    // check stays deterministic across Spark versions.
    val schema = new StructType()
      .add("id", IntegerType)
      .add("g", GeographyType(DefaultSrid))
    val ex = intercept[Throwable] {
      DeltaGeoSpatial.failIfSchemaHasGeoColumn(schema, "CONVERT TO DELTA")
    }
    assert(ex.getMessage.contains("CONVERT TO DELTA"),
      s"Expected message to mention CONVERT TO DELTA, got: ${ex.getMessage}")
  }
}
