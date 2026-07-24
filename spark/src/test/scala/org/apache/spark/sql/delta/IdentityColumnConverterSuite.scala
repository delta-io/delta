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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.sources.DeltaSourceUtils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.catalog.{Column => V2Column, IdentityColumnSpec}
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType}

/**
 * Direct unit coverage for the two IDENTITY converters that Delta's V2 catalog wires up.
 *
 * Two converters exist because Spark 4.0+ carries IDENTITY information in two disjoint shapes
 * depending on the entry point:
 *
 *   - The V2 `Column[]` overloads of `createTable`/`stageCreate`/`stageReplace`/
 *     `stageCreateOrReplace` receive `Column.identityColumnSpec()` on the column object;
 *     `CatalogV2Util.v2ColumnsToStructType` ignores that slot, so Delta has its own
 *     `columnsToStructTypeWithIdentity` that reads the spec and writes `delta.identity.*`.
 *
 *   - The `StructType` overload of `createDeltaTable` receives a schema where the SQL parser's
 *     `ColumnDefinition.toV1Column` path has already encoded the spec into StructField metadata
 *     as `identity.start` / `identity.step` / `identity.allowExplicitInsert`;
 *     `convertSparkIdentityMetadata` remaps those into the `delta.identity.*` keys.
 *
 * These tests pin each converter to its respective input shape so that deleting either one
 * causes a targeted failure here -- the existing end-to-end SQL DDL suite only exercises the
 * V2 `Column[]` path and would not detect removal of `convertSparkIdentityMetadata` (or
 * vice versa).
 */
class IdentityColumnConverterSuite extends SparkFunSuite {

  private def assertHasDeltaIdentity(
      field: StructField,
      start: Long,
      step: Long,
      allowExplicitInsert: Boolean): Unit = {
    assert(field.dataType === LongType)
    val md = field.metadata
    assert(md.getLong(DeltaSourceUtils.IDENTITY_INFO_START) === start)
    assert(md.getLong(DeltaSourceUtils.IDENTITY_INFO_STEP) === step)
    assert(
      md.getBoolean(DeltaSourceUtils.IDENTITY_INFO_ALLOW_EXPLICIT_INSERT)
        === allowExplicitInsert)
    // Spark's own identity keys must never leak through into committed Delta metadata.
    assert(!md.contains("identity.start"))
    assert(!md.contains("identity.step"))
    assert(!md.contains("identity.allowExplicitInsert"))
  }

  // ---------------------------------------------------------------------------------------------
  // convertSparkIdentityMetadata: input = StructType with Spark's identity.* keys (V1 shape).
  // Exercised at runtime by AbstractDeltaCatalog.createDeltaTable.
  // ---------------------------------------------------------------------------------------------

  private def fieldWithSparkIdentity(
      name: String,
      dt: org.apache.spark.sql.types.DataType,
      start: Long,
      step: Long,
      allowExplicitInsert: Boolean): StructField = {
    val md = new MetadataBuilder()
      .putLong("identity.start", start)
      .putLong("identity.step", step)
      .putBoolean("identity.allowExplicitInsert", allowExplicitInsert)
      .build()
    StructField(name, dt, nullable = true, md)
  }

  test("convertSparkIdentityMetadata: rewrites identity.* into delta.identity.*" +
    "and strips originals") {
    val schema = StructType(Seq(
      StructField("plain", StringType),
      fieldWithSparkIdentity("id", LongType, start = 10L, step = 5L, allowExplicitInsert = true)))
    val out = IdentityColumn.convertSparkIdentityMetadata(schema)
    assert(out("plain") === StructField("plain", StringType))
    assertHasDeltaIdentity(out("id"), start = 10L, step = 5L, allowExplicitInsert = true)
  }

  test("convertSparkIdentityMetadata: no-op when no Spark identity keys are present") {
    val schema = StructType(Seq(StructField("a", LongType), StructField("b", StringType)))
    assert(IdentityColumn.convertSparkIdentityMetadata(schema) eq schema)
  }

  test("convertSparkIdentityMetadata: rejects non-BIGINT identity column") {
    val schema = StructType(Seq(
      fieldWithSparkIdentity(
        "id", IntegerType, start = 1L, step = 1L, allowExplicitInsert = false)))
    intercept[DeltaUnsupportedOperationException] {
      IdentityColumn.convertSparkIdentityMetadata(schema)
    }
  }

  test("convertSparkIdentityMetadata: rejects step == 0") {
    val schema = StructType(Seq(
      fieldWithSparkIdentity("id", LongType, start = 1L, step = 0L, allowExplicitInsert = false)))
    intercept[DeltaAnalysisException] {
      IdentityColumn.convertSparkIdentityMetadata(schema)
    }
  }

  // ---------------------------------------------------------------------------------------------
  // columnsToStructTypeWithIdentity: input = V2 Column[] carrying identityColumnSpec().
  // Exercised at runtime by AbstractDeltaCatalog.createTable(ident, Column[], ...) and the three
  // staging overloads. Spark's CatalogV2Util.v2ColumnsToStructType would drop the spec, so this
  // converter is the sole bridge from V2 identity columns to Delta's `delta.identity.*` metadata.
  // ---------------------------------------------------------------------------------------------

  test("columnsToStructTypeWithIdentity: extracts V2 identityColumnSpec into delta.identity.*") {
    val cols: Array[V2Column] = Array(
      V2Column.create("plain", StringType),
      V2Column.create(
        "id",
        LongType,
        /* nullable = */ false,
        /* comment = */ null,
        new IdentityColumnSpec(7L, 3L, true), null))
    val out = IdentityColumn.columnsToStructTypeWithIdentity(cols)
    assert(out("plain").dataType === StringType)
    assert(!out("plain").metadata.contains(DeltaSourceUtils.IDENTITY_INFO_START))
    assertHasDeltaIdentity(out("id"), start = 7L, step = 3L, allowExplicitInsert = true)
  }

  test("columnsToStructTypeWithIdentity: no-op when no column has an identity spec") {
    val cols: Array[V2Column] = Array(
      V2Column.create("a", LongType),
      V2Column.create("b", StringType))
    val out = IdentityColumn.columnsToStructTypeWithIdentity(cols)
    assert(!out.exists(_.metadata.contains(DeltaSourceUtils.IDENTITY_INFO_START)))
  }

  test("columnsToStructTypeWithIdentity: rejects non-BIGINT identity column") {
    val cols: Array[V2Column] = Array(
      V2Column.create(
        "id", IntegerType, false, null, new IdentityColumnSpec(1L, 1L, false), null))
    intercept[DeltaUnsupportedOperationException] {
      IdentityColumn.columnsToStructTypeWithIdentity(cols)
    }
  }

  test("columnsToStructTypeWithIdentity: rejects step == 0") {
    val cols: Array[V2Column] = Array(
      V2Column.create(
        "id", LongType, false, null, new IdentityColumnSpec(1L, 0L, false), null))
    intercept[DeltaAnalysisException] {
      IdentityColumn.columnsToStructTypeWithIdentity(cols)
    }
  }
}
