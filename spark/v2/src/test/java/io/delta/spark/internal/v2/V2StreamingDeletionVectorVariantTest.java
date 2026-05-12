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

package io.delta.spark.internal.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end DSv1 + DSv2 streaming repro for the post-PR-#6578 silent-corruption bug in {@link
 * io.delta.spark.internal.v2.read.deletionvector.ColumnVectorWithFilter#getChild(int)}.
 *
 * <p>Bug: For non-Struct top-level types (e.g. VARIANT) the {@code getChild()} non-Struct branch
 * returns the unwrapped delegate child, dropping the row-id mapping. Since Spark implements {@code
 * ColumnVector.getVariant(rowId)} by calling {@code getChild(0).getBinary(rowId)} and {@code
 * getChild(1).getBinary(rowId)}, the variant value reads from the original (pre-DV-filter) row, not
 * the live row at the mapped position. Under a DV-only delete this returns the wrong variant
 * payload silently - no exception, just wrong data.
 *
 * <p>Each test exercises BOTH the DSv1 and DSv2 streaming paths over the same Delta table and
 * asserts that the two sides agree row-for-row. DSv1 is the oracle (its streaming read does not go
 * through {@code ColumnVectorWithFilter}); a V1/V2 mismatch implicates the V2 path.
 *
 * <p>The DSv1 mirror at {@code DeltaSourceDeletionVectorsSuite} only verifies "no
 * ClassCastException" - it does not assert variant values match the row identity. This file does
 * the value-level assertion at the user-visible {@code spark.readStream} level.
 *
 * <p>Companion to the unit-level test {@code ColumnVectorWithFilterTypeFanoutTest}.
 */
public class V2StreamingDeletionVectorVariantTest extends V2TestBase {

  /**
   * E2E silent-corruption repro: DV + VARIANT.
   *
   * <p>Construct each row's variant as {@code parse_json('{"row":<id>}')} so the variant value is a
   * function of the row's id. After a DV-only delete (half rows removed, file kept), every output
   * row's {@code variant_get(v,'$.row','int')} must equal its {@code id}. If the row-id mapping is
   * dropped on {@code getChild()}, variants will appear at the wrong rows and the assertion fails.
   */
  @Test
  public void testStreamingReadWithDeletionVectorAndVariant(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    // 1. Create table with DVs enabled and a VARIANT column.
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, v VARIANT) USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));

    // 2. Insert 10 rows, each with v = parse_json('{"row":<id>}') so v.row == id by construction.
    //    Coalesce(1) so all rows live in a single Parquet file - the DELETE below then produces a
    //    DV-only delete (no file rewrite), which is the path that goes through ColumnVectorWith-
    //    Filter on read.
    spark
        .range(1, 11)
        .selectExpr("cast(id as int) as id", "parse_json(concat('{\"row\":', id, '}')) as v")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // 3. DELETE half the rows via DV (id even). File is kept; only a DV is written.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    // 4. Exercise both DSv1 and DSv2 streaming reads and assert parity.
    List<Row> v1Rows =
        collectVariantStreamingRows(tablePath, "dv_variant_repro_v1", /* v2= */ false);
    List<Row> v2Rows =
        collectVariantStreamingRows(tablePath, "dv_variant_repro_v2", /* v2= */ true);

    // 5. Oracle assertions on DSv1: per-row identity variant_get(v,'$.row','int') == id.
    //    Surviving ids after DELETE id % 2 = 0: {1, 3, 5, 7, 9}.
    assertEquals(5, v1Rows.size(), () -> "Expected 5 surviving rows from V1, got " + v1Rows);
    for (Row row : v1Rows) {
      int id = row.getInt(0);
      Object vRowObj = row.get(1);
      assertNotNull(
          vRowObj,
          () -> "V1 oracle: variant_get returned NULL for id=" + id + " — variant payload missing");
      int vRow = ((Number) vRowObj).intValue();
      assertEquals(id, vRow, () -> "V1 oracle row identity failed at id=" + id);
    }

    // 6. V1 vs V2 parity. V1 is the oracle.
    assertV1V2Parity(v1Rows, v2Rows, "dv_variant_repro");
  }

  /**
   * Control: same shape WITHOUT deletion vectors. Without DVs the read path skips
   * ColumnVectorWithFilter entirely, so the bug should NOT surface here. If this control fails the
   * harness itself is broken and the DV-test result is unreliable.
   */
  @Test
  public void testStreamingReadWithVariantControl(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    // No DV property, no DELETE - table is plain. Read path does not wrap columns with
    // ColumnVectorWithFilter, so variant reads must be correct on master.
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, v VARIANT) USING delta", tablePath));

    spark
        .range(1, 11)
        .selectExpr("cast(id as int) as id", "parse_json(concat('{\"row\":', id, '}')) as v")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    List<Row> v1Rows =
        collectVariantStreamingRows(tablePath, "dv_variant_control_v1", /* v2= */ false);
    List<Row> v2Rows =
        collectVariantStreamingRows(tablePath, "dv_variant_control_v2", /* v2= */ true);

    // Oracle assertions on DSv1.
    assertEquals(10, v1Rows.size(), () -> "Expected 10 rows from V1, got " + v1Rows);
    for (Row row : v1Rows) {
      int id = row.getInt(0);
      int vRow = row.getInt(1);
      assertEquals(id, vRow, () -> "V1 control failed at id=" + id);
    }

    // V1 vs V2 parity.
    assertV1V2Parity(v1Rows, v2Rows, "dv_variant_control");
  }

  // ---------------------------------------------------------------------------
  // INTERVAL coverage note.
  //
  // ColumnVectorWithFilter.getChild() also drops the row-id mapping for CalendarIntervalType
  // (non-Struct, 3 child columns), so getInterval() reads from the wrong rows on a DV-filtered
  // batch. We attempted to repro this end-to-end via spark.readStream() but Delta OSS rejects
  // top-level interval columns: SchemaUtils.findUnsupportedDataTypesRecursively flags both
  // YearMonthIntervalType and DayTimeIntervalType as UnsupportedDataType, and CalendarIntervalType
  // is not exposed as a creatable SQL column type. There is no path through CREATE TABLE / INSERT
  // that lands an INTERVAL value into a Delta Parquet file, so the bug cannot be exercised at the
  // user-visible spark.readStream() level for INTERVAL on this codebase. The unit-level
  // ColumnVectorWithFilterTypeFanoutTest covers it directly via OnHeapColumnVector.
  // ---------------------------------------------------------------------------

  /**
   * Drives a streaming read against {@code tablePath} through either the DSv1 or DSv2 path,
   * materializes the rows into a memory sink, and projects {@code (id, variant_get(v,'$.row',
   * 'int'))} from the sink. Returns the projected rows sorted by id for direct V1/V2 comparison.
   *
   * @param v2 if true, read via {@code spark.readStream().table("dsv2.delta.`<path>`")}; otherwise
   *     {@code spark.readStream().format("delta").load(path)}.
   */
  private List<Row> collectVariantStreamingRows(String tablePath, String queryName, boolean v2)
      throws Exception {
    Dataset<Row> streamingDF =
        v2
            ? spark.readStream().table(str("dsv2.delta.`%s`", tablePath))
            : spark.readStream().format("delta").load(tablePath);
    assertTrue(streamingDF.isStreaming());

    // Drain the stream into the memory sink (SELECT * - full rows, including the VARIANT column).
    processStreamingQuery(streamingDF, queryName);

    // Project (id, variant_get(...)) from the memory sink and sort by id for stable comparison.
    List<Row> rows =
        spark
            .sql(
                "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM "
                    + queryName
                    + " ORDER BY id")
            .collectAsList();

    // Defensive: sort in-memory too, since the upstream ORDER BY does not bind to the memory sink
    // results across micro-batches in every Spark build.
    rows.sort(Comparator.comparingInt(r -> r.getInt(0)));
    return rows;
  }

  /**
   * Asserts that DSv1 and DSv2 streaming reads produced byte-identical projected rows. V1 is the
   * oracle.
   */
  private void assertV1V2Parity(List<Row> v1Rows, List<Row> v2Rows, String tag) {
    v1Rows.sort(Comparator.comparingInt(r -> r.getInt(0)));
    v2Rows.sort(Comparator.comparingInt(r -> r.getInt(0)));
    assertEquals(
        v1Rows.toString(),
        v2Rows.toString(),
        () -> tag + ": V1 vs V2 row mismatch.\nV1=" + v1Rows + "\nV2=" + v2Rows);
  }
}
