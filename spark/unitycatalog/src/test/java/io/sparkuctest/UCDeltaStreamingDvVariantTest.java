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

package io.sparkuctest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.io.TempDir;

/**
 * UC port of {@code V2StreamingDeletionVectorVariantTest}: end-to-end DSv2 streaming repro for the
 * post-PR-#6578 silent-corruption bug in {@code ColumnVectorWithFilter#getChild(int)} for VARIANT.
 *
 * <p>Each {@code @TestAllTableTypes} runs across EXTERNAL and MANAGED to expose any MANAGED-only
 * regressions in the DV+VARIANT read path.
 */
public class UCDeltaStreamingDvVariantTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  /**
   * E2E silent-corruption repro: DV + VARIANT.
   *
   * <p>For each row {@code v = parse_json('{"row":<id>}')} so {@code v.row == id} by construction.
   * After a DV-only delete (half rows removed, file kept), every output row's {@code
   * variant_get(v,'$.row','int')} must equal its {@code id}. If the row-id mapping is dropped on
   * {@code getChild()}, variants will appear at the wrong rows and the assertion fails.
   */
  @TestAllTableTypes
  public void testManagedStreamingReadWithDeletionVectorAndVariant(TableType tableType)
      throws Exception {
    withNewTable(
        "dv_variant_repro",
        "id INT, v VARIANT",
        null,
        tableType,
        "'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          // Insert 10 rows so v.row == id by construction. Coalesce(1) so they live in one file -
          // the DELETE then produces a DV-only delete (no file rewrite), exercising the
          // ColumnVectorWithFilter read path.
          spark()
              .range(1, 11)
              .selectExpr("cast(id as int) as id", "parse_json(concat('{\"row\":', id, '}')) as v")
              .coalesce(1)
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName);

          // DELETE half the rows via DV (id even). File is kept; only a DV is written.
          sql("DELETE FROM %s WHERE id %% 2 = 0", tableName);

          String queryName =
              "dv_variant_repro_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery query = null;
          try {
            query =
                spark()
                    .readStream()
                    .format("delta")
                    .table(tableName)
                    .writeStream()
                    .format("memory")
                    .queryName(queryName)
                    .outputMode("append")
                    .trigger(Trigger.AvailableNow())
                    .option("checkpointLocation", checkpoint())
                    .start();
            query.processAllAvailable();

            List<Row> rows =
                spark()
                    .sql(
                        "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM "
                            + queryName
                            + " ORDER BY id")
                    .collectAsList();

            // Surviving ids after DELETE id % 2 = 0: {1, 3, 5, 7, 9}.
            assertEquals(5, rows.size(), () -> "Expected 5 surviving rows, got " + rows);
            for (Row row : rows) {
              int id = row.getInt(0);
              Object vRowObj = row.get(1);
              assertNotNull(
                  vRowObj,
                  () -> "variant_get returned NULL for id=" + id + "; variant payload missing");
              int vRow = ((Number) vRowObj).intValue();
              assertEquals(
                  id,
                  vRow,
                  () ->
                      "Silent corruption: row id="
                          + id
                          + " has variant_get(v,'$.row','int')="
                          + vRow
                          + " (expected "
                          + id
                          + "). ColumnVectorWithFilter.getChild() likely dropped row-id mapping"
                          + " for VARIANT.");
            }
          } finally {
            if (query != null) {
              query.stop();
            }
            spark().sql("DROP VIEW IF EXISTS " + queryName);
          }
        });
  }

  /**
   * Control: same shape WITHOUT deletion vectors. Without DVs the read path skips
   * ColumnVectorWithFilter entirely, so the bug should NOT surface here. If this control fails the
   * harness itself is broken.
   */
  @TestAllTableTypes
  public void testManagedStreamingReadWithVariantControl(TableType tableType) throws Exception {
    withNewTable(
        "dv_variant_control",
        "id INT, v VARIANT",
        tableType,
        tableName -> {
          spark()
              .range(1, 11)
              .selectExpr("cast(id as int) as id", "parse_json(concat('{\"row\":', id, '}')) as v")
              .coalesce(1)
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName);

          String queryName =
              "dv_variant_control_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery query = null;
          try {
            query =
                spark()
                    .readStream()
                    .format("delta")
                    .table(tableName)
                    .writeStream()
                    .format("memory")
                    .queryName(queryName)
                    .outputMode("append")
                    .trigger(Trigger.AvailableNow())
                    .option("checkpointLocation", checkpoint())
                    .start();
            query.processAllAvailable();

            List<Row> rows =
                spark()
                    .sql(
                        "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM "
                            + queryName
                            + " ORDER BY id")
                    .collectAsList();

            assertEquals(10, rows.size());
            for (Row row : rows) {
              int id = row.getInt(0);
              int vRow = row.getInt(1);
              assertEquals(id, vRow, () -> "Control failed at id=" + id);
            }
          } finally {
            if (query != null) {
              query.stop();
            }
            spark().sql("DROP VIEW IF EXISTS " + queryName);
          }
        });
  }

  // INTERVAL coverage skipped: Delta OSS rejects top-level interval columns at CREATE TABLE
  // (SchemaUtils.findUnsupportedDataTypesRecursively flags YearMonthIntervalType and
  // DayTimeIntervalType; CalendarIntervalType is not exposed as a creatable SQL column type), so
  // the non-Struct getChild() bug cannot be reached at the spark.readStream() level for INTERVAL
  // - matches V2StreamingDeletionVectorVariantTest's note. See ColumnVectorWithFilterTypeFanout-
  // Test for unit-level coverage.
}
