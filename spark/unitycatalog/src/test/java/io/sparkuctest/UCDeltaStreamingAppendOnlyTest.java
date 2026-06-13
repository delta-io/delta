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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.io.TempDir;

/**
 * Ports {@link io.delta.spark.internal.v2.V2StreamingAppendOnlyTest} (DSv2 EXTERNAL via {@code
 * dsv2.delta.<path>}) to Unity Catalog so each case runs against both EXTERNAL and MANAGED tables
 * via {@code @TestAllTableTypes}.
 *
 * <p>{@code delta.appendOnly=true} is a writer-side table feature that rejects DELETE/UPDATE/MERGE
 * with {@code DELTA_CANNOT_MODIFY_APPEND_ONLY}. The MANAGED variant is the bug-surfacing target:
 * the rejection must fire on the writer-side path used by catalogManaged tables, not be masked or
 * replaced by the catalogManaged kill switch.
 *
 * <p>All cases compose with UC infrastructure: appendOnly is set at CREATE time via TBLPROPERTIES,
 * so no ALTER TABLE is needed (ALTER is blocked on UC MANAGED - see {@code
 * UCDeltaTableBlockMetadataUpdateTest}). DELETE/UPDATE/MERGE are routed through the writer path
 * that the appendOnly check guards, before any UC kill switch fires.
 */
public class UCDeltaStreamingAppendOnlyTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  /** Fresh local checkpoint dir; UC server holds cloud creds, not Spark. */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  /** Unique query name across {@code @TestAllTableTypes} iterations. */
  private String queryName(String tag, TableType tableType) {
    return tag + "_" + tableType.name().toLowerCase() + "_" + checkpointCount;
  }

  /** Run a memory-sink streaming query to completion via {@code processAllAvailable}. */
  private List<Row> drainMemorySink(Dataset<Row> df, String qName) throws Exception {
    StreamingQuery q = null;
    try {
      q =
          df.writeStream()
              .format("memory")
              .queryName(qName)
              .outputMode("append")
              .option("checkpointLocation", checkpoint())
              .start();
      q.processAllAvailable();
      return spark().sql("SELECT * FROM " + qName).collectAsList();
    } finally {
      if (q != null) {
        q.stop();
      }
      spark().sql("DROP VIEW IF EXISTS " + qName);
    }
  }

  // 1. S1 - basic stream from an appendOnly table.
  @TestAllTableTypes
  public void testAppendOnlyBasic(TableType tableType) throws Exception {
    withNewTable(
        "ao_basic",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          assertTrue(df.isStreaming());

          List<Row> rows = drainMemorySink(df, queryName("ao_basic", tableType));
          assertEquals(2, rows.size(), "expected 2 rows; got: " + rows);
          Set<Integer> ids = new HashSet<>();
          for (Row r : rows) {
            ids.add(r.getInt(0));
          }
          assertEquals(Set.of(1, 2), ids);
        });
  }

  // 2. S2/S3 - startingVersion=N skips earlier commits on an appendOnly table.
  @TestAllTableTypes
  public void testAppendOnlyStartingVersion(TableType tableType) throws Exception {
    withNewTable(
        "ao_startver",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A')", tableName);
          long v1 = currentVersion(tableName);
          sql("INSERT INTO %s VALUES (2, 'B')", tableName);
          sql("INSERT INTO %s VALUES (3, 'C')", tableName);

          // Stream from v1+1 = first version after Alice's insert: should see Bob and Charlie.
          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("startingVersion", Long.toString(v1 + 1))
                  .table(tableName);

          List<Row> rows = drainMemorySink(df, queryName("ao_startver", tableType));
          assertEquals(2, rows.size(), "expected 2 rows after startingVersion; got: " + rows);
          Set<Integer> ids = new HashSet<>();
          for (Row r : rows) {
            ids.add(r.getInt(0));
          }
          assertEquals(Set.of(2, 3), ids);
        });
  }

  // 3. S4 - startingTimestamp far-past consumes everything on an appendOnly table.
  @TestAllTableTypes
  public void testAppendOnlyStartingTimestamp(TableType tableType) throws Exception {
    withNewTable(
        "ao_startts",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("startingTimestamp", "1970-01-01 00:00:00")
                  .table(tableName);

          List<Row> rows = drainMemorySink(df, queryName("ao_startts", tableType));
          assertEquals(
              2, rows.size(), "expected 2 rows from far-past startingTimestamp; got: " + rows);
        });
  }

  // 4. S5 - maxFilesPerTrigger=1 splits commits across batches on appendOnly.
  @TestAllTableTypes
  public void testAppendOnlyMaxFilesPerTrigger(TableType tableType) throws Exception {
    withNewTable(
        "ao_mfpt",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A')", tableName);
          sql("INSERT INTO %s VALUES (2, 'B')", tableName);
          sql("INSERT INTO %s VALUES (3, 'C')", tableName);

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("maxFilesPerTrigger", "1")
                  .table(tableName);

          List<Row> rows = drainMemorySink(df, queryName("ao_mfpt", tableType));
          assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
        });
  }

  // 5. S6 - maxBytesPerTrigger small value still emits all rows eventually.
  @TestAllTableTypes
  public void testAppendOnlyMaxBytesPerTrigger(TableType tableType) throws Exception {
    withNewTable(
        "ao_mbpt",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A')", tableName);
          sql("INSERT INTO %s VALUES (2, 'B')", tableName);
          sql("INSERT INTO %s VALUES (3, 'C')", tableName);

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("maxBytesPerTrigger", "1b")
                  .table(tableName);

          List<Row> rows = drainMemorySink(df, queryName("ao_mbpt", tableType));
          assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
        });
  }

  // 6. S7 - Trigger.AvailableNow consumes the whole appendOnly table in one shot.
  @TestAllTableTypes
  public void testAppendOnlyTriggerAvailableNow(TableType tableType) throws Exception {
    withNewTable(
        "ao_avail_now",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B'), (3, 'C')", tableName);

          String qName = queryName("ao_avail_now", tableType);
          StreamingQuery q =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .start();
          try {
            assertTrue(q.awaitTermination(60_000L), "AvailableNow should terminate");
            List<Row> rows = spark().sql("SELECT * FROM " + qName).collectAsList();
            assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
          } finally {
            q.stop();
            spark().sql("DROP VIEW IF EXISTS " + qName);
          }
        });
  }

  // 7. S8 - Trigger.Once consumes the appendOnly table in a single batch.
  @SuppressWarnings("deprecation")
  @TestAllTableTypes
  public void testAppendOnlyTriggerOnce(TableType tableType) throws Exception {
    withNewTable(
        "ao_once",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B'), (3, 'C')", tableName);

          String qName = queryName("ao_once", tableType);
          StreamingQuery q =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .trigger(Trigger.Once())
                  .option("checkpointLocation", checkpoint())
                  .start();
          try {
            assertTrue(q.awaitTermination(60_000L), "Trigger.Once should terminate");
            List<Row> rows = spark().sql("SELECT * FROM " + qName).collectAsList();
            assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
          } finally {
            q.stop();
            spark().sql("DROP VIEW IF EXISTS " + qName);
          }
        });
  }

  // 8. S8 - Trigger.ProcessingTime("0s") doesn't duplicate rows on appendOnly.
  @TestAllTableTypes
  public void testAppendOnlyTriggerProcessingTime(TableType tableType) throws Exception {
    withNewTable(
        "ao_pt",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B'), (3, 'C')", tableName);

          String qName = queryName("ao_pt", tableType);
          StreamingQuery q =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .trigger(Trigger.ProcessingTime(0, TimeUnit.SECONDS))
                  .option("checkpointLocation", checkpoint())
                  .start();
          try {
            q.processAllAvailable();
            List<Row> rows = spark().sql("SELECT * FROM " + qName).collectAsList();
            assertEquals(3, rows.size(), "expected 3 unique rows; got: " + rows);
          } finally {
            q.stop();
            spark().sql("DROP VIEW IF EXISTS " + qName);
          }
        });
  }

  // 9. S14 - restart from checkpoint picks up new commits on appendOnly.
  @TestAllTableTypes
  public void testAppendOnlyRestart(TableType tableType) throws Exception {
    withNewTable(
        "ao_restart",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A')", tableName);

          String qName = queryName("ao_restart", tableType);
          String ck = checkpoint();

          StreamingQuery q1 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          try {
            q1.processAllAvailable();
          } finally {
            q1.stop();
          }

          sql("INSERT INTO %s VALUES (2, 'B')", tableName);

          StreamingQuery q2 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(qName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          try {
            q2.processAllAvailable();
            List<Row> rows = spark().sql("SELECT * FROM " + qName).collectAsList();
            assertEquals(2, rows.size(), "expected 2 rows after restart; got: " + rows);
          } finally {
            q2.stop();
            spark().sql("DROP VIEW IF EXISTS " + qName);
          }
        });
  }

  // 10. S15 - excludeRegex on an appendOnly table excludes matched files.
  @TestAllTableTypes
  public void testAppendOnlyExcludeRegex(TableType tableType) throws Exception {
    withNewTable(
        "ao_excl",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);

          // Match every parquet file -> exclude everything; result must be empty.
          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("excludeRegex", ".*\\.parquet$")
                  .table(tableName);

          List<Row> rows = drainMemorySink(df, queryName("ao_excl", tableType));
          assertEquals(0, rows.size(), "expected 0 rows after excluding all parquet; got: " + rows);
        });
  }

  // 11. DELETE attempt is rejected at write time on an appendOnly table; stream unaffected.
  @TestAllTableTypes
  public void testAppendOnlyDeleteRejected(TableType tableType) throws Exception {
    withNewTable(
        "ao_del_rej",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);

          Exception ex =
              assertThrows(Exception.class, () -> sql("DELETE FROM %s WHERE id = 1", tableName));
          assertTrue(
              ex.toString().contains("DELTA_CANNOT_MODIFY_APPEND_ONLY")
                  || ex.toString().contains("only allow appends"),
              "expected DELTA_CANNOT_MODIFY_APPEND_ONLY error; got: " + ex);

          // Stream is unaffected by the rejected DELETE.
          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          List<Row> rows = drainMemorySink(df, queryName("ao_del_rej", tableType));
          assertEquals(2, rows.size(), "expected 2 rows still visible; got: " + rows);
        });
  }

  // 12. UPDATE attempt is rejected on an appendOnly table; stream unaffected.
  @TestAllTableTypes
  public void testAppendOnlyUpdateRejected(TableType tableType) throws Exception {
    withNewTable(
        "ao_upd_rej",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);

          Exception ex =
              assertThrows(
                  Exception.class, () -> sql("UPDATE %s SET name = 'Z' WHERE id = 1", tableName));
          assertTrue(
              ex.toString().contains("DELTA_CANNOT_MODIFY_APPEND_ONLY")
                  || ex.toString().contains("only allow appends"),
              "expected DELTA_CANNOT_MODIFY_APPEND_ONLY error; got: " + ex);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          List<Row> rows = drainMemorySink(df, queryName("ao_upd_rej", tableType));
          assertEquals(2, rows.size(), "expected 2 rows still visible; got: " + rows);
        });
  }

  // 13. MERGE with non-insert clauses is rejected on appendOnly; stream unaffected.
  @TestAllTableTypes
  public void testAppendOnlyMergeRejected(TableType tableType) throws Exception {
    withNewTable(
        "ao_merge_rej",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);

          spark().sql("DROP VIEW IF EXISTS ao_merge_src");
          spark()
              .sql("SELECT cast(1 AS INT) AS id, 'Z' AS name UNION ALL SELECT 3, 'C'")
              .createOrReplaceTempView("ao_merge_src");

          Exception ex =
              assertThrows(
                  Exception.class,
                  () ->
                      sql(
                          "MERGE INTO %s t USING ao_merge_src s ON t.id = s.id "
                              + "WHEN MATCHED THEN UPDATE SET t.name = s.name "
                              + "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
                          tableName));
          assertTrue(
              ex.toString().contains("DELTA_CANNOT_MODIFY_APPEND_ONLY")
                  || ex.toString().contains("only allow appends"),
              "expected DELTA_CANNOT_MODIFY_APPEND_ONLY error; got: " + ex);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          List<Row> rows = drainMemorySink(df, queryName("ao_merge_rej", tableType));
          assertEquals(2, rows.size(), "expected 2 rows still visible; got: " + rows);
          spark().sql("DROP VIEW IF EXISTS ao_merge_src");
        });
  }

  // 14. appendOnly + column-mapping (name mode) - basic stream works through CM rename layer.
  @TestAllTableTypes
  public void testAppendOnlyWithColumnMapping(TableType tableType) throws Exception {
    withNewTable(
        "ao_cm",
        "id INT, user_name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true', 'delta.columnMapping.mode' = 'name'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          List<Row> rows = drainMemorySink(df, queryName("ao_cm", tableType));
          assertEquals(2, rows.size(), "expected 2 rows; got: " + rows);
        });
  }

  // 15. appendOnly + DV interplay - DELETE rejected even with DVs enabled; stream sees all rows.
  @TestAllTableTypes
  public void testAppendOnlyWithDeletionVectors(TableType tableType) throws Exception {
    withNewTable(
        "ao_dv",
        "id INT",
        null,
        tableType,
        "'delta.appendOnly' = 'true', 'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(5)", tableName);

          // DELETE must be rejected by appendOnly even though DVs are enabled.
          Exception ex =
              assertThrows(Exception.class, () -> sql("DELETE FROM %s WHERE id < 3", tableName));
          assertTrue(
              ex.toString().contains("DELTA_CANNOT_MODIFY_APPEND_ONLY")
                  || ex.toString().contains("only allow appends"),
              "expected DELTA_CANNOT_MODIFY_APPEND_ONLY error; got: " + ex);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          List<Row> rows = drainMemorySink(df, queryName("ao_dv", tableType));
          assertEquals(5, rows.size(), "expected all 5 original rows; got: " + rows);
        });
  }
}
