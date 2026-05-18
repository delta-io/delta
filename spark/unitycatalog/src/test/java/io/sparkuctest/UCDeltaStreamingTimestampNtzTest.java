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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.io.TempDir;

/**
 * UC port of {@code V2StreamingTimestampNtzTest}. Cross-product cell {@code timestampNtz x
 * streaming} is 100% uncovered before this file. TIMESTAMP_NTZ requires a protocol bump
 * (TimestampNTZTableFeature, minReader=3 / minWriter=7), so any reader/writer mismatch surfaces
 * here on both EXTERNAL and MANAGED.
 */
public class UCDeltaStreamingTimestampNtzTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  /** Allocate a fresh local checkpoint directory. */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  /** Drain a streaming DF via memory sink under AvailableNow and return collected rows. */
  private List<Row> drainToMemory(Dataset<Row> df, String queryName) throws Exception {
    StreamingQuery q =
        df.writeStream()
            .format("memory")
            .queryName(queryName)
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .option("checkpointLocation", checkpoint())
            .start();
    try {
      q.awaitTermination();
      return spark().sql("SELECT * FROM " + queryName).collectAsList();
    } finally {
      q.stop();
      spark().sql("DROP VIEW IF EXISTS " + queryName);
    }
  }

  /** 1. Basic: TIMESTAMP_NTZ values across DST boundaries. */
  @TestAllTableTypes
  public void case1_basic(TableType tableType) throws Exception {
    withNewTable(
        "tsntz_basic",
        "id INT, ts TIMESTAMP_NTZ",
        tableType,
        tableName -> {
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, TIMESTAMP_NTZ'2024-03-10 02:30:00'), "
                  + "(2, TIMESTAMP_NTZ'2024-11-03 01:30:00'), "
                  + "(3, TIMESTAMP_NTZ'2024-06-15 12:00:00.123456')",
              tableName);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          assertTrue(df.isStreaming());
          String queryName = "tsntz_1_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          List<Row> rows = drainToMemory(df, queryName);
          assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
          for (Row r : rows) {
            int id = r.getInt(0);
            LocalDateTime ts = r.getAs(1);
            LocalDateTime expected;
            if (id == 1) expected = LocalDateTime.of(2024, 3, 10, 2, 30, 0);
            else if (id == 2) expected = LocalDateTime.of(2024, 11, 3, 1, 30, 0);
            else expected = LocalDateTime.of(2024, 6, 15, 12, 0, 0, 123456000);
            assertEquals(expected, ts, "Mismatch for id=" + id + " row=" + r);
          }
        });
  }

  /** 2. timestampNtz x DV (DELETE + DV). */
  @TestAllTableTypes
  public void case2_dv(TableType tableType) throws Exception {
    withNewTable(
        "tsntz_dv",
        "id INT, ts TIMESTAMP_NTZ",
        null,
        tableType,
        "'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, TIMESTAMP_NTZ'2024-01-01 00:00:00'), "
                  + "(2, TIMESTAMP_NTZ'2024-02-01 00:00:00'), "
                  + "(3, TIMESTAMP_NTZ'2024-03-01 00:00:00'), "
                  + "(4, TIMESTAMP_NTZ'2024-04-01 00:00:00')",
              tableName);
          sql("DELETE FROM %s WHERE id <= 2", tableName);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          String queryName = "tsntz_2_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          List<Row> rows = drainToMemory(df, queryName);
          assertEquals(2, rows.size(), "expected 2 rows after DV; got: " + rows);
          for (Row r : rows) {
            int id = r.getInt(0);
            LocalDateTime ts = r.getAs(1);
            LocalDateTime expected;
            if (id == 3) expected = LocalDateTime.of(2024, 3, 1, 0, 0, 0);
            else expected = LocalDateTime.of(2024, 4, 1, 0, 0, 0);
            assertEquals(expected, ts);
          }
        });
  }

  /** 3. timestampNtz x column mapping name. */
  @TestAllTableTypes
  public void case3_columnMapping(TableType tableType) throws Exception {
    withNewTable(
        "tsntz_cm",
        "id INT, event_time TIMESTAMP_NTZ",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'name', 'delta.minReaderVersion' = '3', "
            + "'delta.minWriterVersion' = '7'",
        tableName -> {
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, TIMESTAMP_NTZ'2024-05-01 10:00:00'), "
                  + "(2, TIMESTAMP_NTZ'2024-05-02 10:00:00')",
              tableName);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          String queryName = "tsntz_3_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          List<Row> rows = drainToMemory(df, queryName);
          assertEquals(2, rows.size(), "expected 2 rows; got: " + rows);
        });
  }

  /** 4. timestampNtz partition column. */
  @TestAllTableTypes
  public void case4_partitionColumn(TableType tableType) throws Exception {
    withNewTable(
        "tsntz_part",
        "id INT, ts TIMESTAMP_NTZ",
        "ts",
        tableType,
        null,
        tableName -> {
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, TIMESTAMP_NTZ'2024-01-01 12:00:00'), "
                  + "(2, TIMESTAMP_NTZ'2024-01-02 12:00:00')",
              tableName);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          String queryName = "tsntz_4_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          List<Row> rows = drainToMemory(df, queryName);
          assertEquals(2, rows.size(), "expected 2 rows; got: " + rows);
          for (Row r : rows) {
            int id = r.getInt(0);
            LocalDateTime ts = r.getAs(1);
            LocalDateTime expected =
                (id == 1)
                    ? LocalDateTime.of(2024, 1, 1, 12, 0, 0)
                    : LocalDateTime.of(2024, 1, 2, 12, 0, 0);
            assertEquals(expected, ts, "partition value mismatch for id=" + id);
          }
        });
  }

  /** 5. timestampNtz x startingTimestamp on the source. */
  @TestAllTableTypes
  public void case5_startingTimestamp(TableType tableType) throws Exception {
    withNewTable(
        "tsntz_startts",
        "id INT, ts TIMESTAMP_NTZ",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, TIMESTAMP_NTZ'2024-01-01 00:00:00')", tableName);

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("startingTimestamp", "1970-01-01 00:00:00")
                  .table(tableName);
          String queryName = "tsntz_5_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          List<Row> rows = drainToMemory(df, queryName);
          assertEquals(1, rows.size(), "expected 1 row from startingTimestamp; got: " + rows);
        });
  }

  /** 6. timestampNtz x restart from checkpoint. */
  @TestAllTableTypes
  public void case6_restart(TableType tableType) throws Exception {
    withNewTable(
        "tsntz_restart",
        "id INT, ts TIMESTAMP_NTZ",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, TIMESTAMP_NTZ'2024-01-01 00:00:00')", tableName);
          String ck = checkpoint();

          // First run.
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .format("noop")
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", ck)
              .start()
              .awaitTermination();

          sql("INSERT INTO %s VALUES (2, TIMESTAMP_NTZ'2024-06-15 12:00:00')", tableName);

          // Restart with same ck - should pick up only the new row.
          String queryName = "tsntz_6_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", ck)
              .start()
              .awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(1, rows.size(), "expected 1 new row after restart; got: " + rows);
          assertEquals(2, rows.get(0).getInt(0));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** 7. Year-9999 boundary timestamp_ntz value. */
  @TestAllTableTypes
  public void case7_year9999Boundary(TableType tableType) throws Exception {
    withNewTable(
        "tsntz_9999",
        "id INT, ts TIMESTAMP_NTZ",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, TIMESTAMP_NTZ'9999-12-31 23:59:59.999999')", tableName);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          String queryName = "tsntz_7_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          List<Row> rows = drainToMemory(df, queryName);
          assertEquals(1, rows.size(), "expected 1 boundary row; got: " + rows);
          LocalDateTime ts = rows.get(0).getAs(1);
          LocalDateTime expected = LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000);
          assertEquals(expected, ts, "Year-9999 boundary mismatch: " + ts);
        });
  }

  /** 8. NULL timestamp_ntz value. */
  @TestAllTableTypes
  public void case8_nullValue(TableType tableType) throws Exception {
    withNewTable(
        "tsntz_null",
        "id INT, ts TIMESTAMP_NTZ",
        tableType,
        tableName -> {
          sql(
              "INSERT INTO %s VALUES (1, NULL), (2, TIMESTAMP_NTZ'2024-01-01 00:00:00')",
              tableName);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          String queryName = "tsntz_8_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          List<Row> rows = drainToMemory(df, queryName);
          assertEquals(2, rows.size(), "expected 2 rows; got: " + rows);
          boolean sawNull = false;
          boolean sawNonNull = false;
          for (Row r : rows) {
            int id = r.getInt(0);
            Object ts = r.get(1);
            if (id == 1) {
              assertNull(ts, "expected NULL ts for id=1, got " + ts);
              sawNull = true;
            } else if (id == 2) {
              assertNotNull(ts, "expected non-null ts for id=2");
              assertEquals(LocalDateTime.of(2024, 1, 1, 0, 0, 0), ts);
              sawNonNull = true;
            }
          }
          assertTrue(sawNull && sawNonNull, "both null and non-null cases must be observed");
        });
  }
}
