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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.io.TempDir;

/**
 * Ports {@link io.delta.spark.internal.v2.V2StreamingIctTest} (DSv2 EXTERNAL via {@code
 * dsv2.delta.<path>}) to Unity Catalog so each ICT case runs against both EXTERNAL and MANAGED
 * tables via {@code @TestAllTableTypes}.
 *
 * <p>The DSv2 reference test relied on {@code IctTestUtils.modifyCommitTimestamp} / {@code
 * setFileMtimeOnly} to plant deterministic synthetic timestamps inside the delta log files. That
 * helper uses {@code new File(deltaFilePath.toUri).setLastModified(...)}, which only works on
 * local-FS paths. UC managed tables live behind the {@code S3CredentialFileSystem} (s3://) facade,
 * so direct {@code java.io.File} mtime manipulation is incompatible.
 *
 * <p>Cases that require synthetic timestamps (sub-second skew, mtime drift, far-future, mid-
 * history ICT enablement) are therefore skipped here -- they remain covered by {@code
 * V2StreamingIctTest} on EXTERNAL via {@code dsv2.delta.<path>}, which uses local-FS @TempDir
 * paths.
 *
 * <p>The ported cases use the real ICT timestamps that get assigned at commit time, with
 * starting-version semantics for the deterministic boundary. UC managed tables enable {@code
 * delta.enableInCommitTimestamps} by default; for EXTERNAL we set the property explicitly.
 */
public class UCDeltaStreamingIctTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  private static final String ICT_PROPS = "'delta.enableInCommitTimestamps' = 'true'";

  // Stream from an ICT-enabled table from the beginning -- baseline ICT path works.
  @TestAllTableTypes
  public void testIctStreamFromBeginning(TableType tableType) throws Exception {
    withNewTable(
        "ict_basic",
        "id BIGINT",
        null,
        tableType,
        ICT_PROPS,
        tableName -> {
          sql("INSERT INTO %s SELECT * FROM range(0, 10)", tableName);
          sql("INSERT INTO %s SELECT * FROM range(10, 20)", tableName);

          String queryName = "ict_basic_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery q =
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
          q.awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(20, rows.size(), "expected 20 rows; got: " + rows.size());
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // ICT + startingVersion: bypass startingTimestamp entirely (covered by ColumnMapping suite for
  // cross-feature; here we sanity-check that startingVersion still works on an ICT table).
  @TestAllTableTypes
  public void testIctStartingVersion(TableType tableType) throws Exception {
    withNewTable(
        "ict_starting_version",
        "id BIGINT",
        null,
        tableType,
        ICT_PROPS,
        tableName -> {
          sql("INSERT INTO %s SELECT * FROM range(0, 10)", tableName);
          long v1 = currentVersion(tableName);
          sql("INSERT INTO %s SELECT * FROM range(10, 20)", tableName);

          String queryName =
              "ict_starting_v_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          long startVersion = v1 + 1;
          StreamingQuery q =
              spark()
                  .readStream()
                  .format("delta")
                  .option("startingVersion", String.valueOf(startVersion))
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .start();
          q.awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(
              10, rows.size(), "expected 10 rows after startingVersion; got: " + rows.size());
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // Restart with checkpoint should resume from where we left off (no replay).
  @TestAllTableTypes
  public void testIctRestartResumesFromCheckpoint(TableType tableType) throws Exception {
    withNewTable(
        "ict_restart",
        "id BIGINT",
        null,
        tableType,
        ICT_PROPS,
        tableName -> {
          sql("INSERT INTO %s SELECT * FROM range(0, 10)", tableName);
          sql("INSERT INTO %s SELECT * FROM range(10, 20)", tableName);

          String queryName1 =
              "ict_restart_q1_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();
          StreamingQuery q1 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName1)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          long initialRows =
              spark().sql("SELECT COUNT(*) FROM " + queryName1).collectAsList().get(0).getLong(0);
          assertEquals(20L, initialRows, "Initial run should process 20 rows");

          // Append more data, then restart from same checkpoint.
          sql("INSERT INTO %s SELECT * FROM range(20, 30)", tableName);

          String queryName2 =
              "ict_restart_q2_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery q2 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName2)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          long restartRows =
              spark().sql("SELECT COUNT(*) FROM " + queryName2).collectAsList().get(0).getLong(0);
          assertEquals(
              10L, restartRows, "Restart should resume from checkpoint and only see new 10 rows");
          spark().sql("DROP VIEW IF EXISTS " + queryName1);
          spark().sql("DROP VIEW IF EXISTS " + queryName2);
        });
  }

  // ICT x AvailableNow: ICT-enabled table drains correctly under one-shot trigger.
  @TestAllTableTypes
  public void testIctAvailableNow(TableType tableType) throws Exception {
    withNewTable(
        "ict_avail_now",
        "id BIGINT",
        null,
        tableType,
        ICT_PROPS,
        tableName -> {
          sql("INSERT INTO %s SELECT * FROM range(0, 10)", tableName);
          sql("INSERT INTO %s SELECT * FROM range(10, 20)", tableName);
          sql("INSERT INTO %s SELECT * FROM range(20, 30)", tableName);

          String queryName =
              "ict_avail_now_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery q =
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
          assertTrue(q.awaitTermination(60_000L), "AvailableNow should terminate");

          long rows =
              spark().sql("SELECT COUNT(*) FROM " + queryName).collectAsList().get(0).getLong(0);
          assertEquals(30L, rows, "AvailableNow should drain all 30 rows");
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // ICT x DV. Combines ICT enablement with DV-driven DELETE filtering.
  @TestAllTableTypes
  public void testIctWithDeletionVectors(TableType tableType) throws Exception {
    withNewTable(
        "ict_dv",
        "id BIGINT",
        null,
        tableType,
        "'delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          sql("INSERT INTO %s SELECT * FROM range(0, 10)", tableName);
          sql("DELETE FROM %s WHERE id < 3", tableName);

          String queryName = "ict_dv_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          StreamingQuery q =
              df.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .start();
          q.awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          // Initial snapshot read post-DELETE: 7 rows survive (3..9).
          assertEquals(7, rows.size(), "ICT + DV: expected 7 surviving rows; got: " + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // ICT x Column Mapping. Combines ICT with column-mapping-mode=name.
  @TestAllTableTypes
  public void testIctWithColumnMapping(TableType tableType) throws Exception {
    withNewTable(
        "ict_cm",
        "id BIGINT",
        null,
        tableType,
        "'delta.enableInCommitTimestamps' = 'true', 'delta.columnMapping.mode' = 'name', "
            + "'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5'",
        tableName -> {
          sql("INSERT INTO %s SELECT * FROM range(0, 10)", tableName);
          long v1 = currentVersion(tableName);
          sql("INSERT INTO %s SELECT * FROM range(10, 20)", tableName);

          String queryName = "ict_cm_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          long startVersion = v1 + 1;
          StreamingQuery q =
              spark()
                  .readStream()
                  .format("delta")
                  .option("startingVersion", String.valueOf(startVersion))
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .start();
          q.awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(10, rows.size(), "ICT + col-map: expected 10 rows from v=" + startVersion);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }
}
