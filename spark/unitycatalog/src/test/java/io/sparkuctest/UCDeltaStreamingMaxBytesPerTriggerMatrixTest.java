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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * UC port of {@link io.delta.spark.internal.v2.V2StreamingMaxBytesPerTriggerMatrixTest}.
 *
 * <p>Cross-product coverage for {@code maxBytesPerTrigger} x feature-row tables on UC EXTERNAL +
 * MANAGED. Pattern: write 5 single-file commits, set {@code maxBytesPerTrigger=1b} so the rate
 * limiter admits exactly one file per batch (the engine always admits at least one file even when
 * its size exceeds the limit; this is DSv1 parity behavior). Assert 5 non-empty batches.
 */
public class UCDeltaStreamingMaxBytesPerTriggerMatrixTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int counter;

  /** Fresh local checkpoint dir; UC server holds cloud creds, not Spark. */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + counter++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  /** Returns recent progress entries that produced rows. */
  private static StreamingQueryProgress[] nonEmptyProgress(StreamingQuery q) {
    return Arrays.stream(q.recentProgress())
        .filter(p -> p.numInputRows() != 0L)
        .toArray(StreamingQueryProgress[]::new);
  }

  /**
   * Runs a streaming query with {@code maxBytesPerTrigger=1b} against the given UC table and
   * asserts exactly {@code expectedBatches} non-empty batches each with 1 row.
   */
  private void assertOneFilePerBatch(String tableName, String queryName, int expectedBatches)
      throws Exception {
    StreamingQuery q =
        spark()
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(tableName)
            .writeStream()
            .format("memory")
            .queryName(queryName)
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .option("checkpointLocation", checkpoint())
            .start();
    try {
      q.processAllAvailable();
      StreamingQueryProgress[] progress = nonEmptyProgress(q);
      assertEquals(
          expectedBatches,
          progress.length,
          () -> "Expected " + expectedBatches + " non-empty batches; got " + progress.length);
      for (StreamingQueryProgress p : progress) {
        assertEquals(1L, p.numInputRows(), "each batch should admit exactly one file");
      }
    } finally {
      q.stop();
      spark().sql("DROP VIEW IF EXISTS " + queryName);
    }
  }

  /** Insert 5 single-row commits with id=0..4 into a (id INT) table. */
  private void insertFiveIntCommits(String tableName) {
    for (int i = 0; i < 5; i++) {
      sql("INSERT INTO %s VALUES (%d)", tableName, i);
    }
  }

  @TestAllTableTypes
  public void testMaxBytes_onDvTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_mbpt_dv",
        "id INT",
        null,
        tableType,
        "'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          insertFiveIntCommits(tableName);
          // DV-only delete on one file - does not introduce a new AddFile.
          sql("DELETE FROM %s WHERE id = 0", tableName);

          String queryName = "uc_mbpt_dv_" + tableType.name().toLowerCase() + "_" + counter;
          assertOneFilePerBatch(tableName, queryName, /* expectedBatches= */ 5);
        });
  }

  @TestAllTableTypes
  public void testMaxBytes_onColMappedNameTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_mbpt_cm_name",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'name'",
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d, 'row%d')", tableName, i, i);
          }
          String queryName = "uc_mbpt_cm_name_" + tableType.name().toLowerCase() + "_" + counter;
          assertOneFilePerBatch(tableName, queryName, /* expectedBatches= */ 5);
        });
  }

  @TestAllTableTypes
  public void testMaxBytes_onColMappedIdTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_mbpt_cm_id",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'id', "
            + "'delta.minReaderVersion' = '2', "
            + "'delta.minWriterVersion' = '5'",
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d, 'row%d')", tableName, i, i);
          }
          String queryName = "uc_mbpt_cm_id_" + tableType.name().toLowerCase() + "_" + counter;
          assertOneFilePerBatch(tableName, queryName, /* expectedBatches= */ 5);
        });
  }

  @TestAllTableTypes
  public void testMaxBytes_onRowTrackedTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_mbpt_rt",
        "id INT",
        null,
        tableType,
        "'delta.enableRowTracking' = 'true'",
        tableName -> {
          insertFiveIntCommits(tableName);
          String queryName = "uc_mbpt_rt_" + tableType.name().toLowerCase() + "_" + counter;
          assertOneFilePerBatch(tableName, queryName, /* expectedBatches= */ 5);
        });
  }

  @TestAllTableTypes
  public void testMaxBytes_onIctTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_mbpt_ict",
        "id INT",
        null,
        tableType,
        "'delta.enableInCommitTimestamps' = 'true'",
        tableName -> {
          insertFiveIntCommits(tableName);
          String queryName = "uc_mbpt_ict_" + tableType.name().toLowerCase() + "_" + counter;
          assertOneFilePerBatch(tableName, queryName, /* expectedBatches= */ 5);
        });
  }

  @TestAllTableTypes
  public void testMaxBytes_onV2CheckpointTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_mbpt_v2ckpt",
        "id INT",
        null,
        tableType,
        "'delta.checkpointPolicy' = 'v2'",
        tableName -> {
          insertFiveIntCommits(tableName);
          String queryName = "uc_mbpt_v2ckpt_" + tableType.name().toLowerCase() + "_" + counter;
          assertOneFilePerBatch(tableName, queryName, /* expectedBatches= */ 5);
        });
  }

  @TestAllTableTypes
  public void testMaxBytes_onTimestampNtzTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_mbpt_tsntz",
        "id INT, ts TIMESTAMP_NTZ",
        tableType,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql(
                "INSERT INTO %s VALUES (%d, TIMESTAMP_NTZ'2024-0%d-01 00:00:00')",
                tableName, i, i + 1);
          }
          String queryName = "uc_mbpt_tsntz_" + tableType.name().toLowerCase() + "_" + counter;
          assertOneFilePerBatch(tableName, queryName, /* expectedBatches= */ 5);
        });
  }

  /**
   * Creates a clustered table directly (not via withNewTable) because the standard helper does not
   * support CLUSTER BY. Mirrors the pattern in UCDeltaStreamingClusteredTest.
   */
  private void withNewClusteredTable(
      String simpleName,
      String schemaSql,
      String clusterCols,
      TableType tableType,
      TestCode testCode)
      throws Exception {
    if (tableType == TableType.EXTERNAL) {
      withTempDir(
          dir -> {
            org.apache.hadoop.fs.Path tablePath = new org.apache.hadoop.fs.Path(dir, simpleName);
            String name = fullTableName(simpleName);
            sql(
                "CREATE TABLE %s (%s) USING DELTA CLUSTER BY (%s) LOCATION '%s'",
                name, schemaSql, clusterCols, tablePath.toString());
            try {
              testCode.run(name);
            } finally {
              sql("DROP TABLE IF EXISTS %s", name);
            }
          });
    } else {
      String name = fullTableName(simpleName);
      sql(
          "CREATE TABLE %s (%s) USING DELTA CLUSTER BY (%s)"
              + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          name, schemaSql, clusterCols);
      try {
        testCode.run(name);
      } finally {
        sql("DROP TABLE IF EXISTS %s", name);
      }
    }
  }

  @TestAllTableTypes
  public void testMaxBytes_onClusteredTable(TableType tableType) throws Exception {
    withNewClusteredTable(
        "uc_mbpt_clustered",
        "id INT, name STRING",
        "id",
        tableType,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d, 'row%d')", tableName, i, i);
          }
          String queryName = "uc_mbpt_clustered_" + tableType.name().toLowerCase() + "_" + counter;
          assertOneFilePerBatch(tableName, queryName, /* expectedBatches= */ 5);
        });
  }

  @TestAllTableTypes
  public void testMaxBytes_onAppendOnlyTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_mbpt_appendonly",
        "id INT",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          insertFiveIntCommits(tableName);
          String queryName = "uc_mbpt_appendonly_" + tableType.name().toLowerCase() + "_" + counter;
          assertOneFilePerBatch(tableName, queryName, /* expectedBatches= */ 5);
        });
  }

  /**
   * Partition column declared last in DDL (Spark's normalized form). 5 single-file commits, one row
   * per partition value.
   */
  @TestAllTableTypes
  public void testMaxBytes_onPartitionedTable_partLast(TableType tableType) throws Exception {
    withNewTable(
        "uc_mbpt_part_last",
        "id INT, part INT",
        "part",
        tableType,
        null,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d, %d)", tableName, i, i);
          }
          String queryName = "uc_mbpt_part_last_" + tableType.name().toLowerCase() + "_" + counter;
          assertOneFilePerBatch(tableName, queryName, /* expectedBatches= */ 5);
        });
  }

  /** Partition column declared FIRST in DDL schema. */
  @TestAllTableTypes
  public void testMaxBytes_onPartitionedTable_partFirst(TableType tableType) throws Exception {
    withNewTable(
        "uc_mbpt_part_first",
        "part INT, id INT",
        "part",
        tableType,
        null,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d, %d)", tableName, i, i);
          }
          String queryName =
              "uc_mbpt_part_first_" + tableType.name().toLowerCase() + "_" + counter;
          assertOneFilePerBatch(tableName, queryName, /* expectedBatches= */ 5);
        });
  }

  /**
   * DV-enabled table: 3 single-file commits, then DELETE half the rows (DV-only commit), then 2
   * more single-file commits. Expect 5 non-empty batches (one per AddFile).
   */
  @TestAllTableTypes
  public void testMaxBytes_withDvDelete_acrossBatches(TableType tableType) throws Exception {
    withNewTable(
        "uc_mbpt_dv_delete_across",
        "id INT",
        null,
        tableType,
        "'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          for (int i = 0; i < 3; i++) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }
          // DV-only delete (does not rewrite files)
          sql("DELETE FROM %s WHERE id = 1", tableName);
          for (int i = 3; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }

          String queryName =
              "uc_mbpt_dv_delete_across_" + tableType.name().toLowerCase() + "_" + counter;
          assertOneFilePerBatch(tableName, queryName, /* expectedBatches= */ 5);
        });
  }

  /**
   * Single commit with 5 files (via {@code repartition(5)}), {@code maxBytesPerTrigger=1b}. The
   * rate limiter must split the within-commit AddFile sequence across batches - one file per batch.
   */
  @TestAllTableTypes
  public void testMaxBytes_withMultiFileCommit(TableType tableType) throws Exception {
    withNewTable(
        "uc_mbpt_multifile",
        "id INT",
        tableType,
        tableName -> {
          // One commit producing 5 files via repartition.
          spark()
              .range(0, 5)
              .selectExpr("cast(id as int) as id")
              .repartition(5)
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName);

          String queryName = "uc_mbpt_multifile_" + tableType.name().toLowerCase() + "_" + counter;
          assertOneFilePerBatch(tableName, queryName, /* expectedBatches= */ 5);
        });
  }

  /**
   * First run with {@code maxBytesPerTrigger=1b} drains 5 single-file commits across 5 batches.
   * After 5 more single-file commits, restart with {@code maxBytesPerTrigger=100gb}; the second run
   * should drain everything in a single batch.
   */
  @TestAllTableTypes
  public void testMaxBytes_changeBetweenRuns(TableType tableType) throws Exception {
    withNewTable(
        "uc_mbpt_change",
        "id INT",
        tableType,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }

          String queryName = "uc_mbpt_change_" + tableType.name().toLowerCase() + "_" + counter;
          String checkpointDir = checkpoint();

          // First run: 1b -> 5 batches of 1 row each
          StreamingQuery q1 =
              spark()
                  .readStream()
                  .option("maxBytesPerTrigger", "1b")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", checkpointDir)
                  .start();
          try {
            q1.processAllAvailable();
            StreamingQueryProgress[] progress = nonEmptyProgress(q1);
            assertEquals(5, progress.length, "first run expected 5 batches with 1b limit");
            for (StreamingQueryProgress p : progress) {
              assertEquals(1L, p.numInputRows());
            }
          } finally {
            q1.stop();
          }

          // Add 5 more commits
          for (int i = 5; i < 10; i++) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }

          // Second run: 100gb -> all 5 new rows in 1 batch
          StreamingQuery q2 =
              spark()
                  .readStream()
                  .option("maxBytesPerTrigger", "100gb")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", checkpointDir)
                  .start();
          try {
            q2.processAllAvailable();
            StreamingQueryProgress[] progress = nonEmptyProgress(q2);
            assertEquals(1, progress.length, "second run expected 1 batch with 100gb limit");
            assertEquals(5L, progress[0].numInputRows());
            List<Row> all = spark().sql("SELECT * FROM " + queryName).collectAsList();
            assertEquals(10, all.size(), "all 10 rows should be present in the sink");
          } finally {
            q2.stop();
            spark().sql("DROP VIEW IF EXISTS " + queryName);
          }
        });
  }

  /**
   * Both rate limits set: {@code maxFilesPerTrigger=5} and {@code maxBytesPerTrigger=1b}. The
   * tighter limit (bytes=1b) should win and admit exactly one file per batch.
   */
  @TestAllTableTypes
  public void testMaxBytes_combinedWithMaxFiles(TableType tableType) throws Exception {
    withNewTable(
        "uc_mbpt_combined",
        "id INT",
        tableType,
        tableName -> {
          insertFiveIntCommits(tableName);

          String queryName = "uc_mbpt_combined_" + tableType.name().toLowerCase() + "_" + counter;
          StreamingQuery q =
              spark()
                  .readStream()
                  .option("maxFilesPerTrigger", "5")
                  .option("maxBytesPerTrigger", "1b")
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .start();
          try {
            q.processAllAvailable();
            StreamingQueryProgress[] progress = nonEmptyProgress(q);
            assertEquals(5, progress.length, "bytes=1b should dominate over files=5");
            for (StreamingQueryProgress p : progress) {
              assertEquals(1L, p.numInputRows());
            }
          } finally {
            q.stop();
            spark().sql("DROP VIEW IF EXISTS " + queryName);
          }
        });
  }
}
