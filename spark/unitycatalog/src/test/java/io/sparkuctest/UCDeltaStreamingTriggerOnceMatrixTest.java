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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * UC port of {@link io.delta.spark.internal.v2.V2StreamingTriggerOnceMatrixTest}.
 *
 * <p>{@code Trigger.Once()} / {@code Trigger.ProcessingTime("0s")} cross-product against
 * feature-bearing UC tables (EXTERNAL + MANAGED). For every feature row the test runs:
 *
 * <ol>
 *   <li>{@code Trigger.AvailableNow} oracle to compute expected total row count.
 *   <li>{@code Trigger.Once()} - assert sink count == oracle.
 *   <li>{@code Trigger.ProcessingTime("0s")} - assert sink count == oracle and the query terminates
 *       cleanly.
 * </ol>
 */
public class UCDeltaStreamingTriggerOnceMatrixTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int counter;

  /** Fresh local checkpoint dir per query. */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + counter++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  /** Run a one-shot {@code Trigger.AvailableNow} stream and return the sink row count. */
  private long oracleCount(String tableName, String queryName) throws Exception {
    StreamingQuery q =
        spark()
            .readStream()
            .table(tableName)
            .writeStream()
            .format("memory")
            .queryName(queryName)
            .trigger(Trigger.AvailableNow())
            .option("checkpointLocation", checkpoint())
            .start();
    try {
      assertTrue(q.awaitTermination(60_000), "AvailableNow oracle did not terminate in 60s");
      return spark().sql("SELECT COUNT(*) FROM " + queryName).collectAsList().get(0).getLong(0);
    } finally {
      q.stop();
      spark().sql("DROP VIEW IF EXISTS " + queryName);
    }
  }

  /** Run a one-shot {@code Trigger.Once()} stream and return the sink row count. */
  @SuppressWarnings("deprecation")
  private long triggerOnceCount(String tableName, String queryName) throws Exception {
    StreamingQuery q =
        spark()
            .readStream()
            .table(tableName)
            .writeStream()
            .format("memory")
            .queryName(queryName)
            .trigger(Trigger.Once())
            .option("checkpointLocation", checkpoint())
            .start();
    try {
      assertTrue(q.awaitTermination(60_000), "Trigger.Once did not terminate in 60s");
      return spark().sql("SELECT COUNT(*) FROM " + queryName).collectAsList().get(0).getLong(0);
    } finally {
      q.stop();
      spark().sql("DROP VIEW IF EXISTS " + queryName);
    }
  }

  /**
   * Run a {@code Trigger.ProcessingTime("0s")} stream until source is drained and return the sink
   * row count. {@code processAllAvailable} blocks until no more input is available; we then stop
   * the query because {@code ProcessingTime} never self-terminates.
   */
  private long processingTimeZeroCount(String tableName, String queryName) throws Exception {
    StreamingQuery q =
        spark()
            .readStream()
            .table(tableName)
            .writeStream()
            .format("memory")
            .queryName(queryName)
            .trigger(Trigger.ProcessingTime(0, TimeUnit.SECONDS))
            .option("checkpointLocation", checkpoint())
            .start();
    try {
      q.processAllAvailable();
      return spark().sql("SELECT COUNT(*) FROM " + queryName).collectAsList().get(0).getLong(0);
    } finally {
      q.stop();
      spark().sql("DROP VIEW IF EXISTS " + queryName);
    }
  }

  /**
   * Run the Trigger.Once and Trigger.ProcessingTime("0s") legs against {@code tableName} and assert
   * both match the AvailableNow oracle. {@code tag} disambiguates memory-sink names so each test
   * uses a fresh sink.
   */
  private void runMatrix(String tableName, String tag) throws Exception {
    String suffix = UUID.randomUUID().toString().replace('-', '_');
    long oracle = oracleCount(tableName, "oracle_" + tag + "_" + suffix);
    assertTrue(oracle > 0, () -> tag + ": oracle row count must be positive (got " + oracle + ")");

    long onceRows = triggerOnceCount(tableName, "once_" + tag + "_" + suffix);
    assertEquals(
        oracle, onceRows, () -> tag + ": Trigger.Once row count must match oracle " + oracle);

    long ptRows = processingTimeZeroCount(tableName, "pt0_" + tag + "_" + suffix);
    assertEquals(
        oracle,
        ptRows,
        () -> tag + ": Trigger.ProcessingTime(0s) row count must match oracle " + oracle);
  }

  /** Append 5 single-row commits with id=base..base+4 to a (id INT) table. */
  private void appendFiveIntCommits(String tableName) {
    for (int i = 0; i < 5; i++) {
      sql("INSERT INTO %s VALUES (%d)", tableName, i);
    }
  }

  /**
   * Creates a clustered table directly because the standard helper does not support CLUSTER BY.
   * Mirrors the pattern in UCDeltaStreamingClusteredTest.
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

  /** 1. DV table. */
  @TestAllTableTypes
  public void testTriggerOnceMatrix_deletionVectorTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_tom_dv",
        "id INT",
        null,
        tableType,
        "'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          appendFiveIntCommits(tableName);
          sql("DELETE FROM %s WHERE id = 2", tableName);
          runMatrix(tableName, "dv");
        });
  }

  /** 2. Column mapping = name. */
  @TestAllTableTypes
  public void testTriggerOnceMatrix_columnMappingNameTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_tom_cm_name",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'name', "
            + "'delta.minReaderVersion' = '2', "
            + "'delta.minWriterVersion' = '5'",
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d, 'n%d')", tableName, i, i);
          }
          runMatrix(tableName, "cm_name");
        });
  }

  /** 3. Column mapping = id. */
  @TestAllTableTypes
  public void testTriggerOnceMatrix_columnMappingIdTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_tom_cm_id",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'id', "
            + "'delta.minReaderVersion' = '2', "
            + "'delta.minWriterVersion' = '5'",
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d, 'n%d')", tableName, i, i);
          }
          runMatrix(tableName, "cm_id");
        });
  }

  /** 4. RowTracking table. */
  @TestAllTableTypes
  public void testTriggerOnceMatrix_rowTrackingTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_tom_rt",
        "id INT",
        null,
        tableType,
        "'delta.enableRowTracking' = 'true'",
        tableName -> {
          appendFiveIntCommits(tableName);
          runMatrix(tableName, "row_tracking");
        });
  }

  /** 5. In-Commit-Timestamp (ICT) table. */
  @TestAllTableTypes
  public void testTriggerOnceMatrix_ictTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_tom_ict",
        "id INT",
        null,
        tableType,
        "'delta.enableInCommitTimestamps' = 'true'",
        tableName -> {
          appendFiveIntCommits(tableName);
          runMatrix(tableName, "ict");
        });
  }

  /** 6. v2Checkpoint table. */
  @TestAllTableTypes
  public void testTriggerOnceMatrix_v2CheckpointTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_tom_v2cp",
        "id INT",
        null,
        tableType,
        "'delta.checkpointPolicy' = 'v2'",
        tableName -> {
          appendFiveIntCommits(tableName);
          runMatrix(tableName, "v2_checkpoint");
        });
  }

  /** 7. TIMESTAMP_NTZ table. */
  @TestAllTableTypes
  public void testTriggerOnceMatrix_timestampNtzTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_tom_tsntz",
        "id INT, ts TIMESTAMP_NTZ",
        tableType,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql(
                "INSERT INTO %s VALUES (%d, TIMESTAMP_NTZ'2024-0%d-01 00:00:00')",
                tableName, i, i + 1);
          }
          runMatrix(tableName, "ts_ntz");
        });
  }

  /** 8. Liquid-clustered table (CLUSTER BY). */
  @TestAllTableTypes
  public void testTriggerOnceMatrix_clusteredTable(TableType tableType) throws Exception {
    withNewClusteredTable(
        "uc_tom_clustered",
        "id INT, name STRING",
        "id",
        tableType,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d, 'n%d')", tableName, i, i);
          }
          runMatrix(tableName, "clustered");
        });
  }

  /** 9. appendOnly = true. */
  @TestAllTableTypes
  public void testTriggerOnceMatrix_appendOnlyTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_tom_appendonly",
        "id INT",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          appendFiveIntCommits(tableName);
          runMatrix(tableName, "append_only");
        });
  }

  /** 10. Partitioned table - partition column declared LAST. */
  @TestAllTableTypes
  public void testTriggerOnceMatrix_partitionedTable_partLast(TableType tableType)
      throws Exception {
    withNewTable(
        "uc_tom_part_last",
        "id INT, p STRING",
        "p",
        tableType,
        null,
        tableName -> {
          String[] parts = {"a", "b", "c", "d", "e"};
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES (%d, '%s')", tableName, i, parts[i]);
          }
          runMatrix(tableName, "part_last");
        });
  }

  /** 11. Partitioned table - partition column declared FIRST in DDL. */
  @TestAllTableTypes
  public void testTriggerOnceMatrix_partitionedTable_partFirst(TableType tableType)
      throws Exception {
    withNewTable(
        "uc_tom_part_first",
        "p STRING, id INT",
        "p",
        tableType,
        null,
        tableName -> {
          String[] parts = {"a", "b", "c", "d", "e"};
          for (int i = 0; i < 5; i++) {
            sql("INSERT INTO %s VALUES ('%s', %d)", tableName, parts[i], i);
          }
          runMatrix(tableName, "part_first");
        });
  }

  /** 12. VARIANT column table. */
  @TestAllTableTypes
  public void testTriggerOnceMatrix_variantTable(TableType tableType) throws Exception {
    withNewTable(
        "uc_tom_variant",
        "id INT, v VARIANT",
        tableType,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            spark()
                .range(i, i + 1)
                .selectExpr(
                    "cast(id as int) as id", "parse_json(concat('{\"row\":', id, '}')) as v")
                .coalesce(1)
                .write()
                .format("delta")
                .mode("append")
                .saveAsTable(tableName);
          }
          runMatrix(tableName, "variant");
        });
  }
}
