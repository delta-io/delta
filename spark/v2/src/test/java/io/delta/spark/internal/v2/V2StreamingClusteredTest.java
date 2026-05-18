/*
 * Copyright (2025) The Delta Lake Project Authors.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for V2 streaming reads on Liquid-clustered Delta tables (CLUSTER BY).
 *
 * <p>Each test verifies a different scenario from the matrix on streaming + clustered tables:
 *
 * <ol>
 *   <li>Basic stream from clustered table.
 *   <li>Clustered + DV (DELETE).
 *   <li>Clustered + column mapping name.
 *   <li>Clustered + restart.
 *   <li>Clustered + Trigger.AvailableNow.
 *   <li>Clustered + maxFilesPerTrigger=1.
 *   <li>Clustered + ALTER CLUSTER BY mid-stream.
 *   <li>Clustered + OPTIMIZE (auto-compaction with dataChange=false should be skipped).
 *   <li>Clustered + multi-column CLUSTER BY.
 *   <li>Clustered + ALTER TABLE CLUSTER BY NONE (drop clustering).
 * </ol>
 */
public class V2StreamingClusteredTest extends V2TestBase {

  /**
   * Runs a streaming query that writes to a parquet sink at {@code outputPath} with the given
   * checkpoint, processes all available data, then stops. Returns rows by reading the parquet
   * output. Parquet sink supports checkpoint recovery, unlike memory sink.
   */
  private List<Row> runWithParquetSink(
      Dataset<Row> streamingDF,
      File outputDir,
      File checkpointDir,
      org.apache.spark.sql.streaming.Trigger trigger)
      throws Exception {
    StreamingQuery query = null;
    try {
      org.apache.spark.sql.streaming.DataStreamWriter<Row> writer =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath());
      if (trigger != null) {
        writer = writer.trigger(trigger);
      }
      query = writer.start();
      query.processAllAvailable();
      // For Trigger.AvailableNow, the query terminates on its own once all data is consumed; wait
      // briefly so the final commit lands before reading back the parquet output.
      if (trigger != null) {
        query.awaitTermination(60_000);
      }
    } finally {
      if (query != null) {
        query.stop();
      }
    }
    return spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
  }

  /** 1. Stream from clustered table, basic — verify rows arrive. */
  @Test
  public void testStreamingReadClusteredBasic(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta CLUSTER BY (id)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    assertTrue(streamingDF.isStreaming());

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_clustered_basic");
    List<Row> expectedRows =
        Arrays.asList(RowFactory.create(1, "Alice"), RowFactory.create(2, "Bob"));
    assertDataEquals(actualRows, expectedRows);
  }

  /** 2. Clustered + DV (DELETE, then stream) — should reflect the DV at initial snapshot. */
  @Test
  public void testStreamingReadClusteredWithDV(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (value INT) USING delta CLUSTER BY (value) "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark
        .range(10)
        .selectExpr("cast(id as int) as value")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
    spark.sql(str("DELETE FROM delta.`%s` WHERE value < 3", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_clustered_dv");

    List<Row> expectedRows = new ArrayList<>();
    for (int i = 3; i < 10; i++) {
      expectedRows.add(RowFactory.create(i));
    }
    assertDataEquals(actualRows, expectedRows);
  }

  /** 3. Clustered + column mapping name. */
  @Test
  public void testStreamingReadClusteredWithColumnMapping(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, user_name STRING) USING delta CLUSTER BY (id) "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_clustered_cm");
    List<Row> expectedRows =
        Arrays.asList(RowFactory.create(1, "Alice"), RowFactory.create(2, "Bob"));
    assertDataEquals(actualRows, expectedRows);
  }

  /** 4. Clustered + restart — checkpoint resumes after stop and picks up new commits. */
  @Test
  public void testStreamingReadClusteredRestart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta CLUSTER BY (id)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);

    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'Bob')", tablePath));

    try {
      List<Row> actualRows =
          runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);
      List<Row> expectedRows =
          Arrays.asList(RowFactory.create(1, "Alice"), RowFactory.create(2, "Bob"));
      assertDataEquals(actualRows, expectedRows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 5. Clustered + Trigger.AvailableNow — single one-shot run consumes all data. */
  @Test
  public void testStreamingReadClusteredTriggerAvailableNow(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta CLUSTER BY (id)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A'), (2, 'B'), (3, 'C')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    try {
      List<Row> actualRows =
          runWithParquetSink(streamingDF, outputDir, checkpointDir, Trigger.AvailableNow());
      List<Row> expectedRows =
          Arrays.asList(
              RowFactory.create(1, "A"), RowFactory.create(2, "B"), RowFactory.create(3, "C"));
      assertDataEquals(actualRows, expectedRows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 6. Clustered + maxFilesPerTrigger=1 — multiple files split across batches. */
  @Test
  public void testStreamingReadClusteredMaxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta CLUSTER BY (id)",
            tablePath));
    // Three separate commits → three separate AddFile batches.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "test_clustered_mfpt");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1, "A"), RowFactory.create(2, "B"), RowFactory.create(3, "C"));
    assertDataEquals(actualRows, expectedRows);
  }

  /** 7. Clustered + ALTER CLUSTER BY mid-stream — does the stream see the layout change? */
  @Test
  public void testStreamingReadClusteredAlterClusterByMidStream(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta CLUSTER BY (id)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);

    // ALTER the clustering column to a different existing column.
    spark.sql(str("ALTER TABLE delta.`%s` CLUSTER BY (name)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));

    try {
      List<Row> actualRows =
          runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);
      List<Row> expectedRows = Arrays.asList(RowFactory.create(1, "A"), RowFactory.create(2, "B"));
      assertDataEquals(actualRows, expectedRows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * 8. Stream + OPTIMIZE on clustered table — auto-compaction creates dataChange=false commits; the
   * streaming source must not surface those rewritten files as new data.
   */
  @Test
  public void testStreamingReadClusteredWithOptimize(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta CLUSTER BY (id)",
            tablePath));
    // Multiple commits to give OPTIMIZE something to compact.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);

    // OPTIMIZE produces dataChange=false AddFiles; streaming should treat them as no-op.
    spark.sql(str("OPTIMIZE delta.`%s`", tablePath));

    try {
      List<Row> actualRows =
          runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);
      // No re-emission expected — output is the original 3 rows only.
      List<Row> expectedRows =
          Arrays.asList(
              RowFactory.create(1, "A"), RowFactory.create(2, "B"), RowFactory.create(3, "C"));
      assertDataEquals(actualRows, expectedRows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /** 9. Stream + multi-column CLUSTER BY (e.g. CLUSTER BY (date, region)). */
  @Test
  public void testStreamingReadClusteredMultiColumn(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (event_date DATE, region STRING, value INT) "
                + "USING delta CLUSTER BY (event_date, region)",
            tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(DATE'2024-01-01', 'us', 1), "
                + "(DATE'2024-01-02', 'eu', 2)",
            tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_clustered_multi");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(java.sql.Date.valueOf("2024-01-01"), "us", 1),
            RowFactory.create(java.sql.Date.valueOf("2024-01-02"), "eu", 2));
    assertDataEquals(actualRows, expectedRows);
  }

  /** 10. Stream + ALTER TABLE ... CLUSTER BY NONE (drop clustering). */
  @Test
  public void testStreamingReadClusteredDropClustering(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta CLUSTER BY (id)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);

    spark.sql(str("ALTER TABLE delta.`%s` CLUSTER BY NONE", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));

    try {
      List<Row> actualRows =
          runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);
      List<Row> expectedRows = Arrays.asList(RowFactory.create(1, "A"), RowFactory.create(2, "B"));
      assertDataEquals(actualRows, expectedRows);
    } finally {
      DeltaLog.clearCache();
    }
  }
}
