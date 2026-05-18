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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Closeout coverage for two scenario columns that the Phase-0 re-baseline flagged as long-tail
 * gaps:
 *
 * <ul>
 *   <li><b>CDF table x regular (non-readChangeFeed) streaming</b> - DSv2 blocks {@code
 *       readChangeFeed} entirely (see {@link V2CDCStreamingReadTest}), but a CDF-enabled table can
 *       still be streamed as a regular row-level source. The existing {@link
 *       V2CDCStreamingReadTest#testCdfTableStreamRestart} pins basic restart; this file fills out
 *       the rest of the streaming-option matrix on CDF-enabled tables.
 *   <li><b>ColMap-id / Variant / RowTracking / Compound long-tail</b> - sibling files cover the
 *       common cells of each feature x streaming cross-product. The cells here are the residual
 *       ones not exercised in {@link V2StreamingColumnMappingIdModeTest}, {@link
 *       V2StreamingVariantScenarioTest}, {@link V2StreamingRowTrackingTest}, or {@link
 *       V2StreamingCmRtDvTest}.
 * </ul>
 *
 * <p>Tests use {@code processStreamingQuery} for one-shot drains and the parquet sink for restart
 * cases (memory sink does not recover from a checkpoint).
 */
public class V2StreamingCdfAndLongTailTest extends V2TestBase {

  /** Creates a Delta table at {@code tablePath} with CDF enabled. */
  private void createCdfTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
            tablePath));
  }

  /** Creates a CDF-enabled table with deletion vectors. */
  private void createCdfDvTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, value STRING) USING delta TBLPROPERTIES ("
                + "'delta.enableChangeDataFeed' = 'true',"
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));
  }

  /** Creates a partitioned CDF-enabled table (partition column {@code p STRING}). */
  private void createCdfPartitionedTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p) "
                + "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
            tablePath));
  }

  /** Creates a column-mapping mode=id table. */
  private void createCmIdTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
  }

  /** Creates a CM-id partitioned table. */
  private void createCmIdPartitionedTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p) "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id', "
                + "  'delta.minReaderVersion' = '2', "
                + "  'delta.minWriterVersion' = '5')",
            tablePath));
  }

  /** Creates a CM-id + row tracking table. */
  private void createCmIdRtTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ("
                + "  'delta.columnMapping.mode' = 'id',"
                + "  'delta.enableRowTracking' = 'true',"
                + "  'delta.minReaderVersion' = '3',"
                + "  'delta.minWriterVersion' = '7')",
            tablePath));
  }

  /** Creates a CM-id + RT + DV table. */
  private void createCmIdRtDvTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ("
                + "  'delta.columnMapping.mode' = 'id',"
                + "  'delta.enableRowTracking' = 'true',"
                + "  'delta.enableDeletionVectors' = 'true',"
                + "  'delta.minReaderVersion' = '3',"
                + "  'delta.minWriterVersion' = '7')",
            tablePath));
  }

  /** Creates a CM-id + RT + DV partitioned table (partition column {@code p STRING}). */
  private void createCmIdRtDvPartitionedTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING, p STRING) USING delta "
                + "PARTITIONED BY (p) TBLPROPERTIES ("
                + "  'delta.columnMapping.mode' = 'id',"
                + "  'delta.enableRowTracking' = 'true',"
                + "  'delta.enableDeletionVectors' = 'true',"
                + "  'delta.minReaderVersion' = '3',"
                + "  'delta.minWriterVersion' = '7')",
            tablePath));
  }

  /** Creates a CM-name + RT + DV partitioned table. */
  private void createCmNameRtDvPartitionedTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING, p STRING) USING delta "
                + "PARTITIONED BY (p) TBLPROPERTIES ("
                + "  'delta.columnMapping.mode' = 'name',"
                + "  'delta.enableRowTracking' = 'true',"
                + "  'delta.enableDeletionVectors' = 'true',"
                + "  'delta.minReaderVersion' = '3',"
                + "  'delta.minWriterVersion' = '7')",
            tablePath));
  }

  /** Creates a row-tracked partitioned table (id LONG, name STRING, p STRING). */
  private void createRtPartitionedTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING, p STRING) USING delta "
                + "PARTITIONED BY (p) "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            tablePath));
  }

  /** Creates a row-tracked table (id LONG, name STRING). */
  private void createRowTrackedTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableRowTracking' = 'true')",
            tablePath));
  }

  /** Creates a top-level VARIANT table. */
  private void createVariantTable(String tablePath) {
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, v VARIANT) USING delta", tablePath));
  }

  /**
   * Appends {@code count} VARIANT rows starting at id {@code startId}, each with {@code v =
   * parse_json('{"row":<id>}')}. Single-file commit via {@code coalesce(1)}.
   */
  private void appendVariantRows(String tablePath, int startId, int count) {
    spark
        .range(startId, startId + count)
        .selectExpr("cast(id as int) as id", "parse_json(concat('{\"row\":', id, '}')) as v")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
  }

  /** Formats wall-clock millis using the session-local time zone, for startingTimestamp. */
  private String formatTs(long millis) {
    String tz = spark.sessionState().conf().sessionLocalTimeZone();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone(tz));
    return sdf.format(new Date(millis));
  }

  /**
   * Run a streaming query and capture the {@link StreamingQueryException} thrown by {@code
   * processAllAvailable}.
   */
  private StreamingQueryException runAndCaptureException(Dataset<Row> streamingDF, String name)
      throws Exception {
    StreamingQuery query =
        streamingDF.writeStream().format("memory").queryName(name).outputMode("append").start();
    try {
      return assertThrows(StreamingQueryException.class, () -> query.processAllAvailable());
    } finally {
      query.stop();
    }
  }

  /**
   * Drain {@code streamingDF} into the parquet sink with the given checkpoint. Stops the query
   * after {@code processAllAvailable} returns. Returns the parquet sink contents.
   */
  private List<Row> drainToParquet(Dataset<Row> streamingDF, File outputDir, File checkpointDir)
      throws Exception {
    StreamingQuery q =
        streamingDF
            .writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q.processAllAvailable();
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
    return spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
  }

  /**
   * Spawn a concurrent writer that runs {@code task.run()} every {@code sleepMillis} ms with id
   * {@code i} up to {@code maxRows}, until {@code stop} flips true. Caller is responsible for
   * shutting down the executor.
   */
  private void launchConcurrentInserter(
      ExecutorService executor,
      String tablePath,
      AtomicBoolean stop,
      int maxRows,
      long sleepMillis) {
    executor.submit(
        () -> {
          int i = 1;
          while (!stop.get() && i <= maxRows) {
            try {
              spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, 'r-%d')", tablePath, i, i));
              Thread.sleep(sleepMillis);
              i++;
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            } catch (Exception ignored) {
              // Concurrent commit may transiently fail; just continue.
            }
          }
        });
  }

  /**
   * Force OPTIMIZE to actually compact small files (otherwise small commits are treated as
   * already-optimal and no rewrite is emitted).
   */
  private void runOptimize(String tablePath) {
    withSQLConf(
        "spark.databricks.delta.optimize.minFileSize",
        Long.toString(1024L * 1024L * 1024L),
        () -> spark.sql(str("OPTIMIZE delta.`%s`", tablePath)));
  }

  /** CDF table + startingVersion: skip earlier commits via the regular (non-CDC) source. */
  @Test
  public void testCdfTable_startingVersion_regularStream(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath)); // v1
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath)); // v2
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath)); // v3

    Dataset<Row> df =
        spark.readStream().option("startingVersion", "2").table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cdf_starting_version");
    assertDataEquals(rows, Arrays.asList(RowFactory.create(2, "b"), RowFactory.create(3, "c")));
  }

  /** CDF table + startingTimestamp: timestamp resolution path, no readChangeFeed option. */
  @Test
  public void testCdfTable_startingTimestamp_regularStream(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    String tsStr = formatTs(0L); // 1970-01-01 -> always before commit mtimes
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingTimestamp", tsStr)
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cdf_starting_timestamp");
    assertDataEquals(rows, Arrays.asList(RowFactory.create(1, "a"), RowFactory.create(2, "b")));
  }

  /** CDF table + maxFilesPerTrigger: per-batch slicing on a CDF-enabled table. */
  @Test
  public void testCdfTable_maxFilesPerTrigger_regularStream(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cdf_max_files");
    assertDataEquals(
        rows,
        Arrays.asList(
            RowFactory.create(1, "a"), RowFactory.create(2, "b"), RowFactory.create(3, "c")));
  }

  /** CDF table + maxBytesPerTrigger: byte-based rate limiter on a CDF-enabled table. */
  @Test
  public void testCdfTable_maxBytesPerTrigger_regularStream(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cdf_max_bytes");
    assertDataEquals(
        rows,
        Arrays.asList(
            RowFactory.create(1, "a"), RowFactory.create(2, "b"), RowFactory.create(3, "c")));
  }

  /** CDF table + excludeRegex (matches nothing): all rows pass through. */
  @Test
  public void testCdfTable_excludeRegex_regularStream(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("excludeRegex", "no-file-matches-this-xyz")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cdf_exclude_regex");
    assertDataEquals(
        rows,
        Arrays.asList(
            RowFactory.create(1, "a"), RowFactory.create(2, "b"), RowFactory.create(3, "c")));
  }

  /** CDF table + Trigger.Once: one-shot consumes all rows. */
  @SuppressWarnings("deprecation")
  @Test
  public void testCdfTable_triggerOnce_regularStream(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));

    StreamingQuery q =
        spark
            .readStream()
            .table(str("dsv2.delta.`%s`", tablePath))
            .writeStream()
            .format("memory")
            .queryName("cdf_trigger_once")
            .outputMode("append")
            .trigger(Trigger.Once())
            .start();
    try {
      assertTrue(q.awaitTermination(60_000), "Trigger.Once should terminate on CDF table");
      List<Row> rows = spark.sql("SELECT * FROM cdf_trigger_once").collectAsList();
      assertDataEquals(
          rows,
          Arrays.asList(
              RowFactory.create(1, "a"), RowFactory.create(2, "b"), RowFactory.create(3, "c")));
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * CDF table + failOnDataLoss=false: the option must be accepted on a CDF-enabled regular stream
   * (no readChangeFeed). Sanity: with no pruning, all rows still arrive.
   */
  @Test
  public void testCdfTable_failOnDataLossFalse_regularStream(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cdf_fnl_false");
    assertDataEquals(rows, Arrays.asList(RowFactory.create(1, "a"), RowFactory.create(2, "b")));
  }

  /**
   * CDF table + concurrent writer: a non-readChangeFeed stream over a CDF-enabled table must drain
   * every committed row even while a writer is racing with the source.
   */
  @Test
  public void testCdfTable_concurrent_regularStream(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0, 'r-0')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();
      launchConcurrentInserter(writer, tablePath, stop, /* maxRows= */ 10, /* sleepMillis= */ 50L);
      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();
    } finally {
      stop.set(true);
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }

    long sourceCount = spark.read().format("delta").load(tablePath).count();
    long sinkCount = spark.read().parquet(outputDir.getAbsolutePath()).count();
    assertEquals(
        sourceCount,
        sinkCount,
        () ->
            "CDF-table concurrent-append stream missed rows: source="
                + sourceCount
                + " sink="
                + sinkCount);
  }

  /**
   * CDF table + OPTIMIZE between runs: OPTIMIZE produces dataChange=false commits, which the
   * regular (non-CDC) stream must not re-emit.
   */
  @Test
  public void testCdfTable_optimizeBetween_regularStream(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    drainToParquet(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);

    runOptimize(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'd')", tablePath));

    List<Row> rows = drainToParquet(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    assertDataEquals(
        rows,
        Arrays.asList(
            RowFactory.create(1, "a"),
            RowFactory.create(2, "b"),
            RowFactory.create(3, "c"),
            RowFactory.create(4, "d")));
  }

  /**
   * CDF + DV table + DV-DELETE + skipChangeCommits: the DV delete is a change commit that must be
   * skipped; the stream emits only the pre-delete snapshot rows.
   */
  @Test
  public void testCdfTable_dvDelete_skipChangeCommits_regularStream(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfDvTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 2", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cdf_dv_skip");
    // skipChangeCommits drops the DV-delete; the initial snapshot still reflects the DV filter,
    // so row id=2 does not appear.
    assertDataEquals(rows, Arrays.asList(RowFactory.create(1, "a"), RowFactory.create(3, "c")));
  }

  /**
   * CDF table + ADD COLUMN (additive): no schemaTrackingLocation needed. New rows carry the added
   * column; pre-change rows project NULL.
   */
  @Test
  public void testCdfTable_addColumn_regularStream(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b', 'x')", tablePath));

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cdf_add_column");
    assertEquals(2, rows.size(), () -> "Expected 2 rows after ADD COLUMN, got: " + rows);
    Map<Integer, String> idToExtra = new HashMap<>();
    for (Row r : rows) {
      idToExtra.put(r.getInt(0), r.isNullAt(2) ? null : r.getString(2));
    }
    assertNull(idToExtra.get(1), "Pre-change row should project NULL for extra");
    assertEquals("x", idToExtra.get(2), "Post-change row should carry extra");
  }

  /**
   * Cross-reference: basic CDF-table restart is already covered by {@link
   * V2CDCStreamingReadTest#testCdfTableStreamRestart}. This case exercises the same shape but with
   * a maxFilesPerTrigger=1 stream to verify rate-limit state survives the restart on a CDF table.
   */
  @Test
  public void testCdfTable_restart_regularStream(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    Dataset<Row> df1 = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);
    drainToParquet(df1, outputDir, checkpointDir);

    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    Dataset<Row> df2 = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);
    List<Row> rows = drainToParquet(df2, outputDir, checkpointDir);
    assertDataEquals(
        rows,
        Arrays.asList(
            RowFactory.create(1, "a"), RowFactory.create(2, "b"), RowFactory.create(3, "c")));
  }

  /** CM-id partitioned table + startingVersion. */
  @Test
  public void testCmId_partitioned_startingVersion(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdPartitionedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath)); // v1
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath)); // v2

    Dataset<Row> df =
        spark.readStream().option("startingVersion", "2").table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cmid_part_starting_version");
    assertDataEquals(rows, Arrays.asList(RowFactory.create(2, "b")));
  }

  /** CM-id partitioned table + maxBytesPerTrigger. */
  @Test
  public void testCmId_partitioned_maxBytesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdPartitionedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cmid_part_max_bytes");
    assertDataEquals(
        rows,
        Arrays.asList(
            RowFactory.create(1, "a"), RowFactory.create(2, "b"), RowFactory.create(3, "c")));
  }

  /** CM-id + RowTracking + startingVersion. */
  @Test
  public void testCmId_rt_startingVersion(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdRtTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'Bob')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Charlie')", tablePath));

    Dataset<Row> df =
        spark.readStream().option("startingVersion", "2").table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cmid_rt_starting_version");
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getLong(0));
    }
    assertEquals(Set.of(2L, 3L), ids);
  }

  /** CM-id + RowTracking + failOnDataLoss=false (no pruning -> just confirms option accepted). */
  @Test
  public void testCmId_rt_failOnDataLossFalse(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdRtTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cmid_rt_fnl");
    assertEquals(2, rows.size(), () -> "Expected 2 rows, got: " + rows);
  }

  /** CM-id + DV + DV-delete + skipChangeCommits. */
  @Test
  public void testCmId_dvDelete_skipChangeCommits(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta TBLPROPERTIES ("
                + "  'delta.columnMapping.mode' = 'id',"
                + "  'delta.enableDeletionVectors' = 'true',"
                + "  'delta.minReaderVersion' = '3',"
                + "  'delta.minWriterVersion' = '7')",
            tablePath));
    spark
        .range(10)
        .selectExpr("cast(id as int) as id", "cast(id as string) as name")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cmid_dv_skip");
    // Initial snapshot reflects the DV filter; skipChangeCommits drops the change commit only.
    assertEquals(7, rows.size(), () -> "Expected 7 surviving rows; got: " + rows);
  }

  /** CM-id + concurrent writer. */
  @Test
  public void testCmId_concurrent_writer(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0, 'r-0')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();
      launchConcurrentInserter(writer, tablePath, stop, /* maxRows= */ 10, /* sleepMillis= */ 50L);
      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();
    } finally {
      stop.set(true);
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }

    long sourceCount = spark.read().format("delta").load(tablePath).count();
    long sinkCount = spark.read().parquet(outputDir.getAbsolutePath()).count();
    assertEquals(
        sourceCount,
        sinkCount,
        () ->
            "CM-id concurrent-append stream missed rows: source="
                + sourceCount
                + " sink="
                + sinkCount);
  }

  /** CM-id + OPTIMIZE between runs: dataChange=false rewrite must not be re-emitted. */
  @Test
  public void testCmId_optimizeBetweenRuns(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    drainToParquet(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    runOptimize(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'd')", tablePath));

    List<Row> rows = drainToParquet(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    assertDataEquals(
        rows,
        Arrays.asList(
            RowFactory.create(1, "a"),
            RowFactory.create(2, "b"),
            RowFactory.create(3, "c"),
            RowFactory.create(4, "d")));
  }

  /** CM-id + ADD COLUMN + restart (additive). */
  @Test
  public void testCmId_addColumn_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    drainToParquet(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);

    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b', 'x')", tablePath));

    List<Row> rows = drainToParquet(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    assertEquals(2, rows.size(), () -> "Expected 2 rows after ADD COLUMN restart, got: " + rows);
  }

  /** Variant + concurrent writer: payloads must remain aligned to ids even under racing commits. */
  @Test
  public void testVariant_concurrent_writer(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createVariantTable(tablePath);
    appendVariantRows(tablePath, 0, 1); // seed row id=0

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(dsv2Ref)
            .selectExpr("id", "variant_get(v, '$.row', 'int') AS v_row");

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    final String finalTablePath = tablePath;
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();
      writer.submit(
          () -> {
            int i = 1;
            while (!stop.get() && i <= 10) {
              try {
                appendVariantRows(finalTablePath, i, 1);
                Thread.sleep(50);
                i++;
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
              } catch (Exception ignored) {
              }
            }
          });
      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();
    } finally {
      stop.set(true);
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }

    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    for (Row r : sink) {
      int id = r.getInt(0);
      Object vRowObj = r.get(1);
      assertNotNull(vRowObj, () -> "variant_get returned NULL for id=" + id);
      int vRow = ((Number) vRowObj).intValue();
      assertEquals(
          id,
          vRow,
          () -> "Variant payload misaligned under concurrent writer: id=" + id + " v=" + vRow);
    }
  }

  /**
   * Variant + OPTIMIZE between runs: dataChange=false rewrite must not perturb variant payloads or
   * re-emit rows.
   */
  @Test
  public void testVariant_optimizeBetween(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createVariantTable(tablePath);
    appendVariantRows(tablePath, 1, 1);
    appendVariantRows(tablePath, 2, 1);
    appendVariantRows(tablePath, 3, 1);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    drainToParquet(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);
    runOptimize(tablePath);
    appendVariantRows(tablePath, 4, 1);

    drainToParquet(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);

    Dataset<Row> sinkDf = spark.read().parquet(outputDir.getAbsolutePath());
    sinkDf.createOrReplaceTempView("variant_opt_sink");
    List<Row> rows =
        spark
            .sql(
                "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM variant_opt_sink ORDER BY id")
            .collectAsList();
    assertEquals(4, rows.size(), () -> "Expected 4 rows after OPTIMIZE+restart, got: " + rows);
    int expectedId = 1;
    for (Row r : rows) {
      int id = r.getInt(0);
      assertEquals(expectedId, id);
      int vRow = ((Number) r.get(1)).intValue();
      assertEquals(id, vRow, () -> "Variant misaligned after OPTIMIZE: id=" + id + " v=" + vRow);
      expectedId++;
    }
  }

  /** Variant + ADD COLUMN + restart: additive change, no schemaTrackingLocation needed. */
  @Test
  public void testVariant_addColumn_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createVariantTable(tablePath);
    appendVariantRows(tablePath, 1, 3);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    drainToParquet(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);

    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark
        .range(4, 7)
        .selectExpr(
            "cast(id as int) as id",
            "parse_json(concat('{\"row\":', id, '}')) as v",
            "cast(id as string) as extra")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    drainToParquet(spark.readStream().table(dsv2Ref), outputDir, checkpointDir);

    Dataset<Row> sinkDf = spark.read().parquet(outputDir.getAbsolutePath());
    sinkDf.createOrReplaceTempView("variant_addcol_sink");
    List<Row> rows =
        spark
            .sql(
                "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM variant_addcol_sink"
                    + " ORDER BY id")
            .collectAsList();
    assertEquals(6, rows.size(), () -> "Expected 6 rows after ADD COLUMN restart, got: " + rows);
    int expectedId = 1;
    for (Row r : rows) {
      assertEquals(expectedId, r.getInt(0));
      assertEquals(expectedId, ((Number) r.get(1)).intValue());
      expectedId++;
    }
  }

  /** Variant + maxBytesPerTrigger (byte rate limiter). */
  @Test
  public void testVariant_maxBytesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createVariantTable(tablePath);
    for (int i = 1; i <= 4; i++) {
      appendVariantRows(tablePath, i, 1);
    }

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath))
            .selectExpr("id", "variant_get(v, '$.row', 'int') AS v_row");

    StreamingQuery query = null;
    try {
      query =
          df.writeStream()
              .format("memory")
              .queryName("variant_max_bytes")
              .outputMode("append")
              .start();
      query.processAllAvailable();
      List<Row> rows =
          spark.sql("SELECT id, v_row FROM variant_max_bytes ORDER BY id").collectAsList();
      assertEquals(4, rows.size(), () -> "Expected 4 rows under maxBytesPerTrigger, got: " + rows);
      int expectedId = 1;
      for (Row r : rows) {
        int id = r.getInt(0);
        int vRow = ((Number) r.get(1)).intValue();
        assertEquals(expectedId, id);
        assertEquals(id, vRow);
        expectedId++;
      }
    } finally {
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
  }

  /**
   * Variant + partitioned table + failOnDataLoss=false: option must be accepted and no rows are
   * lost in the happy path (no pruning).
   */
  @Test
  public void testVariant_failOnDataLossFalse_partitioned(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, v VARIANT, p STRING) USING delta "
                + "PARTITIONED BY (p)",
            tablePath));
    spark
        .range(1, 4)
        .selectExpr(
            "cast(id as int) as id",
            "parse_json(concat('{\"row\":', id, '}')) as v",
            "cast(id as string) as p")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath))
            .selectExpr("id", "variant_get(v, '$.row', 'int') AS v_row");

    StreamingQuery query = null;
    try {
      query =
          df.writeStream()
              .format("memory")
              .queryName("variant_fnl_part")
              .outputMode("append")
              .start();
      query.processAllAvailable();
      List<Row> rows =
          spark.sql("SELECT id, v_row FROM variant_fnl_part ORDER BY id").collectAsList();
      assertEquals(3, rows.size(), () -> "Expected 3 rows, got: " + rows);
      int expectedId = 1;
      for (Row r : rows) {
        assertEquals(expectedId, r.getInt(0));
        assertEquals(expectedId, ((Number) r.get(1)).intValue());
        expectedId++;
      }
    } finally {
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
  }

  /** RT + partitioned table + project row_id. */
  @Test
  public void testRt_partitioned_projectRowId(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createRtPartitionedTable(tablePath);
    spark
        .range(0, 4)
        .selectExpr("id", "concat('name-', cast(id as string)) as name", "cast(id as string) as p")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    Dataset<Row> df =
        spark
            .readStream()
            .table(str("dsv2.delta.`%s`", tablePath))
            .selectExpr("id", "_metadata.row_id AS row_id");
    List<Row> rows = processStreamingQuery(df, "rt_part_row_id");
    assertEquals(4, rows.size());
    Map<Long, Long> idToRowId = new HashMap<>();
    for (Row r : rows) {
      idToRowId.put(r.getLong(0), r.getLong(1));
    }
    // Single-file initial write: row_id == id.
    assertEquals(Set.of(0L, 1L, 2L, 3L), new HashSet<>(idToRowId.values()));
  }

  /** RT + excludeRegex (matches nothing) + project row_id. */
  @Test
  public void testRt_excludeRegex_projectRowId(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createRowTrackedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("excludeRegex", "no-file-matches-this-xyz")
            .table(str("dsv2.delta.`%s`", tablePath))
            .selectExpr("id", "_metadata.row_id AS row_id");
    List<Row> rows = processStreamingQuery(df, "rt_excl_row_id");
    assertEquals(3, rows.size());
    for (Row r : rows) {
      assertFalse(r.isNullAt(1), () -> "row_id must be assigned for RT table, got: " + r);
    }
  }

  /** RT + failOnDataLoss=false + project row_id (happy path, no pruning). */
  @Test
  public void testRt_failOnDataLossFalse_projectRowId(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createRowTrackedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath))
            .selectExpr("id", "_metadata.row_id AS row_id");
    List<Row> rows = processStreamingQuery(df, "rt_fnl_row_id");
    assertEquals(2, rows.size());
    for (Row r : rows) {
      assertFalse(r.isNullAt(1));
    }
  }

  /** RT + concurrent writer + project row_id: surviving rows must carry stable row_ids. */
  @Test
  public void testRt_concurrent_writer_projectRowId(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createRowTrackedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0, 'r-0')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .table(str("dsv2.delta.`%s`", tablePath))
            .selectExpr("id", "_metadata.row_id AS row_id");

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();
      launchConcurrentInserter(writer, tablePath, stop, /* maxRows= */ 10, /* sleepMillis= */ 50L);
      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();
    } finally {
      stop.set(true);
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }

    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    Set<Long> rowIds = new HashSet<>();
    for (Row r : sink) {
      assertFalse(r.isNullAt(1), () -> "row_id must be assigned, got: " + r);
      rowIds.add(r.getLong(1));
    }
    assertEquals(
        sink.size(),
        rowIds.size(),
        () ->
            "row_ids must be unique across concurrent inserts; got " + sink + " rowIds=" + rowIds);
  }

  /** RT + OPTIMIZE between runs: row_ids must remain stable across the dataChange=false rewrite. */
  @Test
  public void testRt_optimizeBetween_idsStable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createRowTrackedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    // Capture pre-OPTIMIZE row_ids via batch.
    List<Row> beforeRows =
        spark
            .sql(
                str(
                    "SELECT id, _metadata.row_id AS row_id FROM dsv2.delta.`%s` ORDER BY id",
                    tablePath))
            .collectAsList();
    Map<Long, Long> beforeIdToRowId = new HashMap<>();
    for (Row r : beforeRows) {
      beforeIdToRowId.put(r.getLong(0), r.getLong(1));
    }

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df1 =
        spark.readStream().table(dsv2Ref).selectExpr("id", "_metadata.row_id AS row_id");
    drainToParquet(df1, outputDir, checkpointDir);

    runOptimize(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'd')", tablePath));

    Dataset<Row> df2 =
        spark.readStream().table(dsv2Ref).selectExpr("id", "_metadata.row_id AS row_id");
    drainToParquet(df2, outputDir, checkpointDir);

    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    Map<Long, Long> afterIdToRowId = new HashMap<>();
    for (Row r : sink) {
      afterIdToRowId.put(r.getLong(0), r.getLong(1));
    }
    // Pre-OPTIMIZE ids 1,2,3 must keep their original row_ids; id=4 is new (not pre-checked).
    for (Map.Entry<Long, Long> e : beforeIdToRowId.entrySet()) {
      assertEquals(
          e.getValue(),
          afterIdToRowId.get(e.getKey()),
          () ->
              "row_id mismatch under RT after OPTIMIZE: id="
                  + e.getKey()
                  + " pre="
                  + e.getValue()
                  + " post="
                  + afterIdToRowId.get(e.getKey()));
    }
  }

  /** RT + ADD COLUMN + restart: row_ids must remain stable across the additive schema change. */
  @Test
  public void testRt_addColumn_restart_idsStable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createRowTrackedTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    List<Row> beforeRows =
        spark
            .sql(
                str(
                    "SELECT id, _metadata.row_id AS row_id FROM dsv2.delta.`%s` ORDER BY id",
                    tablePath))
            .collectAsList();
    Map<Long, Long> beforeIdToRowId = new HashMap<>();
    for (Row r : beforeRows) {
      beforeIdToRowId.put(r.getLong(0), r.getLong(1));
    }

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df1 =
        spark.readStream().table(dsv2Ref).selectExpr("id", "_metadata.row_id AS row_id");
    drainToParquet(df1, outputDir, checkpointDir);

    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN extra STRING", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c', 'x')", tablePath));

    Dataset<Row> df2 =
        spark.readStream().table(dsv2Ref).selectExpr("id", "_metadata.row_id AS row_id");
    drainToParquet(df2, outputDir, checkpointDir);

    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    Map<Long, Long> afterIdToRowId = new HashMap<>();
    for (Row r : sink) {
      afterIdToRowId.put(r.getLong(0), r.getLong(1));
    }
    for (Map.Entry<Long, Long> e : beforeIdToRowId.entrySet()) {
      assertEquals(
          e.getValue(),
          afterIdToRowId.get(e.getKey()),
          () ->
              "row_id mismatch under RT after ADD COLUMN+restart: id="
                  + e.getKey()
                  + " pre="
                  + e.getValue()
                  + " post="
                  + afterIdToRowId.get(e.getKey()));
    }
  }

  /** CM-name + RT + DV + partitioned: basic sanity stream after a DV-delete. */
  @Test
  public void testCmName_rt_dv_partitioned_basic(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmNameRtDvPartitionedTable(tablePath);
    spark
        .range(10)
        .selectExpr(
            "id", "concat('name-', cast(id as string)) as name", "cast(id %% 2 as string) as p")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cmrtdv_part_basic");
    assertEquals(5, rows.size(), () -> "Expected 5 surviving odd rows, got: " + rows);
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getLong(0));
    }
    assertEquals(Set.of(1L, 3L, 5L, 7L, 9L), ids);
  }

  /** CM-name + RT + DV + partitioned + excludeRegex (matches nothing). */
  @Test
  public void testCmName_rt_dv_partitioned_excludeRegex(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmNameRtDvPartitionedTable(tablePath);
    spark
        .range(6)
        .selectExpr(
            "id", "concat('name-', cast(id as string)) as name", "cast(id %% 2 as string) as p")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    Dataset<Row> df =
        spark
            .readStream()
            .option("excludeRegex", "no-file-matches-this-xyz")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cmrtdv_part_excl");
    assertEquals(6, rows.size(), () -> "Expected 6 rows passing excludeRegex, got: " + rows);
  }

  /** CM-name + RT + DV + partitioned + failOnDataLoss=false (happy path, no pruning). */
  @Test
  public void testCmName_rt_dv_partitioned_failOnDataLossFalse(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmNameRtDvPartitionedTable(tablePath);
    spark
        .range(4)
        .selectExpr(
            "id", "concat('name-', cast(id as string)) as name", "cast(id %% 2 as string) as p")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    Dataset<Row> df =
        spark
            .readStream()
            .option("failOnDataLoss", "false")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "cmrtdv_part_fnl");
    assertEquals(4, rows.size(), () -> "Expected 4 rows under fnl=false, got: " + rows);
  }

  /** CM-id + RT + DV + partitioned: basic sanity (mirrors the CM-name variant). */
  @Test
  public void testCmId_rt_dv_partitioned_basic(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdRtDvPartitionedTable(tablePath);
    spark
        .range(10)
        .selectExpr(
            "id", "concat('name-', cast(id as string)) as name", "cast(id %% 2 as string) as p")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .table(str("dsv2.delta.`%s`", tablePath))
            .selectExpr("id", "name", "_metadata.row_id AS row_id");
    List<Row> rows = processStreamingQuery(df, "cmid_rtdv_part_basic");
    assertEquals(5, rows.size(), () -> "Expected 5 surviving odd rows, got: " + rows);
    for (Row r : rows) {
      long id = r.getLong(0);
      assertEquals(1L, id % 2, "Only odd ids should survive");
      assertEquals("name-" + id, r.getString(1));
      assertFalse(r.isNullAt(2), "row_id must be assigned under CM-id+RT+DV partitioned");
    }
  }

  /** CM-id + RT + DV + partitioned + maxBytesPerTrigger (byte rate limiter). */
  @Test
  public void testCmId_rt_dv_partitioned_maxBytesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCmIdRtDvPartitionedTable(tablePath);
    // Three single-file commits across two partitions so the rate limiter has work to slice.
    for (long i = 0; i < 3; i++) {
      final long finalI = i;
      spark
          .range(finalI * 2, finalI * 2 + 2)
          .selectExpr(
              "id", "concat('name-', cast(id as string)) as name", "cast(id %% 2 as string) as p")
          .coalesce(1)
          .write()
          .format("delta")
          .mode("append")
          .save(tablePath);
    }

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath))
            .selectExpr("id", "name");
    List<Row> rows = processStreamingQuery(df, "cmid_rtdv_part_max_bytes");
    assertEquals(6, rows.size(), () -> "Expected 6 rows across rate-limited commits, got: " + rows);
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getLong(0));
      assertEquals("name-" + r.getLong(0), r.getString(1));
    }
    assertEquals(Set.of(0L, 1L, 2L, 3L, 4L, 5L), ids);
  }
}
