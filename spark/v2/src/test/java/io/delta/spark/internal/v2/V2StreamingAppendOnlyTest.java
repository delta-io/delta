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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming coverage for {@code delta.appendOnly=true} tables.
 *
 * <p>{@code appendOnly} is a writer-side table feature that rejects DELETE/UPDATE/MERGE with {@code
 * DELTA_CANNOT_MODIFY_APPEND_ONLY}. The cross-product matrix (see {@code
 * ~/markdown/testgap/COVERAGE_POST_CAMPAIGN.md}) had this row entirely uncovered for DSv2
 * streaming. Each test below pins one scenario cell against an append-only source.
 *
 * <p>Pattern mirrors {@link V2StreamingClusteredTest}: {@link #processStreamingQuery} for
 * non-restart cases, a parquet sink for restart cases (memory sink doesn't recover from
 * checkpoint).
 */
public class V2StreamingAppendOnlyTest extends V2TestBase {

  /**
   * Runs a streaming query against a parquet sink so the checkpoint can be replayed on restart.
   * Copied from {@link V2StreamingClusteredTest} - kept local to follow file-local convention.
   */
  private List<Row> runWithParquetSink(
      Dataset<Row> streamingDF, File outputDir, File checkpointDir, Trigger trigger)
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

  /** Creates an append-only table with the given schema and inserts initial data. */
  private void createAppendOnlyTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.appendOnly' = 'true')",
            tablePath));
  }

  /** 1. S1 - basic stream from an appendOnly table. */
  @Test
  public void testAppendOnlyBasic(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createAppendOnlyTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A'), (2, 'B')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    assertTrue(streamingDF.isStreaming());

    List<Row> actualRows = processStreamingQuery(streamingDF, "appendonly_basic");
    List<Row> expectedRows = Arrays.asList(RowFactory.create(1, "A"), RowFactory.create(2, "B"));
    assertDataEquals(actualRows, expectedRows);
  }

  /** 2. S2/S3 - startingVersion=N skips earlier commits on an appendOnly table. */
  @Test
  public void testAppendOnlyStartingVersion(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createAppendOnlyTable(tablePath); // v0
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath)); // v1
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath)); // v2
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath)); // v3

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("startingVersion", "2").table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "appendonly_startver");

    // Only v=2 and v=3 should be visible.
    List<Row> expectedRows = Arrays.asList(RowFactory.create(2, "B"), RowFactory.create(3, "C"));
    assertDataEquals(actualRows, expectedRows);
  }

  /** 3. S4 - startingTimestamp far-past consumes everything on an appendOnly table. */
  @Test
  public void testAppendOnlyStartingTimestamp(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createAppendOnlyTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A'), (2, 'B')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("startingTimestamp", "1970-01-01 00:00:00").table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "appendonly_startts");

    List<Row> expectedRows = Arrays.asList(RowFactory.create(1, "A"), RowFactory.create(2, "B"));
    assertDataEquals(actualRows, expectedRows);
  }

  /** 4. S5 - maxFilesPerTrigger=1 splits commits across batches on appendOnly. */
  @Test
  public void testAppendOnlyMaxFilesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createAppendOnlyTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "appendonly_mfpt");

    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1, "A"), RowFactory.create(2, "B"), RowFactory.create(3, "C"));
    assertDataEquals(actualRows, expectedRows);
  }

  /** 5. S6 - maxBytesPerTrigger small value still emits all rows eventually. */
  @Test
  public void testAppendOnlyMaxBytesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createAppendOnlyTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("maxBytesPerTrigger", "1b").table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "appendonly_mbpt");

    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1, "A"), RowFactory.create(2, "B"), RowFactory.create(3, "C"));
    assertDataEquals(actualRows, expectedRows);
  }

  /** 6. S7 - Trigger.AvailableNow consumes the whole appendOnly table in one shot. */
  @Test
  public void testAppendOnlyTriggerAvailableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createAppendOnlyTable(tablePath);
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

  /** 7. S8 - Trigger.Once consumes the appendOnly table in a single batch. */
  @SuppressWarnings("deprecation")
  @Test
  public void testAppendOnlyTriggerOnce(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createAppendOnlyTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A'), (2, 'B'), (3, 'C')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    StreamingQuery q =
        spark
            .readStream()
            .table(dsv2TableRef)
            .writeStream()
            .format("memory")
            .queryName("appendonly_once")
            .trigger(Trigger.Once())
            .start();
    try {
      assertTrue(q.awaitTermination(60_000));
      List<Row> rows =
          spark.sql("SELECT id, name FROM appendonly_once ORDER BY id").collectAsList();
      List<Row> expected =
          Arrays.asList(
              RowFactory.create(1, "A"), RowFactory.create(2, "B"), RowFactory.create(3, "C"));
      assertDataEquals(rows, expected);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /** 8. S8 - Trigger.ProcessingTime("0s") doesn't duplicate rows on appendOnly. */
  @Test
  public void testAppendOnlyTriggerProcessingTime(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createAppendOnlyTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A'), (2, 'B'), (3, 'C')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    StreamingQuery q =
        spark
            .readStream()
            .table(dsv2TableRef)
            .writeStream()
            .format("memory")
            .queryName("appendonly_pt")
            .trigger(Trigger.ProcessingTime(0, TimeUnit.SECONDS))
            .start();
    try {
      q.processAllAvailable();
      List<Row> rows = spark.sql("SELECT id, name FROM appendonly_pt ORDER BY id").collectAsList();
      assertEquals(3, rows.size(), () -> "expected 3 unique rows; got " + rows);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /** 9. S14 - restart from checkpoint picks up new commits on appendOnly. */
  @Test
  public void testAppendOnlyRestart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    createAppendOnlyTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    runWithParquetSink(streamingDF, outputDir, checkpointDir, /* trigger= */ null);

    // Second insert after the first run drains.
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

  /** 10. S15 - excludeRegex on an appendOnly table excludes matched files. */
  @Test
  public void testAppendOnlyExcludeRegex(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createAppendOnlyTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A'), (2, 'B')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    // Match every parquet file -> exclude everything; result must be empty.
    Dataset<Row> df =
        spark.readStream().option("excludeRegex", ".*\\.parquet$").table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(df, "appendonly_excl");
    assertEquals(
        0, rows.size(), () -> "expected 0 rows after excluding all parquet files; got " + rows);
  }

  /**
   * 11. DELETE attempt is rejected at write time on an appendOnly table; the stream is unaffected
   * by the rejected DELETE.
   */
  @Test
  public void testAppendOnlyDeleteRejected(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createAppendOnlyTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A'), (2, 'B')", tablePath));

    // DELETE must be rejected with DELTA_CANNOT_MODIFY_APPEND_ONLY.
    Exception ex =
        assertThrows(
            Exception.class,
            () -> spark.sql(str("DELETE FROM delta.`%s` WHERE id = 1", tablePath)));
    assertTrue(
        ex.toString().contains("DELTA_CANNOT_MODIFY_APPEND_ONLY")
            || ex.toString().contains("only allow appends"),
        () -> "expected DELTA_CANNOT_MODIFY_APPEND_ONLY error, got: " + ex);

    // Stream is unaffected by the rejected DELETE - all rows still visible.
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(df, "appendonly_del_rej");
    List<Row> expected = Arrays.asList(RowFactory.create(1, "A"), RowFactory.create(2, "B"));
    assertDataEquals(rows, expected);
  }

  /** 12. UPDATE attempt is rejected on an appendOnly table; stream unaffected. */
  @Test
  public void testAppendOnlyUpdateRejected(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createAppendOnlyTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A'), (2, 'B')", tablePath));

    Exception ex =
        assertThrows(
            Exception.class,
            () -> spark.sql(str("UPDATE delta.`%s` SET name = 'Z' WHERE id = 1", tablePath)));
    assertTrue(
        ex.toString().contains("DELTA_CANNOT_MODIFY_APPEND_ONLY")
            || ex.toString().contains("only allow appends"),
        () -> "expected DELTA_CANNOT_MODIFY_APPEND_ONLY error, got: " + ex);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(df, "appendonly_upd_rej");
    List<Row> expected = Arrays.asList(RowFactory.create(1, "A"), RowFactory.create(2, "B"));
    assertDataEquals(rows, expected);
  }

  /** 13. MERGE with non-insert clauses is rejected on appendOnly; stream unaffected. */
  @Test
  public void testAppendOnlyMergeRejected(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createAppendOnlyTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A'), (2, 'B')", tablePath));

    // Source view with overlapping + new ids; UPDATE clause must trigger the appendOnly rejection.
    spark
        .createDataFrame(
            Arrays.asList(RowFactory.create(1, "Z"), RowFactory.create(3, "C")),
            new org.apache.spark.sql.types.StructType()
                .add("id", org.apache.spark.sql.types.DataTypes.IntegerType, false)
                .add("name", org.apache.spark.sql.types.DataTypes.StringType, false))
        .createOrReplaceTempView("appendonly_merge_src");

    Exception ex =
        assertThrows(
            Exception.class,
            () ->
                spark.sql(
                    str(
                        "MERGE INTO delta.`%s` t USING appendonly_merge_src s ON t.id = s.id "
                            + "WHEN MATCHED THEN UPDATE SET t.name = s.name "
                            + "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
                        tablePath)));
    assertTrue(
        ex.toString().contains("DELTA_CANNOT_MODIFY_APPEND_ONLY")
            || ex.toString().contains("only allow appends"),
        () -> "expected DELTA_CANNOT_MODIFY_APPEND_ONLY error, got: " + ex);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(df, "appendonly_merge_rej");
    List<Row> expected = Arrays.asList(RowFactory.create(1, "A"), RowFactory.create(2, "B"));
    assertDataEquals(rows, expected);
  }

  /** 14. appendOnly + column-mapping (name mode) - basic stream works through CM rename layer. */
  @Test
  public void testAppendOnlyWithColumnMapping(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, user_name STRING) USING delta "
                + "TBLPROPERTIES ('delta.appendOnly' = 'true', 'delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "appendonly_cm");
    List<Row> expectedRows =
        Arrays.asList(RowFactory.create(1, "Alice"), RowFactory.create(2, "Bob"));
    assertDataEquals(actualRows, expectedRows);
  }

  /**
   * 15. appendOnly + DV interplay - DELETE is rejected even with DVs enabled, since the appendOnly
   * check fires before any DV-rewrite logic. Stream sees all original rows.
   */
  @Test
  public void testAppendOnlyWithDeletionVectors(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.appendOnly' = 'true', 'delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark
        .range(0, 5)
        .selectExpr("cast(id as int) as id")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // DELETE must be rejected by appendOnly even though DVs are enabled.
    Exception ex =
        assertThrows(
            Exception.class,
            () -> spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath)));
    assertTrue(
        ex.toString().contains("DELTA_CANNOT_MODIFY_APPEND_ONLY")
            || ex.toString().contains("only allow appends"),
        () -> "expected DELTA_CANNOT_MODIFY_APPEND_ONLY error, got: " + ex);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(df, "appendonly_dv");
    assertEquals(5, rows.size(), () -> "expected all 5 original rows; got " + rows);
  }

  /**
   * 16. S3 - startingVersion=latest on an appendOnly table skips pre-existing commits; only rows
   * appended after the stream pins endOffset should drain.
   */
  @Test
  public void testAppendOnlyStartingVersionLatest(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createAppendOnlyTable(tablePath);
    // Pre-latest commits the stream must NOT see.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'A')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'B')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'C')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> df = spark.readStream().option("startingVersion", "latest").table(dsv2TableRef);
    String queryName = "appendonly_sv_latest";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      // No post-latest commits yet -> empty drain.
      query.processAllAvailable();
      List<Row> beforeAppend = spark.sql("SELECT * FROM " + queryName).collectAsList();
      assertEquals(
          0,
          beforeAppend.size(),
          () -> "startingVersion=latest should skip pre-existing rows; got: " + beforeAppend);

      // Post-start appends should drain.
      spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'D')", tablePath));
      spark.sql(str("INSERT INTO delta.`%s` VALUES (5, 'E')", tablePath));
      query.processAllAvailable();

      List<Row> rows =
          spark.sql("SELECT id, name FROM " + queryName + " ORDER BY id").collectAsList();
      List<Row> expected = Arrays.asList(RowFactory.create(4, "D"), RowFactory.create(5, "E"));
      assertDataEquals(rows, expected);
    } finally {
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }
  }

  /** Spawn a concurrent appender that inserts {@code n} single-row commits until stopped. */
  private ExecutorService startConcurrentAppender(String tablePath, AtomicBoolean stop, int n) {
    ExecutorService writer = Executors.newSingleThreadExecutor();
    writer.submit(
        () -> {
          int i = 1;
          while (!stop.get() && i <= n) {
            try {
              spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, 'c%d')", tablePath, i + 100, i));
              Thread.sleep(50);
              i++;
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            } catch (Exception ignored) {
              // Concurrent commit may transiently fail; just continue.
            }
          }
        });
    return writer;
  }

  /** Stop the appender executor cleanly. */
  private void stopAppender(ExecutorService writer, AtomicBoolean stop) throws Exception {
    stop.set(true);
    writer.shutdownNow();
    writer.awaitTermination(5, TimeUnit.SECONDS);
  }

  /**
   * 17. S22 - concurrent writer x appendOnly. INSERTs are legal on appendOnly tables, so a
   * background appender running alongside a streaming reader must not surface any error and all
   * committed rows (initial + concurrent) should appear in the sink.
   */
  @Test
  public void testAppendOnlyConcurrentWriter(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    createAppendOnlyTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0, 'seed')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = null;
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
      writer = startConcurrentAppender(tablePath, stop, 10);
      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();
    } finally {
      if (writer != null) {
        stopAppender(writer, stop);
      }
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }

    // All committed rows (initial seed + concurrent inserts) must appear in the sink. Stream and
    // source see the same set since INSERTs are legal on appendOnly.
    long sourceCount = spark.read().format("delta").load(tablePath).count();
    List<Row> sink = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertEquals(
        sourceCount,
        (long) sink.size(),
        () ->
            "appendOnly stream missed concurrent commits: source="
                + sourceCount
                + " sink="
                + sink.size());
  }
}
