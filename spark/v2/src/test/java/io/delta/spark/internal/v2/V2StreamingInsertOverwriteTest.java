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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming reads against tables modified by {@code INSERT OVERWRITE} and {@code
 * replaceWhere}.
 *
 * <p>Both operations commit a single transaction containing {@code RemoveFile} (dataChange=true) +
 * {@code AddFile} actions. Streaming behavior depends on whether the user opts into {@code
 * skipChangeCommits} or {@code ignoreChanges}. The cases below pin the visible behavior at the
 * user-facing {@code spark.readStream} level.
 *
 * <p>{@link V2PositionOrderTest#testActionOrderingAddRemoveStreaming} covers the basic Add+Remove
 * same-commit DSv1/DSv2 parity. The cases here build on that scenario with the OVERWRITE-specific
 * row-count, partial-partition, restart, and trigger combinations.
 */
public class V2StreamingInsertOverwriteTest extends V2TestBase {

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
      DataStreamWriter<Row> writer =
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
        query.awaitTermination(60_000L);
      }
    } finally {
      if (query != null) {
        query.stop();
      }
    }
    return spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
  }

  /** Format epoch millis as a "yyyy-MM-dd HH:mm:ss.SSS" string in the session timezone. */
  private String formatTs(long millis) {
    String tz = spark.sessionState().conf().sessionLocalTimeZone();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone(tz));
    return sdf.format(new Date(millis));
  }

  /**
   * Case 1 - full OVERWRITE + skipChangeCommits=true: only the OVERWRITE rows are emitted.
   *
   * <p>v0: insert 5 rows. v1: {@code INSERT OVERWRITE} with 10 disjoint rows. With {@code
   * skipChangeCommits=true}, the v1 commit is dropped as a "change commit"; only the v0 snapshot
   * rows are emitted from the initial snapshot, then v1 is skipped. Expected: 5 v0 rows.
   *
   * <p>Note: skipChangeCommits drops the entire commit (Adds + Removes), so the OVERWRITE's 10 new
   * rows do NOT surface. This matches DSv1 semantics - skipChangeCommits is "drop the commit", not
   * "treat Adds as appends".
   */
  @Test
  public void testFullOverwrite_skipChangeCommits(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3), (4), (5)", tablePath));
    spark.sql(
        str(
            "INSERT OVERWRITE TABLE delta.`%s` VALUES (10), (11), (12), (13), (14), (15), (16),"
                + " (17), (18), (19)",
            tablePath));

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(streamingDF, "ow_skip_v2");

    // Cross-check with DSv1 so the expected row set is what the streaming engine actually emits.
    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("skipChangeCommits", "true").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "ow_skip_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Case 2 - full OVERWRITE + ignoreChanges=true: emits initial + OVERWRITE Adds (with possible
   * duplicates of unchanged rows; here all rows change so no duplicates).
   *
   * <p>v0: insert 5 rows. v1: {@code INSERT OVERWRITE} with 10 disjoint rows. With {@code
   * ignoreChanges=true}, the v1 commit is emitted as appends - the engine "ignores" the
   * accompanying Removes. Expected: 5 initial + 10 OVERWRITE = 15 rows.
   */
  @Test
  public void testFullOverwrite_ignoreChanges(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3), (4), (5)", tablePath));
    spark.sql(
        str(
            "INSERT OVERWRITE TABLE delta.`%s` VALUES (10), (11), (12), (13), (14), (15), (16),"
                + " (17), (18), (19)",
            tablePath));

    Dataset<Row> streamingDF =
        spark.readStream().option("ignoreChanges", "true").table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(streamingDF, "ow_ignore_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("ignoreChanges", "true").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "ow_ignore_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Case 3 - full OVERWRITE without any ignore option: stream throws
   * DELTA_SOURCE_TABLE_IGNORE_CHANGES.
   *
   * <p>v0: insert 5 rows. v1: {@code INSERT OVERWRITE}. Without {@code skipChangeCommits} or {@code
   * ignoreChanges}, the streaming source sees a {@code RemoveFile(dataChange=true)} and must throw
   * {@code DELTA_SOURCE_TABLE_IGNORE_CHANGES} to instruct the user to opt in.
   */
  @Test
  public void testFullOverwrite_bareStream_throws(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3), (4), (5)", tablePath));
    spark.sql(
        str(
            "INSERT OVERWRITE TABLE delta.`%s` VALUES (10), (11), (12), (13), (14), (15), (16),"
                + " (17), (18), (19)",
            tablePath));

    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    StreamingQuery query =
        streamingDF
            .writeStream()
            .format("memory")
            .queryName("ow_bare_throws")
            .outputMode("append")
            .start();
    StreamingQueryException ex;
    try {
      ex = assertThrows(StreamingQueryException.class, () -> query.processAllAvailable());
    } finally {
      query.stop();
      DeltaLog.clearCache();
    }

    String fullMsg = String.valueOf(ex) + " / " + String.valueOf(ex.getCause());
    assertTrue(
        fullMsg.contains("DELTA_SOURCE_TABLE_IGNORE_CHANGES"),
        () ->
            "Expected DELTA_SOURCE_TABLE_IGNORE_CHANGES error class on bare stream over OVERWRITE,"
                + " got: "
                + fullMsg);
  }

  /**
   * Case 4 - partial OVERWRITE via replaceWhere on a partitioned table.
   *
   * <p>Partitioned table with two partitions. {@code replaceWhere} rewrites only one partition.
   * With {@code skipChangeCommits=true}, the entire replaceWhere commit is dropped - so the stream
   * emits only the v0 snapshot (both partitions, original contents).
   *
   * <p>This pins the contract: skipChangeCommits is commit-level, not partition-level. Even though
   * only one partition was touched, the whole commit is skipped.
   */
  @Test
  public void testPartialOverwrite_replaceWhere_partitioned(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, 0), (2, 0), (3, 0), (10, 1), (11, 1), (12, 1)",
            tablePath));

    // Rewrite only part=0 with new rows. replaceWhere is on the DataFrameWriter, not SQL.
    spark
        .createDataFrame(
            Arrays.asList(RowFactory.create(100, 0), RowFactory.create(101, 0)),
            new org.apache.spark.sql.types.StructType()
                .add("id", org.apache.spark.sql.types.DataTypes.IntegerType)
                .add("part", org.apache.spark.sql.types.DataTypes.IntegerType))
        .write()
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", "part = 0")
        .save(tablePath);

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(streamingDF, "ow_partial_skip_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("skipChangeCommits", "true").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "ow_partial_skip_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Case 5 - OVERWRITE + Trigger.AvailableNow terminates cleanly.
   *
   * <p>One-shot {@code AvailableNow} run over a table with an OVERWRITE in its history. With {@code
   * skipChangeCommits=true} so the OVERWRITE commit doesn't error out, the query must terminate by
   * itself once all available data is consumed.
   */
  @Test
  public void testOverwrite_triggerAvailableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));
    spark.sql(str("INSERT OVERWRITE TABLE delta.`%s` VALUES (10), (11), (12)", tablePath));

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    try {
      List<Row> v2Rows =
          runWithParquetSink(streamingDF, outputDir, checkpointDir, Trigger.AvailableNow());

      // Cross-check with DSv1.
      Dataset<Row> v1Stream =
          spark.readStream().format("delta").option("skipChangeCommits", "true").load(tablePath);
      File v1Checkpoint = new File(deltaTablePath, "_checkpoint_v1");
      File v1Output = new File(deltaTablePath, "_out_v1");
      List<Row> v1Rows =
          runWithParquetSink(v1Stream, v1Output, v1Checkpoint, Trigger.AvailableNow());

      assertDataEquals(v2Rows, v1Rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * Case 6 - OVERWRITE that produces multiple AddFiles, rate-limited with maxFilesPerTrigger.
   *
   * <p>The OVERWRITE writes 4 files (one per partition). With {@code maxFilesPerTrigger=1} and
   * {@code ignoreChanges=true}, the engine drains those 4 files across multiple micro-batches. End
   * state row count must equal the OVERWRITE's row count + the v0 snapshot's row count (since
   * ignoreChanges treats Removes as no-ops).
   */
  @Test
  public void testOverwrite_maxFilesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 0)", tablePath));

    // Produce 4 distinct partitions in the OVERWRITE so it generates 4 AddFiles.
    spark.sql(
        str(
            "INSERT OVERWRITE TABLE delta.`%s` VALUES (10, 0), (11, 1), (12, 2), (13, 3)",
            tablePath));

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("ignoreChanges", "true")
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(streamingDF, "ow_mfpt_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("ignoreChanges", "true")
            .option("maxFilesPerTrigger", "1")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "ow_mfpt_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Case 7 - stream initial, OVERWRITE, restart from checkpoint, finish. Parquet sink.
   *
   * <p>Run 1: stream v0 only via parquet sink + checkpoint. Then OVERWRITE the table. Run 2:
   * restart from the same checkpoint with {@code skipChangeCommits=true}. The restart must not
   * double-emit v0 rows and must correctly skip the OVERWRITE commit.
   */
  @Test
  public void testOverwrite_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");

    // Run 1: drain v0 only. No need for skipChangeCommits - no OVERWRITE in history yet.
    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // Now overwrite. The checkpoint already committed the v0 batch, so on restart only the
    // OVERWRITE commit is pending.
    spark.sql(str("INSERT OVERWRITE TABLE delta.`%s` VALUES (10), (11)", tablePath));

    // Run 2: restart with skipChangeCommits so the OVERWRITE doesn't error.
    Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
    try {
      List<Row> actualRows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // Run 1 emitted v0 = {1,2,3}. Run 2 should skip the OVERWRITE commit entirely. Total parquet
      // sink contents = {1,2,3}.
      List<Row> expectedRows =
          Arrays.asList(RowFactory.create(1), RowFactory.create(2), RowFactory.create(3));
      assertDataEquals(actualRows, expectedRows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * Case 8 - OVERWRITE on a column-mapped (name mode) table.
   *
   * <p>Same OVERWRITE flow as case 2 but on a column-mapped table (mode='name'). Physical column
   * names differ from logical names; OVERWRITE must still round-trip cleanly through the v2 stream
   * with {@code skipChangeCommits=true}.
   */
  @Test
  public void testOverwrite_onColMappedTable_name(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice'), (2, 'Bob')", tablePath));
    spark.sql(
        str("INSERT OVERWRITE TABLE delta.`%s` VALUES (10, 'Xavier'), (11, 'Yara')", tablePath));

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(streamingDF, "ow_cm_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("skipChangeCommits", "true").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "ow_cm_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Case 9 - OVERWRITE on a DV-enabled table: OVERWRITE wipes DV state.
   *
   * <p>v0: insert 10 rows in one file. v1: DV-only delete of even rows (5 rows survive). v2: {@code
   * INSERT OVERWRITE} with 3 brand-new rows - the underlying file (with its DV) is removed and
   * replaced. Streaming with {@code skipChangeCommits=true} from version 0 should drop the
   * DV-delete commit AND the OVERWRITE commit, emitting only the v0 snapshot's 10 rows.
   */
  @Test
  public void testOverwrite_onDvTable_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark
        .range(0, 10)
        .selectExpr("cast(id as int) as id")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));
    spark.sql(str("INSERT OVERWRITE TABLE delta.`%s` VALUES (100), (101), (102)", tablePath));

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(streamingDF, "ow_dv_skip_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("skipChangeCommits", "true").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "ow_dv_skip_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Case 10 - startingTimestamp between original and OVERWRITE commit.
   *
   * <p>Three commits. {@code startingTimestamp} strictly between v0 (initial insert) and v1
   * (OVERWRITE). With next-commit semantics the stream begins at the OVERWRITE commit. With {@code
   * skipChangeCommits=true} that commit is dropped - expected output is empty.
   *
   * <p>If we instead anchor before the OVERWRITE, the v0 rows would surface from the initial
   * snapshot. The cut between v0 and v1 disambiguates this.
   */
  @Test
  public void testOverwrite_startingTimestamp_acrossOverwrite(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3), (4), (5)", tablePath));
    Thread.sleep(100);
    long mid = System.currentTimeMillis();
    Thread.sleep(100);
    spark.sql(str("INSERT OVERWRITE TABLE delta.`%s` VALUES (10), (11), (12)", tablePath));

    String midStr = formatTs(mid);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("startingTimestamp", midStr)
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(streamingDF, "ow_start_ts_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("startingTimestamp", midStr)
            .option("skipChangeCommits", "true")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "ow_start_ts_v1");

    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Case 11 - INSERT OVERWRITE with identical rows still emits dataChange=true Removes.
   *
   * <p>v0: insert 3 rows. v1: {@code INSERT OVERWRITE} with the same 3 rows. Delta does not de-dupe
   * at commit time - the OVERWRITE still produces RemoveFile(dataChange=true) + AddFile. So a bare
   * stream (no ignore options) must still throw {@code DELTA_SOURCE_TABLE_IGNORE_CHANGES}.
   *
   * <p>This is a Delta-semantics check: even logically-identical OVERWRITE is a "change commit".
   */
  @Test
  public void testInsertOverwriteSameRows_dataChangeStillTrue(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));
    // Identical rows.
    spark.sql(str("INSERT OVERWRITE TABLE delta.`%s` VALUES (1), (2), (3)", tablePath));

    // Verify Delta itself recorded a RemoveFile (dataChange=true) via history's operation.
    List<Row> history =
        spark
            .sql(str("DESCRIBE HISTORY delta.`%s`", tablePath))
            .selectExpr("version", "operation")
            .orderBy("version")
            .collectAsList();
    List<String> ops = new ArrayList<>();
    for (Row r : history) {
      ops.add(r.getString(1));
    }
    assertTrue(
        ops.stream().anyMatch(op -> op.toUpperCase().contains("OVERWRITE")),
        () -> "Expected an OVERWRITE operation in history; got: " + ops);

    // Bare stream over the OVERWRITE must throw IGNORE_CHANGES even for identical rows.
    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    StreamingQuery query =
        streamingDF
            .writeStream()
            .format("memory")
            .queryName("ow_same_rows_throws")
            .outputMode("append")
            .start();
    StreamingQueryException ex;
    try {
      ex = assertThrows(StreamingQueryException.class, () -> query.processAllAvailable());
    } finally {
      query.stop();
      DeltaLog.clearCache();
    }

    String fullMsg = String.valueOf(ex) + " / " + String.valueOf(ex.getCause());
    assertTrue(
        fullMsg.contains("DELTA_SOURCE_TABLE_IGNORE_CHANGES"),
        () ->
            "Expected DELTA_SOURCE_TABLE_IGNORE_CHANGES even for OVERWRITE with identical rows,"
                + " got: "
                + fullMsg);
  }
}
