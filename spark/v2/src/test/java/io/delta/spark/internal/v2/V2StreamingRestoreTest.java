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
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

/**
 * DSv2 streaming coverage for {@code RESTORE TABLE ... TO VERSION AS OF}.
 *
 * <p>RESTORE writes a single commit containing {@code RemoveFile(dataChange=true)} for files in the
 * current snapshot that are absent in the target snapshot, and {@code AddFile(dataChange=true)} for
 * files in the target snapshot that are absent in the current snapshot. See {@code
 * RestoreTableCommand.scala}.
 *
 * <p>The cross-product matrix audit identified {@code RESTORE} as having zero DSv2 streaming
 * coverage. This file exercises the interaction with the streaming options that the DSv2 connector
 * supports today: {@code skipChangeCommits}, {@code ignoreChanges}, {@code startingVersion}, {@code
 * startingTimestamp}, {@code maxFilesPerTrigger}, and {@code Trigger.AvailableNow}.
 *
 * <p>Code-path notes (read from {@code SparkMicroBatchStream#validateCommitAndDecideSkipping}):
 *
 * <ul>
 *   <li>Without any ignore option, the RESTORE commit hits the {@code hasFileAdd &&
 *       !shouldAllowChanges} branch and throws {@code DELTA_SOURCE_TABLE_IGNORE_CHANGES}.
 *   <li>With {@code skipChangeCommits=true}, the entire RESTORE commit is dropped (no AddFiles
 *       emitted, no RemoveFiles emitted).
 *   <li>With {@code ignoreChanges=true}, the AddFiles in the RESTORE commit are emitted; the rows
 *       inside those files (i.e. the restored content) leak through to the stream.
 * </ul>
 */
public class V2StreamingRestoreTest extends V2TestBase {

  /** Format a UNIX-millis instant as the session-local-timezone string Spark parses. */
  private String formatTs(long millis) {
    String tz = spark.sessionState().conf().sessionLocalTimeZone();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone(tz));
    return sdf.format(new Date(millis));
  }

  /** Issue {@code RESTORE TABLE delta.<path> TO VERSION AS OF v}. */
  private void restoreTo(String tablePath, long version) {
    spark.sql(str("RESTORE TABLE delta.`%s` TO VERSION AS OF %d", tablePath, version));
  }

  /**
   * v0=CREATE; v1=insert(0..9); v2=insert(10..19); v3=insert(20..29); restore to v1 at v4. Stream
   * the whole table with {@code skipChangeCommits=true}.
   *
   * <p>Under {@code skipChangeCommits}, the RESTORE commit (v4) is dropped entirely (it contains
   * both AddFile and RemoveFile actions, all {@code dataChange=true}). The stream therefore reports
   * v1+v2+v3 = 30 rows from the AddFiles of the original three appends.
   */
  @Test
  public void testRestoreBetweenStreamStartAndEnd_withSkipChangeCommits(
      @TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark
        .range(0, 10)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v1
    spark
        .range(10, 20)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v2
    spark
        .range(20, 30)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v3
    restoreTo(tablePath, 1); // v4: RemoveFile(v2, v3 files) + AddFile(v1 files restored)

    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "restore_skip");

    // The RESTORE commit is fully skipped under skipChangeCommits=true; the stream sees the
    // pre-restore history (v1+v2+v3 appends = 30 rows). If the RESTORE commit leaked, we would
    // either get >30 (AddFiles for restored files re-emitted) or fail with IGNORE_CHANGES.
    assertEquals(
        30,
        rows.size(),
        () ->
            "skipChangeCommits=true must drop the RESTORE commit entirely; expected 30 rows from"
                + " v1+v2+v3 appends, got "
                + rows.size());
  }

  /**
   * RESTORE writes a brand-new commit whose mtime is "now", regardless of the version being
   * restored. {@code startingTimestamp} must resolve against the RESTORE commit's mtime (latest),
   * not the mtime of the original commit whose data is being restored.
   *
   * <p>Setup: v1..v3 are appends. RESTORE to v1 creates v4 (mtime = wall-clock now). Use {@code
   * startingTimestamp} captured just before the RESTORE: with {@code ignoreChanges=true} this picks
   * v4 (the RESTORE), which under {@code ignoreChanges} re-emits v1's content as the AddFiles.
   *
   * <p>If {@code startingTimestamp} resolved to v1 instead (the restored content's original
   * version), the stream would replay v1+v2+v3 (30 rows from forward play) plus the RESTORE re-add
   * = a much larger row count.
   */
  @Test
  public void testStartingTimestamp_acrossRestoreVersion(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark
        .range(0, 10)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v1
    spark
        .range(10, 20)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v2
    spark
        .range(20, 30)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v3

    // Sleep so the RESTORE commit's mtime is strictly greater than v3's mtime, and the cutoff
    // is strictly before the RESTORE mtime.
    Thread.sleep(200);
    long cutoff = System.currentTimeMillis();
    Thread.sleep(200);
    restoreTo(tablePath, 1); // v4

    Dataset<Row> df =
        spark
            .readStream()
            .option("ignoreChanges", "true")
            .option("startingTimestamp", formatTs(cutoff))
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "restore_st");

    // startingTimestamp=cutoff (before RESTORE mtime) -> resolves to v4 (RESTORE commit).
    // Under ignoreChanges, the AddFiles in v4 (which are the v1 files restored) are emitted:
    // exactly 10 rows. If startingTimestamp drifted backward to v1, we would see 30+ rows.
    assertEquals(
        10,
        rows.size(),
        () ->
            "startingTimestamp just before RESTORE must resolve to the RESTORE commit; expected 10"
                + " rows (AddFiles from RESTORE re-add of v1 contents), got "
                + rows.size()
                + ". A larger count indicates the timestamp resolved backward to the restored"
                + " version's original mtime.");
  }

  /**
   * Same RESTORE setup; stream from {@code startingVersion = restoreVersion} explicitly with {@code
   * ignoreChanges=true}. Should emit only the AddFiles of the RESTORE commit (v1's contents).
   */
  @Test
  public void testStartingVersion_atRestoreCommit(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark
        .range(0, 10)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v1
    spark
        .range(10, 20)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v2
    spark
        .range(20, 30)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v3
    restoreTo(tablePath, 1); // v4

    long restoreVersion =
        DeltaLog.forTable(spark, tablePath).update(false, Option.empty(), Option.empty()).version();
    assertEquals(4L, restoreVersion, "RESTORE should be at v4");

    Dataset<Row> df =
        spark
            .readStream()
            .option("ignoreChanges", "true")
            .option("startingVersion", Long.toString(restoreVersion))
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "restore_sv");

    // Only the AddFiles inside the RESTORE commit (the v1 files re-added) should surface.
    assertEquals(
        10,
        rows.size(),
        () ->
            "startingVersion="
                + restoreVersion
                + " should emit only the RESTORE commit's AddFiles"
                + " (10 rows from v1's contents), got "
                + rows.size());
  }

  /**
   * DV-enabled table: v1=insert(0..9 in single file); v2=DV-delete(id<5); restore to v1 at v3.
   * After RESTORE, the DV is gone and all 10 rows should be queryable.
   *
   * <p>Stream the whole table with {@code skipChangeCommits=true} — v2 (DV delete) and v3 (RESTORE)
   * are both skipped; the stream emits only v1's AddFile = 10 rows.
   */
  @Test
  public void testRestoreOnDvTable_streamThrough_skipChangeCommits(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta TBLPROPERTIES ("
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));

    spark
        .range(0, 10)
        .selectExpr("cast(id as int) as id")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v1

    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 5", tablePath)); // v2 (DV delete)
    restoreTo(tablePath, 1); // v3: removes the DV-bearing AddFile, adds back the DV-free AddFile

    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "restore_dv_skip");

    // v1 is an append (10 rows emitted). v2 is a DV delete (skipped under skipChangeCommits).
    // v3 is the RESTORE (also skipped). Total: 10 rows.
    assertEquals(
        10,
        rows.size(),
        () ->
            "DV table + RESTORE under skipChangeCommits should emit only the v1 append; expected 10"
                + " rows, got "
                + rows.size());
  }

  /**
   * Partitioned table: v1 inserts to partitions p=0 and p=1; v2 OVERWRITE PARTITION (p=0) rewrites
   * partition 0; restore to v1 at v3 (which reverts the OVERWRITE).
   *
   * <p>Under {@code skipChangeCommits=true}, the OVERWRITE (v2) and the RESTORE (v3) are both
   * dropped; the stream emits only v1's appends.
   */
  @Test
  public void testRestoreOnPartitionedTable_streamThrough_skipChangeCommits(
      @TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str("CREATE TABLE delta.`%s` (id INT, p INT) USING delta PARTITIONED BY (p)", tablePath));

    // v1: 5 rows in p=0, 5 rows in p=1.
    spark
        .range(0, 10)
        .selectExpr("cast(id as int) as id", "cast(id % 2 as int) as p")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v1

    // v2: INSERT OVERWRITE one partition (p=0). This rewrites the p=0 files.
    spark.sql(
        str(
            "INSERT OVERWRITE TABLE delta.`%s` PARTITION (p=0) VALUES (100), (102), (104), (106),"
                + " (108)",
            tablePath)); // v2

    restoreTo(tablePath, 1); // v3

    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "restore_part_skip");

    // Only v1's append should reach the stream (10 rows).
    assertEquals(
        10,
        rows.size(),
        () ->
            "Partitioned table + OVERWRITE PARTITION + RESTORE should emit only v1's 10 rows under"
                + " skipChangeCommits, got "
                + rows.size());
  }

  /**
   * Column-mapped (name mode) table: v1 inserts under (id, value); v2 ADD COLUMN value2; v3 inserts
   * with the new column; restore to v1 (the schema before the ADD COLUMN).
   *
   * <p>{@code skipChangeCommits=true} should drop the RESTORE commit. The stream should still
   * succeed; we just check it ran and produced the v1 appends.
   *
   * <p>Note: ADD COLUMN is a metadata-only commit and is not a data "change" in the
   * skipChangeCommits sense. RESTORE rewinds the schema in addition to the data; the metadata
   * action inside the RESTORE commit will be observed during {@code
   * validateCommitAndDecideSkipping}.
   */
  @Test
  public void testRestoreOnColMappedTable_streamThrough_skipChangeCommits(
      @TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));

    spark.sql(str("INSERT INTO delta.`%s` VALUES (0, 'a'), (1, 'b'), (2, 'c')", tablePath)); // v1
    spark.sql(str("ALTER TABLE delta.`%s` ADD COLUMN (value2 STRING)", tablePath)); // v2
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'd', 'D'), (4, 'e', 'E')", tablePath)); // v3
    restoreTo(tablePath, 1); // v4

    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "restore_cm_skip");

    // v1 emits 3 rows. v3 emits 2 rows (pure append, schema is wider). v4 (RESTORE) is dropped
    // under skipChangeCommits. Expected: 3 + 2 = 5 rows.
    assertEquals(
        5,
        rows.size(),
        () ->
            "Column-mapped table + ADD COLUMN + RESTORE under skipChangeCommits should emit v1's 3"
                + " rows + v3's 2 rows = 5, got "
                + rows.size());
  }

  /**
   * Pin: when the RESTORE commit is the only "change" commit in the stream window, {@code
   * ignoreChanges=true} does NOT behave the same as {@code skipChangeCommits=true}. Under
   * ignoreChanges, the AddFiles inside the RESTORE commit are emitted (the restored content). Under
   * skipChangeCommits, the entire commit is dropped.
   *
   * <p>Setup: v1..v3 appends, v4 RESTORE to v1. ignoreChanges → v1+v2+v3 history (30) + 10 re-added
   * = 40 rows; skipChangeCommits → 30 rows.
   */
  @Test
  public void testRestoreWithIgnoreChanges(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark
        .range(0, 10)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v1
    spark
        .range(10, 20)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v2
    spark
        .range(20, 30)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v3
    restoreTo(tablePath, 1); // v4

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    Dataset<Row> ignoreChangesDF =
        spark.readStream().option("ignoreChanges", "true").table(dsv2TableRef);
    List<Row> ignoreChangesRows = processStreamingQuery(ignoreChangesDF, "restore_ic");

    Dataset<Row> skipDF =
        spark.readStream().option("skipChangeCommits", "true").table(dsv2TableRef);
    List<Row> skipRows = processStreamingQuery(skipDF, "restore_skip_eq");

    // Both options keep the stream alive across the RESTORE, but the row counts differ by design:
    // ignoreChanges admits the RemoveFiles' "ignored" sibling AddFiles, while skipChangeCommits
    // drops the whole commit. Document the (intentional) difference.
    assertEquals(
        30,
        skipRows.size(),
        () -> "skipChangeCommits should drop the RESTORE commit -> 30 rows from v1+v2+v3");
    assertEquals(
        40,
        ignoreChangesRows.size(),
        () ->
            "ignoreChanges should also emit the RESTORE's AddFiles (10 rows re-added from v1) on"
                + " top of v1+v2+v3 -> 40 rows; got "
                + ignoreChangesRows.size());
  }

  /**
   * Without {@code ignoreChanges}/{@code skipChangeCommits}/{@code ignoreDeletes}, the RESTORE
   * commit contains both AddFile(dataChange=true) and RemoveFile(dataChange=true); {@code
   * validateCommitAndDecideSkipping} hits the {@code hasFileAdd && !shouldAllowChanges} branch and
   * throws {@code DELTA_SOURCE_TABLE_IGNORE_CHANGES} with the operation name "RESTORE" in the
   * message.
   */
  @Test
  public void testRestoreWithoutIgnoreChanges_throwsClearly(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark
        .range(0, 10)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v1
    spark
        .range(10, 20)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v2
    restoreTo(tablePath, 1); // v3

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));

    StreamingQuery query =
        df.writeStream()
            .format("memory")
            .queryName("restore_no_ignore")
            .outputMode("append")
            .start();
    StreamingQueryException ex;
    try {
      ex = assertThrows(StreamingQueryException.class, query::processAllAvailable);
    } finally {
      query.stop();
      DeltaLog.clearCache();
    }

    String fullMsg = String.valueOf(ex) + " / " + String.valueOf(ex.getCause());
    // DSv1 emits DELTA_SOURCE_TABLE_IGNORE_CHANGES; DSv2 routes through the same DeltaErrors helper
    // (SparkMicroBatchStream:1216). The message must mention IGNORE_CHANGES and the RESTORE op.
    assertTrue(
        fullMsg.contains("DELTA_SOURCE_TABLE_IGNORE_CHANGES") || fullMsg.contains("ignoreChanges"),
        () ->
            "Expected DELTA_SOURCE_TABLE_IGNORE_CHANGES (or ignoreChanges in body); got: "
                + fullMsg);
    assertTrue(
        fullMsg.toUpperCase().contains("RESTORE"),
        () -> "Expected the error to mention the RESTORE operation; got: " + fullMsg);
  }

  /**
   * AvailableNow must terminate even when the snapshot tail includes a RESTORE commit. With {@code
   * skipChangeCommits=true} the RESTORE commit is dropped; the query should self-terminate.
   */
  @Test
  public void testRestore_triggerAvailableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark
        .range(0, 10)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v1
    spark
        .range(10, 20)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath); // v2
    restoreTo(tablePath, 1); // v3

    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));

    StreamingQuery q =
        df.writeStream()
            .format("memory")
            .queryName("restore_an")
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .start();
    try {
      q.processAllAvailable();
      assertTrue(q.awaitTermination(60_000L), "AvailableNow query should terminate within 60s");

      // v1 (10) + v2 (10); v3 RESTORE skipped under skipChangeCommits.
      long count = spark.sql("SELECT COUNT(*) FROM restore_an").collectAsList().get(0).getLong(0);
      assertEquals(
          20L,
          count,
          () ->
              "AvailableNow + RESTORE + skipChangeCommits should emit v1+v2 (20 rows), got "
                  + count);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * Build a table whose RESTORE commit re-adds multiple files, then stream with {@code
   * maxFilesPerTrigger=1} + {@code ignoreChanges=true}. Each batch should consume at most 1 file.
   * The point is to pin that the RESTORE commit's AddFiles flow through the same rate-limiting path
   * as ordinary AddFiles.
   *
   * <p>Setup: 3 single-file commits (v1, v2, v3); INSERT OVERWRITE in one shot (v4 — single new
   * file plus 3 removes); RESTORE to v3 (v5 — re-adds 3 files, removes the OVERWRITE file).
   */
  @Test
  public void testRestore_maxFilesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));

    // 3 single-file appends so the snapshot at v3 has 3 distinct files.
    for (int i = 0; i < 3; i++) {
      spark
          .range(i * 10, i * 10 + 10)
          .selectExpr("cast(id as int) as id")
          .coalesce(1)
          .write()
          .format("delta")
          .mode("append")
          .save(tablePath);
    }
    // v4: INSERT OVERWRITE collapses to one new file and removes the prior 3.
    spark.sql(str("INSERT OVERWRITE TABLE delta.`%s` VALUES (999)", tablePath));
    // v5: RESTORE to v3 — re-adds the 3 prior files, removes the OVERWRITE file.
    restoreTo(tablePath, 3);

    // Start AFTER the OVERWRITE so the stream window is just v5 (the RESTORE commit). With
    // maxFilesPerTrigger=1, the 3 AddFiles in v5 must be served across 3 batches.
    Dataset<Row> df =
        spark
            .readStream()
            .option("ignoreChanges", "true")
            .option("maxFilesPerTrigger", "1")
            .option("startingVersion", "5")
            .table(str("dsv2.delta.`%s`", tablePath));

    StreamingQuery q =
        df.writeStream().format("memory").queryName("restore_rate").outputMode("append").start();
    try {
      q.processAllAvailable();
      StreamingQueryProgress[] progress = q.recentProgress();
      long batchesWithRows = 0L;
      for (StreamingQueryProgress p : progress) {
        if (p.numInputRows() > 0L) {
          batchesWithRows++;
          assertTrue(
              p.numInputRows() <= 10L,
              () -> "Each batch must read at most 1 file (10 rows); got " + p.numInputRows());
        }
      }
      // 3 files in the RESTORE commit -> 3 non-empty batches.
      final long finalBatchesWithRows = batchesWithRows;
      assertEquals(
          3L,
          finalBatchesWithRows,
          () ->
              "maxFilesPerTrigger=1 across RESTORE should produce exactly 3 non-empty batches, got "
                  + finalBatchesWithRows);

      long total = spark.sql("SELECT COUNT(*) FROM restore_rate").collectAsList().get(0).getLong(0);
      assertEquals(30L, total, () -> "All 30 rows from the 3 restored files should reach the sink");
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }
}
