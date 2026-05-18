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

import io.delta.spark.internal.v2.utils.IctTestUtils;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming data + semantic edges (scenarios 4, 7, 9, 10 from scenario_brainstorm.md).
 *
 * <p>Each test exercises a separate edge of the streaming read path. All four are designed so that
 * the assertion fires <em>only</em> if the targeted bug is real - they intentionally avoid any
 * "well-it-didn't-throw" assertions.
 */
public class V2StreamingDataEdgesTest extends V2TestBase {

  /**
   * Scenario 4: VARIANT nested 3-deep + DV.
   *
   * <p>ColumnVectorWithFilter#getChild wraps the non-Struct child once for top-level VARIANT
   * (fix-AH). For STRUCT&lt;s1: STRUCT&lt;s2: STRUCT&lt;v: VARIANT&gt;&gt;&gt;, Spark drills down
   * via ColumnarRow#getStruct -&gt; getChild repeatedly. Each Struct level yields a
   * ColumnVectorWithFilter child (mapping preserved). Once we hit the VARIANT, getVariant() goes
   * through getChild(0/1) again - the non-Struct branch - and must wrap. If any level drops the
   * row-id mapping, the variant payload reads from the wrong row.
   *
   * <p>Each row's variant encodes its own id; if mapping is dropped at any nesting level, the
   * asserted equality fails.
   */
  @Test
  public void testVariantNestedThreeDeepWithDV(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` ("
                + "id INT, "
                + "s1 STRUCT<s2: STRUCT<s3: STRUCT<v: VARIANT>>>) "
                + "USING delta TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));

    // Insert 10 rows. Each row's deepest-nested variant payload encodes its id, so the row-id
    // mapping is the only thing keeping (id, variant) aligned after the DV-only delete.
    spark
        .range(1, 11)
        .selectExpr(
            "cast(id as int) as id",
            "named_struct("
                + "'s2', named_struct("
                + "'s3', named_struct("
                + "'v', parse_json(concat('{\"row\":', id, '}'))))) as s1")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // DV-only delete (file kept, only DV written).
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    String queryName = "v_nested_dv";
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .start();
      query.processAllAvailable();

      List<Row> rows =
          spark
              .sql(
                  "SELECT id, "
                      + "variant_get(s1.s2.s3.v, '$.row', 'int') AS v_row FROM "
                      + queryName
                      + " ORDER BY id")
              .collectAsList();

      // Surviving ids after DELETE id % 2 = 0: {1, 3, 5, 7, 9}.
      assertEquals(5, rows.size(), () -> "Expected 5 surviving rows, got " + rows);
      for (Row row : rows) {
        int id = row.getInt(0);
        Object vRowObj = row.get(1);
        assertNotNull(
            vRowObj, () -> "variant_get returned NULL for id=" + id + " - variant payload missing");
        int vRow = ((Number) vRowObj).intValue();
        assertEquals(
            id,
            vRow,
            () ->
                "Silent corruption at 3-deep nesting: row id="
                    + id
                    + " has s1.s2.s3.v.row="
                    + vRow
                    + " (expected "
                    + id
                    + "). ColumnVectorWithFilter likely dropped the row-id mapping at"
                    + " STRUCT->STRUCT->STRUCT->VARIANT.");
      }
    } finally {
      if (query != null) query.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * Scenario 7: startingTimestamp == commit mtime, NON-ICT.
   *
   * <p>Task M case 7 (V2StreamingIctTest) showed that canReturnEarliestCommit=true in the non-ICT
   * resolver lets the search drift far back. The exact-match leg ("ts == mtime of v2") is a
   * separate path. We expect the stream to start at v=2 (40 rows from v=2..v=5) for an exact-match
   * query - not v=0 (entire table) and not v=3 (next-commit). DSv1 parity check via
   * .format("delta").
   */
  @Test
  public void testStartingTimestampExactlyEqualsCommitMtimeNonIct(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    // v0 = CREATE TABLE; v1..v5 = appends.
    spark.sql(str("CREATE TABLE delta.`%s` (id BIGINT) USING delta", tablePath));
    DeltaLog log = DeltaLog.forTable(spark, tablePath);

    long base = 1700000000000L;
    spark.range(0, 10).write().format("delta").mode("append").save(tablePath); // v1
    spark.range(10, 20).write().format("delta").mode("append").save(tablePath); // v2
    spark.range(20, 30).write().format("delta").mode("append").save(tablePath); // v3
    spark.range(30, 40).write().format("delta").mode("append").save(tablePath); // v4
    spark.range(40, 50).write().format("delta").mode("append").save(tablePath); // v5

    long[] mtimes = {
      base - 60_000L, base, base + 60_000L, base + 120_000L, base + 180_000L, base + 240_000L
    };
    for (long version = 0; version < mtimes.length; version++) {
      IctTestUtils.setFileMtimeOnly(log, version, mtimes[(int) version]);
    }

    long v2Mtime = mtimes[2]; // exact match leg
    String tsStr = formatTs(v2Mtime);

    // DSv2.
    Dataset<Row> dfV2 =
        spark
            .readStream()
            .option("startingTimestamp", tsStr)
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rowsV2 = processStreamingQuery(dfV2, "edge_st_eq_mtime_v2");
    // Expected: starting at v=2, read v2..v5 = 40 rows.
    assertEquals(
        40,
        rowsV2.size(),
        () ->
            "DSv2 startingTimestamp == v2 mtime should resolve to v=2 (40 rows from v2..v5),"
                + " got "
                + rowsV2.size()
                + ". <40: drifted forward to v3+ (next-commit applied to exact match)."
                + " >40: drifted backward (canReturnEarliestCommit=true).");

    // DSv1 parity.
    Dataset<Row> dfV1 =
        spark.readStream().format("delta").option("startingTimestamp", tsStr).load(tablePath);
    List<Row> rowsV1 = processStreamingQuery(dfV1, "edge_st_eq_mtime_v1");
    assertEquals(
        rowsV2.size(),
        rowsV1.size(),
        () ->
            "DSv1/DSv2 disagree on exact-match starting@v2.mtime: v1="
                + rowsV1.size()
                + " v2="
                + rowsV2.size());
  }

  /**
   * Scenario 9: Offset JSON with future sourceVersion.
   *
   * <p>SMS#deserializeOffset delegates to DeltaSourceOffset$.MODULE$.apply, which validates
   * sourceVersion within [VERSION_1, CURRENT_VERSION]. A future version (e.g. 99) must surface as a
   * friendly DELTA_INVALID_FORMAT_FROM_SOURCE_VERSION error mentioning the version + an upgrade
   * suggestion. DSv1 has DeltaSourceOffsetSuite "unknown sourceVersion value"; the DSv2 path goes
   * through SMS but has no end-to-end coverage that the friendly message reaches the user.
   *
   * <p>Run a stream once to capture a real offsets/0 file, mutate sourceVersion to 99, restart, and
   * assert the expected friendly error.
   */
  @Test
  public void testFutureSourceVersionInOffsetFile(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id BIGINT) USING delta", tablePath));
    spark.range(0, 5).write().format("delta").mode("append").save(tablePath);

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    // Initial run with noop sink to populate offsets/0. Memory sink does not support recovery
    // from checkpoint, so we must use a sink that does (noop is the simplest choice).
    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    StreamingQuery q1 =
        df1.writeStream()
            .format("noop")
            .queryName("future_src_q1")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .start();
    q1.processAllAvailable();
    q1.awaitTermination(60_000L);
    q1.stop();
    DeltaLog.clearCache();

    File offsetFile = new File(checkpointDir, "offsets/0");
    assertTrue(offsetFile.exists(), "offsets/0 not created by initial run");

    // Mutate sourceVersion to 99. OffsetSeqLog format: "v1\n<metadata json>\n<source offset>".
    Path offsetPath = offsetFile.toPath();
    String original = new String(Files.readAllBytes(offsetPath), StandardCharsets.UTF_8);
    String mutated = original.replaceFirst("\"sourceVersion\":\\s*\\d+", "\"sourceVersion\":99");
    assertNotEquals(
        original, mutated, () -> "Failed to find sourceVersion in offset file: " + original);
    Files.write(offsetPath, mutated.getBytes(StandardCharsets.UTF_8));

    // Restart: deserializeOffset(json) on offsets/0 should fail with friendly version-99 error.
    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    StreamingQueryException ex =
        assertThrows(
            StreamingQueryException.class,
            () -> {
              StreamingQuery q2 =
                  df2.writeStream()
                      .format("noop")
                      .queryName("future_src_q2")
                      .option("checkpointLocation", checkpointDir.getAbsolutePath())
                      .outputMode("append")
                      .trigger(Trigger.AvailableNow())
                      .start();
              try {
                q2.processAllAvailable();
                q2.awaitTermination(60_000L);
              } finally {
                q2.stop();
                DeltaLog.clearCache();
              }
            });

    String fullMsg = String.valueOf(ex) + " / " + String.valueOf(ex.getCause());
    // DSv1 message (DELTA_INVALID_FORMAT_FROM_SOURCE_VERSION):
    //   "...source from version 99... source supports up to N. Please upgrade..."
    assertTrue(
        fullMsg.contains("99"),
        () -> "Friendly error must mention the offending version 99; got: " + fullMsg);
    assertTrue(
        fullMsg.toLowerCase().contains("upgrade")
            || fullMsg.contains("DELTA_INVALID_FORMAT_FROM_SOURCE_VERSION"),
        () -> "Friendly error must suggest upgrade or have stable error class; got: " + fullMsg);
  }

  /**
   * Scenario 10: DV-only DELETE + skipChangeCommits=true.
   *
   * <p>SMS:1148 sets shouldSkipCommit only when a RemoveFile(dataChange=true) is observed. DV-only
   * deletes write RemoveFile + AddFile-with-DV. If kernel produces only an AddFile-with-DV for the
   * metadata-DV path (no RemoveFile), shouldSkipCommit stays false and the residual rows leak
   * through. With skipChangeCommits=true the user expects an empty batch for that commit.
   *
   * <p>Single-file DV-enabled table; DELETE all rows produces RemoveFile + AddFile-with-DV. Stream
   * with skipChangeCommits=true and assert no rows leak through.
   */
  @Test
  public void testDvOnlyDeleteWithSkipChangeCommits(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta TBLPROPERTIES ("
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));

    // v1: insert 5 rows in a single file.
    spark
        .range(0, 5)
        .selectExpr("cast(id as int) as id")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // v2: DELETE all rows in the file -> RemoveFile + AddFile-with-DV (DV covers the whole file).
    spark.sql(str("DELETE FROM delta.`%s`", tablePath));

    // Start the stream from v=2 only (skip the insert).
    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("startingVersion", "2")
            .table(str("dsv2.delta.`%s`", tablePath));

    String queryName = "skip_dv_only_delete";
    StreamingQuery query = null;
    try {
      query =
          df.writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .start();
      query.processAllAvailable();
      query.awaitTermination(60_000L);

      List<Row> rows = spark.sql("SELECT * FROM " + queryName).collectAsList();
      // skipChangeCommits=true should drop the v=2 commit entirely (it produces no new logical
      // user rows). Any residual rows = bug: either the AddFile-with-DV branch leaked or the
      // commit was processed as ordinary data.
      assertEquals(
          0,
          rows.size(),
          () ->
              "skipChangeCommits=true on DV-only delete should emit empty batch; got "
                  + rows.size()
                  + " rows: "
                  + rows
                  + ". Likely cause: SMS:1148 missed the RemoveFile, so shouldSkipCommit stayed"
                  + " false and the AddFile-with-DV (post-delete state) was scanned as data.");
    } finally {
      if (query != null) query.stop();
      DeltaLog.clearCache();
    }
  }

  private String formatTs(long millis) {
    String tz = spark.sessionState().conf().sessionLocalTimeZone();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone(tz));
    return sdf.format(new Date(millis));
  }
}
