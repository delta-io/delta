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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming scenario coverage for tables containing a VARIANT column WITHOUT deletion vectors.
 *
 * <p>Existing coverage: {@link V2StreamingDeletionVectorVariantTest} pins down the DV+VARIANT
 * silent-corruption bug, and {@link V2StreamingDataEdgesTest#testVariantNestedThreeDeepWithDV}
 * covers nested-in-struct VARIANT under a DV. All other streaming-option / lifecycle scenarios on
 * VARIANT tables were previously uncovered.
 *
 * <p>Each row's VARIANT payload is built as {@code parse_json('{"row":<id>}')} so the variant value
 * is a function of the row id. Asserting {@code variant_get(v, '$.row', 'int') == id} on every
 * surviving output row catches any silent reordering or payload corruption regardless of which
 * option introduced it.
 */
public class V2StreamingVariantScenarioTest extends V2TestBase {

  /** Creates a non-DV Delta table with schema (id INT, v VARIANT). */
  private void createTopLevelVariantTable(String tablePath) {
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, v VARIANT) USING delta", tablePath));
  }

  /**
   * Appends {@code count} rows starting at id {@code startId}, each with {@code v =
   * parse_json('{"row":<id>}')}. Each call lands as one Delta commit; {@code coalesce(1)} keeps the
   * commit to a single Parquet file so per-commit / per-file rate-limit assertions are stable.
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

  /**
   * Asserts that the memory-sink table named {@code queryName} contains exactly one row per id in
   * [startInclusive, endExclusive) and that for every row {@code variant_get(v,'$.row','int') ==
   * id}.
   */
  private void assertVariantRowsExact(String queryName, int startInclusive, int endExclusive) {
    int expectedCount = endExclusive - startInclusive;
    List<Row> rows =
        spark
            .sql(
                "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM "
                    + queryName
                    + " ORDER BY id")
            .collectAsList();
    assertEquals(
        expectedCount,
        rows.size(),
        () -> "Expected " + expectedCount + " rows in " + queryName + ", got " + rows);
    int expectedId = startInclusive;
    for (Row row : rows) {
      int id = row.getInt(0);
      assertEquals(expectedId, id, () -> "Unexpected id in " + queryName + ": " + id);
      Object vRowObj = row.get(1);
      assertNotNull(vRowObj, () -> "variant_get returned NULL for id=" + id + " in " + queryName);
      int vRow = ((Number) vRowObj).intValue();
      assertEquals(
          id,
          vRow,
          () ->
              "Variant payload misaligned in "
                  + queryName
                  + ": id="
                  + id
                  + " has variant_get(v,'$.row','int')="
                  + vRow);
      expectedId++;
    }
  }

  /** Formats wall-clock millis using the session-local time zone, for startingTimestamp. */
  private String formatTs(long millis) {
    String tz = spark.sessionState().conf().sessionLocalTimeZone();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone(tz));
    return sdf.format(new Date(millis));
  }

  /** Counts batches that produced at least one input row. */
  private long nonEmptyBatchCount(StreamingQuery q) {
    long count = 0L;
    for (StreamingQueryProgress p : q.recentProgress()) {
      if (p.numInputRows() > 0L) {
        count++;
      }
    }
    return count;
  }

  /** 1. Basic streaming read of a VARIANT column, no other options - sanity baseline. */
  @Test
  public void testStreamingBasic_variant(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createTopLevelVariantTable(tablePath);
    appendVariantRows(tablePath, 1, 5); // ids 1..5

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    assertTrue(df.isStreaming());

    String queryName = "variant_basic";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();
      assertVariantRowsExact(queryName, 1, 6);
    } finally {
      if (query != null) query.stop();
      DeltaLog.clearCache();
    }
  }

  /** 2. startingVersion skips earlier commits - payloads from the surviving commits must align. */
  @Test
  public void testVariant_startingVersion(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createTopLevelVariantTable(tablePath); // v=0
    appendVariantRows(tablePath, 1, 1); // v=1 -> id 1
    appendVariantRows(tablePath, 2, 1); // v=2 -> id 2
    appendVariantRows(tablePath, 3, 1); // v=3 -> id 3
    appendVariantRows(tablePath, 4, 1); // v=4 -> id 4

    Dataset<Row> df =
        spark.readStream().option("startingVersion", "3").table(str("dsv2.delta.`%s`", tablePath));

    String queryName = "variant_startingVersion";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();
      // Versions 3 and 4 contribute ids 3 and 4.
      assertVariantRowsExact(queryName, 3, 5);
    } finally {
      if (query != null) query.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * 3. startingTimestamp far in the past - should consume all commits. Distinguished from
   * startingVersion because it goes through the timestamp-resolution path.
   */
  @Test
  public void testVariant_startingTimestamp(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createTopLevelVariantTable(tablePath);
    appendVariantRows(tablePath, 1, 3); // ids 1..3

    String tsStr = formatTs(0L); // 1970-01-01 -> always before commit mtimes
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingTimestamp", tsStr)
            .table(str("dsv2.delta.`%s`", tablePath));

    String queryName = "variant_startingTimestamp";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();
      assertVariantRowsExact(queryName, 1, 4);
    } finally {
      if (query != null) query.stop();
      DeltaLog.clearCache();
    }
  }

  /** 4. Trigger.AvailableNow - one-shot consumes all VARIANT rows. */
  @Test
  public void testVariant_triggerAvailableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createTopLevelVariantTable(tablePath);
    appendVariantRows(tablePath, 1, 4); // ids 1..4

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));

    String queryName = "variant_availableNow";
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
      assertTrue(query.awaitTermination(60_000));
      assertVariantRowsExact(queryName, 1, 5);
    } finally {
      if (query != null) query.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * 5. maxFilesPerTrigger=1 across multiple single-file commits - verifies that VARIANT payloads
   * survive the per-batch slicing path.
   */
  @Test
  public void testVariant_maxFilesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createTopLevelVariantTable(tablePath);
    // 5 single-file commits with distinct ids; rate-limit forces multi-batch consumption.
    for (int i = 1; i <= 5; i++) {
      appendVariantRows(tablePath, i, 1);
    }

    Dataset<Row> df =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", tablePath));

    String queryName = "variant_maxFiles";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();
      assertEquals(
          5L, nonEmptyBatchCount(query), "expected 5 non-empty batches under maxFilesPerTrigger=1");
      assertVariantRowsExact(queryName, 1, 6);
    } finally {
      if (query != null) query.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * 6. excludeRegex matches no files - all VARIANT rows should pass through, payloads aligned.
   * (Matches-all is already covered in V2StreamingOptionMatrixTest for non-VARIANT.)
   */
  @Test
  public void testVariant_excludeRegex(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createTopLevelVariantTable(tablePath);
    appendVariantRows(tablePath, 1, 5);

    Dataset<Row> df =
        spark
            .readStream()
            .option("excludeRegex", "this-pattern-cannot-match-any-real-file-name-xyz")
            .table(str("dsv2.delta.`%s`", tablePath));

    String queryName = "variant_excludeRegex";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();
      assertVariantRowsExact(queryName, 1, 6);
    } finally {
      if (query != null) query.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * 7. Restart from checkpoint: write half, drain to parquet sink, append the rest, restart with
   * same checkpoint, verify full set + alignment. Parquet sink is used because memory sink does not
   * support checkpoint recovery.
   */
  @Test
  public void testVariant_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createTopLevelVariantTable(tablePath);
    appendVariantRows(tablePath, 1, 3); // ids 1..3

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    StreamingQuery q1 =
        df1.writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q1.processAllAvailable();
    } finally {
      q1.stop();
      DeltaLog.clearCache();
    }

    // New commit after the first run; restart must pick this up via the checkpoint.
    appendVariantRows(tablePath, 4, 3); // ids 4..6

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    StreamingQuery q2 =
        df2.writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q2.processAllAvailable();
    } finally {
      q2.stop();
      DeltaLog.clearCache();
    }

    // Reload parquet sink output and assert per-row alignment.
    Dataset<Row> sinkDf = spark.read().parquet(outputDir.getAbsolutePath());
    sinkDf.createOrReplaceTempView("variant_restart_sink");
    List<Row> rows =
        spark
            .sql(
                "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM variant_restart_sink"
                    + " ORDER BY id")
            .collectAsList();
    assertEquals(6, rows.size(), () -> "Expected 6 rows total after restart, got " + rows);
    int expectedId = 1;
    for (Row row : rows) {
      int id = row.getInt(0);
      assertEquals(expectedId, id, () -> "Unexpected id after restart: " + id);
      Object vRowObj = row.get(1);
      assertNotNull(vRowObj, () -> "variant_get returned NULL for id=" + id + " after restart");
      int vRow = ((Number) vRowObj).intValue();
      assertEquals(
          id, vRow, () -> "Variant payload misaligned after restart: id=" + id + " v_row=" + vRow);
      expectedId++;
    }
  }

  /**
   * 8. VARIANT nested in STRUCT, no DV. Companion to {@code
   * V2StreamingDataEdgesTest#testVariantNestedThreeDeepWithDV}, which exercises the DV path. This
   * test covers the non-DV branch through {@code ColumnarRow#getStruct -> getVariant} and asserts
   * payload identity row-by-row.
   */
  @Test
  public void testVariant_nestedInStruct_basic(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, s STRUCT<v: VARIANT>) USING delta", tablePath));

    spark
        .range(1, 6)
        .selectExpr(
            "cast(id as int) as id",
            "named_struct('v', parse_json(concat('{\"row\":', id, '}'))) as s")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));

    String queryName = "variant_nestedInStruct";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();

      List<Row> rows =
          spark
              .sql(
                  "SELECT id, variant_get(s.v, '$.row', 'int') AS v_row FROM "
                      + queryName
                      + " ORDER BY id")
              .collectAsList();
      assertEquals(5, rows.size(), () -> "Expected 5 rows, got " + rows);
      int expectedId = 1;
      for (Row row : rows) {
        int id = row.getInt(0);
        assertEquals(expectedId, id);
        Object vRowObj = row.get(1);
        assertNotNull(vRowObj, () -> "variant_get returned NULL for id=" + id);
        int vRow = ((Number) vRowObj).intValue();
        assertEquals(
            id, vRow, () -> "STRUCT<VARIANT> payload misaligned: id=" + id + " v_row=" + vRow);
        expectedId++;
      }
    } finally {
      if (query != null) query.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * 9. ARRAY&lt;VARIANT&gt; - currently disabled. Delta OSS support for VARIANT-as-array-element is
   * not stable across releases (the schema may be rejected at CREATE TABLE or the column may shred
   * incorrectly). Enable when ARRAY&lt;VARIANT&gt; is officially supported by Delta and
   * compile-verify in this repo.
   */
  @Test
  public void testVariant_nestedInArray(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, a ARRAY<VARIANT>) USING delta", tablePath));

    spark
        .range(1, 6)
        .selectExpr("cast(id as int) as id", "array(parse_json(concat('{\"row\":', id, '}'))) as a")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));

    String queryName = "variant_nestedInArray";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();

      List<Row> rows =
          spark
              .sql(
                  "SELECT id, variant_get(a[0], '$.row', 'int') AS v_row FROM "
                      + queryName
                      + " ORDER BY id")
              .collectAsList();
      assertEquals(5, rows.size());
      int expectedId = 1;
      for (Row row : rows) {
        int id = row.getInt(0);
        assertEquals(expectedId, id);
        int vRow = ((Number) row.get(1)).intValue();
        assertEquals(id, vRow);
        expectedId++;
      }
    } finally {
      if (query != null) query.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * 10. MAP&lt;STRING, VARIANT&gt; - currently disabled for the same reason as {@link
   * #testVariant_nestedInArray}. Enable once Delta accepts MAP&lt;STRING, VARIANT&gt; at CREATE
   * TABLE and the read path is stable in this build.
   */
  @Test
  public void testVariant_nestedInMap_value(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str("CREATE TABLE delta.`%s` (id INT, m MAP<STRING, VARIANT>) USING delta", tablePath));

    spark
        .range(1, 6)
        .selectExpr(
            "cast(id as int) as id", "map('k', parse_json(concat('{\"row\":', id, '}'))) as m")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    Dataset<Row> df = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));

    String queryName = "variant_nestedInMap";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();

      List<Row> rows =
          spark
              .sql(
                  "SELECT id, variant_get(m['k'], '$.row', 'int') AS v_row FROM "
                      + queryName
                      + " ORDER BY id")
              .collectAsList();
      assertEquals(5, rows.size());
      int expectedId = 1;
      for (Row row : rows) {
        int id = row.getInt(0);
        assertEquals(expectedId, id);
        int vRow = ((Number) row.get(1)).intValue();
        assertEquals(id, vRow);
        expectedId++;
      }
    } finally {
      if (query != null) query.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * 11. ignoreDeletes on a DV-enabled VARIANT table with a whole-partition delete. The DELETE drops
   * whole files (no rewrite), so {@code ignoreDeletes=true} must let the stream proceed past the
   * delete commit, and surviving rows' VARIANT payloads must remain aligned with their ids.
   *
   * <p>Partitioning by {@code part} so {@code DELETE WHERE part = 0} produces RemoveFile-only
   * commits - the exact shape {@code ignoreDeletes} contract allows.
   */
  @Test
  public void testVariant_ignoreDeletes(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT, v VARIANT) USING delta "
                + "PARTITIONED BY (part) "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));

    // 3 rows in part=0 (ids 1..3) and 3 rows in part=1 (ids 10..12). Each partition lands as one
    // file via coalesce(1) so DELETE WHERE part = 0 removes a whole file (no DV, no rewrite).
    spark
        .range(1, 4)
        .selectExpr(
            "cast(id as int) as id", "0 as part", "parse_json(concat('{\"row\":', id, '}')) as v")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
    spark
        .range(10, 13)
        .selectExpr(
            "cast(id as int) as id", "1 as part", "parse_json(concat('{\"row\":', id, '}')) as v")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // Whole-partition delete: produces a RemoveFile-only commit (ignoreDeletes-compatible).
    spark.sql(str("DELETE FROM delta.`%s` WHERE part = 0", tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("ignoreDeletes", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));

    String queryName = "variant_ignoreDeletes";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();

      final StreamingQuery finalQuery = query;
      assertTrue(
          finalQuery.exception().isEmpty(),
          () ->
              "Streaming query failed: "
                  + (finalQuery.exception().isDefined()
                      ? finalQuery.exception().get().toString()
                      : ""));

      // The stream emits the initial AddFiles for both partitions. ignoreDeletes drops the
      // RemoveFile commit silently. Memory sink contains the union of both initial AddFiles, so
      // ids 1..3 and 10..12 are all present with aligned variant payloads.
      List<Row> rows =
          spark
              .sql(
                  "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM "
                      + queryName
                      + " ORDER BY id")
              .collectAsList();
      assertEquals(6, rows.size(), () -> "Expected 6 rows emitted before delete, got " + rows);
      int[] expectedIds = {1, 2, 3, 10, 11, 12};
      for (int i = 0; i < rows.size(); i++) {
        Row row = rows.get(i);
        int id = row.getInt(0);
        assertEquals(expectedIds[i], id, () -> "Unexpected id ordering: " + rows);
        Object vRowObj = row.get(1);
        assertNotNull(vRowObj, () -> "variant_get returned NULL for id=" + id);
        int vRow = ((Number) vRowObj).intValue();
        assertEquals(
            id,
            vRow,
            () -> "Variant payload misaligned under ignoreDeletes: id=" + id + " v_row=" + vRow);
      }
    } finally {
      if (query != null) query.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * 12. ignoreChanges on a VARIANT table with an UPDATE that rewrites files. The UPDATE produces
   * RemoveFile + AddFile commits; {@code ignoreChanges=true} surfaces the AddFiles (which may
   * include rows that were not actually changed). Asserts the stream does not error and every
   * emitted variant payload remains aligned with its row id.
   */
  @Test
  public void testVariant_ignoreChanges(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createTopLevelVariantTable(tablePath);
    appendVariantRows(tablePath, 1, 5); // ids 1..5, single file via coalesce(1)

    // UPDATE rewrites the file: id=3 gets a new variant payload whose v.row still equals id.
    // Other rows in the same file are re-emitted unchanged. Stream with ignoreChanges sees these
    // re-emissions as duplicates - but every emitted (id, v.row) pair must satisfy v.row == id.
    spark.sql(
        str(
            "UPDATE delta.`%s` SET v = parse_json(concat('{\"row\":', id, '}')) WHERE id = 3",
            tablePath));

    Dataset<Row> df =
        spark
            .readStream()
            .option("ignoreChanges", "true")
            .option("startingVersion", "0")
            .table(str("dsv2.delta.`%s`", tablePath));

    String queryName = "variant_ignoreChanges";
    StreamingQuery query = null;
    try {
      query = df.writeStream().format("memory").queryName(queryName).outputMode("append").start();
      query.processAllAvailable();

      final StreamingQuery finalQuery = query;
      assertTrue(
          finalQuery.exception().isEmpty(),
          () ->
              "Streaming query failed under ignoreChanges: "
                  + (finalQuery.exception().isDefined()
                      ? finalQuery.exception().get().toString()
                      : ""));

      // Every emitted row must satisfy variant_get(v, '$.row', 'int') == id. Duplicates from the
      // UPDATE rewrite are expected and allowed under the ignoreChanges contract; payload identity
      // is what must hold.
      List<Row> rows =
          spark
              .sql(
                  "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM "
                      + queryName
                      + " ORDER BY id")
              .collectAsList();
      assertTrue(
          rows.size() >= 5,
          () ->
              "Expected at least 5 rows under ignoreChanges (initial + UPDATE re-emit), got "
                  + rows);
      for (Row row : rows) {
        int id = row.getInt(0);
        Object vRowObj = row.get(1);
        assertNotNull(
            vRowObj, () -> "variant_get returned NULL for id=" + id + " under ignoreChanges");
        int vRow = ((Number) vRowObj).intValue();
        assertEquals(
            id,
            vRow,
            () -> "Variant payload misaligned under ignoreChanges: id=" + id + " v_row=" + vRow);
      }
    } finally {
      if (query != null) query.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * 13. S17 x VARIANT: nullability toggle mid-stream. The table has a NOT NULL INT column alongside
   * a VARIANT. With {@code schemaTrackingLocation}, dropping NOT NULL and inserting a row whose INT
   * is null + VARIANT is valid must surface on restart, not raise SCHEMA_MISMATCH_ON_RESTART. The
   * variant payload must remain aligned with the row's id for every non-null row.
   */
  @Test
  public void testVariant_nullabilityToggle(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File schemaTrackingDir = new File(checkpointDir, "_schema_tracking");
    File outputDir = new File(deltaTablePath, "_out");

    // INT NOT NULL + VARIANT. Variant payload is parse_json('{"row":<id>}') so it tracks id.
    spark.sql(str("CREATE TABLE delta.`%s` (id INT NOT NULL, v VARIANT) USING delta", tablePath));
    appendVariantRows(tablePath, 1, 3); // ids 1..3 with v.row = id

    // First run drains the 3 pre-toggle rows.
    Dataset<Row> df1 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2Ref);
    StreamingQuery q1 =
        df1.writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q1.processAllAvailable();
    } finally {
      q1.stop();
      DeltaLog.clearCache();
    }
    long firstRunRows = spark.read().parquet(outputDir.getAbsolutePath()).count();
    assertEquals(3L, firstRunRows, "first run should emit 3 pre-toggle rows");

    // Drop NOT NULL on id, then insert a row with a NULL id + valid variant payload.
    spark.sql(str("ALTER TABLE delta.`%s` ALTER COLUMN id DROP NOT NULL", tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` SELECT cast(NULL as int) as id, "
                + "parse_json('{\"row\":-1}') as v",
            tablePath));

    // Restart from the same checkpoint + schema tracking log.
    Dataset<Row> df2 =
        spark
            .readStream()
            .option("schemaTrackingLocation", schemaTrackingDir.getAbsolutePath())
            .table(dsv2Ref);
    StreamingQuery q2 =
        df2.writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q2.processAllAvailable();
      final StreamingQuery finalQ = q2;
      assertTrue(
          finalQ.exception().isEmpty(),
          () ->
              "Restart should not raise SCHEMA_MISMATCH_ON_RESTART under variant + nullability "
                  + "toggle: "
                  + (finalQ.exception().isDefined() ? finalQ.exception().get().toString() : ""));
    } finally {
      q2.stop();
      DeltaLog.clearCache();
    }

    // Total sink must include the post-toggle null-id row, and every non-null row's variant
    // payload must still align with its id.
    Dataset<Row> sinkDf = spark.read().parquet(outputDir.getAbsolutePath());
    sinkDf.createOrReplaceTempView("variant_nullability_sink");
    List<Row> rows =
        spark
            .sql(
                "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM "
                    + "variant_nullability_sink")
            .collectAsList();
    assertEquals(4, rows.size(), () -> "expected 4 total rows after toggle, got " + rows);
    assertTrue(
        rows.stream().anyMatch(Row::anyNull),
        () -> "post-toggle null-id row must surface, got " + rows);
    for (Row row : rows) {
      if (row.isNullAt(0)) {
        // Null id: variant payload is parse_json('{"row":-1}'); ensure it is present and intact.
        Object vRowObj = row.get(1);
        assertNotNull(vRowObj, "variant payload must survive on null-id row");
        assertEquals(-1, ((Number) vRowObj).intValue(), "null-id row variant must be {\"row\":-1}");
        continue;
      }
      int id = row.getInt(0);
      Object vRowObj = row.get(1);
      assertNotNull(vRowObj, () -> "variant_get returned NULL for non-null id=" + id);
      int vRow = ((Number) vRowObj).intValue();
      assertEquals(
          id,
          vRow,
          () -> "variant payload misaligned after nullability toggle: id=" + id + " v_row=" + vRow);
    }
  }

  /**
   * 14. S22 x VARIANT: concurrent appender writes more variant rows while the stream is running.
   * The stream must not error and every emitted row's variant payload must remain aligned with its
   * id.
   */
  @Test
  public void testVariant_concurrentWriter(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    createTopLevelVariantTable(tablePath);
    appendVariantRows(tablePath, 0, 1); // seed row at id=0

    Dataset<Row> df = spark.readStream().table(dsv2Ref);

    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService writer = Executors.newSingleThreadExecutor();
    StreamingQuery query = null;
    try {
      query =
          df.writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .start();

      // Concurrent appender: 10 single-row commits with distinct ids 1..10.
      writer.submit(
          () -> {
            int i = 1;
            while (!stop.get() && i <= 10) {
              try {
                appendVariantRows(tablePath, i, 1);
                Thread.sleep(50);
                i++;
              } catch (Exception ignored) {
                // Concurrent commits may transiently fail; just continue.
              }
            }
          });

      Thread.sleep(2_000L);
      stop.set(true);
      query.processAllAvailable();

      final StreamingQuery finalQ = query;
      assertTrue(
          finalQ.exception().isEmpty(),
          () ->
              "Concurrent appender on variant table must not error: "
                  + (finalQ.exception().isDefined() ? finalQ.exception().get().toString() : ""));
    } finally {
      stop.set(true);
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      if (query != null) {
        query.stop();
      }
      DeltaLog.clearCache();
    }

    // Sink must include the seed row plus at least some concurrent rows; payloads aligned.
    Dataset<Row> sinkDf = spark.read().parquet(outputDir.getAbsolutePath());
    sinkDf.createOrReplaceTempView("variant_concurrent_sink");
    List<Row> rows =
        spark
            .sql(
                "SELECT id, variant_get(v, '$.row', 'int') AS v_row FROM variant_concurrent_sink "
                    + "ORDER BY id")
            .collectAsList();
    long sourceCount = spark.read().format("delta").load(tablePath).count();
    assertTrue(
        rows.size() >= 1, () -> "stream should emit at least the seed row, got " + rows.size());
    assertTrue(
        rows.size() <= sourceCount,
        () -> "sink size " + rows.size() + " must not exceed source " + sourceCount);
    for (Row row : rows) {
      int id = row.getInt(0);
      Object vRowObj = row.get(1);
      assertNotNull(vRowObj, () -> "variant payload missing for id=" + id);
      int vRow = ((Number) vRowObj).intValue();
      assertEquals(
          id,
          vRow,
          () -> "variant payload misaligned under concurrent writer: id=" + id + " v_row=" + vRow);
    }
  }
}
