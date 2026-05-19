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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv1 vs DSv2 streaming parity for {@code ignoreDeletes=true} on a DV-enabled table.
 *
 * <p>Bug #28: When streaming a DV-enabled table with {@code ignoreDeletes=true}, DSv2 emits fewer
 * rows than expected because DV-based delete commits are not skipped.
 *
 * <p>On a table with {@code delta.enableDeletionVectors=true}, a {@code DELETE} produces an {@code
 * AddFile} with an attached deletion vector (rewriting the file's metadata, not the file itself)
 * rather than a plain {@code RemoveFile}. DSv2's {@code ignoreDeletes} guard only skips commits
 * that consist solely of {@code RemoveFile} actions, so DV-based delete commits bypass the skip
 * logic, the rewritten {@code AddFile} is read as new data, and the surviving (non-deleted) rows
 * are emitted to the stream a second time. The rows that <em>were</em> deleted disappear; the rows
 * that survived may be emitted twice depending on how the post-DELETE {@code AddFile} is consumed.
 *
 * <p>DSv1 (oracle) handles this correctly: the V1 streaming source classifies DV-attached
 * AddFile-only commits as deletes for the purpose of {@code ignoreDeletes} and skips them, so only
 * the original INSERT rows and any post-DELETE INSERT rows are emitted.
 *
 * <p>Each test below writes the same table twice (once for the V1 read, once for the V2 read so the
 * temp dirs do not collide), runs both streaming reads, and asserts row-for-row parity. Until Bug
 * #28 is fixed, the parity assertion fails because V2 emits a different row set than V1.
 */
public class V2StreamingDvIgnoreDeletesTest extends V2TestBase {

  /**
   * Basic stream: INSERT into two partitions, DELETE one whole partition on a DV-enabled table,
   * INSERT more rows, then read with {@code ignoreDeletes=true}. V1 skips the DELETE commit; V2
   * does not. Parity assertion fails until Bug #28 is fixed.
   */
  @Test
  public void testDvTable_ignoreDeletes_basicStream(@TempDir File baseDir) throws Exception {
    File v1Dir = new File(baseDir, "v1");
    File v2Dir = new File(baseDir, "v2");
    assertTrue(v1Dir.mkdirs(), "Failed to create v1 dir");
    assertTrue(v2Dir.mkdirs(), "Failed to create v2 dir");

    String v1TablePath = v1Dir.getAbsolutePath();
    String v2TablePath = v2Dir.getAbsolutePath();

    createDvPartitionedTable(v1TablePath);
    seedAndDeleteAndAppend(v1TablePath);

    createDvPartitionedTable(v2TablePath);
    seedAndDeleteAndAppend(v2TablePath);

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("ignoreDeletes", "true")
            .load(v1TablePath)
            .selectExpr("id", "p");
    List<Row> v1Rows = processStreamingQuery(v1Stream, "dv_ignore_deletes_basic_v1");

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("ignoreDeletes", "true")
            .table(str("dsv2.delta.`%s`", v2TablePath))
            .selectExpr("id", "p");
    List<Row> v2Rows = processStreamingQuery(v2Stream, "dv_ignore_deletes_basic_v2");

    // This assertion fails because of Bug #28 - V2 does not skip DV-based delete commits with
    // ignoreDeletes=true. V1 emits the original INSERT rows plus the post-DELETE INSERT rows; V2
    // emits a different set (DELETE commit not skipped, so the rewritten AddFile surfaces).
    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Same DV + ignoreDeletes setup, but with {@code maxFilesPerTrigger=1} forcing one file per
   * batch. V1 skips the DELETE commit; V2 admits the DV-rewritten AddFile and diverges. Parity
   * assertion fails until Bug #28 is fixed.
   */
  @Test
  public void testDvTable_ignoreDeletes_withMaxFilesPerTrigger(@TempDir File baseDir)
      throws Exception {
    File v1Dir = new File(baseDir, "v1");
    File v2Dir = new File(baseDir, "v2");
    assertTrue(v1Dir.mkdirs(), "Failed to create v1 dir");
    assertTrue(v2Dir.mkdirs(), "Failed to create v2 dir");

    String v1TablePath = v1Dir.getAbsolutePath();
    String v2TablePath = v2Dir.getAbsolutePath();

    createDvPartitionedTable(v1TablePath);
    seedAndDeleteAndAppend(v1TablePath);

    createDvPartitionedTable(v2TablePath);
    seedAndDeleteAndAppend(v2TablePath);

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("ignoreDeletes", "true")
            .option("maxFilesPerTrigger", "1")
            .load(v1TablePath)
            .selectExpr("id", "p");
    List<Row> v1Rows = processStreamingQuery(v1Stream, "dv_ignore_deletes_mft_v1");

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("ignoreDeletes", "true")
            .option("maxFilesPerTrigger", "1")
            .table(str("dsv2.delta.`%s`", v2TablePath))
            .selectExpr("id", "p");
    List<Row> v2Rows = processStreamingQuery(v2Stream, "dv_ignore_deletes_mft_v2");

    // This assertion fails because of Bug #28 - V2 does not skip DV-based delete commits with
    // ignoreDeletes=true.
    assertDataEquals(v2Rows, v1Rows);
  }

  /**
   * Same DV + ignoreDeletes setup, but using {@code Trigger.AvailableNow} so the stream
   * self-terminates after draining the snapshot. V1 skips the DELETE commit; V2 diverges. Parity
   * assertion fails until Bug #28 is fixed.
   */
  @Test
  public void testDvTable_ignoreDeletes_withAvailableNow(@TempDir File baseDir) throws Exception {
    File v1Dir = new File(baseDir, "v1");
    File v2Dir = new File(baseDir, "v2");
    assertTrue(v1Dir.mkdirs(), "Failed to create v1 dir");
    assertTrue(v2Dir.mkdirs(), "Failed to create v2 dir");

    String v1TablePath = v1Dir.getAbsolutePath();
    String v2TablePath = v2Dir.getAbsolutePath();

    createDvPartitionedTable(v1TablePath);
    seedAndDeleteAndAppend(v1TablePath);

    createDvPartitionedTable(v2TablePath);
    seedAndDeleteAndAppend(v2TablePath);

    List<Row> v1Rows =
        runAvailableNowMemoryRows(
            spark
                .readStream()
                .format("delta")
                .option("ignoreDeletes", "true")
                .load(v1TablePath)
                .selectExpr("id", "p"),
            "dv_ignore_deletes_avail_v1");

    List<Row> v2Rows =
        runAvailableNowMemoryRows(
            spark
                .readStream()
                .option("ignoreDeletes", "true")
                .table(str("dsv2.delta.`%s`", v2TablePath))
                .selectExpr("id", "p"),
            "dv_ignore_deletes_avail_v2");

    // This assertion fails because of Bug #28 - V2 does not skip DV-based delete commits with
    // ignoreDeletes=true.
    assertDataEquals(v2Rows, v1Rows);
  }

  // -- helpers --

  /** CREATE TABLE with DV enabled, partitioned by {@code p}. */
  private void createDvPartitionedTable(String path) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p INT) USING delta PARTITIONED BY (p) "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            path));
  }

  /**
   * Standard fixture for Bug #28: INSERT into p=1 and p=2 (separate files per partition),
   * whole-partition DELETE on p=1 (with DV enabled this produces AddFile+DV, not RemoveFile), then
   * INSERT more rows into p=2.
   */
  private void seedAndDeleteAndAppend(String tablePath) {
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 1), (2, 1)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (10, 2), (11, 2)", tablePath));
    // Whole-partition DELETE on p=1. With delta.enableDeletionVectors=true, this commit consists
    // of an AddFile carrying a DV (not a plain RemoveFile), which is the path Bug #28 hits.
    spark.sql(str("DELETE FROM delta.`%s` WHERE p = 1", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (20, 2), (21, 2)", tablePath));
  }

  /** Run a streaming query under {@code Trigger.AvailableNow} against a memory sink. */
  private List<Row> runAvailableNowMemoryRows(Dataset<Row> streamingDF, String queryName)
      throws Exception {
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
      assertTrue(query.awaitTermination(60_000L), "AvailableNow did not terminate within 60s");
      return spark.sql("SELECT * FROM " + queryName).collectAsList();
    } finally {
      if (query != null) {
        query.stop();
        DeltaLog.clearCache();
      }
    }
  }

  // -- Bug #28 surfaces in additional compound-feature and option-combination scenarios. The tests
  // below are copied from V2StreamingCmRtDvTest, V2StreamingOptionCombinationsRateLimitTest,
  // V2StreamingOptionCombinationsTriggerStartingTest, V2StreamingOptionCombinationsFilterFaultTest,
  // and V2StreamingSchemaEvoLongTailTest. Each fails due to Bug #28 (DV + ignoreDeletes /
  // ignoreChanges / skipChangeCommits wrong row counts).

  /**
   * Compound CM-name + RT + DV + partitions x {@code ignoreDeletes=true}. v0 CREATE, v1 INSERT
   * across two partitions, v2 whole-partition DELETE (file-granular, ignoreDeletes-friendly), v3
   * INSERT more. The DELETE commit must be skipped; only INSERT rows surface, all under the
   * column-mapped + row-tracked + DV-enabled physical layout.
   */
  @Test
  public void testCompound_ignoreDeletes(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvPartitionedTable(tablePath);
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 'name-1', 'x'), (2, 'name-2', 'y')", tablePath));
    // Whole-partition delete on p='y': file-granular, allowed under ignoreDeletes.
    spark.sql(str("DELETE FROM delta.`%s` WHERE p = 'y'", tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (3, 'name-3', 'x'), (4, 'name-4', 'z')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("ignoreDeletes", "true")
            .table(dsv2TableRef)
            .selectExpr("id", "name", "p");

    List<Row> rows = processStreamingQuery(streamingDF, "compound_ignore_deletes");
    // 4 INSERT rows survive; the DELETE commit is dropped by ignoreDeletes.
    assertEquals(
        4,
        rows.size(),
        () -> "expected 4 INSERT rows with DELETE skipped under compound features, got: " + rows);
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      long id = r.getLong(0);
      ids.add(id);
      assertEquals("name-" + id, r.getString(1), "name column must align with id under CM rewrite");
    }
    assertEquals(Set.of(1L, 2L, 3L, 4L), ids);
  }

  /**
   * Compound CM-name + RT + DV + partitions x {@code ignoreChanges=true}. v0 CREATE, v1 INSERT, v2
   * UPDATE (re-emits the rewritten file's rows as appends under ignoreChanges), v3 INSERT more. The
   * stream must not error; INSERT rows are present, and ignoreChanges treats the UPDATE rewrite as
   * a re-emitted append rather than a hard failure.
   */
  @Test
  public void testCompound_ignoreChanges(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvPartitionedTable(tablePath);
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 'name-1', 'x'), (2, 'name-2', 'y')", tablePath));
    // UPDATE p='y' row: under DV this is a rewrite commit; ignoreChanges re-emits the new AddFile
    // as an append rather than failing the stream.
    spark.sql(str("UPDATE delta.`%s` SET name = 'updated-2' WHERE id = 2", tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (3, 'name-3', 'x'), (4, 'name-4', 'z')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("ignoreChanges", "true")
            .table(dsv2TableRef)
            .selectExpr("id", "name", "p");

    List<Row> rows = processStreamingQuery(streamingDF, "compound_ignore_changes");
    // The two original INSERT rows + the UPDATE-rewritten p='y' file (re-emitted as an append by
    // ignoreChanges, carrying the updated name) + the two final-INSERT rows = 5 rows total.
    assertEquals(
        5,
        rows.size(),
        () ->
            "expected 5 rows (2 initial INSERTs + UPDATE rewrite re-emitted + 2 final INSERTs)"
                + " under ignoreChanges, got: "
                + rows);
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      ids.add(r.getLong(0));
    }
    // All inserted ids must be present; the UPDATE re-emits id=2 (now once via the rewrite, since
    // the original p='y' file was rewritten in place).
    assertEquals(Set.of(1L, 2L, 3L, 4L), ids);
    long updatedRowCount = rows.stream().filter(r -> "updated-2".equals(r.getString(1))).count();
    assertEquals(
        1,
        updatedRowCount,
        () ->
            "expected exactly one row to carry the updated name under ignoreChanges, got: " + rows);
  }

  /**
   * Compound CM-name + RT + DV + partitions x {@code skipChangeCommits=true}. v0 CREATE, v1 INSERT,
   * v2 UPDATE (change commit - dropped entirely by skipChangeCommits), v3 INSERT more. Only INSERT
   * rows surface; the UPDATE rewrite does not produce any rows on the stream.
   */
  @Test
  public void testCompound_skipChangeCommits(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createCmNameRtDvPartitionedTable(tablePath);
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 'name-1', 'x'), (2, 'name-2', 'y')", tablePath));
    // UPDATE: change commit, dropped entirely by skipChangeCommits.
    spark.sql(str("UPDATE delta.`%s` SET name = 'updated-2' WHERE id = 2", tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (3, 'name-3', 'x'), (4, 'name-4', 'z')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(dsv2TableRef)
            .selectExpr("id", "name", "p");

    List<Row> rows = processStreamingQuery(streamingDF, "compound_skip_change_commits");
    // 4 INSERT rows surface; the UPDATE change commit is dropped, so no row carries 'updated-2'.
    assertEquals(
        4,
        rows.size(),
        () ->
            "expected 4 INSERT rows with UPDATE commit dropped under skipChangeCommits, got: "
                + rows);
    Set<Long> ids = new HashSet<>();
    for (Row r : rows) {
      long id = r.getLong(0);
      ids.add(id);
      assertEquals(
          "name-" + id,
          r.getString(1),
          () -> "skipChangeCommits must not surface the UPDATE's rewritten rows, got: " + rows);
    }
    assertEquals(Set.of(1L, 2L, 3L, 4L), ids);
  }

  /**
   * INSERT, whole-file DELETE, INSERT - stream with {@code maxFilesPerTrigger=1 + ignoreDeletes=
   * true}. Partition by id so the DELETE removes whole files (ignoreDeletes only applies to
   * file-granular deletes, not row-level rewrites). Assert only INSERT rows surface, the rate limit
   * is respected, and the stream does not crash on the DELETE commit.
   */
  @Test
  public void testMaxFilesPerTrigger_withIgnoreDeletes(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta PARTITIONED BY (id)",
            tablePath));
    // Initial INSERTs across separate partitions (one file per partition).
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'Alice')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'Bob')", tablePath));
    // Whole-file DELETE on partition id=2.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 2", tablePath));
    // More INSERTs after the DELETE, into separate partitions.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'Carol')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'Dave')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "1")
            .option("ignoreDeletes", "true")
            .table(dsv2TableRef);

    StreamingQuery q =
        streamingDF
            .writeStream()
            .format("memory")
            .queryName("maxFiles_ignoreDeletes")
            .outputMode("append")
            .start();
    try {
      q.processAllAvailable();
      StreamingQueryProgress[] progress = nonEmptyProgress(q);
      // 4 INSERTs -> 4 files admitted; ignoreDeletes drops the DELETE commit entirely.
      assertEquals(
          4, progress.length, () -> "expected 4 batches (one per INSERT file), got: " + progress);
      for (StreamingQueryProgress p : progress) {
        assertEquals(1L, p.numInputRows(), "maxFilesPerTrigger=1 should admit exactly 1 row");
      }
      List<Row> rows = spark.sql("SELECT * FROM maxFiles_ignoreDeletes").collectAsList();
      assertEquals(4, rows.size(), () -> "expected 4 INSERT rows, got: " + rows);
      // Sanity check: only the inserted ids surface, never id=2's deleted file rewritten.
      Set<Integer> ids = new HashSet<>();
      for (Row r : rows) {
        ids.add(r.getInt(0));
      }
      assertEquals(
          new HashSet<>(Arrays.asList(1, 2, 3, 4)),
          ids,
          () -> "expected ids {1,2,3,4} from the INSERT commits; got: " + ids);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /**
   * D2. AvailableNow + ignoreDeletes=true: whole-file DELETE between INSERTs must not error the
   * stream, and only INSERT rows are visible at the sink. AvailableNow self-terminates.
   */
  @Test
  public void testAvailableNow_withIgnoreDeletes(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            tablePath));
    // INSERT, whole-file DELETE (by partition), then more INSERTs. With ignoreDeletes=true the
    // stream sees v1 (3 rows in part=0) and v3 (3 rows in part=1), but not the DELETE.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 0), (2, 0), (3, 0)", tablePath)); // v=1
    spark.sql(str("DELETE FROM delta.`%s` WHERE part = 0", tablePath)); // v=2 (whole-file)
    spark.sql(str("INSERT INTO delta.`%s` VALUES (10, 1), (11, 1), (12, 1)", tablePath)); // v=3

    long rows =
        runAvailableNowMemoryCount(
            spark.readStream().option("ignoreDeletes", "true").table(dsv2Ref),
            "avail_ignore_deletes");
    // 3 rows from v1 + 3 rows from v3 = 6. The DELETE is silently dropped by ignoreDeletes.
    assertEquals(
        6L, rows, () -> "AvailableNow + ignoreDeletes should see 6 INSERT rows; got: " + rows);
  }

  /**
   * {@code maxBytesPerTrigger=1b} x {@code ignoreDeletes=true}.
   *
   * <p>Partitioned table so DELETE is whole-file. v0 CREATE, v1 INSERT into two partitions, v2
   * DELETE one partition (whole-file delete), v3 INSERT more. ignoreDeletes drops the DELETE commit
   * and the byte-rate limit (at least one file per batch) admits the INSERT files correctly.
   */
  @Test
  public void testMaxBytesPerTrigger_withIgnoreDeletes(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    // Partition by p so DELETE removes whole files (ignoreDeletes only allows file-granular
    // deletes; a row-level rewrite would still error).
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'x'), (2, 'y')", tablePath));
    // Whole-partition delete: removes the file for p='y'.
    spark.sql(str("DELETE FROM delta.`%s` WHERE p = 'y'", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'x'), (4, 'z')", tablePath));

    Dataset<Row> v2Stream =
        spark
            .readStream()
            .option("ignoreDeletes", "true")
            .option("maxBytesPerTrigger", "1b")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "mbpt_id_v2");

    Dataset<Row> v1Stream =
        spark
            .readStream()
            .format("delta")
            .option("ignoreDeletes", "true")
            .option("maxBytesPerTrigger", "1b")
            .load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "mbpt_id_v1");

    assertDataEquals(v2Rows, v1Rows);
    // 4 INSERT rows (id=1,2,3,4) - DELETE commit is dropped.
    assertEquals(
        4, v2Rows.size(), () -> "expected 4 INSERT rows with DELETE skipped, got: " + v2Rows);
  }

  /**
   * S9 Type widening x {@code ignoreDeletes=true}. Widen INT->LONG, insert into a partitioned
   * column, DELETE one partition (file-granular remove), then INSERT more. Without ignoreDeletes
   * the stream would fail on the DELETE; with ignoreDeletes=true the DELETE is dropped and all
   * INSERT rows survive. DSv1 parity check.
   */
  @Test
  public void testTypeWidening_intToLong_ignoreDeletes(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    // Partition by id so DELETE removes a whole file (ignoreDeletes only applies to file-granular
    // deletes, not DV / row-level rewrites).
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, data INT) USING delta PARTITIONED BY (id) "
                + "TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 10), (2, 20)", tablePath));
    spark.sql(str("ALTER TABLE delta.`%s` CHANGE COLUMN data data LONG", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 3000000000), (4, 4000000000)", tablePath));
    // Partition delete: drops the file for id=2 wholesale, no DV rewrite.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 2", tablePath));

    Dataset<Row> v2Stream =
        spark.readStream().option("ignoreDeletes", "true").table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(v2Stream, "widen_ignore_deletes_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("ignoreDeletes", "true").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "widen_ignore_deletes_v1");

    assertDataEquals(v2Rows, v1Rows);
    // Surviving INSERT rows (pre + post-widening) surface; the DELETE commit is suppressed.
    assertEquals(
        4,
        v2Rows.size(),
        () -> "Type widening + ignoreDeletes should emit 4 surviving rows; got: " + v2Rows);
    // Stream schema must carry the widened LONG type.
    assertEquals(
        org.apache.spark.sql.types.DataTypes.LongType,
        v2Stream.schema().apply("data").dataType(),
        () -> "Stream schema for `data` should be LONG; got: " + v2Stream.schema().apply("data"));
  }

  // -- helpers for the appended tests --

  /**
   * CREATE TABLE with column mapping (name mode) + row tracking + deletion vectors enabled,
   * partitioned by {@code p}. Used by the compound-feature tests.
   */
  private void createCmNameRtDvPartitionedTable(String path) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, name STRING, p STRING) USING delta "
                + "PARTITIONED BY (p) "
                + "TBLPROPERTIES ("
                + "  'delta.columnMapping.mode' = 'name',"
                + "  'delta.enableRowTracking' = 'true',"
                + "  'delta.enableDeletionVectors' = 'true',"
                + "  'delta.minReaderVersion' = '3',"
                + "  'delta.minWriterVersion' = '7')",
            path));
  }

  /** Returns recent progress entries that produced rows. */
  private static StreamingQueryProgress[] nonEmptyProgress(StreamingQuery q) {
    return Arrays.stream(q.recentProgress())
        .filter(p -> p.numInputRows() != 0L)
        .toArray(StreamingQueryProgress[]::new);
  }

  /** Run an AvailableNow query against a memory sink and return the sink's row count. */
  private long runAvailableNowMemoryCount(Dataset<Row> streamingDF, String queryName)
      throws Exception {
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
      assertTrue(query.awaitTermination(60_000L), "AvailableNow did not terminate within 60s");
      return spark.sql("SELECT COUNT(*) FROM " + queryName).collectAsList().get(0).getLong(0);
    } finally {
      if (query != null) {
        query.stop();
        DeltaLog.clearCache();
      }
    }
  }
}
