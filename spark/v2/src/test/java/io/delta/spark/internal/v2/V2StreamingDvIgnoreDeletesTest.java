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
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
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
}
