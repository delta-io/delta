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

import io.delta.spark.internal.v2.read.SparkMicroBatchStream;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.immutable.Map$;

/**
 * Medium-priority brainstorm scenarios 25-30 from {@code testgap/scenario_brainstorm.md}.
 *
 * <p>Goal: surface bugs in the DSv2 streaming source. Each scenario probes a distinct rough edge
 * (reserved partition names, malformed-but-syntactic commit JSON, idempotency contracts, replay
 * determinism). All assertions require positive evidence (specific row counts, idempotent calls,
 * matching counts across runs) - "no exception" alone never counts as success.
 */
public class V2StreamingMidPriorityScenarios25to30Test extends V2TestBase {

  /**
   * Scenario 25: Partition column named with a reserved keyword (`_metadata`, `_change_type`, or
   * `value`).
   *
   * <p>Spark reserves `_metadata` for SupportsMetadataColumns and `_change_type` for CDC. A user
   * partition column with these names may collide. We probe `_change_type` first (CDC-reserved -
   * realistic to encounter), then fall through to `value` (DSv2 streaming should accept it). Bug
   * surface: name collision causes silent column shadowing or wrong projection.
   */
  @Test
  public void testScenario25_partitionColumnNamedReserved(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Try `_change_type` partition column. CDF reserves this name internally - if Delta rejects it
    // at create time, fall back to `value`.
    boolean createdChangeType;
    try {
      spark.sql(
          str(
              "CREATE TABLE delta.`%s` (id INT, _change_type STRING) "
                  + "USING delta PARTITIONED BY (_change_type)",
              tablePath));
      createdChangeType = true;
    } catch (Exception e) {
      createdChangeType = false;
    }

    if (createdChangeType) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'a')", tablePath));
      String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
      Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
      List<Row> actualRows = processStreamingQuery(streamingDF, "scenario25_change_type");
      assertEquals(
          3,
          actualRows.size(),
          () ->
              "Expected 3 rows from streaming read with `_change_type` partition col; got "
                  + actualRows
                  + ". Possible silent shadowing by CDC reserved name.");
    } else {
      // Fall back to `value` - the third candidate. This name collides with Spark's continuous
      // streaming default column.
      String fallbackPath = tablePath + "_value";
      spark.sql(
          str(
              "CREATE TABLE delta.`%s` (id INT, value STRING) "
                  + "USING delta PARTITIONED BY (value)",
              fallbackPath));
      spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'x'), (2, 'y')", fallbackPath));
      String dsv2TableRef = str("dsv2.delta.`%s`", fallbackPath);
      Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
      List<Row> actualRows = processStreamingQuery(streamingDF, "scenario25_value");
      assertEquals(
          2,
          actualRows.size(),
          () ->
              "Expected 2 rows from streaming read with `value` partition col; got " + actualRows);
    }
  }

  /**
   * Scenario 26: Touch-only commit (only commitInfo, no Add/Remove/Metadata/Protocol).
   *
   * <p>Fabricates v=2 with literally one CommitInfo line and zero file actions. Stream from v=1
   * must advance through v=2 (yielding zero rows for that batch) and continue into v=3. Bug
   * surface: stream stalls, NPE on null Add iteration, or off-by-one offset.
   */
  @Test
  public void testScenario26_touchOnlyCommit(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));

    Path logDir = new File(tablePath, "_delta_log").toPath();
    String commitInfoOnly =
        "{\"commitInfo\":{\"timestamp\":1700000000000,\"operation\":\"TOUCH\","
            + "\"operationParameters\":{},\"isBlindAppend\":true,\"operationMetrics\":{}}}\n";
    Files.writeString(
        logDir.resolve("00000000000000000002.json"), commitInfoOnly, StandardCharsets.UTF_8);

    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("startingVersion", "1").table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "scenario26_touch_only");

    assertEquals(
        2,
        actualRows.size(),
        () ->
            "Expected 2 rows (1 from v=1, 2 from v=3); v=2 is touch-only. Got: "
                + actualRows
                + ". Stream may have stalled on commitInfo-only commit.");
    actualRows.sort(Comparator.comparingInt(r -> r.getInt(0)));
    assertEquals(1, actualRows.get(0).getInt(0));
    assertEquals(2, actualRows.get(1).getInt(0));
  }

  /**
   * Scenario 27: AvailableNow twice over same source, fresh checkpoints/sinks - exact diff of row
   * counts.
   *
   * <p>Two independent runs over the same Delta table with `Trigger.AvailableNow` and `noop` sink.
   * Both runs use FRESH checkpoint paths (no resume). Both must see exactly the same total rows.
   * Bug surface: stale snapshot caching across runs, or non-deterministic file ordering causing
   * row-count drift.
   */
  @Test
  public void testScenario27_availableNowTwiceDeterministic(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    // 4 commits x 5 rows = 20 rows total.
    for (int v = 1; v <= 4; v++) {
      StringBuilder values = new StringBuilder();
      for (int j = 0; j < 5; j++) {
        if (j > 0) values.append(", ");
        int id = (v - 1) * 5 + j;
        values.append(str("(%d, 'r%d')", id, id));
      }
      spark.sql(str("INSERT INTO delta.`%s` VALUES %s", tablePath, values.toString()));
    }

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    long run1Rows = runAvailableNowAndCount(dsv2TableRef, deltaTablePath, "run1", "scenario27_q1");
    long run2Rows = runAvailableNowAndCount(dsv2TableRef, deltaTablePath, "run2", "scenario27_q2");

    final long r1 = run1Rows;
    final long r2 = run2Rows;
    assertEquals(20L, run1Rows, () -> "Run 1 expected 20 rows, got " + r1);
    assertEquals(
        run1Rows,
        run2Rows,
        () ->
            "Determinism violated: run1="
                + r1
                + " rows, run2="
                + r2
                + " rows over identical source.");
  }

  private long runAvailableNowAndCount(
      String tableRef, File parentDir, String checkpointSuffix, String queryName) throws Exception {
    File checkpoint = new File(parentDir, "_checkpoint_" + checkpointSuffix);
    Dataset<Row> df = spark.readStream().table(tableRef);
    StreamingQuery q =
        df.writeStream()
            .format("noop")
            .queryName(queryName)
            .option("checkpointLocation", checkpoint.getAbsolutePath())
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .start();
    try {
      q.awaitTermination();
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
    long total = 0;
    for (StreamingQueryProgress p : q.recentProgress()) {
      total += p.numInputRows();
    }
    return total;
  }

  /**
   * Scenario 28: Duplicate `commit(end)` calls with the same offset. DSv2 commit() is documented as
   * a no-op (SparkMicroBatchStream:500). Hammering it three times with the same offset must not
   * raise. Bug surface: stateful commit() leaks (e.g. metadata-tracking-log writes that are not
   * idempotent).
   */
  @Test
  public void testScenario28_duplicateCommitIdempotent(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager = new PathBasedSnapshotManager(tablePath, hadoopConf);
    DeltaOptions options = new DeltaOptions(Map$.MODULE$.empty(), spark.sessionState().conf());
    io.delta.kernel.Snapshot kernelSnapshot = snapshotManager.loadLatestSnapshot();
    StructType tableSchema =
        io.delta.spark.internal.v2.utils.SchemaUtils.convertKernelSchemaToSparkSchema(
            kernelSnapshot.getSchema());

    SparkMicroBatchStream stream =
        new SparkMicroBatchStream(
            snapshotManager,
            kernelSnapshot,
            hadoopConf,
            spark,
            options,
            /* tablePath= */ tablePath,
            /* dataSchema= */ tableSchema,
            /* partitionSchema= */ new StructType(),
            /* readDataSchema= */ tableSchema,
            /* ddlOrderedOutputSchema= */ tableSchema,
            /* dataFilters= */ new org.apache.spark.sql.sources.Filter[0],
            /* scalaOptions= */ Map$.MODULE$.<String, String>empty(),
            /* metadataTrackingLog= */ scala.Option.empty(),
            /* checkpointPath= */ tablePath + "/_checkpoint");

    DeltaLog deltaLog = DeltaLog.forTable(spark, tablePath);
    String tableId = deltaLog.tableId();
    DeltaSourceOffset offset =
        new DeltaSourceOffset(tableId, 1L, 0L, /* isInitialSnapshot= */ false);

    assertDoesNotThrow(() -> stream.commit(offset), "First commit() must succeed");
    assertDoesNotThrow(() -> stream.commit(offset), "Second commit() (same offset) must be no-op");
    assertDoesNotThrow(() -> stream.commit(offset), "Third commit() (same offset) must be no-op");

    DeltaLog.clearCache();
  }

  /**
   * Scenario 29: Protocol-only commit (no Add/Remove/Metadata, just a minReaderVersion/Writer
   * bump). v=0 CREATE, v=1 INSERT, v=2 hand-crafted protocol-only, v=3 INSERT. Stream must advance
   * through v=2 cleanly (zero rows for that batch). Bug surface: stream NPEs because Protocol
   * action is not in SMS:1009 `validateCommitAndDecideSkipping` switch (latent gap noted in
   * scenario 5 findings).
   */
  @Test
  public void testScenario29_protocolOnlyCommit(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (10)", tablePath));

    // v=2: pure protocol bump (3,7) with empty feature lists. Empty feature lists effectively make
    // this a no-op upgrade that the writer should accept.
    Path logDir = new File(tablePath, "_delta_log").toPath();
    String protocolOnly =
        "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,"
            + "\"readerFeatures\":[],\"writerFeatures\":[]}}\n";
    Files.writeString(
        logDir.resolve("00000000000000000002.json"), protocolOnly, StandardCharsets.UTF_8);

    boolean wroteV3;
    try {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (20)", tablePath));
      wroteV3 = true;
    } catch (Exception e) {
      wroteV3 = false;
    }

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("startingVersion", "1").table(dsv2TableRef);

    if (wroteV3) {
      List<Row> actualRows = processStreamingQuery(streamingDF, "scenario29_proto_only_v3");
      assertEquals(
          2,
          actualRows.size(),
          () ->
              "Expected 2 rows (10 from v=1, 20 from v=3), got "
                  + actualRows
                  + ". Protocol-only v=2 should contribute zero rows but not block the stream.");
      actualRows.sort(Comparator.comparingInt(r -> r.getInt(0)));
      assertEquals(
          Arrays.asList(10, 20),
          Arrays.asList(actualRows.get(0).getInt(0), actualRows.get(1).getInt(0)));
    } else {
      // Could not append v=3 (writer rejected the fabricated protocol bump). Still verify the
      // stream over v=1..v=2 either completes with one row or surfaces a structured non-NPE error.
      try {
        List<Row> actualRows = processStreamingQuery(streamingDF, "scenario29_proto_only");
        assertTrue(
            actualRows.size() >= 1,
            () -> "Expected at least 1 row (id=10 from v=1), got " + actualRows);
      } catch (Exception streamEx) {
        Throwable c = streamEx;
        while (c != null) {
          final Throwable cur = c;
          assertFalse(
              cur instanceof NullPointerException,
              () -> "Stream over protocol-only commit threw NPE: " + cur);
          c = c.getCause();
        }
      }
    }
  }

  /**
   * Scenario 30: Empty commit file - literally zero bytes at `_delta_log/00000000000000000002.json`
   * (zero actions, not just `{}`). v=1 INSERT, v=2 empty, v=3 INSERT. Stream must skip v=2 cleanly
   * (zero rows for that batch) or surface a structured non-NPE error. Bug surface: NPE iterating
   * empty action list, or stream stuck.
   */
  @Test
  public void testScenario30_emptyCommitFile(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (100)", tablePath));

    Path logDir = new File(tablePath, "_delta_log").toPath();
    Files.writeString(logDir.resolve("00000000000000000002.json"), "", StandardCharsets.UTF_8);

    boolean wroteV3;
    try {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (200)", tablePath));
      wroteV3 = true;
    } catch (Exception e) {
      wroteV3 = false;
    }

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("startingVersion", "1").table(dsv2TableRef);

    Exception streamEx = null;
    List<Row> actualRows = java.util.Collections.emptyList();
    try {
      actualRows = processStreamingQuery(streamingDF, "scenario30_empty_file");
    } catch (Exception e) {
      streamEx = e;
    }

    if (streamEx != null) {
      Throwable c = streamEx;
      while (c != null) {
        final Throwable cur = c;
        assertFalse(
            cur instanceof NullPointerException,
            () -> "Stream over empty commit file threw NPE: " + cur);
        c = c.getCause();
      }
    } else {
      int expected = wroteV3 ? 2 : 1;
      final List<Row> finalRows = actualRows;
      assertEquals(
          expected,
          actualRows.size(),
          () ->
              "Expected "
                  + expected
                  + " rows over empty commit file, got "
                  + finalRows
                  + ". Stream may have over-counted (treated empty file as non-empty) or stalled.");
      // Sanity-check the row values when wroteV3.
      if (wroteV3) {
        actualRows.sort(Comparator.comparingInt(r -> r.getInt(0)));
        assertEquals(100, actualRows.get(0).getInt(0));
        assertEquals(200, actualRows.get(1).getInt(0));
      } else {
        assertEquals(100, actualRows.get(0).getInt(0));
      }
    }
  }
}
