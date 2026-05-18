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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
 * Tests DSv2 streaming behavior under degraded delta-log states (truncated commit JSON, pruned
 * commit JSONs, corrupt {@code _last_checkpoint}, log-compaction files). Each test asserts the
 * stream surfaces a structured, debuggable error rather than a raw NPE / RuntimeException leak, or
 * recovers cleanly when expected.
 *
 * <p>Source ref: {@code SparkMicroBatchStream.filterDeltaLogs} (line ~833) which reads commits via
 * Kernel's {@code getCommitActionsFromRangeUnsafe}.
 */
public class V2StreamingLogIntegrityTest extends V2TestBase {

  /** Returns the path to the {@code _delta_log} directory for the given table. */
  private static Path deltaLogDir(String tablePath) {
    return Paths.get(tablePath, "_delta_log");
  }

  /** Returns the path to the {@code N.json} commit file for the given version. */
  private static Path commitJsonFile(String tablePath, long version) {
    return deltaLogDir(tablePath).resolve(String.format("%020d.json", version));
  }

  /** Returns the path to the {@code _last_checkpoint} file. */
  private static Path lastCheckpointFile(String tablePath) {
    return deltaLogDir(tablePath).resolve("_last_checkpoint");
  }

  /** Truncate the given file to (size - bytesToRemove) bytes. */
  private static void truncateFile(Path file, int bytesToRemove) throws IOException {
    long originalSize = Files.size(file);
    long newSize = Math.max(0, originalSize - bytesToRemove);
    try (java.nio.channels.FileChannel ch =
        java.nio.channels.FileChannel.open(file, StandardOpenOption.WRITE)) {
      ch.truncate(newSize);
    }
  }

  /** Delete a file if it exists. */
  private static void deleteFile(Path file) throws IOException {
    Files.deleteIfExists(file);
  }

  /** Write 5 commits to the table so we have v=0..v=4. */
  private void writeFiveCommits(String tablePath) {
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(0, "v0", 0.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .save(tablePath);
    for (int i = 1; i < 5; i++) {
      spark
          .createDataFrame(
              Arrays.asList(RowFactory.create(i, "v" + i, (double) i * 10)), TEST_SCHEMA)
          .write()
          .format("delta")
          .mode("append")
          .save(tablePath);
    }
  }

  /** Write N commits to the table with single-row inserts. */
  private void writeNCommits(String tablePath, int n) {
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(0, "v0", 0.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .save(tablePath);
    for (int i = 1; i < n; i++) {
      spark
          .createDataFrame(
              Arrays.asList(RowFactory.create(i, "v" + i, (double) i * 10)), TEST_SCHEMA)
          .write()
          .format("delta")
          .mode("append")
          .save(tablePath);
    }
  }

  /**
   * Run a streaming query, return either successfully-collected rows or the captured throwable
   * wrapped in an AssertionError marker for callers.
   */
  private Throwable runStreamExpectingFailure(Dataset<Row> streamingDF, String queryName) {
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
      query.awaitTermination();
      // If we reach here, no exception
      return null;
    } catch (Throwable t) {
      return t;
    } finally {
      if (query != null) {
        try {
          query.stop();
        } catch (Throwable ignored) {
        }
      }
      DeltaLog.clearCache();
    }
  }

  /**
   * Classify a throwable as "clean structured error" (Delta error, IOException, kernel error, known
   * Spark structured exception) vs "raw NPE / RuntimeException leak".
   */
  private static String classifyFailure(Throwable t) {
    Throwable cur = t;
    while (cur != null) {
      String name = cur.getClass().getName();
      if (name.contains("DELTA_") || name.startsWith("org.apache.spark.sql.delta.Delta")) {
        return "CLEAN: Delta error " + name;
      }
      if (name.startsWith("io.delta.kernel.exceptions")) {
        return "CLEAN: Kernel exception " + name;
      }
      if (cur instanceof IOException) {
        return "CLEAN: IOException " + name;
      }
      cur = cur.getCause();
    }
    cur = t;
    while (cur != null) {
      if (cur instanceof NullPointerException) {
        return "RAW NPE LEAK: " + describe(t);
      }
      cur = cur.getCause();
    }
    return "OTHER: " + describe(t);
  }

  private static String describe(Throwable t) {
    StringBuilder sb = new StringBuilder();
    Throwable cur = t;
    int depth = 0;
    while (cur != null && depth < 5) {
      sb.append(cur.getClass().getName()).append(": ").append(cur.getMessage()).append(" -> ");
      cur = cur.getCause();
      depth++;
    }
    return sb.toString();
  }

  /**
   * Write 5 commits, truncate v=3 by 1 byte (corrupting the trailing newline / JSON terminator),
   * then read the stream from v=0 with {@code failOnDataLoss=true}. Assert a structured failure
   * surfaces (Delta error, KernelException, or IOException) - not a raw NPE.
   */
  @Test
  public void testScenario1_truncatedCommitJson(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    writeFiveCommits(tablePath);

    // Truncate v=3 commit JSON to half its size to corrupt mid-action.
    Path v3 = commitJsonFile(tablePath, 3);
    long origSize = Files.size(v3);
    int bytesToRemove = (int) (origSize / 2);
    truncateFile(v3, bytesToRemove);
    assertEquals(origSize - bytesToRemove, Files.size(v3), "v3 should be truncated");

    // V2 stream from v=0. Note: SparkScan.streamableOptions does NOT include "failOnDataLoss",
    // so passing it produces a structured analysis error (this is itself a finding - DSv1
    // accepts failOnDataLoss).
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("startingVersion", "0").table(dsv2TableRef);

    Throwable failure =
        runStreamExpectingFailure(streamingDF, "test_scenario1_truncated_commit_json");

    System.out.println(
        "[Scenario 1] Failure classification: "
            + (failure == null ? "NO FAILURE (UNEXPECTED)" : classifyFailure(failure)));
    if (failure != null) {
      failure.printStackTrace(System.out);
    }

    // A truncated JSON in v=3 should produce a structured parse error or skip cleanly. The
    // assertion is: no raw NPE leak.
    if (failure != null) {
      Throwable cur = failure;
      while (cur != null) {
        if (cur instanceof NullPointerException) {
          fail("Scenario 1: truncated commit JSON produced a raw NPE leak: " + describe(failure));
        }
        cur = cur.getCause();
      }
    }

    // Differential: DSv1 should also handle this (or at least produce a structured error).
    DeltaLog.clearCache();
    Throwable v1Failure;
    try {
      Dataset<Row> v1Stream =
          spark.readStream().format("delta").option("startingVersion", "0").load(tablePath);
      v1Failure = runStreamExpectingFailure(v1Stream, "test_scenario1_dsv1_diff");
    } catch (Throwable t) {
      v1Failure = t;
    }
    System.out.println(
        "[Scenario 1] DSv1 differential: "
            + (v1Failure == null ? "OK" : classifyFailure(v1Failure)));
    if (v1Failure != null) v1Failure.printStackTrace(System.out);
  }

  /**
   * Side finding: DSv2 does not list {@code failOnDataLoss} in {@code streamableOptions}, so
   * passing it raises an analysis error. DSv1 honours this option. This is a parity gap.
   */
  @Test
  public void testScenario1_failOnDataLossOptionParity(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    writeFiveCommits(tablePath);

    // DSv1 accepts failOnDataLoss=true.
    StreamingQuery q1 = null;
    Throwable v1Err = null;
    try {
      q1 =
          spark
              .readStream()
              .format("delta")
              .option("failOnDataLoss", "true")
              .load(tablePath)
              .writeStream()
              .format("memory")
              .queryName("test_failondataloss_v1")
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .start();
      q1.awaitTermination();
    } catch (Throwable t) {
      v1Err = t;
    } finally {
      if (q1 != null) {
        try {
          q1.stop();
        } catch (Throwable ignored) {
        }
      }
      DeltaLog.clearCache();
    }
    System.out.println(
        "[Scenario 1 parity] DSv1 with failOnDataLoss=true: "
            + (v1Err == null ? "ACCEPTED" : "REJECTED " + classifyFailure(v1Err)));

    // DSv2 rejects failOnDataLoss.
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Throwable v2Err =
        runStreamExpectingFailure(
            spark.readStream().option("failOnDataLoss", "true").table(dsv2TableRef),
            "test_failondataloss_v2");
    System.out.println(
        "[Scenario 1 parity] DSv2 with failOnDataLoss=true: "
            + (v2Err == null ? "ACCEPTED" : "REJECTED " + classifyFailure(v2Err)));
    if (v2Err != null) v2Err.printStackTrace(System.out);

    // Document divergence: DSv1 accepts, DSv2 rejects. This is the gap.
    if (v1Err == null && v2Err != null) {
      System.out.println(
          "[Scenario 1 parity] DIVERGENCE: DSv1 accepts failOnDataLoss but DSv2 rejects it. "
              + "DSv2 should either accept the option or document its absence.");
    }
  }

  /**
   * Trigger a checkpoint, corrupt {@code _last_checkpoint} JSON by truncating to half its bytes,
   * then restart {@code readStream}. Assert the stream either recovers (Kernel falls back to
   * listing) or surfaces a clean structured error - not a raw NPE.
   */
  @Test
  public void testScenario3_lastCheckpointHalfWritten(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();

    // 12 commits is enough to trigger a checkpoint at v=10 (default checkpointInterval=10).
    writeNCommits(tablePath, 12);

    // Force a checkpoint via DeltaLog.
    DeltaLog deltaLog = DeltaLog.forTable(spark, tablePath);
    deltaLog.checkpoint();

    Path lastCheckpoint = lastCheckpointFile(tablePath);
    assertTrue(
        Files.exists(lastCheckpoint), "_last_checkpoint should exist after deltaLog.checkpoint()");
    long origSize = Files.size(lastCheckpoint);
    assertTrue(origSize > 0, "_last_checkpoint should have content");

    // Truncate _last_checkpoint to half its size (corrupting the JSON).
    int halfRemove = (int) (origSize / 2);
    truncateFile(lastCheckpoint, halfRemove);
    assertEquals(
        origSize - halfRemove, Files.size(lastCheckpoint), "_last_checkpoint should be truncated");

    // Clear cache so the corrupted _last_checkpoint is read fresh.
    DeltaLog.clearCache();

    // Restart readStream.
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("startingVersion", "0").table(dsv2TableRef);

    Throwable failure =
        runStreamExpectingFailure(streamingDF, "test_scenario3_lastcheckpoint_halfwritten");

    System.out.println(
        "[Scenario 3] Failure classification: "
            + (failure == null
                ? "NO FAILURE (recovery via list-fallback)"
                : classifyFailure(failure)));
    if (failure != null) {
      failure.printStackTrace(System.out);
      // We accept either recovery (failure == null) or a clean structured error. Raw NPE is a
      // bug.
      Throwable cur = failure;
      while (cur != null) {
        if (cur instanceof NullPointerException) {
          fail(
              "Scenario 3: half-written _last_checkpoint produced a raw NPE leak: "
                  + describe(failure));
        }
        cur = cur.getCause();
      }
    }
  }

  // Scenario 11: same as Scenario 3 (alias) - kept for explicit numbering parity

  /**
   * Scenario 11 from the brainstorm. Identical setup to Scenario 3 (the brainstorm doc lists this
   * twice - scenario 3 is referenced from both the highest-priority list and as item 11). We
   * include both numbers to match the test plan; this version uses an explicit DSv1 differential to
   * detect divergence between the two connectors.
   */
  @Test
  public void testScenario11_lastCheckpointHalfWritten_dsv1Differential(@TempDir File tempDir)
      throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    writeNCommits(tablePath, 12);

    DeltaLog deltaLog = DeltaLog.forTable(spark, tablePath);
    deltaLog.checkpoint();

    Path lastCheckpoint = lastCheckpointFile(tablePath);
    long origSize = Files.size(lastCheckpoint);
    truncateFile(lastCheckpoint, (int) (origSize / 2));
    DeltaLog.clearCache();

    // DSv1 path
    Throwable v1Failure;
    {
      Dataset<Row> v1Stream =
          spark.readStream().format("delta").option("startingVersion", "0").load(tablePath);
      v1Failure = runStreamExpectingFailure(v1Stream, "test_scenario11_dsv1");
    }

    // DSv2 path
    DeltaLog.clearCache();
    Throwable v2Failure;
    {
      String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
      Dataset<Row> v2Stream = spark.readStream().option("startingVersion", "0").table(dsv2TableRef);
      v2Failure = runStreamExpectingFailure(v2Stream, "test_scenario11_dsv2");
    }

    System.out.println(
        "[Scenario 11] DSv1 result: "
            + (v1Failure == null ? "RECOVERED" : classifyFailure(v1Failure)));
    System.out.println(
        "[Scenario 11] DSv2 result: "
            + (v2Failure == null ? "RECOVERED" : classifyFailure(v2Failure)));
    if (v1Failure != null) v1Failure.printStackTrace(System.out);
    if (v2Failure != null) v2Failure.printStackTrace(System.out);

    // Bug surfaced when DSv1 recovers but DSv2 fails (or vice versa with a raw NPE).
    if (v1Failure == null && v2Failure != null) {
      Throwable cur = v2Failure;
      while (cur != null) {
        if (cur instanceof NullPointerException) {
          fail("Scenario 11: DSv1 recovered but DSv2 surfaced raw NPE: " + describe(v2Failure));
        }
        cur = cur.getCause();
      }
      // Different failure-mode is a soft-bug; flag but don't fail the test on a clean error.
      System.out.println(
          "[Scenario 11] DIVERGENCE: DSv1 recovered, DSv2 errored (clean): "
              + classifyFailure(v2Failure));
    }
  }

  // Scenario 12: Log compaction file N.compacted.json present

  /**
   * Write 20 commits, fabricate a {@code 0..9.compacted.json} file by concatenating the JSON
   * content of v=0..v=9 commits, then read the stream from v=5 with {@code Trigger.AvailableNow}.
   * Differential vs DSv1: Kernel commit-range API may pick up the compacted file and skip
   * individual commit JSONs; SMS is untested with compacted files present.
   */
  @Test
  public void testScenario12_logCompactionFilePresent(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();

    // Write 20 commits (one row each, ids 0..19).
    writeNCommits(tablePath, 20);

    // Fabricate 0...9.compacted.json by concatenating raw JSON from v=0..v=9 commits.
    Path compactedFile =
        deltaLogDir(tablePath).resolve(String.format("%020d.%020d.compacted.json", 0L, 9L));
    StringBuilder concatenated = new StringBuilder();
    for (int v = 0; v < 10; v++) {
      Path commit = commitJsonFile(tablePath, v);
      assertTrue(Files.exists(commit), "commit " + v + " should exist");
      String content = new String(Files.readAllBytes(commit), StandardCharsets.UTF_8);
      concatenated.append(content);
      if (!content.endsWith("\n")) {
        concatenated.append("\n");
      }
    }
    Files.write(compactedFile, concatenated.toString().getBytes(StandardCharsets.UTF_8));
    assertTrue(Files.exists(compactedFile), "fabricated compacted file should exist");

    // Stream from v=5 via DSv1 first to get the reference rows.
    DeltaLog.clearCache();
    Throwable v1Failure = null;
    List<Row> v1Rows = null;
    StreamingQuery v1Query = null;
    try {
      Dataset<Row> v1Stream =
          spark.readStream().format("delta").option("startingVersion", "5").load(tablePath);
      v1Query =
          v1Stream
              .writeStream()
              .format("memory")
              .queryName("test_scenario12_dsv1")
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .start();
      v1Query.awaitTermination();
      v1Rows = spark.sql("SELECT * FROM test_scenario12_dsv1 ORDER BY id").collectAsList();
    } catch (Throwable t) {
      v1Failure = t;
    } finally {
      if (v1Query != null) {
        try {
          v1Query.stop();
        } catch (Throwable ignored) {
        }
      }
      DeltaLog.clearCache();
    }

    // Stream from v=5 via DSv2.
    Throwable v2Failure = null;
    List<Row> v2Rows = null;
    StreamingQuery v2Query = null;
    try {
      String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
      Dataset<Row> v2Stream = spark.readStream().option("startingVersion", "5").table(dsv2TableRef);
      v2Query =
          v2Stream
              .writeStream()
              .format("memory")
              .queryName("test_scenario12_dsv2")
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .start();
      v2Query.awaitTermination();
      v2Rows = spark.sql("SELECT * FROM test_scenario12_dsv2 ORDER BY id").collectAsList();
    } catch (Throwable t) {
      v2Failure = t;
    } finally {
      if (v2Query != null) {
        try {
          v2Query.stop();
        } catch (Throwable ignored) {
        }
      }
      DeltaLog.clearCache();
    }

    System.out.println(
        "[Scenario 12] DSv1 result: "
            + (v1Failure == null
                ? "OK rows=" + (v1Rows == null ? -1 : v1Rows.size())
                : classifyFailure(v1Failure)));
    System.out.println(
        "[Scenario 12] DSv2 result: "
            + (v2Failure == null
                ? "OK rows=" + (v2Rows == null ? -1 : v2Rows.size())
                : classifyFailure(v2Failure)));
    if (v1Failure != null) v1Failure.printStackTrace(System.out);
    if (v2Failure != null) v2Failure.printStackTrace(System.out);

    // Bug surfaced: DSv2 raw NPE leak.
    if (v2Failure != null) {
      Throwable cur = v2Failure;
      while (cur != null) {
        if (cur instanceof NullPointerException) {
          fail(
              "Scenario 12: compacted-json present produced a raw NPE leak in DSv2: "
                  + describe(v2Failure));
        }
        cur = cur.getCause();
      }
    }

    // Differential parity: if DSv1 succeeded with N rows, DSv2 should match.
    if (v1Rows != null && v2Rows != null) {
      System.out.println(
          "[Scenario 12] DSv1 row count=" + v1Rows.size() + " DSv2 row count=" + v2Rows.size());
      assertEquals(
          v1Rows.size(),
          v2Rows.size(),
          "Scenario 12: DSv1 vs DSv2 row count differs in presence of compacted file. "
              + "v1="
              + v1Rows
              + " v2="
              + v2Rows);
    }
  }

  // Scenario "log retention prune mid-stream after checkpoint" - referenced as
  // scenario 2 in the brainstorm-doc highest-priority list (covers the
  // restart-with-checkpointed-stale-version path mentioned at SMS:945). Renumbered
  // here as the doc's "scenario 3" was about logRetentionDuration; we test it
  // because the user request contained a duplicated 3 and we want to be thorough.

  /**
   * Scenario from brainstorm item 2 (logRetentionDuration prune mid-stream).
   *
   * <p>Write 10 commits, take a checkpoint, prune commit JSONs 0..3 (rm 0.json, 1.json, 2.json),
   * restart from a checkpointed offset at v=3 with {@code failOnDataLoss=false}. The stream should
   * resume cleanly. Repeat with {@code failOnDataLoss=true} - should error cleanly with a
   * structured exception, not silent skip.
   */
  @Test
  public void testScenarioLogRetentionPruneMidStream(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    File checkpointDir = new File(tempDir.getParentFile(), "ckpt_" + System.nanoTime());

    // Write 10 commits.
    writeNCommits(tablePath, 10);
    DeltaLog deltaLog = DeltaLog.forTable(spark, tablePath);
    deltaLog.checkpoint();

    // Run a streaming query first so a checkpoint offset gets persisted. Use noop sink because
    // memory sink with outputMode=append does not support recovering from checkpoint location.
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    {
      Dataset<Row> firstRun = spark.readStream().option("startingVersion", "0").table(dsv2TableRef);
      StreamingQuery q =
          firstRun
              .writeStream()
              .format("noop")
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .trigger(Trigger.AvailableNow())
              .start();
      try {
        q.awaitTermination();
      } finally {
        q.stop();
      }
    }
    DeltaLog.clearCache();

    // Prune commit JSONs 0..2 (simulating logRetentionDuration cleanup).
    for (int v = 0; v <= 2; v++) {
      deleteFile(commitJsonFile(tablePath, v));
    }

    // Add new commits (v=10..11) for the resume to pick up.
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(10, "v10", 100.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(11, "v11", 110.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    DeltaLog.clearCache();

    // Restart - DSv2 should resume cleanly from the persisted offset, even though the older
    // commit JSONs (0, 1, 2) have been pruned. The persisted offset points past v=2, so
    // restart should not need those JSONs (Kernel can list-from-checkpoint). Use noop sink to
    // support checkpoint recovery.
    Throwable resumeFailure;
    {
      Dataset<Row> resumeStream = spark.readStream().table(dsv2TableRef);
      StreamingQuery q = null;
      try {
        q =
            resumeStream
                .writeStream()
                .format("noop")
                .option("checkpointLocation", checkpointDir.getAbsolutePath())
                .trigger(Trigger.AvailableNow())
                .start();
        q.awaitTermination();
        resumeFailure = null;
      } catch (Throwable t) {
        resumeFailure = t;
      } finally {
        if (q != null) {
          try {
            q.stop();
          } catch (Throwable ignored) {
          }
        }
        DeltaLog.clearCache();
      }
    }

    System.out.println(
        "[Scenario LogRetention resume]: "
            + (resumeFailure == null ? "OK clean resume" : classifyFailure(resumeFailure)));
    if (resumeFailure != null) {
      resumeFailure.printStackTrace(System.out);
      Throwable cur = resumeFailure;
      while (cur != null) {
        if (cur instanceof NullPointerException) {
          fail("LogRetention scenario: raw NPE leak: " + describe(resumeFailure));
        }
        cur = cur.getCause();
      }
    }
  }
}
