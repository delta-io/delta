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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Bug #27: DSv2 does not honor {@code failOnDataLoss=false} when {@code logRetentionDuration} has
 * pruned old commit JSON files.
 *
 * <p>When Delta log retention prunes old commit JSON files, DSv2 surfaces {@code
 * InvalidTableException: Missing delta files - versions are not contiguous} from Kernel's {@code
 * CommitRangeFactory}. DSv1 honors {@code failOnDataLoss=false} by skipping over the gap. DSv2
 * propagates the Kernel exception as a hard {@code StreamingQueryException} regardless of the
 * option.
 *
 * <p>Each test in this file uses DSv1 as the oracle (expected to succeed and emit rows) and pins
 * the DSv2 failure shape so the divergence is documented. When the bug is fixed, the V2 assertion
 * blocks here will need to be inverted to match V1.
 */
public class V2StreamingLogRetentionFailOnDataLossTest extends V2TestBase {

  /** Force a checkpoint so the snapshot can be reconstructed without the pruned commit JSON. */
  @SuppressWarnings("deprecation")
  private void checkpoint(String tablePath) {
    DeltaLog.forTable(spark, tablePath).checkpoint();
  }

  /**
   * Simulate {@code logRetentionDuration} expiry by deleting the commit JSON for {@code version}
   * (and its CRC sibling) under {@code _delta_log/}.
   */
  private void pruneCommitJson(String tablePath, long version) throws Exception {
    String name = String.format("%020d.json", version);
    Path json = Paths.get(tablePath, "_delta_log", name);
    Files.delete(json);
    Path crc = Paths.get(tablePath, "_delta_log", String.format("%020d.crc", version));
    if (Files.exists(crc)) {
      Files.delete(crc);
    }
    DeltaLog.clearCache();
  }

  /** Build a table of 4 single-row commits (v1..v4 INSERT after v0 CREATE) at {@code tablePath}. */
  private void buildFourCommitTable(String tablePath) {
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    for (int i = 1; i <= 4; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d)", tablePath, i));
    }
  }

  /**
   * Assert that {@code ex} (a Throwable thrown by a DSv2 streaming query) indicates the Bug #27
   * "not contiguous" / InvalidTableException failure shape. Checks the exception itself and its
   * cause chain.
   */
  private static void assertBug27FailureShape(Throwable ex) {
    String top = ex.toString();
    Throwable cause = ex.getCause();
    String causeStr = cause == null ? "" : cause.toString();
    assertTrue(
        top.contains("not contiguous")
            || top.contains("InvalidTable")
            || causeStr.contains("not contiguous")
            || causeStr.contains("InvalidTable"),
        () -> "Expected InvalidTableException / not-contiguous error but got: " + ex);
  }

  /**
   * Test 1. Basic streaming read with a pruned middle commit JSON.
   *
   * <p>Create a table with 4 INSERT commits (v0=CREATE, v1..v4=INSERT), force a checkpoint, then
   * delete the v1 commit JSON + CRC. Stream with {@code failOnDataLoss=false} from {@code
   * startingVersion=0}.
   *
   * <p>V1 (oracle): succeeds and emits rows from the reconstructed snapshot. V2 (Bug #27):
   * propagates Kernel's {@code InvalidTableException}.
   */
  @Test
  public void testLogRetentionPrune_basicStream(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    buildFourCommitTable(tablePath);

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 1L);

    String tag = "basic";

    // V1 (oracle): should succeed and emit rows.
    List<Row> v1Rows = null;
    try {
      v1Rows =
          processStreamingQuery(
              spark
                  .readStream()
                  .format("delta")
                  .option("failOnDataLoss", "false")
                  .option("startingVersion", "0")
                  .load(tablePath),
              "v1_" + tag);
    } catch (Exception e) {
      fail("V1 should honor failOnDataLoss=false but threw: " + e);
    }
    assertFalse(v1Rows.isEmpty(), () -> "V1 should emit rows from reconstructed snapshot");

    // V2 (Bug #27): currently throws InvalidTableException. When the bug is fixed, this assertion
    // block must be inverted to match V1's behavior.
    try {
      processStreamingQuery(
          spark
              .readStream()
              .option("failOnDataLoss", "false")
              .option("startingVersion", "0")
              .table(str("dsv2.delta.`%s`", tablePath)),
          "v2_" + tag);
      fail(
          "Expected V2 to fail with InvalidTableException / not-contiguous - if this passes, Bug"
              + " #27 is fixed and this test should be updated to assert V1 parity.");
    } catch (Exception e) {
      assertBug27FailureShape(e);
    }
  }

  /**
   * Test 2. Pruned middle commit JSON composed with {@code excludeRegex}.
   *
   * <p>Same setup as Test 1, but also passes an {@code excludeRegex} option to the stream.
   *
   * <p>V1 (oracle): succeeds. V2 (Bug #27): still propagates the Kernel exception - the regex
   * filter does not change the underlying commit-range failure.
   */
  @Test
  public void testLogRetentionPrune_withExcludeRegex(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    buildFourCommitTable(tablePath);

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 1L);

    String tag = "excl";

    // V1 (oracle): should succeed and emit rows.
    List<Row> v1Rows = null;
    try {
      v1Rows =
          processStreamingQuery(
              spark
                  .readStream()
                  .format("delta")
                  .option("failOnDataLoss", "false")
                  .option("excludeRegex", "nonmatching_regex_xyz")
                  .option("startingVersion", "0")
                  .load(tablePath),
              "v1_" + tag);
    } catch (Exception e) {
      fail("V1 should honor failOnDataLoss=false but threw: " + e);
    }
    assertFalse(v1Rows.isEmpty(), () -> "V1 should emit rows from reconstructed snapshot");

    // V2 (Bug #27): currently throws InvalidTableException.
    try {
      processStreamingQuery(
          spark
              .readStream()
              .option("failOnDataLoss", "false")
              .option("excludeRegex", "nonmatching_regex_xyz")
              .option("startingVersion", "0")
              .table(str("dsv2.delta.`%s`", tablePath)),
          "v2_" + tag);
      fail(
          "Expected V2 to fail with InvalidTableException / not-contiguous - if this passes, Bug"
              + " #27 is fixed and this test should be updated to assert V1 parity.");
    } catch (Exception e) {
      assertBug27FailureShape(e);
    }
  }

  /**
   * Test 3. Pruned middle commit JSON composed with {@code maxFilesPerTrigger=1}.
   *
   * <p>Same setup as Test 1, but also passes {@code maxFilesPerTrigger=1} to the stream so each
   * micro-batch admits exactly one file.
   *
   * <p>V1 (oracle): succeeds; rate limit does not interact with the pruned commit. V2 (Bug #27):
   * still propagates the Kernel exception from CommitRangeFactory.
   */
  @Test
  public void testLogRetentionPrune_withMaxFilesPerTrigger(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    buildFourCommitTable(tablePath);

    checkpoint(tablePath);
    pruneCommitJson(tablePath, /* version= */ 1L);

    String tag = "mfpt";

    // V1 (oracle): should succeed and emit rows.
    List<Row> v1Rows = null;
    try {
      v1Rows =
          processStreamingQuery(
              spark
                  .readStream()
                  .format("delta")
                  .option("failOnDataLoss", "false")
                  .option("maxFilesPerTrigger", "1")
                  .option("startingVersion", "0")
                  .load(tablePath),
              "v1_" + tag);
    } catch (Exception e) {
      fail("V1 should honor failOnDataLoss=false but threw: " + e);
    }
    assertFalse(v1Rows.isEmpty(), () -> "V1 should emit rows from reconstructed snapshot");

    // V2 (Bug #27): currently throws InvalidTableException.
    try {
      processStreamingQuery(
          spark
              .readStream()
              .option("failOnDataLoss", "false")
              .option("maxFilesPerTrigger", "1")
              .option("startingVersion", "0")
              .table(str("dsv2.delta.`%s`", tablePath)),
          "v2_" + tag);
      fail(
          "Expected V2 to fail with InvalidTableException / not-contiguous - if this passes, Bug"
              + " #27 is fixed and this test should be updated to assert V1 parity.");
    } catch (Exception e) {
      assertBug27FailureShape(e);
    }
  }
}
