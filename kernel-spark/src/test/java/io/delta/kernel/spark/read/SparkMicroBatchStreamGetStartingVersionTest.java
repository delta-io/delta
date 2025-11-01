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
package io.delta.kernel.spark.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.kernel.spark.SparkDsv2TestBase;
import java.io.File;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.delta.CheckpointInstance;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.apache.spark.sql.delta.sources.DeltaSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Option;
import scala.collection.immutable.Map$;

/** Tests for SparkMicroBatchStream.getStartingVersion parity between DSv1 and DSv2. */
public class SparkMicroBatchStreamGetStartingVersionTest extends SparkDsv2TestBase {

  /**
   * Parameterized test that verifies parity between DSv1 DeltaSource.getStartingVersion and DSv2
   * SparkMicroBatchStream.getStartingVersion.
   */
  @ParameterizedTest
  @MethodSource("getStartingVersionParameters")
  public void testGetStartingVersion(
      String startingVersion, Optional<Long> expectedVersion, @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_starting_version_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create 5 versions (version 0 = CREATE TABLE, versions 1-5 = INSERTs)
    createVersions(testTableName, 5);

    testAndCompareStartingVersion(
        testTablePath, startingVersion, expectedVersion, "startingVersion=" + startingVersion);
  }

  /** Provides test parameters for the parameterized getStartingVersion test. */
  private static Stream<Arguments> getStartingVersionParameters() {
    return Stream.of(
        Arguments.of(/* startingVersion= */ "0", /* expectedVersion= */ Optional.of(0L)),
        Arguments.of(/* startingVersion= */ "1", /* expectedVersion= */ Optional.of(1L)),
        Arguments.of(/* startingVersion= */ "3", /* expectedVersion= */ Optional.of(3L)),
        Arguments.of(/* startingVersion= */ "5", /* expectedVersion= */ Optional.of(5L)),
        Arguments.of(/* startingVersion= */ "latest", /* expectedVersion= */ Optional.of(6L)),
        Arguments.of(/* startingVersion= */ null, /* expectedVersion= */ Optional.empty()));
  }

  /**
   * Test that verifies both DSv1 and DSv2 handle the case where no DeltaOptions are provided. DSv1
   * receives an empty DeltaOptions (no parameters), while DSv2 receives Optional.empty(). This
   * tests the equivalence between these two approaches.
   */
  @Test
  public void testGetStartingVersion_NoOptions(@TempDir File tempDir) throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_no_options_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create 5 versions (version 0 = CREATE TABLE, versions 1-5 = INSERTs)
    createVersions(testTableName, 5);

    // dsv1
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    DeltaOptions emptyOptions = new DeltaOptions(Map$.MODULE$.empty(), spark.sessionState().conf());
    DeltaSource deltaSource = createDeltaSource(deltaLog, testTablePath, emptyOptions);
    scala.Option<Object> dsv1Result = deltaSource.getStartingVersion();

    // dsv2
    SparkMicroBatchStream dsv2Stream =
        new SparkMicroBatchStream(testTablePath, new Configuration());
    Optional<Long> dsv2Result = dsv2Stream.getStartingVersion();

    compareStartingVersionResults(dsv1Result, dsv2Result, Optional.empty(), "No options provided");
  }

  /** Test that verifies both DSv1 and DSv2 handle negative startingVersion values identically. */
  @Test
  public void testGetStartingVersion_NegativeVersion_throwsError(@TempDir File tempDir)
      throws Exception {
    // Negative values are rejected during DeltaOptions parsing, before getStartingVersion is
    // called.
    assertThrows(IllegalArgumentException.class, () -> createDeltaOptions("-1"));
  }

  /**
   * Parameterized test that verifies both DSv1 and DSv2 handle the protocol validation behavior
   * identically with the validation flag on/off.
   *
   * <p>When protocol validation is enabled, validateProtocolAt is called and must succeed. When
   * disabled, the code immediately falls back to checkVersionExists without protocol validation.
   */
  @ParameterizedTest
  @MethodSource("protocolValidationParameters")
  public void testGetStartingVersion_ProtocolValidationFlag(
      boolean enableProtocolValidation,
      String startingVersion,
      String testDescription,
      @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName =
        "test_protocol_fallback_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create 5 versions (version 0 = CREATE TABLE, versions 1-5 = INSERTs)
    createVersions(testTableName, 5);

    // Test with protocol validation enabled/disabled
    String configKey = DeltaSQLConf.FAST_DROP_FEATURE_STREAMING_ALWAYS_VALIDATE_PROTOCOL().key();
    try {
      spark.conf().set(configKey, String.valueOf(enableProtocolValidation));
      testAndCompareStartingVersion(
          testTablePath,
          startingVersion,
          Optional.of(Long.parseLong(startingVersion)),
          testDescription);
    } finally {
      spark.conf().unset(configKey);
    }
  }

  /** Provides test parameters for protocol validation scenarios. */
  private static Stream<Arguments> protocolValidationParameters() {
    return Stream.of(
        Arguments.of(
            /* enableProtocolValidation= */ true,
            /* startingVersion= */ "2",
            "Protocol validation enabled"),
        Arguments.of(
            /* enableProtocolValidation= */ false,
            /* startingVersion= */ "3",
            "Protocol validation disabled"));
  }

  // TODO(#5320): Add test for unsupported table feature
  // Test case where protocol validation encounters an unsupported table feature and throws
  // (does NOT fall back to checkVersionExists). This is difficult to test reliably as it
  // requires creating a table with features that Kernel doesn't support, which Spark SQL
  // validates upfront. This scenario is tested through integration tests.

  /**
   * Test case where protocol validation fails with a non-feature exception (snapshot cannot be
   * recreated), but checkVersionExists succeeds (commit logically exists).
   *
   * <p>Scenario: After creating a checkpoint at version 10, old log files 0-5 are deleted
   * (simulating log cleanup by timestamp). This makes version 7 non-recreatable (it exists between
   * the deleted logs and the checkpoint). Protocol validation fails when trying to build snapshot
   * at version 7, but checkVersionExists succeeds because the commit still logically exists.
   */
  @Test
  public void testGetStartingVersion_ProtocolValidationNonFeatureExceptionFallback(
      @TempDir File tempDir) throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_non_recreatable_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create 10 versions (version 0 = CREATE TABLE, versions 1-10 = INSERTs)
    createVersions(testTableName, /* numVersions= */ 10);

    // Create checkpoint at version 10
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    Snapshot snapshotV10 =
        deltaLog.getSnapshotAt(
            10, Option.<CheckpointInstance>empty(), Option.<CatalogTable>empty(), false);
    deltaLog.checkpoint(snapshotV10, Option.<CatalogTable>empty());

    // Simulate log cleanup by timestamp: delete logs 0-5
    // This makes version 7 non-recreatable while allowing DeltaLog to load the latest snapshot
    Path logPath = new Path(testTablePath, "_delta_log");
    for (long version = 0; version <= 5; version++) {
      Path logFile = new Path(logPath, String.format("%020d.json", version));
      File file = new File(logFile.toUri().getPath());
      if (file.exists()) {
        file.delete();
      }
    }

    // Test with startingVersion=7 (a version that's no longer recreatable but logically exists)
    String startingVersion = "7";

    // dsv1
    DeltaLog freshDeltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    DeltaSource deltaSource =
        createDeltaSource(freshDeltaLog, testTablePath, createDeltaOptions(startingVersion));
    scala.Option<Object> dsv1Result = deltaSource.getStartingVersion();

    // dsv2
    SparkMicroBatchStream dsv2Stream =
        new SparkMicroBatchStream(
            testTablePath, new Configuration(), spark, createDeltaOptions(startingVersion));
    Optional<Long> dsv2Result = dsv2Stream.getStartingVersion();

    compareStartingVersionResults(
        dsv1Result,
        dsv2Result,
        Optional.of(Long.parseLong(startingVersion)),
        "Protocol validation fallback with non-recreatable version");
  }

  // ================================================================================================
  // Helper methods
  // ================================================================================================

  /** Helper method to create multiple versions by inserting rows. */
  private void createVersions(String testTableName, int numVersions) {
    for (int i = 1; i <= numVersions; i++) {
      sql("INSERT INTO %s VALUES (%d, 'User%d')", testTableName, i, i);
    }
  }

  /** Helper method to test and compare getStartingVersion results from DSv1 and DSv2. */
  private void testAndCompareStartingVersion(
      String testTablePath,
      String startingVersion,
      Optional<Long> expectedVersion,
      String testDescription)
      throws Exception {
    // DSv1: Create DeltaSource and get starting version
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    DeltaSource deltaSource =
        createDeltaSource(deltaLog, testTablePath, createDeltaOptions(startingVersion));
    scala.Option<Object> dsv1Result = deltaSource.getStartingVersion();

    // DSv2: Create SparkMicroBatchStream and get starting version
    SparkMicroBatchStream dsv2Stream =
        new SparkMicroBatchStream(
            testTablePath, new Configuration(), spark, createDeltaOptions(startingVersion));
    Optional<Long> dsv2Result = dsv2Stream.getStartingVersion();

    compareStartingVersionResults(dsv1Result, dsv2Result, expectedVersion, testDescription);
  }

  /** Helper method to execute SQL with String.format. */
  private static void sql(String query, Object... args) {
    SparkDsv2TestBase.spark.sql(String.format(query, args));
  }

  /** Helper method to create a DeltaSource instance with custom options for testing. */
  private DeltaSource createDeltaSource(DeltaLog deltaLog, String tablePath, DeltaOptions options) {
    scala.collection.immutable.Seq<org.apache.spark.sql.catalyst.expressions.Expression> emptySeq =
        scala.collection.JavaConverters.asScalaBuffer(
                new java.util.ArrayList<org.apache.spark.sql.catalyst.expressions.Expression>())
            .toList();
    Snapshot snapshot = deltaLog.update(false, Option.empty(), Option.empty());
    return new DeltaSource(
        spark,
        deltaLog,
        /* catalogTableOpt= */ Option.empty(),
        options,
        /* snapshotAtSourceInit= */ snapshot,
        /* metadataPath= */ tablePath + "/_checkpoint",
        /* metadataTrackingLog= */ Option.empty(),
        /* filters= */ emptySeq);
  }

  /** Helper method to create DeltaOptions with startingVersion for testing. */
  private DeltaOptions createDeltaOptions(String startingVersionValue) {
    if (startingVersionValue == null) {
      // Empty options
      return new DeltaOptions(Map$.MODULE$.empty(), spark.sessionState().conf());
    } else {
      // Create Scala Map with startingVersion
      scala.collection.immutable.Map<String, String> scalaMap =
          Map$.MODULE$.<String, String>empty().updated("startingVersion", startingVersionValue);
      return new DeltaOptions(scalaMap, spark.sessionState().conf());
    }
  }

  /** Helper method to compare getStartingVersion results from DSv1 and DSv2. */
  private void compareStartingVersionResults(
      scala.Option<Object> dsv1Result,
      Optional<Long> dsv2Result,
      Optional<Long> expectedVersion,
      String testDescription) {

    Optional<Long> dsv1Optional;
    if (dsv1Result.isEmpty()) {
      dsv1Optional = Optional.empty();
    } else {
      dsv1Optional = Optional.of((Long) dsv1Result.get());
    }

    assertEquals(
        dsv1Optional,
        dsv2Result,
        String.format("DSv1 and DSv2 getStartingVersion should match for %s", testDescription));

    assertEquals(
        expectedVersion,
        dsv2Result,
        String.format("DSv2 getStartingVersion should match for %s", testDescription));
  }
}
