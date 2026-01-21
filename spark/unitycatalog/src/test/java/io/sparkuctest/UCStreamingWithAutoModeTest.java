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

package io.sparkuctest;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2;
import org.apache.spark.sql.delta.RelocatedUtils;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.collection.Iterator;

/**
 * Integration tests for V2 streaming conversion with Unity Catalog and non-UC tables.
 *
 * <p>Tests the ApplyV2Streaming rule and DeltaConnectorMode behavior across different modes (AUTO,
 * STRICT, NONE) with both UC-managed and regular Delta tables.
 */
public class UCStreamingWithAutoModeTest extends UCDeltaTableIntegrationBaseTest {

  private static final String V2_ENABLE_MODE_KEY = "spark.databricks.delta.v2.enableMode";

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  /**
   * Checks if a DataFrame's logical plan uses StreamingRelationV2 (V2 streaming path).
   *
   * @param df the DataFrame to check
   * @return true if V2 streaming is used
   */
  private boolean usesV2Streaming(Dataset<Row> df) {
    LogicalPlan plan = df.queryExecution().analyzed();
    Iterator<LogicalPlan> iter =
        plan.collect(
                new scala.PartialFunction<LogicalPlan, LogicalPlan>() {
                  @Override
                  public LogicalPlan apply(LogicalPlan p) {
                    if (p instanceof StreamingRelationV2) {
                      return p;
                    }
                    throw new UnsupportedOperationException();
                  }

                  @Override
                  public boolean isDefinedAt(LogicalPlan p) {
                    return p instanceof StreamingRelationV2;
                  }
                })
            .iterator();
    return iter.hasNext();
  }

  /**
   * Checks if a DataFrame's logical plan uses StreamingRelation (V1 streaming path).
   *
   * @param df the DataFrame to check
   * @return true if V1 streaming is used
   */
  private boolean usesV1Streaming(Dataset<Row> df) {
    LogicalPlan plan = df.queryExecution().analyzed();
    Iterator<LogicalPlan> iter =
        plan.collect(
                new scala.PartialFunction<LogicalPlan, LogicalPlan>() {
                  @Override
                  public LogicalPlan apply(LogicalPlan p) {
                    if (RelocatedUtils.isStreamingRelation(p)) {
                      return p;
                    }
                    throw new UnsupportedOperationException();
                  }

                  @Override
                  public boolean isDefinedAt(LogicalPlan p) {
                    return RelocatedUtils.isStreamingRelation(p);
                  }
                })
            .iterator();
    return iter.hasNext();
  }

  /** Creates a temporary directory and returns its path. */
  private String createTempDir() {
    try {
      return Files.createTempDirectory("delta-test-").toFile().getAbsolutePath();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Creates a temporary directory for checkpoint location. */
  private String createCheckpointDir() {
    try {
      return Files.createTempDirectory("checkpoint-").toFile().getAbsolutePath();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void withV2Mode(String mode, ThrowingRunnable runnable) throws Exception {
    String prevConf = spark().conf().get(V2_ENABLE_MODE_KEY, "NONE");
    try {
      spark().conf().set(V2_ENABLE_MODE_KEY, mode);
      runnable.run();
    } finally {
      spark().conf().set(V2_ENABLE_MODE_KEY, prevConf);
    }
  }

  private StreamingQuery startDeltaStream(Dataset<Row> df, String checkpointDir, String outputDir)
      throws Exception {
    return df.writeStream()
        .format("delta")
        .option("checkpointLocation", checkpointDir)
        .start(outputDir);
  }

  private StreamingQuery startMemoryStream(Dataset<Row> df, String checkpointDir, String name)
      throws Exception {
    return df.writeStream()
        .format("memory")
        .queryName(name)
        .option("checkpointLocation", checkpointDir)
        .start();
  }

  private void assertV2StreamingFailure(StreamingQuery query) {
    Exception exception =
        assertThrows(
            Exception.class,
            () -> query.processAllAvailable(),
            "V2 streaming should fail with known limitation");

    assertTrue(
        exception.getMessage().contains("initialOffset")
            || (exception.getCause() != null
                && exception.getCause().getMessage().contains("initialOffset")),
        "Should fail with initialOffset limitation, but got: " + exception.getMessage());
  }

  private void cleanupDirs(String... dirs) {
    for (String dir : dirs) {
      deleteDirectory(new File(dir));
    }
  }

  // ==========================================================================
  // AUTO Mode Tests
  // ==========================================================================

  @Test
  public void testAutoModeUCManagedTableUsesV2Streaming() throws Exception {
    withNewTable(
        "auto_mode_uc_table",
        "id BIGINT, value STRING",
        TableType.MANAGED,
        (tableName) -> {
          sql("INSERT INTO %s VALUES (1, 'a')", tableName);

          withV2Mode(
              "AUTO",
              () -> {
                String checkpointDir = createCheckpointDir();
                String outputDir = createTempDir();
                try {
                  // Create streaming read
                  Dataset<Row> df = spark().readStream().table(tableName);

                  // Verify V2 streaming is used for UC-managed table in AUTO mode
                  assertTrue(
                      usesV2Streaming(df),
                      "AUTO mode should use V2 streaming for UC-managed tables");

                  // V2 streaming execution expected to fail with known limitation
                  StreamingQuery query = startDeltaStream(df, checkpointDir, outputDir);
                  assertV2StreamingFailure(query);
                  query.stop();
                } finally {
                  cleanupDirs(checkpointDir, outputDir);
                }
              });
        });
  }

  @Test
  public void testAutoModeNonUCCatalogTableUsesV1Streaming() throws Exception {
    withV2Mode(
        "AUTO",
        () -> {
          String tempDir = createTempDir();
          String tableName = "default.regular_table";
          String checkpointDir = createCheckpointDir();
          String outputDir = createTempDir();
          try {
            // Create regular catalog table (no UC properties)
            sql(
                "CREATE TABLE %s (id INT, value STRING) USING delta LOCATION '%s'",
                tableName, tempDir);
            sql("INSERT INTO %s VALUES (0, 'init')", tableName);

            // Create streaming read
            Dataset<Row> df = spark().readStream().table(tableName);

            // Verify V1 streaming is used (no UC table ID)
            assertTrue(
                usesV1Streaming(df), "AUTO mode should use V1 streaming for non-UC catalog tables");

            // Verify V1 streaming execution works
            StreamingQuery query = startDeltaStream(df, checkpointDir, outputDir);
            query.processAllAvailable();

            // Verify initial data
            Dataset<Row> result = spark().read().format("delta").load(outputDir);
            assertEquals(1, result.count(), "Should have 1 row initially");
            assertEquals(
                1,
                result.filter("id = 0 AND value = 'init'").count(),
                "Should contain initial row");

            // Add more data
            sql("INSERT INTO %s VALUES (1, 'a')", tableName);
            query.processAllAvailable();

            // Verify new data is read
            result = spark().read().format("delta").load(outputDir);
            assertEquals(2, result.count(), "Should have 2 rows after insert");
            assertEquals(
                1, result.filter("id = 1 AND value = 'a'").count(), "Should contain new row");

            query.stop();

          } finally {
            sql("DROP TABLE IF EXISTS %s", tableName);
            cleanupDirs(tempDir, checkpointDir, outputDir);
          }
        });
  }

  @ParameterizedTest
  @ValueSource(strings = {"AUTO", "STRICT", "NONE"})
  public void testPathBasedTableUsesV1Streaming(String mode) throws Exception {
    withV2Mode(
        mode,
        () -> {
          String tempDir = createTempDir();
          String checkpointDir = createCheckpointDir();
          String outputDir = createTempDir();
          try {
            // Create path-based table
            spark()
                .range(3)
                .selectExpr("id", "CAST(id AS STRING) as value")
                .write()
                .format("delta")
                .save(tempDir);

            // Create streaming read from path
            Dataset<Row> df = spark().readStream().format("delta").load(tempDir);

            // Verify V1 streaming is used (no catalog table)
            assertTrue(
                usesV1Streaming(df),
                String.format("%s mode should use V1 streaming for path-based tables", mode));

            // Verify V1 streaming execution works
            StreamingQuery query = startDeltaStream(df, checkpointDir, outputDir);
            query.processAllAvailable();

            // Verify initial data
            Dataset<Row> result = spark().read().format("delta").load(outputDir);
            assertEquals(3, result.count(), "Should have 3 rows initially");

            // Add more data
            spark()
                .range(3, 5)
                .selectExpr("id", "CAST(id AS STRING) as value")
                .write()
                .format("delta")
                .mode("append")
                .save(tempDir);
            query.processAllAvailable();

            // Verify new data is read
            result = spark().read().format("delta").load(outputDir);
            assertEquals(5, result.count(), "Should have 5 rows after append");

            query.stop();

          } finally {
            cleanupDirs(tempDir, checkpointDir, outputDir);
          }
        });
  }

  // ==========================================================================
  // STRICT Mode Tests
  // ==========================================================================

  @Test
  public void testStrictModeUCManagedTableUsesV2Streaming() throws Exception {
    withNewTable(
        "strict_mode_uc_table",
        "id BIGINT, value STRING",
        TableType.MANAGED,
        (tableName) -> {
          sql("INSERT INTO %s VALUES (1, 'a')", tableName);

          withV2Mode(
              "STRICT",
              () -> {
                Dataset<Row> df = spark().readStream().table(tableName);

                // Verify V2 streaming is used for UC-managed table in STRICT mode
                assertTrue(
                    usesV2Streaming(df),
                    "STRICT mode should use V2 streaming for UC-managed tables");

                // V2 streaming execution expected to fail with known limitation
                String checkpointDir = createCheckpointDir();
                String outputDir = createTempDir();
                try {
                  StreamingQuery query = startDeltaStream(df, checkpointDir, outputDir);
                  assertV2StreamingFailure(query);
                  query.stop();
                } finally {
                  cleanupDirs(checkpointDir, outputDir);
                }
              });
        });
  }

  @Test
  public void testStrictModeCatalogTableUsesV2StreamingViaDeltaCatalog() throws Exception {
    withV2Mode(
        "STRICT",
        () -> {
          String checkpointDir = createCheckpointDir();
          String tempDir = createTempDir();
          String tableName = "default.strict_test_table";
          try {
            // Create table using path-based approach with initial data
            spark().range(2).toDF("id").write().format("delta").save(tempDir);

            // Create a catalog table pointing to this location
            sql("CREATE TABLE %s USING delta LOCATION '%s'", tableName, tempDir);

            Dataset<Row> df = spark().readStream().table(tableName);

            // In STRICT mode, catalog returns SparkTable, so we use V2 streaming
            assertTrue(
                usesV2Streaming(df),
                "STRICT mode uses V2 streaming for catalog tables "
                    + "(via DeltaCatalog, not ApplyV2Streaming)");

            // V2 streaming execution expected to fail with known limitation
            // Use memory sink to avoid catalog resolution issues with Delta sink in STRICT mode
            StreamingQuery query = startMemoryStream(df, checkpointDir, "strict_mode_test");
            assertV2StreamingFailure(query);
            query.stop();

          } finally {
            sql("DROP TABLE IF EXISTS %s", tableName);
            cleanupDirs(tempDir, checkpointDir);
          }
        });
  }

  // STRICT path-based behavior is covered by testPathBasedTableUsesV1Streaming.

  // ==========================================================================
  // NONE Mode Tests (Default behavior)
  // ==========================================================================

  @Test
  public void testNoneModeAllTablesUseV1Streaming() throws Exception {
    withV2Mode(
        "NONE",
        () -> {
          String tempDir = createTempDir();
          String tableName = "default.test_table";
          String checkpointDir = createCheckpointDir();
          String outputDir = createTempDir();
          try {
            sql("CREATE TABLE %s (id INT) USING delta LOCATION '%s'", tableName, tempDir);
            sql("INSERT INTO %s VALUES (1)", tableName);

            Dataset<Row> df = spark().readStream().table(tableName);

            assertTrue(usesV1Streaming(df), "NONE mode should use V1 streaming for all tables");

            // Verify V1 streaming execution works
            StreamingQuery query = startDeltaStream(df, checkpointDir, outputDir);
            query.processAllAvailable();

            // Verify initial data
            Dataset<Row> result = spark().read().format("delta").load(outputDir);
            assertEquals(1, result.count(), "Should have 1 row initially");

            // Add more data
            sql("INSERT INTO %s VALUES (2)", tableName);
            query.processAllAvailable();

            // Verify new data is read
            result = spark().read().format("delta").load(outputDir);
            assertEquals(2, result.count(), "Should have 2 rows after insert");

            query.stop();

          } finally {
            sql("DROP TABLE IF EXISTS %s", tableName);
            cleanupDirs(tempDir, checkpointDir, outputDir);
          }
        });
  }

  @Test
  public void testNoneModeUCManagedTableFailsOnDeltaLogAccessOnStreaming() throws Exception {
    withNewTable(
        "none_mode_uc_table",
        "id BIGINT, value STRING",
        TableType.MANAGED,
        (tableName) -> {
          sql("INSERT INTO %s VALUES (1, 'a')", tableName);

          withV2Mode(
              "NONE",
              () -> {
                // NONE mode uses V1 streaming, which requires path-based access to UC-managed
                // tables. This is blocked by UC, so we expect an exception.
                Exception exception =
                    assertThrows(
                        Exception.class,
                        () -> spark().readStream().table(tableName),
                        "V1 streaming with UC tables expected to fail with path-based access error");

                assertTrue(
                    exception
                            .getMessage()
                            .contains("DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED")
                        || (exception.getCause() != null
                            && exception
                                .getCause()
                                .getMessage()
                                .contains(
                                    "DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED")),
                    "Should fail with catalog-managed table path access error, but got: "
                        + exception.getMessage());

                // Note: NONE mode does not support UC-managed tables for streaming because it
                // requires path-based access which is blocked for UC tables.
              });
        });
  }

  /** Helper method to delete a directory recursively. */
  private void deleteDirectory(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            file.delete();
          }
        }
      }
      directory.delete();
    }
  }
}
