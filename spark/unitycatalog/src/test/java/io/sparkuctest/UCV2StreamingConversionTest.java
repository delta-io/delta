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
import org.apache.spark.sql.execution.streaming.StreamingRelation;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;
import scala.collection.Iterator;

/**
 * Integration tests for V2 streaming conversion with Unity Catalog and non-UC tables.
 *
 * <p>Tests the ApplyV2Streaming rule and DeltaConnectorMode behavior across different modes (AUTO,
 * STRICT, NONE) with both UC-managed and regular Delta tables.
 */
public class UCV2StreamingConversionTest extends UCDeltaTableIntegrationBaseTest {

  private static final String V2_ENABLE_MODE_KEY = "spark.databricks.delta.v2.enableMode";

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
                    if (p instanceof StreamingRelation) {
                      return p;
                    }
                    throw new UnsupportedOperationException();
                  }

                  @Override
                  public boolean isDefinedAt(LogicalPlan p) {
                    return p instanceof StreamingRelation;
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

          String prevConf = spark().conf().get(V2_ENABLE_MODE_KEY, "NONE");
          String checkpointDir = createCheckpointDir();
          String outputDir = createTempDir();
          try {
            spark().conf().set(V2_ENABLE_MODE_KEY, "AUTO");

            // Create streaming read
            Dataset<Row> df = spark().readStream().table(tableName);

            // Verify V2 streaming is used for UC-managed table in AUTO mode
            assertTrue(
                usesV2Streaming(df), "AUTO mode should use V2 streaming for UC-managed tables");

            // V2 streaming execution expected to fail with known limitation
            StreamingQuery query =
                df.writeStream()
                    .format("delta")
                    .option("checkpointLocation", checkpointDir)
                    .start(outputDir);

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

            query.stop();

          } finally {
            spark().conf().set(V2_ENABLE_MODE_KEY, prevConf);
          }
        });
  }

  @Test
  public void testAutoModeNonUCCatalogTableUsesV1Streaming() throws Exception {
    String prevConf = spark().conf().get(V2_ENABLE_MODE_KEY, "NONE");
    try {
      spark().conf().set(V2_ENABLE_MODE_KEY, "AUTO");

      String tempDir = createTempDir();
      String tableName = "default.regular_table";
      String checkpointDir = createCheckpointDir();
      String outputDir = createTempDir();
      try {
        // Create regular catalog table (no UC properties)
        sql("CREATE TABLE %s (id INT, value STRING) USING delta LOCATION '%s'", tableName, tempDir);
        sql("INSERT INTO %s VALUES (0, 'init')", tableName);

        // Create streaming read
        Dataset<Row> df = spark().readStream().table(tableName);

        // Verify V1 streaming is used (no UC table ID)
        assertTrue(
            usesV1Streaming(df), "AUTO mode should use V1 streaming for non-UC catalog tables");

        // Verify V1 streaming execution works
        StreamingQuery query =
            df.writeStream()
                .format("delta")
                .option("checkpointLocation", checkpointDir)
                .start(outputDir);

        query.processAllAvailable();

        // Verify initial data
        Dataset<Row> result = spark().read().format("delta").load(outputDir);
        assertEquals(1, result.count(), "Should have 1 row initially");
        assertEquals(
            1, result.filter("id = 0 AND value = 'init'").count(), "Should contain initial row");

        // Add more data
        sql("INSERT INTO %s VALUES (1, 'a')", tableName);
        query.processAllAvailable();

        // Verify new data is read
        result = spark().read().format("delta").load(outputDir);
        assertEquals(2, result.count(), "Should have 2 rows after insert");
        assertEquals(1, result.filter("id = 1 AND value = 'a'").count(), "Should contain new row");

        query.stop();

      } finally {
        sql("DROP TABLE IF EXISTS %s", tableName);
        deleteDirectory(new File(tempDir));
        deleteDirectory(new File(checkpointDir));
        deleteDirectory(new File(outputDir));
      }
    } finally {
      spark().conf().set(V2_ENABLE_MODE_KEY, prevConf);
    }
  }

  @Test
  public void testAutoModePathBasedTableUsesV1Streaming() throws Exception {
    String prevConf = spark().conf().get(V2_ENABLE_MODE_KEY, "NONE");
    try {
      spark().conf().set(V2_ENABLE_MODE_KEY, "AUTO");

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
        assertTrue(usesV1Streaming(df), "AUTO mode should use V1 streaming for path-based tables");

        // Verify V1 streaming execution works
        StreamingQuery query =
            df.writeStream()
                .format("delta")
                .option("checkpointLocation", checkpointDir)
                .start(outputDir);

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
        deleteDirectory(new File(tempDir));
        deleteDirectory(new File(checkpointDir));
        deleteDirectory(new File(outputDir));
      }
    } finally {
      spark().conf().set(V2_ENABLE_MODE_KEY, prevConf);
    }
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

          String prevConf = spark().conf().get(V2_ENABLE_MODE_KEY, "NONE");
          try {
            spark().conf().set(V2_ENABLE_MODE_KEY, "STRICT");

            Dataset<Row> df = spark().readStream().table(tableName);

            // Verify V2 streaming is used for UC-managed table in STRICT mode
            assertTrue(
                usesV2Streaming(df), "STRICT mode should use V2 streaming for UC-managed tables");

            // V2 streaming execution expected to fail with known limitation
            String checkpointDir = createCheckpointDir();
            String outputDir = createTempDir();
            try {
              StreamingQuery query =
                  df.writeStream()
                      .format("delta")
                      .option("checkpointLocation", checkpointDir)
                      .start(outputDir);

              Exception exception =
                  assertThrows(
                      Exception.class,
                      () -> query.processAllAvailable(),
                      "V2 streaming should fail with known limitation");

              assertTrue(
                  exception.getMessage().contains("initialOffset")
                      || exception.getCause().getMessage().contains("initialOffset"),
                  "Should fail with initialOffset limitation, but got: " + exception.getMessage());

              query.stop();
            } finally {
              deleteDirectory(new File(checkpointDir));
              deleteDirectory(new File(outputDir));
            }
          } finally {
            spark().conf().set(V2_ENABLE_MODE_KEY, prevConf);
          }
        });
  }

  @Test
  public void testStrictModeCatalogTableUsesV2StreamingViaDeltaCatalog() throws Exception {
    String prevConf = spark().conf().get(V2_ENABLE_MODE_KEY, "NONE");
    String checkpointDir = createCheckpointDir();
    try {
      spark().conf().set(V2_ENABLE_MODE_KEY, "STRICT");

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
        StreamingQuery query =
            df.writeStream()
                .format("memory")
                .queryName("strict_mode_test")
                .option("checkpointLocation", checkpointDir)
                .start();

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

        query.stop();

      } finally {
        sql("DROP TABLE IF EXISTS %s", tableName);
        deleteDirectory(new File(tempDir));
        deleteDirectory(new File(checkpointDir));
      }
    } finally {
      spark().conf().set(V2_ENABLE_MODE_KEY, prevConf);
    }
  }

  @Test
  public void testStrictModePathBasedTableUsesV1Streaming() throws Exception {
    String prevConf = spark().conf().get(V2_ENABLE_MODE_KEY, "NONE");
    try {
      spark().conf().set(V2_ENABLE_MODE_KEY, "STRICT");

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

        // STRICT mode currently uses V1 for path-based tables (no catalog table)
        assertTrue(
            usesV1Streaming(df),
            "STRICT mode should use V1 streaming for path-based tables (no catalog table)");

        // Verify V1 streaming execution works
        StreamingQuery query =
            df.writeStream()
                .format("delta")
                .option("checkpointLocation", checkpointDir)
                .start(outputDir);

        query.processAllAvailable();

        // Verify data is read correctly
        Dataset<Row> result = spark().read().format("delta").load(outputDir);
        assertEquals(3, result.count(), "Should have 3 rows");

        query.stop();

      } finally {
        deleteDirectory(new File(tempDir));
        deleteDirectory(new File(checkpointDir));
        deleteDirectory(new File(outputDir));
      }
    } finally {
      spark().conf().set(V2_ENABLE_MODE_KEY, prevConf);
    }
  }

  // ==========================================================================
  // NONE Mode Tests (Default behavior)
  // ==========================================================================

  @Test
  public void testNoneModeAllTablesUseV1Streaming() throws Exception {
    String prevConf = spark().conf().get(V2_ENABLE_MODE_KEY, "NONE");
    try {
      spark().conf().set(V2_ENABLE_MODE_KEY, "NONE");

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
        StreamingQuery query =
            df.writeStream()
                .format("delta")
                .option("checkpointLocation", checkpointDir)
                .start(outputDir);

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
        deleteDirectory(new File(tempDir));
        deleteDirectory(new File(checkpointDir));
        deleteDirectory(new File(outputDir));
      }
    } finally {
      spark().conf().set(V2_ENABLE_MODE_KEY, prevConf);
    }
  }

  @Test
  public void testNoneModeUCManagedTableFailsOnDeltaLogAccessOnStreaming() throws Exception {
    withNewTable(
        "none_mode_uc_table",
        "id BIGINT, value STRING",
        TableType.MANAGED,
        (tableName) -> {
          sql("INSERT INTO %s VALUES (1, 'a')", tableName);

          String prevConf = spark().conf().get(V2_ENABLE_MODE_KEY, "NONE");
          try {
            spark().conf().set(V2_ENABLE_MODE_KEY, "NONE");

            // NONE mode uses V1 streaming, which requires path-based access to UC-managed tables.
            // This is blocked by UC, so we expect an exception.
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
                            .contains("DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED")),
                "Should fail with catalog-managed table path access error, but got: "
                    + exception.getMessage());

            // Note: NONE mode does not support UC-managed tables for streaming because it
            // requires path-based access which is blocked for UC tables.

          } finally {
            spark().conf().set(V2_ENABLE_MODE_KEY, prevConf);
          }
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
