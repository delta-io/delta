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

package io.sparkuctest;

import io.delta.kernel.spark.catalog.SparkTable;
import io.delta.kernel.spark.snapshot.DeltaSnapshotManager;
import io.delta.kernel.spark.snapshot.unitycatalog.UCManagedTableSnapshotManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E2E Structured Streaming test for UC-managed (catalogManaged/CCv2) tables using the Kernel DSv2
 * connector.
 *
 * <p>This test is intentionally structured to:
 *
 * <ul>
 *   <li>Force DSv2 table resolution via {@code spark.databricks.delta.v2.enableMode=STRICT}
 *   <li>Assert UC-managed routing selects {@link UCManagedTableSnapshotManager}
 *   <li>Run a real micro-batch query so offsets flow through {@code getTableChanges} (CCv2)
 * </ul>
 */
public class UCDeltaTableStreamingTest extends UCDeltaTableIntegrationBaseTest {

  @Test
  public void testUcManagedTableStreamingReadsNewCommitsThroughDsv2AndCcv2() throws Exception {
    SparkSession spark = getSparkSession();

    // Keep DSv1 enabled by default so CREATE TABLE / INSERT work (DSv2 SparkTable is read-only).
    spark.conf().set("spark.databricks.delta.v2.enableMode", "NONE");

    withNewTable(
        "streaming_uc_managed",
        "id INT, value STRING",
        TableType.MANAGED,
        tableName -> {
          // Force DSv2 for table resolution + streaming source planning.
          spark.conf().set("spark.databricks.delta.v2.enableMode", "STRICT");

          assertKernelDsv2AndUcManagedRouting(spark, tableName);

          withTempDir(
              tempDir -> {
                File checkpointDir = new File(tempDir, "checkpoint");
                String memorySinkName =
                    "uc_stream_" + UUID.randomUUID().toString().replace("-", "");

                StreamingQuery query = null;
                try {
                  Dataset<Row> input =
                      spark.readStream()
                          .format("delta")
                          // Required: DSv2 streaming does not support initial snapshot start.
                          .option("startingVersion", "latest")
                          .table(tableName);

                  query =
                      input.writeStream()
                          .format("memory")
                          .queryName(memorySinkName)
                          .option("checkpointLocation", checkpointDir.getAbsolutePath())
                          .start();

                  // Bootstrap batch - started at "latest", so should read nothing yet.
                  query.processAllAvailable();
                  assertEquals(0L, spark.sql("SELECT * FROM " + memorySinkName).count());

                  // Temporarily flip DSv2 off to perform a write via DSv1.
                  spark.conf().set("spark.databricks.delta.v2.enableMode", "NONE");
                  sql("INSERT INTO %s VALUES (1, 'a')", tableName);
                  spark.conf().set("spark.databricks.delta.v2.enableMode", "STRICT");

                  // Next micro-batch should pick up the new commit.
                  query.processAllAvailable();
                  check(memorySinkName, List.of(List.of("1", "a")));
                } finally {
                  stopQueryQuietly(query);
                }
              });
        });
  }

  private static void assertKernelDsv2AndUcManagedRouting(SparkSession spark, String fullTableName)
      throws Exception {
    String[] parts = fullTableName.split("\\.");
    assertEquals(3, parts.length, "Expected a fully-qualified table name: catalog.schema.table");

    String catalogName = parts[0];
    String schemaName = parts[1];
    String tableName = parts[2];

    TableCatalog catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    Table loaded = catalog.loadTable(Identifier.of(new String[] {schemaName}, tableName));

    assertTrue(
        loaded instanceof SparkTable,
        "Expected Kernel DSv2 io.delta.kernel.spark.catalog.SparkTable when STRICT is enabled");

    DeltaSnapshotManager snapshotManager = getSnapshotManager((SparkTable) loaded);
    assertNotNull(snapshotManager, "Expected SparkTable to have a snapshot manager");
    assertTrue(
        snapshotManager instanceof UCManagedTableSnapshotManager,
        "Expected UCManagedTableSnapshotManager for UC-managed (catalogManaged) tables");
  }

  private static DeltaSnapshotManager getSnapshotManager(SparkTable table) throws Exception {
    Field field = SparkTable.class.getDeclaredField("snapshotManager");
    field.setAccessible(true);
    return (DeltaSnapshotManager) field.get(table);
  }

  private static void stopQueryQuietly(StreamingQuery query) {
    if (query == null) {
      return;
    }
    try {
      query.stop();
    } catch (Exception ignored) {
      // ignore cleanup failures
    }
    try {
      query.awaitTermination(10_000);
    } catch (Exception ignored) {
      // ignore cleanup failures
    }
  }
}

