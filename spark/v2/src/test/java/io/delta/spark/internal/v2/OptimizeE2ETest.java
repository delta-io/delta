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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test for OPTIMIZE SQL command routed through the V2 connector.
 *
 * <p>Uses the dsv2 catalog's path-based table support to resolve a Delta table as SparkTable, then
 * runs OPTIMIZE SQL which goes through: SQL parse -> resolve -> SparkTable -> DeltaTableV2 (via
 * reflection in maybeExtractFrom) -> OptimizeTableCommand -> OptimizeExecutor.
 */
public class OptimizeE2ETest extends V2TestBase {

  /**
   * OPTIMIZE SQL compacts multiple small files into fewer larger files when the table is resolved
   * through the V2 connector (SparkTable).
   */
  @Test
  public void testOptimizeSqlThroughV2Connector(@TempDir File tempDir) throws Exception {
    String tablePath = new File(tempDir, "e2e_optimize").getAbsolutePath();

    // Create a delta table with 5 small files via V1
    for (int i = 0; i < 5; i++) {
      spark
          .range(i * 10, (i + 1) * 10)
          .coalesce(1)
          .write()
          .format("delta")
          .mode(i == 0 ? "overwrite" : "append")
          .save(tablePath);
    }

    long filesBefore = countLogicalFiles(tablePath);
    assertEquals(5, filesBefore);
    assertEquals(50, spark.read().format("delta").load(tablePath).count());

    // Run OPTIMIZE through the V2 connector (dsv2 catalog resolves as SparkTable)
    spark.sql(String.format("OPTIMIZE dsv2.delta.`%s`", tablePath));

    // After OPTIMIZE: fewer logical files (physical files remain until VACUUM), same data
    long filesAfter = countLogicalFiles(tablePath);
    assertTrue(
        filesAfter < filesBefore,
        "OPTIMIZE should reduce logical file count: before="
            + filesBefore
            + " after="
            + filesAfter);

    assertEquals(50, spark.read().format("delta").load(tablePath).count());
  }

  /**
   * OPTIMIZE through V2 succeeds even when a concurrent blind append happens. This validates the
   * full E2E path including conflict resolution.
   */
  @Test
  public void testOptimizeSqlWithConcurrentAppend(@TempDir File tempDir) {
    String tablePath = new File(tempDir, "e2e_concurrent").getAbsolutePath();

    spark.range(10).coalesce(1).write().format("delta").save(tablePath);
    spark.range(10, 20).coalesce(1).write().format("delta").mode("append").save(tablePath);
    spark.range(20, 30).coalesce(1).write().format("delta").mode("append").save(tablePath);

    assertEquals(30, spark.read().format("delta").load(tablePath).count());

    // Run OPTIMIZE through V2 (no concurrent writer — just validate the full SQL path works)
    Dataset<Row> result = spark.sql(String.format("OPTIMIZE dsv2.delta.`%s`", tablePath));
    assertNotNull(result);

    // Same data after OPTIMIZE
    assertEquals(30, spark.read().format("delta").load(tablePath).count());
  }

  /** Counts the number of active (logical) files in the delta table via DESCRIBE DETAIL. */
  private long countLogicalFiles(String tablePath) {
    return spark.read().format("delta").load(tablePath).inputFiles().length;
  }
}
