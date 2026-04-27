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
package io.delta.spark.internal.v2.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.catalog.SparkTable;
import java.io.File;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SparkBatchTest extends DeltaV2TestBase {

  private static String tablePath;
  private static final String tableName = "deltatbl_partitioned_batch";

  @BeforeAll
  public static void setupPartitionedTable(@TempDir File tempDir) {
    createPartitionedTable(tableName, tempDir.getAbsolutePath());
    tablePath = tempDir.getAbsolutePath();
  }

  private final CaseInsensitiveStringMap options =
      new CaseInsensitiveStringMap(new java.util.HashMap<>());

  private final SparkTable table =
      new SparkTable(
          Identifier.of(new String[] {"spark_catalog", "default"}, tableName), tablePath, options);

  @Test
  public void testEqualsAndHashCode() {
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    Batch batch1 = ((SparkScan) builder1.build()).toBatch();

    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    Batch batch2 = ((SparkScan) builder2.build()).toBatch();

    assertEquals(batch1, batch2);
    assertEquals(batch1.hashCode(), batch2.hashCode());
  }

  @Test
  public void testEqualsWithSamePushedFilters() {
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    builder1.pushFilters(new Filter[] {new EqualTo("city", "hz")});
    Batch batch1 = ((SparkScan) builder1.build()).toBatch();

    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    builder2.pushFilters(new Filter[] {new EqualTo("city", "hz")});
    Batch batch2 = ((SparkScan) builder2.build()).toBatch();

    assertEquals(batch1, batch2);
    assertEquals(batch1.hashCode(), batch2.hashCode());
  }

  @Test
  public void testEqualsWithDifferentPushedFilters() {
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    Batch batch1 = ((SparkScan) builder1.build()).toBatch();

    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    builder2.pushFilters(new Filter[] {new EqualTo("city", "hz")});
    Batch batch2 = ((SparkScan) builder2.build()).toBatch();

    assertNotEquals(batch1, batch2);
    assertNotEquals(batch1.hashCode(), batch2.hashCode());
  }

  @Test
  public void testEqualsWithPushedFiltersInDifferentOrder() {
    Filter cityEq = new EqualTo("city", "hz");
    Filter dateEq = new EqualTo("date", "20180520");

    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    builder1.pushFilters(new Filter[] {cityEq, dateEq});
    Batch batch1 = ((SparkScan) builder1.build()).toBatch();

    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    builder2.pushFilters(new Filter[] {dateEq, cityEq});
    Batch batch2 = ((SparkScan) builder2.build()).toBatch();

    assertEquals(batch1, batch2);
    assertEquals(batch1.hashCode(), batch2.hashCode());
  }

  @Test
  public void testEqualsWithDataFiltersInDifferentOrder() {
    // city/date are partition columns, so the test above only exercises pushedToKernelFiltersSet.
    // name and cnt are data columns (per the partitioned table schema), so per
    // ExpressionUtils.classifyFilter these filters have isDataFilter=true and flow into
    // SparkScan.dataFilters / SparkBatch.dataFilters, exercising the dataFiltersSet branch of
    // equals/hashCode.
    Filter nameEq = new EqualTo("name", "x");
    Filter cntGt = new GreaterThan("cnt", 10);

    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    builder1.pushFilters(new Filter[] {nameEq, cntGt});
    Batch batch1 = ((SparkScan) builder1.build()).toBatch();

    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    builder2.pushFilters(new Filter[] {cntGt, nameEq});
    Batch batch2 = ((SparkScan) builder2.build()).toBatch();

    assertEquals(batch1, batch2);
    assertEquals(batch1.hashCode(), batch2.hashCode());
  }
}
