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
package io.delta.spark.internal.v2.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.catalog.SparkTable;
import java.io.File;
import java.util.stream.Stream;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

  // Cases where two batches built from the same table must be equal. city/date are partition
  // columns (exercise pushedToKernelFiltersSet); name/cnt are data columns and per
  // ExpressionUtils.classifyFilter have isDataFilter=true, so they flow into
  // SparkBatch.dataFilters and exercise the dataFiltersSet branch of equals/hashCode.
  static Stream<Arguments> equalBatchCasesProvider() {
    Filter cityEq = new EqualTo("city", "hz");
    Filter dateEq = new EqualTo("date", "20180520");
    Filter nameEq = new EqualTo("name", "x");
    Filter cntGt = new GreaterThan("cnt", 10);
    return Stream.of(
        Arguments.of("no filters", new Filter[0], new Filter[0]),
        Arguments.of("same single filter", new Filter[] {cityEq}, new Filter[] {cityEq}),
        Arguments.of(
            "partition filters in different order",
            new Filter[] {cityEq, dateEq},
            new Filter[] {dateEq, cityEq}),
        Arguments.of(
            "data filters in different order",
            new Filter[] {nameEq, cntGt},
            new Filter[] {cntGt, nameEq}));
  }

  @ParameterizedTest(name = "{0}: batches should be equal")
  @MethodSource("equalBatchCasesProvider")
  public void testEqualsAndHashCode(String description, Filter[] filters1, Filter[] filters2) {
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    if (filters1.length > 0) {
      builder1.pushFilters(filters1);
    }
    Batch batch1 = ((SparkScan) builder1.build()).toBatch();

    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    if (filters2.length > 0) {
      builder2.pushFilters(filters2);
    }
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
}
