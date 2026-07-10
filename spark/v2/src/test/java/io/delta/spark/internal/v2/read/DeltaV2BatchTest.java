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
import io.delta.spark.internal.v2.catalog.DeltaV2Table;
import java.io.File;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
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

public class DeltaV2BatchTest extends DeltaV2TestBase {

  private static String tablePath;
  private static final String tableName = "deltatbl_partitioned_batch";

  @BeforeAll
  public static void setupPartitionedTable(@TempDir File tempDir) {
    createPartitionedTable(tableName, tempDir.getAbsolutePath());
    tablePath = tempDir.getAbsolutePath();
  }

  private final CaseInsensitiveStringMap options =
      new CaseInsensitiveStringMap(new java.util.HashMap<>());

  private final DeltaV2Table table =
      new DeltaV2Table(
          Identifier.of(new String[] {"spark_catalog", "default"}, tableName), tablePath, options);

  // Cases where two batches built from the same table must be equal. city/date are partition
  // columns (exercise pushedToKernelFiltersSet); name/cnt are data columns and per
  // ExpressionUtils.classifyFilter have isDataFilter=true, so they flow into
  // DeltaV2Batch.dataFilters and exercise the dataFiltersSet branch of equals/hashCode.
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
    DeltaV2ScanBuilder builder1 = (DeltaV2ScanBuilder) table.newScanBuilder(options);
    if (filters1.length > 0) {
      builder1.pushFilters(filters1);
    }
    Batch batch1 = ((DeltaV2Scan) builder1.build()).toBatch();

    DeltaV2ScanBuilder builder2 = (DeltaV2ScanBuilder) table.newScanBuilder(options);
    if (filters2.length > 0) {
      builder2.pushFilters(filters2);
    }
    Batch batch2 = ((DeltaV2Scan) builder2.build()).toBatch();

    assertEquals(batch1, batch2);
    assertEquals(batch1.hashCode(), batch2.hashCode());
  }

  @Test
  public void testEqualsWithDifferentPushedFilters() {
    DeltaV2ScanBuilder builder1 = (DeltaV2ScanBuilder) table.newScanBuilder(options);
    Batch batch1 = ((DeltaV2Scan) builder1.build()).toBatch();

    DeltaV2ScanBuilder builder2 = (DeltaV2ScanBuilder) table.newScanBuilder(options);
    builder2.pushFilters(new Filter[] {new EqualTo("city", "hz")});
    Batch batch2 = ((DeltaV2Scan) builder2.build()).toBatch();

    assertNotEquals(batch1, batch2);
    assertNotEquals(batch1.hashCode(), batch2.hashCode());
  }

  @Test
  public void testBatchReadPreservesPercentLiteralStringPartitionValue(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE delta.`%s` (id LONG, p STRING) USING delta PARTITIONED BY (p)", path));
    // Each value is chosen so an erroneous second unescape would produce a result that's
    // pairwise distinct from the input AND from the other cases' bug results, so a regression
    // cannot coincidentally agree with the expected value:
    //   "%20"   -> bug result " "   (canonical space-collapse)
    //   "%25"   -> bug result "%"   (self-encoding of `%`)
    //   "a%2Fb" -> bug result "a/b" (embedded percent escape mid-string)
    spark.sql(
        String.format(
            "INSERT INTO delta.`%s` VALUES (1, '%%20'), (2, '%%25'), (3, 'a%%2Fb')", path));

    List<Row> rows =
        spark
            .sql(String.format("SELECT id, p FROM dsv2.delta.`%s` ORDER BY id", path))
            .collectAsList();

    assertEquals(3, rows.size());
    assertEquals(1L, rows.get(0).getLong(0));
    assertEquals("%20", rows.get(0).getString(1));
    assertEquals(2L, rows.get(1).getLong(0));
    assertEquals("%25", rows.get(1).getString(1));
    assertEquals(3L, rows.get(2).getLong(0));
    assertEquals("a%2Fb", rows.get(2).getString(1));
  }
}
