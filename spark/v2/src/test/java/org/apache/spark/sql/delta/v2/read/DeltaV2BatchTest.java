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
package org.apache.spark.sql.delta.v2.read;

import static org.apache.spark.sql.delta.v2.read.metadata.MetadataColumnTestUtils.renamedMetadataColumnStructField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.delta.v2.catalog.DeltaV2Table;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DeltaV2BatchTest extends DeltaV2PartitionedTableTestBase {

  @BeforeAll
  public static void setupPartitionedTable(@TempDir File tempDir) {
    setUpSharedPartitionedTable("deltatbl_partitioned_batch", tempDir);
  }

  // Cases where two batches built from the same table must be equal. city/date are partition
  // columns; name/cnt are data columns. Both kinds flow into DeltaV2Batch.filterSet for
  // equals/hashCode, but only data-column filters are passed to the Parquet reader.
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
      pushFilters(builder1, filters1);
    }
    Batch batch1 = asDeltaV2Scan(builder1.build()).toBatch();

    DeltaV2ScanBuilder builder2 = (DeltaV2ScanBuilder) table.newScanBuilder(options);
    if (filters2.length > 0) {
      pushFilters(builder2, filters2);
    }
    Batch batch2 = asDeltaV2Scan(builder2.build()).toBatch();

    assertEquals(batch1, batch2);
    assertEquals(batch1.hashCode(), batch2.hashCode());
  }

  @Test
  public void testEqualsWithDifferentPushedFilters() {
    DeltaV2ScanBuilder builder1 = (DeltaV2ScanBuilder) table.newScanBuilder(options);
    Batch batch1 = asDeltaV2Scan(builder1.build()).toBatch();

    DeltaV2ScanBuilder builder2 = (DeltaV2ScanBuilder) table.newScanBuilder(options);
    pushFilters(builder2, new EqualTo("city", "hz"));
    Batch batch2 = asDeltaV2Scan(builder2.build()).toBatch();

    assertNotEquals(batch1, batch2);
    assertNotEquals(batch1.hashCode(), batch2.hashCode());
  }

  @Test
  public void testBatchReaderPreservesUserColumnWithRenamedMetadata(@TempDir File tempDir)
      throws Exception {
    String path = tempDir.getAbsolutePath();
    String collisionTableName = "batch_metadata_collision";
    withTable(
        new String[] {collisionTableName},
        () ->
            withSQLConf(
                "spark.databricks.delta.v2.enableMode",
                "NONE",
                () -> {
                  spark.sql(
                      String.format(
                          "CREATE TABLE %s (id LONG, _metadata STRING) USING delta LOCATION '%s'",
                          collisionTableName, path));
                  spark.sql(
                      String.format(
                          "INSERT INTO %s VALUES (1, 'user_a'), (2, 'user_b')",
                          collisionTableName));
                  List<Row> expected =
                      spark
                          .sql(
                              String.format(
                                  "SELECT id, _metadata, __metadata.file_path FROM %s ORDER BY id",
                                  collisionTableName))
                          .collectAsList();
                  assertEquals(2, expected.size());

                  DeltaV2Table collisionTable =
                      new DeltaV2Table(
                          Identifier.of(
                              new String[] {"spark_catalog", "default"}, collisionTableName),
                          path,
                          options);
                  DeltaV2ScanBuilder builder =
                      (DeltaV2ScanBuilder) collisionTable.newScanBuilder(options);
                  builder.pruneColumns(
                      new StructType()
                          .add("id", DataTypes.LongType)
                          .add("_metadata", DataTypes.StringType)
                          .add(
                              renamedMetadataColumnStructField(
                                  "__metadata",
                                  new StructType().add("file_path", DataTypes.StringType))));

                  // Call the batch API directly to exercise its reader independently of FileScan.
                  Batch batch = asDeltaV2Scan(builder.build()).toBatch();
                  PartitionReaderFactory readerFactory = batch.createReaderFactory();
                  List<Row> actual = new ArrayList<>();
                  for (InputPartition partition : batch.planInputPartitions()) {
                    assertFalse(readerFactory.supportColumnarReads(partition));
                    try (PartitionReader<InternalRow> reader =
                        readerFactory.createReader(partition)) {
                      while (reader.next()) {
                        InternalRow row = reader.get();
                        assertFalse(row.isNullAt(2), "Renamed metadata must be populated");
                        InternalRow metadata = row.getStruct(2, 1);
                        assertFalse(metadata.isNullAt(0), "Generated file_path must be populated");
                        actual.add(
                            RowFactory.create(
                                row.getLong(0),
                                row.getUTF8String(1).toString(),
                                metadata.getUTF8String(0).toString()));
                      }
                    }
                  }
                  actual.sort(Comparator.comparingLong(row -> row.getLong(0)));
                  assertEquals(expected, actual);
                }));
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
