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
package io.delta.spark.internal.v2.read.deletionvector;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.golden.GoldenTableUtils$;
import io.delta.spark.internal.v2.catalog.SparkTable;
import io.delta.spark.internal.v2.read.SparkScan;
import io.delta.spark.internal.v2.read.SparkScanBuilder;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests to verify that DV tables use vectorized reader and data is correctly filtered.
 *
 * <p>Validates:
 *
 * <ol>
 *   <li>{@code supportColumnarReads()} returns true for DV tables
 *   <li>Reading data via {@code createColumnarReader()} correctly filters deleted rows
 * </ol>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DeletionVectorVectorizedReaderTest {

  private SparkSession spark;

  @BeforeAll
  public void setUp(@TempDir File tempDir) {
    SparkConf conf =
        new SparkConf()
            .set("spark.sql.catalog.dsv2", "io.delta.spark.internal.v2.catalog.TestCatalog")
            .set("spark.sql.catalog.dsv2.base_path", tempDir.getAbsolutePath())
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtensionV1")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalogV1")
            .set("spark.sql.parquet.enableVectorizedReader", "true")
            .setMaster("local[*]")
            .setAppName("DeletionVectorVectorizedReaderTest");
    spark = SparkSession.builder().config(conf).getOrCreate();
  }

  @AfterAll
  public void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  /** Test that DV table uses vectorized reader and correctly filters deleted rows. */
  @Test
  void testVectorizedReaderFiltersDeletedRows() throws Exception {
    String tableName = "dv-partitioned-with-checkpoint";
    String tablePath = goldenTablePath(tableName);

    SparkTable table =
        new SparkTable(
            Identifier.of(new String[] {"spark_catalog", "default"}, tableName), tablePath);
    SparkScanBuilder scanBuilder =
        (SparkScanBuilder) table.newScanBuilder(new CaseInsensitiveStringMap(java.util.Map.of()));
    SparkScan scan = (SparkScan) scanBuilder.build();

    Batch batch = scan.toBatch();
    InputPartition[] partitions = batch.planInputPartitions();
    PartitionReaderFactory readerFactory = batch.createReaderFactory();

    // DV table should support vectorized reads
    assertTrue(partitions.length > 0, "Should have at least one partition");
    assertTrue(
        readerFactory.supportColumnarReads(partitions[0]),
        "DV table should support vectorized (columnar) reads");

    // Read data and compare with DSv1
    List<Row> dsv2Rows = readWithColumnarReader(readerFactory, partitions, scan.readSchema());
    long dsv1Count = spark.read().format("delta").load(tablePath).count();

    assertFalse(dsv2Rows.isEmpty(), "Should read at least some rows");
    assertEquals(dsv1Count, dsv2Rows.size(), "Row counts should match between DSv1 and DSv2");
  }

  private List<Row> readWithColumnarReader(
      PartitionReaderFactory readerFactory, InputPartition[] partitions, StructType schema)
      throws Exception {
    List<Row> rows = new ArrayList<>();
    for (InputPartition partition : partitions) {
      try (PartitionReader<ColumnarBatch> reader = readerFactory.createColumnarReader(partition)) {
        while (reader.next()) {
          ColumnarBatch batch = reader.get();
          for (int rowId = 0; rowId < batch.numRows(); rowId++) {
            InternalRow internalRow = batch.getRow(rowId);
            rows.add(convertInternalRowToRow(internalRow, schema));
          }
        }
      }
    }
    return rows;
  }

  private Row convertInternalRowToRow(InternalRow internalRow, StructType schema) {
    scala.collection.Seq<Object> seq = internalRow.toSeq(schema);
    Object[] values = scala.collection.JavaConverters.seqAsJavaList(seq).toArray();
    return new org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema(values, schema);
  }

  private String goldenTablePath(String name) {
    return GoldenTableUtils$.MODULE$.goldenTablePath(name);
  }
}
