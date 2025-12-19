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
package io.delta.spark.internal.v2.utils;

import static io.delta.kernel.internal.util.VectorUtils.stringStringMapValue;
import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.SparkDsv2TestBase;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.Map$;

public class PartitionUtilsTest extends SparkDsv2TestBase {

  private static final long MB = 1024 * 1024;

  @Test
  public void testGetPartitionRow_FieldOrdering() {
    // Schema defines order: year, month, day
    StructType partitionSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("year", DataTypes.IntegerType, true),
              DataTypes.createStructField("month", DataTypes.IntegerType, true),
              DataTypes.createStructField("day", DataTypes.IntegerType, true)
            });

    // map value has different order: day, year, month
    Map<String, String> partitionValues = new HashMap<>();
    partitionValues.put("day", "25");
    partitionValues.put("year", "2024");
    partitionValues.put("month", "11");

    MapValue mapValue = stringStringMapValue(partitionValues);
    InternalRow row = PartitionUtils.getPartitionRow(mapValue, partitionSchema, ZoneId.of("UTC"));

    // verify order is schema order: year, month, day
    assertEquals(2024, row.getInt(0));
    assertEquals(11, row.getInt(1));
    assertEquals(25, row.getInt(2));
  }

  @Test
  public void testGetPartitionRow_SizeMismatchExtraKeys() {
    StructType partitionSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("year", DataTypes.IntegerType, true),
              DataTypes.createStructField("month", DataTypes.IntegerType, true)
            });

    Map<String, String> partitionValues = new HashMap<>();
    partitionValues.put("year", "2024");
    partitionValues.put("month", "11");
    partitionValues.put("day", "25");
    partitionValues.put("hour", "10");

    MapValue mapValue = stringStringMapValue(partitionValues);

    assertThrows(
        AssertionError.class,
        () -> PartitionUtils.getPartitionRow(mapValue, partitionSchema, ZoneId.of("UTC")));
  }

  @Test
  public void testGetPartitionRow_SizeMismatchMissingKeys() {
    StructType partitionSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("year", DataTypes.IntegerType, true),
              DataTypes.createStructField("month", DataTypes.IntegerType, true),
              DataTypes.createStructField("day", DataTypes.IntegerType, true)
            });

    Map<String, String> partitionValues = new HashMap<>();
    partitionValues.put("year", "2024");
    partitionValues.put("month", "11");

    MapValue mapValue = stringStringMapValue(partitionValues);

    assertThrows(
        AssertionError.class,
        () -> PartitionUtils.getPartitionRow(mapValue, partitionSchema, ZoneId.of("UTC")));
  }

  @Test
  public void testCreateDeltaParquetReaderFactory_Basic() {
    String tablePath = createTestTable("test_delta_reader_factory_" + System.nanoTime(), true);

    Table table = Table.forPath(defaultEngine, tablePath);
    Snapshot snapshot = table.getLatestSnapshot(defaultEngine);

    StructType dataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, true),
            });
    StructType partitionSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("part", DataTypes.StringType, true)});
    StructType readDataSchema = dataSchema;
    Filter[] filters = new Filter[0];
    scala.collection.immutable.Map<String, String> options = Map$.MODULE$.empty();
    Configuration hadoopConf = new Configuration();
    SQLConf sqlConf = SQLConf.get();

    PartitionReaderFactory factory =
        PartitionUtils.createDeltaParquetReaderFactory(
            snapshot,
            dataSchema,
            partitionSchema,
            readDataSchema,
            filters,
            options,
            hadoopConf,
            sqlConf);

    assertNotNull(factory, "PartitionReaderFactory should not be null");
  }

  @Test
  public void testCalculateMaxSplitBytes_Basic() {
    SQLConf sqlConf = SQLConf.get();
    long minPartitionNum = 4;
    sqlConf.setConfString("spark.sql.files.minPartitionNum", String.valueOf(minPartitionNum));

    long totalBytes = 100 * MB;
    int fileCount = 10;

    long result = PartitionUtils.calculateMaxSplitBytes(spark, totalBytes, fileCount, sqlConf);
    long openCostInBytes = sqlConf.filesOpenCostInBytes();
    long maxPartitionBytes = sqlConf.filesMaxPartitionBytes();

    long calculatedTotalBytes = totalBytes + (long) fileCount * openCostInBytes;
    assertEquals(calculatedTotalBytes / minPartitionNum, result);
  }

  @Test
  public void testCalculateMaxSplitBytes_BoundaryConditions() {
    SQLConf sqlConf = SQLConf.get();
    // Set minPartitionNum=1 for predictable calculations
    sqlConf.setConfString("spark.sql.files.minPartitionNum", "1");
    long openCostInBytes = sqlConf.filesOpenCostInBytes();
    long maxPartitionBytes = sqlConf.filesMaxPartitionBytes();

    // Zero files and bytes
    long result1 = PartitionUtils.calculateMaxSplitBytes(spark, 0L, 0, sqlConf);
    assertEquals(openCostInBytes, result1);

    // Single large file (exceeds maxPartitionBytes)
    long result2 = PartitionUtils.calculateMaxSplitBytes(spark, 1000 * MB, 1, sqlConf);
    assertEquals(maxPartitionBytes, result2);

    // Very small totalBytes
    long result3 = PartitionUtils.calculateMaxSplitBytes(spark, 1024L, 1, sqlConf);
    long expected3 = 1024L + openCostInBytes;
    assertEquals(expected3, result3);

    // Many small files
    long result4 = PartitionUtils.calculateMaxSplitBytes(spark, 1 * MB, 1000, sqlConf);
    assertEquals(maxPartitionBytes, result4);
  }

  @Test
  public void testCalculateMaxSplitBytes_UndefinedMinPartitionNum() {
    SQLConf sqlConf = SQLConf.get();
    // Ensure filesMinPartitionNum is undefined
    if (sqlConf.filesMinPartitionNum().isDefined()) {
      sqlConf.unsetConf("spark.sql.files.minPartitionNum");
    }

    long totalBytes = 200 * MB;
    int fileCount = 10;

    long result = PartitionUtils.calculateMaxSplitBytes(spark, totalBytes, fileCount, sqlConf);

    // Verify the result is still valid
    assertTrue(result > 0);
    assertTrue(result >= sqlConf.filesOpenCostInBytes());
    assertTrue(result <= sqlConf.filesMaxPartitionBytes());
    long calculatedTotalBytes = totalBytes + (long) fileCount * sqlConf.filesOpenCostInBytes();
    assertTrue(result <= calculatedTotalBytes);
  }

  @Test
  public void testBuildPartitionedFile() throws Exception {
    String tablePath = createTestTable("test_build_partitioned_file_" + System.nanoTime(), true);

    // Get an AddFile from the table
    Table table = Table.forPath(defaultEngine, tablePath);
    Scan scan = table.getLatestSnapshot(defaultEngine).getScanBuilder().build();
    FilteredColumnarBatch batch = scan.getScanFiles(defaultEngine).next();
    CloseableIterator<Row> rows = batch.getRows();
    AddFile addFile = new AddFile(rows.next().getStruct(0));
    rows.close();

    // Build PartitionedFile
    StructType partitionSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("part", DataTypes.StringType, true)});
    String normalizedTablePath = tablePath.endsWith("/") ? tablePath : tablePath + "/";
    PartitionedFile partitionedFile =
        PartitionUtils.buildPartitionedFile(
            addFile, partitionSchema, normalizedTablePath, ZoneId.of("UTC"));

    assertNotNull(partitionedFile);
    assertEquals(addFile.getSize(), partitionedFile.fileSize());
    assertEquals(1, partitionedFile.partitionValues().numFields());
  }

  /** Helper to create a test Delta table. */
  private String createTestTable(String tableName, boolean partitioned) {
    String tablePath = getTempTablePath(tableName);
    if (partitioned) {
      spark
          .range(10)
          .selectExpr("id", "cast(id % 3 as string) as part")
          .write()
          .format("delta")
          .partitionBy("part")
          .save(tablePath);
    } else {
      spark.range(10).write().format("delta").save(tablePath);
    }
    return tablePath;
  }

  private String getTempTablePath(String tableName) {
    return java.nio.file.Paths.get(System.getProperty("java.io.tmpdir"), "delta-test-" + tableName)
        .toString();
  }
}
