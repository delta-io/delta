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
package io.delta.spark.dsv2.scan.batch;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import io.delta.spark.dsv2.KernelSparkDsv2TestBase;
import java.io.File;
import java.util.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class KernelSparkPartitionReaderTest extends KernelSparkDsv2TestBase {

  private final StructType SCAN_STATE_SCHEMA =
      new StructType()
          .add("configuration", new MapType(StringType.STRING, StringType.STRING, false))
          .add("logicalSchemaJson", StringType.STRING)
          .add("physicalSchemaJson", StringType.STRING)
          .add("partitionColumns", new ArrayType(StringType.STRING, false))
          .add("minReaderVersion", IntegerType.INTEGER)
          .add("minWriterVersion", IntegerType.INTEGER)
          .add("tablePath", StringType.STRING);

  @Test
  public void testReadParquetFile(@TempDir File tempDir) throws Exception {
    // Setup schema and test data
    StructType schema =
        new StructType()
            .add("id", IntegerType.INTEGER)
            .add("name", StringType.STRING)
            .add("value", DoubleType.DOUBLE);

    ColumnVector[] vectors =
        new ColumnVector[] {
          VectorUtils.buildColumnVector(Arrays.asList(1, 2, 3), IntegerType.INTEGER),
          VectorUtils.buildColumnVector(
              Arrays.asList("Alice", "Bob", "Charlie"), StringType.STRING),
          VectorUtils.buildColumnVector(Arrays.asList(100.0, 200.0, 300.0), DoubleType.DOUBLE)
        };

    // Create and write parquet file
    String testFile = "test.parquet";
    File parquetFile = new File(tempDir, testFile);
    String normalizedFilePath = "file://" + parquetFile.getAbsolutePath();
    ColumnarBatch batch = new DefaultColumnarBatch(3, schema, vectors);
    FilteredColumnarBatch filteredBatch = new FilteredColumnarBatch(batch, Optional.empty());
    defaultEngine
        .getParquetHandler()
        .writeParquetFileAtomically(
            normalizedFilePath, Utils.singletonCloseableIterator(filteredBatch));
    assertTrue(parquetFile.exists(), "Parquet file not created");

    String tableRoot = "file://" + tempDir.getAbsolutePath();
    KernelSparkPartitionReader reader =
        new KernelSparkPartitionReader(
            defaultEngine, createScanState(schema, tableRoot), createFileRow(tableRoot, testFile));

    assertTrue(reader.next());
    InternalRow row1 = reader.get();
    assertEquals(1, row1.getInt(0));
    assertEquals("Alice", row1.get(1, org.apache.spark.sql.types.DataTypes.StringType).toString());
    assertEquals(100.0, row1.getDouble(2));
    // Test repeat get
    row1 = reader.get();
    assertEquals(1, row1.getInt(0));
    assertEquals("Alice", row1.get(1, org.apache.spark.sql.types.DataTypes.StringType).toString());
    assertEquals(100.0, row1.getDouble(2));

    assertTrue(reader.next());
    InternalRow row2 = reader.get();
    assertEquals(2, row2.getInt(0));
    assertEquals("Bob", row2.get(1, org.apache.spark.sql.types.DataTypes.StringType).toString());
    assertEquals(200.0, row2.getDouble(2));

    assertTrue(reader.next());
    InternalRow row3 = reader.get();
    assertEquals(3, row3.getInt(0));
    assertEquals(
        "Charlie", row3.get(1, org.apache.spark.sql.types.DataTypes.StringType).toString());
    assertEquals(300.0, row3.getDouble(2));

    assertFalse(reader.next());
    reader.close();
  }

  //////////////////////
  // Private helpers //
  /////////////////////

  private Row createScanState(StructType schema, String tableRoot) {
    HashMap<Integer, Object> valueMap = new HashMap<>();
    String schemaJson = DataTypeJsonSerDe.serializeDataType(schema);
    // ScanStateRow fields:
    valueMap.put(0, VectorUtils.stringStringMapValue(new HashMap<>())); // configuration
    valueMap.put(1, schemaJson); // logicalSchemaJson
    valueMap.put(2, schemaJson); // physicalSchemaJson
    valueMap.put(
        3,
        VectorUtils.buildArrayValue( // partitionColumns
            new ArrayList<>(), StringType.STRING));
    valueMap.put(4, 1); // minReaderVersion
    valueMap.put(5, 2); // minWriterVersion
    valueMap.put(6, tableRoot); // tablePath
    return new GenericRow(SCAN_STATE_SCHEMA, valueMap);
  }

  private Row createFileRow(String tableRoot, String parquetFileName) {
    // Strip "file://" prefix to get local file path
    String localPath = tableRoot.substring("file://".length());
    File parquetFile = new File(new File(localPath), parquetFileName);
    assertTrue(parquetFile.exists(), "Parquet file does not exist: " + parquetFile);

    Map<Integer, Object> addFileValues = new HashMap<>();
    addFileValues.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("path"), parquetFileName);
    addFileValues.put(
        AddFile.SCHEMA_WITHOUT_STATS.indexOf("partitionValues"),
        VectorUtils.stringStringMapValue(new HashMap<>()));
    addFileValues.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("size"), parquetFile.length());
    addFileValues.put(
        AddFile.SCHEMA_WITHOUT_STATS.indexOf("modificationTime"), parquetFile.lastModified());
    addFileValues.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("dataChange"), false);

    Row addFileRow = new GenericRow(AddFile.SCHEMA_WITHOUT_STATS, addFileValues);

    Map<Integer, Object> scanFileValues = new HashMap<>();
    scanFileValues.put(InternalScanFileUtils.ADD_FILE_ORDINAL, addFileRow);
    scanFileValues.put(InternalScanFileUtils.SCAN_FILE_SCHEMA.indexOf("tableRoot"), tableRoot);

    return new GenericRow(InternalScanFileUtils.SCAN_FILE_SCHEMA, scanFileValues);
  }
}
