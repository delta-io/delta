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
package io.delta.spark.internal.v2.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.spark.internal.v2.V2TestBase;
import java.io.File;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** E2E DSv2 batch-write tests for column-mapped tables. */
public class V2WriteTest extends V2TestBase {

  @ParameterizedTest(name = "columnMappingMode={0}")
  @ValueSource(strings = {"name", "id"})
  public void writeToColumnMappingTable(String mappingMode, @TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createColumnMappingTable(tablePath, mappingMode);

    spark.sql(
        str(
            "INSERT INTO dsv2.delta.`%s` VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)",
            tablePath));

    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0)));
    check(
        str("SELECT * FROM delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0)));

    // Both name and id mode store data under physical col-* names.
    assertPhysicalParquetUsesMappedColumnNames(tablePath, "id", "user_name", "amount");
  }

  @Test
  public void writeToColumnMappingTableWithRenamedColumn(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();
    createColumnMappingTable(tablePath, "name");

    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, 'Alice', 100.0)", tablePath));
    // The physical name is unchanged after a RENAME under column mapping, so the second append
    // must still land in the same physical columns.
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN user_name TO customer_name", tablePath));
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (2, 'Bob', 200.0)", tablePath));

    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0)));
    check(
        str("SELECT * FROM delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0)));
  }

  @Test
  public void multipleAppendsOnColumnMappingTable(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();
    createColumnMappingTable(tablePath, "name");

    // Interleave V2 and V1 writes to confirm the physical layout produced by the V2 path is
    // consistent.
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, 'Alice', 100.0)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'Bob', 200.0)", tablePath));
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (3, 'Carol', 300.0)", tablePath));

    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0), row(3, "Carol", 300.0)));
    check(
        str("SELECT * FROM delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0), row(3, "Carol", 300.0)));
  }

  @ParameterizedTest(name = "columnMappingMode={0}")
  @ValueSource(strings = {"name", "id"})
  public void writeToPartitionedColumnMappingTable(String mappingMode, @TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createPartitionedColumnMappingTable(tablePath, mappingMode);

    spark.sql(
        str(
            "INSERT INTO dsv2.delta.`%s` VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)",
            tablePath));

    // Read back through both the V2 and V1 paths, both resolve partition values by physical name,
    // so a logical-directory regression would surface here as null partition columns.
    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0)));
    check(
        str("SELECT * FROM delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0)));

    assertPhysicalPartitionDirExists(tablePath, "Alice");
    assertPhysicalPartitionDirExists(tablePath, "Bob");
    assertAddFilePartitionValuesArePhysical(tablePath, "name");
    // The Parquet body omits the partition column and uses physical col-* names for the rest.
    assertPhysicalParquetUsesMappedColumnNames(
        physicalPartitionDir(tablePath, "Alice").getAbsolutePath(), "id", "value");
    assertParquetBodyOmitsPartitionColumns(
        physicalPartitionDir(tablePath, "Alice"), /* expectedDataColumns */ 2);
  }

  @Test
  public void writeToMultiColumnPartitionedColumnMappingTable(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, region STRING, tier INT) USING delta "
                + "PARTITIONED BY (region, tier) "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));

    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, 'us', 1), (2, 'eu', 2)", tablePath));

    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "us", 1), row(2, "eu", 2)));
    check(
        str("SELECT * FROM delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "us", 1), row(2, "eu", 2)));

    // Nested physical partition directories: col-<uuid>=us/col-<uuid>=1.
    File outer = physicalPartitionDir(tablePath, "us");
    File inner = physicalPartitionChildDir(outer, "1");
    assertTrue(
        inner != null, "Expected a nested physical partition directory col-*=1 under " + outer);
    // Both partition columns are path-encoded.
    assertParquetBodyOmitsPartitionColumns(inner, /* expectedDataColumns */ 1);
  }

  @Test
  public void writeNullPartitionValueColumnMappingTable(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createPartitionedColumnMappingTable(tablePath, "name");

    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, NULL, 100.0)", tablePath));

    check(str("SELECT * FROM dsv2.delta.`%s`", tablePath), List.of(row(1, null, 100.0)));
    check(str("SELECT * FROM delta.`%s`", tablePath), List.of(row(1, null, 100.0)));

    // A null partition value is Hive-encoded under the physical parent directory.
    assertPhysicalPartitionDirExists(tablePath, "__HIVE_DEFAULT_PARTITION__");
    assertParquetBodyOmitsPartitionColumns(
        physicalPartitionDir(tablePath, "__HIVE_DEFAULT_PARTITION__"), /* expectedDataColumns */ 2);
  }

  @Test
  public void partitionedIcebergCompatWriteIsRejected(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING, value DOUBLE) USING delta "
                + "PARTITIONED BY (name) TBLPROPERTIES ("
                + "'delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true')",
            tablePath));

    // IcebergCompat materializes partition columns into the Parquet body, which this write path
    // does not do yet.
    assertThrows(
        UnsupportedOperationException.class,
        () -> spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, 'Alice', 100.0)", tablePath)));
  }

  private void createColumnMappingTable(String tablePath, String mappingMode) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, user_name STRING, amount DOUBLE) "
                + "USING delta TBLPROPERTIES ('delta.columnMapping.mode' = '%s')",
            tablePath, mappingMode));
  }

  private void createPartitionedColumnMappingTable(String tablePath, String mappingMode) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING, value DOUBLE) USING delta "
                + "PARTITIONED BY (name) TBLPROPERTIES ('delta.columnMapping.mode' = '%s')",
            tablePath, mappingMode));
  }

  /** Returns the {@code col-<uuid>=<value>} partition directory under {@code tablePath}. */
  private File physicalPartitionDir(String tablePath, String value) {
    File[] dirs =
        new File(tablePath).listFiles((d, n) -> n.startsWith("col-") && n.endsWith("=" + value));
    assertNotNull(dirs, "Expected partition directories under " + tablePath);
    assertTrue(
        dirs.length == 1,
        "Expected one physical partition directory col-*=" + value + ", got " + dirs.length);
    return dirs[0];
  }

  /** Returns the nested {@code col-<uuid>=<value>} directory under {@code parent}, or null. */
  private File physicalPartitionChildDir(File parent, String value) {
    File[] dirs = parent.listFiles((d, n) -> n.startsWith("col-") && n.endsWith("=" + value));
    return (dirs != null && dirs.length == 1) ? dirs[0] : null;
  }

  /**
   * Asserts a physical {@code col-<uuid>=<value>} partition directory exists and contains data, and
   * that no logical {@code <logicalName>=<value>} directory was written instead.
   */
  private void assertPhysicalPartitionDirExists(String tablePath, String value) {
    File dir = physicalPartitionDir(tablePath, value);
    assertTrue(dir.isDirectory(), "Expected a directory at " + dir);
    File[] parquet = dir.listFiles((d, n) -> n.endsWith(".parquet"));
    assertTrue(parquet != null && parquet.length > 0, "Expected a parquet file under " + dir);
    File[] logical =
        new File(tablePath).listFiles((d, n) -> !n.startsWith("col-") && n.endsWith("=" + value));
    assertTrue(
        logical == null || logical.length == 0,
        "Did not expect a logical partition directory ending in '=" + value + "'");
  }

  /**
   * Asserts the AddFile {@code partitionValues} in the log are keyed by the physical col-* name.
   * The commit JSON is read with schema inference, so {@code partitionValues} surfaces as a struct
   * whose field names are the partition keys.
   */
  private void assertAddFilePartitionValuesArePhysical(String tablePath, String logicalName) {
    org.apache.spark.sql.types.StructType addType =
        (org.apache.spark.sql.types.StructType)
            spark
                .read()
                .json(tablePath + "/_delta_log/*.json")
                .where("add is not null")
                .schema()
                .apply("add")
                .dataType();
    org.apache.spark.sql.types.StructType partitionValuesType =
        (org.apache.spark.sql.types.StructType) addType.apply("partitionValues").dataType();
    List<String> keys = List.of(partitionValuesType.fieldNames());
    assertFalse(keys.isEmpty(), "Expected at least one partition-value key");
    assertTrue(
        keys.stream().allMatch(k -> k.startsWith("col-")),
        "Expected physical col-* partition-value keys, got: " + keys);
    assertFalse(
        keys.contains(logicalName),
        "AddFile partitionValues must not use the logical key '" + logicalName + "'");
  }

  /**
   * Verifies the on-disk Parquet schema uses physical column mapping names and that every field
   * carries a Parquet field id.
   */
  private void assertPhysicalParquetUsesMappedColumnNames(
      String tablePath, String... logicalColumnNames) throws Exception {
    File[] parquetFiles = new File(tablePath).listFiles((dir, name) -> name.endsWith(".parquet"));
    assertNotNull(parquetFiles, "Expected parquet data files under " + tablePath);
    assertTrue(parquetFiles.length > 0, "Expected at least one parquet data file");
    Path parquetPath = new Path(parquetFiles[0].getAbsolutePath());
    List<org.apache.parquet.schema.Type> fields =
        ParquetFileReader.readFooter(
                spark.sessionState().newHadoopConf(),
                parquetPath,
                ParquetMetadataConverter.NO_FILTER)
            .getFileMetaData()
            .getSchema()
            .getFields();
    List<String> parquetFieldNames =
        fields.stream()
            .map(org.apache.parquet.schema.Type::getName)
            .collect(java.util.stream.Collectors.toList());
    for (String logicalName : logicalColumnNames) {
      assertFalse(
          parquetFieldNames.contains(logicalName),
          "Parquet schema should not contain logical column name '"
              + logicalName
              + "'; got fields: "
              + parquetFieldNames);
    }
    assertTrue(
        parquetFieldNames.stream().allMatch(name -> name.startsWith("col-")),
        "Expected physical col-* column names in Parquet, got: " + parquetFieldNames);
    for (org.apache.parquet.schema.Type field : fields) {
      assertNotNull(
          field.getId(), "Expected a Parquet field id on column '" + field.getName() + "'");
    }
  }

  /**
   * Verifies the Parquet body under {@code partitionDir} holds only the data columns. Partition
   * columns are path-encoded, not materialized into the file.
   */
  private void assertParquetBodyOmitsPartitionColumns(File partitionDir, int expectedDataColumns)
      throws Exception {
    File[] parquetFiles = partitionDir.listFiles((dir, name) -> name.endsWith(".parquet"));
    assertNotNull(parquetFiles, "Expected parquet data files under " + partitionDir);
    assertTrue(parquetFiles.length > 0, "Expected at least one parquet data file");
    List<String> parquetFieldNames =
        ParquetFileReader.readFooter(
                spark.sessionState().newHadoopConf(),
                new Path(parquetFiles[0].getAbsolutePath()),
                ParquetMetadataConverter.NO_FILTER)
            .getFileMetaData().getSchema().getFields().stream()
            .map(org.apache.parquet.schema.Type::getName)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(
        expectedDataColumns,
        parquetFieldNames.size(),
        "Expected only the data columns in the Parquet body, got: " + parquetFieldNames);
  }
}
