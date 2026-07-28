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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

  private void createColumnMappingTable(String tablePath, String mappingMode) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, user_name STRING, amount DOUBLE) "
                + "USING delta TBLPROPERTIES ('delta.columnMapping.mode' = '%s')",
            tablePath, mappingMode));
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
}
