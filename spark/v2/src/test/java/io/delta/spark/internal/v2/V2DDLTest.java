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
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/** Tests for V2 DDL operations. */
public class V2DDLTest extends V2TestBase {

  @Test
  public void testCreateTable() {
    spark.sql(
        str(
            "CREATE TABLE dsv2.%s.create_table_test (id INT, name STRING, value DOUBLE)",
            nameSpace));

    Dataset<Row> actual = spark.sql(str("DESCRIBE TABLE dsv2.%s.create_table_test", nameSpace));

    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create("id", "int", null),
            RowFactory.create("name", "string", null),
            RowFactory.create("value", "double", null));
    assertDatasetEquals(actual, expectedRows);
  }

  @Test
  public void testQueryTableNotExist() {
    AnalysisException e =
        org.junit.jupiter.api.Assertions.assertThrows(
            AnalysisException.class,
            () -> spark.sql(str("SELECT * FROM dsv2.%s.not_found_test", nameSpace)));
    assertEquals(
        "TABLE_OR_VIEW_NOT_FOUND",
        e.getErrorClass(),
        "Missing table should raise TABLE_OR_VIEW_NOT_FOUND");
  }

  @Test
  public void testPathBasedTable(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();

    // Create test data and write as Delta table
    Dataset<Row> testData =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create(1, "Alice", 100.0),
                RowFactory.create(2, "Bob", 200.0),
                RowFactory.create(3, "Charlie", 300.0)),
            DataTypes.createStructType(
                Arrays.asList(
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("name", DataTypes.StringType, false),
                    DataTypes.createStructField("value", DataTypes.DoubleType, false))));

    testData.write().format("delta").save(tablePath);

    // TODO: [delta-io/delta#5001] change to select query after batch read is supported for dsv2
    // path.
    Dataset<Row> actual = spark.sql(str("DESCRIBE TABLE dsv2.delta.`%s`", tablePath));

    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create("id", "int", null),
            RowFactory.create("name", "string", null),
            RowFactory.create("value", "double", null));

    assertDatasetEquals(actual, expectedRows);
  }
}
