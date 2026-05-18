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

package io.sparkuctest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.io.TempDir;

/**
 * Wave-7 follow-up to {@link UCDeltaStreamingEdgeDataReadTest}: ports the remaining edge-data cases
 * from {@code V2StreamingEdgeDataReadTest} (Cases 5, 7, 8, 9, 10) over the Unity Catalog catalog
 * path for both EXTERNAL and MANAGED table types.
 *
 * <p>Each test runs twice via {@link UCDeltaTableIntegrationBaseTest.TableType}. The MANAGED
 * variant is the relevant repro target; EXTERNAL via UC catalog is a control.
 */
public class UCDeltaStreamingEdgeDataReadTest_W7 extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  /** Allocates a fresh local checkpoint directory. */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  // Case 5: Special-char strings (commas, quotes, empty, surrogate pairs, very long).
  @TestAllTableTypes
  public void testManagedSpecialCharStrings(TableType tableType) throws Exception {
    withNewTable(
        "edge_special_strings",
        "id INT, s STRING",
        tableType,
        tableName -> {
          StringBuilder veryLong = new StringBuilder();
          for (int i = 0; i < 5_000; i++) veryLong.append("x");
          String veryLongStr = veryLong.toString();

          // Use DataFrame writes to avoid SQL-string escaping pitfalls.
          List<Row> rows =
              Arrays.asList(
                  RowFactory.create(1, "a,b,c"),
                  RowFactory.create(2, "it's a 'quote'"),
                  RowFactory.create(3, "say \"hi\""),
                  RowFactory.create(4, ""),
                  // Non-BMP code point (emoji U+1F600).
                  RowFactory.create(5, "smile=😀"),
                  RowFactory.create(6, veryLongStr));

          StructType schema =
              DataTypes.createStructType(
                  Arrays.asList(
                      DataTypes.createStructField("id", DataTypes.IntegerType, false),
                      DataTypes.createStructField("s", DataTypes.StringType, true)));
          spark()
              .createDataFrame(rows, schema)
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName);

          String queryName =
              "edge_special_strings_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> actual =
              spark().sql("SELECT * FROM " + queryName + " ORDER BY id").collectAsList();
          assertEquals(6, actual.size(), "expected 6 rows; got: " + actual);
          assertEquals("a,b,c", actual.get(0).getString(1));
          assertEquals("it's a 'quote'", actual.get(1).getString(1));
          assertEquals("say \"hi\"", actual.get(2).getString(1));
          assertEquals("", actual.get(3).getString(1));
          assertEquals("smile=😀", actual.get(4).getString(1));
          assertEquals(veryLongStr, actual.get(5).getString(1));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // Case 7: Empty-string partition value (must round-trip distinct from NULL).
  @TestAllTableTypes
  public void testManagedEmptyStringPartitionValue(TableType tableType) throws Exception {
    withNewTable(
        "edge_empty_string_part",
        "id INT, p STRING",
        "p",
        tableType,
        null,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, ''), (2, 'a'), (3, NULL)", tableName);

          String queryName =
              "edge_empty_part_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> actual =
              spark().sql("SELECT * FROM " + queryName + " ORDER BY id").collectAsList();
          assertEquals(3, actual.size(), "expected 3 rows; got: " + actual);
          // id=1 row: partition value should be empty-string, NOT null.
          assertEquals(false, actual.get(0).isNullAt(1), "id=1 partition should not be null");
          assertEquals("", actual.get(0).getString(1), "id=1 partition should be empty-string");
          // id=2 row: 'a'.
          assertEquals("a", actual.get(1).getString(1));
          // id=3 row: explicit NULL.
          assertTrue(actual.get(2).isNullAt(1), "id=3 partition should be null");
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // Case 8: Special-char partition values (spaces, =, #, /, %)

  @TestAllTableTypes
  public void testManagedSpecialCharPartitionValues(TableType tableType) throws Exception {
    withNewTable(
        "edge_special_part",
        "id INT, p STRING",
        "p",
        tableType,
        null,
        tableName -> {
          // Use DataFrame writes for partition values containing chars that must be URL-encoded.
          List<Row> rows =
              Arrays.asList(
                  RowFactory.create(1, "with space"),
                  RowFactory.create(2, "k=v"),
                  RowFactory.create(3, "tag#1"),
                  RowFactory.create(4, "a/b"),
                  RowFactory.create(5, "100%done"));
          StructType schema =
              DataTypes.createStructType(
                  Arrays.asList(
                      DataTypes.createStructField("id", DataTypes.IntegerType, false),
                      DataTypes.createStructField("p", DataTypes.StringType, true)));
          spark()
              .createDataFrame(rows, schema)
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName);

          String queryName =
              "edge_special_part_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> actual =
              spark().sql("SELECT * FROM " + queryName + " ORDER BY id").collectAsList();
          assertEquals(5, actual.size(), "expected 5 rows; got: " + actual);
          assertEquals("with space", actual.get(0).getString(1));
          assertEquals("k=v", actual.get(1).getString(1));
          assertEquals("tag#1", actual.get(2).getString(1));
          assertEquals("a/b", actual.get(3).getString(1));
          assertEquals("100%done", actual.get(4).getString(1));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // Case 9a: Single-row table.

  @TestAllTableTypes
  public void testManagedSingleRowTable(TableType tableType) throws Exception {
    withNewTable(
        "edge_single_row",
        "id INT, name STRING",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (42, 'only')", tableName);

          String queryName =
              "edge_single_row_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> actual = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(1, actual.size());
          assertEquals(42, actual.get(0).getInt(0));
          assertEquals("only", actual.get(0).getString(1));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // Case 9b: Single-file table (20 rows coalesced to 1 file).

  @TestAllTableTypes
  public void testManagedSingleFileTable(TableType tableType) throws Exception {
    withNewTable(
        "edge_single_file",
        "id INT, name STRING",
        tableType,
        tableName -> {
          spark()
              .range(20)
              .selectExpr("cast(id as int) as id", "concat('row', cast(id as string)) as name")
              .coalesce(1)
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName);

          String queryName =
              "edge_single_file_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> actual = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(20, actual.size());
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // Case 10: Duplicate first-column values.

  @TestAllTableTypes
  public void testManagedDuplicateFirstColumnValues(TableType tableType) throws Exception {
    withNewTable(
        "edge_dup_first_col",
        "id INT, name STRING",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'a'), (1, 'b'), (1, 'c'), (2, 'd'), (2, 'e')", tableName);

          String queryName =
              "edge_dup_first_col_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> input = spark().readStream().format("delta").table(tableName);
          input
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> actual =
              spark().sql("SELECT * FROM " + queryName + " ORDER BY id, name").collectAsList();
          assertEquals(5, actual.size());
          assertEquals(1, actual.get(0).getInt(0));
          assertEquals("a", actual.get(0).getString(1));
          assertEquals(2, actual.get(4).getInt(0));
          assertEquals("e", actual.get(4).getString(1));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }
}
