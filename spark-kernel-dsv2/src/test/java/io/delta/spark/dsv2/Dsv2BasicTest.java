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
package io.delta.spark.dsv2;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.golden.GoldenTableUtils$;
import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import scala.Function0;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Dsv2BasicTest extends QueryTest {

  private SparkSession spark;
  private String nameSpace;

  @BeforeAll
  public void setUp(@TempDir File tempDir) {
    // Spark doesn't allow '-'
    nameSpace = "ns_" + UUID.randomUUID().toString().replace('-', '_');
    SparkConf conf =
        new SparkConf()
            .set("spark.sql.catalog.dsv2", "io.delta.spark.dsv2.catalog.TestCatalog")
            .set("spark.sql.catalog.dsv2.base_path", tempDir.getAbsolutePath())
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .setMaster("local[*]")
            .setAppName("Dsv2BasicTest");
    spark = SparkSession.builder().config(conf).getOrCreate();
  }

  @AfterAll
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testCreateTable() {
    spark.sql(
        String.format(
            "CREATE TABLE dsv2.%s.create_table_test (id INT, name STRING, value DOUBLE)",
            nameSpace));

    Dataset<Row> actual =
        spark.sql(String.format("DESCRIBE TABLE dsv2.%s.create_table_test", nameSpace));

    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create("id", "int", null),
            RowFactory.create("name", "string", null),
            RowFactory.create("value", "double", null));
    assertDatasetEquals(actual, expectedRows);
  }

  @Test
  public void testBatchRead() {
    spark.sql(
        String.format(
            "CREATE TABLE dsv2.%s.batch_read_test (id INT, name STRING, value DOUBLE)", nameSpace));

    // Select and validate the data
    Dataset<Row> result =
        spark.sql(String.format("SELECT * FROM dsv2.%s.batch_read_test", nameSpace));

    List<Row> expectedRows = Arrays.asList();

    assertDatasetEquals(result, expectedRows);
  }

  @Test
  public void testQueryTableNotExist() {
    AnalysisException e =
        org.junit.jupiter.api.Assertions.assertThrows(
            AnalysisException.class,
            () -> spark.sql(String.format("SELECT * FROM dsv2.%s.not_found_test", nameSpace)));
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
    Dataset<Row> actual = spark.sql(String.format("DESCRIBE TABLE dsv2.delta.`%s`", tablePath));

    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create("id", "int", null),
            RowFactory.create("name", "string", null),
            RowFactory.create("value", "double", null));

    assertDatasetEquals(actual, expectedRows);
  }

  @Test
  public void testTablePrimitives() throws Exception {
    List<Row> expected = new ArrayList<>();
    for (int i = 0; i <= 10; i++) {
      if (i == 10) {
        expected.add(
            new GenericRow(
                new Object[] {null, null, null, null, null, null, null, null, null, null}));
      } else {
        expected.add(
            new GenericRow(
                new Object[] {
                  i,
                  (long) i,
                  (byte) i,
                  (short) i,
                  i % 2 == 0,
                  (float) i,
                  (double) i,
                  Integer.toString(i),
                  new byte[] {(byte) i, (byte) i},
                  new BigDecimal(i)
                }));
      }
    }

    checkTable("data-reader-primitives", expected);
  }

  @Test
  public void testTableWithNestedStruct() {
    List<Row> expected = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Row innerMost = new GenericRow(new Object[] {i, (long) i});
      Row middle =
          new GenericRow(new Object[] {Integer.toString(i), Integer.toString(i), innerMost});
      expected.add(new GenericRow(new Object[] {middle, i}));
    }
    // Assuming `checkTable` is made accessible (e.g., protected in base class)
    checkTable("data-reader-nested-struct", expected);
  }

  @Test
  public void testPartitionedTable() {
    // Build expected rows (excluding unsupported partition column `as_timestamp`)
    List<Row> expected = new ArrayList<>();
    java.sql.Date fixedDate = java.sql.Date.valueOf("2021-09-08");

    for (int i = 0; i < 2; i++) {
      // Array field: Seq(TestRow(i), TestRow(i), TestRow(i)) where TestRow(i) => struct(i)
      List<Row> arrElems =
          Arrays.asList(
              new GenericRow(new Object[] {i}),
              new GenericRow(new Object[] {i}),
              new GenericRow(new Object[] {i}));
      Object arrSeq = scala.collection.JavaConverters.asScalaBuffer(arrElems).toList();

      // Nested struct: TestRow(i.toString, i.toString, TestRow(i, i.toLong))
      Row innerMost = new GenericRow(new Object[] {i, (long) i});
      Row middle =
          new GenericRow(new Object[] {Integer.toString(i), Integer.toString(i), innerMost});

      expected.add(
          new GenericRow(
              new Object[] {
                i, // int
                (long) i, // long
                (byte) i, // byte
                (short) i, // short
                i % 2 == 0, // boolean
                (float) i, // float
                (double) i, // double
                Integer.toString(i), // string
                "null", // literal string
                fixedDate, // date (was daysSinceEpoch int)
                new BigDecimal(i), // decimal
                arrSeq, // array<struct>
                middle, // nested struct
                Integer.toString(i) // final string
              }));
    }

    // Null row variant with specific non-null complex fields (matches Scala test)
    List<Row> nullArrElems =
        Arrays.asList(
            new GenericRow(new Object[] {2}),
            new GenericRow(new Object[] {2}),
            new GenericRow(new Object[] {2}));
    Object nullArrSeq = scala.collection.JavaConverters.asScalaBuffer(nullArrElems).toList();
    Row nullInnerMost = new GenericRow(new Object[] {2, 2L});
    Row nullMiddle = new GenericRow(new Object[] {"2", "2", nullInnerMost});
    expected.add(
        new GenericRow(
            new Object[] {
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              nullArrSeq,
              nullMiddle,
              "2"
            }));

    // Read table, drop unsupported column `as_timestamp`
    String tablePath = goldenTablePath("data-reader-partition-values");
    Dataset<Row> full = spark.sql("SELECT * FROM `dsv2`.`delta`.`" + tablePath + "`");

    List<String> projectedCols = new ArrayList<>();
    for (String f : full.schema().fieldNames()) {
      if (!f.equals("as_timestamp")) {
        projectedCols.add(f);
      }
    }
    Dataset<Row> df = full.selectExpr(projectedCols.toArray(new String[0]));

    Function0<Dataset<Row>> dfFunc =
        new Function0<Dataset<Row>>() {
          @Override
          public Dataset<Row> apply() {
            return df;
          }
        };
    scala.collection.immutable.Seq<Row> expectedSeq =
        scala.collection.JavaConverters.asScalaBuffer(expected).toList();
    checkAnswer(dfFunc, expectedSeq);
  }

  private void checkTable(String path, List<Row> expected) {
    String tablePath = goldenTablePath(path);

    Dataset<Row> df = spark.sql("SELECT * FROM `dsv2`.`delta`.`" + tablePath + "`");
    Function0<Dataset<Row>> dfFunc =
        new Function0<Dataset<Row>>() {
          @Override
          public Dataset<Row> apply() {
            return df;
          }
        };

    scala.collection.immutable.Seq<Row> expectedSeq =
        scala.collection.JavaConverters.asScalaBuffer(expected).toList();
    checkAnswer(dfFunc, expectedSeq);
  }

  private String goldenTablePath(String name) {
    return GoldenTableUtils$.MODULE$.goldenTablePath(name);
  }

  //////////////////////
  // Private helpers //
  /////////////////////
  private void assertDatasetEquals(Dataset<Row> actual, List<Row> expectedRows) {
    List<Row> actualRows = actual.collectAsList();
    assertEquals(
        expectedRows,
        actualRows,
        () -> "Datasets differ: expected=" + expectedRows + "\nactual=" + actualRows);
  }

  @Override
  public SparkSession spark() {
    return spark;
  }
}
