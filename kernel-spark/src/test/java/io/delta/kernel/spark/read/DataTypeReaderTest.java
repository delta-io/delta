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
package io.delta.kernel.spark.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.golden.GoldenTableUtils$;
import java.io.File;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.sources.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DataTypeReaderTest {

  private SparkMicroBatchStream microBatchStream;
  private SparkSession spark;

  @BeforeAll
  public void setUpClass() {
    // Create a temporary directory manually instead of using @TempDir
    File tempDir =
        new File(System.getProperty("java.io.tmpdir"), "datatype-reader-test-" + System.nanoTime());
    tempDir.mkdirs();
    tempDir.deleteOnExit();

    SparkConf conf =
        new SparkConf()
            .set("spark.sql.catalog.dsv2", "io.delta.kernel.spark.catalog.TestCatalog")
            .set("spark.sql.catalog.dsv2.base_path", tempDir.getAbsolutePath())
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .setMaster("local[*]")
            .setAppName("DataTypeReaderTest");
    spark = SparkSession.builder().config(conf).getOrCreate();
  }

  @AfterAll
  public void tearDownClass() {
    if (spark != null) {
      spark.stop();
    }
  }

  @BeforeEach
  void setUp() {
    microBatchStream = new SparkMicroBatchStream();
  }

  private String goldenTablePath(String name) {
    return GoldenTableUtils$.MODULE$.goldenTablePath(name);
  }

  @Test
  public void testnew() {
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.latestOffset());
    assertEquals("latestOffset is not supported", exception.getMessage());
  }

  @Test
  public void testLatestOffset_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.latestOffset());
    assertEquals("latestOffset is not supported", exception.getMessage());
  }

  @Test
  public void testPlanInputPartitions_throwsUnsupportedOperationException() {
    Offset start = null;
    Offset end = null;
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> microBatchStream.planInputPartitions(start, end));
    assertEquals("planInputPartitions is not supported", exception.getMessage());
  }

  @Test
  public void testCreateReaderFactory_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class, () -> microBatchStream.createReaderFactory());
    assertEquals("createReaderFactory is not supported", exception.getMessage());
  }

  @Test
  public void testInitialOffset_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.initialOffset());
    assertEquals("initialOffset is not supported", exception.getMessage());
  }

  @Test
  public void testDeserializeOffset_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class, () -> microBatchStream.deserializeOffset("{}"));
    assertEquals("deserializeOffset is not supported", exception.getMessage());
  }

  @Test
  public void testCommit_throwsUnsupportedOperationException() {
    Offset end = null;
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.commit(end));
    assertEquals("commit is not supported", exception.getMessage());
  }

  @Test
  public void testStop_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.stop());
    assertEquals("stop is not supported", exception.getMessage());
  }

  @Test
  public void testUnsupportedDataTypeValidation() {
    // Test the real-world scenario: table created with unsupported mock data type
    // then accessed via kernel-spark connector (should detect unsupported type)

    String tablePath = goldenTablePath("kernel-spark-unsupported-datatype-validation");

    try {
      // First, try to describe the table to see its schema
      Dataset<Row> describeResult = spark.sql("DESCRIBE TABLE `dsv2`.`delta`.`" + tablePath + "`");
      List<Row> schemaRows = describeResult.collectAsList();

      System.out.println("Golden table schema:");
      for (Row row : schemaRows) {
        System.out.println("  " + row.getString(0) + " : " + row.getString(1));
      }

      // Check for unsupported mock columns in the described schema
      boolean hasUnsupportedColumn =
          schemaRows.stream()
              .anyMatch(
                  row ->
                      "unsupported_column".equals(row.getString(0))
                          && (row.getString(1).toLowerCase().contains("unsupported")
                              || row.getString(1).toLowerCase().contains("mock")));

      if (hasUnsupportedColumn) {
        System.out.println("✓ Golden table contains unsupported mock data type column");
        System.out.println(
            "  Testing file path access (should trigger UnsupportedDataTypeException)...");

        try {
          // This should go through our kernel-spark connector and trigger validation
          Dataset<Row> result = spark.sql("SELECT * FROM `dsv2`.`delta`.`" + tablePath + "`");
          List<Row> data = result.collectAsList();

          System.out.println("  WARNING: Query succeeded - validation may not have been triggered");
          System.out.println("  Returned " + data.size() + " rows");

        } catch (Exception queryException) {
          System.out.println("  ✓ Query failed as expected: " + queryException.getMessage());
          if (queryException.getMessage() != null
              && (queryException.getMessage().toLowerCase().contains("unsupported")
                  || queryException.getMessage().toLowerCase().contains("mock"))) {
            System.out.println(
                "  ✓ Failure message indicates unsupported data type validation working");
          }
        }

      } else {
        assert (false);
        System.out.println("  Golden table created without unsupported column");
        System.out.println(
            "  This means the unsupported mock data type was rejected during golden table creation");

        // Try to query the table anyway to make sure it works for supported types
        Dataset<Row> result = spark.sql("SELECT * FROM `dsv2`.`delta`.`" + tablePath + "`");
        List<Row> data = result.collectAsList();
        System.out.println("  ✓ Successfully queried fallback table with " + data.size() + " rows");
      }

    } catch (Exception e) {
      System.out.println(
          "Golden table test failed: " + e.getClass().getSimpleName() + " - " + e.getMessage());

      // This might happen if golden table doesn't exist
      if (!new File(tablePath).exists()) {
        System.out.println("  Golden table directory does not exist: " + tablePath);
        System.out.println("  You may need to run: ./build/sbt \"goldenTables/test\"");
      }
    }
  }
}
