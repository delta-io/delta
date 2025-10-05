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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.golden.GoldenTableUtils$;
import java.io.File;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
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
  public void testVariantUnsupportedDataType() {
    String tablePath = goldenTablePath("spark-variant-checkpoint");

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              spark.sql("SELECT * FROM `dsv2`.`delta`.`" + tablePath + "`").collect();
            });

    Throwable rootCause = getRootCause(exception);
    String errorMessage = rootCause.getMessage();

    assertTrue(errorMessage.contains("Unsupported data type"));
    assertTrue(errorMessage.contains("variant"));
  }

  @Test
  public void testTimestampUnsupportedDataType() {
    String tablePath = goldenTablePath("kernel-timestamp-PST");

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              spark.sql("SELECT * FROM `dsv2`.`delta`.`" + tablePath + "`").collect();
            });

    Throwable rootCause = getRootCause(exception);
    String errorMessage = rootCause.getMessage();

    assertTrue(errorMessage.contains("Unsupported data type"));
    assertTrue(errorMessage.contains("timestamp"));
  }

  private Throwable getRootCause(Throwable throwable) {
    Throwable cause = throwable;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    return cause;
  }
}
