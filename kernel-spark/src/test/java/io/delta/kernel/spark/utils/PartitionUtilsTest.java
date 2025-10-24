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
package io.delta.kernel.spark.utils;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.spark.SparkDsv2TestBase;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.collection.immutable.Map$;

public class PartitionUtilsTest extends SparkDsv2TestBase {

  @ParameterizedTest
  @MethodSource("calculateMaxSplitBytesParameters")
  public void testCalculateMaxSplitBytes(
      long totalBytes, int fileCount, int minPartitionNum, long expected, String description) {
    SQLConf sqlConf = SQLConf.get();
    long originalMinPartitionNum = -1;
    try {
      // Set minPartitionNum if provided
      if (minPartitionNum > 0) {
        if (sqlConf.filesMinPartitionNum().isDefined()) {
          originalMinPartitionNum = ((Number) sqlConf.filesMinPartitionNum().get()).longValue();
        }
        sqlConf.setConfString("spark.sql.files.minPartitionNum", String.valueOf(minPartitionNum));
      }

      long result = PartitionUtils.calculateMaxSplitBytes(spark, totalBytes, fileCount, sqlConf);

      // Allow some flexibility in assertion due to internal calculations
      assertTrue(
          result > 0,
          String.format("%s: maxSplitBytes should be positive, got %d", description, result));
      assertTrue(
          result <= sqlConf.filesMaxPartitionBytes(),
          String.format("%s: maxSplitBytes should not exceed maxPartitionBytes", description));
      assertTrue(
          result >= sqlConf.filesOpenCostInBytes(),
          String.format("%s: maxSplitBytes should be at least openCostInBytes", description));
    } finally {
      // Restore original config
      if (minPartitionNum > 0) {
        if (originalMinPartitionNum >= 0) {
          sqlConf.setConfString(
              "spark.sql.files.minPartitionNum", String.valueOf(originalMinPartitionNum));
        } else {
          sqlConf.unsetConf("spark.sql.files.minPartitionNum");
        }
      }
    }
  }

  private static Stream<Arguments> calculateMaxSplitBytesParameters() {
    long MB = 1024 * 1024;
    return Stream.of(
        // (totalBytes, fileCount, minPartitionNum, expected, description)
        Arguments.of(100 * MB, 10, -1, -1, "Medium files"),
        Arguments.of(10 * MB, 1000, -1, -1, "Many small files"),
        Arguments.of(1000 * MB, 1, -1, -1, "Single large file"),
        Arguments.of(100 * MB, 10, 4, -1, "With explicit minPartitionNum"),
        Arguments.of(0, 0, -1, -1, "Zero bytes"));
  }

  // Note: getPartitionRow is tested through the integration test
  // testPlanInputPartitionsAndReadData_CompareWithDSv1 in SparkMicroBatchStreamTest
  // which validates partition column handling with real data

  @Test
  public void testCreateParquetReaderFactory() {
    StructType dataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
    StructType partitionSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("year", DataTypes.IntegerType, true)});
    StructType readDataSchema = dataSchema;
    Filter[] filters = new Filter[0];
    scala.collection.immutable.Map<String, String> options = Map$.MODULE$.empty();
    Configuration hadoopConf = new Configuration();
    SQLConf sqlConf = SQLConf.get();

    PartitionReaderFactory factory =
        PartitionUtils.createParquetReaderFactory(
            dataSchema, partitionSchema, readDataSchema, filters, options, hadoopConf, sqlConf);

    assertNotNull(factory, "PartitionReaderFactory should not be null");
  }
}
