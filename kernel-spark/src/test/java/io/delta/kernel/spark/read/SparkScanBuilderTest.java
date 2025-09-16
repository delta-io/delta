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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.spark.SparkDsv2TestBase;
import java.io.File;
import java.lang.reflect.Field;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SparkScanBuilderTest extends SparkDsv2TestBase {

  @Test
  public void testBuild_returnsScanWithExpectedSchema(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "scan_builder_test";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, dep_id INT) USING delta PARTITIONED BY (dep_id) LOCATION '%s'",
            tableName, path));
    Snapshot snapshot = TableManager.loadSnapshot(path).build(defaultEngine);
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    SparkScanBuilder builder =
        new SparkScanBuilder(
            tableName,
            path,
            dataSchema,
            partitionSchema,
            (SnapshotImpl) snapshot,
            CaseInsensitiveStringMap.empty());

    StructType expectedSparkSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true /*nullable*/),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });

    builder.pruneColumns(expectedSparkSchema);
    Scan scan = builder.build();

    assertTrue(scan instanceof SparkScan);
    assertEquals(expectedSparkSchema, scan.readSchema());
  }

  @Test
  public void testToMicroBatchStream_returnsSparkMicroBatchStream(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "microbatch_test";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, dep_id INT) USING delta PARTITIONED BY (dep_id) LOCATION '%s'",
            tableName, path));
    Snapshot snapshot = TableManager.loadSnapshot(path).build(defaultEngine);
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    SparkScanBuilder builder =
        new SparkScanBuilder(
            tableName,
            path,
            dataSchema,
            partitionSchema,
            (SnapshotImpl) snapshot,
            CaseInsensitiveStringMap.empty());
    Scan scan = builder.build();

    String checkpointLocation = "/tmp/checkpoint";
    MicroBatchStream microBatchStream = scan.toMicroBatchStream(checkpointLocation);

    assertNotNull(microBatchStream, "MicroBatchStream should not be null");
    assertTrue(
        microBatchStream instanceof SparkMicroBatchStream,
        "MicroBatchStream should be an instance of SparkMicroBatchStream");
  }

  @Test
  public void testPushFilters_singleSupportedDataFilter(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    Filter[] filters = {new EqualTo("id", 100)};
    Filter[] postScanFilters = builder.pushFilters(filters);

    // data filter needs post-scan evaluation
    assertEquals(1, postScanFilters.length);
    assertArrayEquals(filters, postScanFilters);
    assertArrayEquals(filters, builder.pushedFilters());

    Predicate[] pushedPredicates = getPushedKernelPredicates(builder);
    assertEquals(1, pushedPredicates.length);
    assertEquals("=", pushedPredicates[0].getName());
  }

  @Test
  public void testPushFilters_singleUnsupportedFilter(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    Filter[] filters = {new StringStartsWith("name", "test")};
    Filter[] postScanFilters = builder.pushFilters(filters);

    // unsupported filter needs post-scan evaluation
    assertEquals(1, postScanFilters.length);
    assertArrayEquals(filters, postScanFilters);
    assertEquals(0, builder.pushedFilters().length);

    // unsupported filter is not pushed
    Predicate[] pushedPredicates = getPushedKernelPredicates(builder);
    assertEquals(0, pushedPredicates.length);
  }

  @Test
  public void testPushFilters_mixedSupportedAndUnsupported(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    Filter[] filters = {
      new EqualTo("id", 100), new StringStartsWith("name", "test"), new GreaterThan("id", 50)
    };
    Filter[] postScanFilters = builder.pushFilters(filters);

    // all data filters need post-scan evaluation
    assertEquals(3, postScanFilters.length);
    assertArrayEquals(filters, postScanFilters);

    // only supported filters are pushed
    Filter[] expectedPushed = {filters[0], filters[2]};
    assertArrayEquals(expectedPushed, builder.pushedFilters());
    Predicate[] pushedPredicates = getPushedKernelPredicates(builder);
    assertEquals(2, pushedPredicates.length);
  }

  @Test
  public void testPushFilters_mixedDataAndPartitionFilters(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    Filter[] filters = {
      new EqualTo("id", 100), // data filter, supported
      new EqualTo("dep_id", 1), // partition filter, supported
      new GreaterThan("id", 50) // data filter, supported
    };
    Filter[] postScanFilters = builder.pushFilters(filters);

    // data filters need post-scan evaluation
    Filter[] expectedPost = {filters[0], filters[2]};
    assertEquals(2, postScanFilters.length);
    assertArrayEquals(expectedPost, postScanFilters);

    // all filters are pushed
    Predicate[] pushedPredicates = getPushedKernelPredicates(builder);
    assertEquals(3, pushedPredicates.length);
    assertEquals("=", pushedPredicates[0].getName());
    assertEquals("=", pushedPredicates[1].getName());
    assertEquals(">", pushedPredicates[2].getName());
  }

  @Test
  public void testPushFilters_partitionFilterHandling(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    Filter[] filters = {
      new EqualTo("id", 100), // data filter, supported
      new EqualTo("dep_id", 1), // partition filter, supported
      new GreaterThan("dep_id", 0), // partition filter, supported
      new StringStartsWith("name", "test") // data filter, unsupported
    };
    Filter[] postScanFilters = builder.pushFilters(filters);

    // data filters need post-scan evaluation
    assertEquals(2, postScanFilters.length);
    assertTrue(containsFilter(postScanFilters, filters[0]));
    assertTrue(containsFilter(postScanFilters, filters[3]));

    // only supported filters are pushed
    Filter[] expectedPushed = {filters[0], filters[1], filters[2]};
    assertArrayEquals(expectedPushed, builder.pushedFilters());

    // check data filters
    Filter[] dataFilters = getDataFilters(builder);
    assertEquals(2, dataFilters.length);
    assertTrue(containsFilter(dataFilters, filters[0]));
    assertTrue(containsFilter(dataFilters, filters[3]));
  }

  private SparkScanBuilder createTestScanBuilder(File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = String.format("predicate_test_%d", System.currentTimeMillis());
    spark.sql(
        String.format(
            "CREATE OR REPLACE TABLE %s (id INT, name STRING, dep_id INT) USING delta PARTITIONED BY (dep_id) LOCATION '%s'",
            tableName, path));
    Snapshot snapshot = TableManager.loadSnapshot(path).build(defaultEngine);
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    return new SparkScanBuilder(
        tableName,
        path,
        dataSchema,
        partitionSchema,
        (SnapshotImpl) snapshot,
        CaseInsensitiveStringMap.empty());
  }

  private Predicate[] getPushedKernelPredicates(SparkScanBuilder builder) throws Exception {
    Field field = SparkScanBuilder.class.getDeclaredField("pushedKernelPredicates");
    field.setAccessible(true);
    return (Predicate[]) field.get(builder);
  }

  private Filter[] getDataFilters(SparkScanBuilder builder) throws Exception {
    Field field = SparkScanBuilder.class.getDeclaredField("dataFilters");
    field.setAccessible(true);
    return (Filter[]) field.get(builder);
  }

  private boolean containsFilter(Filter[] filters, Filter target) {
    for (Filter filter : filters) {
      if (filter.equals(target)) {
        return true;
      }
    }
    return false;
  }
}
