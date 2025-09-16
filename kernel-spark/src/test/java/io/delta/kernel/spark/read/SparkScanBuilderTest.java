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
import java.util.Optional;
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

    // check postScanFilters: data filters need post-scan evaluation
    assertEquals(1, postScanFilters.length);
    assertArrayEquals(filters, postScanFilters);

    // check pushed filters
    assertArrayEquals(filters, builder.pushedFilters());

    // check pushedKernelPredicates
    Predicate[] pushedKernelPredicates = getPushedKernelPredicates(builder);
    assertEquals(1, pushedKernelPredicates.length);
    assertEquals("=", pushedKernelPredicates[0].getName());

    // check dataFilters
    Filter[] dataFilters = getDataFilters(builder);
    assertEquals(1, dataFilters.length);
    assertArrayEquals(filters, dataFilters);

    // check kernelScanBuilder's pushed predicates
    Optional<Predicate> predicateOpt = getKernelScanBuilderPredicate(builder);
    assert predicateOpt.isPresent();
    assertEquals("=", predicateOpt.get().getName());
  }

  @Test
  public void testPushFilters_singleUnsupportedDataFilter(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    Filter[] filters = {new StringStartsWith("name", "test")};
    Filter[] postScanFilters = builder.pushFilters(filters);

    // check postScanFilters: unsupported filters need post-scan evaluation
    assertEquals(1, postScanFilters.length);
    assertArrayEquals(filters, postScanFilters);

    // check pushed filters
    assertEquals(0, builder.pushedFilters().length);

    // check pushedKernelPredicates: unsupported filters are not pushed
    Predicate[] pushedPredicates = getPushedKernelPredicates(builder);
    assertEquals(0, pushedPredicates.length);

    // check dataFilters
    Filter[] dataFilters = getDataFilters(builder);
    assertEquals(1, dataFilters.length);
    assertArrayEquals(filters, dataFilters);

    // check kernelScanBuilder's pushed predicates
    Optional<Predicate> predicateOpt = getKernelScanBuilderPredicate(builder);
    assertFalse(predicateOpt.isPresent());
  }

  @Test
  public void testPushFilters_mixedSupportedAndUnsupportedDataFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    Filter[] filters = {
      new EqualTo("id", 100), // supported
      new StringStartsWith("name", "test") // unsupported
    };
    Filter[] postScanFilters = builder.pushFilters(filters);

    // check postScanFilters: data filters need post-scan evaluation
    assertEquals(2, postScanFilters.length);
    assertArrayEquals(filters, postScanFilters);

    // check pushed filters
    Filter[] expectedPushed = {filters[0]};
    assertArrayEquals(expectedPushed, builder.pushedFilters());

    // check pushedKernelPredicates
    Predicate[] pushedPredicates = getPushedKernelPredicates(builder);
    assertEquals(1, pushedPredicates.length);
    assertEquals("=", pushedPredicates[0].getName());

    // check dataFilters
    Filter[] dataFilters = getDataFilters(builder);
    assertEquals(2, dataFilters.length);
    assertTrue(containsFilter(dataFilters, filters[0]));
    assertTrue(containsFilter(dataFilters, filters[1]));

    // check kernelScanBuilder's pushed predicates
    Optional<Predicate> predicateOpt = getKernelScanBuilderPredicate(builder);
    assert predicateOpt.isPresent();
    assertEquals("=", predicateOpt.get().getName());
  }

  @Test
  public void testPushFilters_singleSupportedPartitionFilter(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    // single supported partition filter on dep_id (partition column)
    Filter[] filters = {new EqualTo("dep_id", 1)};
    Filter[] postScanFilters = builder.pushFilters(filters);

    // partition filters are evaluated pre-scan, so no post-scan evaluation needed
    assertEquals(0, postScanFilters.length);

    // supported partition filters are pushed
    assertArrayEquals(filters, builder.pushedFilters());

    // pushed kernel predicates should include the partition predicate
    Predicate[] pushedPredicates = getPushedKernelPredicates(builder);
    assertEquals(1, pushedPredicates.length);
    assertEquals("=", pushedPredicates[0].getName());

    // no data filters should be recorded
    Filter[] dataFilters = getDataFilters(builder);
    assertEquals(0, dataFilters.length);

    // kernelScanBuilder should carry the pushed predicate
    Optional<Predicate> predicateOpt = getKernelScanBuilderPredicate(builder);
    assertTrue(predicateOpt.isPresent());
    assertEquals("=", predicateOpt.get().getName());
  }

  @Test
  public void testPushFilters_singleUnsupportedPartitionFilter(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    // unsupported partition filter
    Filter[] filters = {new StringStartsWith("dep_id", "1")};
    Filter[] postScanFilters = builder.pushFilters(filters);

    // unsupported partition filters should be left for Spark to evaluate post-scan
    assertEquals(1, postScanFilters.length);
    assertArrayEquals(filters, postScanFilters);

    // nothing should be pushed
    assertEquals(0, builder.pushedFilters().length);

    // no kernel predicates pushed
    Predicate[] pushedPredicates = getPushedKernelPredicates(builder);
    assertEquals(0, pushedPredicates.length);

    // still no data filters recorded
    Filter[] dataFilters = getDataFilters(builder);
    assertEquals(0, dataFilters.length);

    // kernelScanBuilder should not have a predicate
    Optional<Predicate> predicateOpt = getKernelScanBuilderPredicate(builder);
    assertFalse(predicateOpt.isPresent());
  }

  @Test
  public void testPushFilters_mixedSupportedAndUnsupportedPartitionFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    Filter[] filters = {
      new EqualTo("dep_id", 1), // supported partition filter
      new StringStartsWith("dep_id", "1") // unsupported partition filter
    };
    Filter[] postScanFilters = builder.pushFilters(filters);

    // only the unsupported partition filter requires post-scan evaluation
    assertEquals(1, postScanFilters.length);
    assertTrue(containsFilter(postScanFilters, filters[1]));

    // only the supported partition filter is pushed
    Filter[] expectedPushed = {filters[0]};
    assertArrayEquals(expectedPushed, builder.pushedFilters());

    // kernel should have a single pushed predicate from the supported filter
    Predicate[] pushedPredicates = getPushedKernelPredicates(builder);
    assertEquals(1, pushedPredicates.length);
    assertEquals("=", pushedPredicates[0].getName());

    // no data filters here
    Filter[] dataFilters = getDataFilters(builder);
    assertEquals(0, dataFilters.length);

    // kernelScanBuilder should have a predicate present
    Optional<Predicate> predicateOpt = getKernelScanBuilderPredicate(builder);
    assertTrue(predicateOpt.isPresent());
    assertEquals("=", predicateOpt.get().getName());
  }

  @Test
  public void testPushFilters_mixedFilters(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    Filter[] filters = {
      new EqualTo("id", 100), // data filter, supported
      new StringStartsWith("name", "foo"), // data filter, unsupported
      new GreaterThan("dep_id", 1), // partition filter, supported
      new StringEndsWith("dep_id", "1") // partition filter, unsupported
    };

    Filter[] postScanFilters = builder.pushFilters(filters);

    // post-scan filters should include: all data filters (supported & unsupported)
    // plus the unsupported partition filter
    assertEquals(3, postScanFilters.length);
    assertTrue(containsFilter(postScanFilters, filters[0]));
    assertTrue(containsFilter(postScanFilters, filters[1]));
    assertTrue(containsFilter(postScanFilters, filters[3]));

    // pushed filters should include the supported data filter and the supported partition filter
    Filter[] expectedPushed = {filters[0], filters[2]};
    assertArrayEquals(expectedPushed, builder.pushedFilters());

    // pushed kernel predicates should correspond to the two supported filters
    Predicate[] pushedPredicates = getPushedKernelPredicates(builder);
    assertEquals(2, pushedPredicates.length);
    assertEquals("=", pushedPredicates[0].getName());
    assertEquals(">", pushedPredicates[1].getName());

    // dataFilters recorded on the builder should include ONLY data filters
    Filter[] dataFilters = getDataFilters(builder);
    assertEquals(2, dataFilters.length);
    assertTrue(containsFilter(dataFilters, filters[0]));
    assertTrue(containsFilter(dataFilters, filters[1]));

    // kernelScanBuilder should have a combined predicate (AND) of the two supported filters.
    // We don't assert the top-level predicate name to avoid coupling; verify its children.
    Optional<Predicate> predicateOpt = getKernelScanBuilderPredicate(builder);
    assertTrue(predicateOpt.isPresent());
    Predicate combined = predicateOpt.get();
    assertEquals("AND", combined.getName());
    assertEquals(2, combined.getChildren().size());
    assertTrue(combined.getChildren().get(0) instanceof io.delta.kernel.expressions.Predicate);
    assertTrue(combined.getChildren().get(1) instanceof io.delta.kernel.expressions.Predicate);
    assertEquals(
        "=", ((io.delta.kernel.expressions.Predicate) combined.getChildren().get(0)).getName());
    assertEquals(
        ">", ((io.delta.kernel.expressions.Predicate) combined.getChildren().get(1)).getName());
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

  private Optional<Predicate> getKernelScanBuilderPredicate(SparkScanBuilder builder)
      throws Exception {
    Field field = SparkScanBuilder.class.getDeclaredField("kernelScanBuilder");
    field.setAccessible(true);
    Object kernelScanBuilder = field.get(builder);
    Field predicateField = kernelScanBuilder.getClass().getDeclaredField("predicate");
    predicateField.setAccessible(true);
    return (Optional<Predicate>) predicateField.get(kernelScanBuilder);
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
