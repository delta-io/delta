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
package io.delta.spark.internal.v2.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link DeltaV2ScanBuilder}'s construction surface: schema pruning, scan/micro-batch
 * construction. Filter pushdown itself now flows as Catalyst Expressions through {@code
 * SupportsPushDownCatalystFilters} and is exercised end-to-end by the data-skipping differential
 * suites (DataSkippingDeltaV2SnapshotSuite), which drive file selection through {@code
 * DeltaV2Snapshot.filesForScan}.
 */
public class DeltaV2ScanBuilderTest extends DeltaV2TestBase {

  @Test
  public void testBuild_returnsScanWithExpectedSchema(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "scan_builder_test";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, dep_id INT) USING delta PARTITIONED BY (dep_id) LOCATION '%s'",
            tableName, path));
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    StructType tableSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    DeltaV2ScanBuilder builder =
        newScanBuilder(tableName, path, dataSchema, partitionSchema, tableSchema);

    StructType expectedSparkSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true /*nullable*/),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });

    builder.pruneColumns(expectedSparkSchema);
    Scan scan = builder.build();

    assertTrue(scan instanceof DeltaV2Scan);
    assertEquals(expectedSparkSchema, scan.readSchema());
  }

  @Test
  public void testPruneColumns_filtersMixedCaseCDCColumn(@TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    String tableName = "scan_builder_mixed_case_cdc_test";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, dep_id INT) USING delta PARTITIONED BY (dep_id) LOCATION '%s'",
            tableName, path));
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    StructType tableSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    DeltaV2ScanBuilder builder =
        newScanBuilder(tableName, path, dataSchema, partitionSchema, tableSchema);

    StructType requiredSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("_Change_Type", DataTypes.StringType, true)
            });
    builder.pruneColumns(requiredSchema);

    StructType requiredDataSchema = getRequiredDataSchema(builder);
    for (StructField f : requiredDataSchema.fields()) {
      assertTrue(
          !f.name().equalsIgnoreCase("_change_type"),
          "Mixed-case CDC column survived pruneColumns: " + f.name());
    }
  }

  @Test
  public void testToMicroBatchStream_returnsDeltaV2MicroBatchStream(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "microbatch_test";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, dep_id INT) USING delta PARTITIONED BY (dep_id) LOCATION '%s'",
            tableName, path));
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    StructType tableSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    DeltaV2ScanBuilder builder =
        newScanBuilder(tableName, path, dataSchema, partitionSchema, tableSchema);
    Scan scan = builder.build();

    String checkpointLocation = "/tmp/checkpoint";
    MicroBatchStream microBatchStream = scan.toMicroBatchStream(checkpointLocation);

    assertNotNull(microBatchStream, "MicroBatchStream should not be null");
    assertTrue(
        microBatchStream instanceof DeltaV2MicroBatchStream,
        "MicroBatchStream should be an instance of DeltaV2MicroBatchStream");
  }

  // Filter-classification cases, one per case in the pre-catalyst-pushdown suite, with the same
  // names. Under SupportsPushDownCatalystFilters a filter is no longer translated to a data-source
  // predicate, so the old "supported"/"unsupported" axis (whether Kernel could represent the filter
  // shape) no longer affects anything: only the columns a filter references decide where it lands.
  // The cases whose names encode that axis are kept anyway, precisely to pin that they now behave
  // identically to their "supported" siblings.

  @Test
  public void testPushFilters_singleSupportedDataFilter(@TempDir File tempDir) throws Exception {
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        // partition filters
        new Filter[0],
        // data filters (also the post-scan residuals)
        new Filter[] {new EqualTo("id", 100)},
        // input
        new EqualTo("id", 100));
  }

  @Test
  public void testPushFilters_singleUnsupportedDataFilter(@TempDir File tempDir) throws Exception {
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[0],
        new Filter[] {new StringEndsWith("name", "test")},
        new StringEndsWith("name", "test"));
  }

  @Test
  public void testPushFilters_singleSupportedDataFilter_StringStartsWith(@TempDir File tempDir)
      throws Exception {
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[0],
        new Filter[] {new StringStartsWith("name", "test")},
        new StringStartsWith("name", "test"));
  }

  @Test
  public void testPushFilters_multiSupportedDataFilters(@TempDir File tempDir) throws Exception {
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[0],
        new Filter[] {new EqualTo("id", 100), new GreaterThan("id", 10)},
        new EqualTo("id", 100),
        new GreaterThan("id", 10));
  }

  @Test
  public void testPushFilters_mixedSupportedAndUnsupportedDataFilters(@TempDir File tempDir)
      throws Exception {
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[0],
        new Filter[] {new EqualTo("id", 100), new StringEndsWith("name", "test")},
        new EqualTo("id", 100),
        new StringEndsWith("name", "test"));
  }

  @Test
  public void testPushFilters_singleSupportedPartitionFilter(@TempDir File tempDir)
      throws Exception {
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[] {new EqualTo("dep_id", 1)},
        new Filter[0],
        new EqualTo("dep_id", 1));
  }

  @Test
  public void testPruneColumnsRetainsFullyPushedFilterColumn(@TempDir File tempDir)
      throws Exception {
    DeltaV2ScanBuilder builder = newFilterScanBuilder(tempDir);
    pushFilters(builder, new EqualTo("dep_id", 1));
    builder.pruneColumns(new StructType().add("id", DataTypes.IntegerType, true /* nullable */));

    Scan scan = builder.build();
    assertEquals(
        Arrays.asList("id", "dep_id", "dep_name"), Arrays.asList(scan.readSchema().fieldNames()));
  }

  @Test
  public void testPushFilters_singleUnsupportedPartitionFilter(@TempDir File tempDir)
      throws Exception {
    // A partition filter Kernel could not represent is still exact, so it is a partition filter.
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[] {new StringStartsWith("dep_name", "d")},
        new Filter[0],
        new StringStartsWith("dep_name", "d"));
  }

  @Test
  public void testPushFilters_multiSupportedPartitionFilters(@TempDir File tempDir)
      throws Exception {
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[] {new EqualTo("dep_id", 1), new GreaterThan("dep_id", 0)},
        new Filter[0],
        new EqualTo("dep_id", 1),
        new GreaterThan("dep_id", 0));
  }

  @Test
  public void testPushFilters_mixedSupportedAndUnsupportedPartitionFilters(@TempDir File tempDir)
      throws Exception {
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[] {new EqualTo("dep_id", 1), new StringStartsWith("dep_name", "d")},
        new Filter[0],
        new EqualTo("dep_id", 1),
        new StringStartsWith("dep_name", "d"));
  }

  @Test
  public void testPushFilters_mixedFilters(@TempDir File tempDir) throws Exception {
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[] {new GreaterThan("dep_id", 0), new StringEndsWith("dep_name", "x")},
        new Filter[] {new EqualTo("id", 100), new StringStartsWith("name", "test")},
        new EqualTo("id", 100),
        new StringStartsWith("name", "test"),
        new GreaterThan("dep_id", 0),
        new StringEndsWith("dep_name", "x"));
  }

  @Test
  public void testPushFilters_ORFilters(@TempDir File tempDir) throws Exception {
    // An OR cannot be split, and this one references only data columns.
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[0],
        new Filter[] {new Or(new GreaterThan("id", 10), new EqualTo("id", 100))},
        new Or(new GreaterThan("id", 10), new EqualTo("id", 100)));
  }

  @Test
  public void testPushFilters_ORSupportedAndUnsupportedDataFilters(@TempDir File tempDir)
      throws Exception {
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[0],
        new Filter[] {new Or(new StringEndsWith("name", "test"), new EqualTo("id", 100))},
        new Or(new StringEndsWith("name", "test"), new EqualTo("id", 100)));
  }

  @Test
  public void testPushFilters_ORSupportedDataAndPartitionFilters(@TempDir File tempDir)
      throws Exception {
    // Mixed OR: not exact, so it stays a residual and yields no partition filter.
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[0],
        new Filter[] {new Or(new GreaterThan("dep_id", 0), new EqualTo("id", 100))},
        new Or(new GreaterThan("dep_id", 0), new EqualTo("id", 100)));
  }

  @Test
  public void testPushFilters_mixedORandAND(@TempDir File tempDir) throws Exception {
    Filter input =
        new Or(
            new And(new StringStartsWith("dep_name", "d"), new EqualTo("id", 100)),
            new GreaterThan("dep_id", 0));
    // The whole OR is a residual, but it still yields a partition filter: extraction distributes
    // over the disjunction, dropping the data-column conjunct from the left branch to leave a
    // weaker partition-only predicate that is still safe for pruning.
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[] {new Or(new StringStartsWith("dep_name", "d"), new GreaterThan("dep_id", 0))},
        new Filter[] {input},
        input);
  }

  @Test
  public void testPushFilters_NOTFilters(@TempDir File tempDir) throws Exception {
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[] {new Not(new EqualTo("dep_id", 1))},
        new Filter[0],
        new Not(new EqualTo("dep_id", 1)));
  }

  @Test
  public void testPushFilters_NOTSupportedDataANDSupportedPartitionFilters(@TempDir File tempDir)
      throws Exception {
    Filter input = new Not(new And(new EqualTo("id", 100), new GreaterThan("dep_id", 0)));
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir), new Filter[0], new Filter[] {input}, input);
  }

  @Test
  public void testPushFilters_NOTSupportedDataANDUnsupportedDataFilters(@TempDir File tempDir)
      throws Exception {
    Filter input = new Not(new And(new EqualTo("id", 100), new StringEndsWith("name", "test")));
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir), new Filter[0], new Filter[] {input}, input);
  }

  @Test
  public void testPushFilters_NOTSupportedDataORSupportedPartitionFilters(@TempDir File tempDir)
      throws Exception {
    Filter input = new Not(new Or(new EqualTo("id", 100), new GreaterThan("dep_id", 0)));
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir), new Filter[0], new Filter[] {input}, input);
  }

  @Test
  public void testPushFilters_NOTSupportedDataORUnsupportedDataFilters(@TempDir File tempDir)
      throws Exception {
    Filter input = new Not(new Or(new EqualTo("id", 100), new StringEndsWith("name", "test")));
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir), new Filter[0], new Filter[] {input}, input);
  }

  // Cases the pre-catalyst-pushdown suite did not cover: a partition-only AND / OR is exact, so it
  // is classified as a partition filter rather than left as a residual.

  @Test
  public void testPushFilters_partitionOnlyANDFilter(@TempDir File tempDir) throws Exception {
    Filter input = new And(new EqualTo("dep_id", 1), new GreaterThan("dep_id", 0));
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir), new Filter[] {input}, new Filter[0], input);
  }

  @Test
  public void testPushFilters_partitionOnlyORFilter(@TempDir File tempDir) throws Exception {
    Filter input = new Or(new EqualTo("dep_id", 1), new GreaterThan("dep_id", 0));
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir), new Filter[] {input}, new Filter[0], input);
  }

  @Test
  public void testPushFilters_mixedANDExtractsPartitionConjunct(@TempDir File tempDir)
      throws Exception {
    // A mixed AND stays a residual in full, but DataSourceUtils also mines its partition-only
    // conjunct out for exact pruning -- the conjunct is added to the partition list without being
    // removed from the data list.
    Filter input = new And(new EqualTo("dep_id", 1), new GreaterThan("id", 10));
    checkSupportsPushDownFilters(
        newFilterScanBuilder(tempDir),
        new Filter[] {new EqualTo("dep_id", 1)},
        new Filter[] {input},
        input);
  }

  /**
   * Pushes {@code inputFilters} and asserts the resulting classification. Same table-driven role as
   * the helper of this name before the catalyst-pushdown switch; the two assertion points that
   * contract removed (the Kernel predicates and {@code kernelScanBuilder.predicate}) are gone, and
   * {@code partitionCatalystFilters} is now checked as well.
   */
  private void checkSupportsPushDownFilters(
      DeltaV2ScanBuilder builder,
      Filter[] expectedPartitionFilters,
      Filter[] expectedDataFilters,
      Filter... inputFilters)
      throws Exception {
    scala.collection.immutable.Seq<Expression> filters =
        scala.jdk.javaapi.CollectionConverters.asScala(
                Arrays.asList(toCatalystFilters(builder, inputFilters)))
            .toList();

    Object postScanFilters = builder.pushFilters(filters);

    // Compare rendered expressions with exprIds stripped: each toCatalystFilter call allocates
    // fresh exprIds, so the expectations cannot be compared to the builder's copies by equality.
    // Data filters are exactly the post-scan residuals -- min/max skipping is not row-exact.
    assertEquals(
        describe(toCatalystFilters(builder, expectedDataFilters)),
        describe(
            scala.jdk.javaapi.CollectionConverters.<Expression>asJava(
                    (scala.collection.immutable.Seq<Expression>) postScanFilters)
                .toArray(new Expression[0])));
    assertEquals(
        describeUnordered(toCatalystFilters(builder, expectedPartitionFilters)),
        describeUnordered(getCatalystFilters(builder, "partitionCatalystFilters")));
    assertEquals(
        describeUnordered(toCatalystFilters(builder, expectedDataFilters)),
        describeUnordered(getCatalystFilters(builder, "dataCatalystFilters")));
    // Nothing is reported through the data-source predicate API under the catalyst contract.
    assertEquals(0, builder.pushedFilters().length);
  }

  /** Renders expressions in order, with {@code #exprId} suffixes stripped. */
  private static java.util.List<String> describe(Expression[] expressions) {
    java.util.List<String> rendered = new java.util.ArrayList<>();
    for (Expression expression : expressions) {
      rendered.add(expression.toString().replaceAll("#\\d+", ""));
    }
    return rendered;
  }

  /** Same as {@link #describe}, sorted so ordering differences do not matter. */
  private static java.util.List<String> describeUnordered(Expression[] expressions) {
    java.util.List<String> rendered = describe(expressions);
    java.util.Collections.sort(rendered);
    return rendered;
  }

  private static Expression[] toCatalystFilters(DeltaV2ScanBuilder builder, Filter[] filters)
      throws Exception {
    Expression[] expressions = new Expression[filters.length];
    for (int i = 0; i < filters.length; i++) {
      expressions[i] = toCatalystFilter(builder, filters[i]);
    }
    return expressions;
  }

  private static Expression[] getCatalystFilters(DeltaV2ScanBuilder builder, String fieldName)
      throws Exception {
    Field field = DeltaV2ScanBuilder.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    return (Expression[]) field.get(builder);
  }

  private DeltaV2ScanBuilder newFilterScanBuilder(File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "filter_builder_" + System.nanoTime();
    // dep_name is a STRING partition column so string predicates can be applied to a partition
    // column as well as to a data column.
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, dep_id INT, dep_name STRING) USING delta "
                + "PARTITIONED BY (dep_id, dep_name) LOCATION '%s'",
            tableName, path));
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true),
              DataTypes.createStructField("dep_name", DataTypes.StringType, true)
            });
    StructType tableSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true),
              DataTypes.createStructField("dep_name", DataTypes.StringType, true)
            });
    return newScanBuilder(tableName, path, dataSchema, partitionSchema, tableSchema);
  }

  private StructType getRequiredDataSchema(DeltaV2ScanBuilder builder) throws Exception {
    Field field = DeltaV2ScanBuilder.class.getDeclaredField("requiredDataSchema");
    field.setAccessible(true);
    return (StructType) field.get(builder);
  }

  private DeltaV2ScanBuilder newScanBuilder(
      String tableName,
      String path,
      StructType dataSchema,
      StructType partitionSchema,
      StructType tableSchema) {
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf());
    Snapshot snapshot = snapshotManager.loadLatestSnapshot();
    Engine engine = DefaultEngine.create(spark.sessionState().newHadoopConf());
    return new DeltaV2ScanBuilder(
        tableName,
        snapshot,
        engine,
        Optional.empty(),
        snapshotManager,
        dataSchema,
        partitionSchema,
        tableSchema,
        Optional.empty(),
        CaseInsensitiveStringMap.empty());
  }

  // Limit Pushdown Tests
  //
  // Filter pushdown now flows as Catalyst Expressions through SupportsPushDownCatalystFilters, and
  // partition/data classification is by partition-column membership: a predicate on a partition
  // column (dep_id) is an exact partition filter that leaves no post-scan residual, while one on a
  // data column (id/name) is a data filter that does. A pushed limit is kept only when no residual
  // remains.

  @Test
  public void testPushLimit_nonNegativeAccepted(@TempDir File tempDir) {
    DeltaV2ScanBuilder builder = newFilterScanBuilder(tempDir);
    assertTrue(builder.pushLimit(10), "A non-negative limit should be accepted as a hint");
  }

  @Test
  public void testPushLimit_isPartiallyPushedIsTrue(@TempDir File tempDir) {
    DeltaV2ScanBuilder builder = newFilterScanBuilder(tempDir);
    builder.pushLimit(10);
    // The pushdown is partial because pruning stops only at file boundaries, so the selected files
    // may produce more than the requested number of rows. Keep the default true so Spark retains
    // the LIMIT and trims the final result to exactly N rows.
    assertTrue(builder.isPartiallyPushed(), "isPartiallyPushed should return true");
  }

  @Test
  public void testPushLimit_propagatedToScan(@TempDir File tempDir) {
    DeltaV2ScanBuilder builder = newFilterScanBuilder(tempDir);
    builder.pushLimit(42);
    DeltaV2Scan scan = (DeltaV2Scan) builder.build();
    assertEquals(OptionalInt.of(42), scan.getPushedLimit());
  }

  @Test
  public void testPushLimit_absentByDefault(@TempDir File tempDir) {
    DeltaV2ScanBuilder builder = newFilterScanBuilder(tempDir);
    DeltaV2Scan scan = (DeltaV2Scan) builder.build();
    assertEquals(OptionalInt.empty(), scan.getPushedLimit());
  }

  @Test
  public void testPushLimit_lastValueWins(@TempDir File tempDir) {
    DeltaV2ScanBuilder builder = newFilterScanBuilder(tempDir);
    builder.pushLimit(10);
    builder.pushLimit(20);
    assertEquals(OptionalInt.of(20), builder.getPushedLimit());
    DeltaV2Scan scan = (DeltaV2Scan) builder.build();
    assertEquals(OptionalInt.of(20), scan.getPushedLimit());
  }

  @Test
  public void testPushLimit_negativeRejected(@TempDir File tempDir) {
    DeltaV2ScanBuilder builder = newFilterScanBuilder(tempDir);
    assertThrows(
        IllegalArgumentException.class,
        () -> builder.pushLimit(-1),
        "A negative pushed limit should be rejected");
  }

  @Test
  public void testPushLimit_clearedWhenDataFiltersPresent(@TempDir File tempDir) {
    DeltaV2ScanBuilder builder = newFilterScanBuilder(tempDir);
    // A data filter (on a non-partition column) becomes a post-scan residual.
    pushFilters(builder, new GreaterThan("id", 10));
    builder.pushLimit(10);
    DeltaV2Scan scan = (DeltaV2Scan) builder.build();
    assertEquals(
        OptionalInt.empty(),
        scan.getPushedLimit(),
        "Pushed limit must be cleared when data filters would leave a post-scan residual");
  }

  @Test
  public void testPushLimit_remainsClearedAfterLaterEmptyPushFilters(@TempDir File tempDir) {
    DeltaV2ScanBuilder builder = newFilterScanBuilder(tempDir);
    pushFilters(builder, new GreaterThan("id", 10));

    // ScanBuilder state is cumulative. A later empty call cannot retract the residual Spark must
    // still evaluate from the earlier call, so it must not make limit pruning safe again.
    builder.pushFilters(
        scala.jdk.javaapi.CollectionConverters.<Expression>asScala(java.util.List.of()).toList());
    assertTrue(builder.pushLimit(10));
    assertEquals(OptionalInt.empty(), ((DeltaV2Scan) builder.build()).getPushedLimit());
  }

  @Test
  public void testPushLimit_keptWhenPartitionFilterLeavesNoResidual(@TempDir File tempDir) {
    // A partition filter (EqualTo on dep_id) is used for partition pruning and leaves NO post-scan
    // residual, so the pushed limit is safe to keep - matching Spark, which still offers pushLimit
    // in this shape.
    DeltaV2ScanBuilder builder = newFilterScanBuilder(tempDir);
    pushFilters(builder, new EqualTo("dep_id", 1));

    builder.pushLimit(10);
    DeltaV2Scan scan = (DeltaV2Scan) builder.build();
    assertEquals(
        OptionalInt.of(10),
        scan.getPushedLimit(),
        "Pushed limit must be kept when a partition filter leaves no post-scan residual");
  }
}
