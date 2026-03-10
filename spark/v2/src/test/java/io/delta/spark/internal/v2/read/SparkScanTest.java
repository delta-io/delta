package io.delta.spark.internal.v2.read;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.catalog.SparkTable;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.HasPartitionKey;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SparkScanTest extends DeltaV2TestBase {

  private static String tablePath;
  private static final String tableName = "deltatbl_partitioned";

  @BeforeAll
  public static void setupPartitionedTable(@TempDir File tempDir) {
    createPartitionedTable(tableName, tempDir.getAbsolutePath());
    tablePath = tempDir.getAbsolutePath();
  }

  private final CaseInsensitiveStringMap options =
      new CaseInsensitiveStringMap(new java.util.HashMap<>());

  private final SparkTable table =
      new SparkTable(
          Identifier.of(new String[] {"spark_catalog", "default"}, tableName), tablePath, options);

  protected static final Predicate cityPredicate =
      new Predicate(
          "=",
          new Expression[] {
            FieldReference.apply("city"), LiteralValue.apply("hz", DataTypes.StringType)
          });

  protected static final Predicate datePredicate =
      new Predicate(
          "=",
          new Expression[] {
            FieldReference.apply("date"), LiteralValue.apply("20180520", DataTypes.StringType)
          });

  protected static final Predicate partPredicate =
      new Predicate(
          ">",
          new Expression[] {
            FieldReference.apply("part"), LiteralValue.apply(1, DataTypes.IntegerType)
          });

  protected static final Predicate dataPredicate =
      new Predicate(
          ">",
          new Expression[] {
            FieldReference.apply("cnt"), LiteralValue.apply(10, DataTypes.IntegerType)
          });

  protected static final Predicate negativeCityPredicate =
      new Predicate(
          "=",
          new Expression[] {
            FieldReference.apply("city"), LiteralValue.apply("zz", DataTypes.StringType)
          });

  protected static final Predicate interColPredicate =
      new Predicate(
          "!=", new Expression[] {FieldReference.apply("city"), FieldReference.apply("date")});

  protected static final Predicate negativeInterColPredicate =
      new Predicate(
          "=", new Expression[] {FieldReference.apply("city"), FieldReference.apply("date")});

  // a full set of cities in the golden table, repsents all partitions
  protected static final List<String> allCities =
      Arrays.asList("city=hz", "city=sh", "city=bj", "city=sz");

  // ===============================================================================================
  // Tests for getDataSchema, getPartitionSchema, getReadDataSchema, getOptions, getConfiguration
  // ===============================================================================================

  @Test
  public void testGetDataSchemaPartitionSchemaReadDataSchemaOptionsConfiguration() {
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan = (SparkScan) builder.build();

    // Table schema: (part INT, date STRING, city STRING, name STRING, cnt INT)
    // Partition columns: (date STRING, city STRING, part INT)
    // Data columns: (name STRING, cnt INT)
    StructType dataSchema = scan.getDataSchema();
    StructType partitionSchema = scan.getPartitionSchema();
    StructType readDataSchema = scan.getReadDataSchema();
    CaseInsensitiveStringMap scanOptions = scan.getOptions();
    Configuration configuration = scan.getConfiguration();

    assertEquals(2, dataSchema.fields().length, "dataSchema should have 2 fields (name, cnt)");
    assertNotNull(dataSchema.fieldNames());
    assertTrue(
        Arrays.asList(dataSchema.fieldNames()).containsAll(Arrays.asList("name", "cnt")),
        "dataSchema should contain name and cnt");

    assertEquals(
        3,
        partitionSchema.fields().length,
        "partitionSchema should have 3 fields (date, city, part)");
    assertTrue(
        Arrays.asList(partitionSchema.fieldNames())
            .containsAll(Arrays.asList("date", "city", "part")),
        "partitionSchema should contain date, city, part");

    assertEquals(
        dataSchema,
        readDataSchema,
        "readDataSchema should equal dataSchema without column pruning");

    assertNotNull(scanOptions, "options should not be null");
    assertEquals(options, scanOptions, "options should match the scan options");

    assertNotNull(configuration, "configuration should not be null");
    // Verify configuration matches expected: built from same options via Spark session
    Configuration expectedConf =
        spark.sessionState().newHadoopConfWithOptions(ScalaUtils.toScalaMap(options));
    assertEquals(
        expectedConf.get("fs.defaultFS"),
        configuration.get("fs.defaultFS"),
        "fs.defaultFS should match expected");
    assertEquals(
        expectedConf.get("fs.default.name"),
        configuration.get("fs.default.name"),
        "fs.default.name should match expected");
  }

  @Test
  public void testGetConfigurationWithHadoopOptions() {
    // Pass Hadoop options and verify they appear in the returned Configuration
    Map<String, String> optionsWithHadoop = new HashMap<>();
    optionsWithHadoop.put("fs.file.impl.disable.cache", "true");
    optionsWithHadoop.put("dfs.replication", "2");
    CaseInsensitiveStringMap optionsWithHadoopMap = new CaseInsensitiveStringMap(optionsWithHadoop);

    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(optionsWithHadoopMap);
    SparkScan scan = (SparkScan) builder.build();
    Configuration configuration = scan.getConfiguration();

    assertEquals(
        "true",
        configuration.get("fs.file.impl.disable.cache"),
        "Hadoop option fs.file.impl.disable.cache should flow through to Configuration");
    assertEquals(
        "2",
        configuration.get("dfs.replication"),
        "Hadoop option dfs.replication should flow through to Configuration");
  }

  @Test
  public void testGetReadDataSchemaWithColumnPruning() {
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);

    StructType prunedSchema =
        new StructType()
            .add("name", DataTypes.StringType)
            .add("date", DataTypes.StringType)
            .add("city", DataTypes.StringType)
            .add("part", DataTypes.IntegerType);
    builder.pruneColumns(prunedSchema);

    SparkScan scan = (SparkScan) builder.build();

    StructType dataSchema = scan.getDataSchema();
    StructType readDataSchema = scan.getReadDataSchema();

    assertEquals(2, dataSchema.fields().length, "dataSchema should still have 2 fields");
    assertEquals(
        1,
        readDataSchema.fields().length,
        "readDataSchema should have 1 field (name) after pruning cnt");
    assertEquals("name", readDataSchema.fields()[0].name());
  }

  @Test
  public void testDPP_singleFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {cityPredicate}, Arrays.asList("city=hz"));

    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {datePredicate}, Arrays.asList("date=20180520"));

    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {partPredicate}, Arrays.asList("part=2"));
  }

  @Test
  public void testDPP_multiFilters() throws Exception {
    checkSupportsRuntimeFilters(
        table,
        options,
        new Predicate[] {cityPredicate, datePredicate},
        Arrays.asList("date=20180520/city=hz"));
  }

  @Test
  public void testDPP_ANDFilters() throws Exception {
    Predicate andPredicate = new Predicate("AND", new Expression[] {cityPredicate, datePredicate});
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {andPredicate}, Arrays.asList("date=20180520/city=hz"));
  }

  @Test
  public void testDPP_ORFilters() throws Exception {
    Predicate orPredicate = new Predicate("OR", new Expression[] {cityPredicate, datePredicate});
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {orPredicate}, Arrays.asList("city=hz", "date=20180520"));
  }

  @Test
  public void testDPP_NOTFilter() throws Exception {
    Predicate notPredicate = new Predicate("NOT", new Expression[] {cityPredicate});
    checkSupportsRuntimeFilters(
        table,
        options,
        new Predicate[] {notPredicate},
        Arrays.asList("city=sh", "city=bj", "city=sz"));
  }

  @Test
  public void testDPP_INFilter() throws Exception {
    Predicate inPredicate =
        new Predicate(
            "IN",
            new Expression[] {
              FieldReference.apply("city"),
              LiteralValue.apply("hz", DataTypes.StringType),
              LiteralValue.apply("sh", DataTypes.StringType)
            });
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {inPredicate}, Arrays.asList("city=hz", "city=sh"));
  }

  @Test
  public void testDPP_negativeFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {negativeCityPredicate}, Arrays.asList());
  }

  @Test
  public void testDPP_ANDNegativeFilter() throws Exception {
    Predicate andPredicate =
        new Predicate("AND", new Expression[] {cityPredicate, negativeCityPredicate});
    checkSupportsRuntimeFilters(table, options, new Predicate[] {andPredicate}, Arrays.asList());
  }

  @Test
  public void testDPP_ORNegativeFilter() throws Exception {
    Predicate orPredicate =
        new Predicate("OR", new Expression[] {cityPredicate, negativeCityPredicate});
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {orPredicate}, Arrays.asList("city=hz"));
  }

  @Test
  public void testDPP_nonPartitionColumnFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {cityPredicate, dataPredicate}, Arrays.asList("city=hz"));
  }

  @Test
  public void testDPP_nonPartitionColumnFilterOnly() throws Exception {
    checkSupportsRuntimeFilters(table, options, new Predicate[] {dataPredicate}, allCities);
  }

  @Test
  public void testDPP_ANDDataPredicate() throws Exception {
    Predicate andPredicate = new Predicate("AND", new Expression[] {cityPredicate, dataPredicate});
    checkSupportsRuntimeFilters(table, options, new Predicate[] {andPredicate}, allCities);
  }

  @Test
  public void testDPP_ORDataPredicate() throws Exception {
    Predicate orPredicate = new Predicate("OR", new Expression[] {cityPredicate, dataPredicate});
    checkSupportsRuntimeFilters(table, options, new Predicate[] {orPredicate}, allCities);
  }

  @Test
  public void testDPP_interColumnFilter() throws Exception {
    checkSupportsRuntimeFilters(table, options, new Predicate[] {interColPredicate}, allCities);
  }

  @Test
  public void testDPP_negativeInterColumnFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {negativeInterColPredicate}, Arrays.asList());
  }

  @Test
  public void testDPP_integerFilter() throws Exception {
    checkSupportsRuntimeFilters(
        table, options, new Predicate[] {partPredicate}, Arrays.asList("part=2"));
  }

  protected static void checkSupportsRuntimeFilters(
      SparkTable table,
      CaseInsensitiveStringMap scanOptions,
      org.apache.spark.sql.connector.expressions.filter.Predicate[] runtimeFilters,
      List<String> remainingPartitionValueAfterDpp)
      throws Exception {
    ScanBuilder newBuilder = table.newScanBuilder(scanOptions);
    SparkScanBuilder builder = (SparkScanBuilder) newBuilder;
    Scan scan = builder.build();
    SparkScan sparkScan = (SparkScan) scan;

    List<PartitionedFile> beforeDppFiles = getPartitionedFiles(sparkScan);
    // make a copy for comparison after DPP
    beforeDppFiles = new ArrayList<>(beforeDppFiles);
    long beforeDppTotalBytes = getTotalBytes(sparkScan);
    long beforeDppEstimatedSize = getEstimatedSizeInBytes(sparkScan);
    assert (beforeDppFiles.size() == 5);
    // Without column pruning, estimatedSizeInBytes should equal totalBytes
    assertEquals(beforeDppTotalBytes, beforeDppEstimatedSize);

    sparkScan.filter(runtimeFilters);
    List<PartitionedFile> afterDppFiles = getPartitionedFiles(sparkScan);
    long afterDppTotalBytes = getTotalBytes(sparkScan);
    long afterDppEstimatedSize = getEstimatedSizeInBytes(sparkScan);
    assert (beforeDppFiles.containsAll(afterDppFiles));
    assert (beforeDppTotalBytes >= afterDppTotalBytes);

    List<PartitionedFile> expectedPartitionFilesAfterDpp = new ArrayList<>();
    long expectedTotalBytesAfterDpp = 0;
    for (PartitionedFile pf : beforeDppFiles) {
      for (String partitionValue : remainingPartitionValueAfterDpp) {
        if (pf.filePath().toString().contains(partitionValue)) {
          expectedPartitionFilesAfterDpp.add(pf);
          expectedTotalBytesAfterDpp += pf.fileSize();
          break;
        }
      }
    }

    assertEquals(expectedPartitionFilesAfterDpp.size(), afterDppFiles.size());
    assertEquals(new HashSet<>(expectedPartitionFilesAfterDpp), new HashSet<>(afterDppFiles));
    assertEquals(expectedTotalBytesAfterDpp, afterDppTotalBytes);
    // Without column pruning, estimatedSizeInBytes should equal totalBytes after filtering too
    assertEquals(afterDppTotalBytes, afterDppEstimatedSize);
  }

  private static List<PartitionedFile> getPartitionedFiles(SparkScan scan) throws Exception {
    scan.estimateStatistics(); // ensurePlanned
    Field field = SparkScan.class.getDeclaredField("partitionedFiles");
    field.setAccessible(true);
    return (List<PartitionedFile>) field.get(scan);
  }

  private static long getTotalBytes(SparkScan scan) throws Exception {
    scan.estimateStatistics(); // ensurePlanned
    Field field = SparkScan.class.getDeclaredField("totalBytes");
    field.setAccessible(true);
    return (long) field.get(scan);
  }

  private static long getEstimatedSizeInBytes(SparkScan scan) throws Exception {
    scan.estimateStatistics(); // ensurePlanned
    Field field = SparkScan.class.getDeclaredField("estimatedSizeInBytes");
    field.setAccessible(true);
    return (long) field.get(scan);
  }

  // ================================================================================================
  // Tests for streaming options validation
  // ================================================================================================

  @Test
  public void testValidateStreamingOptions_SupportedOptions() {
    // Test with supported options (case insensitive) and custom user options
    Map<String, String> javaOptions = new HashMap<>();
    javaOptions.put("startingVersion", "0");
    javaOptions.put("MaxFilesPerTrigger", "100");
    javaOptions.put("MAXBYTESPERTRIGGER", "1g");
    javaOptions.put("myCustomOption", "value");
    scala.collection.immutable.Map<String, String> supportedOptions =
        ScalaUtils.toScalaMap(javaOptions);
    DeltaOptions deltaOptions = new DeltaOptions(supportedOptions, spark.sessionState().conf());

    // Verify DeltaOptions can recognize the options (case insensitive)
    assertEquals(true, deltaOptions.maxFilesPerTrigger().isDefined());
    assertEquals(100, deltaOptions.maxFilesPerTrigger().get());
    assertEquals(true, deltaOptions.maxBytesPerTrigger().isDefined());

    // Should not throw - supported and custom options are allowed
    SparkScan.validateStreamingOptions(deltaOptions);
  }

  @Test
  public void testValidateStreamingOptions_UnsupportedOptions() {
    // Test with blocked DeltaOptions, supported options, and custom user options
    Map<String, String> javaOptions = new HashMap<>();
    javaOptions.put("startingVersion", "0");
    javaOptions.put("readChangeFeed", "true");
    javaOptions.put("myCustomOption", "value");
    scala.collection.immutable.Map<String, String> mixedOptions =
        ScalaUtils.toScalaMap(javaOptions);
    DeltaOptions deltaOptions = new DeltaOptions(mixedOptions, spark.sessionState().conf());

    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> SparkScan.validateStreamingOptions(deltaOptions));

    // Verify exact error message - only the blocked option should appear
    // Note: DeltaOptions uses CaseInsensitiveMap which lowercases keys during iteration
    assertEquals(
        "The following streaming options are not supported: [readchangefeed]. "
            + "Supported options are: [startingVersion, startingTimestamp, maxFilesPerTrigger, maxBytesPerTrigger].",
        exception.getMessage());
  }

  // ================================================================================================
  // Tests for equals and hashCode
  // ================================================================================================

  @Test
  public void testEqualsAndHashCode() {
    // Create two scans from the same table with same options
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan1 = (SparkScan) builder1.build();

    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan2 = (SparkScan) builder2.build();

    // Same table, same options should be equal
    assertEquals(scan1, scan2);
    assertEquals(scan1.hashCode(), scan2.hashCode());
  }

  @Test
  public void testEqualsWithDifferentOptions() {
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan1 = (SparkScan) builder1.build();

    Map<String, String> differentOptions = new HashMap<>();
    differentOptions.put("customOption", "value");
    CaseInsensitiveStringMap optionsMap = new CaseInsensitiveStringMap(differentOptions);
    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(optionsMap);
    SparkScan scan2 = (SparkScan) builder2.build();

    // Different options should not be equal and hashCodes should differ
    assertNotEquals(scan1, scan2);
    assertNotEquals(scan1.hashCode(), scan2.hashCode());
  }

  @Test
  public void testEqualsWithSameFilters() {
    // Both scans with equivalent filters created separately (not same instance)
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    builder1.pushFilters(
        new org.apache.spark.sql.sources.Filter[] {
          new org.apache.spark.sql.sources.EqualTo("city", "hz")
        });
    SparkScan scan1 = (SparkScan) builder1.build();

    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    builder2.pushFilters(
        new org.apache.spark.sql.sources.Filter[] {
          new org.apache.spark.sql.sources.EqualTo("city", "hz")
        });
    SparkScan scan2 = (SparkScan) builder2.build();

    // Same options and equivalent filters should be equal
    assertEquals(scan1, scan2);
    assertEquals(scan1.hashCode(), scan2.hashCode());
  }

  @Test
  public void testEqualsWithDifferentFilters() {
    // Scan without filters
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan1 = (SparkScan) builder1.build();

    // Scan with filters pushed
    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    builder2.pushFilters(
        new org.apache.spark.sql.sources.Filter[] {
          new org.apache.spark.sql.sources.EqualTo("city", "hz")
        });
    SparkScan scan2 = (SparkScan) builder2.build();

    // Same options but different filters should not be equal and hashCodes should differ
    assertNotEquals(scan1, scan2);
    assertNotEquals(scan1.hashCode(), scan2.hashCode());
  }

  // ================================================================================================
  // Tests for estimated size with column projection
  // ================================================================================================

  @Test
  public void testEstimatedSizeMatchesStatistics() throws Exception {
    // Test that estimateStatistics().sizeInBytes() returns the estimatedSizeInBytes field
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan = (SparkScan) builder.build();

    long estimatedSizeFromStats = scan.estimateStatistics().sizeInBytes().getAsLong();
    long estimatedSizeFromField = getEstimatedSizeInBytes(scan);

    assertEquals(estimatedSizeFromField, estimatedSizeFromStats);
  }

  @Test
  public void testEstimatedSizeWithColumnPruning() throws Exception {
    // Test that with column pruning, estimatedSizeInBytes is computed correctly
    // Table schema: (part INT, date STRING, city STRING, name STRING, cnt INT)
    // Partition columns: (date STRING, city STRING, part INT)
    // Data columns: (name STRING, cnt INT)
    //
    // Formula: estimatedBytes = (totalBytes * outputRowSize) / fullSchemaRowSize
    // Where:
    //   ROW_OVERHEAD = 8
    //   dataSchema.defaultSize() = 20 (STRING) + 4 (INT) = 24
    //   partitionSchema.defaultSize() = 20 + 20 + 4 = 44
    //   fullSchemaRowSize = 8 + 24 + 44 = 76
    //
    // With pruning to only 'name' column:
    //   readDataSchema.defaultSize() = 20 (STRING only)
    //   readSchema().defaultSize() = 20 + 44 = 64
    //   outputRowSize = 8 + 64 = 72
    //   estimatedBytes = (totalBytes * 72) / 76
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);

    // Prune columns to only include 'name' (a data column) and partition columns
    // This simulates: SELECT name, date, city, part FROM table
    StructType prunedSchema =
        new StructType()
            .add("name", DataTypes.StringType) // only one data column
            .add("date", DataTypes.StringType) // partition columns are always included
            .add("city", DataTypes.StringType)
            .add("part", DataTypes.IntegerType);
    builder.pruneColumns(prunedSchema);

    SparkScan scan = (SparkScan) builder.build();

    long totalBytes = getTotalBytes(scan);
    long estimatedSize = getEstimatedSizeInBytes(scan);

    // Calculate expected estimated size using the formula
    // outputRowSize = 8 + 64 = 72, fullSchemaRowSize = 8 + 24 + 44 = 76
    // Note: We don't use Math.max(1, ...) here because totalBytes is guaranteed to be large enough
    // (parquet files with actual data) that the division result won't be zero.
    long expectedEstimatedSize = (totalBytes * 72) / 76;

    assertTrue(totalBytes > 0, "totalBytes should be positive");
    assertEquals(
        expectedEstimatedSize,
        estimatedSize,
        String.format(
            "estimatedSize should be (totalBytes * 72) / 76 = (%d * 72) / 76 = %d",
            totalBytes, expectedEstimatedSize));
  }

  @Test
  public void testEstimatedSizeWithColumnPruningAndFiltering() throws Exception {
    // Test that column pruning and runtime filtering work together correctly
    // Using same formula as testEstimatedSizeWithColumnPruning:
    //   estimatedBytes = (totalBytes * 72) / 76
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);

    // Prune columns to only include 'name' column
    StructType prunedSchema =
        new StructType()
            .add("name", DataTypes.StringType)
            .add("date", DataTypes.StringType)
            .add("city", DataTypes.StringType)
            .add("part", DataTypes.IntegerType);
    builder.pruneColumns(prunedSchema);

    SparkScan scan = (SparkScan) builder.build();

    // Get initial stats with column pruning
    long initialTotalBytes = getTotalBytes(scan);
    long initialEstimatedSize = getEstimatedSizeInBytes(scan);

    // Verify initial estimated size matches formula
    // Note: No Math.max(1, ...) needed - totalBytes from parquet files is large enough
    long expectedInitialEstimated = (initialTotalBytes * 72) / 76;
    assertEquals(
        expectedInitialEstimated,
        initialEstimatedSize,
        "Initial estimatedSize should match formula");

    // Apply a runtime filter
    scan.filter(new Predicate[] {cityPredicate}); // city=hz

    // After filtering, verify both values are updated correctly
    long afterFilterTotalBytes = getTotalBytes(scan);
    long afterFilterEstimatedSize = getEstimatedSizeInBytes(scan);

    // Verify estimated size matches formula with new totalBytes
    long expectedAfterFilterEstimated = (afterFilterTotalBytes * 72) / 76;
    assertEquals(
        expectedAfterFilterEstimated,
        afterFilterEstimatedSize,
        "After filter, estimatedSize should match formula with new totalBytes");

    // Verify both values were reduced
    assertTrue(afterFilterTotalBytes < initialTotalBytes, "totalBytes should be reduced");
    assertTrue(afterFilterEstimatedSize < initialEstimatedSize, "estimatedSize should be reduced");
  }

  @Test
  public void testEstimatedSizeZeroAfterFilteringOutAllFiles() throws Exception {
    // Test that filtering out all files results in zero for both sizes
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan = (SparkScan) builder.build();

    // Apply filter that matches nothing
    scan.filter(new Predicate[] {negativeCityPredicate}); // city=zz doesn't exist

    long afterFilterTotalBytes = getTotalBytes(scan);
    long afterFilterEstimatedSize = getEstimatedSizeInBytes(scan);

    assertEquals(0, afterFilterTotalBytes, "totalBytes should be 0 after filtering out all files");
    assertEquals(
        0, afterFilterEstimatedSize, "estimatedSize should be 0 after filtering out all files");
    assertEquals(
        0,
        scan.estimateStatistics().sizeInBytes().getAsLong(),
        "Statistics sizeInBytes should be 0 after filtering out all files");
  }

  // ================================================================================================
  // Tests for equals and hashCode with runtime filters
  // ================================================================================================

  @Test
  public void testEqualsAndHashCodeWithSameRuntimeFilter() {
    // Same filter applied to both scans (same instance)
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan1 = (SparkScan) builder1.build();
    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan2 = (SparkScan) builder2.build();

    scan1.filter(new Predicate[] {cityPredicate});
    scan2.filter(new Predicate[] {cityPredicate});

    assertEquals(scan1, scan2);
    assertEquals(scan1.hashCode(), scan2.hashCode());
  }

  @Test
  public void testEqualsAndHashCodeWithEquivalentRuntimeFilters() {
    // Equivalent filters (different instances)
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan1 = (SparkScan) builder1.build();
    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan2 = (SparkScan) builder2.build();

    scan1.filter(new Predicate[] {cityPredicate});

    Predicate cityPredicateCopy =
        new Predicate(
            "=",
            new Expression[] {
              FieldReference.apply("city"), LiteralValue.apply("hz", DataTypes.StringType)
            });
    scan2.filter(new Predicate[] {cityPredicateCopy});

    assertEquals(scan1, scan2);
    assertEquals(scan1.hashCode(), scan2.hashCode());
  }

  @Test
  public void testEqualsAndHashCodeWithMultipleRuntimeFiltersInSameOrder() {
    // Multiple filters in same order
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan1 = (SparkScan) builder1.build();
    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan2 = (SparkScan) builder2.build();

    scan1.filter(new Predicate[] {cityPredicate, datePredicate});
    scan2.filter(new Predicate[] {cityPredicate, datePredicate});

    assertEquals(scan1, scan2);
    assertEquals(scan1.hashCode(), scan2.hashCode());
  }

  @Test
  public void testEqualsAndHashCodeWithIdempotentRuntimeFilters() {
    // Filter idempotency - applying same filter once vs twice
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan1 = (SparkScan) builder1.build();
    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan2 = (SparkScan) builder2.build();

    scan1.filter(new Predicate[] {cityPredicate});
    scan2.filter(new Predicate[] {cityPredicate});
    scan2.filter(new Predicate[] {cityPredicate}); // Apply same filter twice

    assertEquals(scan1, scan2);
    assertEquals(scan1.hashCode(), scan2.hashCode());
  }

  @Test
  public void testEqualsAndHashCodeWithSeparateRuntimeFilterCalls() {
    // Multiple separate filter() calls vs single call with multiple filters
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan1 = (SparkScan) builder1.build();
    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan2 = (SparkScan) builder2.build();

    scan1.filter(new Predicate[] {cityPredicate});
    scan1.filter(new Predicate[] {datePredicate});
    scan2.filter(new Predicate[] {cityPredicate, datePredicate});

    assertEquals(scan1, scan2);
    assertEquals(scan1.hashCode(), scan2.hashCode());
  }

  @Test
  public void testEqualsAndHashCodeWithRuntimeFiltersInDifferentOrder() {
    // Same filters in different order (order-independent)
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan1 = (SparkScan) builder1.build();
    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan2 = (SparkScan) builder2.build();

    scan1.filter(new Predicate[] {cityPredicate, datePredicate});
    scan2.filter(new Predicate[] {datePredicate, cityPredicate});

    assertEquals(scan1, scan2);
    assertEquals(scan1.hashCode(), scan2.hashCode());
  }

  @Test
  public void testEqualsAndHashCodeWithNonPartitionColumnRuntimeFilters() {
    // Non-partition column predicates should not affect equality
    // Only partition column predicates should be tracked
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan1 = (SparkScan) builder1.build();
    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan2 = (SparkScan) builder2.build();

    // cityPredicate is on partition column, dataPredicate is on non-partition column (cnt)
    scan1.filter(new Predicate[] {cityPredicate});
    scan2.filter(new Predicate[] {cityPredicate, dataPredicate});

    // They should be equal because dataPredicate doesn't produce an evaluator
    assertEquals(scan1, scan2);
    assertEquals(scan1.hashCode(), scan2.hashCode());
  }

  @Test
  public void testNotEqualsWithDifferentRuntimeFilters() {
    // Different filters
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan1 = (SparkScan) builder1.build();
    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan2 = (SparkScan) builder2.build();

    scan1.filter(new Predicate[] {cityPredicate});
    scan2.filter(new Predicate[] {datePredicate});

    assertNotEquals(scan1, scan2);
    assertNotEquals(scan1.hashCode(), scan2.hashCode());
  }

  @Test
  public void testNotEqualsWithAndWithoutRuntimeFilter() {
    // One with filter, one without
    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan1 = (SparkScan) builder1.build();
    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan2 = (SparkScan) builder2.build();

    scan2.filter(new Predicate[] {cityPredicate});

    assertNotEquals(scan1, scan2);
    assertNotEquals(scan1.hashCode(), scan2.hashCode());
  }

  // ================================================================================================
  // Tests for output partitioning (SupportsReportPartitioning)
  // ================================================================================================

  @Test
  public void testOutputPartitioningForPartitionedTable() {
    // Partitioned table should return KeyGroupedPartitioning
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan = (SparkScan) builder.build();

    Partitioning partitioning = scan.outputPartitioning();

    assertTrue(
        partitioning instanceof KeyGroupedPartitioning,
        "Partitioned table should return KeyGroupedPartitioning");

    KeyGroupedPartitioning kgp = (KeyGroupedPartitioning) partitioning;

    // The partitioned table has 3 partition columns: date, city, part
    Expression[] keys = kgp.keys();
    assertEquals(3, keys.length, "Should have 3 partition key expressions");

    // Verify partition key names match partition schema
    Set<String> keyNames = new HashSet<>();
    for (Expression key : keys) {
      assertTrue(key instanceof FieldReference, "Key should be a FieldReference");
      keyNames.add(((FieldReference) key).fieldNames()[0]);
    }
    assertTrue(keyNames.containsAll(Arrays.asList("date", "city", "part")));

    // numPartitions returns partitionedFiles.size() (file count, not unique partition count).
    // In this test data, each partition has one file, so file count equals partition count.
    assertEquals(5, kgp.numPartitions(), "Should have 5 files (one per partition)");
  }

  /** Creates a non-partitioned table with sample data and returns a SparkScan for it. */
  private SparkScan createNonPartitionedScan(File tempDir, String tableName) {
    spark.sql(
        String.format(
            "CREATE TABLE `%s` (id INT, name STRING) USING delta LOCATION '%s'",
            tableName, tempDir.getAbsolutePath()));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob')", tableName));

    SparkTable nonPartTable =
        new SparkTable(
            Identifier.of(new String[] {"spark_catalog", "default"}, tableName),
            tempDir.getAbsolutePath(),
            options);

    SparkScanBuilder builder = (SparkScanBuilder) nonPartTable.newScanBuilder(options);
    return (SparkScan) builder.build();
  }

  @Test
  public void testOutputPartitioningForNonPartitionedTable(@TempDir File tempDir) {
    // Non-partitioned table should return UnknownPartitioning
    SparkScan scan = createNonPartitionedScan(tempDir, "deltatbl_nonpartitioned");

    Partitioning partitioning = scan.outputPartitioning();

    assertTrue(
        partitioning instanceof UnknownPartitioning,
        "Non-partitioned table should return UnknownPartitioning");
  }

  @Test
  public void testOutputPartitioningAfterRuntimeFilter() {
    // Output partitioning should reflect filtered partition count
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan = (SparkScan) builder.build();

    // Apply filter to only keep city=hz (2 rows: part=1/date=20180520 and part=1/date=20180718)
    scan.filter(new Predicate[] {cityPredicate});

    Partitioning partitioning = scan.outputPartitioning();
    assertTrue(partitioning instanceof KeyGroupedPartitioning);

    KeyGroupedPartitioning kgp = (KeyGroupedPartitioning) partitioning;
    // numPartitions returns partitionedFiles.size() (file count); here each partition has one file.
    assertEquals(
        2,
        kgp.numPartitions(),
        "After filtering to city=hz, should have 2 files (one per partition)");
  }

  // ================================================================================================
  // Tests for DeltaInputPartition in planInputPartitions
  // ================================================================================================

  @Test
  public void testPlanInputPartitionsReturnsHasPartitionKeyForPartitionedTable() {
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan = (SparkScan) builder.build();
    Batch batch = scan.toBatch();

    InputPartition[] partitions = batch.planInputPartitions();

    assertTrue(partitions.length > 0, "Should have at least one partition");
    for (InputPartition partition : partitions) {
      assertTrue(
          partition instanceof DeltaInputPartition,
          "Partitioned table should return DeltaInputPartition instances");
      assertTrue(
          partition instanceof HasPartitionKey,
          "DeltaInputPartition should implement HasPartitionKey");

      DeltaInputPartition deltaPartition = (DeltaInputPartition) partition;
      assertNotNull(deltaPartition.partitionKey(), "Partition key should not be null");
      assertNotNull(deltaPartition.getFilePartition(), "FilePartition should not be null");
    }
  }

  @Test
  public void testPlanInputPartitionsGroupsFilesByPartition(@TempDir File tempDir) {
    // Create a table with multiple files per partition to actually exercise the grouping logic
    // in planPartitionedInputPartitions (the default test table has 1 file per partition,
    // which would pass even without grouping).
    String multiFileTableName = "deltatbl_multifile_partitioned";
    spark.sql(
        String.format(
            "CREATE TABLE `%s` (id INT, data STRING, part INT) USING delta "
                + "LOCATION '%s' PARTITIONED BY (part)",
            multiFileTableName, tempDir.getAbsolutePath()));
    // Insert in separate statements to create multiple files per partition
    spark.sql(
        String.format("INSERT INTO `%s` VALUES (1, 'a', 1), (2, 'b', 2)", multiFileTableName));
    spark.sql(
        String.format("INSERT INTO `%s` VALUES (3, 'c', 1), (4, 'd', 2)", multiFileTableName));
    spark.sql(String.format("INSERT INTO `%s` VALUES (5, 'e', 1)", multiFileTableName));
    // Now part=1 has 3 files, part=2 has 2 files

    SparkTable multiFileTable =
        new SparkTable(
            Identifier.of(new String[] {"spark_catalog", "default"}, multiFileTableName),
            tempDir.getAbsolutePath(),
            options);

    SparkScanBuilder builder = (SparkScanBuilder) multiFileTable.newScanBuilder(options);
    SparkScan scan = (SparkScan) builder.build();
    Batch batch = scan.toBatch();

    InputPartition[] partitions = batch.planInputPartitions();

    // Verify all partitions are DeltaInputPartition with partition keys
    Map<InternalRow, List<DeltaInputPartition>> partitionsByKey = new HashMap<>();
    for (InputPartition p : partitions) {
      assertTrue(p instanceof DeltaInputPartition);
      DeltaInputPartition dp = (DeltaInputPartition) p;
      partitionsByKey.computeIfAbsent(dp.partitionKey(), k -> new ArrayList<>()).add(dp);
    }

    // Should have exactly 2 unique partition keys (part=1 and part=2)
    assertEquals(2, partitionsByKey.size(), "Should have 2 unique partition keys");

    // Verify that the grouping actually produced multiple DeltaInputPartitions for a single
    // partition key (since multiple files exist per partition and each gets its own
    // FilePartition at default maxSplitBytes)
    int totalPartitions = partitions.length;
    assertTrue(
        totalPartitions > 2,
        "With 5 files across 2 partitions, should have more than 2 input partitions, "
            + "got "
            + totalPartitions);

    // Verify all DeltaInputPartitions with the same key share the same partition key instance
    for (Map.Entry<InternalRow, List<DeltaInputPartition>> entry : partitionsByKey.entrySet()) {
      List<DeltaInputPartition> group = entry.getValue();
      InternalRow expectedKey = group.get(0).partitionKey();
      for (DeltaInputPartition dp : group) {
        assertEquals(
            expectedKey,
            dp.partitionKey(),
            "All partitions in the same group should have equal partition keys");
      }
    }
  }

  @Test
  public void testPlanInputPartitionsReturnsFilePartitionForNonPartitionedTable(
      @TempDir File tempDir) {
    SparkScan scan = createNonPartitionedScan(tempDir, "deltatbl_nonpartitioned_batch");
    Batch batch = scan.toBatch();

    InputPartition[] partitions = batch.planInputPartitions();

    assertTrue(partitions.length > 0, "Should have at least one partition");
    for (InputPartition partition : partitions) {
      assertTrue(
          partition instanceof FilePartition,
          "Non-partitioned table should return FilePartition instances, not DeltaInputPartition");
      assertFalse(
          partition instanceof DeltaInputPartition,
          "Non-partitioned table should NOT return DeltaInputPartition");
    }
  }

  @Test
  public void testOutputPartitioningForEmptyPartitionedTable(@TempDir File tempDir) {
    // Empty partitioned table should return KeyGroupedPartitioning with 0 partitions
    String emptyTableName = "deltatbl_empty_partitioned";
    spark.sql(
        String.format(
            "CREATE TABLE `%s` (id INT, name STRING, part INT) USING delta "
                + "LOCATION '%s' PARTITIONED BY (part)",
            emptyTableName, tempDir.getAbsolutePath()));

    SparkTable emptyTable =
        new SparkTable(
            Identifier.of(new String[] {"spark_catalog", "default"}, emptyTableName),
            tempDir.getAbsolutePath(),
            options);

    SparkScanBuilder builder = (SparkScanBuilder) emptyTable.newScanBuilder(options);
    SparkScan scan = (SparkScan) builder.build();

    Partitioning partitioning = scan.outputPartitioning();
    assertTrue(
        partitioning instanceof KeyGroupedPartitioning,
        "Empty partitioned table should still return KeyGroupedPartitioning");

    KeyGroupedPartitioning kgp = (KeyGroupedPartitioning) partitioning;
    assertEquals(0, kgp.numPartitions(), "Empty table should have 0 partitions");

    Batch batch = scan.toBatch();
    InputPartition[] partitions = batch.planInputPartitions();
    assertEquals(0, partitions.length, "Empty table should return 0 input partitions");
  }
}
