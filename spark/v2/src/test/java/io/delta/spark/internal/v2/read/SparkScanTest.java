package io.delta.spark.internal.v2.read;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.catalog.SparkTable;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat;
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.delta.DeltaOptions;
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
  // Tests for columnarSupportMode
  // ===============================================================================================

  @Test
  public void testColumnarSupportModeReturnsSupported() {
    // Table schema uses simple types (INT, STRING) which are batch-read-compatible
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan = (SparkScan) builder.build();

    assertEquals(
        Scan.ColumnarSupportMode.SUPPORTED,
        scan.columnarSupportMode(),
        "columnarSupportMode should return SUPPORTED for batch-compatible schema");
  }

  @Test
  public void testColumnarSupportModeDoesNotTriggerPlanning() throws Exception {
    // Calling columnarSupportMode() must NOT trigger file planning (the whole point of the
    // override is to avoid the early planInputPartitions() call that PARTITION_DEFINED causes).
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan = (SparkScan) builder.build();

    // Call columnarSupportMode before any planning
    scan.columnarSupportMode();

    // Verify the scan has not been planned yet
    Field plannedField = SparkScan.class.getDeclaredField("planned");
    plannedField.setAccessible(true);
    assertFalse(
        (boolean) plannedField.get(scan), "columnarSupportMode() should not trigger file planning");
  }

  @Test
  public void testColumnarSupportModeWithUnsupportedSchema(@TempDir File tempDir) throws Exception {
    // Create a table with a MAP column and disable nested column vectorized reading
    // to ensure the schema is not batch-read-compatible
    String path = tempDir.getAbsolutePath();
    String mapTableName = "columnar_map_table";

    withTable(
        new String[] {mapTableName},
        () -> {
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id INT, tags MAP<STRING, STRING>) USING delta LOCATION '%s'",
                  mapTableName, path));
          spark.sql(String.format("INSERT INTO %s VALUES (1, map('k', 'v'))", mapTableName));

          withSQLConf(
              "spark.sql.parquet.enableNestedColumnVectorizedReader",
              "false",
              () -> {
                SparkTable mapTable =
                    new SparkTable(
                        Identifier.of(new String[] {"spark_catalog", "default"}, mapTableName),
                        path,
                        options);
                SparkScanBuilder builder = (SparkScanBuilder) mapTable.newScanBuilder(options);
                SparkScan scan = (SparkScan) builder.build();

                assertEquals(
                    Scan.ColumnarSupportMode.UNSUPPORTED,
                    scan.columnarSupportMode(),
                    "columnarSupportMode should return UNSUPPORTED for schema with MAP type"
                        + " when nested column vectorized reader is disabled");
              });
        });
  }

  @Test
  public void testColumnarSupportModeWithVectorizedReaderDisabled() throws Exception {
    // When spark.sql.parquet.enableVectorizedReader is false, columnarSupportMode should
    // return UNSUPPORTED even for batch-compatible schemas.
    withSQLConf(
        "spark.sql.parquet.enableVectorizedReader",
        "false",
        () -> {
          SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
          SparkScan scan = (SparkScan) builder.build();

          assertEquals(
              Scan.ColumnarSupportMode.UNSUPPORTED,
              scan.columnarSupportMode(),
              "columnarSupportMode should return UNSUPPORTED when vectorized reader is disabled");
        });
  }

  @Test
  public void testColumnarSupportModeWithMetadataColumnPruned() {
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
    StructType prunedSchema =
        new StructType()
            .add("name", DataTypes.StringType)
            .add("_metadata", new StructType())
            .add("date", DataTypes.StringType)
            .add("city", DataTypes.StringType)
            .add("part", DataTypes.IntegerType);
    builder.pruneColumns(prunedSchema);

    SparkScan scan = (SparkScan) builder.build();

    assertEquals(
        Scan.ColumnarSupportMode.UNSUPPORTED,
        scan.columnarSupportMode(),
        "columnarSupportMode should return UNSUPPORTED when _metadata is requested");
  }

  @Test
  public void testColumnarSupportModeWithDeletionVectors(@TempDir File tempDir) throws Exception {
    // For a DV-enabled table with a batch-compatible schema, columnarSupportMode should still
    // return SUPPORTED because the DV internal column (__delta_internal_is_row_deleted, ByteType)
    // is also batch-compatible. This verifies consistency with PartitionUtils reader factory.
    String dvPath = tempDir.getAbsolutePath();
    String dvTableName = "columnar_dv_table";

    withTable(
        new String[] {dvTableName},
        () -> {
          spark.sql(
              String.format(
                  "CREATE TABLE %s (id INT, value STRING) USING delta "
                      + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true') "
                      + "LOCATION '%s'",
                  dvTableName, dvPath));
          spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", dvTableName));

          SparkTable dvTable =
              new SparkTable(
                  Identifier.of(new String[] {"spark_catalog", "default"}, dvTableName),
                  dvPath,
                  options);
          SparkScanBuilder builder = (SparkScanBuilder) dvTable.newScanBuilder(options);
          SparkScan scan = (SparkScan) builder.build();

          // DV-enabled table with simple types should still return SUPPORTED because the
          // DV column (ByteType) is batch-compatible
          assertEquals(
              Scan.ColumnarSupportMode.SUPPORTED,
              scan.columnarSupportMode(),
              "columnarSupportMode should return SUPPORTED for DV-enabled table with"
                  + " batch-compatible schema");
        });
  }

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
  public void testGetTablePathReturnsTablePath() {
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan = (SparkScan) builder.build();

    String retrievedPath = scan.getTablePath();
    assertNotNull(retrievedPath, "getTablePath should not return null");
    // getTablePath returns file URI with trailing slash; tablePath is from tempDir
    String expectedUri = new File(tablePath).toURI().toString();
    String expectedPath = expectedUri.endsWith("/") ? expectedUri : expectedUri + "/";
    assertEquals(
        expectedPath,
        retrievedPath,
        "getTablePath should return path matching table location (with trailing slash)");
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

  private static org.apache.spark.sql.sources.Filter[] getDataFilters(SparkScan scan)
      throws Exception {
    Field field = SparkScan.class.getDeclaredField("dataFilters");
    field.setAccessible(true);
    return (org.apache.spark.sql.sources.Filter[]) field.get(scan);
  }

  private static long getTotalRows(SparkScan scan) throws Exception {
    scan.estimateStatistics(); // ensurePlanned
    Field field = SparkScan.class.getDeclaredField("totalRows");
    field.setAccessible(true);
    return (long) field.get(scan);
  }

  private static boolean isRowCountKnown(SparkScan scan) throws Exception {
    scan.estimateStatistics(); // ensurePlanned
    Field field = SparkScan.class.getDeclaredField("rowCountKnown");
    field.setAccessible(true);
    return (boolean) field.get(scan);
  }

  // ================================================================================================
  // Tests for numRows statistics
  // ================================================================================================

  @Test
  public void testNumRowsEmptyWhenStatsDisabled() throws Exception {
    // With CBO and planStats disabled (the default), numRows() should return empty even when all
    // files have stats, matching V1 behavior (LogicalRelation.computeStats()).
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan = (SparkScan) builder.build();

    assertFalse(
        isRowCountKnown(scan), "rowCountKnown should be false when CBO and planStats are disabled");
    assertFalse(
        scan.estimateStatistics().numRows().isPresent(),
        "numRows() should be empty when CBO and planStats are disabled");
  }

  @Test
  public void testNumRowsInStatistics() throws Exception {
    // Table has 5 rows inserted as 5 separate partitions (1 row each), all with stats.
    withSQLConf(
        "spark.sql.cbo.planStats.enabled",
        "true",
        () -> {
          SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
          SparkScan scan = (SparkScan) builder.build();

          assertTrue(isRowCountKnown(scan), "Row count should be known when all files have stats");
          assertEquals(5L, getTotalRows(scan), "Total rows should match the 5 inserted rows");
          assertTrue(scan.estimateStatistics().numRows().isPresent(), "numRows should be present");
          assertEquals(5L, scan.estimateStatistics().numRows().getAsLong());
        });
  }

  @Test
  public void testNumRowsAfterRuntimeFiltering() throws Exception {
    // Runtime partition filtering recomputes totalRows from the per-file counts of files that
    // survive pruning, so numRows() reflects the post-prune row count rather than the
    // pre-filter total or empty.
    withSQLConf(
        "spark.sql.cbo.planStats.enabled",
        "true",
        () -> {
          SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
          SparkScan scan = (SparkScan) builder.build();

          assertEquals(
              5L, scan.estimateStatistics().numRows().getAsLong(), "5 rows before filtering");

          // Two rows in the table have city=hz (Alice and Bob, in different date partitions).
          scan.filter(new Predicate[] {cityPredicate}); // city=hz

          assertTrue(
              scan.estimateStatistics().numRows().isPresent(),
              "numRows should remain known after runtime filtering");
          assertEquals(
              2L,
              scan.estimateStatistics().numRows().getAsLong(),
              "numRows should be recomputed to the post-prune count (2 city=hz rows)");
        });
  }

  @Test
  public void testNumRowsZeroAfterFilteringOutAllFiles() throws Exception {
    // When runtime filtering prunes every file, totalRows recomputes to 0 (still a known
    // value, not OptionalLong.empty()).
    withSQLConf(
        "spark.sql.cbo.planStats.enabled",
        "true",
        () -> {
          SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
          SparkScan scan = (SparkScan) builder.build();

          scan.filter(new Predicate[] {negativeCityPredicate}); // city=zz doesn't exist

          assertTrue(
              scan.estimateStatistics().numRows().isPresent(),
              "numRows should remain known after runtime filtering (even when all files filtered)");
          assertEquals(
              0L,
              scan.estimateStatistics().numRows().getAsLong(),
              "numRows should be 0 when all files are filtered out");
        });
  }

  @Test
  public void testNumRowsUnknownWhenSomeFilesLackStats(@TempDir File testDir) throws Exception {
    // Older Delta tables or tables written with stats collection disabled will have AddFile entries
    // without numRecords. When even one file lacks stats, rowCountKnown must be false so that
    // numRows() returns OptionalLong.empty() rather than an incorrect partial count.
    String tblName = "test_mixed_stats_numrows";
    try {
      String path = testDir.getAbsolutePath();
      // First write: stats are collected by default (numRecords is present in AddFile stats JSON).
      spark.sql(
          "CREATE TABLE " + tblName + " (id INT, city STRING) USING delta LOCATION '" + path + "'");
      spark.sql("INSERT INTO " + tblName + " VALUES (1, 'hz')");

      // Disable stats collection via session config so the second AddFile has no numRecords.
      spark.sql("SET spark.databricks.delta.stats.collect=false");
      spark.sql("INSERT INTO " + tblName + " VALUES (2, 'sh')");

      // Table now has two AddFile entries: one with stats (first insert), one without (second).
      SparkTable mixedStatsTable =
          new SparkTable(
              Identifier.of(new String[] {"spark_catalog", "default"}, tblName), path, options);

      withSQLConf(
          "spark.sql.cbo.planStats.enabled",
          "true",
          () -> {
            SparkScanBuilder builder = (SparkScanBuilder) mixedStatsTable.newScanBuilder(options);
            SparkScan scan = (SparkScan) builder.build();

            assertFalse(
                isRowCountKnown(scan), "rowCountKnown should be false when some files lack stats");
            assertFalse(
                scan.estimateStatistics().numRows().isPresent(),
                "numRows() should be OptionalLong.empty() when row count is unknown");
          });
    } finally {
      spark.sql("RESET spark.databricks.delta.stats.collect");
      spark.sql("DROP TABLE IF EXISTS " + tblName);
    }
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
    javaOptions.put("readChangeFeed", "true");
    javaOptions.put("myCustomOption", "value");
    scala.collection.immutable.Map<String, String> supportedOptions =
        ScalaUtils.toScalaMap(javaOptions);
    DeltaOptions deltaOptions = new DeltaOptions(supportedOptions, spark.sessionState().conf());

    // Verify DeltaOptions can recognize the options (case insensitive)
    assertEquals(true, deltaOptions.maxFilesPerTrigger().isDefined());
    assertEquals(100, deltaOptions.maxFilesPerTrigger().get());
    assertEquals(true, deltaOptions.maxBytesPerTrigger().isDefined());
    assertEquals(true, deltaOptions.readChangeFeed());

    // Should not throw - supported and custom options are allowed
    SparkScan.validateStreamingOptions(deltaOptions);
  }

  @Test
  public void testValidateStreamingOptions_UnsupportedOptions() {
    // Test with blocked DeltaOptions, supported options, and custom user options
    Map<String, String> javaOptions = new HashMap<>();
    javaOptions.put("startingVersion", "0");
    javaOptions.put("endingTimestamp", "2024-01-01");
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
        "The following streaming options are not supported: [endingtimestamp]. "
            + "Supported options are: [startingVersion, startingTimestamp, maxFilesPerTrigger, "
            + "maxBytesPerTrigger, ignoreFileDeletion, ignoreChanges, ignoreDeletes, "
            + "skipChangeCommits, excludeRegex, failOnDataLoss, readChangeFeed, readChangeData, "
            + "schemaTrackingLocation, schemaLocation, streamingSourceTrackingId].",
        exception.getMessage());
  }

  // ================================================================================================
  // Tests for CDC readSchema
  // ================================================================================================

  @Test
  public void testReadSchema_cdcRead_returnsTableSchemaWithCDCColumns() {
    // When readChangeFeed=true and no pruning is pushed, readSchema() returns
    // readDataSchema + partitionSchema + CDC columns. Without pruneColumns, readDataSchema
    // equals the full data schema, so all table columns appear.
    Map<String, String> cdcOptions = new HashMap<>();
    cdcOptions.put("readChangeFeed", "true");
    CaseInsensitiveStringMap cdcOptionsMap = new CaseInsensitiveStringMap(cdcOptions);

    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(cdcOptionsMap);
    SparkScan scan = (SparkScan) builder.build();

    StructType schema = scan.readSchema();

    // Table schema: name STRING, cnt INT, date STRING, city STRING, part INT (logical order)
    // CDC columns: _change_type STRING, _commit_version LONG, _commit_timestamp TIMESTAMP
    assertTrue(schema.fieldIndex("name") >= 0);
    assertTrue(schema.fieldIndex("cnt") >= 0);
    assertTrue(schema.fieldIndex("date") >= 0);
    assertTrue(schema.fieldIndex("city") >= 0);
    assertTrue(schema.fieldIndex("part") >= 0);
    assertTrue(schema.fieldIndex("_change_type") >= 0);
    assertTrue(schema.fieldIndex("_commit_version") >= 0);
    assertTrue(schema.fieldIndex("_commit_timestamp") >= 0);
    assertEquals(8, schema.fields().length);

    // CDC columns should be at the end, after table columns
    assertEquals("_change_type", schema.fields()[5].name());
    assertEquals("_commit_version", schema.fields()[6].name());
    assertEquals("_commit_timestamp", schema.fields()[7].name());
  }

  @Test
  public void testReadSchema_nonCdcRead_returnsDataAndPartitionSchema() {
    // Without readChangeFeed, readSchema() returns readDataSchema + partitionSchema.
    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(options);
    SparkScan scan = (SparkScan) builder.build();

    StructType schema = scan.readSchema();

    // Should have data columns + partition columns, no CDC columns
    assertTrue(schema.fieldIndex("name") >= 0);
    assertTrue(schema.fieldIndex("cnt") >= 0);
    assertTrue(schema.fieldIndex("date") >= 0);
    assertTrue(schema.fieldIndex("city") >= 0);
    assertTrue(schema.fieldIndex("part") >= 0);
    assertThrows(IllegalArgumentException.class, () -> schema.fieldIndex("_change_type"));
    assertThrows(IllegalArgumentException.class, () -> schema.fieldIndex("_commit_version"));
  }

  @Test
  public void testPruneColumns_cdcRead_filtersCDCColumns() {
    // pruneColumns should filter out partition and CDC columns from the data schema, since
    // partition columns are read separately and CDC columns are injected by CDCReadFunction.
    // The remaining data columns are honored — pruning a data column ("cnt") shrinks the
    // underlying parquet read.
    Map<String, String> cdcOptions = new HashMap<>();
    cdcOptions.put("readChangeFeed", "true");
    CaseInsensitiveStringMap cdcOptionsMap = new CaseInsensitiveStringMap(cdcOptions);

    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(cdcOptionsMap);
    StructType requiredSchema =
        new StructType()
            .add("name", DataTypes.StringType)
            .add("_change_type", DataTypes.StringType)
            .add("_commit_version", DataTypes.LongType)
            .add("_commit_timestamp", DataTypes.TimestampType)
            .add("date", DataTypes.StringType);
    builder.pruneColumns(requiredSchema);

    SparkScan scan = (SparkScan) builder.build();

    // readDataSchema reflects pruning: only "name" remains (cnt dropped, partition + CDC filtered)
    StructType readDataSchema = scan.getReadDataSchema();
    assertEquals(1, readDataSchema.fields().length);
    assertEquals("name", readDataSchema.fields()[0].name());

    // readSchema = readDataSchema + partition + CDC
    StructType readSchema = scan.readSchema();
    assertTrue(readSchema.fieldIndex("name") >= 0);
    assertTrue(readSchema.fieldIndex("date") >= 0);
    assertTrue(readSchema.fieldIndex("_change_type") >= 0);
    assertTrue(readSchema.fieldIndex("_commit_version") >= 0);
    assertTrue(readSchema.fieldIndex("_commit_timestamp") >= 0);
    // Pruned column "cnt" should not appear.
    assertThrows(IllegalArgumentException.class, () -> readSchema.fieldIndex("cnt"));
  }

  @Test
  public void testCdcRead_toBatchRejectsCDCOptions() {
    Map<String, String> cdcOptions = new HashMap<>();
    cdcOptions.put("readChangeFeed", "true");
    CaseInsensitiveStringMap cdcOptionsMap = new CaseInsensitiveStringMap(cdcOptions);

    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(cdcOptionsMap);
    SparkScan scan = (SparkScan) builder.build();

    // Sanity: scan still advertises the CDC schema (this is needed for the streaming path).
    StructType advertised = scan.readSchema();
    assertEquals(8, advertised.fields().length, "scan should advertise 5 base + 3 CDC columns");
    assertTrue(advertised.fieldIndex("_change_type") >= 0);

    UnsupportedOperationException e =
        assertThrows(UnsupportedOperationException.class, scan::toBatch);
    assertTrue(
        e.getMessage().contains("CDC"),
        "exception message should mention CDC; was: " + e.getMessage());
  }

  @Test
  public void testCdcRead_toBatchRejectsLegacyCDCOption() {
    Map<String, String> cdcOptions = new HashMap<>();
    cdcOptions.put("readChangeData", "true");
    CaseInsensitiveStringMap cdcOptionsMap = new CaseInsensitiveStringMap(cdcOptions);

    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(cdcOptionsMap);
    SparkScan scan = (SparkScan) builder.build();

    assertEquals(8, scan.readSchema().fields().length);
    UnsupportedOperationException e =
        assertThrows(UnsupportedOperationException.class, scan::toBatch);
    assertTrue(e.getMessage().contains("CDC"));
  }

  @Test
  public void testCdcRead_invalidBooleanThrowsOnConstruction() {
    Map<String, String> cdcOptions = new HashMap<>();
    cdcOptions.put("readChangeFeed", "yes");
    CaseInsensitiveStringMap cdcOptionsMap = new CaseInsensitiveStringMap(cdcOptions);

    assertThrows(
        IllegalArgumentException.class,
        () -> ((SparkScanBuilder) table.newScanBuilder(cdcOptionsMap)).build());
  }

  @Test
  public void testCdcRead_falseDoesNotAdvertiseCDCColumns() {
    Map<String, String> nonCdcOptions = new HashMap<>();
    nonCdcOptions.put("readChangeFeed", "false");
    CaseInsensitiveStringMap nonCdcOptionsMap = new CaseInsensitiveStringMap(nonCdcOptions);

    SparkScanBuilder builder = (SparkScanBuilder) table.newScanBuilder(nonCdcOptionsMap);
    SparkScan scan = (SparkScan) builder.build();

    StructType schema = scan.readSchema();
    assertThrows(IllegalArgumentException.class, () -> schema.fieldIndex("_change_type"));
    assertNotNull(scan.toBatch());
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

  @Test
  public void testEqualsWithPushedFiltersInDifferentOrder() {
    org.apache.spark.sql.sources.Filter cityEq =
        new org.apache.spark.sql.sources.EqualTo("city", "hz");
    org.apache.spark.sql.sources.Filter dateEq =
        new org.apache.spark.sql.sources.EqualTo("date", "20180520");

    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    builder1.pushFilters(new org.apache.spark.sql.sources.Filter[] {cityEq, dateEq});
    SparkScan scan1 = (SparkScan) builder1.build();

    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    builder2.pushFilters(new org.apache.spark.sql.sources.Filter[] {dateEq, cityEq});
    SparkScan scan2 = (SparkScan) builder2.build();

    assertEquals(scan1, scan2);
    assertEquals(scan1.hashCode(), scan2.hashCode());
  }

  @Test
  public void testEqualsWithDataFiltersInDifferentOrder() throws Exception {
    // city/date are partition columns, so the test above only exercises pushedToKernelFiltersSet.
    // name and cnt are data columns (per the partitioned table schema), so per
    // ExpressionUtils.classifyFilter these filters have isDataFilter=true and flow into
    // SparkScan.dataFilters, exercising the dataFiltersSet branch of equals/hashCode.
    org.apache.spark.sql.sources.Filter nameEq =
        new org.apache.spark.sql.sources.EqualTo("name", "x");
    org.apache.spark.sql.sources.Filter cntGt =
        new org.apache.spark.sql.sources.GreaterThan("cnt", 10);

    SparkScanBuilder builder1 = (SparkScanBuilder) table.newScanBuilder(options);
    builder1.pushFilters(new org.apache.spark.sql.sources.Filter[] {nameEq, cntGt});
    SparkScan scan1 = (SparkScan) builder1.build();

    SparkScanBuilder builder2 = (SparkScanBuilder) table.newScanBuilder(options);
    builder2.pushFilters(new org.apache.spark.sql.sources.Filter[] {cntGt, nameEq});
    SparkScan scan2 = (SparkScan) builder2.build();

    // Sanity check that dataFilters is actually populated, otherwise this test would trivially
    // pass without exercising the dataFiltersSet path it's intended to cover.
    assertEquals(2, getDataFilters(scan1).length);
    assertEquals(2, getDataFilters(scan2).length);

    assertEquals(scan1, scan2);
    assertEquals(scan1.hashCode(), scan2.hashCode());
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
  // Tests for catalog statistics propagation
  // ================================================================================================

  /**
   * Helper to inject CatalogStatistics into a catalog table via alterTableStats. This is needed
   * because ANALYZE TABLE is not supported for V2 tables in the test environment.
   */
  private CatalogTable injectCatalogStats(String tblName, CatalogStatistics stats)
      throws Exception {
    TableIdentifier tableId = new TableIdentifier(tblName);
    spark.sessionState().catalog().alterTableStats(tableId, scala.Option.apply(stats));
    return spark.sessionState().catalog().getTableMetadata(tableId);
  }

  @Test
  public void testEstimateStatisticsWithCatalogStats_cboEnabled(@TempDir File tempDir)
      throws Exception {
    String path = tempDir.getAbsolutePath();
    String tblName = "stats_cbo_enabled";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, value DOUBLE) USING delta LOCATION '%s'",
            tblName, path));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0)", tblName));

    // Inject catalog stats with column stats for "id"
    CatalogColumnStat idColStat =
        new CatalogColumnStat(
            scala.Option.apply(scala.math.BigInt.apply(2L)), // distinctCount
            scala.Option.apply("1"), // min
            scala.Option.apply("2"), // max
            scala.Option.apply(scala.math.BigInt.apply(0L)), // nullCount
            scala.Option.apply((Object) 4L), // avgLen
            scala.Option.apply((Object) 4L), // maxLen
            scala.Option.empty(), // histogram
            CatalogColumnStat.VERSION());

    CatalogStatistics catalogStats =
        new CatalogStatistics(
            scala.math.BigInt.apply(1024L),
            scala.Option.apply(scala.math.BigInt.apply(2L)),
            buildColStatsMap(new String[] {"id"}, new CatalogColumnStat[] {idColStat}));

    CatalogTable catalogTable = injectCatalogStats(tblName, catalogStats);

    withSQLConf(
        "spark.sql.cbo.enabled",
        "true",
        () -> {
          Identifier id = Identifier.of(new String[] {"default"}, tblName);
          SparkTable sparkTable = new SparkTable(id, catalogTable, Collections.emptyMap());

          SparkScanBuilder builder =
              (SparkScanBuilder)
                  sparkTable.newScanBuilder(new CaseInsensitiveStringMap(new HashMap<>()));
          SparkScan scan = (SparkScan) builder.build();
          Statistics stats = scan.estimateStatistics();

          // numRows comes from per-file (post-prune) stats
          assertTrue(stats.numRows().isPresent(), "numRows should be present with CBO enabled");
          assertEquals(2L, stats.numRows().getAsLong(), "numRows should be 2");

          // sizeInBytes should still come from planned files (more accurate)
          assertTrue(stats.sizeInBytes().isPresent(), "sizeInBytes should be present");
          assertTrue(stats.sizeInBytes().getAsLong() > 0, "sizeInBytes should be positive");

          // Should have column stats
          Map<NamedReference, ColumnStatistics> colStats = stats.columnStats();
          assertNotNull(colStats, "columnStats should not be null");
          assertFalse(colStats.isEmpty(), "columnStats should not be empty");

          // Check that column stats contain expected columns
          ColumnStatistics idStats = colStats.get(FieldReference.apply("id"));
          assertNotNull(idStats, "id column stats should be present");
          assertTrue(idStats.nullCount().isPresent(), "id nullCount should be present");
          assertTrue(idStats.distinctCount().isPresent(), "id distinctCount should be present");
          assertTrue(idStats.min().isPresent(), "id min should be present");
          assertTrue(idStats.max().isPresent(), "id max should be present");
          assertEquals(1, idStats.min().get(), "id min should be 1");
          assertEquals(2, idStats.max().get(), "id max should be 2");
        });
  }

  @Test
  public void testPerFileNumRowsPreferredOverCatalog(@TempDir File tempDir) throws Exception {
    // Verifies that per-file (post-prune) numRows is used in preference to catalog numRows.
    // numRows() reflects the row count for this scan after pruning, while catalog stats are
    // table-level and unpruned.
    String path = tempDir.getAbsolutePath();
    String tblName = "stats_per_file_wins";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tblName, path));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tblName));

    // Inject catalog stats claiming 999 rows (distinguishable from the actual per-file count of 2)
    CatalogStatistics catalogStats =
        new CatalogStatistics(
            scala.math.BigInt.apply(1024L),
            scala.Option.apply(scala.math.BigInt.apply(999L)),
            buildColStatsMap(new String[] {}, new CatalogColumnStat[] {}));
    CatalogTable catalogTable = injectCatalogStats(tblName, catalogStats);

    withSQLConf(
        "spark.sql.cbo.enabled",
        "true",
        () -> {
          Identifier id = Identifier.of(new String[] {"default"}, tblName);
          SparkTable sparkTable = new SparkTable(id, catalogTable, Collections.emptyMap());
          SparkScanBuilder builder =
              (SparkScanBuilder)
                  sparkTable.newScanBuilder(new CaseInsensitiveStringMap(new HashMap<>()));
          SparkScan scan = (SparkScan) builder.build();
          Statistics stats = scan.estimateStatistics();

          // Per-file numRows wins over catalog stats.
          assertTrue(stats.numRows().isPresent(), "numRows should be present");
          assertEquals(
              2L,
              stats.numRows().getAsLong(),
              "numRows should come from per-file (2), not catalog (999)");
        });
  }

  @Test
  public void testCatalogNumRowsFallbackWhenPerFileUnknown(@TempDir File tempDir) throws Exception {
    // When per-file numRecords is unavailable for any AddFile (rowCountKnown=false), numRows()
    // should fall back to the catalog value rather than return OptionalLong.empty().
    String path = tempDir.getAbsolutePath();
    String tblName = "stats_catalog_fallback";
    try {
      spark.sql(
          String.format(
              "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tblName, path));
      spark.sql(String.format("INSERT INTO %s VALUES (1, 'a')", tblName));

      // Disable stats collection so the next AddFile has no numRecords. With one file lacking
      // numRecords, rowCountKnown is false for the whole scan.
      spark.sql("SET spark.databricks.delta.stats.collect=false");
      spark.sql(String.format("INSERT INTO %s VALUES (2, 'b')", tblName));

      // Inject catalog stats with a distinguishable row count (777) so the fallback is observable.
      CatalogStatistics catalogStats =
          new CatalogStatistics(
              scala.math.BigInt.apply(1024L),
              scala.Option.apply(scala.math.BigInt.apply(777L)),
              buildColStatsMap(new String[] {}, new CatalogColumnStat[] {}));
      CatalogTable catalogTable = injectCatalogStats(tblName, catalogStats);

      withSQLConf(
          "spark.sql.cbo.enabled",
          "true",
          () -> {
            Identifier id = Identifier.of(new String[] {"default"}, tblName);
            SparkTable sparkTable = new SparkTable(id, catalogTable, Collections.emptyMap());
            SparkScanBuilder builder =
                (SparkScanBuilder)
                    sparkTable.newScanBuilder(new CaseInsensitiveStringMap(new HashMap<>()));
            SparkScan scan = (SparkScan) builder.build();

            assertFalse(
                isRowCountKnown(scan),
                "rowCountKnown should be false when an AddFile lacks numRecords");

            Statistics stats = scan.estimateStatistics();
            assertTrue(
                stats.numRows().isPresent(),
                "numRows should fall back to catalog when per-file unknown");
            assertEquals(
                777L,
                stats.numRows().getAsLong(),
                "numRows should come from catalog stats (777) when per-file is unknown");
          });
    } finally {
      spark.sql("RESET spark.databricks.delta.stats.collect");
      spark.sql("DROP TABLE IF EXISTS " + tblName);
    }
  }

  @Test
  public void testEstimateStatisticsWithCatalogStats_cboDisabled(@TempDir File tempDir)
      throws Exception {
    String path = tempDir.getAbsolutePath();
    String tblName = "stats_cbo_disabled";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tblName, path));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tblName));

    // Inject catalog stats
    CatalogStatistics catalogStats =
        new CatalogStatistics(
            scala.math.BigInt.apply(512L),
            scala.Option.apply(scala.math.BigInt.apply(2L)),
            scala.collection.immutable.Map$.MODULE$.empty());

    CatalogTable catalogTable = injectCatalogStats(tblName, catalogStats);

    withSQLConf(
        "spark.sql.cbo.enabled",
        "false",
        () -> {
          Identifier id = Identifier.of(new String[] {"default"}, tblName);
          SparkTable sparkTable = new SparkTable(id, catalogTable, Collections.emptyMap());

          SparkScanBuilder builder =
              (SparkScanBuilder)
                  sparkTable.newScanBuilder(new CaseInsensitiveStringMap(new HashMap<>()));
          SparkScan scan = (SparkScan) builder.build();
          Statistics stats = scan.estimateStatistics();

          // With CBO disabled, numRows should be empty (matching V1 behavior)
          assertFalse(stats.numRows().isPresent(), "numRows should be empty with CBO disabled");

          // sizeInBytes should still come from planned files
          assertTrue(stats.sizeInBytes().isPresent(), "sizeInBytes should be present");
          assertTrue(stats.sizeInBytes().getAsLong() > 0, "sizeInBytes should be positive");
        });
  }

  @Test
  public void testEstimateStatisticsWithCatalogStats_planStatsEnabled(@TempDir File tempDir)
      throws Exception {
    String path = tempDir.getAbsolutePath();
    String tblName = "stats_plan_stats_enabled";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tblName, path));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tblName));

    // Inject catalog stats with numRows
    CatalogStatistics catalogStats =
        new CatalogStatistics(
            scala.math.BigInt.apply(512L),
            scala.Option.apply(scala.math.BigInt.apply(2L)),
            scala.collection.immutable.Map$.MODULE$.empty());

    CatalogTable catalogTable = injectCatalogStats(tblName, catalogStats);

    // CBO disabled but planStatsEnabled=true should still surface catalog stats
    withSQLConf(
        "spark.sql.cbo.enabled",
        "false",
        () -> {
          withSQLConf(
              "spark.sql.cbo.planStats.enabled",
              "true",
              () -> {
                Identifier id = Identifier.of(new String[] {"default"}, tblName);
                SparkTable sparkTable = new SparkTable(id, catalogTable, Collections.emptyMap());

                SparkScanBuilder builder =
                    (SparkScanBuilder)
                        sparkTable.newScanBuilder(new CaseInsensitiveStringMap(new HashMap<>()));
                SparkScan scan = (SparkScan) builder.build();
                Statistics stats = scan.estimateStatistics();

                // With planStatsEnabled, numRows should be present (from per-file stats)
                assertTrue(
                    stats.numRows().isPresent(), "numRows should be present with planStatsEnabled");
                assertEquals(2L, stats.numRows().getAsLong(), "numRows should be 2");

                // sizeInBytes should still come from planned files
                assertTrue(stats.sizeInBytes().isPresent(), "sizeInBytes should be present");
                assertTrue(stats.sizeInBytes().getAsLong() > 0, "sizeInBytes should be positive");
              });
        });
  }

  @Test
  public void testEstimateStatisticsWithoutCatalogStats(@TempDir File tempDir) throws Exception {
    // Path-based table has no catalog stats
    String path = tempDir.getAbsolutePath();
    String tblName = "stats_no_catalog";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tblName, path));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a')", tblName));

    withSQLConf(
        "spark.sql.cbo.enabled",
        "true",
        () -> {
          // Path-based table — no catalog table, no ANALYZE TABLE stats
          Identifier id = Identifier.of(new String[] {"default"}, tblName);
          SparkTable sparkTable = new SparkTable(id, path);

          SparkScanBuilder builder =
              (SparkScanBuilder)
                  sparkTable.newScanBuilder(new CaseInsensitiveStringMap(new HashMap<>()));
          SparkScan scan = (SparkScan) builder.build();
          Statistics stats = scan.estimateStatistics();

          // Per-file Delta stats (numRecords in the transaction log) are available even without
          // catalog stats from ANALYZE TABLE, so numRows should be present.
          assertTrue(stats.numRows().isPresent(), "numRows should be present from per-file stats");
          assertEquals(
              1L, stats.numRows().getAsLong(), "numRows should match the inserted row count");
          assertTrue(stats.sizeInBytes().isPresent(), "sizeInBytes should be present");
          assertTrue(stats.sizeInBytes().getAsLong() > 0, "sizeInBytes should be positive");
        });
  }

  @Test
  public void testEstimateStatisticsWithPartitionedTableAndCatalogStats(@TempDir File tempDir)
      throws Exception {
    String path = tempDir.getAbsolutePath();
    String tblName = "stats_partitioned";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, part INT) USING delta "
                + "PARTITIONED BY (part) LOCATION '%s'",
            tblName, path));
    spark.sql(
        String.format("INSERT INTO %s VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 2)", tblName));

    // Inject catalog stats with column stats for both data and partition columns
    CatalogColumnStat idColStat =
        new CatalogColumnStat(
            scala.Option.apply(scala.math.BigInt.apply(3L)),
            scala.Option.apply("1"),
            scala.Option.apply("3"),
            scala.Option.apply(scala.math.BigInt.apply(0L)),
            scala.Option.empty(),
            scala.Option.empty(),
            scala.Option.empty(),
            CatalogColumnStat.VERSION());

    CatalogColumnStat partColStat =
        new CatalogColumnStat(
            scala.Option.apply(scala.math.BigInt.apply(2L)),
            scala.Option.apply("1"),
            scala.Option.apply("2"),
            scala.Option.apply(scala.math.BigInt.apply(0L)),
            scala.Option.empty(),
            scala.Option.empty(),
            scala.Option.empty(),
            CatalogColumnStat.VERSION());

    CatalogStatistics catalogStats =
        new CatalogStatistics(
            scala.math.BigInt.apply(2048L),
            scala.Option.apply(scala.math.BigInt.apply(3L)),
            buildColStatsMap(
                new String[] {"id", "part"}, new CatalogColumnStat[] {idColStat, partColStat}));

    CatalogTable catalogTable = injectCatalogStats(tblName, catalogStats);

    withSQLConf(
        "spark.sql.cbo.enabled",
        "true",
        () -> {
          Identifier id = Identifier.of(new String[] {"default"}, tblName);
          SparkTable sparkTable = new SparkTable(id, catalogTable, Collections.emptyMap());

          SparkScanBuilder builder =
              (SparkScanBuilder)
                  sparkTable.newScanBuilder(new CaseInsensitiveStringMap(new HashMap<>()));
          SparkScan scan = (SparkScan) builder.build();
          Statistics stats = scan.estimateStatistics();

          assertTrue(stats.numRows().isPresent(), "numRows should be present");
          assertEquals(3L, stats.numRows().getAsLong(), "numRows should be 3");

          // Verify column stats include both data and partition columns
          Map<NamedReference, ColumnStatistics> colStats = stats.columnStats();
          assertNotNull(colStats.get(FieldReference.apply("id")), "id stats should be present");
          assertNotNull(colStats.get(FieldReference.apply("part")), "part stats should be present");

          // Check partition column stats
          ColumnStatistics partStats = colStats.get(FieldReference.apply("part"));
          assertTrue(partStats.min().isPresent(), "part min should be present");
          assertTrue(partStats.max().isPresent(), "part max should be present");
          assertEquals(1, partStats.min().get(), "part min should be 1");
          assertEquals(2, partStats.max().get(), "part max should be 2");
        });
  }

  @Test
  public void testEstimatedSizeUsesAvgLenFromCatalogStats(@TempDir File tempDir) throws Exception {
    // Verify that computeEstimatedSizeWithColumnProjection uses avgLen from catalog stats
    // instead of defaultSize(), mirroring EstimationUtils.getSizePerRow() (#5952).
    String path = tempDir.getAbsolutePath();
    String tblName = "stats_avglen";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tblName, path));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'bb'), (3, 'ccc')", tblName));

    // Create column stats with avgLen=5 for 'name' (STRING defaultSize is 20)
    CatalogColumnStat nameColStat =
        new CatalogColumnStat(
            scala.Option.empty(),
            scala.Option.empty(),
            scala.Option.empty(),
            scala.Option.empty(),
            scala.Option.apply((Object) 5L), // avgLen = 5 (vs STRING defaultSize 20)
            scala.Option.empty(),
            scala.Option.empty(),
            CatalogColumnStat.VERSION());

    CatalogStatistics catalogStats =
        new CatalogStatistics(
            scala.math.BigInt.apply(1024L),
            scala.Option.apply(scala.math.BigInt.apply(3L)),
            buildColStatsMap(new String[] {"name"}, new CatalogColumnStat[] {nameColStat}));

    CatalogTable catalogTable = injectCatalogStats(tblName, catalogStats);

    // avgLen is used for sizeInBytes estimation regardless of CBO/planStats settings
    withSQLConf(
        "spark.sql.cbo.enabled",
        "false",
        () -> {
          withSQLConf(
              "spark.sql.cbo.planStats.enabled",
              "false",
              () -> {
                Identifier id = Identifier.of(new String[] {"default"}, tblName);
                SparkTable sparkTable = new SparkTable(id, catalogTable, Collections.emptyMap());

                SparkScanBuilder builder =
                    (SparkScanBuilder)
                        sparkTable.newScanBuilder(new CaseInsensitiveStringMap(new HashMap<>()));

                // Prune to only 'name' column to trigger column projection estimation
                StructType prunedSchema = new StructType().add("name", DataTypes.StringType);
                builder.pruneColumns(prunedSchema);

                SparkScan scan = (SparkScan) builder.build();

                long totalBytes = getTotalBytes(scan);
                long estimatedSize = getEstimatedSizeInBytes(scan);
                assertTrue(totalBytes > 0, "totalBytes should be positive");

                // Schema: dataSchema = (id INT, name STRING), partitionSchema = empty
                // readSchema = (name STRING) after pruning
                //
                // With avgLen=5 for name (STRING: avgLen + 12 = 17, vs defaultSize 20):
                //   fullSchemaRowSize = 8 + (4 [INT default] + 17 [STRING avgLen]) = 29
                //   outputRowSize     = 8 + 17 = 25
                //   estimated = (totalBytes * 25) / 29
                //
                // Without avgLen (defaultSize only):
                //   fullSchemaRowSize = 8 + (4 + 20) = 32
                //   outputRowSize     = 8 + 20 = 28
                //   estimated = (totalBytes * 28) / 32
                long expectedWithAvgLen = (totalBytes * 25) / 29;
                long expectedWithoutAvgLen = (totalBytes * 28) / 32;

                assertEquals(
                    expectedWithAvgLen,
                    estimatedSize,
                    "estimatedSize should use avgLen from catalog stats");
                assertNotEquals(
                    expectedWithoutAvgLen,
                    estimatedSize,
                    "estimatedSize should differ from defaultSize-only calculation");
              });
        });
  }

  @Test
  public void testEstimateStatisticsWithoutAnalyze(@TempDir File tempDir) throws Exception {
    // Table exists in catalog but no stats were injected
    String path = tempDir.getAbsolutePath();
    String tblName = "stats_no_analyze";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tblName, path));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a')", tblName));

    withSQLConf(
        "spark.sql.cbo.enabled",
        "true",
        () -> {
          CatalogTable catalogTable =
              spark.sessionState().catalog().getTableMetadata(new TableIdentifier(tblName));
          Identifier id = Identifier.of(new String[] {"default"}, tblName);
          SparkTable sparkTable = new SparkTable(id, catalogTable, Collections.emptyMap());

          SparkScanBuilder builder =
              (SparkScanBuilder)
                  sparkTable.newScanBuilder(new CaseInsensitiveStringMap(new HashMap<>()));
          SparkScan scan = (SparkScan) builder.build();
          Statistics stats = scan.estimateStatistics();

          // Per-file Delta stats (numRecords in the transaction log) provide numRows even when
          // ANALYZE TABLE has not been run and no catalog stats exist.
          assertTrue(stats.numRows().isPresent(), "numRows should be present from per-file stats");
          assertEquals(
              1L, stats.numRows().getAsLong(), "numRows should match the inserted row count");
          assertTrue(stats.sizeInBytes().isPresent(), "sizeInBytes should be present");
          assertTrue(stats.sizeInBytes().getAsLong() > 0, "sizeInBytes should be positive");
        });
  }

  @Test
  public void testEstimateStatisticsWithCatalogStats_noNumRows(@TempDir File tempDir)
      throws Exception {
    // Catalog stats with sizeInBytes but no numRows: when per-file stats are available,
    // numRows should still be reported from per-file numRecords (not from catalog stats).
    // sizeInBytes should come from planned files, not the stale catalog value.
    String path = tempDir.getAbsolutePath();
    String tblName = "stats_no_numrows";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tblName, path));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tblName));

    // Catalog stats with sizeInBytes only, no numRows
    CatalogStatistics catalogStats =
        new CatalogStatistics(
            scala.math.BigInt.apply(99999L),
            scala.Option.empty(), // no numRows
            scala.collection.immutable.Map$.MODULE$.empty());

    CatalogTable catalogTable = injectCatalogStats(tblName, catalogStats);

    withSQLConf(
        "spark.sql.cbo.enabled",
        "true",
        () -> {
          Identifier id = Identifier.of(new String[] {"default"}, tblName);
          SparkTable sparkTable = new SparkTable(id, catalogTable, Collections.emptyMap());

          SparkScanBuilder builder =
              (SparkScanBuilder)
                  sparkTable.newScanBuilder(new CaseInsensitiveStringMap(new HashMap<>()));
          SparkScan scan = (SparkScan) builder.build();
          Statistics stats = scan.estimateStatistics();

          // Per-file stats provide numRows even when catalog stats lack it
          assertTrue(stats.numRows().isPresent(), "numRows should be present from per-file stats");
          assertEquals(
              2L, stats.numRows().getAsLong(), "numRows should reflect per-file row count");
          assertTrue(stats.sizeInBytes().isPresent(), "sizeInBytes should be present");
          assertTrue(stats.sizeInBytes().getAsLong() > 0, "sizeInBytes should be positive");

          // sizeInBytes should NOT be the catalog sizeInBytes (99999)
          assertNotEquals(
              99999L,
              stats.sizeInBytes().getAsLong(),
              "sizeInBytes should come from planned files, not catalog stats");

          // columnStats should be empty (catalog stats had no column stats)
          assertTrue(
              stats.columnStats().isEmpty(),
              "columnStats should be empty when catalog stats have no column stats");
        });
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static scala.collection.immutable.Map<String, CatalogColumnStat> buildColStatsMap(
      String[] keys, CatalogColumnStat[] values) {
    scala.collection.mutable.Builder b = scala.collection.immutable.Map$.MODULE$.newBuilder();
    for (int i = 0; i < keys.length; i++) {
      b.$plus$eq(new scala.Tuple2<>(keys[i], values[i]));
    }
    return (scala.collection.immutable.Map<String, CatalogColumnStat>) b.result();
  }
}
