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
package io.delta.spark.internal.v2.catalog;

import static org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ;
import static org.apache.spark.sql.connector.catalog.TableCapability.BATCH_WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.adapters.KernelMetadataAdapter;
import io.delta.spark.internal.v2.adapters.KernelProtocolAdapter;
import io.delta.spark.internal.v2.read.cdc.CDCSchemaContext;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.apache.spark.sql.delta.sources.DeltaSourceMetadataTrackingLog;
import org.apache.spark.sql.delta.sources.PersistedMetadata;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Option;

public class SparkTableTest extends DeltaV2TestBase {

  @ParameterizedTest(name = "{0} - {1}")
  @MethodSource("tableTestCases")
  public void testDeltaKernelTable(
      TableTestCase testCase, ConstructionMethod method, @TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    String tableName =
        "test_" + testCase.name.toLowerCase().replace(" ", "_") + "_" + method.name().toLowerCase();
    testCase.createTableSql.apply(tableName, path);
    Identifier identifier = Identifier.of(new String[] {"default"}, tableName);

    // Create SparkTable based on construction method
    SparkTable kernelTable;
    CatalogTable catalogTable = null;

    switch (method) {
      case FROM_PATH:
        kernelTable = new SparkTable(identifier, path);
        break;
      case FROM_CATALOG_TABLE:
        catalogTable =
            spark.sessionState().catalog().getTableMetadata(new TableIdentifier(tableName));
        kernelTable = new SparkTable(identifier, catalogTable, Collections.emptyMap());
        break;
      default:
        throw new IllegalArgumentException("Unknown construction method: " + method);
    }

    // ===== Test table name =====
    String expectedName;
    switch (method) {
      case FROM_PATH:
        expectedName = "delta.`" + path + "`";
        break;
      case FROM_CATALOG_TABLE:
        // Catalog table should return fully qualified name: spark_catalog.default.tableName
        expectedName = "spark_catalog.default." + tableName;
        break;
      default:
        throw new IllegalArgumentException("Unknown method: " + method);
    }
    assertEquals(expectedName, kernelTable.name());

    // ===== Test schema =====
    StructType sparkSchema = kernelTable.schema();
    Column[] actualColumns = kernelTable.columns();
    assertEquals(testCase.expectedColumns.size(), sparkSchema.fields().length);
    for (int i = 0; i < testCase.expectedColumns.size(); i++) {
      Column expectedCol = testCase.expectedColumns.get(i);
      assertEquals(
          expectedCol.name(),
          sparkSchema.fields()[i].name(),
          "Column name mismatch at position " + i);
      assertEquals(
          expectedCol.dataType(),
          sparkSchema.fields()[i].dataType(),
          "Data type mismatch for column: " + expectedCol.name());
      // Check column object from table.columns()
      assertEquals(expectedCol, actualColumns[i], "Column mismatch at position " + i);
    }

    // ===== Verify schema consistency with DeltaTableV2 =====
    // This ensures SparkTable (Kernel-based) returns the same schema as DeltaTableV2 (V1-based)
    // Both should properly remove internal Delta metadata (e.g., column mapping physical names)
    DeltaTableV2 deltaTableV2;
    switch (method) {
      case FROM_PATH:
        deltaTableV2 =
            DeltaTableV2.apply(
                spark,
                new Path(path),
                Option.empty(),
                Option.empty(),
                scala.collection.immutable.Map$.MODULE$.empty(),
                Option.empty());
        break;
      case FROM_CATALOG_TABLE:
        deltaTableV2 =
            DeltaTableV2.apply(
                spark,
                new Path(path),
                Option.apply(catalogTable),
                Option.apply(tableName),
                scala.collection.immutable.Map$.MODULE$.empty(),
                Option.empty());
        break;
      default:
        throw new IllegalArgumentException("Unknown method: " + method);
    }

    // Verify schemas are equal (including field names, types, and metadata)
    assertEquals(
        deltaTableV2.schema(),
        sparkSchema,
        "SparkTable schema should match DeltaTableV2 schema for test case: " + testCase.name);

    // ===== Test partitioning =====
    Transform[] partitioning = kernelTable.partitioning();
    assertEquals(testCase.expectedPartitionColumns.length, partitioning.length);
    for (int i = 0; i < testCase.expectedPartitionColumns.length; i++) {
      assertEquals(
          testCase.expectedPartitionColumns[i],
          partitioning[i].references()[0].describe(),
          "Partition column mismatch at position " + i);
    }

    // ===== Test properties =====
    Map<String, String> properties = kernelTable.properties();
    testCase.expectedProperties.forEach(
        (key, value) -> {
          assertTrue(properties.containsKey(key), "Property not found: " + key);
          assertEquals(value, properties.get(key), "Property value mismatch for: " + key);
        });

    // ===== Test capabilities =====
    assertTrue(kernelTable.capabilities().contains(BATCH_READ));
    assertTrue(kernelTable.capabilities().contains(BATCH_WRITE));
    assertTrue(kernelTable instanceof SupportsWrite);

    // ===== Test getCatalogTable based on construction method =====
    Optional<CatalogTable> retrievedCatalogTable = kernelTable.getCatalogTable();
    switch (method) {
      case FROM_PATH:
        assertFalse(
            retrievedCatalogTable.isPresent(),
            "Path-based SparkTable should not have catalog table");
        break;
      case FROM_CATALOG_TABLE:
        assertTrue(
            retrievedCatalogTable.isPresent(),
            "CatalogTable-based SparkTable should have catalog table");
        assertEquals(
            catalogTable,
            retrievedCatalogTable.get(),
            "Retrieved catalog table should match the original");
        break;
    }

    // ===== Test getTablePath returns Path from tablePath =====
    Path retrievedPath = kernelTable.getTablePath();
    assertEquals(new Path(path), retrievedPath, "getTablePath should return Path from tablePath");
  }

  /** Enum to represent different construction methods for SparkTable */
  enum ConstructionMethod {
    FROM_PATH("Path"),
    FROM_CATALOG_TABLE("CatalogTable");

    private final String displayName;

    ConstructionMethod(String displayName) {
      this.displayName = displayName;
    }

    @Override
    public String toString() {
      return displayName;
    }
  }

  /** Represents a test case configuration for Delta tables */
  private static class TableTestCase {
    final String name;
    final BiFunction<String, String, Void> createTableSql;
    final List<Column> expectedColumns;
    final String[] expectedPartitionColumns;
    final Map<String, String> expectedProperties;

    public TableTestCase(
        String name,
        BiFunction<String, String, Void> createTableSql,
        List<Column> expectedColumns,
        String[] expectedPartitionColumns,
        Map<String, String> expectedProperties) {

      this.name = name;
      this.createTableSql = createTableSql;
      this.expectedColumns = expectedColumns;
      this.expectedPartitionColumns = expectedPartitionColumns;
      this.expectedProperties = expectedProperties;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /** Provides different test cases for Delta tables combined with construction methods */
  static Stream<Arguments> tableTestCases() {

    // ===== Partitioned Table =====
    List<Column> partitionedTableColumns = new ArrayList<>();
    partitionedTableColumns.add(Column.create("id", DataTypes.IntegerType));
    partitionedTableColumns.add(Column.create("data", DataTypes.StringType));
    partitionedTableColumns.add(Column.create("part", DataTypes.IntegerType));

    // ===== Unpartitioned Table =====
    List<Column> unPartitionedTableColumns = new ArrayList<>();
    unPartitionedTableColumns.add(Column.create("id", DataTypes.IntegerType));
    unPartitionedTableColumns.add(Column.create("data", DataTypes.StringType));

    // ===== Setup Single Properties =====
    Map<String, String> basicProps = new HashMap<>();
    basicProps.put("foo", "bar");

    // ===== Setup Multiple Properties =====
    Map<String, String> multiProps = new HashMap<>();
    multiProps.put("prop1", "value1");
    multiProps.put("prop2", "value2");
    multiProps.put("delta.enableChangeDataFeed", "true");

    List<Column> singleColumn = new ArrayList<>();
    singleColumn.add(Column.create("id", DataTypes.IntegerType));

    // ===== Name Mapping Table =====
    List<Column> nameMappingTableColumns = new ArrayList<>();
    nameMappingTableColumns.add(Column.create("id", DataTypes.IntegerType));
    nameMappingTableColumns.add(Column.create("name", DataTypes.StringType));
    nameMappingTableColumns.add(Column.create("value", DataTypes.DoubleType));

    Map<String, String> nameMappingProps = new HashMap<>();
    nameMappingProps.put("delta.columnMapping.mode", "name");

    List<TableTestCase> testCases =
        Arrays.asList(
            new TableTestCase(
                "Partitioned Table",
                (tableName, path) -> {
                  spark.sql(
                      String.format(
                          "CREATE TABLE %s (id INT, data STRING, part INT) USING delta "
                              + "PARTITIONED BY (part) TBLPROPERTIES ('foo'='bar') LOCATION '%s'",
                          tableName, path));
                  return null;
                },
                partitionedTableColumns,
                new String[] {"part"},
                basicProps),
            new TableTestCase(
                "UnPartitioned Table",
                (tableName, path) -> {
                  spark.sql(
                      String.format(
                          "CREATE TABLE %s (id INT, data STRING) USING delta LOCATION '%s'",
                          tableName, path));
                  return null;
                },
                unPartitionedTableColumns,
                new String[] {},
                new HashMap<>()),
            new TableTestCase(
                "Multiple Properties",
                (tableName, path) -> {
                  spark.sql(
                      String.format(
                          "CREATE TABLE %s (id INT) USING delta "
                              + "TBLPROPERTIES ('prop1'='value1', 'prop2'='value2', 'delta.enableChangeDataFeed'='true') "
                              + "LOCATION '%s'",
                          tableName, path));
                  return null;
                },
                singleColumn,
                new String[] {},
                multiProps),
            new TableTestCase(
                "Name Mapping Table",
                (tableName, path) -> {
                  spark.sql(
                      String.format(
                          "CREATE TABLE %s (id INT, name STRING, value DOUBLE) USING delta "
                              + "TBLPROPERTIES ('delta.columnMapping.mode'='name') "
                              + "LOCATION '%s'",
                          tableName, path));
                  spark.sql(String.format("INSERT INTO %s VALUES (1, 'test', 100.0)", tableName));
                  return null;
                },
                nameMappingTableColumns,
                new String[] {},
                nameMappingProps));

    // Create cartesian product of test cases and construction methods
    return testCases.stream()
        .flatMap(
            testCase ->
                Stream.of(ConstructionMethod.FROM_PATH, ConstructionMethod.FROM_CATALOG_TABLE)
                    .map(method -> Arguments.of(testCase, method)));
  }

  /**
   * Test that getDecodedPath handles various URI schemes correctly, not just file:// URIs. This
   * verifies the fix for supporting cloud storage paths (s3, abfss, gs) and HDFS.
   */
  @ParameterizedTest(name = "URI scheme: {0}")
  @MethodSource("uriSchemeTestCases")
  public void testGetDecodedPathSupportsVariousUriSchemes(String scheme, String uriString)
      throws Exception {
    // Access the private static method via reflection
    Method getDecodedPath =
        SparkTable.class.getDeclaredMethod("getDecodedPath", java.net.URI.class);
    getDecodedPath.setAccessible(true);

    URI uri = new URI(uriString);
    String result = (String) getDecodedPath.invoke(null, uri);

    // Verify the path is decoded correctly
    // The result should contain the path portion without URL encoding issues
    assertTrue(
        result.contains("/path/to/table"),
        "Decoded path should contain the expected path. Got: " + result);
  }

  /** Test that URL-encoded characters are properly decoded */
  @Test
  public void testGetDecodedPathDecodesUrlEncodedCharacters() throws Exception {
    // Access the private static method via reflection
    Method getDecodedPath =
        SparkTable.class.getDeclaredMethod("getDecodedPath", java.net.URI.class);
    getDecodedPath.setAccessible(true);

    // Test URL-encoded path: "spark%25dir%25prefix" should decode to "spark%dir%prefix"
    // %25 is the URL encoding for %
    URI uri = new URI("file:///data/spark%25dir%25prefix/table");
    String result = (String) getDecodedPath.invoke(null, uri);

    // For file URIs, getDecodedPath returns just the path without the scheme
    assertEquals(
        "/data/spark%dir%prefix/table",
        result, "URL-encoded characters should be properly decoded");
  }

  /** Provides test cases for different URI schemes */
  static Stream<Arguments> uriSchemeTestCases() {
    return Stream.of(
        Arguments.of("file", "file:///path/to/table"),
        Arguments.of("s3", "s3://bucket/path/to/table"),
        Arguments.of("s3a", "s3a://bucket/path/to/table"),
        Arguments.of("abfss", "abfss://container@account.dfs.core.windows.net/path/to/table"),
        Arguments.of("gs", "gs://bucket/path/to/table"),
        Arguments.of("hdfs", "hdfs://namenode:8020/path/to/table"));
  }

  @Test
  public void testEqualsAndHashCode(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    spark.sql(String.format("CREATE TABLE test_equals (id INT) USING delta LOCATION '%s'", path));

    Identifier identifier = Identifier.of(new String[] {"default"}, "test_equals");
    Map<String, String> options = Collections.singletonMap("key", "value");

    SparkTable table1 = new SparkTable(identifier, path, options);
    SparkTable table2 = new SparkTable(identifier, path, options);
    SparkTable table3 = new SparkTable(identifier, path, Collections.emptyMap());

    // Same identifier, path, and options should be equal
    assertEquals(table1, table2);
    assertEquals(table1.hashCode(), table2.hashCode());

    // Different options should not be equal and hashCodes should differ
    assertNotEquals(table1, table3);
    assertNotEquals(table1.hashCode(), table3.hashCode());
  }

  @Test
  public void testEqualsAndHashCodeWithCatalogTable(@TempDir File tempDir) throws Exception {
    String path1 = new File(tempDir, "table1").getAbsolutePath();
    String path2 = new File(tempDir, "table2").getAbsolutePath();
    spark.sql(
        String.format("CREATE TABLE test_catalog1 (id INT) USING delta LOCATION '%s'", path1));
    spark.sql(
        String.format("CREATE TABLE test_catalog2 (id INT) USING delta LOCATION '%s'", path2));

    Identifier identifier = Identifier.of(new String[] {"default"}, "test_catalog");

    // Create table1 and table2 with separately fetched CatalogTable objects (not same instance)
    SparkTable table1 =
        new SparkTable(
            identifier,
            spark.sessionState().catalog().getTableMetadata(new TableIdentifier("test_catalog1")),
            Collections.emptyMap());
    SparkTable table2 =
        new SparkTable(
            identifier,
            spark.sessionState().catalog().getTableMetadata(new TableIdentifier("test_catalog1")),
            Collections.emptyMap());

    // Same identifier, catalogTable, and options should be equal
    assertEquals(table1, table2);
    assertEquals(table1.hashCode(), table2.hashCode());

    // Different catalogTable should not be equal
    SparkTable table3 =
        new SparkTable(
            identifier,
            spark.sessionState().catalog().getTableMetadata(new TableIdentifier("test_catalog2")),
            Collections.emptyMap());
    assertNotEquals(table1, table3);
    assertNotEquals(table1.hashCode(), table3.hashCode());

    // Path-based table (no catalogTable) should not equal catalog-based table
    SparkTable table4 = new SparkTable(identifier, path1, Collections.emptyMap());
    assertNotEquals(table1, table4);
    assertNotEquals(table1.hashCode(), table4.hashCode());
  }

  @Test
  public void testEqualsAndHashCodeWithDifferentSnapshotVersions(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    spark.sql(String.format("CREATE TABLE test_snapshot (id INT) USING delta LOCATION '%s'", path));

    Identifier identifier = Identifier.of(new String[] {"default"}, "test_snapshot");

    // Create first SparkTable instance at version 0
    SparkTable table1 = new SparkTable(identifier, path);

    // Modify the table to create a new version
    spark.sql("INSERT INTO test_snapshot VALUES (1)");

    // Create second SparkTable instance at version 1
    SparkTable table2 = new SparkTable(identifier, path);

    // Same identifier and path but different snapshot versions should not be equal
    assertNotEquals(
        table1,
        table2,
        "SparkTable instances with different snapshot versions should not be equal");
    assertNotEquals(
        table1.hashCode(),
        table2.hashCode(),
        "Hash codes should differ for different snapshot versions");
  }

  @Test
  public void testNewWriteBuilderReturnsWriteBuilder(@TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format("CREATE TABLE test_write_builder (id INT) USING delta LOCATION '%s'", path));

    SparkTable table =
        new SparkTable(Identifier.of(new String[] {"default"}, "test_write_builder"), path);
    LogicalWriteInfo writeInfo =
        new LogicalWriteInfo() {
          @Override
          public String queryId() {
            return "test-query-id";
          }

          @Override
          public StructType schema() {
            return new StructType().add("id", DataTypes.IntegerType);
          }

          @Override
          public CaseInsensitiveStringMap options() {
            return new CaseInsensitiveStringMap(Collections.emptyMap());
          }
        };

    assertNotNull(table.newWriteBuilder(writeInfo), "newWriteBuilder should return non-null");
  }

  @Test
  public void testSchemaWithReadChangeFeedIncludesCDCColumns(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    spark.sql(String.format("CREATE TABLE test_cdc_on (id INT) USING delta LOCATION '%s'", path));

    Identifier identifier = Identifier.of(new String[] {"default"}, "test_cdc_on");
    Map<String, String> options = Collections.singletonMap("readChangeFeed", "true");
    SparkTable table = new SparkTable(identifier, path, options);

    StructType schema = table.schema();
    List<String> names = Arrays.asList(schema.fieldNames());
    assertTrue(names.contains("id"), "physical column missing: " + names);
    assertTrue(names.contains(CDCSchemaContext.CDC_TYPE_COLUMN), "missing _change_type: " + names);
    assertTrue(
        names.contains(CDCSchemaContext.CDC_COMMIT_VERSION), "missing _commit_version: " + names);
    assertTrue(
        names.contains(CDCSchemaContext.CDC_COMMIT_TIMESTAMP),
        "missing _commit_timestamp: " + names);

    // Spark Table contract: columns() must agree with schema().
    List<String> columnNames =
        Arrays.stream(table.columns())
            .map(Column::name)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(names, columnNames, "columns() must agree with schema()");
  }

  @Test
  public void testSchemaWithoutReadChangeFeedExcludesCDCColumns(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    spark.sql(String.format("CREATE TABLE test_cdc_off (id INT) USING delta LOCATION '%s'", path));

    Identifier identifier = Identifier.of(new String[] {"default"}, "test_cdc_off");
    SparkTable table = new SparkTable(identifier, path, Collections.emptyMap());

    StructType schema = table.schema();
    List<String> names = Arrays.asList(schema.fieldNames());
    assertFalse(names.contains(CDCSchemaContext.CDC_TYPE_COLUMN), "unexpected _change_type");
    assertFalse(names.contains(CDCSchemaContext.CDC_COMMIT_VERSION), "unexpected _commit_version");
    assertFalse(
        names.contains(CDCSchemaContext.CDC_COMMIT_TIMESTAMP), "unexpected _commit_timestamp");
  }

  // ---------------------------------------------------------------------------
  // Schema-tracking-aware schema construction
  // ---------------------------------------------------------------------------

  /** Empty schema-tracking log → snapshot schema is used as fallback. */
  @Test
  public void testSchemaTracking_emptyLogFallsBackToSnapshotSchema(@TempDir File tempDir) {
    String tablePath = new File(tempDir, "table").getAbsolutePath();
    String schemaLogPath = new File(tempDir, "schema_log").getAbsolutePath();
    String tableName =
        "test_schema_tracking_empty_log_" + UUID.randomUUID().toString().replace('-', '_');
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'",
            tableName, tablePath));

    Identifier identifier = Identifier.of(new String[] {"default"}, tableName);
    Map<String, String> options = new HashMap<>();
    options.put(DeltaOptions.SCHEMA_TRACKING_LOCATION(), schemaLogPath);

    SparkTable table = new SparkTable(identifier, tablePath, options);

    StructType schema = table.schema();
    assertEquals(2, schema.fields().length);
    assertEquals("id", schema.fields()[0].name());
    assertEquals("name", schema.fields()[1].name());
  }

  /**
   * Verifies that pre-seeded persisted metadata (older snapshot, fewer columns) overrides the
   * current snapshot schema (which has been evolved with ALTER TABLE).
   */
  private void verifyPersistedEntryDrivesSparkTableSchema(
      File tempDir, String optionKey, String tableNameSuffix, boolean usePathBasedConstructor) {
    String tablePath = new File(tempDir, "table").getAbsolutePath();
    String schemaLogPath = new File(tempDir, "schema_log").getAbsolutePath();
    String tableName =
        "test_schema_tracking_persisted_override_"
            + tableNameSuffix
            + "_"
            + UUID.randomUUID().toString().replace('-', '_');
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta "
                + "PARTITIONED BY (name) LOCATION '%s'",
            tableName, tablePath));

    // Capture v0 metadata BEFORE evolving the table.
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(tablePath, spark.sessionState().newHadoopConf());
    SnapshotImpl snapshotV0 = (SnapshotImpl) snapshotManager.loadSnapshotAt(0L);
    Metadata metadataV0 = snapshotV0.getMetadata();
    Protocol protocolV0 = snapshotV0.getProtocol();
    String tableId = metadataV0.getId();

    // Seed the schema log with v0's metadata (id INT data, name STRING partition).
    DeltaSourceMetadataTrackingLog trackingLog =
        DeltaSourceMetadataTrackingLog.create(
            spark,
            schemaLogPath,
            tableId,
            tablePath,
            scala.collection.immutable.Map$.MODULE$.empty(),
            scala.Option.empty(),
            /* mergeConsecutiveSchemaChanges= */ false,
            /* consecutiveSchemaChangesMerger= */ scala.Option.empty(),
            /* initMetadataLogEagerly= */ true);
    PersistedMetadata seededEntry =
        PersistedMetadata.apply(
            tableId,
            0L,
            new KernelMetadataAdapter(metadataV0),
            new KernelProtocolAdapter(protocolV0),
            tablePath + "/_delta_log/_streaming_metadata");
    trackingLog.writeNewMetadata(seededEntry, false);

    // Evolve the table — version 1 has an extra non-partition column.
    spark.sql(String.format("ALTER TABLE %s ADD COLUMNS (value DOUBLE)", tableName));

    // Construct SparkTable with the schema-tracking option pointing at the seeded log. The
    // snapshot is at v1 (3 columns) but the persisted entry is at v0 (2 columns). Default to
    // the catalog-table constructor since that's how production code typically loads the table.
    Identifier identifier = Identifier.of(new String[] {"default"}, tableName);
    Map<String, String> options = new HashMap<>();
    options.put(optionKey, schemaLogPath);
    SparkTable table;
    if (usePathBasedConstructor) {
      table = new SparkTable(identifier, tablePath, options);
    } else {
      CatalogTable catalogTable;
      try {
        catalogTable =
            spark.sessionState().catalog().getTableMetadata(new TableIdentifier(tableName));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      table = new SparkTable(identifier, catalogTable, options);
    }

    // Persisted metadata wins: schema reflects v0 (2 columns), not v1 (3 columns).
    // Public schema layout is data fields followed by partition fields, so [id, name].
    StructType schema = table.schema();
    assertEquals(2, schema.fields().length, "Persisted entry should override snapshot schema");
    assertEquals("id", schema.fields()[0].name());
    assertEquals("name", schema.fields()[1].name());

    // Partition column from the persisted entry's partitionSchema is also honored.
    Transform[] partitioning = table.partitioning();
    assertEquals(
        1, partitioning.length, "Should have one partition column from the persisted entry");
    assertEquals(
        "name",
        partitioning[0].references()[0].describe(),
        "Partition column should be 'name' from the persisted entry");
  }

  /** Persisted entry overrides the snapshot schema when SCHEMA_TRACKING_LOCATION is set. */
  @Test
  public void testSchemaTracking_persistedEntryOverridesSnapshotSchema(@TempDir File tempDir) {
    verifyPersistedEntryDrivesSparkTableSchema(
        tempDir,
        DeltaOptions.SCHEMA_TRACKING_LOCATION(),
        "primary_key",
        /* usePathBasedConstructor= */ false);
  }

  /** Alias key triggers the same code path. */
  @Test
  public void testSchemaTracking_persistedEntryOverridesSnapshotSchemaWithAliasKey(
      @TempDir File tempDir) {
    verifyPersistedEntryDrivesSparkTableSchema(
        tempDir,
        DeltaOptions.SCHEMA_TRACKING_LOCATION_ALIAS(),
        "alias_key",
        /* usePathBasedConstructor= */ false);
  }

  /** Path-based constructor honors the persisted entry too. */
  @Test
  public void testSchemaTracking_persistedEntryOverridesSnapshotSchemaWithPathBasedConstructor(
      @TempDir File tempDir) {
    verifyPersistedEntryDrivesSparkTableSchema(
        tempDir,
        DeltaOptions.SCHEMA_TRACKING_LOCATION(),
        "path_based_constructor",
        /* usePathBasedConstructor= */ true);
  }

  /** Setting the option while the feature flag is off throws at construction. */
  @Test
  public void testSchemaTracking_throwsWhenFeatureFlagDisabled(@TempDir File tempDir)
      throws Exception {
    String tablePath = new File(tempDir, "table").getAbsolutePath();
    String schemaLogPath = new File(tempDir, "schema_log").getAbsolutePath();
    String tableName =
        "test_schema_tracking_feature_flag_disabled_"
            + UUID.randomUUID().toString().replace('-', '_');
    spark.sql(
        String.format("CREATE TABLE %s (id INT) USING delta LOCATION '%s'", tableName, tablePath));

    Identifier identifier = Identifier.of(new String[] {"default"}, tableName);
    Map<String, String> options = new HashMap<>();
    options.put(DeltaOptions.SCHEMA_TRACKING_LOCATION(), schemaLogPath);

    withSQLConf(
        DeltaSQLConf.DELTA_STREAMING_ENABLE_SCHEMA_TRACKING().key(),
        "false",
        () -> {
          UnsupportedOperationException ex =
              assertThrows(
                  UnsupportedOperationException.class,
                  () -> new SparkTable(identifier, tablePath, options));
          assertEquals(
              "Schema tracking location is not supported for Delta streaming source",
              ex.getMessage());
        });
  }
}
