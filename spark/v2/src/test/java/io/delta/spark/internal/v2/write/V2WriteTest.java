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
package io.delta.spark.internal.v2.write;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.delta.spark.internal.v2.V2TestBase;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.spark.sql.delta.DeltaConfigs;
import org.apache.spark.sql.delta.shims.VariantShreddingShims;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Option;

/** E2E DSv2 batch-write tests for column-mapped tables. */
public class V2WriteTest extends V2TestBase {

  @ParameterizedTest(name = "columnMappingMode={0}")
  @ValueSource(strings = {"name", "id"})
  public void writeToColumnMappingTable(String mappingMode, @TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createColumnMappingTable(tablePath, mappingMode);

    spark.sql(
        str(
            "INSERT INTO dsv2.delta.`%s` VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)",
            tablePath));

    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0)));
    check(
        str("SELECT * FROM delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0)));

    // Both name and id mode store data under physical col-* names.
    assertPhysicalParquetUsesMappedColumnNames(tablePath, "id", "user_name", "amount");
  }

  @Test
  public void writeToColumnMappingTableWithRenamedColumn(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();
    createColumnMappingTable(tablePath, "name");

    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, 'Alice', 100.0)", tablePath));
    // The physical name is unchanged after a RENAME under column mapping, so the second append
    // must still land in the same physical columns.
    spark.sql(str("ALTER TABLE delta.`%s` RENAME COLUMN user_name TO customer_name", tablePath));
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (2, 'Bob', 200.0)", tablePath));

    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0)));
    check(
        str("SELECT * FROM delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0)));
  }

  @Test
  public void multipleAppendsOnColumnMappingTable(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();
    createColumnMappingTable(tablePath, "name");

    // Interleave V2 and V1 writes to confirm the physical layout produced by the V2 path is
    // consistent.
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, 'Alice', 100.0)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'Bob', 200.0)", tablePath));
    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (3, 'Carol', 300.0)", tablePath));

    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0), row(3, "Carol", 300.0)));
    check(
        str("SELECT * FROM delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0), row(3, "Carol", 300.0)));
  }

  @ParameterizedTest(name = "columnMappingMode={0}")
  @ValueSource(strings = {"name", "id"})
  public void writeToPartitionedColumnMappingTable(String mappingMode, @TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createPartitionedColumnMappingTable(tablePath, mappingMode);

    spark.sql(
        str(
            "INSERT INTO dsv2.delta.`%s` VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)",
            tablePath));

    // Read back through both the V2 and V1 paths, both resolve partition values by physical name,
    // so a logical-directory regression would surface here as null partition columns.
    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0)));
    check(
        str("SELECT * FROM delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "Alice", 100.0), row(2, "Bob", 200.0)));

    assertPhysicalPartitionDirExists(tablePath, "Alice");
    assertPhysicalPartitionDirExists(tablePath, "Bob");
    assertAddFilePartitionValuesArePhysical(tablePath, "name");
    // The Parquet body omits the partition column and uses physical col-* names for the rest.
    assertPhysicalParquetUsesMappedColumnNames(
        physicalPartitionDir(tablePath, "Alice").getAbsolutePath(), "id", "value");
    assertParquetBodyOmitsPartitionColumns(
        physicalPartitionDir(tablePath, "Alice"), /* expectedDataColumns */ 2);
  }

  @Test
  public void writeToMultiColumnPartitionedColumnMappingTable(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, region STRING, tier INT) USING delta "
                + "PARTITIONED BY (region, tier) "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));

    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, 'us', 1), (2, 'eu', 2)", tablePath));

    check(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "us", 1), row(2, "eu", 2)));
    check(
        str("SELECT * FROM delta.`%s` ORDER BY id", tablePath),
        List.of(row(1, "us", 1), row(2, "eu", 2)));

    // Nested physical partition directories: col-<uuid>=us/col-<uuid>=1.
    File outer = physicalPartitionDir(tablePath, "us");
    File inner = physicalPartitionChildDir(outer, "1");
    assertTrue(
        inner != null, "Expected a nested physical partition directory col-*=1 under " + outer);
    // Both partition columns are path-encoded.
    assertParquetBodyOmitsPartitionColumns(inner, /* expectedDataColumns */ 1);
  }

  @Test
  public void writeNullPartitionValueColumnMappingTable(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createPartitionedColumnMappingTable(tablePath, "name");

    spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, NULL, 100.0)", tablePath));

    check(str("SELECT * FROM dsv2.delta.`%s`", tablePath), List.of(row(1, null, 100.0)));
    check(str("SELECT * FROM delta.`%s`", tablePath), List.of(row(1, null, 100.0)));

    // A null partition value is Hive-encoded under the physical parent directory.
    assertPhysicalPartitionDirExists(tablePath, "__HIVE_DEFAULT_PARTITION__");
    assertParquetBodyOmitsPartitionColumns(
        physicalPartitionDir(tablePath, "__HIVE_DEFAULT_PARTITION__"), /* expectedDataColumns */ 2);
  }

  @Test
  public void partitionedIcebergCompatWriteIsRejected(@TempDir File deltaTablePath) {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING, value DOUBLE) USING delta "
                + "PARTITIONED BY (name) TBLPROPERTIES ("
                + "'delta.columnMapping.mode' = 'name', 'delta.enableIcebergCompatV2' = 'true')",
            tablePath));

    // IcebergCompat materializes partition columns into the Parquet body, which this write path
    // does not do yet.
    assertThrows(
        UnsupportedOperationException.class,
        () -> spark.sql(str("INSERT INTO dsv2.delta.`%s` VALUES (1, 'Alice', 100.0)", tablePath)));
  }

  private void createColumnMappingTable(String tablePath, String mappingMode) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, user_name STRING, amount DOUBLE) "
                + "USING delta TBLPROPERTIES ('delta.columnMapping.mode' = '%s')",
            tablePath, mappingMode));
  }

  private void createPartitionedColumnMappingTable(String tablePath, String mappingMode) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING, value DOUBLE) USING delta "
                + "PARTITIONED BY (name) TBLPROPERTIES ('delta.columnMapping.mode' = '%s')",
            tablePath, mappingMode));
  }

  /** Returns the {@code col-<uuid>=<value>} partition directory under {@code tablePath}. */
  private File physicalPartitionDir(String tablePath, String value) {
    File[] dirs =
        new File(tablePath).listFiles((d, n) -> n.startsWith("col-") && n.endsWith("=" + value));
    assertNotNull(dirs, "Expected partition directories under " + tablePath);
    assertTrue(
        dirs.length == 1,
        "Expected one physical partition directory col-*=" + value + ", got " + dirs.length);
    return dirs[0];
  }

  /** Returns the nested {@code col-<uuid>=<value>} directory under {@code parent}, or null. */
  private File physicalPartitionChildDir(File parent, String value) {
    File[] dirs = parent.listFiles((d, n) -> n.startsWith("col-") && n.endsWith("=" + value));
    return (dirs != null && dirs.length == 1) ? dirs[0] : null;
  }

  /**
   * Asserts a physical {@code col-<uuid>=<value>} partition directory exists and contains data, and
   * that no logical {@code <logicalName>=<value>} directory was written instead.
   */
  private void assertPhysicalPartitionDirExists(String tablePath, String value) {
    File dir = physicalPartitionDir(tablePath, value);
    assertTrue(dir.isDirectory(), "Expected a directory at " + dir);
    File[] parquet = dir.listFiles((d, n) -> n.endsWith(".parquet"));
    assertTrue(parquet != null && parquet.length > 0, "Expected a parquet file under " + dir);
    File[] logical =
        new File(tablePath).listFiles((d, n) -> !n.startsWith("col-") && n.endsWith("=" + value));
    assertTrue(
        logical == null || logical.length == 0,
        "Did not expect a logical partition directory ending in '=" + value + "'");
  }

  /**
   * Asserts the AddFile {@code partitionValues} in the log are keyed by the physical col-* name.
   * The commit JSON is read with schema inference, so {@code partitionValues} surfaces as a struct
   * whose field names are the partition keys.
   */
  private void assertAddFilePartitionValuesArePhysical(String tablePath, String logicalName) {
    org.apache.spark.sql.types.StructType addType =
        (org.apache.spark.sql.types.StructType)
            spark
                .read()
                .json(tablePath + "/_delta_log/*.json")
                .where("add is not null")
                .schema()
                .apply("add")
                .dataType();
    org.apache.spark.sql.types.StructType partitionValuesType =
        (org.apache.spark.sql.types.StructType) addType.apply("partitionValues").dataType();
    List<String> keys = List.of(partitionValuesType.fieldNames());
    assertFalse(keys.isEmpty(), "Expected at least one partition-value key");
    assertTrue(
        keys.stream().allMatch(k -> k.startsWith("col-")),
        "Expected physical col-* partition-value keys, got: " + keys);
    assertFalse(
        keys.contains(logicalName),
        "AddFile partitionValues must not use the logical key '" + logicalName + "'");
  }

  /**
   * Verifies the on-disk Parquet schema uses physical column mapping names and that every field
   * carries a Parquet field id.
   */
  private void assertPhysicalParquetUsesMappedColumnNames(
      String tablePath, String... logicalColumnNames) throws Exception {
    File[] parquetFiles = new File(tablePath).listFiles((dir, name) -> name.endsWith(".parquet"));
    assertNotNull(parquetFiles, "Expected parquet data files under " + tablePath);
    assertTrue(parquetFiles.length > 0, "Expected at least one parquet data file");
    Path parquetPath = new Path(parquetFiles[0].getAbsolutePath());
    List<org.apache.parquet.schema.Type> fields =
        ParquetFileReader.readFooter(
                spark.sessionState().newHadoopConf(),
                parquetPath,
                ParquetMetadataConverter.NO_FILTER)
            .getFileMetaData()
            .getSchema()
            .getFields();
    List<String> parquetFieldNames =
        fields.stream()
            .map(org.apache.parquet.schema.Type::getName)
            .collect(java.util.stream.Collectors.toList());
    for (String logicalName : logicalColumnNames) {
      assertFalse(
          parquetFieldNames.contains(logicalName),
          "Parquet schema should not contain logical column name '"
              + logicalName
              + "'; got fields: "
              + parquetFieldNames);
    }
    assertTrue(
        parquetFieldNames.stream().allMatch(name -> name.startsWith("col-")),
        "Expected physical col-* column names in Parquet, got: " + parquetFieldNames);
    for (org.apache.parquet.schema.Type field : fields) {
      assertNotNull(
          field.getId(), "Expected a Parquet field id on column '" + field.getName() + "'");
    }
  }

  /**
   * Verifies the Parquet body under {@code partitionDir} holds only the data columns. Partition
   * columns are path-encoded, not materialized into the file.
   */
  private void assertParquetBodyOmitsPartitionColumns(File partitionDir, int expectedDataColumns)
      throws Exception {
    File[] parquetFiles = partitionDir.listFiles((dir, name) -> name.endsWith(".parquet"));
    assertNotNull(parquetFiles, "Expected parquet data files under " + partitionDir);
    assertTrue(parquetFiles.length > 0, "Expected at least one parquet data file");
    List<String> parquetFieldNames =
        ParquetFileReader.readFooter(
                spark.sessionState().newHadoopConf(),
                new Path(parquetFiles[0].getAbsolutePath()),
                ParquetMetadataConverter.NO_FILTER)
            .getFileMetaData().getSchema().getFields().stream()
            .map(org.apache.parquet.schema.Type::getName)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(
        expectedDataColumns,
        parquetFieldNames.size(),
        "Expected only the data columns in the Parquet body, got: " + parquetFieldNames);
  }

  @ParameterizedTest(name = "enableVariantShredding={0}")
  @ValueSource(booleans = {false, true})
  public void variantWriteFollowsTableShreddingProperty(
      boolean shreddingEnabled, @TempDir File deltaTablePath) throws Exception {
    // The opt-in arm asserts that shredding happens, which needs a Spark version that can infer a
    // shredding schema. The opt-out arm asserts it does not, which holds on every version.
    assumeTrue(!shreddingEnabled || shreddedWritesSupported(), SHREDDING_UNSUPPORTED);
    String v2Path = new File(deltaTablePath, "v2").getAbsolutePath();
    String v1Path = new File(deltaTablePath, "v1").getAbsolutePath();
    withShreddedWritesAllowed(
        () -> {
          for (String path : List.of(v1Path, v2Path)) {
            spark.sql(
                str(
                    "CREATE TABLE delta.`%s` (id INT, v VARIANT) USING delta "
                        + "TBLPROPERTIES ('%s' = '%s')",
                    path, DeltaConfigs.ENABLE_VARIANT_SHREDDING().key(), shreddingEnabled));
          }
          spark.sql(str("INSERT INTO delta.`%s` %s", v1Path, VARIANT_ROW));
          spark.sql(str("INSERT INTO dsv2.delta.`%s` %s", v2Path, VARIANT_ROW));

          assertEquals(
              shreddingEnabled,
              snapshotHasShreddedVariant(str("delta.`%s`", v1Path)),
              "V1 shredding state must follow the table property");
          assertEquals(
              shreddingEnabled,
              snapshotHasShreddedVariant(str("delta.`%s`", v2Path)),
              "V2 shredding state must follow the table property, like V1");

          List<List<Object>> expected = List.of(row(1, 1, "xy"));
          check(str("%s FROM dsv2.delta.`%s`", VARIANT_PROJECTION, v2Path), expected);
          check(str("%s FROM delta.`%s`", VARIANT_PROJECTION, v2Path), expected);
        });
  }

  /**
   * The state left behind by {@code REORG ... APPLY (UNSHRED VARIANT)}: the {@code
   * variantShredding} feature stays in the protocol while the table property is gone. Shredding
   * must follow the property, not the protocol, so a later write must not silently re-shred the
   * table.
   */
  @Test
  public void variantWriteFollowsPropertyAfterUnshred(@TempDir File deltaTablePath) {
    // Seeds a shredded table before unshredding it, so it needs shredding support.
    assumeTrue(shreddedWritesSupported(), SHREDDING_UNSUPPORTED);
    String tablePath = new File(deltaTablePath, "unshred").getAbsolutePath();
    String tbl = "variant_unshred_tbl";
    withShreddedWritesAllowed(
        () -> {
          try {
            spark.sql(str("DROP TABLE IF EXISTS %s", tbl));
            spark.sql(
                str(
                    "CREATE TABLE %s (id INT, v VARIANT) USING delta LOCATION '%s' "
                        + "TBLPROPERTIES ('%s' = 'true')",
                    tbl, tablePath, DeltaConfigs.ENABLE_VARIANT_SHREDDING().key()));
            spark.sql(str("INSERT INTO %s %s", tbl, VARIANT_ROW));
            assertTrue(snapshotHasShreddedVariant(tbl), "Expected the opt-in write to be shredded");

            // Unsets the property and rewrites the files; the protocol feature stays behind.
            spark.sql(str("REORG TABLE %s APPLY (UNSHRED VARIANT)", tbl));
            assertFalse(
                snapshotHasShreddedVariant(tbl), "REORG UNSHRED must leave no shredded file");
            assertTrue(
                tableFeatures(tbl).contains("variantShredding"),
                "Premise: the protocol feature outlives the property, so the two cannot be "
                    + "conflated");
            assertFalse(
                tableProperties(tbl).containsKey(DeltaConfigs.ENABLE_VARIANT_SHREDDING().key()),
                "Premise: REORG UNSHRED leaves the property absent rather than false");

            // Neither connector may re-shred while the property is absent.
            spark.sql(str("INSERT INTO dsv2.delta.`%s` %s", tablePath, VARIANT_ROW));
            assertFalse(
                snapshotHasShreddedVariant(tbl),
                "A DSv2 write must not re-shred a table whose property is absent");
            spark.sql(str("INSERT INTO %s %s", tbl, VARIANT_ROW));
            assertFalse(snapshotHasShreddedVariant(tbl), "A V1 write must not re-shred it either");
          } finally {
            spark.sql(str("DROP TABLE IF EXISTS %s", tbl));
          }
        });
  }

  /**
   * The table-derived option must win over a caller-supplied spelling of the same option that
   * differs only in case: Parquet reads write options through a case-insensitive map, so leaving
   * both spellings in place would make the effective value depend on map iteration order.
   *
   * <p>This covers the end-to-end outcome only: which of two colliding spellings survives the
   * collapse is decided by map iteration order, which a test cannot steer from out here, so this
   * passes with or without the normalization. {@code
   * DeltaV2WriteContextTest#mergeVariantShreddingOptionsOverridesCallerSpellings} asserts on the
   * merge itself and does fail without it. What this one still catches is writer options being
   * allowed to override the table-derived value outright.
   */
  @Test
  public void variantWriteIgnoresMixedCaseInferShreddingOption(@TempDir File deltaTablePath) {
    String tablePath = new File(deltaTablePath, "mixedcase").getAbsolutePath();
    withShreddedWritesAllowed(
        () -> {
          spark.sql(
              str(
                  "CREATE TABLE delta.`%s` (id INT, v VARIANT) USING delta "
                      + "TBLPROPERTIES ('%s' = 'false')",
                  tablePath, DeltaConfigs.ENABLE_VARIANT_SHREDDING().key()));
          spark
              .sql(VARIANT_ROW)
              .writeTo(str("dsv2.delta.`%s`", tablePath))
              .option("SPARK.SQL.Variant.InferShreddingSchema", "true")
              .append();
          assertFalse(
              snapshotHasShreddedVariant(str("delta.`%s`", tablePath)),
              "A mixed-case write option must not override the table property");
        });
  }

  private static final String SHREDDING_UNSUPPORTED =
      "This Spark version cannot infer a variant shredding schema, so nothing shreds on write";

  /**
   * Whether writes can shred variant columns on this Spark version. Asked of the production shim
   * rather than of a version string: the shim yields no inference option where the underlying conf
   * does not exist, and without that option the writer is never asked to shred.
   */
  private static boolean shreddedWritesSupported() {
    return !VariantShreddingShims.getVariantInferShreddingSchemaOptions(true).isEmpty();
  }

  private static final String VARIANT_ROW =
      "SELECT 1 AS id, parse_json('{\"a\":1,\"b\":\"xy\"}') AS v";

  private static final String VARIANT_PROJECTION =
      "SELECT id, variant_get(v, '$.a', 'int'), variant_get(v, '$.b', 'string')";

  /** Runs {@code body} with shredded Parquet writes allowed, restoring the previous setting. */
  private void withShreddedWritesAllowed(ThrowingRunnable body) {
    String key = "spark.sql.variant.writeShredding.enabled";
    Option<String> previous = spark.conf().getOption(key);
    spark.conf().set(key, "true");
    try {
      body.run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (previous.isDefined()) {
        spark.conf().set(key, previous.get());
      } else {
        spark.conf().unset(key);
      }
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  /** Whether any data file in {@code table}'s current snapshot stores the variant shredded. */
  private boolean snapshotHasShreddedVariant(String table) throws Exception {
    List<org.apache.spark.sql.Row> files =
        spark.sql(str("SELECT DISTINCT input_file_name() AS f FROM %s", table)).collectAsList();
    assertFalse(files.isEmpty(), "Expected at least one data file in " + table);
    for (org.apache.spark.sql.Row file : files) {
      org.apache.parquet.schema.Type variant =
          ParquetFileReader.readFooter(
                  spark.sessionState().newHadoopConf(),
                  new Path(file.getString(0)),
                  ParquetMetadataConverter.NO_FILTER)
              .getFileMetaData()
              .getSchema()
              .getType("v");
      assertTrue(
          variant instanceof org.apache.parquet.schema.GroupType,
          "Expected the variant column to be a Parquet group, got: " + variant);
      if (((org.apache.parquet.schema.GroupType) variant).containsField("typed_value")) {
        return true;
      }
    }
    return false;
  }

  private String tableFeatures(String tbl) {
    return spark
        .sql(str("DESCRIBE DETAIL %s", tbl))
        .selectExpr("tableFeatures")
        .collectAsList()
        .toString();
  }

  private Map<String, String> tableProperties(String tbl) {
    Map<String, String> properties = new java.util.HashMap<>();
    spark
        .sql(str("SHOW TBLPROPERTIES %s", tbl))
        .collectAsList()
        .forEach(r -> properties.put(r.getString(0), r.getString(1)));
    return properties;
  }
}
