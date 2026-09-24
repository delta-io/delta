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

package io.delta.spark.internal.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.hooks.AutoCompact$;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Option;
import scala.collection.JavaConverters;

/**
 * Matrix coverage for DSv2 streaming source reads over Delta tables created and mutated in
 * different ways.
 *
 * <p>The matrix intentionally omits unsupported operations in this repo (COPY INTO, PK/FK,
 * compute-delta-statistics, deep clone).
 */
public class V2StreamingTableCombinationTest extends V2TestBase {

  private enum Feature {
    PARTITIONED,
    CLUSTERED,
    GENERATED,
    DEFAULTS,
    IDENTITY,
    CONSTRAINT,
    COLUMN_MAPPING,
    DV_ENABLED
  }

  private enum CreationScenario {
    SIMPLE(EnumSet.noneOf(Feature.class)),
    REPLACED(EnumSet.noneOf(Feature.class)),
    WIDE_TYPES(EnumSet.noneOf(Feature.class)),
    PARTITIONED(EnumSet.of(Feature.PARTITIONED)),
    CLUSTERED(EnumSet.of(Feature.CLUSTERED)),
    GENERATED_COLUMNS(EnumSet.of(Feature.GENERATED)),
    DEFAULT_COLUMNS(EnumSet.of(Feature.DEFAULTS)),
    IDENTITY_COLUMNS(EnumSet.of(Feature.IDENTITY)),
    CHECK_CONSTRAINTS(EnumSet.of(Feature.CONSTRAINT)),
    COLUMN_MAPPING_NAME(EnumSet.of(Feature.COLUMN_MAPPING)),
    DELETION_VECTORS(EnumSet.of(Feature.DV_ENABLED));

    private final EnumSet<Feature> features;

    CreationScenario(EnumSet<Feature> features) {
      this.features = features;
    }

    EnumSet<Feature> features() {
      return EnumSet.copyOf(features);
    }
  }

  private enum OperationScenario {
    INSERT_APPEND,
    INSERT_OVERWRITE,
    INSERT_REPLACE_WHERE,
    INSERT_DPO,
    UPDATE,
    DELETE,
    MERGE,
    TRUNCATE,
    RESTORE,
    CLONE_SHALLOW,
    VACUUM,
    OPTIMIZE_FOREGROUND,
    OPTIMIZE_ZORDER,
    OPTIMIZE_LIQUID,
    OPTIMIZE_BACKGROUND,
    REORG_PURGE,
    CHECKPOINT,
    ALTER_ADD_COLUMN,
    ALTER_RENAME_COLUMN,
    ALTER_DROP_COLUMN,
    ALTER_CHANGE_CLUSTERING,
    ALTER_ADD_DROP_CONSTRAINT,
    AUTO_SCHEMA_EVOLUTION_INSERT,
    AUTO_SCHEMA_EVOLUTION_MERGE;

    boolean applicableTo(CreationScenario creation) {
      EnumSet<Feature> features = creation.features();
      switch (this) {
        case INSERT_REPLACE_WHERE:
        case INSERT_DPO:
          return features.contains(Feature.PARTITIONED);
        case OPTIMIZE_LIQUID:
          return features.contains(Feature.CLUSTERED);
        case OPTIMIZE_ZORDER:
          return !features.contains(Feature.CLUSTERED);
        case REORG_PURGE:
          return features.contains(Feature.DV_ENABLED);
        case ALTER_RENAME_COLUMN:
        case ALTER_DROP_COLUMN:
          return features.contains(Feature.COLUMN_MAPPING);
        case ALTER_CHANGE_CLUSTERING:
          // Cluster-by and partitioned layouts are mutually exclusive in Delta.
          return !features.contains(Feature.PARTITIONED);
        case ALTER_ADD_COLUMN:
          // Keep the post-alter append shape simple in this matrix. Partitioned tables already
          // exercise schema evolution through dedicated INSERT/DPO coverage.
          return !features.contains(Feature.PARTITIONED);
        case AUTO_SCHEMA_EVOLUTION_INSERT:
        case AUTO_SCHEMA_EVOLUTION_MERGE:
          // These cases are about schema evolution itself, so start from the smallest baseline.
          return creation == CreationScenario.SIMPLE;
        default:
          return true;
      }
    }
  }

  private static final class TableRef {
    final String path;
    final EnumSet<Feature> features;
    final String dataColumn;

    TableRef(String path, EnumSet<Feature> features, String dataColumn) {
      this.path = path;
      this.features = EnumSet.copyOf(features);
      this.dataColumn = dataColumn;
    }

    String sqlIdent() {
      return deltaPathIdent(path);
    }

    boolean hasFeature(Feature feature) {
      return features.contains(feature);
    }

    TableRef withPath(String newPath) {
      return new TableRef(newPath, features, dataColumn);
    }

    TableRef withDataColumn(String newDataColumn) {
      return new TableRef(path, features, newDataColumn);
    }
  }

  static Stream<Arguments> matrixCases() {
    return Arrays.stream(CreationScenario.values())
        .filter(V2StreamingTableCombinationTest::isSupportedMatrixCreation)
        .flatMap(
            creation ->
                Arrays.stream(OperationScenario.values())
                    .filter(op -> op.applicableTo(creation))
                    .map(op -> Arguments.of(creation, op)));
  }

  private static boolean isSupportedMatrixCreation(CreationScenario creation) {
    switch (creation) {
      case GENERATED_COLUMNS:
      case IDENTITY_COLUMNS:
        // The matrix uses generic append/update helpers with explicit column lists. Generated and
        // identity columns need scenario-specific write plans, so keep them covered elsewhere
        // rather than making every mutation helper branch on them here.
        return false;
      default:
        return true;
    }
  }

  @ParameterizedTest(name = "{index} => create={0}, op={1}")
  @MethodSource("matrixCases")
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  public void testDsv2StreamingTableCombination(
      CreationScenario creation, OperationScenario operation, @TempDir File tempDir)
      throws Exception {
    Path caseDir =
        tempDir
            .toPath()
            .resolve(creation.name().toLowerCase(Locale.ROOT))
            .resolve(operation.name().toLowerCase(Locale.ROOT));
    Files.createDirectories(caseDir);

    try {
      // Each matrix case follows the same lifecycle: create the table, apply one mutation, then
      // verify the streaming source against the full table snapshot before and after a final
      // append. Keeping that sequence centralized makes the case matrix easier to scan.
      TableRef table = createTableByScenario(creation, caseDir);
      TableRef operated = applyOperation(operation, caseDir, table);

      verifyStreamingInitialAndPostAppend(
          operated,
          caseDir,
          creation.name().toLowerCase(Locale.ROOT)
              + "_"
              + operation.name().toLowerCase(Locale.ROOT));
    } catch (Exception e) {
      throw new RuntimeException(
          str("Matrix case failed: creation=%s operation=%s", creation, operation), e);
    }
  }

  @Test
  public void testConvertToDeltaStreamingSource(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.toPath().resolve("convert_table").toString();

    spark
        .range(0, 8)
        .selectExpr("cast(id as int) as id", "concat('c', cast(id as string)) as data")
        .write()
        .format("parquet")
        .save(tablePath);

    spark.sql(str("CONVERT TO DELTA parquet.`%s`", escapePath(tablePath)));

    TableRef table = new TableRef(tablePath, EnumSet.noneOf(Feature.class), "data");
    verifyStreamingInitialAndPostAppend(table, tempDir.toPath(), "convert_to_delta");
  }

  private TableRef createTableByScenario(CreationScenario creation, Path caseDir) {
    switch (creation) {
      case SIMPLE:
        return createSimpleTable(caseDir);
      case REPLACED:
        return createReplacedTable(caseDir);
      case WIDE_TYPES:
        return createWideTypesTable(caseDir);
      case PARTITIONED:
        return createPartitionedTable(caseDir);
      case CLUSTERED:
        return createClusteredTable(caseDir);
      case GENERATED_COLUMNS:
        return createGeneratedColumnTable(caseDir);
      case DEFAULT_COLUMNS:
        return createDefaultColumnTable(caseDir);
      case IDENTITY_COLUMNS:
        return createIdentityColumnTable(caseDir);
      case CHECK_CONSTRAINTS:
        return createCheckConstraintTable(caseDir);
      case COLUMN_MAPPING_NAME:
        return createColumnMappingTable(caseDir);
      case DELETION_VECTORS:
        return createDeletionVectorTable(caseDir);
      default:
        throw new IllegalArgumentException("Unknown creation scenario: " + creation);
    }
  }

  private TableRef applyOperation(OperationScenario operation, Path caseDir, TableRef table) {
    switch (operation) {
      case INSERT_APPEND:
        return applyInsertAppend(table);
      case INSERT_OVERWRITE:
        return applyInsertOverwrite(table);
      case INSERT_REPLACE_WHERE:
        return applyInsertReplaceWhere(table);
      case INSERT_DPO:
        return applyInsertDynamicPartitionOverwrite(table);
      case UPDATE:
        return applyUpdate(table);
      case DELETE:
        return applyDelete(table);
      case MERGE:
        return applyMergeOperation(table);
      case TRUNCATE:
        return applyTruncate(table);
      case RESTORE:
        return applyRestore(table);
      case CLONE_SHALLOW:
        return applyShallowClone(caseDir, table);
      case VACUUM:
        return applyVacuum(table);
      case OPTIMIZE_FOREGROUND:
        return applyOptimizeForeground(table);
      case OPTIMIZE_ZORDER:
        return applyOptimizeZOrder(table);
      case OPTIMIZE_LIQUID:
        return applyOptimizeLiquid(table);
      case OPTIMIZE_BACKGROUND:
        return applyOptimizeBackground(table);
      case REORG_PURGE:
        return applyReorgPurge(table);
      case CHECKPOINT:
        return applyCheckpoint(table);
      case ALTER_ADD_COLUMN:
        return applyAlterAddColumn(table);
      case ALTER_RENAME_COLUMN:
        return applyAlterRenameColumn(table);
      case ALTER_DROP_COLUMN:
        return applyAlterDropColumn(table);
      case ALTER_CHANGE_CLUSTERING:
        return applyAlterChangeClustering(table);
      case ALTER_ADD_DROP_CONSTRAINT:
        return applyAlterAddDropConstraint(table);
      case AUTO_SCHEMA_EVOLUTION_INSERT:
        return applyAutoSchemaEvolutionInsert(table);
      case AUTO_SCHEMA_EVOLUTION_MERGE:
        return applyAutoSchemaEvolutionMerge(table);
      default:
        throw new IllegalArgumentException("Unknown operation scenario: " + operation);
    }
  }

  private void verifyStreamingInitialAndPostAppend(TableRef table, Path caseDir, String label)
      throws Exception {
    String queryName =
        "dsv2_stream_"
            + label.replaceAll("[^A-Za-z0-9_]", "_")
            + "_"
            + UUID.randomUUID().toString().replace('-', '_');

    Path checkpointPath = caseDir.resolve("checkpoint").resolve(queryName);
    Files.createDirectories(checkpointPath);

    StreamingQuery query = null;
    try {
      Dataset<Row> streamDf = spark.readStream().table(dsv2DeltaPathIdent(table.path));
      query =
          streamDf
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .option("checkpointLocation", checkpointPath.toString())
              .start();

      query.processAllAvailable();
      assertDatasetMultisetEquals(
          spark.sql(str("SELECT * FROM %s", queryName)),
          spark.read().format("delta").load(table.path));

      appendRows(table, 10000, 1);
      query.processAllAvailable();

      Dataset<Row> actualAfterAppend = spark.sql(str("SELECT * FROM %s", queryName));
      Dataset<Row> expectedAfterAppend = spark.read().format("delta").load(table.path);
      assertDatasetMultisetEquals(actualAfterAppend, expectedAfterAppend);

      assertFalse(actualAfterAppend.isEmpty(), "streaming result should not be empty after append");
    } finally {
      if (query != null) {
        query.stop();
      }
      spark.sql(str("DROP VIEW IF EXISTS %s", queryName));
    }
  }

  private Dataset<Row> canonicalize(Dataset<Row> df) {
    String[] sortedColumns = df.columns().clone();
    Arrays.sort(sortedColumns, Comparator.naturalOrder());
    Column[] projection = Arrays.stream(sortedColumns).map(functions::col).toArray(Column[]::new);
    return df.select(projection);
  }

  private void assertDatasetMultisetEquals(Dataset<Row> actual, Dataset<Row> expected) {
    Dataset<Row> canonicalActual = canonicalize(actual);
    Dataset<Row> canonicalExpected = canonicalize(expected);

    long actualMinusExpected = canonicalActual.exceptAll(canonicalExpected).count();
    long expectedMinusActual = canonicalExpected.exceptAll(canonicalActual).count();

    assertEquals(0L, actualMinusExpected, "actual has unexpected extra rows");
    assertEquals(0L, expectedMinusActual, "actual is missing expected rows");
  }

  private void appendRows(TableRef table, int startId, int count) {
    List<String> valueLiterals = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      int id = startId + i;
      if (table.hasFeature(Feature.PARTITIONED)) {
        valueLiterals.add(str("(%d, 'v%d', %d)", id, id, id % 2));
      } else {
        valueLiterals.add(str("(%d, 'v%d')", id, id));
      }
    }

    String columns =
        table.hasFeature(Feature.PARTITIONED)
            ? str("id, %s, part", table.dataColumn)
            : str("id, %s", table.dataColumn);

    spark.sql(
        str(
            "INSERT INTO %s (%s) VALUES %s",
            table.sqlIdent(), columns, String.join(", ", valueLiterals)));
  }

  private void overwriteRows(TableRef table, int startId, int count) {
    List<String> valueLiterals = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      int id = startId + i;
      if (table.hasFeature(Feature.PARTITIONED)) {
        valueLiterals.add(str("(%d, 'ov%d', %d)", id, id, id % 2));
      } else {
        valueLiterals.add(str("(%d, 'ov%d')", id, id));
      }
    }

    String columns =
        table.hasFeature(Feature.PARTITIONED)
            ? str("id, %s, part", table.dataColumn)
            : str("id, %s", table.dataColumn);

    spark.sql(
        str(
            "INSERT OVERWRITE TABLE %s (%s) VALUES %s",
            table.sqlIdent(), columns, String.join(", ", valueLiterals)));
  }

  private void applyReplaceWhere(TableRef table) {
    String srcView = "replace_where_src_" + UUID.randomUUID().toString().replace('-', '_');
    StructType schema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField(table.dataColumn, DataTypes.StringType, false),
                DataTypes.createStructField("part", DataTypes.IntegerType, false)));

    try {
      spark
          .createDataFrame(
              Arrays.asList(RowFactory.create(801, "rw801", 1), RowFactory.create(803, "rw803", 1)),
              schema)
          .createOrReplaceTempView(srcView);

      spark.sql(
          str(
              "INSERT INTO %s REPLACE WHERE part = 1 SELECT id, %s, part FROM %s",
              table.sqlIdent(), table.dataColumn, srcView));
    } finally {
      spark.catalog().dropTempView(srcView);
    }
  }

  private void applyDynamicPartitionOverwrite(TableRef table) {
    StructType schema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField(table.dataColumn, DataTypes.StringType, false),
                DataTypes.createStructField("part", DataTypes.IntegerType, false)));

    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(901, "dpo901", 1),
                RowFactory.create(903, "dpo903", 1),
                RowFactory.create(902, "dpo902", 0)),
            schema)
        .write()
        .format("delta")
        .mode(SaveMode.Overwrite)
        .option("partitionOverwriteMode", "dynamic")
        .save(table.path);
  }

  private void applyMerge(TableRef table, String sourcePrefix, int startId, int rows) {
    String srcView = sourcePrefix + "_" + UUID.randomUUID().toString().replace('-', '_');
    try {
      if (table.hasFeature(Feature.PARTITIONED)) {
        StructType sourceSchema =
            DataTypes.createStructType(
                Arrays.asList(
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField(table.dataColumn, DataTypes.StringType, false),
                    DataTypes.createStructField("part", DataTypes.IntegerType, false)));
        List<Row> srcRows = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
          int id = startId + i;
          srcRows.add(RowFactory.create(id, "m" + id, id % 2));
        }
        spark.createDataFrame(srcRows, sourceSchema).createOrReplaceTempView(srcView);
        spark.sql(
            str(
                "MERGE INTO %s t USING %s s ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET t.%s = s.%s "
                    + "WHEN NOT MATCHED THEN INSERT (id, %s, part) VALUES (s.id, s.%s, s.part)",
                table.sqlIdent(),
                srcView,
                table.dataColumn,
                table.dataColumn,
                table.dataColumn,
                table.dataColumn));
      } else {
        StructType sourceSchema =
            DataTypes.createStructType(
                Arrays.asList(
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField(table.dataColumn, DataTypes.StringType, false)));
        List<Row> srcRows = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
          int id = startId + i;
          srcRows.add(RowFactory.create(id, "m" + id));
        }
        spark.createDataFrame(srcRows, sourceSchema).createOrReplaceTempView(srcView);
        spark.sql(
            str(
                "MERGE INTO %s t USING %s s ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET t.%s = s.%s "
                    + "WHEN NOT MATCHED THEN INSERT (id, %s) VALUES (s.id, s.%s)",
                table.sqlIdent(),
                srcView,
                table.dataColumn,
                table.dataColumn,
                table.dataColumn,
                table.dataColumn));
      }
    } finally {
      spark.catalog().dropTempView(srcView);
    }
  }

  private void applyAutoSchemaMerge(TableRef table, String sourcePrefix) {
    String srcView = sourcePrefix + "_" + UUID.randomUUID().toString().replace('-', '_');
    StructType sourceSchema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField(table.dataColumn, DataTypes.StringType, false),
                DataTypes.createStructField("new_col", DataTypes.IntegerType, false)));

    try {
      spark
          .createDataFrame(
              Arrays.asList(
                  RowFactory.create(1200, "mg1200", 1200), RowFactory.create(1201, "mg1201", 1201)),
              sourceSchema)
          .createOrReplaceTempView(srcView);

      withSQLConf(
          "spark.databricks.delta.schema.autoMerge.enabled",
          "true",
          () ->
              spark.sql(
                  str(
                      "MERGE INTO %s t USING %s s ON t.id = s.id "
                          + "WHEN MATCHED THEN UPDATE SET t.%s = s.%s "
                          + "WHEN NOT MATCHED THEN INSERT *",
                      table.sqlIdent(), srcView, table.dataColumn, table.dataColumn)));
      spark.sql(str("UPDATE %s SET new_col = coalesce(new_col, -1)", table.sqlIdent()));
    } finally {
      spark.catalog().dropTempView(srcView);
    }
  }

  private void writeSmallFileCommits(TableRef table, int commits) {
    for (int i = 0; i < commits; i++) {
      appendRows(table, 20000 + i, 1);
    }
  }

  private static TableRef newTableRef(Path caseDir, EnumSet<Feature> features) {
    return new TableRef(caseDir.resolve("table").toString(), features, "data");
  }

  private TableRef createSimpleTable(Path caseDir) {
    TableRef table = newTableRef(caseDir, CreationScenario.SIMPLE.features());
    spark.sql(str("CREATE TABLE %s (id INT, data STRING) USING delta", table.sqlIdent()));
    appendRows(table, 0, 8);
    return table;
  }

  private TableRef createReplacedTable(Path caseDir) {
    TableRef table = newTableRef(caseDir, CreationScenario.REPLACED.features());
    spark.sql(str("CREATE TABLE %s (id INT, data STRING) USING delta", table.sqlIdent()));
    appendRows(table, 0, 3);
    spark.sql(str("REPLACE TABLE %s (id INT, data STRING) USING delta", table.sqlIdent()));
    appendRows(table, 10, 8);
    return table;
  }

  private TableRef createWideTypesTable(Path caseDir) {
    TableRef table = newTableRef(caseDir, CreationScenario.WIDE_TYPES.features());
    spark.sql(
        str(
            "CREATE TABLE %s ("
                + "id INT, "
                + "data STRING, "
                + "flag BOOLEAN, "
                + "amount DECIMAL(10,2), "
                + "event_date DATE, "
                + "event_ts TIMESTAMP, "
                + "arr ARRAY<INT>, "
                + "meta STRUCT<k:STRING,v:INT>, "
                + "tags MAP<STRING,INT>, "
                + "blob BINARY"
                + ") USING delta",
            table.sqlIdent()));
    appendRows(table, 0, 8);
    return table;
  }

  private TableRef createPartitionedTable(Path caseDir) {
    TableRef table = newTableRef(caseDir, CreationScenario.PARTITIONED.features());
    spark.sql(
        str(
            "CREATE TABLE %s (id INT, data STRING, part INT) USING delta PARTITIONED BY (part)",
            table.sqlIdent()));
    appendRows(table, 0, 12);
    return table;
  }

  private TableRef createClusteredTable(Path caseDir) {
    TableRef table = newTableRef(caseDir, CreationScenario.CLUSTERED.features());
    spark.sql(
        str("CREATE TABLE %s (id INT, data STRING) USING delta CLUSTER BY (id)", table.sqlIdent()));
    appendRows(table, 0, 12);
    return table;
  }

  private TableRef createGeneratedColumnTable(Path caseDir) {
    TableRef table = newTableRef(caseDir, CreationScenario.GENERATED_COLUMNS.features());
    spark.sql(
        str(
            "CREATE TABLE %s ("
                + "id INT, "
                + "data STRING, "
                + "id_plus_one INT GENERATED ALWAYS AS (id + 1)"
                + ") USING delta",
            table.sqlIdent()));
    appendRows(table, 0, 8);
    return table;
  }

  private TableRef createDefaultColumnTable(Path caseDir) {
    TableRef table = newTableRef(caseDir, CreationScenario.DEFAULT_COLUMNS.features());
    spark.sql(
        str(
            "CREATE TABLE %s (id INT, data STRING, d INT DEFAULT 2) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')",
            table.sqlIdent()));
    appendRows(table, 0, 8);
    return table;
  }

  private TableRef createIdentityColumnTable(Path caseDir) {
    TableRef table = newTableRef(caseDir, CreationScenario.IDENTITY_COLUMNS.features());
    spark.sql(
        str(
            "CREATE TABLE %s ("
                + "ident BIGINT GENERATED BY DEFAULT AS IDENTITY, "
                + "id INT, "
                + "data STRING"
                + ") USING delta",
            table.sqlIdent()));
    appendRows(table, 0, 8);
    return table;
  }

  private TableRef createCheckConstraintTable(Path caseDir) {
    TableRef table = newTableRef(caseDir, CreationScenario.CHECK_CONSTRAINTS.features());
    spark.sql(str("CREATE TABLE %s (id INT, data STRING) USING delta", table.sqlIdent()));
    spark.sql(
        str("ALTER TABLE %s ADD CONSTRAINT id_non_negative CHECK (id >= 0)", table.sqlIdent()));
    appendRows(table, 0, 8);
    return table;
  }

  private TableRef createColumnMappingTable(Path caseDir) {
    TableRef table = newTableRef(caseDir, CreationScenario.COLUMN_MAPPING_NAME.features());
    spark.sql(
        str(
            "CREATE TABLE %s (id INT, data STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            table.sqlIdent()));
    appendRows(table, 0, 8);
    return table;
  }

  private TableRef createDeletionVectorTable(Path caseDir) {
    TableRef table = newTableRef(caseDir, CreationScenario.DELETION_VECTORS.features());
    spark.sql(
        str(
            "CREATE TABLE %s (id INT, data STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            table.sqlIdent()));
    spark
        .range(0, 1000)
        .selectExpr("cast(id as int) as id", "concat('dv', cast(id as string)) as data")
        .coalesce(1)
        .write()
        .format("delta")
        .mode(SaveMode.Append)
        .save(table.path);
    return table;
  }

  private TableRef applyInsertAppend(TableRef table) {
    appendRows(table, 100, 2);
    return table;
  }

  private TableRef applyInsertOverwrite(TableRef table) {
    overwriteRows(table, 200, 3);
    return table;
  }

  private TableRef applyInsertReplaceWhere(TableRef table) {
    applyReplaceWhere(table);
    return table;
  }

  private TableRef applyInsertDynamicPartitionOverwrite(TableRef table) {
    applyDynamicPartitionOverwrite(table);
    return table;
  }

  private TableRef applyUpdate(TableRef table) {
    spark.sql(
        str(
            "UPDATE %s SET %s = concat(%s, '_u') WHERE id %% 2 = 0",
            table.sqlIdent(), table.dataColumn, table.dataColumn));
    return table;
  }

  private TableRef applyDelete(TableRef table) {
    spark.sql(str("DELETE FROM %s WHERE id %% 3 = 0", table.sqlIdent()));
    return table;
  }

  private TableRef applyMergeOperation(TableRef table) {
    applyMerge(table, "merge_src", 300, 3);
    return table;
  }

  private TableRef applyTruncate(TableRef table) {
    spark.sql(str("DELETE FROM %s", table.sqlIdent()));
    return table;
  }

  private TableRef applyRestore(TableRef table) {
    long restoreVersion =
        DeltaLog.forTable(spark, table.path)
            .update(false, Option.empty(), Option.empty())
            .version();
    appendRows(table, 400, 2);
    spark.sql(str("RESTORE TABLE %s TO VERSION AS OF %d", table.sqlIdent(), restoreVersion));
    return table;
  }

  private TableRef applyShallowClone(Path caseDir, TableRef table) {
    String clonePath = caseDir.resolve("clone_target").toString();
    spark.sql(str("CREATE TABLE %s SHALLOW CLONE %s", deltaPathIdent(clonePath), table.sqlIdent()));
    return table.withPath(clonePath);
  }

  private TableRef applyVacuum(TableRef table) {
    withSQLConf(
        "spark.databricks.delta.retentionDurationCheck.enabled",
        "false",
        () -> spark.sql(str("VACUUM %s RETAIN 0 HOURS", table.sqlIdent())));
    return table;
  }

  private TableRef applyOptimizeForeground(TableRef table) {
    writeSmallFileCommits(table, 4);
    spark.sql(str("OPTIMIZE %s", table.sqlIdent()));
    return table;
  }

  private TableRef applyOptimizeZOrder(TableRef table) {
    writeSmallFileCommits(table, 4);
    spark.sql(str("OPTIMIZE %s ZORDER BY (id)", table.sqlIdent()));
    return table;
  }

  private TableRef applyOptimizeLiquid(TableRef table) {
    writeSmallFileCommits(table, 4);
    spark.sql(str("OPTIMIZE %s FULL", table.sqlIdent()));
    return table;
  }

  private TableRef applyOptimizeBackground(TableRef table) {
    writeSmallFileCommits(table, 4);
    AutoCompact$.MODULE$.compact(
        spark,
        DeltaLog.forTable(spark, table.path),
        Option.empty(),
        JavaConverters.<Expression>asScalaBuffer(new ArrayList<Expression>()).toSeq(),
        "delta.autoOptimize",
        Option.empty());
    return table;
  }

  private TableRef applyReorgPurge(TableRef table) {
    spark.sql(str("DELETE FROM %s WHERE id < 10", table.sqlIdent()));
    spark.sql(str("DELETE FROM %s WHERE id >= 10 AND id < 20", table.sqlIdent()));
    spark.sql(str("REORG TABLE %s APPLY (PURGE)", table.sqlIdent()));
    return table;
  }

  private TableRef applyCheckpoint(TableRef table) {
    DeltaLog.forTable(spark, table.path).checkpoint();
    return table;
  }

  private TableRef applyAlterAddColumn(TableRef table) {
    spark.sql(str("ALTER TABLE %s ADD COLUMNS (added_col INT)", table.sqlIdent()));
    spark.sql(str("UPDATE %s SET added_col = id", table.sqlIdent()));
    return table;
  }

  private TableRef applyAlterRenameColumn(TableRef table) {
    spark.sql(
        str("ALTER TABLE %s RENAME COLUMN %s TO data_renamed", table.sqlIdent(), table.dataColumn));
    return table.withDataColumn("data_renamed");
  }

  private TableRef applyAlterDropColumn(TableRef table) {
    spark.sql(str("ALTER TABLE %s ADD COLUMNS (drop_me STRING)", table.sqlIdent()));
    spark.sql(str("ALTER TABLE %s DROP COLUMN drop_me", table.sqlIdent()));
    return table;
  }

  private TableRef applyAlterChangeClustering(TableRef table) {
    spark.sql(str("ALTER TABLE %s CLUSTER BY (id)", table.sqlIdent()));
    return table;
  }

  private TableRef applyAlterAddDropConstraint(TableRef table) {
    spark.sql(str("ALTER TABLE %s ADD CONSTRAINT c_tmp CHECK (id >= 0)", table.sqlIdent()));
    spark.sql(str("ALTER TABLE %s DROP CONSTRAINT c_tmp", table.sqlIdent()));
    return table;
  }

  private TableRef applyAutoSchemaEvolutionInsert(TableRef table) {
    spark
        .range(500, 503)
        .selectExpr(
            "cast(id as int) as id",
            "concat('e', cast(id as string)) as data",
            "cast(id as int) as new_col")
        .write()
        .format("delta")
        .option("mergeSchema", "true")
        .mode(SaveMode.Append)
        .save(table.path);
    spark.sql(str("UPDATE %s SET new_col = coalesce(new_col, -1)", table.sqlIdent()));
    return table;
  }

  private TableRef applyAutoSchemaEvolutionMerge(TableRef table) {
    applyAutoSchemaMerge(table, "schema_merge_src");
    return table;
  }

  private static String deltaPathIdent(String path) {
    return str("delta.`%s`", escapePath(path));
  }

  private static String dsv2DeltaPathIdent(String path) {
    return str("dsv2.delta.`%s`", escapePath(path));
  }

  private static String escapePath(String path) {
    return path.replace("`", "``");
  }
}
