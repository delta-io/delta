/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.io.File;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.delta.shims.VariantShreddingShims;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link DeltaV2WriteContext}, the operation-independent base context shared by the
 * batch and streaming write paths. {@link DeltaV2BatchWriteContextTest} covers the batch subclass;
 * this exercises the base directly (the streaming path builds a bare {@code DeltaV2WriteContext}).
 */
public class DeltaV2WriteContextTest extends DeltaV2TestBase {

  @Test
  public void createInitializesOperationIndependentState(@TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    StructType tableSchema = tableSchema();
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    createKernelTable(path, tableSchema, engine);
    Snapshot snapshot = TableManager.loadSnapshot(path).build(engine);

    DeltaV2WriteContext context =
        DeltaV2WriteContext.create(
            engine,
            hadoopConf,
            path,
            snapshot,
            tableSchema,
            new StructType(),
            new TestLogicalWriteInfo(tableSchema),
            /* variantShreddingEnabled */ false);

    assertSame(engine, context.getEngine());
    assertNotNull(context.getOutputWriterFactory());
    assertNotNull(context.getSerializableHadoopConf());
    assertNotNull(context.getSerializableHadoopConf().value());

    // dataSchema is wired through unchanged (not re-derived from the snapshot); the (unpartitioned)
    // table has no partition columns.
    assertArrayEquals(tableSchema.fieldNames(), context.getDataSchema().fieldNames());
    assertEquals(0, context.getPartitionSchema().fields().length);

    String sessionTimeZone = spark.sessionState().conf().sessionLocalTimeZone();
    assertEquals(sessionTimeZone, context.getSessionTimeZoneId());
    assertEquals(ZoneId.of(sessionTimeZone), context.getSessionTimeZone());
  }

  @Test
  public void buildDataWriterFactoryProducesExecutorState(@TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    StructType tableSchema = tableSchema();
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    createKernelTable(path, tableSchema, engine);
    Snapshot snapshot = TableManager.loadSnapshot(path).build(engine);

    DeltaV2WriteContext context =
        DeltaV2WriteContext.create(
            engine,
            hadoopConf,
            path,
            snapshot,
            tableSchema,
            new StructType(),
            new TestLogicalWriteInfo(tableSchema),
            /* variantShreddingEnabled */ false);

    // The base is operation-independent: any transaction (here a WRITE txn off the snapshot) can be
    // turned into the executor-side factory. A real factory with a serialized txn state proves the
    // shared setup produced usable state.
    Transaction txn =
        snapshot
            .buildUpdateTableTransaction(DeltaV2WriteContext.getEngineInfo(), Operation.WRITE)
            .build(engine);
    DeltaV2DataWriterFactory factory = context.buildDataWriterFactory(txn);
    assertNotNull(factory);
  }

  @Test
  public void wiredPartitionSchemaIsPreserved(@TempDir File tempDir) throws Exception {
    // Table (value INT) partitioned by (part STRING). The data / partition schema split is wired in
    // from DeltaV2Table's SchemaProvider; the context preserves it rather than re-deriving.
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE part_ctx (value INT, part STRING) USING delta PARTITIONED BY (part) "
                + "LOCATION '%s'",
            path));
    StructType fullSchema =
        new StructType().add("value", DataTypes.IntegerType).add("part", DataTypes.StringType);
    StructType dataSchema = new StructType().add("value", DataTypes.IntegerType);
    StructType partitionSchema = new StructType().add("part", DataTypes.StringType);

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    Snapshot snapshot = TableManager.loadSnapshot(path).build(engine);

    DeltaV2WriteContext context =
        DeltaV2WriteContext.create(
            engine,
            hadoopConf,
            path,
            snapshot,
            dataSchema,
            partitionSchema,
            new TestLogicalWriteInfo(fullSchema),
            /* variantShreddingEnabled */ false);

    assertArrayEquals(dataSchema.fieldNames(), context.getDataSchema().fieldNames());
    assertArrayEquals(partitionSchema.fieldNames(), context.getPartitionSchema().fieldNames());
  }

  @Test
  public void buildDataWriterFactoryResolvesColumnsCaseInsensitively(@TempDir File tempDir)
      throws Exception {
    // Table (value INT) partitioned by (part STRING), but the incoming write schema uses a
    // different case for both columns. Column resolution must be case-insensitive (matching
    // validateDataSchema) so the writer factory builds instead of throwing on exact-case lookup.
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE part_ci (value INT, part STRING) USING delta PARTITIONED BY (part) "
                + "LOCATION '%s'",
            path));
    StructType mixedCaseWriteSchema =
        new StructType().add("VALUE", DataTypes.IntegerType).add("PART", DataTypes.StringType);
    StructType dataSchema = new StructType().add("value", DataTypes.IntegerType);
    StructType partitionSchema = new StructType().add("part", DataTypes.StringType);

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    Snapshot snapshot = TableManager.loadSnapshot(path).build(engine);

    DeltaV2WriteContext context =
        DeltaV2WriteContext.create(
            engine,
            hadoopConf,
            path,
            snapshot,
            dataSchema,
            partitionSchema,
            new TestLogicalWriteInfo(mixedCaseWriteSchema),
            /* variantShreddingEnabled */ false);

    Transaction txn =
        snapshot
            .buildUpdateTableTransaction(DeltaV2WriteContext.getEngineInfo(), Operation.WRITE)
            .build(engine);
    assertNotNull(context.buildDataWriterFactory(txn));
  }

  /**
   * The table-derived shredding option must be the only surviving spelling of its key.
   *
   * <p>Asserted on the merge rather than on the resulting file layout, because the layout cannot
   * tell which of two colliding spellings won: {@code ParquetOptions} collapses them through a
   * case-insensitive map in iteration order, which a test cannot steer. Deleting the normalization
   * in {@link DeltaV2WriteContext#mergeVariantShreddingOptions} fails this test.
   */
  @Test
  public void mergeVariantShreddingOptionsOverridesCallerSpellings() {
    Map<String, String> shreddingOptions =
        ScalaUtils.toJavaMap(VariantShreddingShims.getVariantInferShreddingSchemaOptions(false));
    assumeFalse(
        shreddingOptions.isEmpty(),
        "This Spark version has no shredding inference option to override");

    Map<String, String> callerOptions = new HashMap<>();
    callerOptions.put("unrelated.option", "kept");
    for (String key : shreddingOptions.keySet()) {
      // Two spellings that differ from the shim's key only in case, as a caller could pass.
      callerOptions.put(key.toUpperCase(Locale.ROOT), "true");
      callerOptions.put(key.substring(0, 1).toUpperCase(Locale.ROOT) + key.substring(1), "true");
    }

    Map<String, String> merged =
        DeltaV2WriteContext.mergeVariantShreddingOptions(callerOptions, false);

    assertEquals("kept", merged.get("unrelated.option"), "unrelated options must pass through");
    for (Map.Entry<String, String> expected : shreddingOptions.entrySet()) {
      List<String> spellings =
          merged.keySet().stream()
              .filter(key -> key.equalsIgnoreCase(expected.getKey()))
              .collect(Collectors.toList());
      assertEquals(
          Collections.singletonList(expected.getKey()),
          spellings,
          "only the table-derived spelling of " + expected.getKey() + " may survive the merge");
      assertEquals(
          expected.getValue(),
          merged.get(expected.getKey()),
          "the surviving value must come from the table property, not the caller");
    }
  }

  @Test
  public void engineInfoUsesExpectedPrefix() {
    assertTrue(DeltaV2WriteContext.getEngineInfo().startsWith("Apache-Spark/"));
  }

  /**
   * {@code variantLayoutFollowsProperty} must track the writer's actual shredding eligibility: a
   * variant at the top level or nested through structs, but not one reached through an array or map
   * element ({@code InferVariantShreddingSchema.getPathsToVariant}). For an unshreddable schema a
   * shredding-property change cannot alter any file, so the layout must not be marked
   * property-sensitive.
   */
  @Test
  public void variantLayoutFollowsPropertyMatchesShreddingEligibility(@TempDir File tempDir)
      throws Exception {
    StructType arrayOfVariant =
        new StructType()
            .add("id", DataTypes.IntegerType)
            .add("arr", DataTypes.createArrayType(DataTypes.VariantType, true));
    assertFalse(
        buildContext(new File(tempDir, "arr"), arrayOfVariant).variantLayoutFollowsProperty(),
        "array-of-variant is not shreddable, so its layout does not follow the property");

    // The shreddable schemas depend on the property only where this Spark version can shred (the
    // kill switch is on under test); on a version without shredding the layout follows nothing.
    assumeTrue(
        !VariantShreddingShims.getVariantInferShreddingSchemaOptions(true).isEmpty(),
        "no variant shredding support on this Spark version");
    StructType topLevelVariant =
        new StructType().add("id", DataTypes.IntegerType).add("v", DataTypes.VariantType);
    assertTrue(
        buildContext(new File(tempDir, "top"), topLevelVariant).variantLayoutFollowsProperty(),
        "top-level variant is shreddable, so its layout follows the property");
    StructType structNestedVariant =
        new StructType()
            .add("id", DataTypes.IntegerType)
            .add("s", new StructType().add("v", DataTypes.VariantType));
    assertTrue(
        buildContext(new File(tempDir, "struct"), structNestedVariant)
            .variantLayoutFollowsProperty(),
        "struct-nested variant is shreddable, so its layout follows the property");
  }

  private DeltaV2WriteContext buildContext(File dir, StructType schema) throws Exception {
    String path = dir.getAbsolutePath();
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    createKernelTable(path, schema, engine);
    Snapshot snapshot = TableManager.loadSnapshot(path).build(engine);
    return DeltaV2WriteContext.create(
        engine,
        hadoopConf,
        path,
        snapshot,
        schema,
        new StructType(),
        new TestLogicalWriteInfo(schema),
        /* variantShreddingEnabled */ false);
  }

  private static void createKernelTable(String path, StructType schema, Engine engine) {
    TableManager.buildCreateTableTransaction(
            path, SchemaUtils.convertSparkSchemaToKernelSchema(schema), "test")
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable());
  }

  private static StructType tableSchema() {
    return new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
  }

  private static class TestLogicalWriteInfo implements LogicalWriteInfo {
    private final StructType schema;

    TestLogicalWriteInfo(StructType schema) {
      this.schema = schema;
    }

    @Override
    public String queryId() {
      return "test-query-id";
    }

    @Override
    public StructType schema() {
      return schema;
    }

    @Override
    public CaseInsensitiveStringMap options() {
      return new CaseInsensitiveStringMap(Collections.emptyMap());
    }
  }
}
