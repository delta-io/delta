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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.Transaction;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.io.File;
import java.time.ZoneId;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
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
            new TestLogicalWriteInfo(tableSchema));

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
            new TestLogicalWriteInfo(tableSchema));

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
            new TestLogicalWriteInfo(fullSchema));

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
            new TestLogicalWriteInfo(mixedCaseWriteSchema));

    Transaction txn =
        snapshot
            .buildUpdateTableTransaction(DeltaV2WriteContext.getEngineInfo(), Operation.WRITE)
            .build(engine);
    assertNotNull(context.buildDataWriterFactory(txn));
  }

  @Test
  public void engineInfoUsesExpectedPrefix() {
    assertTrue(DeltaV2WriteContext.getEngineInfo().startsWith("Apache-Spark/"));
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
