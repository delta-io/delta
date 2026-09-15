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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
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

public class DeltaV2BatchWriteContextTest extends DeltaV2TestBase {

  @Test
  public void createInitializesDriverWriteState(@TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    StructType tableSchema = tableSchema();
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    createKernelTable(path, tableSchema, engine);
    Snapshot snapshot = TableManager.loadSnapshot(path).build(engine);

    DeltaV2BatchWriteContext context =
        DeltaV2BatchWriteContext.create(
            engine,
            hadoopConf,
            path,
            snapshot,
            tableSchema,
            new StructType(),
            new TestLogicalWriteInfo(tableSchema));

    assertSame(engine, context.getEngine());
    assertNotNull(context.getTransaction());
    assertNotNull(context.getSerializedTxnState());
    assertNotNull(context.getSerializedTxnState().getRow());
    assertNotNull(context.getOutputWriterFactory());
    assertNotNull(context.getSerializableHadoopConf());
    assertNotNull(context.getSerializableHadoopConf().value());

    assertArrayEquals(tableSchema.fieldNames(), context.getDataSchema().fieldNames());
    assertEquals(0, context.getPartitionSchema().fields().length);
    assertEquals(snapshot.getSchema().length(), context.getKernelTableSchema().length());

    String sessionTimeZone = spark.sessionState().conf().sessionLocalTimeZone();
    assertEquals(sessionTimeZone, context.getSessionTimeZoneId());
    assertEquals(ZoneId.of(sessionTimeZone), context.getSessionTimeZone());

    String targetDirectory = context.getTargetDirectory(Collections.emptyMap());
    assertNotNull(targetDirectory);
    assertFalse(targetDirectory.isEmpty());
    assertTrue(targetDirectory.contains(path));
  }

  @Test
  public void engineInfoUsesExpectedPrefix() {
    assertTrue(DeltaV2BatchWriteContext.getEngineInfo().startsWith("Apache-Spark/"));
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
