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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DeltaSparkParquetWriteBuilderTest extends DeltaV2TestBase {

  @Test
  public void buildReturnsWriteWithBatchWrite(@TempDir File tempDir) throws Exception {
    DeltaSparkParquetWriteBuilder builder = createBuilder(tempDir, tableSchema());

    Write write = builder.build();
    assertNotNull(write);

    BatchWrite batchWrite = write.toBatch();
    assertNotNull(batchWrite);
    assertTrue(batchWrite instanceof DeltaSparkParquetBatchWrite);
  }

  @Test
  public void buildRejectsExtraFieldInWriteSchema(@TempDir File tempDir) throws Exception {
    StructType badSchema =
        new StructType()
            .add("id", DataTypes.IntegerType)
            .add("name", DataTypes.StringType)
            .add("nonexistent_column", DataTypes.StringType);

    DeltaSparkParquetWriteBuilder builder = createBuilder(tempDir, badSchema);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  public void buildAcceptsSubsetOfTableSchema(@TempDir File tempDir) throws Exception {
    StructType subsetSchema = new StructType().add("id", DataTypes.IntegerType);

    DeltaSparkParquetWriteBuilder builder = createBuilder(tempDir, subsetSchema);

    Write write = builder.build();
    assertNotNull(write);
  }

  @Test
  public void constructorRejectsNullArguments() {
    assertThrows(
        NullPointerException.class,
        () -> new DeltaSparkParquetWriteBuilder(null, new Configuration(), null, null));
  }

  private DeltaSparkParquetWriteBuilder createBuilder(File tempDir, StructType writeSchema)
      throws Exception {
    String path = tempDir.getAbsolutePath();
    createEmptyTestTable(path, "t_builder_" + System.currentTimeMillis());

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Snapshot snapshot = new PathBasedSnapshotManager(path, hadoopConf).loadLatestSnapshot();

    LogicalWriteInfo writeInfo = new TestLogicalWriteInfo(writeSchema);
    return new DeltaSparkParquetWriteBuilder(path, hadoopConf, snapshot, writeInfo);
  }

  private static StructType tableSchema() {
    return new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
  }

  /**
   * Minimal LogicalWriteInfo for testing. Spark does not expose a public constructor for the
   * default implementation.
   */
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
