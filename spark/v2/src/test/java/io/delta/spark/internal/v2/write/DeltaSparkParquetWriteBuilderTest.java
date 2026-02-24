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
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DeltaSparkParquetWriteBuilderTest extends DeltaV2TestBase {

  @Test
  public void testBuildForBatchCarriesAllConstructionInputs(@TempDir File tempDir)
      throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE test_write_builder (id INT) USING delta LOCATION '%s'", tablePath));

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.create(tablePath, engine, Optional.empty());
    Snapshot initialSnapshot = snapshotManager.loadLatestSnapshot();

    StructType writeSchema =
        new StructType().add("id", DataTypes.IntegerType).add("value", DataTypes.StringType);
    String queryId = "query-123";
    Map<String, String> options = new HashMap<>();
    options.put("compression", "snappy");
    List<String> partitionColumnNames = Arrays.asList("id");

    DeltaSparkParquetWriteBuilder builder =
        new DeltaSparkParquetWriteBuilder(
            tablePath,
            hadoopConf,
            initialSnapshot,
            writeSchema,
            queryId,
            options,
            partitionColumnNames);

    DeltaSparkParquetBatchWrite batchWrite = builder.buildForBatch();
    assertEquals(tablePath, batchWrite.getTablePath());
    assertEquals(hadoopConf, batchWrite.getHadoopConf());
    assertEquals(initialSnapshot, batchWrite.getInitialSnapshot());
    assertEquals(writeSchema, batchWrite.getWriteSchema());
    assertEquals(queryId, batchWrite.getQueryId());
    assertEquals(options, batchWrite.getOptions());
    assertEquals(partitionColumnNames, batchWrite.getPartitionColumnNames());
  }

  @Test
  public void testBatchWriteDefensiveBehavior(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE test_write_builder_noop (id INT) USING delta LOCATION '%s'", tablePath));

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.create(tablePath, engine, Optional.empty());
    Snapshot initialSnapshot = snapshotManager.loadLatestSnapshot();
    StructType writeSchema = new StructType().add("id", DataTypes.IntegerType);

    DeltaSparkParquetBatchWrite batchWrite =
        new DeltaSparkParquetBatchWrite(
            tablePath,
            hadoopConf,
            initialSnapshot,
            writeSchema,
            "query-xyz",
            new HashMap<>(),
            Arrays.asList("id"));

    assertThrows(
        UnsupportedOperationException.class, () -> batchWrite.createBatchWriterFactory(null));
    assertThrows(UnsupportedOperationException.class, () -> batchWrite.commit(null));
  }
}
