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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
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

public class SparkParquetWriteBuilderTest extends DeltaV2TestBase {

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

    SparkParquetWriteBuilder builder =
        new SparkParquetWriteBuilder(
            tablePath,
            hadoopConf,
            initialSnapshot,
            writeSchema,
            queryId,
            options,
            partitionColumnNames);

    SparkParquetBatchWrite batchWrite = builder.buildForBatch();
    assertEquals(tablePath, batchWrite.getTablePath());
    assertEquals(hadoopConf, batchWrite.getHadoopConf());
    assertEquals(initialSnapshot, batchWrite.getInitialSnapshot());
    assertEquals(writeSchema, batchWrite.getWriteSchema());
    assertEquals(queryId, batchWrite.getQueryId());
    assertEquals(options, batchWrite.getOptions());
    assertEquals(partitionColumnNames, batchWrite.getPartitionColumnNames());
    assertNotNull(batchWrite.getEngine());
    assertNotNull(batchWrite.getTransaction());
    assertNotNull(batchWrite.getTxnState());
    assertNotNull(batchWrite.getWriteContext());
    assertNotNull(batchWrite.getSerializableTxnState());
    assertNotNull(batchWrite.getSerializableHadoopConf());
    assertTrue(
        batchWrite.getTargetDirectory().contains(tablePath),
        "Target directory should be under the table path");
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

    SparkParquetBatchWrite batchWrite =
        new SparkParquetBatchWrite(
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

  @Test
  public void testTxnStateWrapperRoundTripsViaSerialization(@TempDir File tempDir)
      throws Exception {
    SparkParquetBatchWrite batchWrite = createBatchWrite(tempDir, "test_txn_state_roundtrip");
    SerializableKernelRowWrapper roundTripped =
        roundTrip(batchWrite.getSerializableTxnState(), SerializableKernelRowWrapper.class);

    assertEquals(batchWrite.getSerializableTxnState(), roundTripped);
    assertNotNull(roundTripped.getRow());
  }

  @Test
  public void testHadoopConfSnapshotRoundTripsAndReconstructs(@TempDir File tempDir)
      throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE test_hadoop_conf_roundtrip (id INT) USING delta LOCATION '%s'",
            tablePath));

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    hadoopConf.set("spark.delta.test.key", "spark-delta-value");
    hadoopConf.set("delta.test.key", "delta-value");

    Engine engine = DefaultEngine.create(hadoopConf);
    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.create(tablePath, engine, Optional.empty());
    Snapshot initialSnapshot = snapshotManager.loadLatestSnapshot();
    StructType writeSchema = new StructType().add("id", DataTypes.IntegerType);

    SparkParquetBatchWrite batchWrite =
        new SparkParquetBatchWrite(
            tablePath,
            hadoopConf,
            initialSnapshot,
            writeSchema,
            "query-hadoop-conf-roundtrip",
            new HashMap<>(),
            Arrays.asList("id"));

    SerializableHadoopConf roundTripped =
        roundTrip(batchWrite.getSerializableHadoopConf(), SerializableHadoopConf.class);
    Configuration reconstructed = roundTripped.toConfiguration();

    assertEquals("spark-delta-value", reconstructed.get("spark.delta.test.key"));
    assertEquals("delta-value", reconstructed.get("delta.test.key"));
    assertEquals(
        countEntries(batchWrite.getHadoopConf()),
        countEntries(reconstructed),
        "Reconstructed configuration should preserve all captured entries");
  }

  @Test
  public void testSerializableHadoopConfValidatesRequiredInput() {
    NullPointerException ex =
        assertThrows(NullPointerException.class, () -> new SerializableHadoopConf(null));
    assertEquals("configuration is null", ex.getMessage());
  }

  private SparkParquetBatchWrite createBatchWrite(File tempDir, String tableName) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        String.format("CREATE TABLE %s (id INT) USING delta LOCATION '%s'", tableName, tablePath));

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.create(tablePath, engine, Optional.empty());
    Snapshot initialSnapshot = snapshotManager.loadLatestSnapshot();
    StructType writeSchema = new StructType().add("id", DataTypes.IntegerType);

    return new SparkParquetBatchWrite(
        tablePath,
        hadoopConf,
        initialSnapshot,
        writeSchema,
        "query-roundtrip",
        new HashMap<>(),
        Arrays.asList("id"));
  }

  private static <T extends Serializable> T roundTrip(T value, Class<T> expectedType)
      throws Exception {
    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    try (ObjectOutputStream outputStream = new ObjectOutputStream(byteOutputStream)) {
      outputStream.writeObject(value);
    }

    Object deserialized;
    try (ObjectInputStream inputStream =
        new ObjectInputStream(new ByteArrayInputStream(byteOutputStream.toByteArray()))) {
      deserialized = inputStream.readObject();
    }
    return expectedType.cast(deserialized);
  }

  private static int countEntries(Configuration configuration) {
    int count = 0;
    for (Map.Entry<String, String> ignored : configuration) {
      count++;
    }
    return count;
  }
}
