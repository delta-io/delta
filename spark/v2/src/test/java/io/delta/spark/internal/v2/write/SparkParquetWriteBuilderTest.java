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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
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

    StructType writeSchema = new StructType().add("id", DataTypes.IntegerType, false);
    String queryId = "query-123";
    Map<String, String> options = new HashMap<>();
    options.put("compression", "snappy");
    List<String> partitionColumnNames = new ArrayList<>(Arrays.asList("id"));

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
    assertEquals(new StructType().add("id", DataTypes.IntegerType), batchWrite.getWriteSchema());
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
  public void testBuilderSnapshotsMutableInputCollections(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE test_write_builder_snapshots (id INT) USING delta LOCATION '%s'",
            tablePath));

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.create(tablePath, engine, Optional.empty());
    Snapshot initialSnapshot = snapshotManager.loadLatestSnapshot();
    StructType writeSchema = new StructType().add("id", DataTypes.IntegerType);

    Map<String, String> options = new HashMap<>();
    options.put("compression", "snappy");
    List<String> partitionColumnNames = new ArrayList<>(Arrays.asList("id"));

    SparkParquetWriteBuilder builder =
        new SparkParquetWriteBuilder(
            tablePath,
            hadoopConf,
            initialSnapshot,
            writeSchema,
            "query-input-snapshot",
            options,
            partitionColumnNames);

    options.put("compression", "gzip");
    partitionColumnNames.add("late_added_partition");

    SparkParquetBatchWrite batchWrite = builder.buildForBatch();
    assertEquals("snappy", batchWrite.getOptions().get("compression"));
    assertEquals(Arrays.asList("id"), batchWrite.getPartitionColumnNames());
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
        UnsupportedOperationException.class,
        () -> batchWrite.createBatchWriterFactory(null).createWriter(0, 0L).write(null));
    assertThrows(NullPointerException.class, () -> batchWrite.commit(null));
  }

  @Test
  public void testBatchWriteCreatesSerializableDataWriterFactory(@TempDir File tempDir)
      throws Exception {
    SparkParquetBatchWrite batchWrite =
        SparkParquetWriteTestUtils.createBatchWrite(
            spark, tempDir, "test_factory_creation", "query-roundtrip");
    DataWriterFactory factory = batchWrite.createBatchWriterFactory(null);
    DataWriter<?> dataWriter = factory.createWriter(1, 101L);

    assertNotNull(factory);
    assertNotNull(dataWriter);
    assertTrue(factory instanceof SparkParquetDataWriterFactory);
    assertTrue(dataWriter instanceof SparkParquetDataWriter);
  }

  @Test
  public void testDataWriterCommitMessageContainsRowCount(@TempDir File tempDir) throws Exception {
    SparkParquetBatchWrite batchWrite =
        SparkParquetWriteTestUtils.createBatchWrite(
            spark, tempDir, "test_data_writer_commit_message", "query-roundtrip");
    DataWriterFactory factory = batchWrite.createBatchWriterFactory(null);
    @SuppressWarnings("unchecked")
    DataWriter<org.apache.spark.sql.catalyst.InternalRow> dataWriter =
        (DataWriter<org.apache.spark.sql.catalyst.InternalRow>) factory.createWriter(2, 202L);

    dataWriter.write(new GenericInternalRow(new Object[] {1}));
    dataWriter.write(new GenericInternalRow(new Object[] {2}));
    WriterCommitMessage commitMessage = dataWriter.commit();

    assertTrue(commitMessage instanceof SparkParquetWriterCommitMessage);
    SparkParquetWriterCommitMessage message = (SparkParquetWriterCommitMessage) commitMessage;
    assertEquals(2, message.getPartitionId());
    assertEquals(202L, message.getTaskId());
    assertEquals(2L, message.getNumRowsWritten());
    assertNotNull(message.getTargetDirectory());
  }

  @Test
  public void testCommitMessageRejectsNegativeRowCount() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SparkParquetWriterCommitMessage(0, 1L, -1L, "/tmp/path"));
    assertEquals("numRowsWritten must be non-negative", ex.getMessage());
  }

  @Test
  public void testCommitMessageRejectsNullOrEmptyTargetDirectory() {
    assertThrows(
        NullPointerException.class, () -> new SparkParquetWriterCommitMessage(0, 1L, 0L, null));

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> new SparkParquetWriterCommitMessage(0, 1L, 0L, ""));
    assertEquals("target directory is empty", ex.getMessage());
  }

  @Test
  public void testTxnStateWrapperRoundTripsViaSerialization(@TempDir File tempDir)
      throws Exception {
    SparkParquetBatchWrite batchWrite =
        SparkParquetWriteTestUtils.createBatchWrite(
            spark, tempDir, "test_txn_state_roundtrip", "query-roundtrip");
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

    SerializableConfiguration roundTripped =
        roundTrip(batchWrite.getSerializableHadoopConf(), SerializableConfiguration.class);
    Configuration reconstructed = roundTripped.value();

    assertEquals("spark-delta-value", reconstructed.get("spark.delta.test.key"));
    assertEquals("delta-value", reconstructed.get("delta.test.key"));
    assertEquals(
        countEntries(batchWrite.getHadoopConf()),
        countEntries(reconstructed),
        "Reconstructed configuration should preserve all captured entries");
  }

  @Test
  public void testBatchWriteRejectsIncompatibleWriteSchema(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE test_incompatible_schema (id INT) USING delta LOCATION '%s'", tablePath));

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    DeltaSnapshotManager snapshotManager =
        SnapshotManagerFactory.create(tablePath, engine, Optional.empty());
    Snapshot initialSnapshot = snapshotManager.loadLatestSnapshot();

    StructType incompatibleSchema = new StructType().add("id", DataTypes.LongType);
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new SparkParquetBatchWrite(
                    tablePath,
                    hadoopConf,
                    initialSnapshot,
                    incompatibleSchema,
                    "query-incompatible-schema",
                    new HashMap<>(),
                    Arrays.asList("id")));
    assertEquals(
        "Write schema does not match table schema after nullability normalization",
        ex.getMessage());
  }

  @Test
  public void testBatchWriteCommitRejectsUnexpectedMessageTypes(@TempDir File tempDir)
      throws Exception {
    SparkParquetBatchWrite batchWrite =
        SparkParquetWriteTestUtils.createBatchWrite(
            spark, tempDir, "test_commit_message_decode", "query-roundtrip");
    WriterCommitMessage[] invalidMessages =
        new WriterCommitMessage[] {
          new WriterCommitMessage() {
            private static final long serialVersionUID = 1L;
          }
        };

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> batchWrite.commit(invalidMessages));
    assertTrue(ex.getMessage().contains("Unexpected commit message type"));
    assertTrue(ex.getMessage().contains("index 0"));
  }

  @Test
  public void testBatchWriteCommitRejectsUnexpectedMessageTypesInMixedArray(@TempDir File tempDir)
      throws Exception {
    SparkParquetBatchWrite batchWrite =
        SparkParquetWriteTestUtils.createBatchWrite(
            spark, tempDir, "test_commit_message_decode_mixed_array", "query-roundtrip");
    WriterCommitMessage[] invalidMessages =
        new WriterCommitMessage[] {
          new SparkParquetWriterCommitMessage(0, 1L, 1L, batchWrite.getTargetDirectory()),
          new WriterCommitMessage() {
            private static final long serialVersionUID = 1L;
          }
        };

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> batchWrite.commit(invalidMessages));
    assertTrue(ex.getMessage().contains("Unexpected commit message type"));
    assertTrue(ex.getMessage().contains("index 1"));
  }

  @Test
  public void testBatchWriteCommitRejectsDuplicateWriterAttemptMessages(@TempDir File tempDir)
      throws Exception {
    SparkParquetBatchWrite batchWrite =
        SparkParquetWriteTestUtils.createBatchWrite(
            spark, tempDir, "test_commit_duplicate_writer_attempt", "query-roundtrip");
    WriterCommitMessage[] duplicateMessages =
        new WriterCommitMessage[] {
          new SparkParquetWriterCommitMessage(0, 10L, 1L, batchWrite.getTargetDirectory()),
          new SparkParquetWriterCommitMessage(0, 10L, 2L, batchWrite.getTargetDirectory())
        };

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> batchWrite.commit(duplicateMessages));
    assertTrue(ex.getMessage().contains("Duplicate commit message for writer attempt"));
    assertTrue(ex.getMessage().contains("partitionId=0"));
    assertTrue(ex.getMessage().contains("taskId=10"));
  }

  @Test
  public void testBatchWriteCommitAllowsEmptyAndZeroRowMessages(@TempDir File tempDir)
      throws Exception {
    SparkParquetBatchWrite batchWrite =
        SparkParquetWriteTestUtils.createBatchWrite(
            spark, tempDir, "test_commit_zero_rows", "query-roundtrip");

    batchWrite.commit(new WriterCommitMessage[] {});
    batchWrite.commit(
        new WriterCommitMessage[] {
          new SparkParquetWriterCommitMessage(0, 1L, 0L, batchWrite.getTargetDirectory())
        });
  }

  @Test
  public void testBatchWriteCommitSignalsFollowUpForNonEmptyMessages(@TempDir File tempDir)
      throws Exception {
    SparkParquetBatchWrite batchWrite =
        SparkParquetWriteTestUtils.createBatchWrite(
            spark, tempDir, "test_commit_non_zero_rows_followup", "query-roundtrip");
    WriterCommitMessage[] messages =
        new WriterCommitMessage[] {
          new SparkParquetWriterCommitMessage(0, 1L, 1L, batchWrite.getTargetDirectory())
        };

    UnsupportedOperationException ex =
        assertThrows(UnsupportedOperationException.class, () -> batchWrite.commit(messages));
    assertEquals(
        "Driver append-action generation and transaction commit are implemented in follow-up changes",
        ex.getMessage());
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
