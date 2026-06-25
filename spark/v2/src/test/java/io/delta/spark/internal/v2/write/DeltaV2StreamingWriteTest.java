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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.InternalRowTestUtils;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link DeltaV2StreamingWrite}; the write-side counterpart of
 * SparkMicroBatchStreamTest.
 */
public class DeltaV2StreamingWriteTest extends DeltaV2TestBase {

  private static final StructType TABLE_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true)
          });

  @Test
  public void testUseCommitCoordinator_isFalse(@TempDir File tempDir) {
    DeltaV2StreamingWrite write = newWrite(createTable(tempDir, "streaming_no_coordinator"));
    assertFalse(write.useCommitCoordinator());
  }

  @Test
  public void testCreateStreamingWriterFactory_returnsDeltaV2DataWriterFactory(
      @TempDir File tempDir) {
    DeltaV2StreamingWrite write = newWrite(createTable(tempDir, "streaming_factory_type"));
    StreamingDataWriterFactory factory =
        write.createStreamingWriterFactory(WriteTestUtils.physicalWriteInfo(1));
    assertTrue(factory instanceof DeltaV2DataWriterFactory);
  }

  /**
   * Distinct epochs each append: epoch 0 and epoch 1 accumulate, and the rows are the ones written.
   */
  @Test
  public void testCommit_distinctEpochsAppend(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "streaming_distinct_epochs");
    DeltaV2StreamingWrite write = newWrite(path);

    write.commit(0L, new WriterCommitMessage[] {writeEpoch(write, 0L, 1, "Alice", 2, "Bob")});
    write.commit(1L, new WriterCommitMessage[] {writeEpoch(write, 1L, 3, "Carol", 4, "Dave")});

    List<Row> rows = spark.read().format("delta").load(path).orderBy("id").collectAsList();
    assertEquals(4, rows.size(), "distinct epochs must accumulate");
    assertEquals("Alice", rows.get(0).getString(1));
    assertEquals("Bob", rows.get(1).getString(1));
    assertEquals("Carol", rows.get(2).getString(1));
    assertEquals("Dave", rows.get(3).getString(1));
  }

  /**
   * Replaying an already-committed epoch must be an idempotent no-op: the data is committed once,
   * and re-committing the same epoch does not duplicate it (the {@code withTransactionId} guard).
   */
  @Test
  public void testCommit_isIdempotentOnEpochReplay(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "streaming_idempotent_replay");
    DeltaV2StreamingWrite write = newWrite(path);
    WriterCommitMessage[] messages = {writeEpoch(write, 0L, 1, "Alice", 2, "Bob")};

    write.commit(0L, messages);
    long rowsAfterFirstCommit = spark.read().format("delta").load(path).count();

    write.commit(0L, messages); // replay the same epoch
    long rowsAfterReplay = spark.read().format("delta").load(path).count();

    assertEquals(2L, rowsAfterFirstCommit);
    assertEquals(
        2L, rowsAfterReplay, "replaying an already-committed epoch must not duplicate data");
  }

  /**
   * Aborting an epoch commits nothing: the written files are left orphaned, not added to the table.
   */
  @Test
  public void testAbort_doesNotCommit(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "streaming_abort");
    DeltaV2StreamingWrite write = newWrite(path);
    WriterCommitMessage[] messages = {writeEpoch(write, 0L, 1, "Alice", 2, "Bob")};

    write.abort(0L, messages);

    assertEquals(0L, spark.read().format("delta").load(path).count(), "abort must not commit data");
  }

  /**
   * Runs the executor-side writer for one epoch and returns its commit message. {@code idNamePairs}
   * is a flat list of (int id, String name) values.
   */
  private WriterCommitMessage writeEpoch(
      DeltaV2StreamingWrite write, long epochId, Object... idNamePairs) throws Exception {
    DataWriter<InternalRow> writer =
        write
            .createStreamingWriterFactory(WriteTestUtils.physicalWriteInfo(1))
            .createWriter(0, 0L, epochId);
    for (int i = 0; i < idNamePairs.length; i += 2) {
      writer.write(InternalRowTestUtils.row(idNamePairs[i], idNamePairs[i + 1]));
    }
    return writer.commit();
  }

  private String createTable(File tempDir, String tableName) {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tableName, path));
    return path;
  }

  private DeltaV2StreamingWrite newWrite(String path) {
    Snapshot snapshot =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf())
            .loadLatestSnapshot();
    LogicalWriteInfo info =
        WriteTestUtils.logicalWriteInfo(TABLE_SCHEMA, CaseInsensitiveStringMap.empty());
    DeltaV2Write write =
        new DeltaV2Write(
            defaultEngine,
            spark.sessionState().newHadoopConf(),
            path,
            snapshot,
            TABLE_SCHEMA,
            info);
    return (DeltaV2StreamingWrite) write.toStreaming();
  }
}
