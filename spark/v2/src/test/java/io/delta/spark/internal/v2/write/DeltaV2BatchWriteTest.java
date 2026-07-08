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
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link DeltaV2BatchWrite}. */
public class DeltaV2BatchWriteTest extends DeltaV2TestBase {

  private static final StructType TABLE_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true)
          });

  @Test
  public void testCreateBatchWriterFactory_returnsDeltaV2DataWriterFactory(@TempDir File tempDir) {
    DeltaV2BatchWrite write = newWrite(createTable(tempDir, "batch_factory_type"));
    assertTrue(
        write.createBatchWriterFactory(WriteTestUtils.physicalWriteInfo(1))
            instanceof DeltaV2DataWriterFactory);
  }

  @Test
  public void testCommit_appendsData(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "batch_commit");
    DeltaV2BatchWrite write = newWrite(path);
    WriterCommitMessage[] messages = {writeFile(write, 1, "Alice", 2, "Bob")};

    write.commit(messages);

    List<Row> rows = spark.read().format("delta").load(path).orderBy("id").collectAsList();
    assertEquals(2, rows.size());
    assertEquals("Alice", rows.get(0).getString(1));
    assertEquals("Bob", rows.get(1).getString(1));
  }

  @Test
  public void testCommit_multipleTasks_appendsAllData(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "batch_commit_multi");
    DeltaV2BatchWrite write = newWrite(path);
    // Two tasks each write their own file; commit must flatten both tasks' AddFile actions into a
    // single Delta commit rather than dropping all but one.
    DataWriterFactory factory = write.createBatchWriterFactory(WriteTestUtils.physicalWriteInfo(2));
    WriterCommitMessage[] messages = {
      writeFile(factory, 0, 1, "Alice", 2, "Bob"), writeFile(factory, 1, 3, "Carol")
    };

    write.commit(messages);

    List<Row> rows = spark.read().format("delta").load(path).orderBy("id").collectAsList();
    assertEquals(3, rows.size());
    assertEquals("Alice", rows.get(0).getString(1));
    assertEquals("Bob", rows.get(1).getString(1));
    assertEquals("Carol", rows.get(2).getString(1));
  }

  @Test
  public void testAbort_doesNotCommit(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "batch_abort");
    DeltaV2BatchWrite write = newWrite(path);
    // Stage a real file (writeFile runs the executor writer and produces a non-empty commit
    // message) so the test exercises an abort that has data to discard, not a trivial no-op.
    WriterCommitMessage[] messages = {writeFile(write, 1, "Alice", 2, "Bob")};
    long versionsBefore = spark.sql("DESCRIBE HISTORY delta.`" + path + "`").count();

    write.abort(messages);

    assertEquals(0L, spark.read().format("delta").load(path).count(), "abort must not commit data");
    // Distinguishes a correct abort from an erroneous (even empty) commit: a commit would advance
    // the table version, so the history row count must be unchanged.
    assertEquals(
        versionsBefore,
        spark.sql("DESCRIBE HISTORY delta.`" + path + "`").count(),
        "abort must not create a new table version");
  }

  @Test
  public void testAbort_hasNoSideEffectsOnSurroundingWrites(@TempDir File tempDir)
      throws Exception {
    String path = createTable(tempDir, "batch_abort_no_side_effects");

    // Commit before the abort.
    DeltaV2BatchWrite before = newWrite(path);
    before.commit(new WriterCommitMessage[] {writeFile(before, 1, "Alice")});
    // Abort a write that staged real data.
    DeltaV2BatchWrite aborted = newWrite(path);
    aborted.abort(new WriterCommitMessage[] {writeFile(aborted, 2, "Bob")});
    // Commit after the abort.
    DeltaV2BatchWrite after = newWrite(path);
    after.commit(new WriterCommitMessage[] {writeFile(after, 3, "Carol")});

    // Only the surrounding commits survive; the aborted row must not leak.
    List<Row> rows = spark.read().format("delta").load(path).orderBy("id").collectAsList();
    assertEquals(2, rows.size());
    assertEquals("Alice", rows.get(0).getString(1));
    assertEquals("Carol", rows.get(1).getString(1));
  }

  /** Runs the executor-side writer at partition 0 / task 0 and returns its commit message. */
  private WriterCommitMessage writeFile(DeltaV2BatchWrite write, Object... idNamePairs)
      throws Exception {
    return writeFile(
        write.createBatchWriterFactory(WriteTestUtils.physicalWriteInfo(1)), 0, idNamePairs);
  }

  /** Runs the executor-side writer for the given partition and returns its commit message. */
  private WriterCommitMessage writeFile(
      DataWriterFactory factory, int partitionId, Object... idNamePairs) throws Exception {
    DataWriter<InternalRow> writer = factory.createWriter(partitionId, 0L);
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

  private DeltaV2BatchWrite newWrite(String path) {
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
    return (DeltaV2BatchWrite) write.toBatch();
  }
}
