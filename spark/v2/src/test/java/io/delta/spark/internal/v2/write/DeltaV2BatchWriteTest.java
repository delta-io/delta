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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link DeltaV2BatchWrite}; the batch counterpart of DeltaV2StreamingWriteTest. */
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
  public void testAbort_doesNotCommit(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "batch_abort");
    DeltaV2BatchWrite write = newWrite(path);
    WriterCommitMessage[] messages = {writeFile(write, 1, "Alice", 2, "Bob")};

    write.abort(messages);

    assertEquals(0L, spark.read().format("delta").load(path).count(), "abort must not commit data");
  }

  /** Runs the executor-side writer and returns its commit message. */
  private WriterCommitMessage writeFile(DeltaV2BatchWrite write, Object... idNamePairs)
      throws Exception {
    DataWriter<InternalRow> writer =
        write.createBatchWriterFactory(WriteTestUtils.physicalWriteInfo(1)).createWriter(0, 0L);
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
