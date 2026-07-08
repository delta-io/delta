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

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.InternalRowTestUtils;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link DeltaV2WriterCommitMessage}, focused on {@code toDataActions}. */
public class DeltaV2WriterCommitMessageTest extends DeltaV2TestBase {

  private static final StructType TABLE_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true)
          });

  @Test
  public void testConstructor_nullActionRows_isEmpty() {
    assertEquals(0, new DeltaV2WriterCommitMessage(null).getActionRows().size());
  }

  @Test
  public void testToDataActions_noMessages_isEmpty() {
    assertEquals(
        0, countActions(DeltaV2WriterCommitMessage.toDataActions(new WriterCommitMessage[0])));
  }

  @Test
  public void testToDataActions_ignoresNonDeltaMessages() {
    WriterCommitMessage other = new WriterCommitMessage() {};
    assertEquals(
        0,
        countActions(DeltaV2WriterCommitMessage.toDataActions(new WriterCommitMessage[] {other})));
  }

  @Test
  public void testToDataActions_flattensActionsFromAllMessages(@TempDir File tempDir)
      throws Exception {
    String path = createTable(tempDir, "commit_msg_flatten");
    DeltaV2DataWriterFactory factory = dataWriterFactory(path);

    // Two tasks, one file (one AddFile action) each; toDataActions must combine both.
    WriterCommitMessage m1 = writeFile(factory, 0, 1, "Alice");
    WriterCommitMessage m2 = writeFile(factory, 1, 2, "Bob");

    assertEquals(
        2,
        countActions(DeltaV2WriterCommitMessage.toDataActions(new WriterCommitMessage[] {m1, m2})));
  }

  @Test
  public void testToDataActions_multipleRowsPerFile(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, "commit_msg_multi_row");
    DeltaV2DataWriterFactory factory = dataWriterFactory(path);

    // Multiple rows in one file still yield one AddFile action (actions are per-file, not per-row).
    WriterCommitMessage m = writeFile(factory, 0, 1, "Alice", 2, "Bob", 3, "Carol");

    assertEquals(
        1, countActions(DeltaV2WriterCommitMessage.toDataActions(new WriterCommitMessage[] {m})));
  }

  private static int countActions(CloseableIterable<Row> actions) {
    int n = 0;
    CloseableIterator<Row> it = actions.iterator();
    try {
      while (it.hasNext()) {
        it.next();
        n++;
      }
    } finally {
      try {
        it.close();
      } catch (Exception ignored) {
        // Test cleanup best-effort.
      }
    }
    return n;
  }

  private WriterCommitMessage writeFile(
      DeltaV2DataWriterFactory factory, int partitionId, Object... idNamePairs) throws Exception {
    DataWriter<InternalRow> writer = factory.createWriter(partitionId, 0L);
    for (int i = 0; i < idNamePairs.length; i += 2) {
      writer.write(InternalRowTestUtils.row(idNamePairs[i], idNamePairs[i + 1]));
    }
    return writer.commit();
  }

  private DeltaV2DataWriterFactory dataWriterFactory(String path) {
    Snapshot snapshot =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf())
            .loadLatestSnapshot();
    DeltaV2Write write =
        new DeltaV2Write(
            defaultEngine,
            spark.sessionState().newHadoopConf(),
            path,
            snapshot,
            TABLE_SCHEMA,
            WriteTestUtils.logicalWriteInfo(TABLE_SCHEMA, CaseInsensitiveStringMap.empty()));
    return (DeltaV2DataWriterFactory)
        write.toBatch().createBatchWriterFactory(WriteTestUtils.physicalWriteInfo(1));
  }

  private String createTable(File tempDir, String tableName) {
    String path = tempDir.getAbsolutePath();
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tableName, path));
    return path;
  }
}
