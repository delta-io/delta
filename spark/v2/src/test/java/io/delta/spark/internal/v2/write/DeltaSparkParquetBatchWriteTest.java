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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.delta.kernel.Snapshot;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DeltaSparkParquetBatchWriteTest extends DeltaV2TestBase {

  @Test
  public void createBatchWriterFactoryReturnsFactoryThatCreatesDataWriter(@TempDir File tempDir)
      throws Exception {
    DeltaSparkParquetBatchWrite batchWrite = createBatchWrite(tempDir);

    DataWriterFactory factory = batchWrite.createBatchWriterFactory(/* physicalWriteInfo= */ null);
    assertNotNull(factory);
    assertNotNull(factory.createWriter(0, 0));
  }

  @Test
  public void commitWithEmptyMessagesSucceeds(@TempDir File tempDir) throws Exception {
    DeltaSparkParquetBatchWrite batchWrite = createBatchWrite(tempDir);
    WriterCommitMessage[] empty = new WriterCommitMessage[0];

    assertDoesNotThrow(() -> batchWrite.commit(empty));
  }

  @Test
  public void commitWithWrittenDataSucceeds(@TempDir File tempDir) throws Exception {
    DeltaSparkParquetBatchWrite batchWrite = createBatchWrite(tempDir);

    DataWriterFactory factory = batchWrite.createBatchWriterFactory(null);
    DataWriter<InternalRow> writer = factory.createWriter(0, 0);

    writer.write(new GenericInternalRow(new Object[] {1, UTF8String.fromString("Alice")}));
    writer.write(new GenericInternalRow(new Object[] {2, UTF8String.fromString("Bob")}));
    WriterCommitMessage msg = writer.commit();

    assertDoesNotThrow(() -> batchWrite.commit(new WriterCommitMessage[] {msg}));
  }

  @Test
  public void abortDoesNotThrow(@TempDir File tempDir) throws Exception {
    DeltaSparkParquetBatchWrite batchWrite = createBatchWrite(tempDir);
    WriterCommitMessage[] empty = new WriterCommitMessage[0];

    assertDoesNotThrow(() -> batchWrite.abort(empty));
  }

  private DeltaSparkParquetBatchWrite createBatchWrite(File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    createEmptyTestTable(path, "t_" + System.currentTimeMillis());

    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Snapshot snapshot = new PathBasedSnapshotManager(path, hadoopConf).loadLatestSnapshot();

    return new DeltaSparkParquetBatchWrite(hadoopConf, snapshot, Collections.emptyMap());
  }
}
