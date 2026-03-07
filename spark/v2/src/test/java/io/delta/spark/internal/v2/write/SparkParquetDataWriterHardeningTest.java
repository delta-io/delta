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

import io.delta.spark.internal.v2.DeltaV2TestBase;
import java.io.File;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SparkParquetDataWriterHardeningTest extends DeltaV2TestBase {
  @Test
  public void testDataWriterRejectsNullRows(@TempDir File tempDir) throws Exception {
    SparkParquetBatchWrite batchWrite =
        SparkParquetWriteTestUtils.createBatchWrite(
            spark, tempDir, "test_writer_rejects_null_rows", "query-hardening");
    DataWriterFactory factory = batchWrite.createBatchWriterFactory(null);
    @SuppressWarnings("unchecked")
    DataWriter<org.apache.spark.sql.catalyst.InternalRow> writer =
        (DataWriter<org.apache.spark.sql.catalyst.InternalRow>) factory.createWriter(0, 1L);

    NullPointerException ex = assertThrows(NullPointerException.class, () -> writer.write(null));
    assertEquals("record is null", ex.getMessage());
  }

  @Test
  public void testDataWriterAbortAndCloseAreNoOps(@TempDir File tempDir) throws Exception {
    SparkParquetBatchWrite batchWrite =
        SparkParquetWriteTestUtils.createBatchWrite(
            spark, tempDir, "test_writer_abort_close_idempotent", "query-hardening");
    DataWriterFactory factory = batchWrite.createBatchWriterFactory(null);
    @SuppressWarnings("unchecked")
    DataWriter<org.apache.spark.sql.catalyst.InternalRow> writer =
        (DataWriter<org.apache.spark.sql.catalyst.InternalRow>) factory.createWriter(0, 1L);

    writer.abort();
    writer.close();
    writer.abort();
    writer.close();
  }

  @Test
  public void testDataWriterCommitMessageIncludesTargetDirectory(@TempDir File tempDir)
      throws Exception {
    SparkParquetBatchWrite batchWrite =
        SparkParquetWriteTestUtils.createBatchWrite(
            spark, tempDir, "test_writer_commit_message_directory", "query-hardening");
    DataWriterFactory factory = batchWrite.createBatchWriterFactory(null);
    @SuppressWarnings("unchecked")
    DataWriter<org.apache.spark.sql.catalyst.InternalRow> writer =
        (DataWriter<org.apache.spark.sql.catalyst.InternalRow>) factory.createWriter(5, 22L);

    writer.write(new GenericInternalRow(new Object[] {1}));
    WriterCommitMessage commitMessage = writer.commit();
    assertTrue(commitMessage instanceof SparkParquetWriterCommitMessage);
    SparkParquetWriterCommitMessage message = (SparkParquetWriterCommitMessage) commitMessage;
    assertNotNull(message.getTargetDirectory());
    assertTrue(
        message.getTargetDirectory().contains(batchWrite.getTablePath()),
        "Commit message target directory should remain under table path");
  }

  @Test
  public void testDecodeMessagesRejectsNullArray() {
    NullPointerException ex =
        assertThrows(
            NullPointerException.class, () -> SparkParquetCommitMessageUtils.decodeMessages(null));
    assertEquals("commit messages are null", ex.getMessage());
  }
}
