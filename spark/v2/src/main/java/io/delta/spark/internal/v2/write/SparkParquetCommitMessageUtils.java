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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/** Utility methods for decoding and validating DSv2 writer commit messages. */
public final class SparkParquetCommitMessageUtils {
  private SparkParquetCommitMessageUtils() {}

  static List<SparkParquetWriterCommitMessage> decodeMessages(WriterCommitMessage[] messages) {
    requireNonNull(messages, "commit messages are null");
    List<SparkParquetWriterCommitMessage> decodedMessages = new ArrayList<>(messages.length);
    Set<String> seenWriterAttempts = new HashSet<>(messages.length);
    for (int index = 0; index < messages.length; index++) {
      WriterCommitMessage message = messages[index];
      if (!(message instanceof SparkParquetWriterCommitMessage)) {
        throw new IllegalArgumentException(
            "Unexpected commit message type at index "
                + index
                + ": "
                + (message == null ? "null" : message.getClass().getName()));
      }
      SparkParquetWriterCommitMessage decodedMessage = (SparkParquetWriterCommitMessage) message;
      String writerAttemptKey = decodedMessage.getPartitionId() + ":" + decodedMessage.getTaskId();
      if (!seenWriterAttempts.add(writerAttemptKey)) {
        throw new IllegalArgumentException(
            "Duplicate commit message for writer attempt (partitionId="
                + decodedMessage.getPartitionId()
                + ", taskId="
                + decodedMessage.getTaskId()
                + ")");
      }
      decodedMessages.add(decodedMessage);
    }
    return decodedMessages;
  }

  static long totalRows(List<SparkParquetWriterCommitMessage> messages) {
    long rows = 0L;
    for (SparkParquetWriterCommitMessage message : messages) {
      rows += message.getNumRowsWritten();
    }
    return rows;
  }
}
