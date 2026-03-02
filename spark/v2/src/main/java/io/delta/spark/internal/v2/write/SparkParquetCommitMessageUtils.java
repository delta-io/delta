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
import java.util.List;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/** Utility methods for decoding and validating DSv2 writer commit messages. */
public final class SparkParquetCommitMessageUtils {
  private SparkParquetCommitMessageUtils() {}

  static List<SparkParquetWriterCommitMessage> decodeMessages(WriterCommitMessage[] messages) {
    requireNonNull(messages, "commit messages are null");
    List<SparkParquetWriterCommitMessage> decodedMessages = new ArrayList<>(messages.length);
    for (WriterCommitMessage message : messages) {
      if (!(message instanceof SparkParquetWriterCommitMessage)) {
        throw new IllegalArgumentException(
            "Unexpected commit message type: "
                + (message == null ? "null" : message.getClass().getName()));
      }
      decodedMessages.add((SparkParquetWriterCommitMessage) message);
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
