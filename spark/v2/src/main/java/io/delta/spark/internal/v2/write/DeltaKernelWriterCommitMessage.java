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

import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * Commit message for a single write task. Holds the serialized Delta log action rows (e.g. AddFile)
 * produced by that task. The driver deserializes these and passes them to {@link
 * io.delta.kernel.Transaction#commit}.
 */
public class DeltaKernelWriterCommitMessage implements WriterCommitMessage {

  private final List<SerializableKernelRowWrapper> actionRows;

  public DeltaKernelWriterCommitMessage(List<SerializableKernelRowWrapper> actionRows) {
    this.actionRows = actionRows != null ? actionRows : Collections.emptyList();
  }

  public List<SerializableKernelRowWrapper> getActionRows() {
    return actionRows;
  }
}
