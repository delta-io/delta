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
package org.apache.spark.sql.delta.v2.write;

import com.google.common.annotations.VisibleForTesting;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.v2.kernel.KernelActionUtils$;
import org.apache.spark.sql.delta.v2.utils.SerializableKernelRowWrapper;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Commit message for a single write task. Holds the serialized Delta log action rows (e.g. AddFile)
 * produced by that task. The driver deserializes these and passes them to {@link
 * io.delta.kernel.Transaction#commit}.
 */
class DeltaV2WriterCommitMessage implements WriterCommitMessage {

  private final List<SerializableKernelRowWrapper> actionRows;

  DeltaV2WriterCommitMessage(List<SerializableKernelRowWrapper> actionRows) {
    this.actionRows = actionRows != null ? actionRows : Collections.emptyList();
  }

  @VisibleForTesting
  List<SerializableKernelRowWrapper> getActionRows() {
    return Collections.unmodifiableList(actionRows);
  }

  /**
   * Flattens the Delta log action rows from every task's commit message into the {@link
   * CloseableIterable} that {@link io.delta.kernel.Transaction#commit} expects.
   */
  static CloseableIterable<Row> toDataActions(WriterCommitMessage[] messages) {
    List<Row> allActionRows = new ArrayList<>();
    forEachActionRow(messages, allActionRows::add);
    return CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(allActionRows.iterator()));
  }

  /**
   * Converts the Delta log action rows from every task's commit message into V1 {@link AddFile}s
   * for {@link DeltaV2BatchWrite#commit}. Fails with {@link IllegalArgumentException} for a row
   * with other than exactly one action, and with {@link UnsupportedOperationException} for a single
   * action that is not an {@link AddFile}.
   */
  static List<AddFile> toAddFiles(WriterCommitMessage[] messages) {
    List<AddFile> addFiles = new ArrayList<>();
    forEachActionRow(
        messages,
        kernelRow -> {
          List<Action> rowActions =
              CollectionConverters.asJava(
                  KernelActionUtils$.MODULE$.actionsFromKernelRow(kernelRow));
          if (rowActions.size() != 1) {
            throw new IllegalArgumentException(
                "Expected exactly one action from writer commit message, but found "
                    + rowActions.size()
                    + " actions");
          }
          Action action = rowActions.get(0);
          if (!(action instanceof AddFile)) {
            throw new UnsupportedOperationException(
                "DeltaV2BatchWrite does not support '"
                    + action.getClass().getSimpleName()
                    + "' actions");
          }
          addFiles.add((AddFile) action);
        });
    return addFiles;
  }

  /**
   * Passes the Delta log action rows of every {@link DeltaV2WriterCommitMessage} in {@code
   * messages} to {@code sink}, in order. Messages of any other type are skipped.
   */
  private static void forEachActionRow(WriterCommitMessage[] messages, Consumer<Row> sink) {
    for (WriterCommitMessage msg : messages) {
      if (msg instanceof DeltaV2WriterCommitMessage) {
        DeltaV2WriterCommitMessage deltaMsg = (DeltaV2WriterCommitMessage) msg;
        for (SerializableKernelRowWrapper wrapper : deltaMsg.actionRows) {
          sink.accept(wrapper.getRow());
        }
      }
    }
  }
}
