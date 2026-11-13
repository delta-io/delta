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

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
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

  List<SerializableKernelRowWrapper> getActionRows() {
    return Collections.unmodifiableList(actionRows);
  }

  /**
   * Flattens the Delta log action rows from every task's commit message into the {@link
   * CloseableIterable} that {@link io.delta.kernel.Transaction#commit} expects.
   *
   * <p>The rows are validated as described in {@link #unpackAddFiles}.
   */
  static CloseableIterable<Row> toDataActions(WriterCommitMessage[] messages) {
    List<Row> allActionRows = new ArrayList<>();
    unpackAddFiles(messages, (kernelRow, addFile) -> allActionRows.add(kernelRow));
    return CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(allActionRows.iterator()));
  }

  /**
   * Converts the Delta log action rows from every task's commit message into V1 {@link AddFile}s.
   * The rows are validated as described in {@link #unpackAddFiles}.
   */
  static List<AddFile> toAddFiles(WriterCommitMessage[] messages) {
    List<AddFile> addFiles = new ArrayList<>();
    unpackAddFiles(messages, (kernelRow, addFile) -> addFiles.add(addFile));
    return addFiles;
  }

  /**
   * Unpacks every task's commit message, shared by the batch and streaming commit paths, and passes
   * each action row with its V1 {@link AddFile} form to {@code sink}.
   *
   * <p>{@link DeltaV2DataWriter} emits one Kernel append action row per written file, holding only
   * an {@code add} action. Any other row means a writer bug and must not reach the commit, so this
   * fails the whole commit before anything is committed: a row with other than exactly one action
   * fails with {@link IllegalArgumentException}, a single non-AddFile action with {@link
   * UnsupportedOperationException}.
   *
   * <p>Messages of another type (or null) are skipped: Spark only hands the commit messages
   * returned by this connector's own data writers, so there is nothing to commit for them.
   */
  private static void unpackAddFiles(
      WriterCommitMessage[] messages, BiConsumer<Row, AddFile> sink) {
    for (WriterCommitMessage msg : messages) {
      if (!(msg instanceof DeltaV2WriterCommitMessage)) {
        continue;
      }
      for (SerializableKernelRowWrapper wrapper : ((DeltaV2WriterCommitMessage) msg).actionRows) {
        Row kernelRow = wrapper.getRow();
        List<Action> rowActions =
            CollectionConverters.asJava(KernelActionUtils$.MODULE$.actionsFromKernelRow(kernelRow));
        if (rowActions.size() != 1) {
          throw new IllegalArgumentException(
              "Expected exactly one action from writer commit message, but found "
                  + rowActions.size()
                  + " actions");
        }
        Action action = rowActions.get(0);
        if (!(action instanceof AddFile)) {
          throw new UnsupportedOperationException(
              "DSv2 Delta writes do not support '"
                  + action.getClass().getSimpleName()
                  + "' actions");
        }
        sink.accept(kernelRow, (AddFile) action);
      }
    }
  }
}
