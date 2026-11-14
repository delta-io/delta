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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.v2.kernel.KernelActionUtils$;
import org.apache.spark.sql.delta.v2.utils.SerializableKernelRowWrapper;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Commit message for a single write task. Holds the serialized Delta log action rows (e.g. AddFile)
 * produced by that task. The driver decodes and validates them before committing through the
 * optimistic transaction.
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
   * Decodes append actions shared by batch and streaming writes. Each task row must contain exactly
   * one AddFile; driver-owned actions such as SetTransaction cannot come from a task.
   */
  static List<Action> toAddFileActions(WriterCommitMessage[] messages) {
    List<Action> actions = new ArrayList<>();
    for (WriterCommitMessage msg : messages) {
      if (msg instanceof DeltaV2WriterCommitMessage) {
        DeltaV2WriterCommitMessage deltaMsg = (DeltaV2WriterCommitMessage) msg;
        for (SerializableKernelRowWrapper wrapper : deltaMsg.actionRows) {
          List<Action> rowActions =
              CollectionConverters.asJava(
                  KernelActionUtils$.MODULE$.actionsFromKernelRow(wrapper.getRow()));
          if (rowActions.size() != 1) {
            throw new IllegalArgumentException(
                "Expected exactly one action from writer commit message, but found "
                    + rowActions.size()
                    + " actions");
          }
          Action action = rowActions.get(0);
          if (!(action instanceof AddFile)) {
            throw new UnsupportedOperationException(
                "Delta V2 writers do not support '"
                    + action.getClass().getSimpleName()
                    + "' actions");
          }
          actions.add(action);
        }
      }
    }
    return actions;
  }
}
