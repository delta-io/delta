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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/** Commit message for a ReplaceData write task. */
class DeltaReplaceDataCommitMessage implements WriterCommitMessage {

  private final List<SerializableKernelRowWrapper> addFileActionRows;
  private final Set<String> sourceFilePaths;

  DeltaReplaceDataCommitMessage(
      List<SerializableKernelRowWrapper> addFileActionRows, Set<String> sourceFilePaths) {
    this.addFileActionRows =
        addFileActionRows != null ? new ArrayList<>(addFileActionRows) : Collections.emptyList();
    this.sourceFilePaths =
        sourceFilePaths != null ? new LinkedHashSet<>(sourceFilePaths) : Collections.emptySet();
  }

  List<SerializableKernelRowWrapper> getAddFileActionRows() {
    return Collections.unmodifiableList(addFileActionRows);
  }

  Set<String> getSourceFilePaths() {
    return Collections.unmodifiableSet(sourceFilePaths);
  }
}
