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

package io.delta.kernel.defaults.utils

import io.delta.kernel.commit.{CommitMetadata, CommitResponse, Committer}
import io.delta.kernel.data.Row
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.files.ParsedLogData
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

trait TestCommitterUtils {
  val committerUsingPutIfAbsent = new Committer {
    override def commit(
        engine: Engine,
        finalizedActions: CloseableIterator[Row],
        commitMetadata: CommitMetadata): CommitResponse = {
      val filePath =
        FileNames.deltaFile(commitMetadata.getDeltaLogDirPath, commitMetadata.getVersion)
      engine
        .getJsonHandler
        .writeJsonFileAtomically(filePath, finalizedActions, false)
      new CommitResponse(ParsedLogData.forFileStatus(FileStatus.of(filePath)))
    }
  }
}
