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

import java.util.Collections

import io.delta.kernel.commit.{CatalogCommitter, CommitMetadata, CommitResponse, Committer, PublishMetadata}
import io.delta.kernel.data.Row
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.files.ParsedPublishedDeltaData
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
      new CommitResponse(ParsedPublishedDeltaData.forFileStatus(FileStatus.of(filePath)))
    }
  }

  val customCatalogCommitter = new CatalogCommitter {

    val REQUIRED_PROPERTY_KEY = "test.committer.required.foo"
    val REQUIRED_PROPERTY_VALUE = "bar"

    override def commit(
        engine: Engine,
        finalizedActions: CloseableIterator[Row],
        commitMetadata: CommitMetadata): CommitResponse = {
      committerUsingPutIfAbsent.commit(engine, finalizedActions, commitMetadata)
    }

    override def getRequiredTableProperties: java.util.Map[String, String] = {
      Collections.singletonMap(REQUIRED_PROPERTY_KEY, REQUIRED_PROPERTY_VALUE)
    }

    override def publish(engine: Engine, publishMetadata: PublishMetadata): Unit = {
      // No-op
    }

  }
}
