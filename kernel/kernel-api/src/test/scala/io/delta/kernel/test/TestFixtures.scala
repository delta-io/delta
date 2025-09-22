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

package io.delta.kernel.test

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.commit.CommitMetadata
import io.delta.kernel.internal.actions.{CommitInfo, DomainMetadata, Metadata, Protocol}
import io.delta.kernel.internal.util.Tuple2

/**
 * Test fixtures including factory methods and constants for creating test objects with sensible
 * defaults.
 */
trait TestFixtures extends ActionUtils {
  def createCommitMetadata(
      version: Long,
      logPath: String = "/fake/_delta_log",
      commitInfo: CommitInfo = testCommitInfo(),
      commitDomainMetadatas: List[DomainMetadata] = List.empty,
      readPandMOpt: Optional[Tuple2[Protocol, Metadata]] = Optional.empty(),
      newProtocolOpt: Optional[Protocol] = Optional.empty(),
      newMetadataOpt: Optional[Metadata] = Optional.empty()): CommitMetadata = {
    new CommitMetadata(
      version,
      logPath,
      commitInfo,
      commitDomainMetadatas.asJava,
      readPandMOpt,
      newProtocolOpt,
      newMetadataOpt)
  }
}
