/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.defaults.internal.coordinatedcommits

import io.delta.kernel.internal.lang.Lazy
import io.delta.storage.commit.{CommitCoordinatorClient, InMemoryCommitCoordinator}
import org.apache.hadoop.conf.Configuration

import java.util

/**
 * The [[InMemoryCommitCoordinatorBuilder]] class is responsible for creating singleton instances of
 * [[InMemoryCommitCoordinator]] with the specified batchSize.
 */
class InMemoryCommitCoordinatorBuilder(hadoopConf: Configuration)
  extends CommitCoordinatorBuilder(hadoopConf) {
  val BATCH_SIZE_CONF_KEY = "delta.kernel.default.in-memory.batch-size"
  private val batchSize = hadoopConf.getLong(BATCH_SIZE_CONF_KEY, 1)
  private val inMemoryCoordinator: Lazy[InMemoryCommitCoordinator] =
    new Lazy[InMemoryCommitCoordinator](() => new InMemoryCommitCoordinator(batchSize))

  /** Name of the commit-coordinator */
  override def getName: String = "in-memory"

  /** Returns a commit-coordinator based on the given conf */
  override def build(
    conf: util.Map[String, String]): CommitCoordinatorClient = inMemoryCoordinator.get
}
