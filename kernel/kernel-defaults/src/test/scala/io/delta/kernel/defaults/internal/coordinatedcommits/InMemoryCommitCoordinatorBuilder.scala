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

import io.delta.kernel.config.ConfigurationProvider
import io.delta.kernel.coordinatedcommits.{AbstractCommitCoordinatorBuilder, CommitCoordinatorClient, InMemoryCommitCoordinatorClient}

/**
 * The [[InMemoryCommitCoordinatorBuilder]] class is responsible for creating singleton instances of
 * [[InMemoryCommitCoordinatorClient]] with the specified batchSize.
 *
 * For testing purposes, a test can clear the instances of [[InMemoryCommitCoordinatorClient]] by
 * calling [[InMemoryCommitCoordinatorBuilder.clearInMemoryInstances]], configure the
 * [[InMemoryCommitCoordinatorBuilder]] and batchSize in hadoopConf passed to the engine. In this
 * way, the [[InMemoryCommitCoordinatorClient]] instances can be used by Kernel read and write
 * across the test.
 */
class InMemoryCommitCoordinatorBuilder extends AbstractCommitCoordinatorBuilder() {

  /** Name of the commit-coordinator */
  override def getName: String = "in-memory"

  /** Returns a commit-coordinator based on the given conf */
  override def build(
      sessionConfig: ConfigurationProvider,
      commitCoordinatorConf: java.util.Map[String, String]): CommitCoordinatorClient = {
    val batchSize = sessionConfig
      .getOptional(InMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY)
      .orElse("1")
      .toLong

    if (InMemoryCommitCoordinatorBuilder.batchSizeMap.containsKey(batchSize)) {
      InMemoryCommitCoordinatorBuilder.batchSizeMap.get(batchSize)
    } else {
      val coordinator = new PredictableUuidInMemoryCommitCoordinatorClient(batchSize)
      InMemoryCommitCoordinatorBuilder.batchSizeMap.put(batchSize, coordinator)
      coordinator
    }
  }
}

/**
 * The [[InMemoryCommitCoordinatorBuilder]] companion object is responsible for storing the
 * singleton instances of [[InMemoryCommitCoordinatorClient]] based on the batchSize. This is useful
 * for checking the state of the instances in UTs.
 */
object InMemoryCommitCoordinatorBuilder {
  val BATCH_SIZE_CONF_KEY = "delta.kernel.default.coordinatedCommits.inMemoryCoordinator.batchSize"
  val batchSizeMap: java.util.Map[Long, InMemoryCommitCoordinatorClient] =
    new java.util.HashMap[Long, InMemoryCommitCoordinatorClient]()

  // Visible only for UTs
  private[defaults] def clearInMemoryInstances(): Unit = {
    batchSizeMap.clear()
  }
}

class PredictableUuidInMemoryCommitCoordinatorClient(batchSize: Long)
  extends InMemoryCommitCoordinatorClient(batchSize) {

  var nextUuidSuffix = 1L

  override def generateUUID(): String = {
    nextUuidSuffix += 1
    s"uuid-${nextUuidSuffix - 1}"
  }
}
