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

package org.apache.spark.sql.delta.coordinatedcommits

import io.delta.storage.commit.uccommitcoordinator.UCClient

/**
 * Shared helpers for constructing Unity Catalog [[UCClient]] instances.
 *
 * The default implementation delegates to [[UCCommitCoordinatorBuilder.ucClientFactory]] so that
 * both V1 and Kernel-backed code paths reuse the same client-construction logic.
 */
object UnityCatalogClientFactories {

  def defaultClient(uri: String, token: String): UCClient = {
    UCCommitCoordinatorBuilder.ucClientFactory.createUCClient(uri, token)
  }
}


