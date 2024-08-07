/*
 * Copyright (2021) The Delta Lake Project Authors.
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

/** Class containing usage logs emitted by Coordinated Commits. */
object CoordinatedCommitsUsageLogs {

  // Common prefix for all coordinated-commits usage logs.
  private val PREFIX = "delta.coordinatedCommits"

  // Usage log emitted as part of [[CommitCoordinatorClient.getCommits]] call.
  val COMMIT_COORDINATOR_CLIENT_GET_COMMITS = s"$PREFIX.commitCoordinatorClient.getCommits"

  // Usage log emitted when listing files in CommitCoordinatorClient (i.e. getCommits) can't be done
  // in separate thread because the thread pool is full.
  val COMMIT_COORDINATOR_LISTING_THREADPOOL_FULL =
    s"$PREFIX.listDeltaAndCheckpointFiles.GetCommitsThreadpoolFull"

  // Usage log emitted when we need a 2nd roundtrip to list files in FileSystem.
  // This happens when:
  // 1. FileSystem returns File 1/2/3
  // 2. CommitCoordinatorClient returns File 5/6 -- 4 got backfilled by the time our request reached
  //    CommitCoordinatorClient
  // 3. We need to list again in FileSystem to get File 4.
  val COMMIT_COORDINATOR_ADDITIONAL_LISTING_REQUIRED =
    s"$PREFIX.listDeltaAndCheckpointFiles.requiresAdditionalFsListing"

  // Usage log emitted when listing files via FileSystem and CommitCoordinatorClient
  // (i.e. getCommits) shows an unexpected gap.
  val FS_COMMIT_COORDINATOR_LISTING_UNEXPECTED_GAPS =
    s"$PREFIX.listDeltaAndCheckpointFiles.unexpectedGapsInResults"
}
