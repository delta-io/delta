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

package org.apache.spark.sql.delta.managedcommit

/** Class containing usage logs emitted by Managed Commit. */
object ManagedCommitUsageLogs {

  // Common prefix for all managed-commit usage logs.
  private val PREFIX = "delta.managedCommit"

  // Usage log emitted as part of [[CommitOwnerClient.getCommits]] call.
  val COMMIT_OWNER_CLIENT_GET_COMMITS = s"$PREFIX.commitOwnerClient.getCommits"

  // Usage log emitted when listing files in CommitOwnerClient (i.e. getCommits) can't be done in
  // separate thread because the thread pool is full.
  val COMMIT_OWNER_LISTING_THREADPOOL_FULL =
    s"$PREFIX.listDeltaAndCheckpointFiles.GetCommitsThreadpoolFull"

  // Usage log emitted when we need a 2nd roundtrip to list files in FileSystem.
  // This happens when:
  // 1. FileSystem returns File 1/2/3
  // 2. CommitOwnerClient returns File 5/6 -- 4 got backfilled by the time our request reached
  //    CommitOwnerClient
  // 3. We need to list again in FileSystem to get File 4.
  val COMMIT_OWNER_ADDITIONAL_LISTING_REQUIRED =
    s"$PREFIX.listDeltaAndCheckpointFiles.requiresAdditionalFsListing"

  // Usage log emitted when listing files via FileSystem and CommitOwnerClient (i.e. getCommits)
  // shows an unexpected gap.
  val FS_COMMIT_OWNER_LISTING_UNEXPECTED_GAPS =
    s"$PREFIX.listDeltaAndCheckpointFiles.unexpectedGapsInResults"
}
