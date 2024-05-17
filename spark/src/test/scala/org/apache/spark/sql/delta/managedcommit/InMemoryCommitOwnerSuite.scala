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

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.hadoop.fs.Path

abstract class InMemoryCommitOwnerSuite(batchSize: Int) extends CommitOwnerClientImplSuiteBase {

  override protected def createTableCommitOwnerClient(
      deltaLog: DeltaLog): TableCommitOwnerClient = {
    val cs = InMemoryCommitOwnerBuilder(batchSize).build(spark, Map.empty)
    TableCommitOwnerClient(cs, deltaLog, Map.empty[String, String])
  }

  override protected def registerBackfillOp(
      commitOwnerClient: CommitOwnerClient,
      deltaLog: DeltaLog,
      version: Long): Unit = {
    val inMemoryCS = commitOwnerClient.asInstanceOf[InMemoryCommitOwner]
    inMemoryCS.registerBackfill(deltaLog.logPath, version)
  }

  override protected def validateBackfillStrategy(
      tableCommitOwnerClient: TableCommitOwnerClient,
      logPath: Path,
      version: Long): Unit = {
    val lastExpectedBackfilledVersion = (version - (version % batchSize)).toInt
    val unbackfilledCommitVersionsAll = tableCommitOwnerClient
      .getCommits().getCommits.map(_.getVersion)
    val expectedVersions = lastExpectedBackfilledVersion + 1 to version.toInt

    assert(unbackfilledCommitVersionsAll == expectedVersions)
    (0 to lastExpectedBackfilledVersion).foreach { v =>
      assertBackfilled(v, logPath, Some(v))
    }
  }

  protected def validateGetCommitsResult(
      result: GetCommitsResponse,
      startVersion: Option[Long],
      endVersion: Option[Long],
      maxVersion: Long): Unit = {
    val commitVersions = result.getCommits.map(_.getVersion)
    val lastExpectedBackfilledVersion = (maxVersion - (maxVersion % batchSize)).toInt
    val expectedVersions = lastExpectedBackfilledVersion + 1 to maxVersion.toInt
    assert(commitVersions == expectedVersions)
    assert(result.getLatestTableVersion == maxVersion)
  }

  test("InMemoryCommitOwnerBuilder works as expected") {
    val builder1 = InMemoryCommitOwnerBuilder(5)
    val cs1 = builder1.build(spark, Map.empty)
    assert(cs1.isInstanceOf[InMemoryCommitOwner])
    assert(cs1.asInstanceOf[InMemoryCommitOwner].batchSize == 5)

    val cs1_again = builder1.build(spark, Map.empty)
    assert(cs1_again.isInstanceOf[InMemoryCommitOwner])
    assert(cs1 == cs1_again)

    val builder2 = InMemoryCommitOwnerBuilder(10)
    val cs2 = builder2.build(spark, Map.empty)
    assert(cs2.isInstanceOf[InMemoryCommitOwner])
    assert(cs2.asInstanceOf[InMemoryCommitOwner].batchSize == 10)
    assert(cs2 ne cs1)

    val builder3 = InMemoryCommitOwnerBuilder(10)
    val cs3 = builder3.build(spark, Map.empty)
    assert(cs3.isInstanceOf[InMemoryCommitOwner])
    assert(cs3.asInstanceOf[InMemoryCommitOwner].batchSize == 10)
    assert(cs3 ne cs2)
  }

  test("test commit > 1 is rejected as first commit") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val tcs = createTableCommitOwnerClient(log)
      tcs.commitOwnerClient.registerTable(
        logPath, currentVersion = -1L, initMetadata, Protocol(1, 1))

      // Anything other than version-0 or version-1 should be rejected as the first commit
      // version-0 will be directly backfilled and won't be recorded in InMemoryCommitOwner.
      // version-1 is what commit-owner is accepting.
      assertCommitFail(2, 1, retryable = false, commit(2, 0, tcs))
    }
  }
}

class InMemoryCommitOwner1Suite extends InMemoryCommitOwnerSuite(1)
class InMemoryCommitOwner5Suite extends InMemoryCommitOwnerSuite(5)
