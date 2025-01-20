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

import java.util.Optional

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import io.delta.storage.commit.{GetCommitsResponse => JGetCommitsResponse}
import org.apache.hadoop.fs.Path

abstract class InMemoryCommitCoordinatorSuite(batchSize: Int)
  extends CommitCoordinatorClientImplSuiteBase {

  override protected def createTableCommitCoordinatorClient(
      deltaLog: DeltaLog): TableCommitCoordinatorClient = {
    val cs = InMemoryCommitCoordinatorBuilder(batchSize).build(spark, Map.empty)
    val conf = cs.registerTable(
      deltaLog.logPath, Optional.empty(), -1L, initMetadata, Protocol(1, 1))
    TableCommitCoordinatorClient(cs, deltaLog, conf.asScala.toMap)
  }

  override protected def registerBackfillOp(
      tableCommitCoordinatorClient: TableCommitCoordinatorClient,
      deltaLog: DeltaLog,
      version: Long): Unit = {
    val commitCoordinatorClient = tableCommitCoordinatorClient.commitCoordinatorClient
    val inMemoryCS = commitCoordinatorClient.asInstanceOf[InMemoryCommitCoordinator]
    inMemoryCS.registerBackfill(deltaLog.logPath, version)
  }

  override protected def validateBackfillStrategy(
      tableCommitCoordinatorClient: TableCommitCoordinatorClient,
      logPath: Path,
      version: Long): Unit = {
    val lastExpectedBackfilledVersion = (version - (version % batchSize)).toInt
    val unbackfilledCommitVersionsAll = tableCommitCoordinatorClient
      .getCommits().getCommits.asScala.map(_.getVersion)
    val expectedVersions = lastExpectedBackfilledVersion + 1 to version.toInt

    assert(unbackfilledCommitVersionsAll == expectedVersions)
    (0 to lastExpectedBackfilledVersion).foreach { v =>
      assertBackfilled(v, logPath, Some(v))
    }
  }

  protected def validateGetCommitsResult(
      result: JGetCommitsResponse,
      startVersion: Option[Long],
      endVersion: Option[Long],
      maxVersion: Long): Unit = {
    val commitVersions = result.getCommits.asScala.map(_.getVersion)
    val lastExpectedBackfilledVersion = (maxVersion - (maxVersion % batchSize)).toInt
    val expectedVersions = lastExpectedBackfilledVersion + 1 to maxVersion.toInt
    assert(commitVersions == expectedVersions)
    assert(result.getLatestTableVersion == maxVersion)
  }

  test("InMemoryCommitCoordinatorBuilder works as expected") {
    val builder1 = InMemoryCommitCoordinatorBuilder(5)
    val cs1 = builder1.build(spark, Map.empty)
    assert(cs1.isInstanceOf[InMemoryCommitCoordinator])
    assert(cs1.asInstanceOf[InMemoryCommitCoordinator].batchSize == 5)

    val cs1_again = builder1.build(spark, Map.empty)
    assert(cs1_again.isInstanceOf[InMemoryCommitCoordinator])
    assert(cs1 == cs1_again)

    val builder2 = InMemoryCommitCoordinatorBuilder(10)
    val cs2 = builder2.build(spark, Map.empty)
    assert(cs2.isInstanceOf[InMemoryCommitCoordinator])
    assert(cs2.asInstanceOf[InMemoryCommitCoordinator].batchSize == 10)
    assert(cs2 ne cs1)

    val builder3 = InMemoryCommitCoordinatorBuilder(10)
    val cs3 = builder3.build(spark, Map.empty)
    assert(cs3.isInstanceOf[InMemoryCommitCoordinator])
    assert(cs3.asInstanceOf[InMemoryCommitCoordinator].batchSize == 10)
    assert(cs3 ne cs2)
  }

  test("test commit > 1 is rejected as first commit") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      val tcs = createTableCommitCoordinatorClient(log)

      // Anything other than version-0 or version-1 should be rejected as the first commit
      // version-0 will be directly backfilled and won't be recorded in InMemoryCommitCoordinator.
      // version-1 is what commit-coordinator is accepting.
      assertCommitFail(2, 1, retryable = false, commit(2, 0, tcs))
    }
  }
}

class InMemoryCommitCoordinator1Suite extends InMemoryCommitCoordinatorSuite(1)
class InMemoryCommitCoordinator5Suite extends InMemoryCommitCoordinatorSuite(5)
