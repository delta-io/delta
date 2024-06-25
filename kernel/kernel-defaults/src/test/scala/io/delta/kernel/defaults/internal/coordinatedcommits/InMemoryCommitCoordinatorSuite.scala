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
package io.delta.kernel.defaults.internal.coordinatedcommits

import io.delta.kernel.Table
import io.delta.kernel.defaults.engine.DefaultJsonHandler
import io.delta.kernel.defaults.internal.logstore.LogStoreProvider
import io.delta.kernel.defaults.utils.DefaultVectorTestUtils
import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.actions.{CommitInfo, Metadata, Protocol}
import io.delta.kernel.types.{StringType, StructType}
import io.delta.storage.commit.{Commit, CommitCoordinatorClient, CommitFailedException, GetCommitsResponse}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.util
import java.util.{Collections, Optional}
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.{`iterator asScala`, `list asScalaBuffer`}

abstract class InMemoryCommitCoordinatorSuite(batchSize: Int) extends DeltaTableWriteSuiteBase
  with CoordinatedCommitsTestUtils
  with DefaultVectorTestUtils {

  val jsonHandler = new DefaultJsonHandler(hadoopConf)

  private def assertGetCommitResponseEqual(x: GetCommitsResponse, y: GetCommitsResponse): Unit = {
    assert(x.getLatestTableVersion == y.getLatestTableVersion)
    assert(x.getCommits.size() == y.getCommits.size())
    for (i <- 0 until x.getCommits.size()) {
      assert(x.getCommits.get(i).getVersion == y.getCommits.get(i).getVersion)
      assert(x.getCommits.get(i).getFileStatus.getPath == y.getCommits.get(i).getFileStatus.getPath)
      assert(x.getCommits.get(i).getFileStatus.getLen == y.getCommits.get(i).getFileStatus.getLen)
      assert(x
        .getCommits
        .get(i)
        .getFileStatus
        .getModificationTime == y.getCommits.get(i).getFileStatus.getModificationTime)
      assert(x.getCommits.get(i).getCommitTimestamp == y.getCommits.get(i).getCommitTimestamp)
    }
  }

  protected def assertBackfilled(
    version: Long,
    logPath: Path,
    timestampOpt: Option[Long] = None): Unit = {
    val logStore = LogStoreProvider.getLogStore(hadoopConf, logPath.toUri.getScheme)
    val delta = CoordinatedCommitsUtils.getHadoopDeltaFile(logPath, version)
    if (timestampOpt.isDefined) {
      assert(logStore.read(delta, hadoopConf).toSeq == Seq(s"$version", s"${timestampOpt.get}"))
    } else {
      assert(logStore.read(delta, hadoopConf).take(1).toSeq == Seq(s"$version"))
    }
  }

  protected def registerBackfillOp(
    commitCoordinatorClient: CommitCoordinatorClient,
    logPath: Path,
    version: Long): Unit = {
    val inMemoryCS = commitCoordinatorClient.asInstanceOf[InMemoryCommitCoordinator]
    inMemoryCS.registerBackfill(logPath, version)
  }

  protected def validateBackfillStrategy(
    engine: Engine,
    commitCoordinatorClient: CommitCoordinatorClient,
    logPath: Path,
    tableConf: util.Map[String, String],
    version: Long): Unit = {
    val lastExpectedBackfilledVersion = (version - (version % batchSize)).toInt
    val unbackfilledCommitVersionsAll = commitCoordinatorClient
      .getCommits(logPath, tableConf, null, null)
      .getCommits.map(_.getVersion)
    val expectedVersions = lastExpectedBackfilledVersion + 1 to version.toInt

    assert(unbackfilledCommitVersionsAll == expectedVersions)
    (0 to lastExpectedBackfilledVersion).foreach { v =>
      assertBackfilled(v, logPath, Some(v))
    }
  }

  /**
   * Checks that the commit coordinator state is correct in terms of
   *  - The latest table version in the commit coordinator is correct
   *  - All supposedly backfilled commits are indeed backfilled
   *  - The contents of the backfilled commits are correct (verified
   *     if commitTimestampOpt is provided)
   *
   * This can be overridden by implementing classes to implement
   * more specific invariants.
   */
  protected def assertInvariants(
    logPath: Path,
    tableConf: util.Map[String, String],
    commitCoordinatorClient: CommitCoordinatorClient,
    commitTimestampsOpt: Option[Array[Long]] = None): Unit = {
    val maxUntrackedVersion: Int = {
      val commitResponse = commitCoordinatorClient.getCommits(logPath, tableConf, null, null)
      if (commitResponse.getCommits.isEmpty) {
        commitResponse.getLatestTableVersion.toInt
      } else {
        assert(
          commitResponse.getCommits.last.getVersion == commitResponse.getLatestTableVersion,
          s"Max commit tracked by the commit coordinator ${commitResponse.getCommits.last} must " +
            s"match latestTableVersion tracked by the commit coordinator " +
            s"${commitResponse.getLatestTableVersion}."
        )
        val minVersion = commitResponse.getCommits.head.getVersion
        assert(
          commitResponse.getLatestTableVersion - minVersion + 1 == commitResponse.getCommits.size,
          "Commit map should have a contiguous range of unbackfilled commits."
        )
        minVersion.toInt - 1
      }
    }
    (0 to maxUntrackedVersion).foreach { version =>
      assertBackfilled(version, logPath, commitTimestampsOpt.map(_(version)))
    }
  }

  test("test basic commit and backfill functionality") {
    withTempDirAndEngine { (tablePath, engine) =>
      val cc = new InMemoryCommitCoordinatorBuilder(batchSize).build(Collections.emptyMap())
      val logPath = new Path(tablePath, "_delta_log")

      val tableConf = cc.registerTable(
        logPath,
        -1L,
        CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(Metadata.empty()),
        CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(new Protocol(1, 1)))

      val e = intercept[CommitFailedException] {
        commit(
          logPath,
          tableConf, version = 0, timestamp = 0, util.Arrays.asList("0", "0"), cc)
      }
      assert(e.getMessage === "Commit version 0 must go via filesystem.")
      writeCommitZero(engine, logPath, util.Arrays.asList("0", "0"))
      assertGetCommitResponseEqual(
        cc.getCommits(logPath, tableConf, null, null),
        new GetCommitsResponse(Collections.emptyList(), -1))
      assertBackfilled(version = 0, logPath, Some(0L))

      // Test backfilling functionality for commits 1 - 8
      (1 to 8).foreach { version =>
        commit(
          logPath,
          tableConf,
          version, version, util.Arrays.asList(s"$version", s"$version"), cc)
        validateBackfillStrategy(engine, cc, logPath, tableConf, version)
        assert(cc.getCommits(logPath, tableConf, null, null).getLatestTableVersion == version)
      }

      // Test that out-of-order backfill is rejected
      intercept[IllegalArgumentException] {
        registerBackfillOp(cc, logPath, 10)
      }
      assertInvariants(logPath, tableConf, cc)
    }
  }
}

class InMemoryCommitCoordinator1Suite extends InMemoryCommitCoordinatorSuite(1)
class InMemoryCommitCoordinator5Suite extends InMemoryCommitCoordinatorSuite(5)
