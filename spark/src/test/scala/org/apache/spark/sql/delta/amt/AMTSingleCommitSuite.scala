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

package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.{DeltaFileProviderUtils, DeltaLog, DeltaOperations, SingleCommit}
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}

/**
 * Tests for SingleCommit related handling.
 */
class AMTSingleCommitSuite extends AMTCheckpointTestBase {

  /**
   * Create an AMT-featured table with `numVersions` plain INSERT commits that all stay classic log
   * commits (checkpoint interval far away, huge size threshold so no manifest is emitted); return
   * its [[DeltaLog]].
   */
  private def createLogCommitTable(name: String, numVersions: Int): DeltaLog = {
    createAMTTable(name, checkpointInterval = 1000)
    (1 to numVersions).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))
    deltaLogForName(name)
  }

  /** A [[SingleCommit]] handle for exactly `version`. */
  private def commitAt(deltaLog: DeltaLog, version: Long): SingleCommit =
    deltaLog.getChangesIterator(startVersion = version)
      .find(_.version == version)
      .getOrElse(fail(s"no commit at version $version"))

  test("getActionsIterator returns the commit's actions and matches getChanges") {
    withTable("amt_getactions") {
      val deltaLog = createLogCommitTable("amt_getactions", numVersions = 3)
      val viaHandle = deltaLog.getChangesIterator(startVersion = 0).map { commit =>
        (commit.version, commit.getActionsIterator().processAndClose(_.toList))
      }.toList
      val viaGetChanges = deltaLog.getChanges(startVersion = 0).map {
        case (version, actions) => (version, actions.toList)
      }.toList
      assert(viaHandle === viaGetChanges)
    }
  }

  test("getActionsIterator is rewindable and replays the same actions") {
    withTable("amt_rewind") {
      val deltaLog = createLogCommitTable("amt_rewind", numVersions = 1)
      // Version 1 is the single INSERT; open its actions and read twice via rewind().
      val iter = commitAt(deltaLog, 1L).getActionsIterator()
      try {
        val firstPass = iter.toList
        assert(firstPass.nonEmpty)
        iter.rewind()
        assert(iter.toList === firstPass)
      } finally {
        iter.close()
      }
    }
  }

  test("getActionsIterator returns all actions of a multi-action commit") {
    withTable("amt_multi_action") {
      val name = "amt_multi_action"
      // v1 adds "old"; v2 packs several data actions -- three new AddFiles and a RemoveFile of
      // "old" -- into one manual commit so the commit has more than one action.
      val deltaLog = createLogCommitTable(name, numVersions = 0)
      val oldFile = createTestAddFile(encodedPath = "old")
      deltaLog.startTransaction()
        .commit(Seq(oldFile), DeltaOperations.ManualUpdate)
      val dataActions: Seq[Action] = Seq(
        createTestAddFile(encodedPath = "a"),
        createTestAddFile(encodedPath = "b"),
        createTestAddFile(encodedPath = "c"),
        oldFile.remove)
      deltaLog.startTransaction()
        .commit(dataActions, DeltaOperations.ManualUpdate)

      // Read commit 2 (the multi-action one) back through the SingleCommit API.
      val commit = commitAt(deltaLog, 2L)
      val actions = commit.getActionsIterator().processAndClose(_.toList)

      // It contains more than one action, and every data action we committed is present (order and
      // exact identity of synthesized CommitInfo/Protocol are not asserted -- commit() adds those).
      assert(actions.size > 1, s"expected a multi-action commit, got: $actions")
      val addedPaths = actions.collect { case a: AddFile => a.path }.toSet
      assert(addedPaths === Set("a", "b", "c"), s"missing AddFiles, got: $addedPaths")
      val removedPaths = actions.collect { case r: RemoveFile => r.path }.toSet
      assert(removedPaths === Set("old"), s"missing RemoveFile, got: $removedPaths")

      // The authoritative invariant: getActionsIterator returns exactly what getChanges returns for
      // this same multi-action commit.
      val viaGetChanges = deltaLog.getChanges(startVersion = 2).next()._2.toList
      assert(actions === viaGetChanges)
    }
  }

  test("getChangesIterator yields one SingleCommit per commit with the right version") {
    withTable("amt_versions") {
      val deltaLog = createLogCommitTable("amt_versions", numVersions = 3)
      // CREATE is version 0, the three INSERTs are versions 1..3.
      val commits = deltaLog.getChangesIterator(startVersion = 0).toList
      assert(commits.map(_.version) === (0L to 3L).toList)

      // The handle exposes the commit file's modification time (not the in-commit timestamp), so it
      // matches what getChangeLogFiles reports for the same commit.
      val fileModTimes =
        deltaLog.getChangeLogFiles(startVersion = 0).map(_._2.getModificationTime).toList
      assert(commits.map(_.fileModificationTimestamp) === fileModTimes)
    }
  }

  test("getChangesIterator endVersion override bounds the range (both inclusive)") {
    withTable("amt_range") {
      val deltaLog = createLogCommitTable("amt_range", numVersions = 5)
      val commits = deltaLog.getChangesIterator(
        startVersion = 1, endVersion = 3, catalogTableOpt = None, failOnDataLoss = true).toList
      assert(commits.map(_.version) === List(1L, 2L, 3L))
    }
  }

  private val insertTwoRows: String => Unit = { name =>
    sql(s"INSERT INTO $name VALUES (1)")
    sql(s"INSERT INTO $name VALUES (2)")
  }

  private val insertThirdRow: Option[AMTCheckpointTrigger] =
    Some(name => Right(s"INSERT INTO $name VALUES (3)"))

  testAcrossAMTCheckpointScenarios(
    "getLogCommitActionsIteratorUnsafe reads a log commit like getActionsIterator",
    tableName = "amt_log_read")(
    setup = insertTwoRows,
    inlineCheckpointTriggerActionsOrSQL = insertThirdRow
  ) { context =>
    val deltaLog = deltaLogForName(context.tableName)
    val logCommits =
      (0L to context.manifestCommitVersion).filter(checkpointAt(deltaLog, _).isEmpty)
    assert(logCommits.nonEmpty, "expected at least one log commit before the manifest commit")
    logCommits.foreach { v =>
      val commit = commitAt(deltaLog, v)
      val viaUnsafe = commit.getLogCommitActionsIteratorUnsafe().processAndClose(_.toList)
      val viaPlain = commit.getActionsIterator().processAndClose(_.toList)
      assert(viaUnsafe === viaPlain, s"v$v disagreed between the unsafe and plain readers")
    }
  }

  testAcrossAMTCheckpointScenarios(
    "getLogCommitActionsIteratorUnsafe throws on a manifest commit",
    tableName = "amt_manifest_read")(
    setup = insertTwoRows,
    inlineCheckpointTriggerActionsOrSQL = insertThirdRow
  ) { context =>
    val manifestVersion = context.manifestCommitVersion
    val commit = commitAt(deltaLogForName(context.tableName), manifestVersion)
    val e = intercept[IllegalStateException] {
      commit.getLogCommitActionsIteratorUnsafe().processAndClose(_.toList)
    }
    assert(e.getMessage.contains(s"commit $manifestVersion"))
    assert(e.getMessage.contains("cannot be read as a log commit"))
  }

  testAcrossAMTCheckpointScenarios(
    "parallelReadAndParseLogCommitsAsSeqUnsafe throws if any commit is a manifest commit",
    tableName = "amt_par_manifest")(
    setup = insertTwoRows,
    inlineCheckpointTriggerActionsOrSQL = insertThirdRow
  ) { context =>
    val deltaLog = deltaLogForName(context.tableName)
    val firstLogCommit = context.postSetupSnapshot.version
    assert(checkpointAt(deltaLog, firstLogCommit).isEmpty, s"v$firstLogCommit must be a log commit")
    val commits = DeltaFileProviderUtils.getCommitsInVersionRange(
      spark,
      deltaLog,
      startVersion = firstLogCommit,
      endVersion = context.manifestCommitVersion,
      catalogTableOpt = None)
    val e = intercept[Exception] {
      DeltaFileProviderUtils.parallelReadAndParseLogCommitsAsSeqUnsafe(spark, commits)
    }
    val causes = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).toList
    assert(
      causes.exists { c =>
        c.isInstanceOf[IllegalStateException] &&
          c.getMessage != null && c.getMessage.contains("cannot be read as a log commit")
      },
      s"expected an IllegalStateException about a manifest commit in the cause chain, got: " +
        causes.map(c => s"${c.getClass.getName}: ${c.getMessage}").mkString(" -> "))
  }

  testAcrossAMTCheckpointScenarios(
    "parallelReadAndParseLogCommitsAsSeqUnsafe reads a range after the manifest commit",
    tableName = "amt_par_after_manifest")(
    setup = insertTwoRows,
    inlineCheckpointTriggerActionsOrSQL = insertThirdRow
  ) { context =>
    val deltaLog = deltaLogForName(context.tableName)
    // Three log commits land after the manifest commit, so the table looks like
    // [.., log, manifest, log, log, log] and the range below starts past the manifest commit.
    (4 to 6).foreach(i => sql(s"INSERT INTO ${context.tableName} VALUES ($i)"))
    val firstLogCommit = context.manifestCommitVersion + 1
    val lastLogCommit = context.manifestCommitVersion + 3
    assert((firstLogCommit to lastLogCommit).forall(checkpointAt(deltaLog, _).isEmpty),
      s"v$firstLogCommit..v$lastLogCommit must all be log commits")

    val commits = DeltaFileProviderUtils.getCommitsInVersionRange(
      spark,
      deltaLog,
      startVersion = firstLogCommit,
      endVersion = lastLogCommit,
      catalogTableOpt = None)
    // Does not throw.
    val viaUnsafe = DeltaFileProviderUtils.parallelReadAndParseLogCommitsAsSeqUnsafe(spark, commits)
    val viaPlain = commits.map(_.getActionsIterator().processAndClose(_.toSeq))
    assert(viaUnsafe === viaPlain)
  }
}
