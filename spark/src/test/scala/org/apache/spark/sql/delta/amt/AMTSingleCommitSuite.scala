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

import java.sql.Timestamp

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaFileProviderUtils, DeltaLog, DeltaOperations, DeltaSingleCommit, SingleCommit}
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, AddFile, Checkpoint, CommitInfo, FileAction, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.shims.GridTestShim
import org.apache.spark.sql.delta.util.{DeltaCommitFileProvider, FileNames}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

/**
 * Tests for DeltaSingleCommit handling with log and manifest commits.
 */
class AMTSingleCommitSuite extends AMTIncrementalWriteTestBase with GridTestShim {

  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(1)

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_COMMIT_INFO_DATA_CHANGE_READ_ENABLED.key, "true")
    .set(DeltaSQLConf.AMT_SINGLE_COMMIT_READ_ENABLED.key, "true")

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
    deltaLog.getSingleCommitIterator(startVersion = version)
      .find(_.version == version)
      .getOrElse(fail(s"no commit at version $version"))
      .asInstanceOf[SingleCommit]

  gridTest("timestamp uses ICT or falls back to file modification time")(
    Seq[(Option[CommitInfo], Long)](
      (Some(CommitInfo.empty().copy(
        timestamp = new Timestamp(1000L), inCommitTimestamp = Some(2000L))), 2000L),
      (Some(CommitInfo.empty().copy(timestamp = new Timestamp(1000L))), 3000L),
      (None, 3000L))) { case (commitInfoOpt, expectedTimestamp) =>
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(dir.getCanonicalPath))
      val hadoopConf = deltaLog.newDeltaHadoopConf()
      val commitPath = FileNames.unsafeDeltaFile(deltaLog.logPath, 0L)
      val fs = commitPath.getFileSystem(hadoopConf)
      fs.mkdirs(deltaLog.logPath)
      val actions: Seq[Action] = commitInfoOpt.toSeq :+ createTestAddFile(encodedPath = "a")
      deltaLog.store.write(commitPath, actions.iterator.map(_.json), overwrite = false, hadoopConf)
      fs.setTimes(commitPath, 3000L, -1L)
      val status = fs.getFileStatus(commitPath)
      val commit: DeltaSingleCommit = SingleCommit(deltaLog, 0L, status)
      assert(commit.timestamp === expectedTimestamp)
      assert(commit.getActionsIterator().processAndClose(_.toList) === actions.toList)
    }
  }

  /** Removes inline AddFile and RemoveFile actions while preserving every other JSON action. */
  private def stripInlineAddAndRemoveActions(
      deltaLog: DeltaLog,
      version: Long): Seq[Action] = {
    val commitPath =
      DeltaCommitFileProvider(deltaLog.unsafeVolatileSnapshot).deltaFile(version)
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    val retainedJson = deltaLog.store.readAsIterator(commitPath, hadoopConf).filter { json =>
      Action.fromJson(json) match {
        case _: AddFile | _: RemoveFile => false
        case _ => true
      }
    }.toSeq
    deltaLog.store.write(commitPath, retainedJson.iterator, overwrite = true, hadoopConf)
    retainedJson.map(Action.fromJson)
  }

  test("getActionsIterator returns the commit's actions and matches getChanges") {
    withTable("amt_getactions") {
      val deltaLog = createLogCommitTable("amt_getactions", numVersions = 3)
      val viaHandle = deltaLog.getSingleCommitIterator(startVersion = 0).map { commit =>
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

      // Read commit 2 (the multi-action one) back through the DeltaSingleCommit API.
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

  test("getSingleCommitIterator yields one DeltaSingleCommit per commit with the right version") {
    withSQLConf(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "true") {
      withTable("amt_versions") {
        val deltaLog = createLogCommitTable("amt_versions", numVersions = 3)
        // CREATE is version 0, the three INSERTs are versions 1..3.
        val commits = deltaLog.getSingleCommitIterator(startVersion = 0).toList
        assert(commits.map(_.version) === (0L to 3L).toList)

        val inCommitTimestamps = deltaLog.getChanges(startVersion = 0).map {
          case (version, actions) =>
            actions.collectFirst { case info: CommitInfo => info }.flatMap(_.inCommitTimestamp)
              .getOrElse(fail(s"missing ICT for version $version"))
        }.toList
        assert(commits.map(_.timestamp) === inCommitTimestamps)

        // The implementation-specific modification time matches getChangeLogFiles even with ICT.
        val fileModTimes =
          deltaLog.getChangeLogFiles(startVersion = 0).map(_._2.getModificationTime).toList
        assert(commits.map(_.asInstanceOf[SingleCommit].fileModificationTimestamp) === fileModTimes)
      }
    }
  }

  test("getSingleCommitIterator endVersion override bounds the range (both inclusive)") {
    withTable("amt_range") {
      val deltaLog = createLogCommitTable("amt_range", numVersions = 5)
      val commits = deltaLog.getSingleCommitIterator(
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
      val viaUnsafe = commit.asInstanceOf[SingleCommit]
        .getLogCommitActionsIteratorUnsafe().processAndClose(_.toList)
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
    val deltaLog = deltaLogForName(context.tableName)
    val manifestVersion = context.manifestCommitVersion
    val commit = commitAt(deltaLog, manifestVersion)
    val e = intercept[IllegalStateException] {
      commit.asInstanceOf[SingleCommit]
        .getLogCommitActionsIteratorUnsafe().processAndClose(_.toList)
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

  /**
   * The data-changing file actions in `actions`, keyed by (path, deletion-vector id,
   * add-vs-remove).
   */
  private def dataChangeKeys(amtDeltaLog: DeltaLog, actions: Seq[Action]): Set[(String, Boolean)] =
    actions
      .collect { case f: FileAction if f.dataChange => f }
      .map(f => (
        f.toUniqueFileActionTuple(amtDeltaLog.dataPath, useObjectIdentity = true).toString,
        f.isInstanceOf[AddFile]))
      .toSet

  /**
   * Asserts that the manifest commit at `version` reconstructs `expectedAdds` AddFiles and
   * `expectedRemoves` RemoveFiles for Change Data Feed, and that they are the data-changing file
   * actions the commit wrote inline.
   */
  private def assertReconstructedActions(
      amtDeltaLog: DeltaLog,
      version: Long,
      expectedAdds: Int,
      expectedRemoves: Int): Unit = {
    val actions = commitAt(amtDeltaLog, version).getActions(skipNoDataChangeFileActions = true)
    val adds = actions.count(_.isInstanceOf[AddFile])
    val removes = actions.count(_.isInstanceOf[RemoveFile])
    assert(adds == expectedAdds && removes == expectedRemoves,
      s"v$version: expected $expectedAdds add(s) / $expectedRemoves remove(s), got " +
        s"$adds / $removes from $actions")
    assert(
      dataChangeKeys(amtDeltaLog, actions) ==
        dataChangeKeys(amtDeltaLog, actionsAt(amtDeltaLog, version)),
      s"v$version: the reconstructed file actions must match the inline ones; got $actions")
  }

  test("getActions returns the inline actions on a log commit") {
    withTable("amt_cdf_log") {
      val deltaLog = createLogCommitTable("amt_cdf_log", numVersions = 3)
      assert(checkpointAt(deltaLog, 2L).isEmpty, "v2 must be a log commit for this test")
      val commit = commitAt(deltaLog, 2L)
      val fromApi = commit.getActions()
      val inline = commit.getActionsIterator().processAndClose(_.toList)
      assert(fromApi === inline,
        s"log-commit actions must equal the inline actions; got $fromApi vs $inline")
    }
  }

  test(
    "getActions returns inline file actions when the content root version lags"
  ) {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val manifestCommitVersion = amtDeltaLog.update().version
        val checkpoint = checkpointAt(amtDeltaLog, manifestCommitVersion)
          .getOrElse(fail(s"expected a checkpoint at v$manifestCommitVersion"))
        assert(checkpoint.contentRoot.version < manifestCommitVersion,
          "the deferred checkpoint must have a lagging content root")

        val inlineAdd = fakeAdd(100)
        val commitPath =
          DeltaCommitFileProvider(amtDeltaLog.unsafeVolatileSnapshot)
            .deltaFile(manifestCommitVersion)
        val hadoopConf = amtDeltaLog.newDeltaHadoopConf()
        val actions = amtDeltaLog.store.readAsIterator(commitPath, hadoopConf).toSeq :+
          inlineAdd.json
        amtDeltaLog.store.write(commitPath, actions.iterator, overwrite = true, hadoopConf)

        val fileActions = commitAt(amtDeltaLog, manifestCommitVersion).getActions()
          .collect { case action: FileAction => action }
        assert(fileActions === Seq(inlineAdd))
      }
    }
  }

  test("getActions return AddFiles from root correctly") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val version = amtDeltaLog.update().version + 1
        // A handful of net-new files, staying in root.
        withInline { commitAmt(amtDeltaLog, (100 to 102).map(fakeAdd)) }
        assertReconstructedActions(amtDeltaLog, version, expectedAdds = 3, expectedRemoves = 0)
      }
    }
  }

  test("getActions return AddFiles from leaf correctly") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val leavesBefore = leafPointers(amtDeltaLog.update()).size
        val version = amtDeltaLog.update().version + 1
        // Enough new files that the root spills to new leaves.
        withInline { commitAmt(amtDeltaLog, (100 to 124).map(fakeAdd)) }
        assert(leafPointers(amtDeltaLog.update()).size > leavesBefore,
          "The insert must spill at least one new leaf for this scenario to be meaningful.")
        assertReconstructedActions(amtDeltaLog, version, expectedAdds = 25, expectedRemoves = 0)
      }
    }
  }

  test("getActions returns RemoveFiles from deleted_positions correctly") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val version = amtDeltaLog.update().version + 1
        withInline { commitAmt(amtDeltaLog, Seq(removeOf(amtDeltaLog, 1))) }
        assertReconstructedActions(amtDeltaLog, version, expectedAdds = 0, expectedRemoves = 1)
      }
    }
  }

  test("getActions returns RemoveFiles from root tombstone correctly") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        // Append a root-resident file (under the cap), then delete it in the next inline commit so
        // the delete is a no-back-reference remove that becomes a DELETED root DATA entry.
        withInline { commitAmt(amtDeltaLog, Seq(fakeAdd(31))) }
        val version = amtDeltaLog.update().version + 1
        withInline { commitAmt(amtDeltaLog, Seq(removeOf(amtDeltaLog, 31))) }
        assertReconstructedActions(amtDeltaLog, version, expectedAdds = 0, expectedRemoves = 1)
      }
    }
  }

  test(
    "getActions returns RemoveFiles and AddFiles from replaced_positions correctly"
  ) {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val version = amtDeltaLog.update().version + 1
        // Remove the leaf-resident file and re-add it under a new DV.
        withInline {
          commitAmt(amtDeltaLog, removeAndReAddWithDV(amtDeltaLog, Left(1)))
        }
        assertReconstructedActions(amtDeltaLog, version, expectedAdds = 1, expectedRemoves = 1)
      }
    }
  }

  test("getActions returns RemoveFiles and AddFiles root update correctly") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        withInline { commitAmt(amtDeltaLog, Seq(fakeAdd(31))) }
        val version = amtDeltaLog.update().version + 1
        // Remove the root-resident file and re-add it under a new DV.
        withInline {
          commitAmt(amtDeltaLog, removeAndReAddWithDV(amtDeltaLog, Left(31)))
        }
        assertReconstructedActions(amtDeltaLog, version, expectedAdds = 1, expectedRemoves = 1)
      }
    }
  }

  test(
    "getActions mixed case correctly - insert + leaf delete + leaf DV update"
  ) {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val version = amtDeltaLog.update().version + 1
        // File 1 deleted, file 2 DV-updated, file 100 inserted -- all in one inline commit.
        val actions =
          Seq[Action](removeOf(amtDeltaLog, 1)) ++
            removeAndReAddWithDV(amtDeltaLog, Left(2)) ++
            Seq(fakeAdd(100))
        withInline { commitAmt(amtDeltaLog, actions) }
        assertReconstructedActions(amtDeltaLog, version, expectedAdds = 2, expectedRemoves = 2)
      }
    }
  }

  test("getActions returns RemoveFiles across multiple leaves correctly") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val leaves = leafToAddFileMap(amtDeltaLog).values.toSeq
        assert(leaves.size >= 2, "scenario needs at least two leaves.")
        val version = amtDeltaLog.update().version + 1
        val actions = Seq[Action](leaves(0).head.remove, leaves(1).head.remove)
        withInline { commitAmt(amtDeltaLog, actions) }
        assertReconstructedActions(amtDeltaLog, version, expectedAdds = 0, expectedRemoves = 2)
      }
    }
  }

  test("getActions pairs preimage/postimage across multiple leaves correctly") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val leaves = leafToAddFileMap(amtDeltaLog).values.toSeq
        assert(leaves.size >= 2, "scenario needs at least two leaves.")
        val version = amtDeltaLog.update().version + 1
        // DV-update one file in each of two leaves: each leaf contributes a preimage remove
        // plus a modified root re-add.
        val actions =
          removeAndReAddWithDV(amtDeltaLog, Right(leaves(0).head)) ++
            removeAndReAddWithDV(amtDeltaLog, Right(leaves(1).head))
        withInline { commitAmt(amtDeltaLog, actions) }
        assertReconstructedActions(amtDeltaLog, version, expectedAdds = 2, expectedRemoves = 2)
      }
    }
  }

  test("getActions reconstructs a fully deleted leaf correctly") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val leaf = leafToAddFileMap(amtDeltaLog).values.head
        val version = amtDeltaLog.update().version + 1
        withInline { commitAmt(amtDeltaLog, leaf.map(_.remove)) }
        assertReconstructedActions(
          amtDeltaLog, version, expectedAdds = 0, expectedRemoves = leaf.size)
      }
    }
  }

  test("getActions mixed case across leaves correctly") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        // A root-resident file to delete alongside the leaf-level changes.
        withInline { commitAmt(amtDeltaLog, Seq(fakeAdd(31))) }
        val leaves = leafToAddFileMap(amtDeltaLog).values.toSeq
        assert(leaves.size >= 2, "scenario needs at least two leaves.")
        val version = amtDeltaLog.update().version + 1
        // Leaf A: delete a file. Leaf B: DV-update. Plus a root delete and an insert.
        val actions =
          Seq[Action](leaves(0).head.remove) ++
            removeAndReAddWithDV(amtDeltaLog, Right(leaves(1).head)) ++
            Seq[Action](removeOf(amtDeltaLog, 31), fakeAdd(100))
        withInline { commitAmt(amtDeltaLog, actions) }
        // leaf A delete (1 rm) + leaf B DV update (1 add + 1 rm) + root delete (1 rm) + insert
        // (1 add) = 2 adds, 3 removes.
        assertReconstructedActions(amtDeltaLog, version, expectedAdds = 2, expectedRemoves = 3)
      }
    }
  }

  test("getActions honors commitInfo.dataChange with skipNoDataChangeFileActions") {
    withSQLConf(
        DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10",
        DeltaSQLConf.DELTA_COMMIT_INFO_DATA_CHANGE_READ_ENABLED.key -> "true") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val version = amtDeltaLog.update().version + 1
        // A compaction style commit: remove a live leaf file and add a fresh file, both
        // dataChange=false. The tree still records the delete + insert, but CommitInfo.dataChange
        // is Some(false), so the reconstruction must suppress them entirely.
        val actions = Seq[Action](
          removeOf(amtDeltaLog, 1).copy(dataChange = false),
          fakeAdd(200, dataChange = false))
        withInline { commitAmt(amtDeltaLog, actions) }

        val commit = commitAt(amtDeltaLog, version)
        val commitInfo = commit.getActionsIterator().processAndClose(_.toList)
          .collectFirst { case ci: CommitInfo => ci }
        assert(commitInfo.flatMap(_.dataChange).contains(false),
          s"expected a no-data-change commit; got ${commitInfo.flatMap(_.dataChange)}.")
        assertReconstructedActions(amtDeltaLog, version, expectedAdds = 0, expectedRemoves = 0)
        // Without skipNoDataChangeFileActions, the tree's delete and insert are still returned,
        // and carry the commit's dataChange=false.
        val allFileActions = commit.getActions().collect { case action: FileAction => action }
        assert(allFileActions.count(_.isInstanceOf[AddFile]) === 1)
        assert(allFileActions.count(_.isInstanceOf[RemoveFile]) === 1)
        assert(allFileActions.forall(!_.dataChange), allFileActions)
      }
    }
  }

  test("getChanges reconstructs AddFile and RemoveFile while preserving inline actions") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val version = amtDeltaLog.update().version + 1
        val cdc = AddCDCFile("cdc-100.parquet", Map.empty, size = 1L)
        val add = fakeAdd(100)
        val remove = removeOf(amtDeltaLog, 1)
        withInline {
          commitAmt(amtDeltaLog, Seq[Action](remove, add, cdc))
        }

        val retainedInlineActions = stripInlineAddAndRemoveActions(amtDeltaLog, version)
        val actions = amtDeltaLog.getChanges(version).next()._2

        assert(actions.collect { case action: AddFile => action.path } === Seq(add.path))
        assert(actions.collect { case action: RemoveFile => action.path } === Seq(remove.path))
        // The tree does not record when a file was removed, so a reconstructed remove carries
        // the commit timestamp rather than the time it was read.
        val commitTimestamp = actions.collectFirst { case ci: CommitInfo => ci }
          .flatMap(_.inCommitTimestamp)
          .getOrElse(commitAt(amtDeltaLog, version).fileModificationTimestamp)
        assert(actions.collect { case action: RemoveFile => action.deletionTimestamp } ===
          Seq(Some(commitTimestamp)))
        assert(actions.collect { case action: AddCDCFile => action } === Seq(cdc))
        assert(actions.exists(_.isInstanceOf[Checkpoint]))
        assert(actions.filter {
          case _: AddFile | _: RemoveFile => false
          case _ => true
        } === retainedInlineActions)
        // Re-reading the commit reconstructs exactly the same actions.
        assert(amtDeltaLog.getChanges(version).next()._2 === actions)
        // The commit changed data, so skipping file actions that change no data keeps them all,
        // including the AddCDCFile, whose dataChange is always false.
        assert(amtDeltaLog.getChanges(
          version,
          version,
          catalogTableOpt = None,
          failOnDataLoss = false,
          skipNoDataChangeFileActions = true).next()._2 === actions)
      }
    }
  }

  /**
   * Commits leaf deletes, a leaf DV update (an add and a remove of one path), a root delete and
   * inserts out of path order, so the commit's changes span the root and several leaves. Returns
   * the commit's version and its file actions.
   */
  private def commitAcrossRootAndLeaves(amtDeltaLog: DeltaLog): (Long, Seq[Action]) = {
    setupAmt(
      amtDeltaLog,
      entriesPerLeaf = entriesPerLeaf,
      initialIdRangeInLeaf = 1 to leafPackedFiles)
    val rootFileId = leafPackedFiles + 1
    withInline { commitAmt(amtDeltaLog, Seq(fakeAdd(rootFileId))) }
    val leaves = leafToAddFileMap(amtDeltaLog).values.toSeq
    assert(leaves.size >= 2, "scenario needs at least two leaves.")
    val version = amtDeltaLog.update().version + 1
    val changes =
      Seq[Action](
        leaves(0).head.remove, leaves(1).head.remove, removeOf(amtDeltaLog, rootFileId)) ++
        removeAndReAddWithDV(amtDeltaLog, Right(leaves(0)(1))) ++
        Seq(fakeAdd(101), fakeAdd(100))
    withInline { commitAmt(amtDeltaLog, changes) }
    (version, changes)
  }

  /** The provider of the tree the inline manifest commit at `version` wrote. */
  private def providerAt(deltaLog: DeltaLog, version: Long): AMTCheckpointProvider = {
    val checkpoint = checkpointAt(deltaLog, version)
      .getOrElse(fail(s"expected a manifest commit at v$version"))
    AMTCheckpointProvider.fromCheckpoint(spark, deltaLog, checkpoint, version)
  }

  test("getChanges orders a manifest commit's reconstructed file actions by manifest position") {
    withSQLConf(leafPackingConfs: _*) {
      withAmtTable { amtDeltaLog =>
        val (version, changes) = commitAcrossRootAndLeaves(amtDeltaLog)
        def fileActions(): Seq[FileAction] = amtDeltaLog.getChanges(version).next()._2
          .collect { case action: FileAction => action }

        val ordered = fileActions()
        assert(ordered.size === changes.size)
        // The actions follow the manifest path, then the position, of the entries recording them.
        val provider = providerAt(amtDeltaLog, version)
        val byManifestPosition = provider.reconstructChangeFiles(amtDeltaLog)
          .select("leafPath", "pos", "add.path", provider.REMOVED_COLUMN)
          .collect()
          .sortBy(row => (row.getString(0), row.getLong(1)))
          .map(row => (row.getString(2), row.getBoolean(3)))
          .toSeq
        assert(ordered.map {
          case add: AddFile => (add.path, false)
          case remove: RemoveFile => (remove.path, true)
          case action => fail(s"unexpected file action $action")
        } === byManifestPosition)
        // Re-reading lists them in the same order, however many partitions the sort uses.
        Seq("1", "7").foreach { numShufflePartitions =>
          withSQLConf(
              SQLConf.SHUFFLE_PARTITIONS.key -> numShufflePartitions,
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
            assert(fileActions() === ordered)
          }
        }
      }
    }
  }

  /**
   * The actions of versions `startVersion` to `endVersion` through each overload of `getChanges`,
   * with single-commit read on or off.
   */
  private def changesWithSingleCommitRead(
      deltaLog: DeltaLog,
      startVersion: Long,
      endVersion: Long,
      singleCommitRead: Boolean): Seq[List[(Long, List[Action])]] =
    withSQLConf(DeltaSQLConf.AMT_SINGLE_COMMIT_READ_ENABLED.key -> singleCommitRead.toString) {
      Seq(
        deltaLog.getChanges(startVersion).takeWhile(_._1 <= endVersion),
        deltaLog.getChanges(
          startVersion,
          endVersion,
          catalogTableOpt = None,
          failOnDataLoss = false,
          skipNoDataChangeFileActions = false))
        .map(_.map { case (version, actions) => (version, actions.toList) }.toList)
    }

  test("getChanges reads log commits the same with single-commit read on or off") {
    withTable("amt_getchanges_single_commit_read") {
      val deltaLog = createLogCommitTable("amt_getchanges_single_commit_read", numVersions = 3)
      val latestVersion = deltaLog.update().version
      val enabled = changesWithSingleCommitRead(
        deltaLog, startVersion = 0, latestVersion, singleCommitRead = true)
      enabled.foreach(changes => assert(changes.map(_._1) === (0L to latestVersion).toList))
      assert(changesWithSingleCommitRead(
        deltaLog, startVersion = 0, latestVersion, singleCommitRead = false) === enabled)
    }
  }

  test("getChanges reads the log file as written with single-commit read off") {
    withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
      withAmtTable { amtDeltaLog =>
        setupAmt(amtDeltaLog, entriesPerLeaf = 10, initialIdRangeInLeaf = 1 to 30)
        val version = amtDeltaLog.update().version + 1
        val add = fakeAdd(100)
        val remove = removeOf(amtDeltaLog, 1)
        withInline {
          commitAmt(amtDeltaLog, Seq[Action](remove, add))
        }
        // Without its inline file actions, only the tree records the commit's file actions.
        val retainedInlineActions = stripInlineAddAndRemoveActions(amtDeltaLog, version).toList

        changesWithSingleCommitRead(amtDeltaLog, version, version, singleCommitRead = false)
          .foreach(changes => assert(changes === List((version, retainedInlineActions))))
        changesWithSingleCommitRead(amtDeltaLog, version, version, singleCommitRead = true)
          .foreach { changes =>
            assert(changes.map(_._1) === List(version))
            val actions = changes.head._2
            assert(actions.collect { case action: AddFile => action.path } === Seq(add.path))
            assert(actions.collect { case action: RemoveFile => action.path } === Seq(remove.path))
          }
      }
    }
  }
}
