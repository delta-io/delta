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

import org.apache.spark.sql.delta.{Checkpoints, DeletionVectorsTestUtils, DeltaLog, DeltaOperations, Snapshot}
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.actions.{AddFile, Checkpoint, ContentRoot, DeletionVectorDescriptor}
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.functions.col

/**
 * Verifies that [[Snapshot]] APIs correct results on AMT tables.
 */
class AMTSnapshotSuite extends AMTCheckpointTestBase with DeletionVectorsTestUtils {

  import testImplicits._

  ///////////////////////////
  // Post commit snapshot
  ///////////////////////////

  testAcrossAMTCheckpointScenarios(
      "snapshot.allFiles reflects a DELETE on the checkpoint boundary",
      "amt_delete_boundary")(
      setup = name => (1 to 3).foreach(i => sql(s"INSERT INTO $name VALUES ($i)")),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"DELETE FROM $name WHERE id = 1"))) { context =>
    // allFiles must reflect the DELETE: the removed file is gone from the post-commit live set.
    checkAnswer(spark.read.table(context.tableName), Seq(Row(2), Row(3)))
    // The manifest tree captures exactly the post-DELETE live files (computePostCommitState applied
    // the RemoveFile), i.e. it matches snapshot.allFiles.
    assertReconstructsLiveFileSet(context)
  }

  testAcrossAMTCheckpointScenarios(
      "plain AMT tables reconstruct AddFiles with no amtPassthrough",
      "amt_passthrough_roundtrip")(
      setup = name => sql(s"INSERT INTO $name VALUES (1)"),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (2)"))) { context =>
    // These are plain parquet/v4 files with no Iceberg passthrough, so `toAddFile` carries
    // nothing (avoids per-file overhead).
    val files = context.postCheckpointSnapshot.allFiles.collect()
    assert(files.nonEmpty)
    assert(files.forall(_.amtPassthrough.isEmpty),
      s"plain AMT files must carry no passthrough; got " +
        s"${files.map(_.amtPassthrough).mkString(", ")}")
  }

  testAcrossAMTCheckpointScenarios(
      "snapshot.allFiles matches leaves across insert/overwrite/delete before a checkpoint",
      "amt_overwrite")(
      setup = name => {
        sql(s"INSERT INTO $name VALUES (1)")
        sql(s"INSERT INTO $name VALUES (2)")
        sql(s"INSERT OVERWRITE $name VALUES (10), (20)")
      },
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"DELETE FROM $name WHERE id = 10"))) { context =>
    // The overwrite drops the files for 1 and 2; the DELETE then drops id=10, leaving only id=20.
    checkAnswer(spark.read.table(context.tableName), Seq(Row(20)))
    assert(context.postCheckpointSnapshot.allFiles.count() == 1,
      "Only the surviving overwrite file should be live.")
    // The tree must capture exactly the surviving live files after overwrite + delete.
    assertReconstructsLiveFileSet(context)
  }

  // AMT inline or not should be irrelevant to test result.
  testAcrossAMTCheckpointScenarios(
    "amtPassthrough survives from one AMT into the next",
    "amt_passthrough_amt_to_amt",
    deferredScenarios = Seq.empty
  )(
    setup = name => {
      sql(s"INSERT INTO $name VALUES (1)")
      withSQLConf(
        DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key
          -> "1"
      ) {
        deltaLogForName(name)
          .startTransaction()
          .commit(
            Seq(
              createTestAddFile(encodedPath = "passthrough-file")
                .copy(amtPassthrough = Some(fullPassthrough))
            ),
            DeltaOperations.ManualUpdate
          )
      }
      // At this point tree should be materialized.
      val seeded =
        amtFilesInTree(deltaLogForName(name).update(), Some("passthrough-file"))
      assert(
        seeded.length == 1,
        s"expected one tree entry for passthrough-file, got: $seeded."
      )
      assert(
        seeded.head.amtPassthrough.contains(fullPassthrough),
        "sanity: passthrough must be in the seeded AMT before the checkpoint."
      )
    },
    inlineCheckpointTriggerActionsOrSQL =
      Some(name => Right(s"INSERT INTO $name VALUES (100)"))
  ) { context =>
    val seededVersion = amtProvider(context.postSetupSnapshot)
      .getOrElse(fail("the seed commit must have emitted an AMT"))
      .version
    assert(
      context.provider.version > seededVersion,
      s"a new AMT must have re-materialized past the seeded one at v$seededVersion; " +
        s"got v${context.provider.version}."
    )

    // The passthrough survives into the re-materialized AMT and reconstructs on read.
    val snapshot = context.postCheckpointSnapshot
    val inTree = amtFilesInTree(snapshot, Some("passthrough-file"))
    assert(
      inTree.length == 1,
      s"expected one tree entry for passthrough-file, got: $inTree."
    )
    assert(
      inTree.head.amtPassthrough.contains(fullPassthrough),
      s"passthrough must propagate into the re-materialized AMT; got " +
        s"${inTree.head.amtPassthrough}."
    )
    val liveFile =
      snapshot.allFiles.collect().filter(_.path == "passthrough-file")
    assert(
      liveFile.length == 1,
      s"expected exactly one seeded file, got: ${liveFile.toSeq}."
    )
    assert(
      liveFile.head.amtPassthrough.contains(fullPassthrough),
      s"passthrough must reconstruct from the re-materialized AMT; got " +
        s"${liveFile.head.amtPassthrough}."
    )
  }

  // AMT inline or not should be irrelevant to test result.
  testAcrossAMTCheckpointScenarios(
    "amtPassthrough survives from one AMT, with in-place rewrite, into the next",
    "amt_passthrough_inplace",
    deferredScenarios = Seq.empty
  )(
    setup = name => {
      sql(s"INSERT INTO $name VALUES (1)")
      val log = deltaLogForName(name)
      withSQLConf(
        DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key
          -> "1"
      ) {
        log
          .startTransaction()
          .commit(
            Seq(
              createTestAddFile(encodedPath = "passthrough-file")
                .copy(
                  amtPassthrough = Some(fullPassthrough),
                  stats = """{"numRecords":1}"""
                )
            ),
            DeltaOperations.ManualUpdate
          )
      }
      val seedAMTVersion = amtProvider(log.update()).map(_.version)
      // AMTPassthrough before the rewrite.
      val seeded = amtFilesInTree(log.update(), Some("passthrough-file"))
      assert(
        seeded.length == 1,
        s"expected one tree entry for passthrough-file, got: $seeded."
      )
      assert(
        seeded.head.amtPassthrough.contains(fullPassthrough),
        "passthrough seeded into the AMT."
      )
      val current = log
        .update()
        .allFiles
        .collect()
        .find(_.path == "passthrough-file")
        .getOrElse(fail("passthrough-file must be live."))
      assert(
        current.amtPassthrough.contains(fullPassthrough),
        s"the live file carries passthrough before the rewrite; got " +
          s"${current.amtPassthrough}."
      )

      // The rewrite must land as a plain LOG commit.
      withSQLConf(
        DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key
          -> Long.MaxValue.toString
      ) {
        log
          .startTransaction()
          .commit(
            Seq(
              current.copy(
                stats = """{"numRecords":1,"probe":true}""",
                dataChange = false
              )
            ),
            DeltaOperations.ComputeStats(predicate = Nil)
          )
      }
      // Verify that AMT checkpoint version stays the same.
      val afterRewriteAMTVersion = amtProvider(log.update()).map(_.version)
      assert(
        afterRewriteAMTVersion == seedAMTVersion,
        s"the in-place rewrite must NOT be committed into an AMT; the tree moved from " +
          s"$seedAMTVersion to $afterRewriteAMTVersion."
      )

      // The reconstructed live file keeps its passthrough, and the stats update lands.
      val afterRewrite =
        log.update().allFiles.collect().filter(_.path == "passthrough-file")
      assert(afterRewrite.length == 1)
      assert(
        afterRewrite.head.stats == """{"numRecords":1,"probe":true}""",
        "the in-place stats update itself must land."
      )
      assert(
        afterRewrite.head.amtPassthrough.contains(fullPassthrough),
        s"passthrough must survive the in-place rewrite; got " +
          s"${afterRewrite.head.amtPassthrough}."
      )
    },
    inlineCheckpointTriggerActionsOrSQL =
      Some(name => Right(s"INSERT INTO $name VALUES (100)"))
  ) { context =>
    // A new AMT re-materialized past the rewrite and still carries the passthrough.
    val seededVersion = amtProvider(context.postSetupSnapshot)
      .getOrElse(fail("the seed commit must have emitted an AMT"))
      .version
    assert(
      context.provider.version > seededVersion,
      s"a new AMT must have re-materialized past the rewrite; the tree is still at " +
        s"v${context.provider.version} (seeded at v$seededVersion)."
    )
    val snapshot = context.postCheckpointSnapshot
    val survived = amtFilesInTree(snapshot, Some("passthrough-file"))
    assert(
      survived.length == 1,
      s"expected one tree entry for passthrough-file, got: $survived."
    )
    assert(
      survived.head.amtPassthrough.contains(fullPassthrough),
      s"passthrough must survive into the re-materialized AMT; got " +
        s"${survived.head.amtPassthrough}."
    )
    val finalLive =
      snapshot.allFiles.collect().filter(_.path == "passthrough-file")
    assert(
      finalLive.length == 1 &&
        finalLive.head.amtPassthrough.contains(fullPassthrough),
      s"passthrough must survive the rewrite + re-materialization; got " +
        s"${finalLive.map(_.amtPassthrough).toSeq}."
    )
  }

  // Bypasses `testAcrossAMTCheckpointScenarios`: this test needs the table to have NO AMT at all
  // while the passthrough is seeded.
  test("amtPassthrough from a log commit is carried into the next AMT") {
    withSQLConf(
      DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key
        -> "1"
    ) {
      withTable("amt_passthrough_from_log_commit") {
        val name = "amt_passthrough_from_log_commit"
        createAMTTable(name, checkpointInterval = 10000)
        val log = deltaLogForName(name)
        assert(
          amtProvider(log.update()).isEmpty,
          "no AMT may exist before the first trigger."
        )

        // V1 is a LOG commit: the file's passthrough lives only in the commit JSON at this point.
        val passthrough = fullPassthrough
        val fileWithPassthrough = createTestAddFile(encodedPath = "passthrough-file")
          .copy(amtPassthrough = Some(passthrough), stats = """{"numRecords":1}""")
        log
          .startTransaction()
          .commit(Seq(fileWithPassthrough), DeltaOperations.ManualUpdate)
        assert(
          amtProvider(deltaLogForName(name).update()).isEmpty,
          "the seeding commit must stay a LOG commit (no AMT yet)."
        )
        val fromLog = deltaLogForName(name)
          .update()
          .allFiles
          .collect()
          .filter(_.path == "passthrough-file")
        assert(
          fromLog.length == 1,
          s"expected the seeded file to be live, got: ${fromLog.toSeq}."
        )
        assert(
          fromLog.head.amtPassthrough.contains(passthrough),
          s"the log-commit file must carry its passthrough; got " +
            s"${fromLog.head.amtPassthrough}."
        )

        // Now trigger the table's first AMT.
        sql(s"ALTER TABLE $name SET TBLPROPERTIES ('delta.checkpointInterval' = '2')")
        sql(s"INSERT INTO $name VALUES (100)")
        sql(s"INSERT INTO $name VALUES (101)")
        val snapshot = deltaLogForName(name).update()
        val amtVersion = amtProvider(snapshot).map(_.version)
        assert(
          amtVersion.contains(5L),
          s"the first AMT must describe content version 5; got $amtVersion."
        )

        val inTree = amtFilesInTree(snapshot, Some("passthrough-file"))
        assert(
          inTree.length == 1,
          s"expected one tree entry for passthrough-file, got: $inTree."
        )
        assert(
          inTree.head.amtPassthrough.contains(passthrough),
          s"the log commit's passthrough must be carried into the AMT; got " +
            s"${inTree.head.amtPassthrough}."
        )
        val live = snapshot.allFiles.collect().filter(_.path == "passthrough-file")
        assert(
          live.length == 1 && live.head.amtPassthrough.contains(passthrough),
          s"passthrough must reconstruct from the AMT; got " +
            s"${live.map(_.amtPassthrough).toSeq}."
        )
      }
    }
  }

  /**
   * A passthrough with every [[AMTPassthrough]] field set, so tests use the whole carrier.
   */
  private def fullPassthrough: AMTPassthrough = AMTPassthrough(
    spec_id = Some(42),
    sort_order_id = Some(7),
    key_metadata = Some(Array[Byte](1, 2, 3)),
    split_offsets = Some(Seq(0L, 128L, 256L)))

  /**
   * The AMT-derived live [[AddFile]]s in the snapshot's current tree, optionally restricted to
   * `path`.
   */
  private def amtFilesInTree(snapshot: Snapshot, path: Option[String] = None): Seq[AddFile] = {
    val provider = amtProvider(snapshot).getOrElse(fail("Snapshot has no AMTCheckpointProvider."))
    val live = provider.loadActionsForStateReconstruction(spark, snapshot.deltaLog)
      .getOrElse(fail("AMT provider must contribute tree-derived file actions."))
      .where("add is not null")
      .select(col("add").as[AddFile])
      .collect()
      .toSeq
    path.map(p => live.filter(_.path == p)).getOrElse(live)
  }

  /**
   * Returns the latest snapshot after the caller adds trailing LOG commits.
   * This asserts that the tail does not emit a new AMT.
   */
  private def latestSnapshotAfterLogTail(context: AMTCheckpointScenarioContext): Snapshot = {
    val log = context.postCheckpointSnapshot.deltaLog
    val snapshot = log.update()
    assert(snapshot.version > context.postCheckpointSnapshot.version)
    assert(checkpointAt(log, snapshot.version).isEmpty)
    val provider = amtProvider(snapshot).getOrElse(
      fail("The latest snapshot must still use the AMT checkpoint provider."))
    assert(provider.checkpointAction == context.provider.checkpointAction)
    snapshot
  }

  testAcrossAMTCheckpointScenarios(
      "snapshot replays trailing append after an AMT checkpoint",
      "amt_tail_append")(
      setup = name => sql(s"INSERT INTO $name VALUES (1)"),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (2)"))) { context =>
    val addFilesAtCheckpointVersion = context.postCheckpointSnapshot.allFiles.collect()
    val addFilePathsAtCheckpointVersion = addFilesAtCheckpointVersion.map(_.path).toSet
    assert(addFilePathsAtCheckpointVersion.size == 2)

    sql(s"INSERT INTO ${context.tableName} VALUES (3)")
    val finalSnapshot = latestSnapshotAfterLogTail(context)

    assert(amtFilesInTree(finalSnapshot).map(_.path).toSet == addFilePathsAtCheckpointVersion)
    assert(finalSnapshot.allFiles.count() == addFilesAtCheckpointVersion.length + 1)
    checkAnswer(spark.read.table(context.tableName), Seq(Row(1), Row(2), Row(3)))
  }

  testAcrossAMTCheckpointScenarios(
      "snapshot replays trailing remove after an AMT checkpoint",
      "amt_tail_remove")(
      setup = name => sql(s"INSERT INTO $name VALUES (1)"),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (2)"))) { context =>
    val log = context.postCheckpointSnapshot.deltaLog
    val addFilesAtCheckpointVersion = context.postCheckpointSnapshot.allFiles.collect()
    assert(addFilesAtCheckpointVersion.length == 2)
    val setupFile = addFilesAtCheckpointVersion.head

    log.startTransaction().commit(
      Seq(setupFile.removeWithTimestamp()),
      DeltaOperations.ManualUpdate)
    val finalSnapshot = latestSnapshotAfterLogTail(context)

    assert(
      amtFilesInTree(finalSnapshot).map(_.path).toSet == addFilesAtCheckpointVersion
        .map(_.path)
        .toSet
    )
    val livePaths = finalSnapshot.allFiles.collect().map(_.path).toSet
    assert(!livePaths.contains(setupFile.path))
    assert(livePaths.size == 1)
    checkAnswer(
      spark.read.table(context.tableName).groupBy().count(),
      Seq(Row(1L)))
  }

  testAcrossAMTCheckpointScenarios(
      "snapshot replays trailing overwrite after an AMT checkpoint",
      "amt_tail_overwrite")(
      setup = name => sql(s"INSERT INTO $name VALUES (1)"),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (2)"))) { context =>
    val addFilePathsAtCheckpointVersion =
      context.postCheckpointSnapshot.allFiles.collect().map(_.path).toSet
    assert(addFilePathsAtCheckpointVersion.size == 2)

    sql(s"INSERT OVERWRITE ${context.tableName} VALUES (10), (20)")
    val finalSnapshot = latestSnapshotAfterLogTail(context)

    val livePaths = finalSnapshot.allFiles.collect().map(_.path).toSet
    assert(livePaths.nonEmpty)
    assert((livePaths & addFilePathsAtCheckpointVersion).isEmpty)
    checkAnswer(spark.read.table(context.tableName), Seq(Row(10), Row(20)))
  }

  testAcrossAMTCheckpointScenarios(
      "snapshot replays trailing DV update after an AMT checkpoint",
      "amt_tail_dv",
      sqlConfs = Seq(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false"))(
      setup = name => {
        Seq(10, 20, 30, 40, 50).toDF("id").coalesce(1)
          .write.mode("append").insertInto(name)
      },
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (60)"))) { context =>
    val log = context.postCheckpointSnapshot.deltaLog
    val addFilesAtCheckpointVersion =
      context.postCheckpointSnapshot.allFiles.collect()
    val addFilesAtSetupVersion =
      context.postSetupSnapshot.allFiles.collect()
    assert(addFilesAtSetupVersion.length == 1, "The five setup rows must land in a single file.")
    val setupFileAtSetupVersion = addFilesAtSetupVersion.head

    // Write a DV to the setup file.
    val dvActions = writeFileWithDVOnDisk(
      log,
      setupFileAtSetupVersion,
      RoaringBitmapArray(0L, 2L, 4L)
    )
    log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
    val finalSnapshot = latestSnapshotAfterLogTail(context)

    // Tree correctness test
    val addFilesInAmt = amtFilesInTree(finalSnapshot, Some(setupFileAtSetupVersion.path))
    assert(addFilesInAmt.length == 1)
    assert(
      addFilesInAmt.head.deletionVector == null,
      "The AMT provider should still expose the pre-DV checkpointed file.")
    checkAnswer(spark.read.table(context.tableName), Seq(Row(20), Row(40), Row(60)))

    // The setup file now has a DV associated with 3 rows deleted.
    val addFilesInFinalVersion = finalSnapshot.allFiles.collect()
    assert(addFilesInFinalVersion.length == addFilesAtCheckpointVersion.length)
    val setupFileAtFinalVersion = addFilesInFinalVersion
      .find(_.path == setupFileAtSetupVersion.path)
      .getOrElse(
        fail(
          s"The DV-updated file ${setupFileAtSetupVersion.path} must stay live."
        )
      )
    assert(setupFileAtFinalVersion.deletionVector != null)
    assert(setupFileAtFinalVersion.numPhysicalRecords.contains(5L))
    assert(setupFileAtFinalVersion.numLogicalRecords.contains(2L))
  }

  testAcrossAMTCheckpointScenarios(
      "snapshot replays trailing DV that fully deletes a file after an AMT checkpoint",
      "amt_tail_dv_full_delete",
      sqlConfs = Seq(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false"))(
      setup = name => {
        Seq(1, 2).toDF("id").coalesce(1).write.mode("append").insertInto(name)
      },
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (3)"))) { context =>
    val log = context.postCheckpointSnapshot.deltaLog
    val addFilesAtCheckpointVersion = context.postCheckpointSnapshot.allFiles.collect()
    val addFilesAtSetupVersion = context.postSetupSnapshot.allFiles.collect()
    assert(addFilesAtSetupVersion.length == 1, "The two setup rows must land in a single file.")
    val setupFileAtSetupVersion = addFilesAtSetupVersion.head

    // Fully delete the setup file.
    val dvActions = writeFileWithDVOnDisk(log, setupFileAtSetupVersion, RoaringBitmapArray(0L, 1L))
    log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
    val finalSnapshot = latestSnapshotAfterLogTail(context)

    checkAnswer(spark.read.table(context.tableName), Seq(Row(3)))

    // The setup file now has a DV associated with all rows deleted.
    val addFilesInFinalVersion = finalSnapshot.allFiles.collect()
    assert(addFilesInFinalVersion.length == addFilesAtCheckpointVersion.length)
    val setupFileAtFinalVersion = addFilesInFinalVersion
      .find(_.path == setupFileAtSetupVersion.path)
      .getOrElse(fail(s"The fully-deleted file ${setupFileAtSetupVersion.path} must stay live."))
    assert(setupFileAtFinalVersion.deletionVector != null)
    assert(setupFileAtFinalVersion.numPhysicalRecords.contains(2L))
    assert(setupFileAtFinalVersion.numLogicalRecords.contains(0L))
  }

  testAcrossAMTCheckpointScenarios(
      "snapshot replays trailing shared-DV update after an AMT checkpoint",
      "amt_tail_dv_shared_blob",
      sqlConfs = Seq(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false"))(
      setup = name => {
        Seq(1, 2).toDF("id").coalesce(1).write.mode("append").insertInto(name)
        Seq(3, 4).toDF("id").coalesce(1).write.mode("append").insertInto(name)
      },
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (5)"))) { context =>
    val log = context.postCheckpointSnapshot.deltaLog
    val addFilesAtSetupVersion = context.postSetupSnapshot.allFiles.collect()
    assert(addFilesAtSetupVersion.length == 2)

    // Attach DVs to both setup files.
    val dvActions = writeFilesWithDVsOnDisk(log, Seq(
      addFilesAtSetupVersion(0) -> RoaringBitmapArray(0L),
      addFilesAtSetupVersion(1) -> RoaringBitmapArray(0L)))
    log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
    val finalSnapshot = latestSnapshotAfterLogTail(context)

    checkAnswer(spark.read.table(context.tableName), Seq(Row(2), Row(4), Row(5)))
    val addFilesInFinalVersion = finalSnapshot.allFiles.collect()
    assert(addFilesInFinalVersion.length == 3,
      "Both DV'd files and the trigger file must remain live.")
    val filesWithDvs = addFilesInFinalVersion.filter(_.deletionVector != null)
    assert(filesWithDvs.length == 2)
    assert(filesWithDvs.forall(_.numLogicalRecords.contains(1L)))
    val blobPaths = filesWithDvs.map(_.deletionVector.absolutePath(log.dataPath)).toSet
    assert(blobPaths.size == 1)
  }

  testAcrossAMTCheckpointScenarios(
      "snapshot replays trailing remove of a file with DV in an AMT",
      "amt_tail_remove_dv_in_amt",
      sqlConfs = Seq(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false"))(
      setup = name => {
        Seq(10, 20, 30, 40, 50).toDF("id").coalesce(1)
          .write.mode("append").insertInto(name)
        // Attach a DV and checkpoint with AMT.
        val log = deltaLogForName(name)
        val fileToDv = log.update().allFiles.collect()
        assert(fileToDv.length == 1, "The five setup rows must land in a single file.")
        val dvActions = writeFileWithDVOnDisk(log, fileToDv.head, RoaringBitmapArray(0L, 2L, 4L))
        log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
      },
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (60)"))) { context =>
    val log = context.postCheckpointSnapshot.deltaLog
    val addFilesAtCheckpointVersion = context.postCheckpointSnapshot.allFiles.collect()
    assert(addFilesAtCheckpointVersion.length == 2)

    // AMT converts the DV type from u to r when reading it back.
    val addFilesAtSetupVersion = context.postSetupSnapshot.allFiles.collect()
    assert(addFilesAtSetupVersion.length == 1)
    val setupFileAtSetupVersion = addFilesAtSetupVersion.head
    assert(setupFileAtSetupVersion.deletionVector.storageType ==
      DeletionVectorDescriptor.UUID_DV_MARKER)
    val addFilesInAmt =
      amtFilesInTree(context.postCheckpointSnapshot, Some(setupFileAtSetupVersion.path))
    assert(addFilesInAmt.head.deletionVector.storageType ==
      DeletionVectorDescriptor.RELATIVE_DV_MARKER)

    // Delete the setup file
    log.startTransaction().commit(
      Seq(setupFileAtSetupVersion.removeWithTimestamp()),
      DeltaOperations.ManualUpdate)
    val finalSnapshot = latestSnapshotAfterLogTail(context)

    assert(
      amtFilesInTree(finalSnapshot).map(_.path).toSet ==
        addFilesAtCheckpointVersion.map(_.path).toSet)
    val livePaths = finalSnapshot.allFiles.collect().map(_.path).toSet
    assert(!livePaths.contains(setupFileAtSetupVersion.path))
    assert(livePaths.size == 1)
    checkAnswer(spark.read.table(context.tableName), Seq(Row(60)))
  }

  testAcrossAMTCheckpointScenarios(
      "snapshot replays trailing DV update of a file with DV in an AMT",
      "amt_tail_update_dv_in_amt",
      sqlConfs = Seq(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false"))(
      setup = name => {
        Seq(10, 20, 30, 40, 50).toDF("id").coalesce(1)
          .write.mode("append").insertInto(name)
        // Attach a DV and checkpoint with AMT.
        val log = deltaLogForName(name)
        val fileToDv = log.update().allFiles.collect()
        assert(fileToDv.length == 1, "The five setup rows must land in a single file.")
        val dvActions = writeFileWithDVOnDisk(log, fileToDv.head, RoaringBitmapArray(0L, 2L, 4L))
        log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
      },
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (60)"))) { context =>
    val log = context.postCheckpointSnapshot.deltaLog
    val addFilesAtCheckpointVersion = context.postCheckpointSnapshot.allFiles.collect()
    assert(addFilesAtCheckpointVersion.length == 2)

    // The checkpointed file's DV is committed in u form but stored in the leaf in r form.
    val addFilesAtSetupVersion = context.postSetupSnapshot.allFiles.collect()
    assert(addFilesAtSetupVersion.length == 1)
    val setupFileAtSetupVersion = addFilesAtSetupVersion.head
    assert(setupFileAtSetupVersion.deletionVector.storageType ==
      DeletionVectorDescriptor.UUID_DV_MARKER)

    // Supersede the checkpointed DV with a wider one
    val dvActions =
      writeFileWithDVOnDisk(log, setupFileAtSetupVersion, RoaringBitmapArray(0L, 1L, 2L, 4L))
    log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
    val finalSnapshot = latestSnapshotAfterLogTail(context)

    checkAnswer(spark.read.table(context.tableName), Seq(Row(40), Row(60)))
    val addFilesInFinalVersion = finalSnapshot.allFiles.collect()
    assert(addFilesInFinalVersion.length == addFilesAtCheckpointVersion.length)
    val setupFileAtFinalVersion = addFilesInFinalVersion
      .find(_.path == setupFileAtSetupVersion.path)
      .getOrElse(fail(s"The DV-updated file ${setupFileAtSetupVersion.path} must stay live."))
    assert(setupFileAtFinalVersion.deletionVector != null)
    assert(setupFileAtFinalVersion.deletionVector.cardinality == 4L)
    assert(setupFileAtFinalVersion.numPhysicalRecords.contains(5L))
    assert(setupFileAtFinalVersion.numLogicalRecords.contains(1L))
  }

  testAcrossAMTCheckpointScenarios(
      "snapshot exposes trailing AMTPassthrough add after an AMT checkpoint",
      "amt_tail_passthrough")(
      setup = name => sql(s"INSERT INTO $name VALUES (1)"),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (2)"))) { context =>
    val log = context.postCheckpointSnapshot.deltaLog
    val path = "trailing-passthrough-file"
    val add = createTestAddFile(encodedPath = path)
      .copy(amtPassthrough = Some(fullPassthrough), stats = """{"numRecords":1}""")

    log.startTransaction().commit(Seq(add), DeltaOperations.ManualUpdate)
    val finalSnapshot = latestSnapshotAfterLogTail(context)

    assert(amtFilesInTree(finalSnapshot, Some(path)).isEmpty)
    val live = finalSnapshot.allFiles.collect().filter(_.path == path)
    assert(live.length == 1)
    assert(live.head.amtPassthrough.contains(fullPassthrough))
  }

  testAcrossAMTCheckpointScenarios(
      "filtered scan is correct after emission (trimmed deltas + leaves)",
      "amt_filtered_scan")(
      setup = name => (1 to 3).foreach(i => sql(s"INSERT INTO $name VALUES ($i)")),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"DELETE FROM $name WHERE id = 1"))) { context =>
    // SQL data-skipping reconstruction is disabled for AMT tables, so reads go through the
    // allFiles-based reconstruction: the leaves supply state up to the checkpoint and the
    // trimmed deltas supply the rest. Each row must appear exactly once -- a double-count would
    // surface as duplicate rows or wrong counts.
    val table = spark.read.table(context.tableName)
    val expectedRows = Seq(Row(2), Row(3))
    checkAnswer(table.filter("id >= 2"), expectedRows)
    checkAnswer(
      table.groupBy().count(), Seq(Row(expectedRows.size.toLong)))
    checkAnswer(table, expectedRows)
  }

  testAcrossAMTCheckpointScenarios(
      "deletion vector round-trips through the leaves as a relative-path DV",
      "amt_dv",
      sqlConfs = Seq(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false"))(
      setup = name => {
        Seq(10, 20, 30, 40, 50).toDF("id").coalesce(1)
          .write.mode("append").insertInto(name)
        // Attach a persistent DV directly rather than relying on DELETE's rewrite heuristic.
        val log = deltaLogForName(name)
        val fileToDv = log.unsafeVolatileSnapshot.allFiles.collect()
        assert(fileToDv.length == 1, "The five rows must land in a single file.")
        val dvActions = writeFileWithDVOnDisk(log, fileToDv.head, RoaringBitmapArray(0L, 2L, 4L))
        log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
      }) { context =>
    val snapshot = context.postCheckpointSnapshot
    val provider = context.provider
    checkAnswer(spark.read.table(context.tableName), Seq(Row(20), Row(40)))

    // The one surviving live file must carry a deletion vector in committed state.
    val committed = snapshot.allFiles.collect()
    assert(committed.length == 1)
    val committedFile = committed.head
    val committedDv = committedFile.deletionVector
    assert(committedDv != null, "The committed file must carry a deletion vector.")
    // Five physical rows, three deleted by the DV -> two logical rows.
    assert(committedFile.numPhysicalRecords.contains(5L))
    assert(committedFile.numLogicalRecords.contains(2L))

    val dvRow = provider
      .loadActionsForStateReconstruction(spark, snapshot.deltaLog)
      .getOrElse(fail("AMT provider must contribute leaf-derived file actions."))
      .where("add is not null and add.deletionVector is not null")
      .selectExpr("add.deletionVector.storageType", "add.deletionVector.pathOrInlineDv",
        "add.deletionVector.offset")
      .collect()
    assert(dvRow.length == 1, "Exactly one leaf DATA entry must carry a DV.")
    val reconstructedDv = DeletionVectorDescriptor(
      storageType = dvRow.head.getString(0),
      pathOrInlineDv = dvRow.head.getString(1),
      offset = Option(dvRow.head.get(2)).map(_ => dvRow.head.getInt(2)),
      sizeInBytes = committedDv.sizeInBytes,
      cardinality = committedDv.cardinality)
    val tableRoot = snapshot.deltaLog.dataPath
    assert(reconstructedDv.storageType == DeletionVectorDescriptor.RELATIVE_DV_MARKER)
    assert(reconstructedDv.absolutePath(tableRoot) == committedDv.absolutePath(tableRoot))
    assert(reconstructedDv.offset == committedDv.offset)

    // The leaf-reconstructed AddFile must recover the same physical/logical record counts as the
    // committed file: `numRecords` in stats is the physical count, so it must round-trip without
    // being off by the DV cardinality.
    import org.apache.spark.sql.delta.implicits._
    val reconstructed = provider
      .loadActionsForStateReconstruction(spark, snapshot.deltaLog)
      .getOrElse(fail("AMT provider must contribute leaf-derived file actions."))
      .where("add is not null")
      .select("add.*")
      .as[AddFile]
      .collect()
    assert(reconstructed.length == 1)
    assert(reconstructed.head.numPhysicalRecords.contains(5L),
      "Reconstructed physical record count must match the committed file.")
    assert(reconstructed.head.numLogicalRecords.contains(2L),
      "Reconstructed logical record count must match the committed file.")
  }

  testAcrossAMTCheckpointScenarios(
      "DV that fully deletes a file",
      "amt_dv_full_delete",
      sqlConfs = Seq(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false"))(
      setup = name => {
        Seq(1, 2).toDF("id").coalesce(1).write.mode("append").insertInto(name)
        val log = deltaLogForName(name)
        val fileToDv = log.unsafeVolatileSnapshot.allFiles.collect()
        assert(fileToDv.length == 1)
        val dvActions = writeFileWithDVOnDisk(log, fileToDv.head, RoaringBitmapArray(0L, 1L))
        log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
      }) { context =>
    val snapshot = context.postCheckpointSnapshot
    checkAnswer(spark.read.table(context.tableName), Seq.empty)

    val committed = snapshot.allFiles.collect()
    assert(committed.length == 1, "The fully-deleted file must stay live.")
    assert(committed.head.numPhysicalRecords.contains(2L))
    assert(committed.head.numLogicalRecords.contains(0L))

    // The tree reconstructs the same file.
    val reconstructed = amtFilesInTree(snapshot)
    assert(reconstructed.length == 1, "The fully-deleted file must stay live.")
    val dv = reconstructed.head.deletionVector
    assert(dv != null)
    assert(dv.cardinality == 2L)
    assert(reconstructed.head.numPhysicalRecords.contains(2L))
    assert(reconstructed.head.numLogicalRecords.contains(0L))
  }

  testAcrossAMTCheckpointScenarios(
      "two files sharing one DV file on disk",
      "amt_dv_shared_blob",
      sqlConfs = Seq(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false"))(
      setup = name => {
        Seq(1, 2).toDF("id").coalesce(1).write.mode("append").insertInto(name)
        Seq(3, 4).toDF("id").coalesce(1).write.mode("append").insertInto(name)
        val log = deltaLogForName(name)
        val files = log.unsafeVolatileSnapshot.allFiles.collect()
        assert(files.length == 2)
        val dvActions = writeFilesWithDVsOnDisk(log, Seq(
          files(0) -> RoaringBitmapArray(0L),
          files(1) -> RoaringBitmapArray(0L)))
        log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
      }) { context =>
    val snapshot = context.postCheckpointSnapshot
    // Each file drops its row 0 and the row-1 values (2 and 4) survive.
    checkAnswer(spark.read.table(context.tableName), Seq(Row(2), Row(4)))

    // Both DV'd files stay live, each with one logical row left.
    val committed = snapshot.allFiles.collect()
    assert(committed.length == 2, "Both DV'd files must remain live.")
    assert(committed.forall(_.numLogicalRecords.contains(1L)))

    // The tree reconstructs the same files.
    val tableRoot = snapshot.deltaLog.dataPath
    val reconstructed = amtFilesInTree(snapshot)
    assert(reconstructed.length == 2)
    val dvs = reconstructed.map(_.deletionVector)
    assert(dvs.forall(dv => dv != null))
    // Both AddFiles reference the same DV file on disk.
    val blobPaths = dvs.map(_.absolutePath(tableRoot)).toSet
    assert(blobPaths.size == 1)
    // Offsets are still different.
    val offsets = dvs.map(_.offset).toSet
    assert(offsets.size == 2)
  }

  testAcrossAMTCheckpointScenarios(
      "snapshot replays trailing remove of a shared checkpointed DV file",
      "amt_tail_remove_checkpointed_dv",
      sqlConfs = Seq(
        DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false"
      ))(
      setup = name => {
        Seq(1, 2).toDF("id").coalesce(1).write.mode("append").insertInto(name)
        Seq(3, 4).toDF("id").coalesce(1).write.mode("append").insertInto(name)
        val log = deltaLogForName(name)
        val files = log.unsafeVolatileSnapshot.allFiles.collect()
        assert(files.length == 2)

        val dvActions = writeFilesWithDVsOnDisk(log, Seq(
          files(0) -> RoaringBitmapArray(0L),
          files(1) -> RoaringBitmapArray(0L)))
        log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
      },
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (5)"))) { context =>
    val log = context.postCheckpointSnapshot.deltaLog
    val addFilesAtSetupVersion = context.postSetupSnapshot.allFiles.collect()
      .filter(_.deletionVector != null)
    assert(addFilesAtSetupVersion.length == 2)
    val addFilesAtCheckpointVersion = context.postCheckpointSnapshot.allFiles.collect()
    assert(addFilesAtCheckpointVersion.count(_.deletionVector != null) == 2)
    assert(addFilesAtCheckpointVersion.filter(_.deletionVector != null).forall(
      _.deletionVector.storageType == DeletionVectorDescriptor.RELATIVE_DV_MARKER))
    val checkpointRowCount = spark.read.table(context.tableName).count()

    val firstFile = addFilesAtSetupVersion.head
    log.startTransaction().commit(
      Seq(firstFile.removeWithTimestamp()),
      DeltaOperations.ManualUpdate)
    val finalSnapshot = latestSnapshotAfterLogTail(context)

    val addFilesInAmt = amtFilesInTree(finalSnapshot)
    assert(addFilesInAmt.length == addFilesAtCheckpointVersion.length,
      "The AMT provider should still expose every checkpointed file.")
    val addFilesInFinalVersion = finalSnapshot.allFiles.collect()
    assert(addFilesInFinalVersion.length == addFilesAtCheckpointVersion.length - 1,
      "Only one file left after the last remove.")
    val liveFiles = addFilesInFinalVersion.filter(_.deletionVector != null)
    assert(liveFiles.length == 1)
    assert(
      liveFiles.head.deletionVector.absolutePath(log.dataPath) ==
        firstFile.deletionVector.absolutePath(log.dataPath),
      "The surviving file should share the same DV path as the removed file.")
    assert(liveFiles.head.deletionVector.offset != firstFile.deletionVector.offset)
    checkAnswer(
      spark.read.table(context.tableName).groupBy().count(),
      Seq(Row(checkpointRowCount - 1)))
  }

  testAcrossAMTCheckpointScenarios(
      "DVs on files across multiple leaf manifests",
      "amt_dv_multi_leaf",
      sqlConfs = Seq(
        DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "2",
        DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false"))(
      setup = name => {
        // Four two-row files
        Seq((10, 11), (20, 21), (30, 31), (40, 41)).foreach { case (a, b) =>
          Seq(a, b).toDF("id").coalesce(1).write.mode("append").insertInto(name)
        }
        val log = deltaLogForName(name)
        val files = log.unsafeVolatileSnapshot.allFiles.collect()
        assert(files.length == 4)
        val dvActions = writeFilesWithDVsOnDisk(log, files.map(_ -> RoaringBitmapArray(0L)).toSeq)
        log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
      }) { context =>
    val snapshot = context.postCheckpointSnapshot
    checkAnswer(spark.read.table(context.tableName), Seq(Row(11), Row(21), Row(31), Row(41)))

    val committed = snapshot.allFiles.collect()
    assert(committed.length == 4, "All four DV'd files remain live.")
    assert(committed.forall(_.numLogicalRecords.contains(1L)))

    // The tree reconstructs the same files.
    val reconstructed = amtFilesInTree(snapshot)
    assert(reconstructed.length == 4)
    val filesWithDvs = reconstructed.filter(_.deletionVector != null)
    assert(filesWithDvs.length == 4)
  }

  test("DV with shallow cloned table") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false") {
      withTable("amt_dv_clone_src", "amt_dv_clone_tgt") {
        val src = "amt_dv_clone_src"
        // The source needs no AMT of its own, the clone reads its files.
        createAMTTable(src, checkpointInterval = 100)
        Seq(1, 2).toDF("id").coalesce(1).write.mode("append").insertInto(src)
        val srcLog = deltaLogForName(src)
        val srcFile = srcLog.unsafeVolatileSnapshot.allFiles.collect()
        assert(srcFile.length == 1, "The two rows must land in a single file.")
        val dvActions = writeFileWithDVOnDisk(srcLog, srcFile.head, RoaringBitmapArray(0L))
        srcLog.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))

        val tgt = "amt_dv_clone_tgt"
        sql(s"CREATE TABLE $tgt SHALLOW CLONE $src")
        // Materialize the target's AMT.
        commitCheckpoint(deltaLogForName(tgt), incremental = false)

        val tgtLog = deltaLogForName(tgt)
        val snapshot = tgtLog.update()
        amtProvider(snapshot).getOrElse(
          fail("the clone target must have an AMTCheckpointProvider."))
        checkAnswer(spark.read.table(tgt), Seq(Row(2)))

        val reconstructed = amtFilesInTree(snapshot)
        assert(reconstructed.length == 1)
        val dv = reconstructed.head.deletionVector
        assert(dv != null)
        assert(dv.storageType == DeletionVectorDescriptor.PATH_DV_MARKER,
          "An outside-root DV must reconstruct as a p DV")
        // The DV blob physically lives outside the target's root.
        val tgtRoot = tgtLog.dataPath
        val dvPath = dv.absolutePath(tgtRoot).toString
        assert(!dvPath.startsWith(tgtRoot.toString))
      }
    }
  }

  testAcrossAMTCheckpointScenarios(
      "distributed reconstruction returns every action exactly once",
      "amt_dist_reconstruction",
      sqlConfs = leafPackingConfs)(
      setup = name => {
        appendRowsAsSeparateFiles(name, leafPackedFiles)
        appendRowsAsSeparateFiles(name, leafPackedFiles - 1, startId = leafPackedFiles)
      },
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${2 * leafPackedFiles - 1})"))) { context =>
    val snapshot = context.postCheckpointSnapshot
    val provider = context.provider
    val expectedRows = 0 until 2 * leafPackedFiles
    checkAnswer(spark.read.table(context.tableName), expectedRows.map(Row(_)))
    val committedPaths = snapshot.allFiles.select("path").as[String].collect().toSet
    assert(committedPaths.size == expectedRows.size)
    assertLeafCount(provider.leaves, numFiles = 2 * leafPackedFiles)
    val df = provider.loadActionsForStateReconstruction(spark, snapshot.deltaLog)
      .getOrElse(fail("AMT provider must contribute leaf-derived actions."))

    // The leaves must be read through a distributed parquet scan, not a driver-side collected
    // LocalRelation.
    val hasFileScan = df.queryExecution.optimizedPlan.collectFirst {
      case l: LogicalRelation => l
    }.isDefined
    assert(hasFileScan,
      "Reconstruction must read the leaves through a distributed file scan, " +
        "not collect them to the driver.")

    // Every leaf entry must surface exactly once across the leaves, plus the inline protocol and
    // metadata actions.
    assertReconstructsLiveFileSet(context)
    assert(df.where("protocol.minReaderVersion is not null").count() == 1,
      "Reconstruction must carry the inline protocol action.")
    assert(df.where("metaData.id is not null").count() == 1,
      "Reconstruction must carry the inline metadata action.")
  }

  // The large multi-leaf tree is covered above; this is the small deterministic case, where the
  // inline incremental writer spills a known number of files into one leaf each.
  testAcrossAMTCheckpointScenarios(
      "inline incremental reconstruction reads across multiple leaves via a file scan",
      "amt_dist_multileaf",
      deferredScenarios = Seq.empty,
      sqlConfs = Seq(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "2"))(
      setup = name => (1 to 4).foreach(i => sql(s"INSERT INTO $name VALUES ($i)")),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (5)"))) { context =>
    assert(context.provider.leaves.size == 3,
      s"Five files packed two per leaf must produce three leaves: ${context.provider.leaves}")
    val reconstruction = context.provider
      .loadActionsForStateReconstruction(spark, context.postCheckpointSnapshot.deltaLog)
      .getOrElse(fail("AMT provider must contribute leaf-derived actions."))
    assert(reconstruction.queryExecution.optimizedPlan.collectFirst {
      case relation: LogicalRelation => relation
    }.isDefined, "Reconstruction must read leaves through a distributed file scan.")
  }

  test("reconstruction surfaces DATA entries that live directly in the root") {
    withTable("amt_root_data") {
      val name = "amt_root_data"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)") // v1.
      sql(s"INSERT INTO $name VALUES (2)") // v2: emit -> a real root + leaf.

      val deltaLog = deltaLogForName(name)
      val provider = amtProvider(deltaLog.unsafeVolatileSnapshot)
        .getOrElse(fail("expected AMTCheckpointProvider"))
      val tableRoot = deltaLog.dataPath

      // TODO(v4amt): once the write path can emit DATA entries directly into the root, drop this
      // synthetic-root scaffolding (checkpointWithSyntheticRoot / addedTracking) and drive the test
      // from a real writer-produced root instead.
      val rootAdds = Seq("root-data-1.parquet", "root-data-2.parquet").map { path =>
        AddFile(path = path, partitionValues = Map.empty, size = 128L, modificationTime = 0L,
          dataChange = false, stats = s"""{"numRecords":3}""")
      }
      val rootDataRows = rootAdds.map(add =>
        AMTSingleAction.fromAddFile(add, addedTracking, tableRoot))
      val checkpoint =
        checkpointWithSyntheticRoot(deltaLog, provider.checkpointAction, rootDataRows)

      val rootProvider = AMTCheckpointProvider.fromCheckpoint(
        deltaLog, checkpoint, manifestCommitVersion = provider.manifestCommitVersion)
      assert(rootProvider.leaves.isEmpty, "A DATA-only root must yield no leaf pointers.")

      val expected = rootAdds.map(_.path).toSet
      val df = rootProvider.loadActionsForStateReconstruction(spark, deltaLog)
        .getOrElse(fail("AMT provider must contribute root-derived actions."))
      val addPaths = df.where("add is not null").select("add.path").as[String].collect().toSet
      assert(addPaths == expected,
        "Reconstruction must surface the DATA entries stored directly in the root.")
      assert(df.where("protocol.minReaderVersion is not null").count() == 1,
        "Reconstruction must still carry the inline protocol action.")
      assert(df.where("metaData.id is not null").count() == 1,
        "Reconstruction must still carry the inline metadata action.")
    }
  }

  testAcrossAMTCheckpointScenarios(
      "manifest deletion vector drops superseded leaf entries during reconstruction",
      "amt_mdv_drop",
      sqlConfs = leafPackingConfs)(
      setup = name => appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles - 1),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) { context =>
    val snapshot = context.postCheckpointSnapshot
    val base = context.provider
    val baseCount = snapshot.allFiles.count()
    assert(baseCount == leafPackedFiles, "The seeded files plus the trigger's.")

    val leaf = base.leaves.head
    val posToLoc = leafPosToLoc(leaf, base.tableRoot)
    val deletedPos = posToLoc.keys.min
    val deletedLoc = posToLoc(deletedPos)

    // Sanity: without an MDV the distributed reconstruction surfaces every live file.
    val full = reconstructedPaths(base, snapshot.deltaLog)
    assert(full.size == baseCount)
    assert(full.contains(deletedLoc))

    // Patch the first leaf's pointer with an MDV marking `deletedPos` deleted, then reconstruct.
    val patchedLeaves =
      leaf.copy(manifest_info =
        leaf.manifest_info.copy(dv = Some(mdvBytesFor(deletedPos)), dv_cardinality = Some(1L))) +:
        base.leaves.tail
    val provider = new AMTCheckpointProvider(
      base.manifestCommitVersion, base.checkpointAction, patchedLeaves, base.tableRoot)

    val reconstructed = reconstructedPaths(provider, snapshot.deltaLog)
    assert(reconstructed.size == baseCount - 1,
      "The MDV-marked leaf entry must be dropped from the reconstructed live set.")
    assert(!reconstructed.contains(deletedLoc),
      "The MDV-marked file must not resurface as a live AddFile.")
    assert(reconstructed == full - deletedLoc,
      "Only the MDV-marked entry may be dropped; all other live files must survive.")
  }

  testAcrossAMTCheckpointScenarios(
      "manifest deletion vectors apply per leaf across a multi-leaf tree",
      "amt_mdv_multi",
      sqlConfs = leafPackingConfs)(
      setup = name => {
        appendRowsAsSeparateFiles(name, leafPackedFiles)
        appendRowsAsSeparateFiles(name, leafPackedFiles - 1, startId = leafPackedFiles)
      },
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${2 * leafPackedFiles - 1})"))) { context =>
    val base = context.provider
    assertLeafCount(base.leaves, numFiles = 2 * leafPackedFiles)
    val deltaLog = context.postCheckpointSnapshot.deltaLog
    val full = reconstructedPaths(base, deltaLog)
    assert(full.size == 2 * leafPackedFiles)

    // The leaf-local row-index -> entry-location map for each observed leaf.
    val locs = base.leaves.map(leafPosToLoc(_, base.tableRoot))
    def withMdv(idx: Int, positions: Seq[Long]): DataManifestEntry = {
      val leaf = base.leaves(idx)
      leaf.copy(manifest_info = leaf.manifest_info.copy(
        dv = Some(mdvBytesFor(positions: _*)), dv_cardinality = Some(positions.size.toLong)))
    }
    // Exercise every MDV shape in one tree (positions are leaf-local row indices):
    //   - one leaf: drop a single position (pos 0),
    //   - one leaf: drop all its positions (whole leaf),
    //   - one leaf: drop two positions (its 2nd and 4th, i.e. sorted indices 1 and 3),
    //   - one leaf: an empty MDV (no-op),
    //   - the remaining leaves: no MDV.
    // The clustered rewrite does not guarantee a fixed leaf order/size, so the shapes are bound
    // to leaves chosen by observed size (largest first); the two-position shape needs >= 4
    // entries.
    val bySizeDesc = locs.indices.sortBy(i => -locs(i).size)
    val twoLeaf = bySizeDesc(0)
    val wholeLeaf = bySizeDesc(1)
    val singleLeaf = bySizeDesc(2)
    val emptyLeaf = bySizeDesc(3)
    def sortedPos(idx: Int): Seq[Long] = locs(idx).keys.toSeq.sorted
    assert(sortedPos(twoLeaf).size >= 4,
      s"the largest leaf must hold >= 4 entries for the two-position MDV; " +
        s"got ${sortedPos(twoLeaf).size}.")
    val twoPositions = Seq(sortedPos(twoLeaf)(1), sortedPos(twoLeaf)(3))
    val patched = base.leaves.zipWithIndex.map { case (leaf, idx) =>
      idx match {
        case `twoLeaf` => withMdv(idx, twoPositions)
        case `wholeLeaf` => withMdv(idx, sortedPos(idx))
        case `singleLeaf` => withMdv(idx, Seq(sortedPos(idx).head))
        case `emptyLeaf` => withMdv(idx, Seq.empty)
        case _ => leaf
      }
    }
    val provider = new AMTCheckpointProvider(
      base.manifestCommitVersion, base.checkpointAction, patched, base.tableRoot)

    val dropped =
      twoPositions.map(locs(twoLeaf)).toSet ++
        locs(wholeLeaf).values.toSet +
        locs(singleLeaf)(sortedPos(singleLeaf).head)
    val reconstructed = reconstructedPaths(provider, deltaLog)
    assert(reconstructed == full -- dropped,
      "Each MDV must drop exactly its marked positions from its own leaf, and nothing else.")
    assert(reconstructed.size == full.size - dropped.size)
  }

  testAcrossAMTCheckpointScenarios(
      "surviving entries keep their DV and stats when an MDV drops a sibling",
      "amt_mdv_sibling",
      // We commit the DV actions directly as a `Delete` (rather than running a DELETE command), so
      // the SQL operation metrics a real DELETE would populate are absent. History metrics would
      // call `Delete.transformMetrics`, which requires `numDeletedRows`; it is missing here and
      // would fail with `key not found: numDeletedRows`. Disable history metrics to skip that.
      sqlConfs = Seq(
        DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> entriesPerLeaf.toString,
        DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false"))(
      setup = name => {
        // File A with two physical rows, then a persistent on-disk DV marking row 0 deleted.
        Seq(1, 2).toDF("id").coalesce(1).write.mode("append").insertInto(name)
        val log = deltaLogForName(name)
        val fileA = log.unsafeVolatileSnapshot.allFiles.collect()
        assert(fileA.length == 1, "The two rows must land in a single file.")
        val dvActions = writeFileWithDVOnDisk(log, fileA.head, RoaringBitmapArray(0L))
        log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
        // DV-less siblings, so the tree packs multi-entry leaves around file A. File A and the
        // trigger's file make up the rest of the packed count.
        appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles - 2, startId = 10)
      },
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (3)"))) { context =>
    val snapshot = context.postCheckpointSnapshot
    val base = context.provider

    // Classify the reconstructed entries by DV presence, staying entirely within the reconstruction
    // so path strings are consistent with the leaf `location` values. Exactly one entry carries a
    // DV (file A); the rest are the DV-less siblings the setup seeded.
    val fullAdds = base.loadActionsForStateReconstruction(spark, snapshot.deltaLog)
      .getOrElse(fail("AMT provider must contribute leaf-derived file actions."))
      .where("add is not null")
      .selectExpr("add.path", "add.deletionVector.storageType", "add.stats")
      .collect()
      .map(r => r.getString(0) -> (Option(r.getString(1)), r.getString(2)))
      .toMap
    assert(fullAdds.size == leafPackedFiles,
      "One DV-bearing file plus the DV-less siblings.")
    val withDv = fullAdds.filter(_._2._1.isDefined)
    assert(withDv.size == 1, s"Exactly one reconstructed entry must carry a DV; got $withDv")
    val aPath = withDv.head._1
    val aStats = fullAdds(aPath)._2

    // Drop one DV-less entry that shares file A's own leaf, so the MDV removes a true sibling from
    // a multi-entry leaf while A sits beside it.
    val aLeaf = base.leaves.find(leafPosToLoc(_, base.tableRoot).values.exists(_ == aPath))
      .getOrElse(fail(s"No leaf holds the DV-bearing file $aPath."))
    val aLeafEntries = leafPosToLoc(aLeaf, base.tableRoot)
    val (bPos, bPath) = aLeafEntries.filter { case (_, loc) => loc != aPath }.head
    val bLeafAndPos = (aLeaf, bPos)
    val patched = base.leaves.map { leaf =>
      if (leaf eq bLeafAndPos._1) {
        leaf.copy(manifest_info = leaf.manifest_info.copy(
          dv = Some(mdvBytesFor(bLeafAndPos._2)), dv_cardinality = Some(1L)))
      } else leaf
    }
    val provider = new AMTCheckpointProvider(
      base.manifestCommitVersion, base.checkpointAction, patched, base.tableRoot)

    val survivors = provider.loadActionsForStateReconstruction(spark, snapshot.deltaLog)
      .getOrElse(fail("AMT provider must contribute leaf-derived file actions."))
      .where("add is not null")
      .selectExpr("add.path", "add.deletionVector.storageType", "add.stats")
      .collect()
    // Only the MDV-marked sibling is dropped; every other entry -- including file A, which shares
    // its leaf -- survives with its deletion vector and stats intact.
    assert(survivors.length == fullAdds.size - 1,
      s"The MDV must drop exactly one entry; kept ${survivors.length} of ${fullAdds.size}.")
    assert(!survivors.exists(_.getString(0) == bPath),
      s"The MDV-marked sibling $bPath must not survive.")
    val a = survivors.find(_.getString(0) == aPath)
      .getOrElse(fail(s"The DV-bearing file $aPath must survive the MDV."))
    assert(a.getString(1) != null,
      "The surviving entry must retain its deletion vector after MDV filtering.")
    assert(a.getString(2) == aStats,
      "The surviving entry's stats must be unchanged by MDV filtering.")
  }

  testAcrossAMTCheckpointScenarios(
      "a manifest DV with only one of dv/dv_cardinality set is rejected",
      "amt_mdv_malformed",
      sqlConfs = leafPackingConfs)(
      setup = name => appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles - 1),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) { context =>
    val base = context.provider

    // `dv` set but `dv_cardinality` missing: the AMT spec requires both or neither.
    val patched =
      base.leaves.head.copy(manifest_info = base.leaves.head.manifest_info.copy(
        dv = Some(mdvBytesFor(0L)), dv_cardinality = None)) +:
        base.leaves.tail
    val provider = new AMTCheckpointProvider(
      base.manifestCommitVersion, base.checkpointAction, patched, base.tableRoot)

    val e = intercept[IllegalStateException] {
      provider.loadActionsForStateReconstruction(
        spark, context.postCheckpointSnapshot.deltaLog)
    }
    assert(e.getMessage.contains("dv and dv_cardinality must both be set or both unset"),
      s"Unexpected message: ${e.getMessage}")
  }

  /** Reconstructed live `add.path`s from the (possibly MDV-patched) provider. */
  private def reconstructedPaths(
      provider: AMTCheckpointProvider, deltaLog: DeltaLog): Set[String] =
    provider.loadActionsForStateReconstruction(spark, deltaLog)
      .getOrElse(fail("AMT provider must contribute leaf-derived file actions."))
      .where("add is not null").select("add.path").as[String].collect().toSet

  /** Maps a leaf's parquet row positions to their entry `location`s (bypasses the format check). */
  private def leafPosToLoc(leaf: DataManifestEntry, tableRoot: Path): Map[Long, String] =
    allowReadWithinDeltaLog {
      spark.read.parquet(leaf.getAbsolutePath(tableRoot).toString)
        .select(col("_metadata.row_index").as("pos"), col("location"))
        .as[(Long, String)].collect().toMap
    }

  /** Serializes a manifest DV bitmap over the given leaf entry positions. */
  private def mdvBytesFor(positions: Long*): Array[Byte] =
    AMTUtils.serializeMdv(RoaringBitmapArray(positions: _*))

  /** An ADDED tracking envelope with no lineage/sequence numbers, matching the AMT writer. */
  private def addedTracking: Tracking = Tracking(
    status = Tracking.Status.Added,
    snapshot_id = None,
    dv_snapshot_id = None,
    sequence_number = None,
    file_sequence_number = None,
    first_row_id = None,
    deleted_positions = None,
    replaced_positions = None)

  /**
   * Writes `rows` to a fresh AMT root manifest parquet under the table's metadata dir and returns
   * a [[Checkpoint]] (copied from `base`) pointing at it.
   */
  private def checkpointWithSyntheticRoot(
      deltaLog: DeltaLog, base: Checkpoint, rows: Seq[AMTSingleAction]): Checkpoint = {
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    val metadataDir = FileNames.amtMetadataDirPath(deltaLog.dataPath)
    val rootFile = FileNames.newAMTRootManifestFile(metadataDir)
    val useRename = deltaLog.store.isPartialWriteVisible(deltaLog.logPath, hadoopConf)
    val enc = org.apache.spark.sql.delta.implicits.amtSingleActionEncoder
    val metadata = base.metaData
    val protocol = base.protocol
    val withPartition = AMTPartitionValues.forWrite(
      spark.createDataset(rows)(enc).toDF(), metadata.partitionSchema)
    val df = AMTContentStats.forWrite(withPartition, metadata, protocol)
    Checkpoints.writeAtomicCheckpointParquetFile(
      spark,
      df,
      rootFile,
      hadoopConf,
      useRename,
      outputSchema = Some(AMTSingleAction.persistedSchema(metadata, protocol)),
      writeAsIcebergManifest = true)
    val size = rootFile.getFileSystem(hadoopConf).getFileStatus(rootFile).getLen
    base.copy(contentRoot = ContentRoot(
      path = rootFile.toString, sizeInBytes = size, version = base.version))
  }
}
