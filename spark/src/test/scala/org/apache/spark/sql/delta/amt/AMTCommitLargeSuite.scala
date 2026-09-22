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

import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}

import org.apache.spark.sql.Row

class AMTCommitLargeSuite extends AMTCheckpointTestBase {

  /**
   * One `commitLarge` operation under test.
   *
   * @param operationName operation name, used in test names and assertion messages.
   * @param targetTable   the AMT table the operation commits to.
   * @param tableNames    every table the scenario creates, for `withTable` cleanup.
   * @param commitVersion the version the operation itself commits at; the follow-up AMT lands at
   *                      `commitVersion + 1`.
   * @param expectedRows  rows the target holds once the operation completes.
   * @param setup         creates the tables and commits everything up to, but not including, the
   *                      operation.
   * @param commitLargeOperation runs the operation, which commits at `commitVersion`.
   */
  private case class CommitLargeOp(
      operationName: String,
      targetTable: String,
      tableNames: Seq[String],
      commitVersion: Long,
      expectedRows: Seq[Row],
      setup: () => Unit,
      commitLargeOperation: () => Unit)

  /**
   * RESTORE on an AMT table: three INSERTs (v1-v3), then RESTORE commits at v4. `onBoundary` picks
   * the checkpoint interval that lands the RESTORE ON an interval boundary (the version it commits
   * at) or well OFF every boundary; emission is unconditional either way.
   */
  private def restoreOp(onBoundary: Boolean): CommitLargeOp = {
    val table = "amt_cl_restore"
    val checkpointInterval = if (onBoundary) 4 else 100
    CommitLargeOp(
      operationName = "RESTORE",
      targetTable = table,
      tableNames = Seq(table),
      commitVersion = 4,
      expectedRows = Seq(Row(1)), // RESTORE TO VERSION 1 keeps only the first INSERT's file.
      setup = () => {
        createAMTTable(table, checkpointInterval = checkpointInterval)
        (1 to 3).foreach(i => sql(s"INSERT INTO $table VALUES ($i)")) // v1, v2, v3.
      },
      commitLargeOperation = () => sql(s"RESTORE TABLE $table TO VERSION AS OF 1")) // v4.
  }


  /**
   * Asserts `op` emitted exactly one FULL AMT via a follow-up OPTIMIZE CHECKPOINT commit at
   * `commitVersion + 1`, and that the resulting snapshot reads back through the manifest tree.
   */
  private def checkFullAMTEmitted(op: CommitLargeOp): Unit = {
    // A freshly-resolved DeltaLog cold-reads the emitted AMT: reconciliation installs the manifest
    // checkpoint provider from the CRC, so no warm handle from the emitting commit is needed.
    val deltaLog = deltaLogForName(op.targetTable)
    val commitVersion = op.commitVersion
    val followUpVersion = commitVersion + 1
    val snapshot = deltaLog.update()
    assert(snapshot.version == followUpVersion,
      s"A follow-up OPTIMIZE CHECKPOINT commit must land at v$followUpVersion.")
    assert(checkpointAt(deltaLog, commitVersion).isEmpty,
      s"The ${op.operationName} commit itself must carry no Checkpoint action.")
    val checkpoint = checkpointAt(deltaLog, followUpVersion)
      .getOrElse(fail(s"Expected a Checkpoint at v$followUpVersion."))
    assert(checkpoint.version == commitVersion,
      s"The Checkpoint must describe state as of v$commitVersion; got ${checkpoint.version}.")
    // A commitLarge AMT is always a full rewrite, and a full rewrite records its own described
    // version as the last-full-rewrite marker.
    assert(checkpoint.contentRoot.isIncremental.contains(false),
      "A commitLarge AMT must be a full rewrite.")
    assert(checkpoint.contentRoot.lastManifestCommitWithFullRewrite.contains(commitVersion),
      "A full rewrite records its own described version as the last-full-rewrite marker.")
    assert(!actionsAt(deltaLog, followUpVersion).exists(_.isInstanceOf[AddFile]),
      "The follow-up commit carries no user AddFile.")

    // The manifest tree is on disk and the data reads back correctly (all cold-read-safe). A small
    // tree may be a single leaf promoted to the root (no separate root- file), so assert the tree
    // exists via the root/leaf union rather than requiring a distinct root manifest.
    assert((rootFiles(tablePath(op.targetTable)) ++ leafFiles(tablePath(op.targetTable))).nonEmpty,
      "A manifest tree (root and/or promoted leaf) must be written.")
    checkAnswer(spark.read.table(op.targetTable), op.expectedRows)

    // The post-operation snapshot is AMT-backed and its tree reconstructs the live file set. Use
    // currentLiveDataEntries (whole tree) since DATA entries can live directly in the root when the
    // tree is small enough to skip separate leaves. Reconstruct the pre-checkpoint live file set by
    // replaying the commit log up to commitVersion (deltas only, independent of the AMT tree) so
    // this is a real cross-check rather than a tautological tree-vs-tree count.
    val liveFilePaths = scala.collection.mutable.Set.empty[String]
    deltaLog.getChanges(0).takeWhile(_._1 <= commitVersion).foreach { case (_, actions) =>
      actions.foreach {
        case a: AddFile => liveFilePaths += a.path
        case r: RemoveFile => liveFilePaths -= r.path
        case _ =>
      }
    }
    val preCheckpointFileCount = liveFilePaths.size.toLong
    assert(amtProvider(snapshot).isDefined, "The post-operation snapshot must be AMT-backed.")
    assert(currentLiveDataEntries(snapshot) == preCheckpointFileCount,
      "AMT live DATA entries must equal the pre-checkpoint live file count.")
  }

  for {
    // Each operation is run once ON its checkpoint interval boundary and once well OFF every
    // boundary; each op derives its own interval from `onBoundary` (see its factory). Emission is
    // unconditional either way.
    buildOp <- Seq[Boolean => CommitLargeOp](
      restoreOp
    )
    (boundaryLabel, onBoundary) <- Seq(
      "on the checkpoint interval boundary" -> true,
      "off the checkpoint interval boundary" -> false)
  } {
    val op = buildOp(onBoundary)
    test(s"${op.operationName} emits a full AMT $boundaryLabel") {
      withTable(op.tableNames: _*) {
        op.setup()
        // The table carries no AMT before the commitLarge operation.
        assert(amtProvider(deltaLogForName(op.targetTable).update()).isEmpty,
          s"No AMT should exist before the ${op.operationName}.")

        op.commitLargeOperation()

        checkFullAMTEmitted(op)
      }
    }
  }


  test("a RESTORE emits a full rewrite even when a prior AMT already exists") {
    val op = restoreOp(onBoundary = true)
    withTable(op.tableNames: _*) {
      op.setup()
      op.commitLargeOperation() // v4 -> follow-up AMT at v5.
      checkFullAMTEmitted(op)

      // A second RESTORE has a prior tree it *could* build on incrementally, but commitLarge always
      // emits a full rewrite, so this one is full too (with an advanced marker).
      sql(s"RESTORE TABLE ${op.targetTable} TO VERSION AS OF 2") // v6 -> follow-up AMT at v7.
      checkFullAMTEmitted(op.copy(commitVersion = 6, expectedRows = Seq(Row(1), Row(2))))
    }
  }
}
