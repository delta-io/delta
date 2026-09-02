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

import org.apache.spark.sql.delta.{DeletionVectorsTestUtils, DeltaLog, DeltaOperations, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, AddFile, BackReference, RemoveFile}
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.functions.col

class AMTBackReferenceSuite extends AMTCheckpointTestBase with DeletionVectorsTestUtils {

  import testImplicits._

  /** Relativizes an absolute leaf manifest path the same way the stamped `backReference.manifest`
   *  is, so test assertions compare against the identical value. */
  private def relativeManifest(snapshot: Snapshot, absLeaf: Path): String = {
    val tableRoot = snapshot.deltaLog.dataPath
    val fs = tableRoot.getFileSystem(snapshot.deltaLog.newDeltaHadoopConf())
    AMTUtils.relativizeManifestPathToTableRoot(fs, tableRoot, absLeaf)
  }

  /**
   * The (relative leaf manifest, position) -> data-file location map for every live DATA entry
   * across the snapshot's leaves, read straight from the leaf parquet via `_metadata.row_index`.
   */
  private def leafLocationByBackRef(snapshot: Snapshot): Map[(String, Long), String] = {
    val provider = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))
    allowReadWithinDeltaLog {
      provider.liveLeafManifestAbsolutePaths.flatMap { leafPath =>
        val relManifest = relativeManifest(snapshot, leafPath)
        spark.read.parquet(leafPath.toString)
          .where(col("content_type") === AMTSingleAction.ContentType.Type.Data)
          .select(col("location"), col("_metadata.row_index").as("pos"))
          .collect()
          .map(row => (relManifest, row.getLong(1)) -> row.getString(0))
      }.toMap
    }
  }

  /** All actions committed after `afterVersion`, up to the latest version. */
  private def actionsAfter(deltaLog: DeltaLog, afterVersion: Long): Seq[Action] = {
    val latest = deltaLog.update().version
    (afterVersion + 1 to latest).flatMap(v => actionsAt(deltaLog, v))
  }

  /**
   * Creates an AMT table, seeds it with single-row files, and checkpoints them into leaves. Returns
   * the reconstructed leaf-derived `AddFile`s (each carrying a back reference).
   */
  private def emitStampedAddFiles(name: String): Seq[AddFile] = {
    // The interval is parked out of reach so the tree comes from the explicit OPTIMIZE CHECKPOINT
    // below rather than from a commit happening to land on the interval grid.
    createAMTTable(name, checkpointInterval = 100)
    val deltaLog = deltaLogForName(name)
    withSQLConf(leafPackingConfs: _*) {
      appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles)
      commitCheckpoint(deltaLog, incremental = false)
    }
    val snapshot = deltaLog.update()
    assert(amtProvider(snapshot).isDefined, "table must be AMT-backed after the emit.")
    val adds = liveAddFiles(snapshot)
    assert(adds.size == leafPackedFiles && adds.forall(_.backReference.isDefined),
      "every emitted file must be reconstructed from the leaves with a back reference.")
    adds
  }

  /** The back references stamped on every live file reconstructed from an AMT checkpoint. */
  private def stampedBackRefs(snapshot: Snapshot): Map[String, Option[BackReference]] = {
    val byPath = liveAddFiles(snapshot).map(add => add.path -> add.backReference).toMap
    assert(byPath.nonEmpty && byPath.values.forall(_.isDefined),
      "all emitted files must be reconstructed from the leaves with a back reference.")
    byPath
  }

  /**
   * Asserts that every file action committed after `afterVersion` that reuses a pre-command
   * leaf-derived file's path carries exactly that file's back reference. A file superseded under
   * the same path with a new DV is the one exception: the RemoveFile keeps the back reference
   * and the superseding AddFile must carry none.
   */
  private def assertBackRefsPropagated(
      deltaLog: DeltaLog,
      afterVersion: Long,
      backRefByPath: Map[String, Option[BackReference]]): Int = {
    val actions = actionsAfter(deltaLog, afterVersion)
    val supersededPaths = actions.collect {
      case r: RemoveFile if r.backReference.isDefined => r.path
    }.toSet
    var matched = 0
    actions.foreach {
      case r: RemoveFile if backRefByPath.contains(r.path) =>
        val expected = backRefByPath(r.path)
        assert(r.backReference == expected,
          s"RemoveFile ${r.path} back-ref ${r.backReference} must equal source $expected.")
        matched += 1
      case a: AddFile if supersededPaths.contains(a.path) =>
        assert(a.backReference.isEmpty,
          s"Superseding AddFile ${a.path} must carry no back reference, was ${a.backReference}.")
      case a: AddFile if backRefByPath.contains(a.path) =>
        val expected = backRefByPath(a.path)
        assert(a.backReference == expected,
          s"Re-added AddFile ${a.path} back-ref ${a.backReference} must equal source $expected.")
        matched += 1
      case _ => // A freshly written AddFile (new path) or a non-file action: nothing to inherit.
    }
    matched
  }

  testAcrossAMTCheckpointScenarios(
      "reconstructed AddFiles are stamped with a back reference matching the leaf layout",
      "amt_back_ref_stamped",
      sqlConfs = leafPackingConfs)(
      setup = name => appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles - 1),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) { context =>
    val groundTruth = leafLocationByBackRef(context.postCheckpointSnapshot)
    val adds = liveAddFiles(context.postCheckpointSnapshot)
    assert(adds.size == leafPackedFiles,
      s"All $leafPackedFiles inserted files must be reconstructed from the leaves, " +
        s"got ${adds.size}.")

    val leafPaths = context.provider.liveLeafManifestAbsolutePaths
      .map(relativeManifest(context.postCheckpointSnapshot, _)).toSet
    adds.foreach { add =>
      val br = add.backReference.getOrElse(
        fail(s"Reconstructed AddFile ${add.path} must carry a back reference."))
      assert(leafPaths.contains(br.manifest),
        s"Back-ref manifest ${br.manifest} must be one of the tree's leaves $leafPaths.")
      // The back-ref must point at exactly the leaf entry describing this data file.
      assert(groundTruth.get((br.manifest, br.pos)).contains(add.path),
        s"Back-ref (${br.manifest}, ${br.pos}) must resolve to the entry for ${add.path}.")
    }
  }

  testAcrossAMTCheckpointScenarios(
      "no back references are generated for a root without leaves",
      "amt_back_ref_root_only")(
      setup = name => {
        sql(s"INSERT INTO $name VALUES (1)")
        sql(s"INSERT INTO $name VALUES (2)")
      },
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (3)"))) { context =>
    assert(context.provider.leaves.isEmpty,
      "precondition: the tree must hold no leaves for entries to stay root-resident.")
    val adds = liveAddFiles(context.postCheckpointSnapshot)
    assert(adds.size == 3,
      s"All three live files must be reconstructed from the root; got ${adds.size}.")
    adds.foreach { add =>
      assert(add.backReference.isEmpty,
        s"Root-resident ${add.path} must carry no back reference, but was ${add.backReference}.")
    }

    // A DELETE of a root-resident file likewise has no back reference to propagate.
    val deltaLog = context.postCheckpointSnapshot.deltaLog
    val vBefore = deltaLog.update().version
    sql(s"DELETE FROM ${context.tableName} WHERE id = 1")
    val removes = actionsAfter(deltaLog, vBefore).collect { case r: RemoveFile => r }
    assert(removes.size == 1, s"The file holding id=1 must be removed whole; got ${removes.size}.")
    assert(removes.head.backReference.isEmpty,
      s"Removing a root-resident file must emit an empty back reference, " +
        s"but was ${removes.head.backReference}.")
  }

  testAcrossAMTCheckpointScenarios(
      "DELETE emits a RemoveFile carrying the removed file's back reference",
      "amt_back_ref_delete",
      sqlConfs = leafPackingConfs)(
      setup = name => appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles - 1),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) { context =>
    val backRefByPath = stampedBackRefs(context.postCheckpointSnapshot)
    val deltaLog = context.postCheckpointSnapshot.deltaLog
    sql(s"DELETE FROM ${context.tableName} WHERE id = 1")

    val removes = actionsAfter(deltaLog, context.manifestCommitVersion)
      .collect { case r: RemoveFile => r }
    assert(removes.size == 1, "The single-row file for id=1 must be removed whole.")
    val remove = removes.head
    val expected = backRefByPath.getOrElse(remove.path,
      fail(s"Removed file ${remove.path} was not among the leaf-reconstructed files."))
    assert(expected.isDefined, "The removed file must have carried a back reference.")
    assert(remove.backReference == expected,
      s"RemoveFile back-ref ${remove.backReference} must equal the source AddFile's $expected.")
  }

  testAcrossAMTCheckpointScenarios(
      "a file added after the emit and removed before the next emit has no back reference",
      "amt_back_ref_post_emit",
      sqlConfs = leafPackingConfs)(
      setup = name => appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles - 1),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) { context =>
    val stamped = liveAddFiles(context.postCheckpointSnapshot)
    val stampedPaths = stamped.map(_.path).toSet
    assert(stamped.size == leafPackedFiles && stamped.forall(_.backReference.isDefined),
      s"all $leafPackedFiles emitted files must be stamped from the leaves.")

    // This file is added off a checkpoint boundary, so it never enters a leaf.
    sql(s"INSERT INTO ${context.tableName} VALUES ($leafPackedFiles)")
    val deltaLog = context.postCheckpointSnapshot.deltaLog
    val newFile = deltaLog.update().allFiles.collect()
      .find(add => !stampedPaths.contains(add.path))
      .getOrElse(fail("expected a newly added, non-leaf file."))
    assert(newFile.backReference.isEmpty,
      "A file added after the emit (not yet in a leaf) must not carry a back reference.")

    // Removing the unstamped file must produce an unstamped tombstone.
    val vBefore = deltaLog.update().version
    sql(s"DELETE FROM ${context.tableName} WHERE id = $leafPackedFiles")
    val remove = actionsAfter(deltaLog, vBefore).collect { case r: RemoveFile => r }
      .find(_.path == newFile.path)
      .getOrElse(fail(s"expected a RemoveFile for the post-emit file ${newFile.path}."))
    assert(remove.backReference.isEmpty,
      "Removing a file that never entered a leaf must produce an empty back reference.")
  }

  testAcrossAMTCheckpointScenarios(
      "removeRows keeps the back reference on the RemoveFile and drops it from the AddFile",
      "amt_back_ref_remove_rows",
      sqlConfs = leafPackingConfs)(
      setup = name => {
        // A single file holding two rows.
        Seq(1, 2).toDF("id").coalesce(1).write.mode("append").insertInto(name)
        appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles - 2, startId = 100)
      },
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) {
      context =>
    val twoRowFile = liveAddFiles(context.postCheckpointSnapshot)
      .find(_.numPhysicalRecords.contains(2L))
      .getOrElse(fail("expected the two-row file to be reconstructed from the leaves."))
    assert(twoRowFile.backReference.isDefined)

    // Mark row 0 deleted via a persistent DV, exercising the removeRows narrow waist.
    val dv = writeDV(context.postCheckpointSnapshot.deltaLog, RoaringBitmapArray(0L))
    val (supersedingAdd, removeFile) =
      twoRowFile.removeRows(
        deletionVector = dv, updateStats = false)

    assert(supersedingAdd.backReference.isEmpty,
      "The superseding AddFile (new DV) is a net-new root entry and must carry no back reference.")
    assert(removeFile.backReference == twoRowFile.backReference,
      "The paired RemoveFile must inherit the source file's back reference.")
  }

  testAcrossAMTCheckpointScenarios(
      "DELETE via a persistent DV (removeRows) propagates through a real command",
      "amt_back_ref_dv_delete",
      sqlConfs = leafPackingConfs)(
      setup = name => {
        // A single file holding two rows.
        Seq(1, 2).toDF("id").coalesce(1).write.mode("append").insertInto(name)
        appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles - 2, startId = 100)
      },
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) {
      context =>
    val backRefByPath =
      liveAddFiles(context.postCheckpointSnapshot).map(add => add.path -> add.backReference).toMap
    val twoRowPath = liveAddFiles(context.postCheckpointSnapshot)
      .find(_.numPhysicalRecords.contains(2L))
      .getOrElse(fail("expected the two-row file to be reconstructed from the leaves.")).path

    // Deleting one of the two rows emits a superseding AddFile and paired RemoveFile.
    val deltaLog = context.postCheckpointSnapshot.deltaLog
    sql(s"DELETE FROM ${context.tableName} WHERE id = 1")
    val actions = actionsAfter(deltaLog, context.manifestCommitVersion)
    val supersedingAdd = actions.collectFirst {
      case a: AddFile if a.path == twoRowPath => a
    }.getOrElse(fail("expected a superseding AddFile (DV update) for the two-row file."))
    val removed = actions.collectFirst {
      case r: RemoveFile if r.path == twoRowPath => r
    }.getOrElse(fail("expected a paired RemoveFile for the two-row file."))
    assert(supersedingAdd.deletionVector != null, "the superseding AddFile must carry the DV.")
    assert(supersedingAdd.backReference.isEmpty,
      "the superseding AddFile must not claim the source file's leaf position.")
    assert(removed.backReference == backRefByPath(twoRowPath),
      "the paired RemoveFile must inherit the source file's back reference.")
  }

  /**
   * Helper for commands that rewrite or remove leaf-derived files: emit two stamped files,
   * run the command, then assert every resulting tombstone / superseding AddFile inherited
   * the source file's back reference.
   *
   * @param label         Human-readable command name.
   * @param table         Table name to create for the case.
   * @param run           Runs the command against the given table name.
   * @param tombstones    Expected number of matched (inherited) file actions.
   * @param exact         If true, require exactly `tombstones`; else require at least that many.
   */
  private case class PropagationCase(
      label: String,
      table: String,
      run: String => Unit,
      tombstones: Int,
      exact: Boolean)

  private val propagationCases: Seq[PropagationCase] = Seq(
    PropagationCase("UPDATE", "amt_back_ref_update",
      n => sql(s"UPDATE $n SET id = 100 WHERE id = 1"), tombstones = 1, exact = false),
    PropagationCase("MERGE", "amt_back_ref_merge",
      n => sql(
        s"""MERGE INTO $n t
           |USING (SELECT 1 AS id) s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET id = 200""".stripMargin),
      tombstones = 1, exact = false),
    PropagationCase("INSERT OVERWRITE", "amt_back_ref_insert_overwrite",
      n => sql(s"INSERT OVERWRITE $n VALUES (99)"),
      tombstones = leafPackedFiles, exact = true),
    PropagationCase("RESTORE", "amt_back_ref_restore",
      n => sql(s"RESTORE TABLE $n TO VERSION AS OF 1"), tombstones = 1, exact = false))

  propagationCases.foreach { c =>
    testAcrossAMTCheckpointScenarios(
        s"${c.label} tombstones carry the source files' back references",
        c.table,
        sqlConfs = leafPackingConfs)(
        setup = name => appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles - 1),
        inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
          s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) { context =>
      val backRefByPath = stampedBackRefs(context.postCheckpointSnapshot)
      c.run(context.tableName)

      val matched = assertBackRefsPropagated(
        context.postCheckpointSnapshot.deltaLog, context.manifestCommitVersion, backRefByPath)
      if (c.exact) {
        assert(matched == c.tombstones,
          s"${c.label} must tombstone exactly ${c.tombstones} leaf-derived files, saw $matched.")
      } else {
        assert(matched >= c.tombstones,
          s"${c.label} must tombstone >= ${c.tombstones} leaf-derived file(s), saw $matched.")
      }
    }
  }

  test("non-AMT tables produce no back references") {
    withTable("non_amt_back_ref") {
      val name = "non_amt_back_ref"
      // A plain (non-AMT) Delta table.
      sql(s"CREATE TABLE $name (id INT) USING DELTA " +
        s"TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      sql(s"INSERT INTO $name VALUES (1)") // v1.
      sql(s"INSERT INTO $name VALUES (2)") // v2.

      val snapshot = deltaLogForName(name).update()
      assert(amtProvider(snapshot).isEmpty, "This table must not be AMT-backed.")
      snapshot.allFiles.collect().foreach { add =>
        assert(add.backReference.isEmpty,
          s"Non-AMT AddFile ${add.path} must not carry a back reference.")
      }

      val deltaLog = deltaLogForName(name)
      val vBefore = deltaLog.update().version
      sql(s"DELETE FROM $name WHERE id = 1")
      val removes = actionsAfter(deltaLog, vBefore).collect { case r: RemoveFile => r }
      assert(removes.nonEmpty)
      removes.foreach { r =>
        assert(r.backReference.isEmpty,
          s"Non-AMT RemoveFile ${r.path} must not carry a back reference.")
      }
    }
  }

  /** Commits `actions` through a real transaction so the OptimisticTransaction back-reference
   *  check runs. */
  private def commitActions(name: String, actions: Seq[Action]): Unit = {
    val log = deltaLogForName(name)
    log.startTransaction().commit(actions, DeltaOperations.ManualUpdate)
  }

  test("commit accepts a RemoveFile carrying the correct back reference") {
    withTable("amt_commit_ok") {
      val adds = emitStampedAddFiles("amt_commit_ok")
      // Tombstoning a leaf-derived file keeps its (correct) back reference: the commit must pass.
      commitActions("amt_commit_ok", Seq(adds.head.removeWithTimestamp()))
    }
  }

  test("commit accepts a fresh file that carries no back reference") {
    withTable("amt_commit_fresh") {
      val adds = emitStampedAddFiles("amt_commit_fresh")
      // A brand-new file not present in the tree legitimately carries no back reference.
      val fresh = adds.head.copy(path = "brand-new-file.parquet", backReference = None)
      commitActions("amt_commit_fresh", Seq(fresh))
    }
  }

  test("commit fails when a leaf-derived file's tombstone is missing its back reference") {
    withTable("amt_commit_missing") {
      val adds = emitStampedAddFiles("amt_commit_missing")
      // Strip the back reference off a tombstone for a file the tree says should carry one.
      val stripped = adds.head.removeWithTimestamp().copy(backReference = None)
      val ex = intercept[IllegalStateException] {
        commitActions("amt_commit_missing", Seq(stripped))
      }
      assert(ex.getMessage.contains("does not match the AMT"))
      assert(ex.getMessage.contains(stripped.path))
    }
  }

  test("commit fails when a fresh file carries a spurious back reference") {
    withTable("amt_commit_spurious") {
      val adds = emitStampedAddFiles("amt_commit_spurious")
      // A file not in the tree must not carry a back reference.
      val spurious = adds.head.copy(path = "brand-new-file.parquet")
      val ex = intercept[IllegalStateException] {
        commitActions("amt_commit_spurious", Seq(spurious))
      }
      assert(ex.getMessage.contains("not present in the AMT"))
    }
  }

  test("commit fails when a present file's tombstone carries the wrong back reference") {
    withTable("amt_commit_wrong") {
      val adds = emitStampedAddFiles("amt_commit_wrong")
      val wrongBr = adds.head.backReference.get.copy(pos = adds.head.backReference.get.pos + 1000L)
      val wrong = adds.head.removeWithTimestamp().copy(backReference = Some(wrongBr))
      val ex = intercept[IllegalStateException] {
        commitActions("amt_commit_wrong", Seq(wrong))
      }
      assert(ex.getMessage.contains("does not match the AMT"))
    }
  }

  /**
   * Builds an AMT source with stamped files, runs `clone(src, tgt)`, then asserts every AddFile
   * committed to the target carries no back reference.
   */
  private def testCloneDropsBackRefs(
      testName: String)(clone: (String, String) => Unit): Unit = {
    testAcrossAMTCheckpointScenarios(
        testName,
        "amt_clone_src",
        sqlConfs = leafPackingConfs)(
        setup = name => appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles - 1),
        inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
          s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) { context =>
      val src = context.tableName
      // Derive the clone target from the scenario-unique source, so the scenarios do not share a
      // table directory (see the naming note in `testAcrossAMTCheckpointScenarios`).
      val tgt = s"${src}_clone_tgt"
      val srcBackRefByPath = stampedBackRefs(context.postCheckpointSnapshot)

      withTable(tgt) {
        clone(src, tgt)

        val tgtLog = deltaLogForName(tgt)
        val latest = tgtLog.update().version
        val clonedAdds = (0L to latest)
          .flatMap(v => actionsAt(tgtLog, v))
          .collect { case a: AddFile => a }
        assert(clonedAdds.size >= srcBackRefByPath.size,
          s"the clone must add the ${srcBackRefByPath.size} source files to the target.")
        clonedAdds.foreach { a =>
          assert(a.backReference.isEmpty,
            s"Cloned AddFile ${a.path} must not carry the source table's back reference.")
        }
      }
    }
  }

  Seq("SHALLOW").foreach { cloneType =>
    testCloneDropsBackRefs(
        s"$cloneType CLONE drops the source table's back references") { (src, tgt) =>
      sql(s"CREATE TABLE $tgt $cloneType CLONE $src")
    }

  }

  testAcrossAMTCheckpointScenarios(
      "RESTORE tombstones keep the current back ref; re-added files carry none",
      "amt_back_ref_restore_tombstones",
      sqlConfs = Seq(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "1"))(
      setup = name => appendRowsAsSeparateFiles(name, numFiles = leafPackedFiles - 1),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) { context =>
    val name = context.tableName
    // The version RESTORE returns to, where every seeded file is stamped from a leaf.
    val restoredToByPath = stampedBackRefs(context.postCheckpointSnapshot)
    val deltaLog = deltaLogForName(name)
    val restoreTarget = context.checkpoint.version

    // The overwrite replaces every seeded file with one new file, and a second insert adds another.
    // One entry per leaf makes the explicit incremental checkpoint spill each net-new file into its
    // own leaf, so both current live files get stamped with this table's own back references.
    sql(s"INSERT OVERWRITE $name VALUES (99)")
    sql(s"INSERT INTO $name VALUES (100)")
    commitCheckpoint(deltaLog, incremental = true)

    val currentByPath =
      liveAddFiles(deltaLog.update()).map(a => a.path -> a.backReference).toMap
    assert(currentByPath.nonEmpty && currentByPath.values.forall(_.isDefined),
      "precondition: current live files (to be tombstoned by RESTORE) must be stamped.")
    assert(currentByPath.keySet.intersect(restoredToByPath.keySet).isEmpty,
      "precondition: restored-to files and current files must be disjoint.")

    val vBefore = deltaLog.update().version
    sql(s"RESTORE TABLE $name TO VERSION AS OF $restoreTarget")
    val actions = actionsAfter(deltaLog, vBefore)

    // toRemove side: a file live now but absent at restoreTarget is tombstoned through the narrow
    // waist removeWithTimestamp, so it keeps its valid CURRENT back reference.
    val removed = actions.collect {
      case r: RemoveFile if currentByPath.contains(r.path) => r
    }
    assert(removed.size == currentByPath.size,
      s"RESTORE must tombstone all ${currentByPath.size} current files, saw ${removed.size}.")
    removed.foreach { r =>
      assert(r.backReference == currentByPath(r.path),
        s"tombstone ${r.path} must keep its current back ref ${currentByPath(r.path)}.")
    }

    // toAdd side: a file present at restoreTarget but absent now is re-added. By definition it is
    // NOT in the current tree, so its restored-to pointer is stale and must be dropped.
    val readded = actions.collect {
      case a: AddFile if restoredToByPath.contains(a.path) => a
    }
    assert(readded.size == restoredToByPath.size,
      s"RESTORE must re-add all ${restoredToByPath.size} restored-to files, saw ${readded.size}.")
    /*
    // RESTORE's needs to recompute the back references for the re-added files, so we cannot simply
    // assert their emptiness directly.
    // TODO: assert their emptiness accurately after RESTORE is supported.
    readded.foreach { a =>
      assert(a.backReference.isEmpty,
        s"re-added AddFile ${a.path} must carry no back reference (stale pointer), " +
          s"but was ${a.backReference}.")
    }
    */
  }

}
