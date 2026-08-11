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
      provider.leafManifestAbsolutePaths.flatMap { leafPath =>
        val relManifest = relativeManifest(snapshot, leafPath)
        spark.read.parquet(leafPath.toString)
          .where(col("content_type") === AMTSingleAction.ContentType.Type.Data)
          .select(col("location"), col("_metadata.row_index").as("pos"))
          .collect()
          .map(row => (relManifest, row.getLong(1)) -> row.getString(0))
      }.toMap
  }

  /**
   * All live [[AddFile]]s of snapshot.
   */
  private def liveAddFiles(snapshot: Snapshot): Seq[AddFile] =
    snapshot.allFiles.collect().toSeq

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
      appendRowsAsSeparateFiles(name, numRows = leafPackedFiles)
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
   * leaf-derived file's path carries exactly that file's back reference.
   */
  private def assertBackRefsPropagated(
      deltaLog: DeltaLog,
      afterVersion: Long,
      backRefByPath: Map[String, Option[BackReference]]): Int = {
    var matched = 0
    actionsAfter(deltaLog, afterVersion).foreach {
      case r: RemoveFile if backRefByPath.contains(r.path) =>
        val expected = backRefByPath(r.path)
        assert(r.backReference == expected,
          s"RemoveFile ${r.path} back-ref ${r.backReference} must equal source $expected.")
        matched += 1
      case a: AddFile if backRefByPath.contains(a.path) =>
        val expected = backRefByPath(a.path)
        assert(a.backReference == expected,
          s"Superseding AddFile ${a.path} back-ref ${a.backReference} must equal source $expected.")
        matched += 1
      case _ => // A freshly written AddFile (new path) or a non-file action: nothing to inherit.
    }
    matched
  }

  testAcrossAMTCheckpointScenarios(
      "reconstructed AddFiles are stamped with a back reference matching the leaf layout",
      "amt_back_ref_stamped",
      sqlConfs = leafPackingConfs)(
      setup = name => appendRowsAsSeparateFiles(name, numRows = leafPackedFiles - 1),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) { context =>
    val groundTruth = leafLocationByBackRef(context.postCheckpointSnapshot)
    val adds = liveAddFiles(context.postCheckpointSnapshot)
    assert(adds.size == leafPackedFiles,
      s"All $leafPackedFiles inserted files must be reconstructed from the leaves, " +
        s"got ${adds.size}.")

    val leafPaths = context.provider.leafManifestAbsolutePaths
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
      setup = name => appendRowsAsSeparateFiles(name, numRows = leafPackedFiles - 1),
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
      setup = name => appendRowsAsSeparateFiles(name, numRows = leafPackedFiles - 1),
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
      "removeRows propagates the back reference to the superseding AddFile and the RemoveFile",
      "amt_back_ref_remove_rows",
      sqlConfs = leafPackingConfs)(
      setup = name => {
        // A single file holding two rows.
        Seq(1, 2).toDF("id").coalesce(1).write.mode("append").insertInto(name)
        appendRowsAsSeparateFiles(name, numRows = leafPackedFiles - 2, startId = 100)
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

    assert(supersedingAdd.backReference == twoRowFile.backReference,
      "The superseding AddFile (new DV) must inherit the source file's back reference.")
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
        appendRowsAsSeparateFiles(name, numRows = leafPackedFiles - 2, startId = 100)
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
    assert(supersedingAdd.backReference == backRefByPath(twoRowPath),
      "the superseding AddFile must inherit the source file's back reference.")
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
        setup = name => appendRowsAsSeparateFiles(name, numRows = leafPackedFiles - 1),
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
        setup = name => appendRowsAsSeparateFiles(name, numRows = leafPackedFiles - 1),
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
      setup = name => appendRowsAsSeparateFiles(name, numRows = leafPackedFiles - 1),
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
    readded.foreach { a =>
      assert(a.backReference.isEmpty,
        s"re-added AddFile ${a.path} must carry no back reference (stale pointer), " +
          s"but was ${a.backReference}.")
    }
  }

  /**
   * ANALYZE COMPUTE STATISTICS back reference integration test.
   * First build a tree with 40 leaf files and 5 root files.
   * Then do 3 commits on top:
   *   - commit A: INSERT five more net-new files. They are not in the tree, so each must carry no
   *     back reference.
   *   - commit B: re-add three existing files through a manual transaction - one leaf file (must
   *     carry its stamped back reference), one root file, and one commit-A file (both must carry
   *     none).
   *   - commit C: remove three files - one leaf file (tombstone must carry its back reference),
   *     one root file, and one commit-A file (tombstones must carry none).
   *
   * Finally ANALYZE recomputes stats for every surviving file and recommits each as an
   * `AddFile.copy` that preserves `backReference`, in a single transaction, and asserts the whole
   * surviving set. 39 leaf files (stamped), 4 root files, and 4 commit-A files (unstamped)
   */
  testAcrossAMTCheckpointScenarios(
      "ANALYZE COMPUTE STATISTICS over a mixed root+leaf tree carries correct back reference",
      "amt_back_ref_analyze",
      sqlConfs = leafPackingConfs)(
      setup = name => appendRowsAsSeparateFiles(name, numRows = 4 * entriesPerLeaf - 1),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${4 * entriesPerLeaf - 1})"))) { context =>
    val name = context.tableName
    val deltaLog = context.postCheckpointSnapshot.deltaLog
    val numLeafFiles = 4 * entriesPerLeaf // Seeded ids [0, numLeafFiles).
    val numRootFiles = 5
    val numCommitAFiles = 5
    // The full checkpoint stamped every seeded file into a leaf.
    val leafBackRefByPath = stampedBackRefs(context.postCheckpointSnapshot)
    assertLeafCount(context.provider.leaves, numLeafFiles)

    // Five net-new files, then an incremental checkpoint so they land directly in the new root.
    appendRowsAsSeparateFiles(name, numRows = numRootFiles, startId = numLeafFiles)
    commitCheckpoint(deltaLog, incremental = true)
    val rootTree = amtProvider(deltaLog.update()).getOrElse(fail("expected AMTCheckpointProvider"))
    assert(rootTree.checkpointAction.contentRoot.isIncremental.contains(true),
      "the follow-up checkpoint must be an incremental rewrite.")
    assertLeafCount(rootTree.leaves, numLeafFiles)
    val rootFilePaths = AMTCheckpointProvider
      .readLiveRootDataEntries(spark, deltaLog, rootTree.checkpointAction)
      .map(_.path).toSet
    assert(rootFilePaths.size == numRootFiles,
      s"expected $numRootFiles root-resident files, got ${rootFilePaths.size}.")
    assert(rootFilePaths.intersect(leafBackRefByPath.keySet).isEmpty,
      "root-resident and leaf-resident files must be disjoint.")

    val treeSnapshot = deltaLog.update()
    val leafAdds = liveAddFiles(treeSnapshot).filter(a => leafBackRefByPath.contains(a.path))
    val rootAdds = liveAddFiles(treeSnapshot).filter(a => rootFilePaths.contains(a.path))
    assert(leafAdds.size == numLeafFiles && leafAdds.forall(_.backReference.isDefined))
    assert(rootAdds.size == numRootFiles && rootAdds.forall(_.backReference.isEmpty))

    // commit A: five more net-new files(not in the tree).
    val vBeforeCommitA = deltaLog.update().version
    appendRowsAsSeparateFiles(
      name, numRows = numCommitAFiles, startId = numLeafFiles + numRootFiles)
    val commitAAdds = actionsAfter(deltaLog, vBeforeCommitA).collect { case a: AddFile => a }
    assert(commitAAdds.size == numCommitAFiles && commitAAdds.forall(_.backReference.isEmpty),
      "commit-A files never entered the tree, so each must carry no back reference.")

    // commit B: readd three existing files through a manual transaction
    val vBeforeCommitB = deltaLog.update().version
    val reAdds = Seq(leafAdds(0), rootAdds(0), commitAAdds(0)).map(_.copy(dataChange = false))
    commitActions(name, reAdds)
    val readded = actionsAfter(deltaLog, vBeforeCommitB).collect { case a: AddFile => a }
      .map(a => a.path -> a.backReference).toMap
    assert(readded(leafAdds(0).path) == leafAdds(0).backReference,
      "the re-added leaf file must keep its stamped back reference.")
    assert(readded(rootAdds(0).path).isEmpty && readded(commitAAdds(0).path).isEmpty,
      "the re-added root and commit-A files must carry no back reference.")

    // commit C: remove three files
    val vBeforeCommitC = deltaLog.update().version
    commitActions(name, Seq(
      leafAdds(1).removeWithTimestamp(),
      rootAdds(1).removeWithTimestamp(),
      commitAAdds(1).removeWithTimestamp()))
    val tombstones = actionsAfter(deltaLog, vBeforeCommitC).collect { case r: RemoveFile => r }
      .map(r => r.path -> r.backReference).toMap
    assert(tombstones(leafAdds(1).path) == leafAdds(1).backReference,
      "the leaf file's tombstone must carry its stamped back reference.")
    assert(tombstones(rootAdds(1).path).isEmpty && tombstones(commitAAdds(1).path).isEmpty,
      "the root and commit-A tombstones must carry no back reference.")

    // The surviving live set: 39 leaf (stamped), 4 root, 4 commit-A (unstamped).
    val liveByPath = liveAddFiles(deltaLog.update()).map(add => add.path -> add.backReference).toMap
    val survivingLeaf = liveByPath.filter { case (p, _) => leafBackRefByPath.contains(p) }
    val survivingRoot = liveByPath.filter { case (p, _) => rootFilePaths.contains(p) }
    val commitAPaths = commitAAdds.map(_.path).toSet
    val survivingCommitA = liveByPath.filter { case (p, _) => commitAPaths.contains(p) }
    assert(survivingLeaf.size == numLeafFiles - 1 && survivingLeaf.values.forall(_.isDefined),
      s"expected ${numLeafFiles - 1} stamped leaf survivors, got ${survivingLeaf.size}.")
    assert(survivingRoot.size == numRootFiles - 1 && survivingRoot.values.forall(_.isEmpty),
      s"expected ${numRootFiles - 1} unstamped root survivors, got ${survivingRoot.size}.")
    assert(survivingCommitA.size == numCommitAFiles - 1 &&
        survivingCommitA.values.forall(_.isEmpty),
      s"expected ${numCommitAFiles - 1} unstamped commit-A survivors, got " +
        s"${survivingCommitA.size}.")

    // ANALYZE: recompute stats for the whole live set in a single transaction.
    // The commit passes only if each recomputed AddFile carried exactly the back reference the tree
    // expects: stamped for leaf survivors, empty for root and commit-A survivors.
    val vBeforeAnalyze = deltaLog.update().version
    sql(s"ANALYZE TABLE $name COMPUTE DELTA STATISTICS")

    val recomputed = actionsAfter(deltaLog, vBeforeAnalyze).collect { case a: AddFile => a }
    assert(recomputed.nonEmpty, "ANALYZE must re-commit the live files with fresh stats.")
    recomputed.foreach { a =>
      val expected = liveByPath.getOrElse(a.path,
        fail(s"ANALYZE re-committed $a for a path that was not live before it ran."))
      assert(a.backReference == expected,
        s"recomputed AddFile ${a.path} back-ref ${a.backReference} must equal source $expected.")
    }
    val recomputedByPath = recomputed.map(a => a.path -> a.backReference).toMap
    survivingLeaf.foreach { case (path, expected) =>
      assert(recomputedByPath.get(path).contains(expected),
        s"surviving leaf file $path must be recomputed with its back reference $expected.")
    }
    (survivingRoot.keys ++ survivingCommitA.keys).foreach { path =>
      assert(recomputedByPath.get(path).contains(None),
        s"unstamped survivor $path must be recomputed with no back reference.")
    }
  }

}
