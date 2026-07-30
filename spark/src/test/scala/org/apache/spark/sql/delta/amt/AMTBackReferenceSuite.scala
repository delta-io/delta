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

import org.apache.spark.sql.delta.{DeletionVectorsTestUtils, DeltaLog, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, AddFile, BackReference, RemoveFile}
import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
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
   * Creates an AMT table and inserts two single-row files, the second insert landing on a
   * checkpoint boundary so both files are captured in the leaves and stamped with back references.
   * Returns the post-emit `path -> back reference` map (the source of truth every command's
   * tombstone / superseding AddFile must inherit).
   */
  private def emitTwoStampedFiles(name: String): Map[String, Option[BackReference]] = {
    createAMTTable(name, checkpointInterval = 2)
    sql(s"INSERT INTO $name VALUES (1)")
    sql(s"INSERT INTO $name VALUES (2)") // Lands on a checkpoint boundary -> emit.
    val snapshot = deltaLogForName(name).update()
    assert(amtProvider(snapshot).isDefined, "table must be AMT-backed after the emit.")
    val byPath = liveAddFiles(snapshot).map(add => add.path -> add.backReference).toMap
    assert(byPath.size == 2 && byPath.values.forall(_.isDefined),
      "both emitted files must be reconstructed from the leaves with a back reference.")
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

  test("reconstructed AddFiles are stamped with a back reference matching the leaf layout") {
    withTable("amt_back_ref_stamped") {
      val name = "amt_back_ref_stamped"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)") // v1.
      sql(s"INSERT INTO $name VALUES (2)") // v2: checkpoint boundary -> emit.

      val snapshot = deltaLogForName(name).update()
      assert(amtProvider(snapshot).isDefined)
      val groundTruth = leafLocationByBackRef(snapshot)
      val adds = liveAddFiles(snapshot)
      assert(adds.size == 2, "Both inserted files must be reconstructed from the leaves.")

      val leafPaths = amtProvider(snapshot).get.leafManifestAbsolutePaths
        .map(relativeManifest(snapshot, _)).toSet
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
  }

  test("DELETE emits a RemoveFile carrying the removed file's back reference") {
    withTable("amt_back_ref_delete") {
      val name = "amt_back_ref_delete"
      val backRefByPath = emitTwoStampedFiles(name)
      val deltaLog = deltaLogForName(name)

      val vBefore = deltaLog.update().version
      sql(s"DELETE FROM $name WHERE id = 1")

      val removes = actionsAfter(deltaLog, vBefore).collect { case r: RemoveFile => r }
      assert(removes.size == 1, "The single-row file for id=1 must be removed whole.")
      val remove = removes.head
      val expected = backRefByPath.getOrElse(remove.path,
        fail(s"Removed file ${remove.path} was not among the leaf-reconstructed files."))
      assert(expected.isDefined, "The removed file must have carried a back reference.")
      assert(remove.backReference == expected,
        s"RemoveFile back-ref ${remove.backReference} must equal the source AddFile's $expected.")
    }
  }

  test("a file added after the emit and removed before the next emit has no back reference") {
    withTable("amt_back_ref_post_emit") {
      val name = "amt_back_ref_post_emit"
      createAMTTable(name, checkpointInterval = 3)
      sql(s"INSERT INTO $name VALUES (1)") // v1.
      sql(s"INSERT INTO $name VALUES (2)") // v2.
      sql(s"INSERT INTO $name VALUES (3)") // v3: checkpoint boundary -> emit; leaves stamp 3 files.

      val deltaLog = deltaLogForName(name)
      val emitted = deltaLog.update()
      assert(amtProvider(emitted).isDefined)
      val stamped = liveAddFiles(emitted)
      val stampedPaths = stamped.map(_.path).toSet
      assert(stamped.size == 3 && stamped.forall(_.backReference.isDefined),
        "all three emitted files must be stamped from the leaves.")

      // A new single-row file committed as a plain delta AddFile. It is not on a checkpoint
      // boundary (interval 3), so this file never enters a leaf and carries no back reference.
      sql(s"INSERT INTO $name VALUES (4)")
      val newFile = deltaLog.update().allFiles.collect()
        .find(add => !stampedPaths.contains(add.path))
        .getOrElse(fail("expected a newly added, non-leaf file."))
      assert(newFile.backReference.isEmpty,
        "A file added after the emit (not yet in a leaf) must not carry a back reference.")

      // Deleting its only row removes the whole file via removeWithTimestamp. Because the source
      // AddFile was never stamped, the tombstone must have an empty back reference too.
      val vBefore = deltaLog.update().version
      sql(s"DELETE FROM $name WHERE id = 4")
      val remove = actionsAfter(deltaLog, vBefore).collect { case r: RemoveFile => r }
        .find(_.path == newFile.path)
        .getOrElse(fail(s"expected a RemoveFile for the post-emit file ${newFile.path}."))
      assert(remove.backReference.isEmpty,
        "Removing a file that never entered a leaf must produce an empty back reference.")
    }
  }

  test("removeRows propagates the back reference to the superseding AddFile and the RemoveFile") {
    withTable("amt_back_ref_remove_rows") {
      val name = "amt_back_ref_remove_rows"
      createAMTTable(name, checkpointInterval = 2)
      // Two rows in a single file so a DV can mark one row without rewriting.
      Seq(1, 2).toDF("id").coalesce(1).write.mode("append").insertInto(name) // v1.
      sql(s"INSERT INTO $name VALUES (3)") // v2: emit.

      val snapshot = deltaLogForName(name).update()
      val twoRowFile = liveAddFiles(snapshot)
        .find(_.numPhysicalRecords.contains(2L))
        .getOrElse(fail("expected the two-row file to be reconstructed from the leaves."))
      assert(twoRowFile.backReference.isDefined)

      // Mark row 0 deleted via a persistent DV, exercising the removeRows narrow waist.
      val dv = writeDV(deltaLogForName(name), RoaringBitmapArray(0L))
      val (supersedingAdd, removeFile) =
        twoRowFile.removeRows(
          deletionVector = dv, updateStats = false)

      assert(supersedingAdd.backReference == twoRowFile.backReference,
        "The superseding AddFile (new DV) must inherit the source file's back reference.")
      assert(removeFile.backReference == twoRowFile.backReference,
        "The paired RemoveFile must inherit the source file's back reference.")
    }
  }

  test("DELETE via a persistent DV (removeRows) propagates through a real command") {
    withTable("amt_back_ref_dv_delete") {
      val name = "amt_back_ref_dv_delete"
      createAMTTable(name, checkpointInterval = 2)
      // A two-row file so a subset delete uses a DV instead of rewriting the whole file.
      Seq(1, 2).toDF("id").coalesce(1).write.mode("append").insertInto(name) // v1.
      sql(s"INSERT INTO $name VALUES (3)") // v2: checkpoint boundary -> emit.

      val snapshot = deltaLogForName(name).update()
      val backRefByPath =
        liveAddFiles(snapshot).map(add => add.path -> add.backReference).toMap
      val twoRowPath = liveAddFiles(snapshot)
        .find(_.numPhysicalRecords.contains(2L))
        .getOrElse(fail("expected the two-row file to be reconstructed from the leaves.")).path

      // Deleting one of the two rows marks it with a DV -> removeRows emits a superseding AddFile
      // (same path, new DV) plus a paired RemoveFile; both must inherit the back reference.
      val deltaLog = deltaLogForName(name)
      val vBefore = deltaLog.update().version
      sql(s"DELETE FROM $name WHERE id = 1")

      val actions = actionsAfter(deltaLog, vBefore)
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
      n => sql(s"INSERT OVERWRITE $n VALUES (99)"), tombstones = 2, exact = true),
    PropagationCase("RESTORE", "amt_back_ref_restore",
      n => sql(s"RESTORE TABLE $n TO VERSION AS OF 1"), tombstones = 1, exact = false))

  propagationCases.foreach { c =>
    test(s"${c.label} tombstones carry the source files' back references") {
      withTable(c.table) {
        val backRefByPath = emitTwoStampedFiles(c.table)
        val deltaLog = deltaLogForName(c.table)

        val vBefore = deltaLog.update().version
        c.run(c.table)

        val matched = assertBackRefsPropagated(deltaLog, vBefore, backRefByPath)
        if (c.exact) {
          assert(matched == c.tombstones,
            s"${c.label} must tombstone exactly ${c.tombstones} leaf-derived files, saw $matched.")
        } else {
          assert(matched >= c.tombstones,
            s"${c.label} must tombstone >= ${c.tombstones} leaf-derived file(s), saw $matched.")
        }
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

  /**
   * Builds an AMT source with stamped files, runs `clone(src, tgt)`, then asserts every AddFile
   * committed to the target carries no back reference.
   */
  private def assertCloneDropsBackRefs(clone: (String, String) => Unit): Unit = {
    withTable("amt_clone_src", "amt_clone_tgt") {
      val src = "amt_clone_src"
      val tgt = "amt_clone_tgt"
      val srcBackRefByPath = emitTwoStampedFiles(src)
      assert(srcBackRefByPath.values.forall(_.isDefined),
        "precondition: every source file must carry a back reference.")

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

  Seq("SHALLOW").foreach { cloneType =>
    test(s"$cloneType CLONE drops the source table's back references") {
      assertCloneDropsBackRefs { (src, tgt) =>
        sql(s"CREATE TABLE $tgt $cloneType CLONE $src")
      }
    }

  }

  test("RESTORE tombstones keep the current back ref; re-added files carry none") {
    withTable("amt_back_ref_restore_tombstones") {
      val name = "amt_back_ref_restore_tombstones"
      // restoreTarget holds two stamped files A, B.
      val restoredToByPath = emitTwoStampedFiles(name)
      val deltaLog = deltaLogForName(name)
      val restoreTarget = deltaLog.update().version

      // The overwrite drops A, B and writes C; a second insert then adds D and lands on a
      // checkpoint boundary, so the current live files C, D get stamped with this table's own
      // back references. (Exact commit versions are not asserted: an AMT checkpoint commits as a
      // separate follow-up commit, so it shifts version numbers; the checks scan via actionsAfter.)
      sql(s"INSERT OVERWRITE $name VALUES (99)")
      sql(s"INSERT INTO $name VALUES (100)") // Lands on a checkpoint boundary -> C, D stamped.

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
  }

}
