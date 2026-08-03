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

import org.apache.spark.sql.delta.{Checkpoints, DeletionVectorsTestUtils, DeltaLog, DeltaOperations}
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

  testInlineAndDeferred("snapshot.allFiles reflects a DELETE on the checkpoint boundary") {
    _ =>
    withTable("amt_delete_boundary") {
      val name = "amt_delete_boundary"
      // Interval 1 so every commit is a boundary; combined with inline mode this makes the last
      // DML AMT-backed in both modes regardless of the follow-up commit's version bookkeeping.
      createAMTTable(name, checkpointInterval = 1)
      sql(s"INSERT INTO $name VALUES (1)")
      sql(s"INSERT INTO $name VALUES (2)")
      sql(s"INSERT INTO $name VALUES (3)")
      sql(s"DELETE FROM $name WHERE id = 1") // removes id=1; triggers an AMT (inline or follow-up).

      val snapshot = deltaLogForName(name).unsafeVolatileSnapshot
      assert(amtProvider(snapshot).isDefined,
        "The post-DELETE snapshot must be AMT-backed.")
      // allFiles must reflect the DELETE: the removed file is gone from the post-commit live set.
      checkAnswer(spark.read.table(name), Seq(Row(2), Row(3)))
      // The manifest tree captures exactly the post-DELETE live files: a full rewrite drops the
      // removed entry outright, while an incremental rewrite masks it (MDV on a carried leaf, or a
      // root tombstone). Either way, the tree's live entry count -- across root and leaves -- must
      // match snapshot.allFiles.
      assert(currentLiveDataEntries(snapshot) == snapshot.allFiles.count(),
        "Live tree DATA entries must equal the post-commit live file count.")
    }
  }

  testInlineAndDeferred(
      "snapshot.allFiles matches leaves across insert/overwrite/delete before a checkpoint") { _ =>
    withTable("amt_overwrite") {
      val name = "amt_overwrite"
      // Interval 1 so the final DELETE triggers an AMT in both modes.
      createAMTTable(name, checkpointInterval = 1)
      sql(s"INSERT INTO $name VALUES (1)")
      sql(s"INSERT INTO $name VALUES (2)")
      sql(s"INSERT OVERWRITE $name VALUES (10), (20)") // replaces all prior files.
      sql(s"DELETE FROM $name WHERE id = 10")          // removes one; triggers an AMT.

      val snapshot = deltaLogForName(name).unsafeVolatileSnapshot
      assert(amtProvider(snapshot).isDefined)
      checkAnswer(spark.read.table(name), Seq(Row(20)))
      assert(snapshot.allFiles.count() == 1, "Only the surviving overwrite file should be live.")
      assert(currentLiveDataEntries(snapshot) == 1,
        "The tree must capture exactly the surviving live file after overwrite + delete.")
    }
  }

  testInlineAndDeferred(
      "filtered scan is correct after emission (trimmed deltas + leaves)") { _ =>
    withTable("amt_filtered_scan") {
      val name = "amt_filtered_scan"
      createAMTTable(name, checkpointInterval = 1)
      sql(s"INSERT INTO $name VALUES (1)")
      sql(s"INSERT INTO $name VALUES (2)")
      sql(s"INSERT INTO $name VALUES (3)")
      sql(s"DELETE FROM $name WHERE id = 1") // removes id=1; triggers an AMT.

      // SQL data-skipping reconstruction is disabled for AMT tables, so reads go through the
      // allFiles-based reconstruction: the leaves supply state up to the checkpoint and the
      // trimmed deltas supply the rest. Each row must appear exactly once -- a double-count would
      // surface as duplicate rows or wrong counts.
      checkAnswer(spark.read.table(name).filter("id >= 2"), Seq(Row(2), Row(3)))
      checkAnswer(
        spark.read.table(name).groupBy().count(), Seq(Row(2L)))
      checkAnswer(spark.read.table(name), Seq(Row(2), Row(3)))
    }
  }

  test("deletion vector round-trips through the leaves with a matching uniqueId") {
    withTable("amt_dv") {
      val name = "amt_dv"
      // Interval 1 so the DV commit triggers an AMT (via its follow-up OPTIMIZE CHECKPOINT commit).
      createAMTTable(name, checkpointInterval = 1)
      Seq(1, 2).toDF("id").coalesce(1)
        .write.mode("append").insertInto(name) // one file, two rows.

      // Attach a persistent DV directly rather than relying on DELETE's DV-vs-rewrite heuristic:
      // write a DV marking row 0 deleted and commit the resulting AddFile (with DV) + RemoveFile.
      val log = deltaLogForName(name)
      val fileToDv = log.unsafeVolatileSnapshot.allFiles.collect()
      assert(fileToDv.length == 1, "The two rows must land in a single file.")
      val dvActions = writeFileWithDVOnDisk(log, fileToDv.head, RoaringBitmapArray(0L))
      // Disable history metrics: this is a manual commit, not a real DELETE command, so the
      // operation-metrics (e.g. numDeletedRows) the Delete op expects are not populated.
      withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false") {
        log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
      }

      val snapshot = deltaLogForName(name).unsafeVolatileSnapshot
      val provider = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))
      checkAnswer(spark.read.table(name), Seq(Row(2)))

      // The one surviving live file must carry a deletion vector in committed state.
      val committed = snapshot.allFiles.collect()
      assert(committed.length == 1)
      val committedFile = committed.head
      val committedDv = committedFile.deletionVector
      assert(committedDv != null, "The committed file must carry a deletion vector.")
      // Two physical rows, one deleted by the DV -> one logical row.
      assert(committedFile.numPhysicalRecords.contains(2L))
      assert(committedFile.numLogicalRecords.contains(1L))

      // The leaf-reconstructed AddFile must recover a DV with the SAME uniqueId, so the
      // (path, deletionVectorUniqueId) dedup key matches the committed file exactly (no
      // double-count in the reader path). uniqueId is
      // storageType + pathOrInlineDv (+ "@offset"), so compare it from the reconstructed fields.
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
      assert(reconstructedDv.uniqueId == committedDv.uniqueId,
        s"Reconstructed DV uniqueId ${reconstructedDv.uniqueId} must equal committed " +
          s"${committedDv.uniqueId}.")

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
      assert(reconstructed.head.numPhysicalRecords.contains(2L),
        "Reconstructed physical record count must match the committed file.")
      assert(reconstructed.head.numLogicalRecords.contains(1L),
        "Reconstructed logical record count must match the committed file.")
    }
  }

  test("distributed reconstruction reads across multiple leaves via a file scan") {
    withTable("amt_dist_multileaf") {
      val name = "amt_dist_multileaf"
      val provider = withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
        createAMTTable(name, checkpointInterval = 2)
        appendRowsAsSeparateFiles(name, 30) // v1: ids 0..29.
        appendRowsAsSeparateFiles(name, 30, startId = 30) // v2: ids 30..59; emits full tree at v3.
        amtProvider(deltaLogForName(name).update())
          .getOrElse(fail("The interval-boundary append must trigger an AMT-backed snapshot."))
      }
      assert(provider.leaves.size == 6,
        s"entriesPerLeaf=10 with 60 files must pack into 6 leaves; got ${provider.leaves.size}.")
      val snapshot = deltaLogForName(name).update()
      checkAnswer(spark.read.table(name), (0 until 60).map(Row(_)))
      val committedPaths = snapshot.allFiles.select("path").as[String].collect().toSet
      assert(committedPaths.size == 60)
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

      val addPaths = df.where("add is not null").select("add.path").as[String].collect().toSet
      assert(addPaths == committedPaths,
        "Reconstruction must surface every leaf entry exactly once across leaves.")
      assert(df.where("protocol.minReaderVersion is not null").count() == 1,
        "Reconstruction must carry the inline protocol action.")
      assert(df.where("metaData.id is not null").count() == 1,
        "Reconstruction must carry the inline metadata action.")
    }
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

      val rootProvider = AMTCheckpointProvider.fromCheckpoint(spark, deltaLog, checkpoint)
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

  test("isEntryMaskedByManifestDv respects per-leaf inline manifest DV bytes") {
    val leafPath = "file:/table/leaf0.parquet"
    val mdv = Map(leafPath -> mdvBytesFor(1L, 3L))
    assert(AMTCheckpointProvider.isEntryMaskedByManifestDv(mdv, leafPath, pos = 1L))
    assert(AMTCheckpointProvider.isEntryMaskedByManifestDv(mdv, leafPath, pos = 3L))
    assert(!AMTCheckpointProvider.isEntryMaskedByManifestDv(mdv, leafPath, pos = 0L))
    assert(!AMTCheckpointProvider.isEntryMaskedByManifestDv(mdv, leafPath, pos = 2L))
    assert(!AMTCheckpointProvider.isEntryMaskedByManifestDv(mdv, "file:/other.parquet", pos = 1L))
    assert(!AMTCheckpointProvider.isEntryMaskedByManifestDv(Map.empty, leafPath, pos = 1L))
  }

  test("manifest deletion vector drops superseded leaf entries during reconstruction") {
    withTable("amt_mdv_drop") {
      val name = "amt_mdv_drop"
      createAMTTable(name, checkpointInterval = 4)
      (1 to 4).foreach(i => sql(s"INSERT INTO $name VALUES ($i)")) // v4: emit.

      val snapshot = deltaLogForName(name).unsafeVolatileSnapshot
      val base = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))
      val baseCount = snapshot.allFiles.count()
      assert(baseCount == 4)

      val leaf = base.leaves.head
      val posToLoc = leafPosToLoc(leaf, base.tableRoot)
      val deletedPos = 0L
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
      val provider =
        new AMTCheckpointProvider(base.checkpointAction, patchedLeaves, base.tableRoot)

      val reconstructed = reconstructedPaths(provider, snapshot.deltaLog)
      assert(reconstructed.size == baseCount - 1,
        "The MDV-marked leaf entry must be dropped from the reconstructed live set.")
      assert(!reconstructed.contains(deletedLoc),
        "The MDV-marked file must not resurface as a live AddFile.")
      assert(reconstructed == full - deletedLoc,
        "Only the MDV-marked entry may be dropped; all other live files must survive.")
    }
  }

  test("manifest deletion vectors apply per leaf across a multi-leaf tree") {
    withTable("amt_mdv_multi") {
      val name = "amt_mdv_multi"
      val base = withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "10") {
        createAMTTable(name, checkpointInterval = 2)
        appendRowsAsSeparateFiles(name, 30) // v1: ids 0..29.
        appendRowsAsSeparateFiles(name, 30, startId = 30) // v2: ids 30..59; emits full tree at v3.
        amtProvider(deltaLogForName(name).update())
          .getOrElse(fail("The interval-boundary append must trigger an AMT-backed snapshot."))
      }
      assert(base.leaves.size == 6,
        s"60 files with entriesPerLeaf=10 must pack into 6 leaves; got ${base.leaves.size}.")
      val deltaLog = deltaLogForName(name)
      val full = reconstructedPaths(base, deltaLog)
      assert(full.size == 60)

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
      val provider = new AMTCheckpointProvider(base.checkpointAction, patched, base.tableRoot)

      val dropped =
        twoPositions.map(locs(twoLeaf)).toSet ++
          locs(wholeLeaf).values.toSet +
          locs(singleLeaf)(sortedPos(singleLeaf).head)
      val reconstructed = reconstructedPaths(provider, deltaLog)
      assert(reconstructed == full -- dropped,
        "Each MDV must drop exactly its marked positions from its own leaf, and nothing else.")
      assert(reconstructed.size == full.size - dropped.size)
    }
  }

  test("surviving entries keep their DV and stats when an MDV drops a sibling") {
    withTable("amt_mdv_sibling") {
      val name = "amt_mdv_sibling"
      createAMTTable(name, checkpointInterval = 3)
      // v1: file A with two physical rows.
      Seq(1, 2).toDF("id").coalesce(1).write.mode("append").insertInto(name)

      // v2: attach a persistent on-disk DV to A (row 0 deleted). interval=3 -> not an emit yet.
      val log = deltaLogForName(name)
      val fileA = log.unsafeVolatileSnapshot.allFiles.collect()
      assert(fileA.length == 1, "The two rows must land in a single file.")
      val dvActions = writeFileWithDVOnDisk(log, fileA.head, RoaringBitmapArray(0L))
      // We commit the DV actions directly as a `Delete` (rather than running a DELETE command), so
      // the SQL operation metrics a real DELETE would populate are absent. History metrics would
      // call `Delete.transformMetrics`, which requires `numDeletedRows`; it is missing here and
      // would fail with `key not found: numDeletedRows`. Disable history metrics to skip that.
      withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false") {
        log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
      }
      sql(s"INSERT INTO $name VALUES (3)") // v3: file B hits the interval; triggers emit.

      val snapshot = deltaLogForName(name).unsafeVolatileSnapshot
      assert(snapshot.version == 4) // v3 triggers emit; v4 materializes the manifest tree.
      val base = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))

      // Classify the two reconstructed entries by DV presence, staying entirely within the
      // reconstruction so path strings are consistent with the leaf `location` values.
      val fullAdds = base.loadActionsForStateReconstruction(spark, snapshot.deltaLog)
        .getOrElse(fail("AMT provider must contribute leaf-derived file actions."))
        .where("add is not null")
        .selectExpr("add.path", "add.deletionVector.storageType", "add.stats")
        .collect()
        .map(r => r.getString(0) -> (Option(r.getString(1)), r.getString(2)))
        .toMap
      assert(fullAdds.size == 2)
      val aPath = fullAdds.collectFirst { case (p, (Some(_), _)) => p }
        .getOrElse(fail("Expected one reconstructed entry to carry a DV."))
      val bPath = fullAdds.collectFirst { case (p, (None, _)) => p }
        .getOrElse(fail("Expected one reconstructed entry without a DV."))
      val aStats = fullAdds(aPath)._2

      // Drop B (the DV-less sibling) via an MDV on its leaf; A must survive untouched.
      val leaf = base.leaves.head
      assert(base.leaves.size == 1, "Both files should pack into a single leaf here.")
      val locToPos = leafPosToLoc(leaf, base.tableRoot).map(_.swap)
      val bPos = locToPos(bPath)
      val patched = Seq(
        leaf.copy(manifest_info =
          leaf.manifest_info.copy(dv = Some(mdvBytesFor(bPos)), dv_cardinality = Some(1L))))
      val provider = new AMTCheckpointProvider(base.checkpointAction, patched, base.tableRoot)

      val survivors = provider.loadActionsForStateReconstruction(spark, snapshot.deltaLog)
        .getOrElse(fail("AMT provider must contribute leaf-derived file actions."))
        .where("add is not null")
        .selectExpr("add.path", "add.deletionVector.storageType", "add.stats")
        .collect()
      assert(survivors.length == 1, "Only the DV-bearing sibling must survive the MDV.")
      assert(survivors.head.getString(0) == aPath,
        "The surviving path must be the DV-bearing file.")
      assert(survivors.head.getString(1) != null,
        "The surviving entry must retain its deletion vector after MDV filtering.")
      assert(survivors.head.getString(2) == aStats,
        "The surviving entry's stats must be unchanged by MDV filtering.")
    }
  }

  test("a manifest DV with only one of dv/dv_cardinality set is rejected") {
    withTable("amt_mdv_malformed") {
      val name = "amt_mdv_malformed"
      createAMTTable(name, checkpointInterval = 3)
      (1 to 3).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))

      val snapshot = deltaLogForName(name).unsafeVolatileSnapshot
      val base = amtProvider(snapshot).getOrElse(fail("expected AMTCheckpointProvider"))

      // `dv` set but `dv_cardinality` missing: the AMT spec requires both or neither.
      val patched =
        base.leaves.head.copy(manifest_info = base.leaves.head.manifest_info.copy(
          dv = Some(mdvBytesFor(0L)), dv_cardinality = None)) +:
          base.leaves.tail
      val provider = new AMTCheckpointProvider(base.checkpointAction, patched, base.tableRoot)

      val e = intercept[IllegalStateException] {
        provider.loadActionsForStateReconstruction(spark, snapshot.deltaLog)
      }
      assert(e.getMessage.contains("dv and dv_cardinality must both be set or both unset"),
        s"Unexpected message: ${e.getMessage}")
    }
  }

  /** Reconstructed live `add.path`s from the (possibly MDV-patched) provider. */
  private def reconstructedPaths(
      provider: AMTCheckpointProvider, deltaLog: DeltaLog): Set[String] =
    provider.loadActionsForStateReconstruction(spark, deltaLog)
      .getOrElse(fail("AMT provider must contribute leaf-derived file actions."))
      .where("add is not null").select("add.path").as[String].collect().toSet

  /** Maps a leaf's parquet row positions to their entry `location`s (bypasses the format check). */
  private def leafPosToLoc(leaf: DataManifestEntry, tableRoot: Path): Map[Long, String] =
    {
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
    val df = spark.createDataset(rows)(enc).toDF()
    Checkpoints.writeAtomicCheckpointParquetFile(spark, df, rootFile, hadoopConf, useRename)
    val size = rootFile.getFileSystem(hadoopConf).getFileStatus(rootFile).getLen
    base.copy(contentRoot = ContentRoot(path = rootFile.toString, sizeInBytes = size))
  }
}
