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
      "deletion vector round-trips through the leaves with a matching uniqueId",
      "amt_dv",
      sqlConfs = Seq(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "false"))(
      setup = name => {
        Seq(1, 2).toDF("id").coalesce(1)
          .write.mode("append").insertInto(name) // one file, two rows.
        // Attach a persistent DV directly rather than relying on DELETE's rewrite heuristic.
        val log = deltaLogForName(name)
        val fileToDv = log.unsafeVolatileSnapshot.allFiles.collect()
        assert(fileToDv.length == 1, "The two rows must land in a single file.")
        val dvActions = writeFileWithDVOnDisk(log, fileToDv.head, RoaringBitmapArray(0L))
        log.startTransaction().commit(dvActions, DeltaOperations.Delete(predicate = Seq.empty))
      }) { context =>
    val snapshot = context.postCheckpointSnapshot
    val provider = context.provider
    checkAnswer(spark.read.table(context.tableName), Seq(Row(2)))

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

  testAcrossAMTCheckpointScenarios(
      "manifest deletion vector drops superseded leaf entries during reconstruction",
      "amt_mdv_drop",
      sqlConfs = leafPackingConfs)(
      setup = name => appendRowsAsSeparateFiles(name, numRows = leafPackedFiles - 1),
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
        appendRowsAsSeparateFiles(name, numRows = leafPackedFiles - 2, startId = 10)
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
    val provider = new AMTCheckpointProvider(base.checkpointAction, patched, base.tableRoot)

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
      setup = name => appendRowsAsSeparateFiles(name, numRows = leafPackedFiles - 1),
      inlineCheckpointTriggerActionsOrSQL = Some(name => Right(
        s"INSERT INTO $name VALUES (${leafPackedFiles - 1})"))) { context =>
    val base = context.provider

    // `dv` set but `dv_cardinality` missing: the AMT spec requires both or neither.
    val patched =
      base.leaves.head.copy(manifest_info = base.leaves.head.manifest_info.copy(
        dv = Some(mdvBytesFor(0L)), dv_cardinality = None)) +:
        base.leaves.tail
    val provider = new AMTCheckpointProvider(base.checkpointAction, patched, base.tableRoot)

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
