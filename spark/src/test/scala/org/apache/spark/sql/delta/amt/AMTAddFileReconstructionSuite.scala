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

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaLog}
import org.apache.spark.sql.delta.actions.{AddFile, BackReference, Checkpoint, DeletionVectorDescriptor, Metadata, SingleAction}
import org.apache.spark.sql.delta.implicits.singleActionEncoder
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.StructType

/**
 * Checks every field of the `AddFile`s that AMT state reconstruction builds, for every kind of
 * entry it reads: root and leaf entries, inherited and materialized tracking, masked and dead leaf
 * entries, every optional field set or empty, trees without live files, and the malformed entries
 * it must reject. Every tree is reconstructed by both [[PerRowAMTCheckpointProvider]] and
 * [[ColumnarAMTCheckpointProvider]], which must agree exactly, so each expectation holds for both.
 *
 * The hand-built trees pin each field to a literal. The writer-produced trees cover what the AMT
 * writer persists for every partition and stats type, deletion vectors, and leaves.
 */
class AMTAddFileReconstructionSuite extends AMTSyntheticTreeTestBase {

  // ---------------------------------------------------------------------------------------------
  // Reading the AddFiles
  // ---------------------------------------------------------------------------------------------

  /** The live AddFiles state reconstruction builds from `provider`'s tree. */
  private def stateReconstructionAddFiles(
      provider: AMTCheckpointProvider, deltaLog: DeltaLog): Seq[AddFile] =
    provider.loadActionsForStateReconstruction(spark, deltaLog)
      .getOrElse(fail("AMT provider must contribute reconstructed actions."))
      .where(col("add").isNotNull)
      .as[SingleAction]
      .collect()
      .map(_.add)
      .toSeq

  /** The per-row and the columnar provider over `checkpoint`'s tree, in that order. */
  private def providers(
      deltaLog: DeltaLog,
      checkpoint: Checkpoint,
      manifestCommitVersion: Long): (AMTCheckpointProvider, AMTCheckpointProvider) = {
    def provider(columnar: Boolean): AMTCheckpointProvider =
      withSQLConf(
          DeltaSQLConf.AMT_COLUMNAR_STATE_RECONSTRUCTION_ENABLED.key -> columnar.toString) {
        AMTCheckpointProvider.fromCheckpoint(deltaLog, checkpoint, manifestCommitVersion)
      }
    val perRow = provider(columnar = false)
    val columnar = provider(columnar = true)
    assert(perRow.isInstanceOf[PerRowAMTCheckpointProvider], perRow)
    assert(columnar.isInstanceOf[ColumnarAMTCheckpointProvider], columnar)
    (perRow, columnar)
  }

  /**
   * The live AddFiles both providers over `checkpoint`'s tree reconstruct, by path: none if
   * `expectEmpty`, some otherwise. Asserts that the two reconstruct exactly the same AddFiles, so
   * an expectation on the result holds for both.
   */
  private def reconstructedAddFiles(
      deltaLog: DeltaLog,
      checkpoint: Checkpoint,
      manifestCommitVersion: Long,
      expectEmpty: Boolean): Map[String, AddFile] = {
    val (perRowProvider, columnarProvider) = providers(deltaLog, checkpoint, manifestCommitVersion)
    val perRow = stateReconstructionAddFiles(perRowProvider, deltaLog)
    val columnar = stateReconstructionAddFiles(columnarProvider, deltaLog)
    assert(perRow.isEmpty == expectEmpty,
      if (expectEmpty) "the tree must have no live files: " + perRow
      else "the tree must have live files to compare")
    def sorted(adds: Seq[AddFile]) = adds.map(comparable).sortBy(_._1.path)
    assert(sorted(columnar) == sorted(perRow),
      "the columnar AddFiles must match the per-row ones\n" +
        "  columnar=" + columnar.sortBy(_.path).mkString("\n    ") + "\n" +
        "  perRow=" + perRow.sortBy(_.path).mkString("\n    "))
    val byPath = perRow.map(add => add.path -> add).toMap
    assert(byPath.size == perRow.size, "reconstruction must not duplicate files")
    byPath
  }

  /** [[reconstructedAddFiles]] of the hand-built tree with root entries `rootEntries`. */
  private def reconstructedAddFiles(
      tree: SyntheticTree,
      rootEntries: Seq[AMTSingleAction],
      expectEmpty: Boolean = false): Map[String, AddFile] = {
    val checkpoint = tree.checkpoint(rootEntries)
    reconstructedAddFiles(tree.deltaLog, checkpoint, checkpoint.version, expectEmpty)
  }

  /** [[reconstructedAddFiles]] of the checkpoint a writer-produced scenario emitted. */
  private def reconstructedAddFiles(context: AMTCheckpointScenarioContext): Map[String, AddFile] =
    reconstructedAddFiles(
      context.postCheckpointSnapshot.deltaLog,
      context.checkpoint,
      context.manifestCommitVersion,
      expectEmpty = false)

  /**
   * Asserts that both providers reject the hand-built tree with root entries `rootEntries`, the
   * columnar one with `expectedMessage`. The per-row one words some of its errors differently;
   * `perRowMessage` is what it must report instead.
   */
  private def assertRejected(
      tree: SyntheticTree,
      rootEntries: Seq[AMTSingleAction],
      expectedMessage: String,
      perRowMessage: Option[String] = None): Unit = {
    val checkpoint = tree.checkpoint(rootEntries)
    val (perRow, columnar) = providers(tree.deltaLog, checkpoint, checkpoint.version)
    val columnarError = intercept[Exception](stateReconstructionAddFiles(columnar, tree.deltaLog))
    assert(causeMessages(columnarError).contains(expectedMessage), causeMessages(columnarError))
    val perRowError = intercept[Exception](stateReconstructionAddFiles(perRow, tree.deltaLog))
    assert(causeMessages(perRowError).contains(perRowMessage.getOrElse(expectedMessage)),
      causeMessages(perRowError))
  }

  /**
   * `add` in a form that compares by value: [[AddFile.tags]] and the passthrough's `key_metadata`
   * hold arrays, which case-class equality compares by reference.
   */
  private def comparable(add: AddFile)
      : (AddFile, Option[Map[String, String]], Option[(Option[Int], Option[Int], Option[Seq[Byte]],
        Option[Seq[Long]])]) = (
    add.copy(tags = null, amtPassthrough = None),
    Option(add.tags),
    add.amtPassthrough.map { passthrough =>
      (passthrough.spec_id, passthrough.sort_order_id, passthrough.key_metadata.map(_.toSeq),
        passthrough.split_offsets)
    })

  private def assertAddFiles(actual: Map[String, AddFile], expected: AddFile*): Unit = {
    assert(actual.keySet == expected.map(_.path).toSet)
    expected.foreach { add =>
      assert(comparable(actual(add.path)) == comparable(add),
        "\n  actual=" + actual(add.path) + "\n  expected=" + add)
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Expected values
  // ---------------------------------------------------------------------------------------------

  /** The AddFile of an entry built by [[dataEntry]], read from the root. */
  private def plainAddFile(path: String, numRecords: Long): AddFile = AddFile(
    path = path,
    partitionValues = Map.empty,
    size = 128L,
    modificationTime = 0L,
    dataChange = false,
    stats = "{\"numRecords\":" + numRecords + ",\"tightBounds\":false}")

  /** The AddFile of an entry built by [[dataEntry]], read from position `pos` of `leaf`. */
  private def leafAddFile(
      path: String,
      numRecords: Long,
      leaf: WrittenLeaf,
      pos: Int,
      baseRowId: Long,
      defaultRowCommitVersion: Long = inheritedFileSequenceNumber,
      snapshotId: Long = inheritedSnapshotId): AddFile =
    plainAddFile(path, numRecords).copy(
      baseRowId = Some(baseRowId),
      defaultRowCommitVersion = Some(defaultRowCommitVersion),
      backReference = Some(BackReference(leaf.location, pos)),
      snapshotId = Some(snapshotId))

  // ---------------------------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------------------------

  private def physicalName(metadata: Metadata, column: String): String =
    DeltaColumnMapping.getPhysicalName(metadata.schema(column))

  private def existingTracking(
      snapshotId: Option[Long] = Some(3L),
      modifiedSnapshotId: Option[Long] = None,
      sequenceNumber: Option[Long] = Some(4L),
      fileSequenceNumber: Option[Long] = Some(5L),
      firstRowId: Option[Long] = Some(5000L)): Tracking =
    unassignedTracking(Tracking.Status.Existing).copy(
      snapshot_id = snapshotId,
      modified_snapshot_id = modifiedSnapshotId,
      sequence_number = sequenceNumber,
      file_sequence_number = fileSequenceNumber,
      first_row_id = firstRowId)

  // ---------------------------------------------------------------------------------------------
  // Hand-built trees
  // ---------------------------------------------------------------------------------------------

  test("a root entry with only the required fields set") {
    withSyntheticTree("amt_add_file_required_fields") { tree =>
      val files =
        reconstructedAddFiles(tree, Seq(dataEntry("data/a.parquet", recordCount = 4L).wrap))
      assertAddFiles(files, plainAddFile("data/a.parquet", numRecords = 4L))
    }
  }

  test("a root entry with every optional field set") {
    withSyntheticTree(
        "amt_add_file_every_field", tableSchema = "id INT, part STRING",
        partitionColumns = Seq("part")) { tree =>
      val id = physicalName(tree.base.metaData, "id")
      val part = physicalName(tree.base.metaData, "part")
      val stats = "{\"numRecords\":4,\"minValues\":{\"" + id + "\":1}," +
        "\"maxValues\":{\"" + id + "\":4},\"nullCount\":{\"" + id + "\":0},\"tightBounds\":true}"
      val entry = dataEntry(
          "data/part=x/a.parquet",
          recordCount = 4L,
          tracking = existingTracking(modifiedSnapshotId = Some(6L)))
        .copy(
          partition = Some(Map(part -> "x")),
          // The stored size counts the bitmap's 4-byte length and checksum; Delta's does not.
          deletion_vector = Some(DeletionVector(
            location = "deletion_vector_1.bin",
            offset = 1L,
            size_in_bytes = 48L,
            cardinality = 2L)),
          spec_id = Some(0),
          sort_order_id = Some(1),
          content_stats = Some(stats),
          tight_stats = Some(true),
          key_metadata = Some(Array[Byte](1, 2)),
          split_offsets = Some(Seq(4L, 64L)),
          tags = Some(Map("tag" -> "value")))
      val files = reconstructedAddFiles(tree, Seq(entry.wrap))

      assertAddFiles(files, AddFile(
        path = "data/part=x/a.parquet",
        partitionValues = Map(part -> "x"),
        size = 128L,
        modificationTime = 0L,
        dataChange = false,
        stats = stats,
        tags = Map("tag" -> "value"),
        deletionVector = DeletionVectorDescriptor.createRelativePathDVDescriptor(
          relativePath = "deletion_vector_1.bin", sizeInBytes = 40, cardinality = 2L,
          offset = Some(1)),
        baseRowId = Some(5000L),
        defaultRowCommitVersion = Some(5L),
        amtPassthrough = Some(AMTPassthrough(
          spec_id = Some(0),
          sort_order_id = Some(1),
          key_metadata = Some(Array[Byte](1, 2)),
          split_offsets = Some(Seq(4L, 64L)))),
        snapshotId = Some(3L),
        modifiedSnapshotId = Some(6L)))
    }
  }

  test("a deletion vector at an absolute location") {
    withSyntheticTree("amt_add_file_absolute_dv") { tree =>
      val entry = dataEntry("data/a.parquet", recordCount = 4L).copy(
        deletion_vector = Some(DeletionVector(
          location = "file:/tmp/dvs/deletion vector.bin",
          offset = 1L,
          size_in_bytes = 48L,
          cardinality = 2L)))
      val files = reconstructedAddFiles(tree, Seq(entry.wrap))

      assertAddFiles(files, plainAddFile("data/a.parquet", numRecords = 4L).copy(
        deletionVector = DeletionVectorDescriptor.onDiskWithAbsolutePath(
          path = "file:/tmp/dvs/deletion%20vector.bin", sizeInBytes = 40, cardinality = 2L,
          offset = Some(1))))
    }
  }

  test("paths are URL encoded") {
    withSyntheticTree("amt_add_file_paths") { tree =>
      val files = reconstructedAddFiles(tree, Seq(
        dataEntry("data/part=a b/c%d.parquet", recordCount = 1L).wrap,
        dataEntry("file:/tmp/amt data/e.parquet", recordCount = 2L).wrap))

      assertAddFiles(files,
        plainAddFile("data/part=a%20b/c%25d.parquet", numRecords = 1L),
        plainAddFile("file:/tmp/amt%20data/e.parquet", numRecords = 2L))
    }
  }

  test("an entry without content_stats spells out tightBounds, reading an absent flag as false") {
    withSyntheticTree("amt_add_file_tight_bounds") { tree =>
      val files = reconstructedAddFiles(tree, Seq(
        dataEntry("data/absent.parquet", recordCount = 1L).copy(tight_stats = None).wrap,
        dataEntry("data/tight.parquet", recordCount = 2L).copy(tight_stats = Some(true)).wrap,
        dataEntry("data/loose.parquet", recordCount = 3L).copy(tight_stats = Some(false)).wrap))

      assertAddFiles(files,
        plainAddFile("data/absent.parquet", numRecords = 1L),
        plainAddFile("data/tight.parquet", numRecords = 2L)
          .copy(stats = "{\"numRecords\":2,\"tightBounds\":true}"),
        plainAddFile("data/loose.parquet", numRecords = 3L))
    }
  }

  test("leaf entries reference their leaf and inherit from its root pointer") {
    withSyntheticTree("amt_add_file_leaves") { tree =>
      val first = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L, tracking = existingTracking()),
        dataEntry("data/c.parquet", recordCount = 30L)))
      val second = tree.writeLeaf(Seq(dataEntry("data/d.parquet", recordCount = 5L)))
      val files = reconstructedAddFiles(tree, Seq(
        dataEntry("data/root.parquet", recordCount = 4L).wrap,
        leafPointer(first).wrap,
        leafPointer(second, pointerTracking(
          modifiedSnapshotId = Some(98L),
          sequenceNumber = Some(99L),
          fileSequenceNumber = Some(99L),
          firstRowId = Some(9000L))).wrap))

      assertAddFiles(files,
        // A root entry has no parent: it keeps its own (here empty) tracking.
        plainAddFile("data/root.parquet", numRecords = 4L),
        leafAddFile("data/a.parquet", 10L, first, pos = 0, baseRowId = 1000L),
        // A materialized entry keeps its own tracking and does not advance the row ids after it.
        leafAddFile("data/b.parquet", 20L, first, pos = 1, baseRowId = 5000L,
          defaultRowCommitVersion = 5L, snapshotId = 3L),
        leafAddFile("data/c.parquet", 30L, first, pos = 2, baseRowId = 1010L),
        // The pointer's modified_snapshot_id is not inherited.
        leafAddFile("data/d.parquet", 5L, second, pos = 0, baseRowId = 9000L,
          defaultRowCommitVersion = 99L))
    }
  }

  test("filter CSE does not run the entry checks on root leaf pointers") {
    // With subexpression elimination in FilterExec, the checks of the AddFile columns can be
    // evaluated before the DATA filter, on the root's DATA_MANIFEST pointer rows.
    withSQLConf(
        "spark.sql.subexpressionElimination.enabled" -> "true",
        "spark.sql.subexpressionElimination.filterExec.enabled" -> "true",
        "spark.sql.subexpressionElimination.skipForShortcutExpr" -> "false",
        "spark.sql.codegen.wholeStage" -> "true",
        "spark.sql.adaptive.enabled" -> "false") {
      withSyntheticTree("amt_add_file_filter_cse") { tree =>
        val leaf = tree.writeLeaf(Seq(dataEntry("data/a.parquet", recordCount = 10L)))
        val files = reconstructedAddFiles(tree, Seq(
          dataEntry("data/root.parquet", recordCount = 5L).wrap,
          leafPointer(leaf).wrap))
        assert(files.keySet == Set("data/root.parquet", "data/a.parquet"))
      }
    }
  }

  test("masked and dead leaf entries are skipped, and only masked ones advance the row ids") {
    withSyntheticTree("amt_add_file_skipped_leaf_entries") { tree =>
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/masked.parquet", recordCount = 20L),
        dataEntry("data/deleted.parquet", recordCount = 30L,
          tracking = existingTracking().copy(status = Tracking.Status.Deleted)),
        dataEntry("data/d.parquet", recordCount = 40L)))
      val files = reconstructedAddFiles(tree, Seq(leafPointer(leaf, mdvPositions = Seq(1L)).wrap))

      assertAddFiles(files,
        leafAddFile("data/a.parquet", 10L, leaf, pos = 0, baseRowId = 1000L),
        leafAddFile("data/d.parquet", 40L, leaf, pos = 3, baseRowId = 1030L))
    }
  }

  test("a tree whose root has no live DATA entry and no leaf reconstructs no file") {
    withSyntheticTree("amt_add_file_no_live_root_entry") { tree =>
      assert(reconstructedAddFiles(tree, Seq(
        dataEntry("data/deleted.parquet", recordCount = 4L,
          tracking = existingTracking().copy(status = Tracking.Status.Deleted)).wrap),
        expectEmpty = true).isEmpty)
    }
  }

  test("a tree whose leaf entries are all masked or dead reconstructs no file") {
    withSyntheticTree("amt_add_file_no_live_leaf_entry") { tree =>
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/masked.parquet", recordCount = 10L),
        dataEntry("data/deleted.parquet", recordCount = 20L,
          tracking = existingTracking().copy(status = Tracking.Status.Deleted))))
      assert(reconstructedAddFiles(
        tree, Seq(leafPointer(leaf, mdvPositions = Seq(0L)).wrap), expectEmpty = true).isEmpty)
    }
  }

  test("both implementations read the same root files for the incremental writer") {
    withSyntheticTree("amt_add_file_root_read") { tree =>
      val leaf = tree.writeLeaf(Seq(dataEntry("data/leaf.parquet", recordCount = 10L)))
      val checkpoint = tree.checkpoint(Seq(
        dataEntry("data/a.parquet", recordCount = 4L).wrap,
        dataEntry("data/deleted.parquet", recordCount = 5L,
          tracking = existingTracking().copy(status = Tracking.Status.Deleted)).wrap,
        leafPointer(leaf).wrap))
      val (perRow, columnar) = providers(tree.deltaLog, checkpoint, checkpoint.version)
      Seq(perRow, columnar).foreach { provider =>
        val rootFiles = provider.readLiveRootDataEntries(tree.deltaLog)
        assertAddFiles(rootFiles.map(add => add.path -> add).toMap,
          plainAddFile("data/a.parquet", numRecords = 4L))
      }
    }
  }

  test("a root pointer without first_row_id is rejected") {
    withSyntheticTree("amt_add_file_pointer_without_row_id") { tree =>
      val leaf = tree.writeLeaf(Seq(dataEntry("data/a.parquet", recordCount = 10L)))
      assertRejected(
        tree,
        Seq(leafPointer(leaf, pointerTracking(firstRowId = None)).wrap),
        "tracking.first_row_id must not be null in AMT root DATA_MANIFEST entries.")
    }
  }

  Seq(
    "snapshot_id" -> existingTracking(snapshotId = None),
    "sequence_number" -> existingTracking(sequenceNumber = None),
    "file_sequence_number" -> existingTracking(fileSequenceNumber = None),
    "first_row_id" -> existingTracking(firstRowId = None)
  ).foreach { case (field, tracking) =>
    test("a leaf entry that is not ADDED and has no " + field + " is rejected") {
      withSyntheticTree("amt_add_file_without_" + field) { tree =>
        val leaf = tree.writeLeaf(Seq(
          dataEntry("data/a.parquet", recordCount = 10L, tracking = tracking)))
        assertRejected(
          tree,
          Seq(leafPointer(leaf).wrap),
          "Malformed AMT data entry 'data/a.parquet': tracking." + field + " is null but the " +
            "entry's status is EXISTING. Only ADDED entries inherit " + field + " from the root " +
            "DATA_MANIFEST entry; every other status must materialize it.")
      }
    }
  }

  test("a leaf entry whose data file is not Parquet is rejected") {
    withSyntheticTree("amt_add_file_orc_entry") { tree =>
      val leaf = tree.corruptLeaf(
        tree.writeLeaf(Seq(dataEntry("data/a.parquet", recordCount = 10L))),
        "file_format")(_ => lit("orc"))
      assertRejected(
        tree,
        Seq(leafPointer(leaf).wrap),
        "Malformed AMT data entry 'data/a.parquet': amtPassthrough only supports parquet. got orc.",
        perRowMessage = Some("file_format must be missing or 'parquet'; got orc."))
    }
  }

  test("a leaf DATA entry with manifest_info is rejected") {
    withSyntheticTree("amt_add_file_data_manifest_info") { tree =>
      val leaf = tree.corruptLeaf(
          tree.writeLeaf(Seq(dataEntry("data/a.parquet", recordCount = 10L))),
          "manifest_info") { dataType =>
        struct(dataType.asInstanceOf[StructType].fields.toIndexedSeq.map { field =>
          (if (field.nullable) lit(null) else lit(0)).cast(field.dataType).as(field.name)
        }: _*)
      }
      assertRejected(
        tree,
        Seq(leafPointer(leaf).wrap),
        "Malformed AMT data entry 'data/a.parquet': manifest_info must be null for a DATA entry.",
        perRowMessage = Some("manifest_info must be set iff content_type is a manifest pointer"))
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Writer-produced trees
  // ---------------------------------------------------------------------------------------------

  private val allTypesSchema = (
    allTypeColumns.values.map(_.columnDef("d_")) ++
      partitionableTestColumns.map(_.columnDef("p_"))).mkString(", ")

  testAcrossAMTCheckpointScenarios(
      "partition values and stats of every type",
      "amt_add_file_all_types",
      tableSchema = allTypesSchema,
      partitionColumns = partitionableTestColumns.map("p_" + _.name))(
      setup = table =>
        appendRowsAsSeparateFiles(table, numFiles = 2, columnExprs = allTypesTableColumnExprs),
      inlineCheckpointTriggerActionsOrSQL = Some(table => Right(
        "INSERT INTO " + table + " VALUES (" +
          Seq.fill(allTypeColumns.size + partitionableTestColumns.size)("NULL").mkString(", ") +
          ")"))) { context =>
    val metadata = context.checkpoint.metaData
    val files = reconstructedAddFiles(context)
    val partitionColumns = partitionableTestColumns.map(column => "p_" + column.name)
    val byLogicalName = files.values.map { add =>
      partitionColumns.map(name => name -> add.partitionValues(physicalName(metadata, name))).toMap
    }.toSet
    val nullRow = partitionColumns.map(_ -> (null: String)).toMap
    assert(byLogicalName == Set(
      Map(
        "p_int" -> "0", "p_long" -> "0", "p_short" -> "0", "p_byte" -> "0", "p_str" -> "row0",
        "p_date" -> "2026-07-25", "p_ts" -> "2026-07-25T08:02:03.456000Z",
        "p_ts_ntz" -> "2026-07-25 01:02:03.456", "p_dec" -> "0.000", "p_bool" -> "false",
        "p_float" -> "0.0", "p_double" -> "0.0", "p_binary" -> "b0"),
      Map(
        "p_int" -> "1", "p_long" -> "1", "p_short" -> "1", "p_byte" -> "1", "p_str" -> "row1",
        "p_date" -> "2026-07-26", "p_ts" -> "2026-07-26T08:02:03.456000Z",
        "p_ts_ntz" -> "2026-07-26 01:02:03.456", "p_dec" -> "0.125", "p_bool" -> "true",
        "p_float" -> "1.0", "p_double" -> "1.0", "p_binary" -> "b1"),
      nullRow), byLogicalName)

    val dInt = physicalName(metadata, "d_int")
    val statsByIntPartition = files.values.map { add =>
      val stats = JsonUtils.mapper.readTree(add.stats)
      Option(add.partitionValues(physicalName(metadata, "p_int"))) ->
        (stats.path("numRecords").asLong, stats.path("minValues").path(dInt).asText(null),
          stats.path("nullCount").path(dInt).asLong, stats.path("tightBounds").asBoolean)
    }.toMap
    assert(statsByIntPartition == Map(
      Some("0") -> (1L, "0", 0L, false),
      Some("1") -> (1L, "1", 0L, false),
      None -> (1L, null, 1L, false)), statsByIntPartition)
    assert(files.values.forall { add =>
      add.size > 0 && add.modificationTime == 0L && !add.dataChange &&
        add.deletionVector == null && add.clusteringProvider.isEmpty &&
        add.amtPassthrough.isEmpty
    }, files.values)
  }

  testAcrossAMTCheckpointScenarios(
      "deletion vectors, back references and masked leaf entries",
      "amt_add_file_leaf_tree",
      sqlConfs = leafPackingConfs)(
      setup = { table =>
        appendRowsAsSeparateFiles(table, numFiles = leafPackedFiles, rowsPerFile = 2)
        commitCheckpoint(deltaLogForName(table), incremental = false)
      },
      // Removes the first three files whole and one row of every other remaining file.
      inlineCheckpointTriggerActionsOrSQL = Some(table => Right(
        "DELETE FROM " + table + " WHERE id < 6 OR id % 4 = 1"))) { context =>
    val files = reconstructedAddFiles(context)
    assert(files.size == leafPackedFiles - 3)
    val withDeletionVectors = files.values.filter(_.deletionVector != null)
    assert(withDeletionVectors.size == (leafPackedFiles - 3) / 2)
    assert(withDeletionVectors.forall(_.deletionVector.cardinality == 1L))

    // Each back reference must point at the leaf row of its own file.
    val dataPath = context.postCheckpointSnapshot.deltaLog.dataPath.toString
    val leafRows = context.provider.liveLeafManifestAbsolutePaths.flatMap { leafPath =>
      val manifest = AMTUtils.relativizeLocation(dataPath, leafPath.toString)
      allowReadWithinDeltaLog {
        spark.read.parquet(leafPath.toString)
          .select(col("_metadata.row_index"), col("location"))
          .collect()
          .map(row => BackReference(manifest, row.getLong(0).toInt) -> row.getString(1))
      }
    }.toMap
    val leafResident = files.values.filter(_.backReference.isDefined)
    assert(leafResident.nonEmpty, "the tree must keep some files in leaves")
    leafResident.foreach { add =>
      assert(leafRows.get(add.backReference.get).map(SparkPath.fromPathString(_).urlEncoded)
        .contains(add.path), add)
    }
  }
}

