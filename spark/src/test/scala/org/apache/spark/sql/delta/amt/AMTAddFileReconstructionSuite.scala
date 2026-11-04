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
import org.apache.spark.sql.delta.actions.{AddFile, Checkpoint, Metadata, SingleAction}
import org.apache.spark.sql.delta.implicits.singleActionEncoder

import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.StructType

/**
 * Checks that the `AddFile`s built as columns
 * ([[AMTCheckpointProviderImpl.liveAddSingleActionsFromColumns]]) are exactly the ones AMT state
 * reconstruction builds per row, for every kind of entry it reads: root and leaf entries,
 * inherited and materialized tracking, masked and dead leaf entries, every optional field set or
 * empty, and trees without live files. Both builds must also reject the same malformed entries.
 *
 * The hand-built trees set each field explicitly. The writer-produced trees cover what the AMT
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

  /** The provider over `checkpoint`'s tree. */
  private def provider(
      deltaLog: DeltaLog,
      checkpoint: Checkpoint,
      manifestCommitVersion: Long): AMTCheckpointProvider =
    AMTCheckpointProvider.fromCheckpoint(deltaLog, checkpoint, manifestCommitVersion)

  /** The live AddFiles `provider`'s column build constructs from its tree. */
  private def columnAddFiles(provider: AMTCheckpointProvider, deltaLog: DeltaLog): Seq[AddFile] =
    provider.liveAddSingleActionsFromColumns(deltaLog).collect().map(_.add).toSeq

  /**
   * Asserts that the column build over `checkpoint`'s tree constructs exactly the AddFiles state
   * reconstruction does: none if `expectEmpty`, some otherwise.
   */
  private def compareReconstructions(
      deltaLog: DeltaLog,
      checkpoint: Checkpoint,
      manifestCommitVersion: Long,
      expectEmpty: Boolean): Unit = {
    val tree = provider(deltaLog, checkpoint, manifestCommitVersion)
    val perRow = stateReconstructionAddFiles(tree, deltaLog)
    val columnar = columnAddFiles(tree, deltaLog)
    assert(perRow.isEmpty == expectEmpty,
      if (expectEmpty) "the tree must have no live files: " + perRow
      else "the tree must have live files to compare")
    def sorted(adds: Seq[AddFile]) = adds.map(comparable).sortBy(_._1.path)
    assert(sorted(columnar) == sorted(perRow),
      "the AddFiles built as columns must match the ones built per row\n" +
        "  columnar=" + columnar.sortBy(_.path).mkString("\n    ") + "\n" +
        "  perRow=" + perRow.sortBy(_.path).mkString("\n    "))
  }

  /** [[compareReconstructions]] over the hand-built tree with root entries `rootEntries`. */
  private def compareReconstructions(
      tree: SyntheticTree,
      rootEntries: Seq[AMTSingleAction],
      expectEmpty: Boolean = false): Unit = {
    val checkpoint = tree.checkpoint(rootEntries)
    compareReconstructions(tree.deltaLog, checkpoint, checkpoint.version, expectEmpty)
  }

  /** [[compareReconstructions]] over the checkpoint a writer-produced scenario emitted. */
  private def compareReconstructions(context: AMTCheckpointScenarioContext): Unit =
    compareReconstructions(
      context.postCheckpointSnapshot.deltaLog,
      context.checkpoint,
      context.manifestCommitVersion,
      expectEmpty = false)

  /**
   * Asserts that both the column build and state reconstruction reject the hand-built tree with
   * root entries `rootEntries`.
   */
  private def compareRejections(tree: SyntheticTree, rootEntries: Seq[AMTSingleAction]): Unit = {
    val checkpoint = tree.checkpoint(rootEntries)
    val rejected = provider(tree.deltaLog, checkpoint, checkpoint.version)
    intercept[Exception](columnAddFiles(rejected, tree.deltaLog))
    intercept[Exception](stateReconstructionAddFiles(rejected, tree.deltaLog))
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
      compareReconstructions(tree, Seq(dataEntry("data/a.parquet", recordCount = 4L).wrap))
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
      compareReconstructions(tree, Seq(entry.wrap))
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
      compareReconstructions(tree, Seq(entry.wrap))
    }
  }

  test("paths are URL encoded") {
    withSyntheticTree("amt_add_file_paths") { tree =>
      compareReconstructions(tree, Seq(
        dataEntry("data/part=a b/c%d.parquet", recordCount = 1L).wrap,
        dataEntry("file:/tmp/amt data/e.parquet", recordCount = 2L).wrap))
    }
  }

  test("an entry without content_stats spells out tightBounds, reading an absent flag as false") {
    withSyntheticTree("amt_add_file_tight_bounds") { tree =>
      compareReconstructions(tree, Seq(
        dataEntry("data/absent.parquet", recordCount = 1L).copy(tight_stats = None).wrap,
        dataEntry("data/tight.parquet", recordCount = 2L).copy(tight_stats = Some(true)).wrap,
        dataEntry("data/loose.parquet", recordCount = 3L).copy(tight_stats = Some(false)).wrap))
    }
  }

  test("leaf entries reference their leaf and inherit from its root pointer") {
    withSyntheticTree("amt_add_file_leaves") { tree =>
      val first = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L, tracking = existingTracking()),
        dataEntry("data/c.parquet", recordCount = 30L)))
      val second = tree.writeLeaf(Seq(dataEntry("data/d.parquet", recordCount = 5L)))
      compareReconstructions(tree, Seq(
        dataEntry("data/root.parquet", recordCount = 4L).wrap,
        leafPointer(first).wrap,
        leafPointer(second, pointerTracking(
          modifiedSnapshotId = Some(98L),
          sequenceNumber = Some(99L),
          fileSequenceNumber = Some(99L),
          firstRowId = Some(9000L))).wrap))
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
        compareReconstructions(tree, Seq(
          dataEntry("data/root.parquet", recordCount = 5L).wrap,
          leafPointer(leaf).wrap))
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
      compareReconstructions(tree, Seq(leafPointer(leaf, mdvPositions = Seq(1L)).wrap))
    }
  }

  test("a tree whose root has no live DATA entry and no leaf reconstructs no file") {
    withSyntheticTree("amt_add_file_no_live_root_entry") { tree =>
      compareReconstructions(tree, Seq(
        dataEntry("data/deleted.parquet", recordCount = 4L,
          tracking = existingTracking().copy(status = Tracking.Status.Deleted)).wrap),
        expectEmpty = true)
    }
  }

  test("a tree whose leaf entries are all masked or dead reconstructs no file") {
    withSyntheticTree("amt_add_file_no_live_leaf_entry") { tree =>
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/masked.parquet", recordCount = 10L),
        dataEntry("data/deleted.parquet", recordCount = 20L,
          tracking = existingTracking().copy(status = Tracking.Status.Deleted))))
      compareReconstructions(
        tree, Seq(leafPointer(leaf, mdvPositions = Seq(0L)).wrap), expectEmpty = true)
    }
  }

  test("a root pointer without first_row_id is rejected") {
    withSyntheticTree("amt_add_file_pointer_without_row_id") { tree =>
      val leaf = tree.writeLeaf(Seq(dataEntry("data/a.parquet", recordCount = 10L)))
      compareRejections(tree, Seq(leafPointer(leaf, pointerTracking(firstRowId = None)).wrap))
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
        compareRejections(tree, Seq(leafPointer(leaf).wrap))
      }
    }
  }

  test("a leaf entry whose data file is not Parquet is rejected") {
    withSyntheticTree("amt_add_file_orc_entry") { tree =>
      val leaf = tree.corruptLeaf(
        tree.writeLeaf(Seq(dataEntry("data/a.parquet", recordCount = 10L))),
        "file_format")(_ => lit("orc"))
      compareRejections(tree, Seq(leafPointer(leaf).wrap))
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
      compareRejections(tree, Seq(leafPointer(leaf).wrap))
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
    compareReconstructions(context)
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
    compareReconstructions(context)
  }
}

