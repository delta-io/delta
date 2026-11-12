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

/**
 * The AMT writer materializes every leaf value today, so no tree it produces exercises
 * inheritance. These tests therefore hand-build manifest trees with deliberately null leaf
 * tracking.
 */
class AMTInheritanceReadSuite extends AMTSyntheticTreeTestBase {

  test("leaf entries inherit the file sequence number and row ids of their root pointer") {
    withSyntheticTree("amt_inherit_all") { tree =>
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L),
        dataEntry("data/c.parquet", recordCount = 30L)))
      val files = tree.reconstruct(Seq(leafPointer(leaf).wrap))

      assert(files.keySet == Set("data/a.parquet", "data/b.parquet", "data/c.parquet"))
      assert(files.values.forall(_.snapshotId.contains(inheritedSnapshotId)))
      assert(files.values.forall(_.defaultRowCommitVersion.contains(inheritedFileSequenceNumber)),
        s"every entry must inherit the pointer's file sequence number; got ${files.values}")
      // Row ids run from the pointer's first_row_id in the leaf's physical order, each file
      // starting where the previous one's records ended.
      assert(files("data/a.parquet").baseRowId.contains(1000L))
      assert(files("data/b.parquet").baseRowId.contains(1010L))
      assert(files("data/c.parquet").baseRowId.contains(1030L))
    }
  }

  test("leaf entries do not inherit modified_snapshot_id from their root pointer") {
    withSyntheticTree("amt_no_dv_snapshot_inheritance") { tree =>
      val leaf = tree.writeLeaf(Seq(dataEntry("data/a.parquet", recordCount = 10L)))
      val files = tree.reconstruct(Seq(
        leafPointer(leaf, pointerTracking(modifiedSnapshotId = Some(99L))).wrap))

      assert(files("data/a.parquet").snapshotId.contains(inheritedSnapshotId))
      assert(files("data/a.parquet").modifiedSnapshotId.isEmpty)
    }
  }

  test("a materialized first_row_id is kept and does not advance the ones after it") {
    withSyntheticTree("amt_inherit_mixed") { tree =>
      val materialized = unassignedTracking().copy(first_row_id = Some(5000L))
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L, tracking = materialized),
        dataEntry("data/c.parquet", recordCount = 30L)))
      val files = tree.reconstruct(Seq(leafPointer(leaf).wrap))

      assert(files("data/a.parquet").baseRowId.contains(1000L))
      assert(files("data/b.parquet").baseRowId.contains(5000L))
      // Only the entries that inherit a row id contribute to the running offset, so `c` follows
      // `a` rather than `b`.
      assert(files("data/c.parquet").baseRowId.contains(1010L))
    }
  }

  test("an MDV-masked entry is skipped but still advances the row ids after it") {
    withSyntheticTree("amt_inherit_mdv") { tree =>
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L),
        dataEntry("data/c.parquet", recordCount = 30L)))
      val files = tree.reconstruct(Seq(leafPointer(leaf, mdvPositions = Seq(1L)).wrap))

      assert(files.keySet == Set("data/a.parquet", "data/c.parquet"),
        "the MDV-masked entry must not appear in the reconstructed live set")
      assert(files("data/a.parquet").baseRowId.contains(1000L))
      // Masking `b` must not pull `c`'s row id down to 1010.
      assert(files("data/c.parquet").baseRowId.contains(1030L))
    }
  }

  test("a non-live entry with a materialized row id does not consume the inherited range") {
    withSyntheticTree("amt_inherit_dead_entry") { tree =>
      val deletedTracking = unassignedTracking(Tracking.Status.Deleted).copy(
        snapshot_id = Some(1L),
        sequence_number = Some(9L),
        file_sequence_number = Some(9L),
        first_row_id = Some(5000L))
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L,
          tracking = deletedTracking),
        dataEntry("data/c.parquet", recordCount = 30L)))
      val files = tree.reconstruct(Seq(leafPointer(leaf).wrap))

      assert(files.keySet == Set("data/a.parquet", "data/c.parquet"))
      assert(files("data/a.parquet").baseRowId.contains(1000L))
      assert(files("data/c.parquet").baseRowId.contains(1010L))
    }
  }

  test("manifest filters stay local to their leaf while masked entries advance inheritance") {
    withSyntheticTree("amt_inherit_mdv_two_leaves") { tree =>
      val first = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L)))
      val second = tree.writeLeaf(Seq(
        dataEntry("data/c.parquet", recordCount = 5L),
        dataEntry("data/d.parquet", recordCount = 7L)))
      val files = tree.reconstruct(Seq(
        leafPointer(
          first,
          pointerTracking(firstRowId = Some(1000L)),
          mdvPositions = Seq(0L)).wrap,
        leafPointer(second, pointerTracking(firstRowId = Some(9000L))).wrap))

      assert(files.keySet == Set("data/b.parquet", "data/c.parquet", "data/d.parquet"))
      assert(files("data/b.parquet").baseRowId.contains(1010L))
      assert(files("data/c.parquet").baseRowId.contains(9000L))
      assert(files("data/d.parquet").baseRowId.contains(9005L))
    }
  }

  test("each leaf inherits from its own root pointer") {
    withSyntheticTree("amt_inherit_two_leaves") { tree =>
      val first = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L)))
      val second = tree.writeLeaf(Seq(
        dataEntry("data/c.parquet", recordCount = 5L),
        dataEntry("data/d.parquet", recordCount = 7L)))
      val files = tree.reconstruct(Seq(
        leafPointer(first).wrap,
        leafPointer(second, pointerTracking(
          sequenceNumber = Some(99L),
          fileSequenceNumber = Some(99L), firstRowId = Some(9000L))).wrap))

      assert(files("data/b.parquet").defaultRowCommitVersion.contains(inheritedFileSequenceNumber))
      assert(files("data/d.parquet").defaultRowCommitVersion.contains(99L))
      assert(files("data/a.parquet").baseRowId.contains(1000L))
      assert(files("data/b.parquet").baseRowId.contains(1010L))
      assert(files("data/c.parquet").baseRowId.contains(9000L))
      assert(files("data/d.parquet").baseRowId.contains(9005L))
    }
  }

  test("a root pointer that assigns a file sequence number but no first_row_id is rejected") {
    withSyntheticTree("amt_inherit_no_row_ids") { tree =>
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L)))
      val error = intercept[Exception] {
        tree.reconstruct(Seq(leafPointer(leaf, pointerTracking(firstRowId = None)).wrap))
      }
      assert(causeMessages(error).contains("tracking.first_row_id must not be null"))
    }
  }

  test("a root pointer without snapshot_id is rejected") {
    withSyntheticTree("amt_inherit_nothing") { tree =>
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L),
        dataEntry("data/b.parquet", recordCount = 20L)))
      val error = intercept[Exception] {
        tree.reconstruct(Seq(leafPointer(leaf, unassignedTracking()).wrap))
      }
      assert(causeMessages(error).contains("tracking.snapshot_id must not be null"))
    }
  }

  test("root-resident entries are read verbatim") {
    withSyntheticTree("amt_inherit_root_entries") { tree =>
      val leaf = tree.writeLeaf(Seq(dataEntry("data/leaf.parquet", recordCount = 10L)))
      // A root DATA entry has nothing above it to inherit from, so it keeps its own tracking even
      // while a sibling pointer hands values down to its leaf.
      val files = tree.reconstruct(Seq(
        dataEntry("data/inline.parquet", recordCount = 4L).wrap,
        leafPointer(leaf).wrap))

      assert(files("data/inline.parquet").defaultRowCommitVersion.isEmpty)
      assert(files("data/inline.parquet").baseRowId.isEmpty)
      assert(files("data/leaf.parquet").defaultRowCommitVersion
        .contains(inheritedFileSequenceNumber))
      assert(files("data/leaf.parquet").baseRowId.contains(1000L))
    }
  }

  test("a live entry that is not ADDED and has no file sequence number is rejected") {
    withSyntheticTree("amt_inherit_malformed") { tree =>
      // Only an ADDED entry may leave its file sequence number to inheritance. An EXISTING entry
      // predates the tree, so there is no value it could correctly inherit.
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L,
          tracking = unassignedTracking(Tracking.Status.Existing).copy(
            snapshot_id = Some(1L),
            sequence_number = Some(7L),
            first_row_id = Some(9L)))))
      val error = intercept[Exception] {
        tree.reconstruct(Seq(leafPointer(leaf).wrap))
      }
      val messages = causeMessages(error)
      assert(messages.contains("tracking.file_sequence_number is null"), messages)
      assert(messages.contains("EXISTING"), messages)
    }
  }

  test("a live entry that is not ADDED and has no first_row_id is rejected") {
    withSyntheticTree("amt_inherit_malformed_row_id") { tree =>
      val leaf = tree.writeLeaf(Seq(
        dataEntry("data/a.parquet", recordCount = 10L,
          tracking = unassignedTracking(Tracking.Status.Existing)
            .copy(
              snapshot_id = Some(1L),
              sequence_number = Some(7L),
              file_sequence_number = Some(8L)))))
      val error = intercept[Exception] {
        tree.reconstruct(Seq(leafPointer(leaf).wrap))
      }
      val messages = causeMessages(error)
      assert(messages.contains("tracking.first_row_id is null"), messages)
      assert(messages.contains("EXISTING"), messages)
    }
  }

  test("a materialized file_sequence_number on a leaf entry is read without inheriting") {
    Tracking.Status.liveEntryStatuses.foreach { status =>
      withSyntheticTree(s"amt_materialized_leaf_${Tracking.Status.nameOf(status)}") { tree =>
        val materialized = 77L
        val materializedFirstRowId =
          if (status == Tracking.Status.Added) None else Some(1000L)
        val materializedSequenceNumber =
          if (status == Tracking.Status.Added) None else Some(76L)
        val materializedSnapshotId =
          if (status == Tracking.Status.Added) None else Some(75L)
        val leaf = tree.writeLeaf(Seq(
          dataEntry("data/a.parquet", recordCount = 10L,
            tracking = unassignedTracking(status).copy(
              snapshot_id = materializedSnapshotId,
              sequence_number = materializedSequenceNumber,
              file_sequence_number = Some(materialized),
              first_row_id = materializedFirstRowId))))
        val files = tree.reconstruct(Seq(
          leafPointer(leaf).wrap))
        assert(files("data/a.parquet").defaultRowCommitVersion.contains(materialized),
          s"${Tracking.Status.nameOf(status)} must keep its materialized value")
      }
    }
  }

  test("a materialized file_sequence_number on a root-resident entry is read without inheriting") {
    Tracking.Status.liveEntryStatuses.foreach { status =>
      withSyntheticTree(s"amt_materialized_root_${Tracking.Status.nameOf(status)}") { tree =>
        val materialized = 55L
        val files = tree.reconstruct(Seq(
          dataEntry("data/inline.parquet", recordCount = 4L,
            tracking = unassignedTracking(status).copy(file_sequence_number = Some(materialized)))
            .wrap))
        assert(files("data/inline.parquet").defaultRowCommitVersion.contains(materialized),
          s"${Tracking.Status.nameOf(status)} must keep its materialized value")
      }
    }
  }
}
