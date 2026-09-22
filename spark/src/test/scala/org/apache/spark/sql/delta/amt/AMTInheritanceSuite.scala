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

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for the Iceberg V4 tracking inheritance rules.
 */
class AMTInheritanceSuite extends SparkFunSuite {

  /** The statuses an entry can carry that are not `ADDED`. */
  private val nonAddedStatuses: Seq[Int] = (Tracking.Status.all - Tracking.Status.Added).toSeq

  /** A root `DATA_MANIFEST` parent that declares a file sequence number. */
  private val fullParent = InheritableTracking(file_sequence_number = Some(42L))

  private def childTracking(
      status: Int = Tracking.Status.Added,
      snapshotId: Option[Long] = None,
      dvSnapshotId: Option[Long] = None,
      sequenceNumber: Option[Long] = None,
      fileSequenceNumber: Option[Long] = None,
      firstRowId: Option[Long] = None,
      deletedPositions: Option[Array[Byte]] = None,
      replacedPositions: Option[Array[Byte]] = None): Tracking = Tracking(
    status = status,
    snapshot_id = snapshotId,
    dv_snapshot_id = dvSnapshotId,
    sequence_number = sequenceNumber,
    file_sequence_number = fileSequenceNumber,
    first_row_id = firstRowId,
    deleted_positions = deletedPositions,
    replaced_positions = replacedPositions)

  private def resolve(
      child: Tracking,
      parent: InheritableTracking = fullParent): Tracking =
    Tracking.resolve(child, parent, childEntryLocationForLogging = "data/part-0.parquet")

  /** A root `DATA_MANIFEST` entry carrying the given tracking. */
  private def parentPointer(tracking: Tracking): DataManifestEntry = DataManifestEntry(
    location = "_delta_log/_amt/leaf-0.parquet",
    file_format = AMTSingleAction.FileFormatParquet,
    tracking = tracking,
    record_count = 1L,
    file_size_in_bytes = 1L,
    manifest_info = ManifestInfo(
      added_files_count = 1,
      existing_files_count = 0,
      deleted_files_count = 0,
      replaced_files_count = 0,
      modified_files_count = 0,
      added_rows_count = 1L,
      existing_rows_count = 0L,
      deleted_rows_count = 0L,
      replaced_rows_count = 0L,
      modified_rows_count = 0L,
      min_sequence_number = 0L,
      dv = None,
      dv_cardinality = None))

  test("an ADDED entry with a null file_sequence_number inherits it from its parent") {
    val resolved = resolve(childTracking())
    assert(resolved.file_sequence_number.contains(42L))
    assert(resolved.sequence_number.isEmpty)
  }

  test("a materialized child file_sequence_number is kept in preference to the parent's") {
    val resolved = resolve(childTracking(fileSequenceNumber = Some(3L)))
    assert(resolved.file_sequence_number.contains(3L))
  }

  test("file_sequence_number is inherited only by ADDED entries") {
    val added = resolve(childTracking(status = Tracking.Status.Added))
    assert(added.file_sequence_number.contains(42L))
    nonAddedStatuses.foreach { status =>
      val resolved = resolve(childTracking(status = status, fileSequenceNumber = Some(8L)))
      assert(resolved.file_sequence_number.contains(8L))
    }
  }

  test("status and the CDF position bitmaps pass through untouched") {
    val deleted = Array[Byte](1, 2, 3)
    val replaced = Array[Byte](4, 5)
    val resolved = resolve(childTracking(
      status = Tracking.Status.Added,
      deletedPositions = Some(deleted),
      replacedPositions = Some(replaced)))
    assert(resolved.status == Tracking.Status.Added)
    assert(resolved.deleted_positions.contains(deleted))
    assert(resolved.replaced_positions.contains(replaced))
  }

  test("a parent that declares nothing leaves the child unchanged") {
    assert(InheritableTracking.none.isEmpty)
    Tracking.Status.all.foreach { status =>
      val child = childTracking(status = status)
      assert(resolve(child, InheritableTracking.none) == child)
    }
  }

  test("a null file_sequence_number on a non-ADDED entry is rejected") {
    nonAddedStatuses.foreach { status =>
      val error = intercept[IllegalStateException] {
        resolve(childTracking(status = status))
      }
      assert(error.getMessage.contains("tracking.file_sequence_number is null"))
      assert(error.getMessage.contains(Tracking.Status.nameOf(status)))
      assert(error.getMessage.contains("data/part-0.parquet"))
    }
  }

  test("InheritableTracking projects only the inheritable fields of a parent") {
    val parent = parentPointer(Tracking(
      status = Tracking.Status.Existing,
      snapshot_id = Some(7L),
      dv_snapshot_id = Some(8L),
      sequence_number = Some(42L),
      file_sequence_number = Some(43L),
      first_row_id = Some(1000L),
      deleted_positions = Some(Array[Byte](1)),
      replaced_positions = Some(Array[Byte](2))))
    assert(InheritableTracking(parent) == InheritableTracking(file_sequence_number = Some(43L)))
  }
}
