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

import scala.reflect.runtime.universe._

import org.apache.spark.sql.delta.actions.{Action, AddFile}

import org.apache.spark.sql.functions.lit

/**
 * Pins the fields of [[AddFile]] and of the AMT types the DataEntry -> AddFile conversion reads,
 * so that adding or removing a field fails here. Each expected map notes how the conversion
 * handles the field, or why it is dropped or left null.
 *
 * Reflection only sees constructor parameters, so a field added to a case class is caught even
 * when nothing else in the test refers to it. `addFileColumn` is also called for every
 * [[AddFile]] field, so a field it has no mapping for fails here rather than at read time.
 */
class AMTAddFileFieldCoverageSuite extends AMTSyntheticTreeTestBase {

  private val updateInstructions =
    "Revisit addFileColumn in AMTCheckpointProvider and DataEntry.toAddFile, decide " +
      "whether each added or removed field needs a transformation in the conversion, then " +
      "update the expected map in AMTAddFileFieldCoverageSuite."

  private val expectedAddFileFields: Map[String, String] = Map(
    "path" -> "DataEntry.location, URL-encoded (encodePath / toAddFile)",
    "partitionValues" -> "DataEntry.partition, normalized via the metadata partition schema",
    "size" -> "DataEntry.file_size_in_bytes",
    "modificationTime" -> "always 0L",
    "dataChange" -> "always false",
    "stats" -> "content_stats, record_count and tight_stats rendered as Delta stats JSON",
    "tags" -> "DataEntry.tags",
    "deletionVector" -> "DataEntry.deletion_vector converted to a DeletionVectorDescriptor",
    "baseRowId" -> "tracking.first_row_id, inherited by ADDED leaf entries",
    "defaultRowCommitVersion" -> "tracking.file_sequence_number, inherited by ADDED leaf entries",
    "clusteringProvider" -> "null, AMT entries carry no clustering provider",
    "backReference" -> "built from the leaf location and entry position, not a DataEntry field",
    "amtPassthrough" -> "spec_id, sort_order_id, key_metadata, split_offsets (AMTPassthrough)",
    "snapshotId" -> "tracking.snapshot_id, inherited by ADDED leaf entries",
    "modifiedSnapshotId" -> "tracking.modified_snapshot_id, never inherited"
  )

  private val expectedDataEntryFields: Map[String, String] = Map(
    "location" -> "AddFile.path, URL-encoded",
    "file_format" -> "not carried; must be parquet for the AMTPassthrough and column builds",
    "tracking" -> "see Tracking below",
    "record_count" -> "stats only (numRecords in the stats JSON)",
    "file_size_in_bytes" -> "AddFile.size",
    "partition" -> "AddFile.partitionValues",
    "deletion_vector" -> "AddFile.deletionVector, see DeletionVector below",
    "spec_id" -> "AMTPassthrough.spec_id",
    "sort_order_id" -> "AMTPassthrough.sort_order_id",
    "content_stats" -> "AddFile.stats",
    "tight_stats" -> "AddFile.stats (tightBounds)",
    "key_metadata" -> "AMTPassthrough.key_metadata",
    "split_offsets" -> "AMTPassthrough.split_offsets",
    "tags" -> "AddFile.tags"
  )

  private val expectedTrackingFields: Map[String, String] = Map(
    "status" -> "not an AddFile field; drives ADDED inheritance and the malformed-row checks",
    "snapshot_id" -> "AddFile.snapshotId",
    "modified_snapshot_id" -> "AddFile.modifiedSnapshotId",
    "sequence_number" -> "not an AddFile field; required by the entry checks only",
    "file_sequence_number" -> "AddFile.defaultRowCommitVersion",
    "first_row_id" -> "AddFile.baseRowId",
    "deleted_positions" -> "not an AddFile field; read by CDF via changedPositionsBitmaps",
    "replaced_positions" -> "not an AddFile field; read by CDF via changedPositionsBitmaps"
  )

  private val expectedDeletionVectorFields: Map[String, String] = Map(
    "location" -> "descriptor path, absolute or relative to the table root",
    "offset" -> "descriptor offset",
    "size_in_bytes" -> "descriptor sizeInBytes, after removing the DV file header bytes",
    "cardinality" -> "descriptor cardinality"
  )

  private val expectedAMTPassthroughFields: Map[String, String] = Map(
    "spec_id" -> "copied from DataEntry.spec_id",
    "sort_order_id" -> "copied from DataEntry.sort_order_id",
    "key_metadata" -> "copied from DataEntry.key_metadata",
    "split_offsets" -> "copied from DataEntry.split_offsets"
  )

  test("addFileColumn maps every AddFile field") {
    withSyntheticTree("amt_add_file_column_fields") { tree =>
      val provider = tree.provider(Seq.empty)
      // `addFileColumn` has no fallback case, so an unmapped field throws a MatchError.
      val unmapped = Action.addFileSchema.fields.toSeq.filter { field =>
        try {
          provider.addFileColumn(spark, field, lit(0L))
          false
        } catch {
          case _: MatchError => true
        }
      }.map(_.name)
      assert(unmapped.isEmpty,
        "addFileColumn has no mapping for AddFile fields " + unmapped.mkString("[", ", ", "]") +
          ". " + updateInstructions)
    }
  }

  test("AddFile fields are all reviewed for the AMT conversion") {
    assertReviewed("AddFile", typeOf[AddFile], expectedAddFileFields)
  }

  test("DataEntry fields are all reviewed for the AMT conversion") {
    assertReviewed("DataEntry", typeOf[DataEntry], expectedDataEntryFields)
  }

  test("Tracking fields are all reviewed for the AMT conversion") {
    assertReviewed("Tracking", typeOf[Tracking], expectedTrackingFields)
  }

  test("DeletionVector fields are all reviewed for the AMT conversion") {
    assertReviewed("DeletionVector", typeOf[DeletionVector], expectedDeletionVectorFields)
  }

  test("AMTPassthrough fields are all reviewed for the AMT conversion") {
    assertReviewed("AMTPassthrough", typeOf[AMTPassthrough], expectedAMTPassthroughFields)
  }

  private def assertReviewed(typeName: String, tpe: Type, reviewed: Map[String, String]): Unit = {
    val actual = constructorFields(tpe)
    val added = actual.filterNot(reviewed.contains).sorted
    val removed = reviewed.keys.filterNot(actual.contains).toSeq.sorted
    if (added.nonEmpty || removed.nonEmpty) {
      fail(
        typeName + " fields changed. Added: " + added.mkString("[", ", ", "]") +
          ". Removed: " + removed.mkString("[", ", ", "]") + ". " + updateInstructions)
    }
  }

  private def constructorFields(tpe: Type): Seq[String] =
    tpe.decl(termNames.CONSTRUCTOR).asMethod.paramLists.flatten.map(_.name.decodedName.toString)
}
