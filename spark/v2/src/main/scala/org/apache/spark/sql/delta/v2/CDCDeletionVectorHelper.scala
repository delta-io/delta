/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package org.apache.spark.sql.delta.v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.delta.actions.{DeletionVectorDescriptor => SparkDvDescriptor}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore

/**
 * Helper for DV-diff operations needed by DSv2 CDC streaming. Lives in `org.apache.spark.sql.delta`
 * to access `private[delta]` APIs (DeletionVectorStore, DeletionVectorUtils, etc.) needed for
 * computing DV bitmap diffs.
 */
object CDCDeletionVectorHelper {

  /**
   * Compute the DV diff for a same-path Add+Remove pair and return serialized inline DV
   * descriptors (base64 strings) for the changed rows.
   *
   * @param addDvBase64 base64-serialized DV descriptor for the AddFile (null if no DV)
   * @param removeDvBase64 base64-serialized DV descriptor for the RemoveFile (null if no DV)
   * @param hadoopConf Hadoop configuration for reading DV files
   * @param tablePath data path of the table
   * @return array of (changeType, dvBase64) pairs: "delete" for newly deleted rows,
   *         "insert" for re-added rows
   */
  def computeDVDiff(
      addDvBase64: String,
      removeDvBase64: String,
      hadoopConf: Configuration,
      tablePath: String): Array[(String, String)] = {
    val addDv = if (addDvBase64 != null) {
      Some(SparkDvDescriptor.deserializeFromBase64(addDvBase64))
    } else None
    val removeDv = if (removeDvBase64 != null) {
      Some(SparkDvDescriptor.deserializeFromBase64(removeDvBase64))
    } else None

    require(addDv.isDefined || removeDv.isDefined,
      s"Same-path Add+Remove pair must have at least one DV, tablePath: $tablePath")

    val dvStore = DeletionVectorStore.createInstance(hadoopConf)
    val dataPath = new Path(tablePath)
    val results = scala.collection.mutable.ArrayBuffer[(String, String)]()

    (removeDv, addDv) match {
      case (None, Some(aDv)) =>
        // Remove without DV, Add with DV -> rows in addDv are deleted
        results += (("delete", readAndSerializeInline(dvStore, aDv, dataPath)))

      case (Some(rDv), None) =>
        // Remove with DV, Add without DV -> rows in removeDv are re-added
        results += (("insert", readAndSerializeInline(dvStore, rDv, dataPath)))

      case (Some(rDv), Some(aDv)) =>
        // Both have DVs -> compute bitmap diff
        val removeBitmap = dvStore.read(rDv, dataPath)
        val addBitmap = dvStore.read(aDv, dataPath)

        // Rows in addDv but not removeDv are newly deleted
        val deletedRows = addBitmap.copy()
        deletedRows.diff(removeBitmap)
        if (!deletedRows.isEmpty) {
          results += (("delete", serializeInline(deletedRows, dataPath)))
        }

        // Rows in removeDv but not addDv are re-added (table restore case)
        val readdedRows = removeBitmap.copy()
        readdedRows.diff(addBitmap)
        if (!readdedRows.isEmpty) {
          results += (("insert", serializeInline(readdedRows, dataPath)))
        }

      case (None, None) =>
        throw new IllegalStateException("Unreachable: both DVs are None")
    }

    results.toArray
  }

  private def readAndSerializeInline(
      dvStore: DeletionVectorStore,
      sparkDv: SparkDvDescriptor,
      dataPath: Path): String = {
    val bitmap = dvStore.read(sparkDv, dataPath)
    serializeInline(bitmap, dataPath)
  }

  private def serializeInline(bitmap: RoaringBitmapArray, dataPath: Path): String = {
    val bytes = DeletionVectorUtils.serialize(
      bitmap, RoaringBitmapArrayFormat.Portable, Some(dataPath))
    SparkDvDescriptor.inlineInLog(bytes, bitmap.cardinality).serializeToBase64
  }
}
