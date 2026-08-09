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

import java.util.{ArrayList => JArrayList, List => JList}
import javax.annotation.Nullable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.delta.actions.{DeletionVectorDescriptor => SparkDvDescriptor}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore

import io.delta.kernel.internal.actions.{AddFile, RemoveFile}
import io.delta.spark.internal.v2.read.CDCDataFile

/**
 * Helper for DV-diff operations needed by DSv2 CDC streaming. Lives in the
 * `org.apache.spark.sql.delta.v2` package (Scala) because it needs access to `private[delta]`
 * APIs (DeletionVectorStore, RoaringBitmapArray, DeletionVectorUtils) that are not exposed to
 * Java.
 */
object CDCDeletionVectorHelper {

  /**
   * Computes the CDC diff between an AddFile's DV and a RemoveFile's DV for a same-path pair.
   *
   * @param addDvBase64    base64 of the AddFile's DV descriptor, or null if no DV.
   * @param removeDvBase64 base64 of the RemoveFile's DV descriptor, or null if no DV.
   * @param hadoopConf     Hadoop conf used to construct a DV store for any on-disk DV reads.
   * @param tablePath      table data path; used when resolving on-disk DV files and as a
   *                       diagnostic in the require() message.
   * @return array of `(changeType, dvBase64)` pairs describing the bitmap diff. At least one of
   *         `addDvBase64` or `removeDvBase64` must be non-null.
   */
  def computeDVDiff(
      @Nullable addDvBase64: String,
      @Nullable removeDvBase64: String,
      hadoopConf: Configuration,
      tablePath: String): Array[(String, String)] = {
    computeDVDiffInternal(Option(addDvBase64), Option(removeDvBase64), hadoopConf, tablePath)
  }

  /**
   * Produces the CDCDataFiles for a same-path Add+Remove pair by computing the DV bitmap diff.
   * Returns one or two CDCDataFiles depending on which diff entries are non-empty.
   */
  def generateDVDiffCDCFiles(
      add: AddFile,
      remove: RemoveFile,
      commitTimestamp: Long,
      dataPath: String,
      hadoopConf: Configuration): JList[CDCDataFile] = {
    val addDvBase64: String =
      if (add.getDeletionVector.isPresent) add.getDeletionVector.get.serializeToBase64 else null
    val removeDvBase64: String =
      if (remove.getDeletionVector.isPresent) remove.getDeletionVector.get.serializeToBase64
      else null

    val diffs = computeDVDiff(addDvBase64, removeDvBase64, hadoopConf, dataPath)
    val out = new JArrayList[CDCDataFile](diffs.length)
    diffs.foreach { case (changeType, dvBase64) =>
      out.add(CDCDataFile.fromDVDiff(add, changeType, commitTimestamp, dvBase64))
    }
    out
  }

  private def computeDVDiffInternal(
      addDvOpt: Option[String],
      removeDvOpt: Option[String],
      hadoopConf: Configuration,
      tablePath: String): Array[(String, String)] = {
    require(addDvOpt.isDefined || removeDvOpt.isDefined,
      s"Same-path Add+Remove pair must have at least one DV, tablePath: $tablePath")
    val addDv = addDvOpt.map(SparkDvDescriptor.deserializeFromBase64)
    val removeDv = removeDvOpt.map(SparkDvDescriptor.deserializeFromBase64)

    val dvStore = DeletionVectorStore.createInstance(hadoopConf)
    val dataPath = new Path(tablePath)
    val results = scala.collection.mutable.ArrayBuffer[(String, String)]()

    (removeDv, addDv) match {
      case (None, Some(aDv)) =>
        // Remove without DV, Add with DV -> rows in addDv are deleted.
        // Skip disk read if the DV is already inline.
        val dvBase64 = if (aDv.isInline) addDvOpt.get
                       else readAndSerializeInline(dvStore, aDv, dataPath)
        results += ((CDCReader.CDC_TYPE_DELETE_STRING, dvBase64))

      case (Some(rDv), None) =>
        // Remove with DV, Add without DV -> rows in removeDv are re-added.
        // Skip disk read if the DV is already inline.
        val dvBase64 = if (rDv.isInline) removeDvOpt.get
                       else readAndSerializeInline(dvStore, rDv, dataPath)
        results += ((CDCReader.CDC_TYPE_INSERT, dvBase64))

      case (Some(rDv), Some(aDv)) =>
        // Both have DVs -> compute bitmap diff
        val removeBitmap = dvStore.read(rDv, dataPath)
        val addBitmap = dvStore.read(aDv, dataPath)

        // Rows in addDv but not removeDv are newly deleted
        val deletedRows = addBitmap.copy()
        deletedRows.diff(removeBitmap)
        if (!deletedRows.isEmpty) {
          results += ((CDCReader.CDC_TYPE_DELETE_STRING, serializeInline(deletedRows, dataPath)))
        }

        // Rows in removeDv but not addDv are re-added (table restore case)
        val readdedRows = removeBitmap.copy()
        readdedRows.diff(addBitmap)
        if (!readdedRows.isEmpty) {
          results += ((CDCReader.CDC_TYPE_INSERT, serializeInline(readdedRows, dataPath)))
        }

      case (None, None) => // unreachable - guarded by require() above
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
