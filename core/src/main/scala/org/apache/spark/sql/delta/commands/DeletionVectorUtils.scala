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

package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.delta.{DeletionVectorsTableFeature, DeltaConfigs, Snapshot, SnapshotDescriptor}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.files.SupportsRowIndexFilters
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.functions.col
import org.apache.spark.util.Utils

trait DeletionVectorUtils {

  /**
   * Run a query on the delta log to determine if the given snapshot contains no deletion vectors.
   * Return `false` if it does contain deletion vectors.
   */
  def isTableDVFree(spark: SparkSession, snapshot: Snapshot): Boolean = {
    val dvsReadable = deletionVectorsReadable(snapshot)

    if (dvsReadable) {
      val dvCount = snapshot.allFiles
        .filter(col("deletionVector").isNotNull)
        .limit(1)
        .count()

      dvCount == 0L
    } else {
      true
    }
  }

  /**
   * Returns true if persistent deletion vectors are enabled and
   * readable with the current reader version.
   */
  def fileIndexSupportsReadingDVs(fileIndex: FileIndex): Boolean = fileIndex match {
    case index: TahoeFileIndex => deletionVectorsReadable(index)
    case _: SupportsRowIndexFilters => true
    case _ => false
  }

  def deletionVectorsWritable(
      snapshot: SnapshotDescriptor,
      newProtocol: Option[Protocol] = None,
      newMetadata: Option[Metadata] = None): Boolean =
    deletionVectorsWritable(
      protocol = newProtocol.getOrElse(snapshot.protocol),
      metadata = newMetadata.getOrElse(snapshot.metadata))

  def deletionVectorsWritable(protocol: Protocol, metadata: Metadata): Boolean =
    protocol.isFeatureSupported(DeletionVectorsTableFeature) &&
      DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.fromMetaData(metadata)

  def deletionVectorsReadable(
      snapshot: SnapshotDescriptor,
      newProtocol: Option[Protocol] = None,
      newMetadata: Option[Metadata] = None): Boolean = {
    deletionVectorsReadable(
      newProtocol.getOrElse(snapshot.protocol),
      newMetadata.getOrElse(snapshot.metadata))
  }

  def deletionVectorsReadable(
      protocol: Protocol,
      metadata: Metadata): Boolean = {
    protocol.isFeatureSupported(DeletionVectorsTableFeature) &&
      metadata.format.provider == "parquet" // DVs are only supported on parquet tables.
  }

  /**
   * Utility method that checks the table has no Deletion Vectors enabled. Deletion vectors
   * are supported in read-only mode for now. Any updates to tables with deletion vectors
   * feature are disabled until we add support.
   */
  def assertDeletionVectorsNotReadable(
      spark: SparkSession, metadata: Metadata, protocol: Protocol): Unit = {
    val disable =
      Utils.isTesting && // We are in testing and enabled blocking updates on DV tables
          spark.conf.get(DeltaSQLConf.DELTA_ENABLE_BLOCKING_UPDATES_ON_DV_TABLES)
    if (!disable && deletionVectorsReadable(protocol, metadata)) {
      throw new UnsupportedOperationException(
        "Updates to tables with Deletion Vectors feature enabled are not supported in " +
          "this version of Delta Lake.")
    }
  }

  /**
   * Utility method that checks the table metadata has no deletion vectors enabled. Deletion vectors
   * are supported in read-only mode for now. Any updates to metadata to enable deletion vectors are
   * blocked until we add support.
   */
  def assertDeletionVectorsNotEnabled(
    spark: SparkSession, metadata: Metadata, protocol: Protocol): Unit = {
    val disable =
      Utils.isTesting && // We are in testing and enabled blocking updates on DV tables
        spark.conf.get(DeltaSQLConf.DELTA_ENABLE_BLOCKING_UPDATES_ON_DV_TABLES)
    if (!disable &&
      (protocol.isFeatureSupported(DeletionVectorsTableFeature) ||
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.fromMetaData(metadata)
      )
    ) {
      throw new UnsupportedOperationException(
        "Enabling Deletion Vectors on the table is not supported in this version of Delta Lake.")
    }
  }
}

// To access utilities from places where mixing in a trait is inconvenient.
object DeletionVectorUtils extends DeletionVectorUtils
