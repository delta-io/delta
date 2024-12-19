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
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.files.SupportsRowIndexFilters
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.sql.functions.col
import org.apache.spark.util.Utils

trait DeletionVectorUtils extends DeltaLogging {

  /**
   * Run a query on the delta log to determine if the given snapshot contains no deletion vectors.
   * Return `false` if it does contain deletion vectors.
   */
  def isTableDVFree(snapshot: Snapshot): Boolean = {
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
   * Serializes `bitmap` into a byte array using `serializationFormat`. If it fails, it records a
   * delta event and re-throws the exception.
   */
  def serialize(
      bitmap: RoaringBitmapArray,
      serializationFormat: RoaringBitmapArrayFormat.Value,
      tablePath: Option[Path] = None,
      debugInfo: Map[String, Any] = Map.empty): Array[Byte] = {
    try {
      bitmap.serializeAsByteArray(serializationFormat)
    } catch {
      case e: Exception =>
        recordDeltaEvent(
          deltaLog = null,
          opType = "delta.assertions.deletionVectorSerializationError",
          data = debugInfo ++ Map(
            "serializationFormat" -> serializationFormat,
            "cardinality" -> bitmap.cardinality,
            "errorMsg" -> e.getMessage,
            "errorStackTrace" -> e.getStackTrace),
          path = tablePath)
        throw e
    }
  }

  /**
   * Deserializes a RoaringBitmapArray from `bytes`. If it fails, it records a delta event and
   * re-throws the exception.
   */
  def deserialize(
      bytes: Array[Byte],
      tablePath: Option[Path] = None,
      debugInfo: Map[String, Any] = Map.empty): RoaringBitmapArray = {
    try {
      RoaringBitmapArray.readFrom(bytes)
    } catch {
      case e: Exception =>
        recordDeltaEvent(
          deltaLog = null,
          "delta.assertions.deletionVectorDeserializationError",
          data = debugInfo ++ Map(
            "errorMsg" -> e.getMessage,
            "errorStackTrace" -> e.getStackTrace),
          path = tablePath)
        throw e
    }
  }
}

// To access utilities from places where mixing in a trait is inconvenient.
object DeletionVectorUtils extends DeletionVectorUtils
