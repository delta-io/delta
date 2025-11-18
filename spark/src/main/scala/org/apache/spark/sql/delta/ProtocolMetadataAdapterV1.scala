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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Implementation of ProtocolMetadataAdapter for delta-spark v1 Protocol and Metadata.
 *
 * This class adapts the existing delta-spark Protocol and Metadata actions to the
 * ProtocolMetadataAdapter interface, enabling code reuse in DeltaParquetFileFormat.
 */
case class ProtocolMetadataAdapterV1(
    protocol: Protocol,
    metadata: Metadata) extends ProtocolMetadataAdapter {

  override def columnMappingMode: DeltaColumnMappingMode = metadata.columnMappingMode

  override def getReferenceSchema: StructType = metadata.schema

  override def isRowIdEnabled: Boolean = RowId.isEnabled(protocol, metadata)

  override def isDeletionVectorReadable: Boolean =
    DeletionVectorUtils.deletionVectorsReadable(protocol, metadata)

  override def isIcebergCompatAnyEnabled: Boolean = IcebergCompat.isAnyEnabled(metadata)

  override def isIcebergCompatGeqEnabled(version: Int): Boolean =
    IcebergCompat.isGeqEnabled(metadata, version)

  override def assertTableReadable(sparkSession: SparkSession): Unit = {
    TypeWidening.assertTableReadable(sparkSession.sessionState.conf, protocol, metadata)
  }

  override def createRowTrackingMetadataFields(
      nullableRowTrackingConstantFields: Boolean,
      nullableRowTrackingGeneratedFields: Boolean): Iterable[StructField] = {
    RowTracking.createMetadataStructFields(
      protocol,
      metadata,
      nullableConstantFields = nullableRowTrackingConstantFields,
      nullableGeneratedFields = nullableRowTrackingGeneratedFields)
  }

}

