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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Implementation of DeltaWrapper that wraps the existing Spark Delta actions.
 *
 * This is used by delta-spark connector and provides backward compatibility
 * with existing code.
 */
case class DeltaWrapperV1(
    protocol: Protocol,
    metadata: Metadata) extends DeltaWrapper {

  override def columnMappingMode(): DeltaColumnMappingMode =
      metadata.columnMappingMode

  override def getReferenceSchema(): StructType =
      metadata.schema

  override def isRowIdEnabled(): Boolean =
      RowId.isEnabled(protocol, metadata)

  override def isDeletionVectorReadable(): Boolean =
      DeletionVectorUtils.deletionVectorsReadable(protocol, metadata)

  override def isIcebergCompatEnabled(): Boolean =
      IcebergCompat.isAnyEnabled(metadata)

  override def isIcebergCompatV2OrAbove(): Boolean =
      IcebergCompat.isGeqEnabled(metadata, 2)

  override def validateTableReadableForTypeWidening(conf: SQLConf): Unit =
      TypeWidening.assertTableReadable(conf, protocol, metadata)

  override def createRowTrackingMetadataFields(
      nullableConstantFields: Boolean,
      nullableGeneratedFields: Boolean): Iterable[StructField] =
      RowTracking.createMetadataStructFields(
          protocol, metadata, nullableConstantFields, nullableGeneratedFields)
}

