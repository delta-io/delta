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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Abstraction over Delta table protocol and metadata.
 *
 * This trait allows kernel-spark connector to provide its own implementation
 * without depending on spark's actions.Protocol/Metadata classes.
 *
 * The main use case is to enable DeltaParquetFileFormat to work with both:
 * - delta-spark: using org.apache.spark.sql.delta.actions.{Protocol, Metadata}
 * - kernel-spark: using io.delta.kernel.internal.actions.{Protocol, Metadata}
 */
trait DeltaWrapper extends Serializable {

  /**
   * Returns the column mapping mode for this Delta table.
   */
  def columnMappingMode(): DeltaColumnMappingMode

  /**
   * Returns the logical schema of the Delta table.
   */
  def getReferenceSchema(): StructType

  /**
   * Returns true if Row Tracking is enabled for this table.
   */
  def isRowIdEnabled(): Boolean

  /**
   * Returns true if Deletion Vectors are readable for this table.
   */
  def isDeletionVectorReadable(): Boolean

  /**
   * Returns true if Iceberg compatibility is enabled.
   */
  def isIcebergCompatEnabled(): Boolean

  /**
   * Returns true if Iceberg compatibility V2 or above is enabled.
   */
  def isIcebergCompatV2OrAbove(): Boolean

  /**
   * Validates that the table is readable given the current Spark configuration
   * and type widening settings.
   *
   * @throws IllegalStateException if table is not readable
   */
  def validateTableReadableForTypeWidening(conf: SQLConf): Unit

  /**
   * Creates the metadata struct fields for row tracking.
   *
   * @param nullableConstantFields whether constant fields should be nullable
   * @param nullableGeneratedFields whether generated fields should be nullable
   * @return metadata fields for row tracking (_metadata.row_id, etc.)
   */
  def createRowTrackingMetadataFields(
      nullableConstantFields: Boolean,
      nullableGeneratedFields: Boolean): Iterable[StructField]
}

