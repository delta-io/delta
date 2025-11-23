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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Abstraction layer over Delta Protocol and Metadata hide implementation details of
 * Protocol and Metadata classes, enabling DeltaParquetFileFormat to be reused without depending on
 * specific action class implementations.
 * This helps delta kernel based connector reusing DeltaParquetFileFormat.
 */
trait ProtocolMetadataAdapter {

  /**
   * Returns the column mapping mode for this table.
   */
  def columnMappingMode: DeltaColumnMappingMode

  /**
   * Returns the logical schema of the table.
   */
  def getReferenceSchema: StructType

  /**
   * Returns whether Row IDs(Row tracking) are enabled on this table.
   */
  def isRowIdEnabled: Boolean

  /**
   * Returns whether Deletion Vectors are readable on this table.
   */
  def isDeletionVectorReadable: Boolean

  /**
   * Returns whether any version of IcebergCompat is enabled on this table.
   */
  def isIcebergCompatAnyEnabled: Boolean

  /**
   * Returns whether IcebergCompat is enabled at or above the specified version.
   * @param version The IcebergCompat version to check (e.g., 2, 3)
   */
  def isIcebergCompatGeqEnabled(version: Int): Boolean

  /**
   * Asserts that the table is readable given the current configuration.
   * Throws an exception if the table cannot be read.
   *
   * @param sparkSession The current Spark session
   */
  def assertTableReadable(sparkSession: SparkSession): Unit

  /**
   * Creates the metadata struct fields for row tracking.
   *
   * @param nullableRowTrackingConstantFields whether constant fields should be nullable
   * @param nullableRowTrackingGeneratedFields whether generated fields should be nullable
   * @return metadata fields for row tracking (_metadata.row_id, _metadata.base_row_id, etc.)
   */
  def createRowTrackingMetadataFields(
      nullableRowTrackingConstantFields: Boolean,
      nullableRowTrackingGeneratedFields: Boolean): Iterable[StructField]

}

