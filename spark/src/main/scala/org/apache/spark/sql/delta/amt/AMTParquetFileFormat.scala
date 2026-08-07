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

import org.apache.spark.sql.delta.{DeltaColumnMappingMode, DeltaParquetFileFormatBase, NoMapping, ProtocolMetadataAdapter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Parquet format for AMT manifest scans that materializes a manifest-deletion-vector (MDV) skip
 * column via [[DeltaParquetFileFormatBase]].
 *
 * The per-leaf inline MDV is carried as scan metadata under the same
 * `row_index_filter_id_encoded` / `row_index_filter_type` keys that data-file DVs use, so the base
 * deserializes it once per file and materializes the `__delta_internal_is_row_deleted` column; the
 * AMT read then drops masked rows with a filter on top.
 *
 * @param tableRootPath Table root. Required by the base for deletion-vector processing; for inline
 *                      manifest DVs the bytes travel in the descriptor so the path is not read, but
 *                      the base requires it to be present for non-empty DVs.
 */
final class AMTParquetFileFormat(val tableRootPath: String)
  extends DeltaParquetFileFormatBase(
    protocolMetadataAdapter = AMTParquetFileFormat.Adapter,
    optimizationsEnabled = false,
    tablePath = Some(tableRootPath),
    useMetadataRowIndexOpt = Some(false)) {

  override def equals(other: Any): Boolean = other match {
    case that: AMTParquetFileFormat => that.tableRootPath == this.tableRootPath
    case _ => false
  }

  override def hashCode(): Int = tableRootPath.hashCode

  override def shortName(): String = "amt-manifest-parquet"
}

object AMTParquetFileFormat {
  private object Adapter extends ProtocolMetadataAdapter {
    override def columnMappingMode: DeltaColumnMappingMode = NoMapping

    override def getReferenceSchema: StructType = new StructType()

    override def isRowIdEnabled: Boolean = false

    override def isDeletionVectorReadable: Boolean = true

    override def isIcebergCompatAnyEnabled: Boolean = false

    override def isIcebergCompatGeqEnabled(version: Int): Boolean = false

    override def assertTableReadable(sparkSession: SparkSession): Unit = ()

    override def createRowTrackingMetadataFields(
        nullableRowTrackingConstantFields: Boolean,
        nullableRowTrackingGeneratedFields: Boolean): Iterable[StructField] = Iterable.empty

  }
}
