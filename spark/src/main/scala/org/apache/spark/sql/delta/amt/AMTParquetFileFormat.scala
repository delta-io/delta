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

import org.apache.spark.sql.catalyst.expressions.FileSourceConstantMetadataStructField
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{BinaryType, MetadataBuilder, StructField}

/**
 * Parquet format for AMT manifest scans that exposes each leaf's inline manifest deletion
 * vector (MDV) as a file-constant `_metadata` column, mirroring how
 * [[org.apache.spark.sql.delta.DeltaParquetFileFormat]] exposes inline file deletion vectors via
 * per-file metadata.
 */
final class AMTParquetFileFormat extends ParquetFileFormat {

  override def metadataSchemaFields: Seq[StructField] =
    super.metadataSchemaFields ++ Seq(AMTParquetFileFormat.ManifestDvBytesMetadataStructField())
}

object AMTParquetFileFormat {

  /** Key in each scan task's `PartitionedFile.otherConstantMetadataColumnValues`. */
  val MANIFEST_DV_BYTES = "manifest_dv_bytes"

  lazy val INSTANCE: AMTParquetFileFormat = new AMTParquetFileFormat

  private object ManifestDvBytesMetadataStructField {
    private val METADATA_COL_ATTR_KEY = "__manifest_dv_bytes_metadata_col"

    def apply(): StructField =
      StructField(
        MANIFEST_DV_BYTES,
        BinaryType,
        nullable = true,
        metadata = new MetadataBuilder()
          .withMetadata(FileSourceConstantMetadataStructField.metadata(MANIFEST_DV_BYTES))
          .putBoolean(METADATA_COL_ATTR_KEY, value = true)
          .build())
  }
}
