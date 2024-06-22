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

package org.apache.spark.sql.delta.commands.convert

import org.apache.spark.sql.delta.DeltaColumnMapping
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.iceberg.Schema
import org.apache.iceberg.spark.SparkSchemaUtil

import org.apache.spark.sql.types.{MetadataBuilder, StructType}

object IcebergSchemaUtils {

  /**
   * Given an iceberg schema, convert it to a Spark schema. This conversion will keep the Iceberg
   * column IDs (used to read Parquet files) in the field metadata
   *
   * @param icebergSchema
   * @return StructType for the converted schema
   */
  def convertIcebergSchemaToSpark(icebergSchema: Schema): StructType = {
    // Convert from Iceberg schema to Spark schema but without the column IDs
    val baseConvertedSchema = SparkSchemaUtil.convert(icebergSchema)

    // For each field, find the column ID (fieldId) and add to the StructField metadata
    SchemaMergingUtils.transformColumns(baseConvertedSchema) { (path, field, _) =>
      // This should be safe to access fields
      // scalastyle:off
      // https://github.com/apache/iceberg/blob/d98224a82b104888281d4e901ccf948f9642590b/api/src/main/java/org/apache/iceberg/types/IndexByName.java#L171
      // scalastyle:on
      val fieldPath = (path :+ field.name).mkString(".")
      val id = icebergSchema.findField(fieldPath).fieldId()
      field.copy(
        metadata = new MetadataBuilder()
          .withMetadata(field.metadata)
          .putLong(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY, id)
          .build())
    }
  }
}
