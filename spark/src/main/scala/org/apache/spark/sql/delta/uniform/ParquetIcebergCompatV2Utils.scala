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

package org.apache.spark.sql.delta.uniform

import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.metadata.ParquetMetadata

import org.apache.spark.sql.execution.datasources.parquet.ParquetFooterReader

/**
 * Contains utilities to check whether a specific parquet data file
 * is considered `IcebergCompatV2`.
 * See [[isParquetIcebergV2Compatible]] for details.
 */
object ParquetIcebergCompatV2Utils {
  // TIMESTAMP stored as INT96 is NOT considered `IcebergCompatV2`.
  // NOTE: `TIMESTAMP <-> INT96` is an exact *one-to-one* mapping
  // in default and `IcebergCompatV1` delta table.
  // this means we could confidently claim an `INT96` must be `TIMESTAMP`
  // if found in the parquet footer schema field.
  private val TIMESTAMP_AS_INT96 = org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96

  /**
   * Recursively traverse a specific parquet schema field,
   * check the following properties, i.e.,
   * - for primitive type,
   *   - check if TIMESTAMP is stored as INT96.
   *   - check for the `field_id`.
   * - for group type,
   *   - check for the `field_id`.
   *   - iterate through all fields and check each field recursively in the same way.
   *
   * @param field the field to check, this corresponds to a specific parquet file.
   * @return whether the parquet field contains TIMESTAMP stored as INT96 or
   *         lacking `field_id` for any (nested) field or not;
   *         if so, return true; otherwise return false.
   */
  private def hasTimestampAsInt96OrFieldIdNotExistForType(
      field: org.apache.parquet.schema.Type): Boolean = field match {
    // note: `getId` returns null indicates the field does not contain `field_id`.
    case p: org.apache.parquet.schema.PrimitiveType =>
      (p.getPrimitiveTypeName == TIMESTAMP_AS_INT96) || (p.getId == null)
    case g: org.apache.parquet.schema.GroupType =>
      if (g.getId != null) {
        val logicalAnnotation = g.getLogicalTypeAnnotation
        val fields = if (logicalAnnotation != null &&
            (logicalAnnotation.toString == "LIST" || logicalAnnotation.toString == "MAP")) {
          // according to parquet's spec,
          // - for LIST:
          //   - the outer-most level must be a group annotated with LIST
          //     that contains a **single** field named list.
          // - for MAP:
          //   - the outer-most level must be a group annotated with MAP
          //     that contains a **single** field named key_value.
          // details could be found at
          // [[https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists]] and
          // [[https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps]] and
          g.getFields.get(0).asGroupType().getFields
        } else {
          g.getFields
        }
        fields.toArray.exists {
          case field: org.apache.parquet.schema.Type =>
            hasTimestampAsInt96OrFieldIdNotExistForType(field)
        }
      } else {
        true
      }
  }

  /**
   * Check if the parquet file is `IcebergCompatV2` by inspecting the
   * provided parquet footer.
   *
   * note: icebergV2-compatible check refer to the following two properties.
   * 1. TIMESTAMP
   *    - If TIMESTAMP is stored as `int96`, it's considered incompatible since
   *      iceberg stores TIMESTAMP as `int64` according to the iceberg spec.
   * 2. field_id
   *    - `field_id` is needed for *every* field in a parquet footer, this includes
   *      the field of each column, and the potential nested fields for nested types
   *      like LIST, MAP or STRUCT.
   *      See [[https://github.com/apache/parquet-format/blob/master/LogicalTypes.md]] for details.
   *    - This is checked by inspecting whether the `field_id` for each column
   *      is null or not recursively.
   *
   * @param footer the parquet footer to be checked.
   * @return whether the parquet file is considered `IcebergCompatV2`.
   */
  def isParquetIcebergCompatV2(footer: ParquetMetadata): Boolean = {
    // iterate through each column/field and check if there exists
    // any column/field that contains TIMESTAMP stored as INT96,
    // or lacking any `field_id` (included nested one as in LIST or MAP).
    !footer.getFileMetaData.getSchema.getFields.toArray.exists {
      case field: org.apache.parquet.schema.Type =>
        hasTimestampAsInt96OrFieldIdNotExistForType(field)
    }
  }

  /**
   * Get the parquet footer based on the input `parquetPath`.
   *
   * @param parquetPath the absolute path to the parquet file.
   * @return the corresponding parquet metadata/footer.
   */
  def getParquetFooter(parquetPath: String): ParquetMetadata = {
    val path = new org.apache.hadoop.fs.Path(parquetPath)
    val conf = new org.apache.hadoop.conf.Configuration
    val fs = path.getFileSystem(conf)
    val status = fs.getFileStatus(path)
    ParquetFooterReader.readFooter(conf, status, ParquetMetadataConverter.NO_FILTER)
  }
}
