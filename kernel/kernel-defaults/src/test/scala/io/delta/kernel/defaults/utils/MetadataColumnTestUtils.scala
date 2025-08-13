/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.utils

import io.delta.kernel.types.{FieldMetadata, LongType, MetadataColumnType, StructField}

trait MetadataColumnTestUtils {
  val ROW_INDEX = new StructField(
    "_metadata.row_index",
    LongType.LONG,
    true,
    FieldMetadata.builder().putMetadataType(
      StructField.METADATA_TYPE_KEY,
      MetadataColumnType.ROW_INDEX).build())

  val ROW_ID = new StructField(
    "_metadata.row_id",
    LongType.LONG,
    true,
    FieldMetadata.builder().putMetadataType(
      StructField.METADATA_TYPE_KEY,
      MetadataColumnType.ROW_ID).build())

  val ROW_COMMIT_VERSION = new StructField(
    "_metadata.row_commit_version",
    LongType.LONG,
    true,
    FieldMetadata.builder().putMetadataType(
      StructField.METADATA_TYPE_KEY,
      MetadataColumnType.ROW_COMMIT_VERSION).build())
}
