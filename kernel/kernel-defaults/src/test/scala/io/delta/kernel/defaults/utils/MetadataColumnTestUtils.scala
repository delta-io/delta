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
