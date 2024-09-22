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

import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.sql.catalyst.expressions.FileSourceConstantMetadataStructField
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{LongType, MetadataBuilder, StructField}

object DefaultRowCommitVersion {
  def assignIfMissing(
      protocol: Protocol,
      actions: Iterator[Action],
      version: Long): Iterator[Action] = {
    // Type Widening relies on default row commit versions to be set.
    if (!RowTracking.isSupported(protocol) && !TypeWidening.isSupported(protocol)) {
      return actions
    }
    actions.map {
      case a: AddFile if a.defaultRowCommitVersion.isEmpty =>
        a.copy(defaultRowCommitVersion = Some(version))
      case a =>
        a
    }
  }

  def createDefaultRowCommitVersionField(
      protocol: Protocol, metadata: Metadata): Option[StructField] = {
    Option.when(RowTracking.isEnabled(protocol, metadata)) {
      MetadataStructField()
    }
  }

  val METADATA_STRUCT_FIELD_NAME = "default_row_commit_version"

  private object MetadataStructField {
    private val METADATA_COL_ATTR_KEY = "__default_row_version_metadata_col"

    def apply(): StructField =
      StructField(
        METADATA_STRUCT_FIELD_NAME,
        LongType,
        nullable = false,
        metadata = metadata)

    def unapply(field: StructField): Option[StructField] =
      Some(field).filter(isValid)

    private def metadata: types.Metadata = new MetadataBuilder()
      .withMetadata(FileSourceConstantMetadataStructField.metadata(METADATA_STRUCT_FIELD_NAME))
      .putBoolean(METADATA_COL_ATTR_KEY, value = true)
      .build()


    private def isValid(s: StructField): Boolean = {
      FileSourceConstantMetadataStructField.isValid(s.dataType, s.metadata) &&
        metadata.contains(METADATA_COL_ATTR_KEY) &&
        metadata.getBoolean(METADATA_COL_ATTR_KEY)
    }
  }
}
