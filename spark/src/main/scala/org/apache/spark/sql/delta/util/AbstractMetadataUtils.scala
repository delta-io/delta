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

package org.apache.spark.sql.delta.util

import scala.collection.JavaConverters._

import io.delta.storage.commit.actions.AbstractMetadata
import org.apache.spark.sql.delta.{DeltaColumnMappingMode, DeltaConfigs}

import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Implicit class to provide convenience methods for AbstractMetadata,
 * similar to those available in the Metadata class.
 */
object AbstractMetadataUtils {
  implicit class RichAbstractMetadata(val metadata: AbstractMetadata) extends AnyVal {
    /** Returns the table id */
    def id: String = metadata.getId

    /** Returns the schema as a [[StructType]] */
    def schema: StructType = Option(metadata.getSchemaString)
      .map(DataType.fromJson(_).asInstanceOf[StructType])
      .getOrElse(StructType.apply(Nil))

    /** Returns partition columns as Scala Seq */
    def partitionColumns: Seq[String] = metadata.getPartitionColumns.asScala.toSeq

    /** Returns the partitionSchema as a [[StructType]] */
    def partitionSchema: StructType =
      new StructType(partitionColumns.map(c => schema(c)).toArray)

    /** Returns configuration as Scala Map */
    def configuration: Map[String, String] = metadata.getConfiguration.asScala.toMap

    /** Returns the column mapping mode for this table */
    def columnMappingMode: DeltaColumnMappingMode =
      DeltaConfigs.COLUMN_MAPPING_MODE.fromMap(configuration)
  }
}

