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

import java.io.Closeable

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{DeltaColumnMappingMode, NoMapping, SerializableFileStatus}

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

/**
 * An interface for the table to be converted to Delta.
 */
trait ConvertTargetTable {
  /** The table schema of the target table */
  def tableSchema: StructType

  /** The table properties of the target table */
  def properties: Map[String, String] = Map.empty

  /** The partition schema of the target table */
  def partitionSchema: StructType

  /** The file manifest of the target table */
  def fileManifest: ConvertTargetFileManifest

  /** The number of files from the target table */
  def numFiles: Long

  /** Whether this table requires column mapping to be converted */
  def requiredColumnMappingMode: DeltaColumnMappingMode = NoMapping

  /* The format of the table */
  def format: String

}

/** An interface for providing an iterator of files for a table. */
trait ConvertTargetFileManifest extends Closeable {
  /** The base path of a table. Should be a qualified, normalized path. */
  val basePath: String

  /** Return all files as a Dataset for parallelized processing. */
  def allFiles: Dataset[ConvertTargetFile]

  /** Return the active files for a table in sequence */
  def getFiles: Iterator[ConvertTargetFile] = allFiles.toLocalIterator().asScala

  /** Return the number of files for the table */
  def numFiles: Long = allFiles.count()

  /** Return the parquet schema for the table.
   *  Defined only when the schema cannot be inferred from CatalogTable.
   */
  def parquetSchema: Option[StructType] = None
}

/**
 * An interface for the file to be included during conversion.
 *
 * @param fileStatus the file info
 * @param partitionValues partition values of this file that may be available from the source
 *                        table format. If none, the converter will infer partition values from the
 *                        file path, assuming the Hive directory format.
 * @param parquetSchemaDDL the Parquet schema DDL associated with the file.
 */
case class ConvertTargetFile(
  fileStatus: SerializableFileStatus,
  partitionValues: Option[Map[String, String]] = None,
  parquetSchemaDDL: Option[String] = None) extends Serializable
