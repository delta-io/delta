/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.connector.read.file

import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex}
import org.apache.spark.sql.types.StructType

/**
 * Bundle of everything a planner strategy needs to synthesize a `HadoopFsRelation`
 * (and therefore a `FileSourceScanExec`) from a [[FileScan]]: the [[FileIndex]], the
 * [[FileFormat]], partition/data schemas, an optional [[BucketSpec]], data-source options,
 * and the originating [[CatalogTable]] (when the relation is catalog-bound).
 *
 * `isPinned` indicates that the underlying file listing is locked to a specific snapshot
 * (e.g. time travel, CDC, deletion vectors) and must not be re-resolved during planning.
 */
trait FileSet {
  def fileIndex: FileIndex
  def fileFormat: FileFormat
  def partitionSchema: StructType
  def dataSchema: StructType
  def bucketSpec: Option[BucketSpec]
  def options: Map[String, String]
  def catalogTable: Option[CatalogTable]
  def isPinned: Boolean
}
