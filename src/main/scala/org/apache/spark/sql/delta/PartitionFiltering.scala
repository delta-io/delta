/*
 * Copyright 2019 Databricks, Inc.
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

import org.apache.spark.sql.delta.actions.{AddFile, FileAction, RemoveFile, SingleAction}
import org.apache.spark.sql.delta.stats.DeltaScan
import org.apache.spark.sql.catalyst.expressions._

trait PartitionFiltering {
  self: Snapshot =>

  def filesForScan(
      projection: Seq[Attribute],
      filters: Seq[Expression],
      keepStats: Boolean = false): DeltaScan = {
    implicit val enc = SingleAction.addFileEncoder

    val files = DeltaLog.filterFileList(
      metadata.partitionColumns,
      allFiles.toDF(),
      partitionFilters(filters)).as[AddFile].collect()

    DeltaScan(version = version, files, null, null, null)(null, null, null, null)
  }

  def partitionFilters(filters: Seq[Expression]): Seq[Expression] = filters.flatMap { filter =>
    DeltaTableUtils.splitMetadataAndDataPredicates(filter, metadata.partitionColumns, spark)._1
  }

  def filterFileActions(
      actions: Seq[FileAction],
      filters: Seq[Expression]) : Seq[FileAction] = {
    val implicits = spark.implicits
    import implicits._

    val addFileDf = actions.flatMap {
      case a: AddFile => Some(a)
      case _: RemoveFile => None
    }.toDF()
    val addFiles = DeltaLog.filterFileList(
      metadata.partitionColumns,
      addFileDf,
      filters
    ).as[AddFile].collect()

    val removeFileDf = actions.flatMap {
      case _: AddFile => None
      case r: RemoveFile => Some(r)
    }.toDF()
    val removeFiles = DeltaLog.filterFileList(
      metadata.partitionColumns,
      removeFileDf,
      filters
    ).as[RemoveFile].collect()
    addFiles ++ removeFiles
  }
}
