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


package io.delta.standalone.util

import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.SaveMode

object OSSUtil {

  val schema: StructType = StructType(Array(
    StructField("col1_part", IntegerType, nullable = true),
    StructField("col2_part", StringType, nullable = true)
  ))

  private val partitionColumns = schema.fieldNames.filter(_.contains("part")).toSeq

  val op: DeltaOperations.Write =
    DeltaOperations.Write(SaveMode.Append, Some(partitionColumns), Some("predicate_str"))

  val metadata: Metadata = Metadata(
    id = "id",
    name = "name",
    description = "description",
    format = Format(provider = "parquet", options = Map("format_key" -> "format_value")),
    partitionColumns = partitionColumns,
    schemaString = schema.json,
    createdTime = Some(1000L)
  )

  val addFiles: Seq[AddFile] = (0 until 50).map { i =>
    AddFile(
      path = i.toString,
      partitionValues = partitionColumns.map { col => col -> i.toString }.toMap,
      size = 100L,
      modificationTime = 1000L,
      dataChange = true,
      stats = null,
      tags = Map("tag_key" -> "tag_val")
    )
  }

  val removeFiles: Seq[RemoveFile] = addFiles.map(_.removeWithTimestamp(2000L, dataChange = true))

  def getCommitInfoAt(log: DeltaLog, version: Long): CommitInfo = {
    log.update()

    val firstChange = log.getChanges(version).next()
    assert(firstChange._1 == version, s"getOssCommitInfoAt: expected first version to be $version" +
      s"but got ${firstChange._1} instead.")

    val commitInfoOpt = firstChange._2.collectFirst { case c: CommitInfo => c }
    assert(commitInfoOpt.isDefined, s"getOssCommitInfoAt: expected to find a CommitInfo action at" +
      s"version $version, but none was found.")

    commitInfoOpt.get
  }
}
