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


package io.delta.standalone.internal.util // need this to access internal.util.JsonUtils

import java.util.Collections

import scala.collection.JavaConverters._

import io.delta.standalone.Operation
import io.delta.standalone.actions.{AddFile, Format, Metadata, RemoveFile}
import io.delta.standalone.types.{IntegerType, StringType, StructField, StructType}

object StandaloneUtil {

  val engineInfo = "standaloneEngineInfo"

  val schema = new StructType(Array(
    new StructField("col1_part", new IntegerType(), true),
    new StructField("col2_part", new StringType(), true)
  ))

  val partitionColumns: Seq[String] =
    schema.getFieldNames.filter(_.contains("part")).toSeq

  val op = new Operation(Operation.Name.MANUAL_UPDATE, Map[String, Object](
    "mode" -> "Append",
    "partitionBy" -> JsonUtils.toJson(partitionColumns),
    "predicate" -> "predicate_str"
  ).asJava)

  val metadata: Metadata = Metadata.builder()
    .id("id")
    .name("name")
    .description("description")
    .format(new Format("parquet", Collections.singletonMap("format_key", "format_value")))
    .partitionColumns(partitionColumns.asJava)
    .schema(schema)
    .createdTime(1000L)
    .build()

  val addFiles: Seq[AddFile] = (0 until 50).map { i =>
    new AddFile(
      i.toString, // path
      partitionColumns.map { col => col -> i.toString }.toMap.asJava, // partition values
      100L, // size
      1000, // modification time
      true, // data change
      null, // stats
      Map("tag_key" -> "tag_val").asJava // tags
    )
  }

  val removeFiles: Seq[RemoveFile] = addFiles.map { a =>
    RemoveFile.builder(a.getPath)
      .deletionTimestamp(2000L)
      .dataChange(true)
      .extendedFileMetadata(true)
      .partitionValues(a.getPartitionValues)
      .size(a.getSize)
      .tags(a.getTags)
      .build()
  }
}
