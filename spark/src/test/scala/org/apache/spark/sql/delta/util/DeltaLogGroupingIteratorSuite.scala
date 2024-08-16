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

import org.apache.spark.sql.delta.SerializableFileStatus

import org.apache.spark.SparkFunSuite

class DeltaLogGroupingIteratorSuite extends SparkFunSuite {

  test("DeltaLogGroupingIterator") {
    val paths = Seq(
      // both checkpoint and commit file present for v1
      "file://a/b/_delta_log/1.checkpoint.parquet",
      "file://a/b/_delta_log/1.json",

      // only json file present for v2
      "file://a/b/_delta_log/2.json",
      // v3 missing

      // multiple types of checkpoint present for v4
      "file://a/b/_delta_log/4.checkpoint.parquet",
      "file://a/b/_delta_log/4.checkpoint.uuid.parquet",
      "file://a/b/_delta_log/4.checkpoint.json.parquet",
      "file://a/b/_delta_log/4.checkpoint.0.1.parquet",
      "file://a/b/_delta_log/4.checkpoint.1.1.parquet",
      "file://a/b/_delta_log/4.json",
      // v5, v6 with single checkpoint file
      "file://a/b/_delta_log/5.checkpoint.parquet",
      "file://a/b/_delta_log/5.json",
      "file://a/b/_delta_log/6.checkpoint.parquet",
      // no checkpoint files in the end
      "file://a/b/_delta_log/6.json",
      "file://a/b/_delta_log/7.json",
      "file://a/b/_delta_log/8.json",
      "file://a/b/_delta_log/9.json",
      "file://a/b/_delta_log/11.checkpoint.0.1.parquet",
      "file://a/b/_delta_log/11.checkpoint.1.1.parquet",
      "file://a/b/_delta_log/11.checkpoint.uuid.parquet",
      "file://a/b/_delta_log/12.checkpoint.parquet",
      "file://a/b/_delta_log/14.json"
    )
    val fileStatuses = paths.map { path =>
      SerializableFileStatus(path, length = 10, isDir = false, modificationTime = 1).toFileStatus
    }.toIterator
    val groupedFileStatuses = new DeltaLogGroupingIterator(fileStatuses)
    val groupedPaths = groupedFileStatuses.toIndexedSeq.map { case (version, files) =>
      (version, files.map(_.getPath.toString).toList)
    }
    assert(groupedPaths === Seq(
      1 -> List("file://a/b/_delta_log/1.checkpoint.parquet", "file://a/b/_delta_log/1.json"),
      2 -> List("file://a/b/_delta_log/2.json"),
      4 -> List(
        "file://a/b/_delta_log/4.checkpoint.parquet",
        "file://a/b/_delta_log/4.checkpoint.uuid.parquet",
        "file://a/b/_delta_log/4.checkpoint.json.parquet",
        "file://a/b/_delta_log/4.checkpoint.0.1.parquet",
        "file://a/b/_delta_log/4.checkpoint.1.1.parquet",
        "file://a/b/_delta_log/4.json"),
      5 -> List("file://a/b/_delta_log/5.checkpoint.parquet", "file://a/b/_delta_log/5.json"),
      6 -> List("file://a/b/_delta_log/6.checkpoint.parquet", "file://a/b/_delta_log/6.json"),
      7 -> List("file://a/b/_delta_log/7.json"),
      8 -> List("file://a/b/_delta_log/8.json"),
      9 -> List("file://a/b/_delta_log/9.json"),
      11 -> List(
        "file://a/b/_delta_log/11.checkpoint.0.1.parquet",
        "file://a/b/_delta_log/11.checkpoint.1.1.parquet",
        "file://a/b/_delta_log/11.checkpoint.uuid.parquet"),
      12 -> List("file://a/b/_delta_log/12.checkpoint.parquet"),
      14 -> List("file://a/b/_delta_log/14.json")
    ))
  }
}
