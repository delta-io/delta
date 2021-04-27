/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite

class FileNamesSuite extends SparkFunSuite {

  import org.apache.spark.sql.delta.util.FileNames._

  test("isDeltaFile") {
    assert(isDeltaFile(new Path("/a/123.json")))
    assert(!isDeltaFile(new Path("/a/123ajson")))
    assert(!isDeltaFile(new Path("/a/123.jso")))
    assert(!isDeltaFile(new Path("/a/123a.json")))
    assert(!isDeltaFile(new Path("/a/a123.json")))
  }

  test("isCheckpointFile") {
    assert(isCheckpointFile(new Path("/a/123.checkpoint.parquet")))
    assert(isCheckpointFile(new Path("/a/123.checkpoint.0000000001.0000000087.parquet")))
    assert(!isCheckpointFile(new Path("/a/123.json")))
  }

  test("checkpointVersion") {
    assert(checkpointVersion(new Path("/a/123.checkpoint.parquet")) == 123)
    assert(checkpointVersion(new Path("/a/0.checkpoint.parquet")) == 0)
    assert(checkpointVersion(new Path("/a/00000000000000000151.checkpoint.parquet")) == 151)
    assert(checkpointVersion(new Path("/a/999.checkpoint.0000000090.0000000099.parquet")) == 999)
  }

  test("checkpointPrefix") {
    assert(checkpointPrefix(new Path("/a"), 1234) == new Path("/a/00000000000000001234.checkpoint"))
  }

  test("checkpointFileWithParts") {
    assert(checkpointFileWithParts(new Path("/a"), 1, 1) == Seq(
      new Path("/a/00000000000000000001.checkpoint.0000000001.0000000001.parquet")))
    assert(checkpointFileWithParts(new Path("/a"), 1, 2) == Seq(
      new Path("/a/00000000000000000001.checkpoint.0000000001.0000000002.parquet"),
      new Path("/a/00000000000000000001.checkpoint.0000000002.0000000002.parquet")))
    assert(checkpointFileWithParts(new Path("/a"), 1, 5) == Seq(
      new Path("/a/00000000000000000001.checkpoint.0000000001.0000000005.parquet"),
      new Path("/a/00000000000000000001.checkpoint.0000000002.0000000005.parquet"),
      new Path("/a/00000000000000000001.checkpoint.0000000003.0000000005.parquet"),
      new Path("/a/00000000000000000001.checkpoint.0000000004.0000000005.parquet"),
      new Path("/a/00000000000000000001.checkpoint.0000000005.0000000005.parquet")))
  }

  test("numCheckpointParts") {
    assert(numCheckpointParts(new Path("/a/00000000000000000099.checkpoint.parquet")).isEmpty)
    assert(
      numCheckpointParts(
        new Path("/a/00000000000000000099.checkpoint.0000000078.0000000092.parquet"))
        .contains(92))
  }
}
