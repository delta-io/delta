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

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite

class FileNamesSuite extends SparkFunSuite {

  import org.apache.spark.sql.delta.util.FileNames._

  test("isDeltaFile") {
    assert(isDeltaFile(new Path("/a/_delta_log/123.json")))
    assert(isDeltaFile(new Path("/a/123.json")))
    assert(!isDeltaFile(new Path("/a/_delta_log/123ajson")))
    assert(!isDeltaFile(new Path("/a/_delta_log/123.jso")))
    assert(!isDeltaFile(new Path("/a/_delta_log/123a.json")))
    assert(!isDeltaFile(new Path("/a/_delta_log/a123.json")))

    // UUID Files
    assert(!isDeltaFile(new Path("/a/123.uuid.json")))
    assert(!isDeltaFile(new Path("/a/_delta_log/123.uuid.json")))
    assert(isDeltaFile(new Path("/a/_delta_log/_commits/123.uuid.json")))
    assert(!isDeltaFile(new Path("/a/_delta_log/_commits/123.uuid1.uuid2.json")))
  }

  test("DeltaFile.unapply") {
    assert(DeltaFile.unapply(new Path("/a/_delta_log/123.json")) ===
      Some((new Path("/a/_delta_log/123.json"), 123)))
    assert(DeltaFile.unapply(new Path("/a/123.json")) ===
      Some((new Path("/a/123.json"), 123)))
    assert(DeltaFile.unapply(new Path("/a/_delta_log/123ajson")).isEmpty)
    assert(DeltaFile.unapply(new Path("/a/_delta_log/123.jso")).isEmpty)
    assert(DeltaFile.unapply(new Path("/a/_delta_log/123a.json")).isEmpty)
    assert(DeltaFile.unapply(new Path("/a/_delta_log/a123.json")).isEmpty)

    // UUID Files
    assert(DeltaFile.unapply(new Path("/a/123.uuid.json")).isEmpty)
    assert(DeltaFile.unapply(new Path("/a/_delta_log/123.uuid.json")).isEmpty)
    assert(DeltaFile.unapply(new Path("/a/_delta_log/_commits/123.uuid.json")) ===
      Some((new Path("/a/_delta_log/_commits/123.uuid.json"), 123)))
    assert(DeltaFile.unapply(new Path("/a/_delta_log/_commits/123.uuid1.uuid2.json")).isEmpty)
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

  test("listingPrefix") {
    assert(listingPrefix(new Path("/a"), 1234) == new Path("/a/00000000000000001234."))
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

  test("commitDirPath") {
    assert(commitDirPath(logPath = new Path("/a/_delta_log")) ===
      new Path("/a/_delta_log/_commits"))
    assert(commitDirPath(logPath = new Path("/a/_delta_log/")) ===
      new Path("/a/_delta_log/_commits"))
  }
}
