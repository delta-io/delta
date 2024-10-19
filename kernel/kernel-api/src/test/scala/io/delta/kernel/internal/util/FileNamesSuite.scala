/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.util

import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames._
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class FileNamesSuite extends AnyFunSuite {

  test("isCheckpointFile") {
    assert(isCheckpointFile("/a/123.checkpoint.parquet"))
    assert(isCheckpointFile("/a/123.checkpoint.0000000001.0000000087.parquet"))
    assert(isCheckpointFile("/a/000000010.checkpoint.80a083e8-7026.json"))
    assert(isCheckpointFile("/a/000000010.checkpoint.80a083e8-7026.parquet"))
    assert(!isCheckpointFile("/a/123.json"))
    assert(!isCommitFile("/a/123.checkpoint.3.json"))
  }

  test("checkpointVersion") {
    assert(checkpointVersion(new Path("/a/123.checkpoint.parquet")) == 123)
    assert(checkpointVersion(new Path("/a/0.checkpoint.parquet")) == 0)
    assert(checkpointVersion(
      new Path("/a/00000000000000000151.checkpoint.parquet")) == 151)
    assert(checkpointVersion(
      new Path("/a/999.checkpoint.0000000090.0000000099.parquet")) == 999)
    assert(checkpointVersion("/a/000000010.checkpoint.80a083e8-7026.json") == 10)
  }

  test("listingPrefix") {
    assert(listingPrefix(new Path("/a"), 1234) == "/a/00000000000000001234.")
  }

  test("checkpointFileWithParts") {
    assert(checkpointFileWithParts(new Path("/a"), 1, 1).asScala == Seq(
      new Path("/a/00000000000000000001.checkpoint.0000000001.0000000001.parquet")))
    assert(checkpointFileWithParts(new Path("/a"), 1, 2).asScala == Seq(
      new Path("/a/00000000000000000001.checkpoint.0000000001.0000000002.parquet"),
      new Path("/a/00000000000000000001.checkpoint.0000000002.0000000002.parquet")))
    assert(checkpointFileWithParts(new Path("/a"), 1, 5).asScala == Seq(
      new Path("/a/00000000000000000001.checkpoint.0000000001.0000000005.parquet"),
      new Path("/a/00000000000000000001.checkpoint.0000000002.0000000005.parquet"),
      new Path("/a/00000000000000000001.checkpoint.0000000003.0000000005.parquet"),
      new Path("/a/00000000000000000001.checkpoint.0000000004.0000000005.parquet"),
      new Path("/a/00000000000000000001.checkpoint.0000000005.0000000005.parquet")))
  }
}
