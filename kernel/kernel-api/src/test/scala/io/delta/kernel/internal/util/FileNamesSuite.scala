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

  private val checkpointV1 = "/a/123.checkpoint.parquet"
  private val checkpointMultiPart = "/a/123.checkpoint.0000000001.0000000087.parquet"
  private val checkpointV2Json = "/a/000000010.checkpoint.80a083e8-7026.json"
  private val checkpointV2Parquet = "/a/000000010.checkpoint.80a083e8-7026.parquet"
  private val commitNormal = "/a/0000000088.json"
  private val commitUUID = "/a/00000022.dc0f9f58-a1a0.json"

  /////////////////////////////
  // Version extractor tests //
  /////////////////////////////

  test("checkpointVersion") {
    assert(checkpointVersion(new Path(checkpointV1)) == 123)
    assert(checkpointVersion(new Path(checkpointMultiPart)) == 123)
    assert(checkpointVersion(new Path(checkpointV2Json)) == 10)
    assert(checkpointVersion(new Path(checkpointV2Parquet)) == 10)
  }

  test("deltaVersion") {
    assert(deltaVersion(new Path(commitNormal)) == 88)
    assert(deltaVersion(new Path(commitUUID)) == 22)
  }

  test("getFileVersion") {
    assert(getFileVersion(new Path(checkpointV1)) == 123)
    assert(getFileVersion(new Path(checkpointMultiPart)) == 123)
    assert(getFileVersion(new Path(checkpointV2Json)) == 10)
    assert(getFileVersion(new Path(checkpointV2Parquet)) == 10)
    assert(getFileVersion(new Path(commitNormal)) == 88)
    assert(getFileVersion(new Path(commitUUID)) == 22)
  }

  /////////////////////////////////////////
  // File path and prefix builders tests //
  /////////////////////////////////////////

  test("deltaFile") {
    assert(deltaFile(new Path("/a"), 1234) == "/a/00000000000000001234.json")
  }

  test("sidecarFile") {
    assert(sidecarFile(new Path("/a"), "7d17ac10.parquet") == "/a/_sidecars/7d17ac10.parquet")
  }

  test("listingPrefix") {
    assert(listingPrefix(new Path("/a"), 1234) == "/a/00000000000000001234.")
  }

  test("checkpointFileSingular") {
    assert(
      checkpointFileSingular(new Path("/a"), 1234).toString ==
      "/a/00000000000000001234.checkpoint.parquet")
  }

  test("topLevelV2CheckpointFile") {
    assert(
      topLevelV2CheckpointFile(new Path("/a"), 1234, "7d17ac10", "json").toString ==
      "/a/00000000000000001234.checkpoint.7d17ac10.json")
    assert(
      topLevelV2CheckpointFile(new Path("/a"), 1234, "7d17ac10", "parquet").toString ==
      "/a/00000000000000001234.checkpoint.7d17ac10.parquet")
  }

  test("v2CheckpointSidecarFile") {
    assert(
      v2CheckpointSidecarFile(new Path("/a"), "7d17ac10").toString ==
      "/a/_sidecars/7d17ac10.parquet")
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

  ///////////////////////////////////
  // Is <type> file checkers tests //
  ///////////////////////////////////

  test("is checkpoint file") {
    // ===== V1 checkpoint =====
    // Positive cases
    assert(isCheckpointFile(new Path(checkpointV1)))
    assert(isClassicCheckpointFile(new Path(checkpointV1)))
    // Negative cases
    assert(!isMultiPartCheckpointFile(new Path(checkpointV1)))
    assert(!isV2CheckpointFile(new Path(checkpointV1)))
    assert(!isCommitFile(new Path(checkpointV1)))

    // ===== Multipart checkpoint =====
    // Positive cases
    assert(isCheckpointFile(new Path(checkpointMultiPart)))
    assert(isMultiPartCheckpointFile(new Path(checkpointMultiPart)))
    // Negative cases
    assert(!isClassicCheckpointFile(new Path(checkpointMultiPart)))
    assert(!isV2CheckpointFile(new Path(checkpointMultiPart)))
    assert(!isCommitFile(new Path(checkpointMultiPart)))

    // ===== V2 checkpoint =====
    // Positive cases
    assert(isCheckpointFile(new Path(checkpointV2Json)))
    assert(isV2CheckpointFile(new Path(checkpointV2Json)))
    assert(isCheckpointFile(new Path(checkpointV2Parquet)))
    assert(isV2CheckpointFile(new Path(checkpointV2Parquet)))
    // Negative cases
    assert(!isClassicCheckpointFile(new Path(checkpointV2Json)))
    assert(!isClassicCheckpointFile(new Path(checkpointV2Parquet)))
    assert(!isMultiPartCheckpointFile(new Path(checkpointV2Json)))
    assert(!isMultiPartCheckpointFile(new Path(checkpointV2Parquet)))
    assert(!isCommitFile(new Path(checkpointV2Json)))
    assert(!isCommitFile(new Path(checkpointV2Parquet)))

    // ===== Others =====
    assert(!isCheckpointFile(new Path("/a/123.json")))
    assert(!isCommitFile(new Path("/a/123.checkpoint.3.json")))
  }

  test("is commit file") {
    assert(isCommitFile(new Path(commitNormal)))
    assert(isCommitFile(new Path(commitUUID)))
  }
}
