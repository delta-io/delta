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
package io.delta.kernel.internal.checkpoints

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for [[LastCheckpointInfo]] parsing that a real golden file cannot exercise: the parse
 * contract's error cases (every golden blob is valid) and the `None`-vs-`Some(empty)` distinction
 * for `nonFileActions` / `sidecarFiles` (no golden blob carries empty arrays).
 *
 * The happy-path parse against real on-disk bytes -- classic, V2 (json), and V2 (parquet) pointers,
 * including `checkpointSchema` and sidecars -- is covered end-to-end by
 * `LastCheckpointInfoGoldenSuite` in kernel-defaults.
 */
class LastCheckpointInfoSuite extends AnyFunSuite {

  test("absent optional fields parse to empty, not defaults") {
    // The minimal legacy pointer: only version/size/parts, as written by kernel's own writer.
    val info = LastCheckpointInfo.fromJson("""{"version": 7, "size": 3, "parts": 2}""")
    assert(info.getVersion == 7L)
    assert(info.getSize == 3L)
    assert(info.getParts.get == 2)
    assert(!info.getSizeInBytes.isPresent)
    assert(!info.getNumOfAddFiles.isPresent)
    assert(!info.getCheckpointSchemaJson.isPresent)
    assert(!info.getChecksum.isPresent)
    assert(!info.getV2Checkpoint.isPresent)
  }

  test("absent nonFileActions/sidecarFiles are None, distinct from empty") {
    val absent =
      LastCheckpointInfo.fromJson(
        """{"version": 1, "size": 1, "v2Checkpoint":
          |{"path": "p", "sizeInBytes": 1, "modificationTime": 2}}""".stripMargin)
    assert(!absent.getV2Checkpoint.get.getNonFileActionsJson.isPresent)
    assert(!absent.getV2Checkpoint.get.getSidecarFiles.isPresent)

    val empty =
      LastCheckpointInfo.fromJson(
        """{"version": 1, "size": 1, "v2Checkpoint":
          |{"path": "p", "sizeInBytes": 1, "modificationTime": 2,
          | "nonFileActions": [], "sidecarFiles": []}}""".stripMargin)
    assert(empty.getV2Checkpoint.get.getNonFileActionsJson.get.isEmpty)
    assert(empty.getV2Checkpoint.get.getSidecarFiles.get.isEmpty)
  }

  test("V2 pointer without a checkpointMetadata action yields empty version") {
    val blob =
      """{"version": 4, "size": 10, "v2Checkpoint": {
        |  "path": "p", "sizeInBytes": 1, "modificationTime": 2,
        |  "nonFileActions": [{"protocol": {"minReaderVersion": 3}}]
        |}}""".stripMargin
    val v2 = LastCheckpointInfo.fromJson(blob).getV2Checkpoint.get
    assert(v2.getNonFileActionsJson.get.size() == 1)
    assert(!v2.getCheckpointMetadataVersion.isPresent)
  }

  test("sidecar tags are parsed as a string map") {
    val blob =
      """{"version": 1, "size": 1, "v2Checkpoint": {
        |  "path": "p", "sizeInBytes": 1, "modificationTime": 2,
        |  "sidecarFiles": [{"path": "s", "sizeInBytes": 3, "modificationTime": 4,
        |    "tags": {"k1": "v1", "k2": "v2"}}]
        |}}""".stripMargin
    val sidecar = LastCheckpointInfo.fromJson(blob).getV2Checkpoint.get.getSidecarFiles.get.get(0)
    val tags = sidecar.getTags
    assert(tags.size() == 2)
    assert(tags.get("k1") == "v1")
    assert(tags.get("k2") == "v2")
  }

  test("rejects a non-object blob") {
    val e = intercept[IllegalArgumentException](LastCheckpointInfo.fromJson("[1, 2, 3]"))
    assert(e.getMessage.contains("must be a JSON object"))
  }

  test("rejects a blob missing the required version field") {
    val e = intercept[IllegalArgumentException](LastCheckpointInfo.fromJson("""{"size": 1}"""))
    assert(e.getMessage.contains("version"))
  }

  test("rejects unparseable JSON") {
    intercept[IllegalArgumentException](LastCheckpointInfo.fromJson("not json"))
  }
}
