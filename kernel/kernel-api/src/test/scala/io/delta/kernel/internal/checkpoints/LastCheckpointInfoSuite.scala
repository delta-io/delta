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

import io.delta.kernel.internal.types.DataTypeJsonSerDe
import io.delta.kernel.internal.util.JsonUtils

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for [[LastCheckpointInfo]] parsing. Uses inline `_last_checkpoint` blobs modeled on
 * the kernel-defaults golden tables (`spark-variant-checkpoint`, `v2-checkpoint-json`) so the parse
 * is exercised without an engine or golden-table dependency.
 */
class LastCheckpointInfoSuite extends AnyFunSuite {

  // Classic (single-file) checkpoint pointer, shaped like spark-variant-checkpoint's
  // _last_checkpoint: version/size/sizeInBytes/numOfAddFiles/checkpointSchema/checksum, no parts,
  // no v2Checkpoint.
  private val classicBlob =
    """{
      |  "version": 2,
      |  "size": 6,
      |  "sizeInBytes": 21929,
      |  "numOfAddFiles": 4,
      |  "checkpointSchema": {
      |    "type": "struct",
      |    "fields": [
      |      {"name": "txn", "type": "string", "nullable": true, "metadata": {}}
      |    ]
      |  },
      |  "checksum": "a8d400a03ead8a86dbb412f2a693e26e"
      |}""".stripMargin

  // V2 checkpoint pointer, shaped like v2-checkpoint-json's _last_checkpoint: a v2Checkpoint block
  // with nonFileActions (protocol / metaData / checkpointMetadata) and one sidecar.
  private val v2Blob =
    """{
      |  "version": 4,
      |  "size": 10,
      |  "sizeInBytes": 10228,
      |  "numOfAddFiles": 6,
      |  "v2Checkpoint": {
      |    "path": "00000000000000000004.checkpoint.a267.json",
      |    "sizeInBytes": 717,
      |    "modificationTime": 1752616673818,
      |    "nonFileActions": [
      |      {"protocol": {"minReaderVersion": 3, "minWriterVersion": 7}},
      |      {"metaData": {"id": "8a390218-e4ee-4341-b6de-4920e27d3f78"}},
      |      {"checkpointMetadata": {"version": 4}}
      |    ],
      |    "sidecarFiles": [
      |      {
      |        "path": "00000000000000000004.checkpoint.0000.parquet",
      |        "sizeInBytes": 9511,
      |        "modificationTime": 1752616673806
      |      }
      |    ]
      |  },
      |  "checksum": "e0e16b97d85501a7f67b00c24aaa07f2"
      |}""".stripMargin

  test("parses a classic checkpoint pointer") {
    val info = LastCheckpointInfo.fromJson(classicBlob)
    assert(info.getVersion == 2L)
    assert(info.getSize == 6L)
    assert(!info.getParts.isPresent)
    assert(info.getSizeInBytes.get == 21929L)
    assert(info.getNumOfAddFiles.get == 4L)
    assert(info.getChecksum.get == "a8d400a03ead8a86dbb412f2a693e26e")
    assert(!info.getV2Checkpoint.isPresent)
  }

  test("checkpointSchema is captured as raw JSON that re-parses into a StructType") {
    val info = LastCheckpointInfo.fromJson(classicBlob)
    assert(info.getCheckpointSchemaJson.isPresent)
    val schemaJson = info.getCheckpointSchemaJson.get
    // Raw JSON, not decoded eagerly.
    assert(schemaJson.contains("\"type\":\"struct\""))
    // ... but it round-trips into the structural type on demand.
    val parsed = DataTypeJsonSerDe.deserializeStructType(schemaJson)
    assert(parsed.length() == 1)
    assert(parsed.fieldNames().contains("txn"))
  }

  test("parses a V2 checkpoint pointer including sidecars and checkpointMetadata version") {
    val info = LastCheckpointInfo.fromJson(v2Blob)
    assert(info.getVersion == 4L)
    assert(info.getV2Checkpoint.isPresent)
    val v2 = info.getV2Checkpoint.get
    assert(v2.getPath == "00000000000000000004.checkpoint.a267.json")
    assert(v2.getSizeInBytes == 717L)

    // nonFileActions are kept as verbatim per-action JSON.
    assert(v2.getNonFileActionsJson.isPresent)
    assert(v2.getNonFileActionsJson.get.size() == 3)
    assert(v2.getNonFileActionsJson.get.get(0).contains("\"protocol\""))

    // The checkpointMetadata action's version is surfaced directly.
    assert(v2.getCheckpointMetadataVersion.isPresent)
    assert(v2.getCheckpointMetadataVersion.get == 4L)

    // Sidecars fully parsed.
    assert(v2.getSidecarFiles.isPresent)
    assert(v2.getSidecarFiles.get.size() == 1)
    val sidecar = v2.getSidecarFiles.get.get(0)
    assert(sidecar.getPath == "00000000000000000004.checkpoint.0000.parquet")
    assert(sidecar.getSizeInBytes == 9511L)
    assert(sidecar.getTags.isEmpty)
  }

  test("V2 pointer without a checkpointMetadata action yields empty version") {
    val blob =
      """{"version": 4, "size": 10, "v2Checkpoint": {
        |  "path": "p", "sizeInBytes": 1, "modificationTime": 2,
        |  "nonFileActions": [{"protocol": {"minReaderVersion": 3}}]
        |}}""".stripMargin
    val info = LastCheckpointInfo.fromJson(blob)
    val v2 = info.getV2Checkpoint.get
    assert(v2.getNonFileActionsJson.get.size() == 1)
    assert(!v2.getCheckpointMetadataVersion.isPresent)
  }

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

  test("equality is by value") {
    assert(LastCheckpointInfo.fromJson(v2Blob) == LastCheckpointInfo.fromJson(v2Blob))
    assert(LastCheckpointInfo.fromJson(v2Blob) != LastCheckpointInfo.fromJson(classicBlob))
  }

  test("blob re-serialization keeps checkpointSchema byte-identical to the source subtree") {
    // Guards the raw-string contract: what we hand back must equal the source node's own JSON.
    val root = JsonUtils.mapper().readTree(classicBlob)
    val expected = root.get("checkpointSchema").toString
    assert(LastCheckpointInfo.fromJson(classicBlob).getCheckpointSchemaJson.get == expected)
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
