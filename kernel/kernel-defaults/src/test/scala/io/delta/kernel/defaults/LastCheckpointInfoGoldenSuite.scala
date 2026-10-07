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
package io.delta.kernel.defaults

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.internal.checkpoints.Checkpointer
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.types.DataTypeJsonSerDe

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

/**
 * End-to-end tests for `Checkpointer.readLastCheckpointInfo`, which reads a real `_last_checkpoint`
 * golden file through the engine and parses it into a typed
 * [[io.delta.kernel.internal.checkpoints.LastCheckpointInfo]].
 *
 * The parse contract's error and `None`-vs-`Some(empty)` cases -- which no valid golden file can
 * exercise -- are covered by the unit suite `LastCheckpointInfoSuite` in kernel-api.
 */
class LastCheckpointInfoGoldenSuite extends AnyFunSuite {

  private val engine = DefaultEngine.create(new Configuration())

  private def logPathFor(goldenTable: String): Path =
    new Path(new Path(goldenTablePath(goldenTable)), "_delta_log")

  private def readInfo(goldenTable: String) =
    new Checkpointer(logPathFor(goldenTable)).readLastCheckpointInfo(engine).get()

  test("classic checkpoint pointer (spark-variant-checkpoint)") {
    val info = readInfo("spark-variant-checkpoint")
    assert(info.getVersion == 2L)
    assert(info.getSize == 6L)
    assert(!info.getParts.isPresent)
    assert(info.getSizeInBytes.get == 21929L)
    assert(info.getNumOfAddFiles.get == 4L)
    assert(info.getChecksum.get == "a8d400a03ead8a86dbb412f2a693e26e")
    assert(!info.getV2Checkpoint.isPresent)

    // checkpointSchema is kept as raw JSON and re-parses into the checkpoint file schema.
    assert(info.getCheckpointSchemaJson.isPresent)
    val schema = DataTypeJsonSerDe.deserializeStructType(info.getCheckpointSchemaJson.get)
    assert(schema.length() == 6)
    assert(schema.fieldNames().contains("add"))
    assert(schema.fieldNames().contains("metaData"))
  }

  test("V2 (json) checkpoint pointer with sidecars, no top-level checkpointSchema") {
    val info = readInfo("v2-checkpoint-json")
    assert(info.getVersion == 2L)
    assert(info.getSize == 9L)
    assert(info.getSizeInBytes.get == 19554L)
    assert(info.getNumOfAddFiles.get == 4L)
    assert(info.getChecksum.get == "d09f95a326aab562c60d415a32ddd216")
    // This pointer has no top-level checkpointSchema.
    assert(!info.getCheckpointSchemaJson.isPresent)

    assert(info.getV2Checkpoint.isPresent)
    val v2 = info.getV2Checkpoint.get
    assert(v2.getPath.endsWith(".json"))
    assert(v2.getSizeInBytes == 891L)
    assert(v2.getModificationTime == 1714496115810L)

    // nonFileActions: protocol / metaData / checkpointMetadata, kept as verbatim JSON.
    assert(v2.getNonFileActionsJson.isPresent)
    assert(v2.getNonFileActionsJson.get.size() == 3)
    assert(v2.getCheckpointMetadataVersion.get == 2L)

    // Two fully-parsed sidecars.
    assert(v2.getSidecarFiles.isPresent)
    val sidecars = v2.getSidecarFiles.get
    assert(sidecars.size() == 2)
    assert(sidecars.get(0).getSizeInBytes == 9367L)
    assert(sidecars.get(1).getSizeInBytes == 9296L)
    assert(sidecars.get(0).getPath.endsWith(".parquet"))
  }

  test("V2 (parquet) checkpoint pointer carrying both checkpointSchema and a v2Checkpoint block") {
    val info = readInfo("v2-checkpoint-parquet")
    assert(info.getVersion == 2L)
    assert(info.getSizeInBytes.get == 37269L)
    assert(info.getNumOfAddFiles.get == 4L)

    // Both the top-level checkpointSchema and the v2Checkpoint block are present.
    assert(info.getCheckpointSchemaJson.isPresent)
    assert(DataTypeJsonSerDe.deserializeStructType(info.getCheckpointSchemaJson.get).length() > 0)

    val v2 = info.getV2Checkpoint.get
    assert(v2.getPath.endsWith(".parquet"))
    assert(v2.getCheckpointMetadataVersion.get == 2L)
    assert(v2.getSidecarFiles.get.size() == 2)
  }

  test("readLastCheckpointInfo matches the fields projected by readLastCheckpointFile") {
    val cp = new Checkpointer(logPathFor("v2-checkpoint-json"))
    val info = cp.readLastCheckpointInfo(engine).get()
    val columnar = cp.readLastCheckpointFile(engine).get()
    assert(info.getVersion == columnar.version)
    assert(info.getSize == columnar.size)
    assert(info.getParts.isPresent == columnar.parts.isPresent)
  }
}
