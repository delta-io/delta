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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Optional

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.internal.checkpoints.Checkpointer
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.types.DataTypeJsonSerDe
import io.delta.kernel.internal.util.JsonUtils

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

/**
 * End-to-end tests for `Checkpointer.readLastCheckpointFile` / `readLastCheckpointSerialized`.
 *
 * The opaque JSON blob lives in [[io.delta.kernel.internal.checkpoints.LastCheckpointSerialized]]
 * only. [[io.delta.kernel.internal.checkpoints.CheckpointMetaData]] carries classic columnar fields.
 */
class LastCheckpointHintSuite extends AnyFunSuite {

  private val engine = DefaultEngine.create(new Configuration())

  private def assertJsonEquals(expected: String, actual: String): Unit = {
    val expectedNode = JsonUtils.mapper().readTree(expected)
    val actualNode = JsonUtils.mapper().readTree(actual)
    assert(
      expectedNode == actualNode,
      s"JSON mismatch.\n expected: $expectedNode\n actual:   $actualNode")
  }

  private def logPathFor(goldenTable: String): Path =
    new Path(new Path(goldenTablePath(goldenTable)), "_delta_log")

  private def readLastCheckpoint(logPath: Path): String = {
    val bytes = Files.readAllBytes(
      Paths.get(new Path(logPath, Checkpointer.LAST_CHECKPOINT_FILE_NAME).toString))
    new String(bytes, StandardCharsets.UTF_8).trim
  }

  test("readLastCheckpointSerialized round-trips a V2 (json format) pointer") {
    val logPath = logPathFor("v2-checkpoint-json")
    val expectedJson = readLastCheckpoint(logPath)

    val cpm = new Checkpointer(logPath).readLastCheckpointFile(engine).get()
    assert(cpm.version == 2L)
    assert(cpm.size == 9L)
    assert(!cpm.parts.isPresent, "V2 pointer has no `parts`")

    val serialized = new Checkpointer(logPath).readLastCheckpointSerialized(engine).get()
    assertJsonEquals(expectedJson, serialized.toJson())
  }

  test("V2 (parquet format) pointer round-trips every field including checkpointSchema") {
    val logPath = logPathFor("v2-checkpoint-parquet")
    val rawJson = readLastCheckpoint(logPath)

    val serialized = new Checkpointer(logPath).readLastCheckpointSerialized(engine).get()
    val cpm = new Checkpointer(logPath).readLastCheckpointFile(engine).get()
    assert(cpm.version == 2L)
    assert(cpm.size == 9L)

    val parsedSchema =
      DataTypeJsonSerDe.deserializeStructType(
        JsonUtils.mapper().readTree(rawJson).get("checkpointSchema").toString)
    assert(parsedSchema.length() > 0)

    assert(serialized.json() == rawJson)
    assert(serialized.toJson().contains("\"checkpointSchema\":{"))
    assertJsonEquals(rawJson, serialized.toJson())
  }

  test("classic checkpoint round-trips columnar fields and checkpointSchema via the blob") {
    val logPath = logPathFor("spark-variant-checkpoint")
    val raw = readLastCheckpoint(logPath)

    val cpm = new Checkpointer(logPath).readLastCheckpointFile(engine).get()
    assert(cpm.version == 2L)
    assert(cpm.size == 6L)
    assert(!cpm.parts.isPresent, "classic pointer here has no `parts`")

    val serialized = new Checkpointer(logPath).readLastCheckpointSerialized(engine).get()
    assert(serialized.toJson().contains("\"checkpointSchema\":{"))
    assertJsonEquals(raw, serialized.toJson())
  }

  test("readLastCheckpointSerialized exposes utf8 bytes for softstore relay") {
    val logPath = logPathFor("v2-checkpoint-parquet")
    val raw = readLastCheckpoint(logPath)

    val serialized = new Checkpointer(logPath).readLastCheckpointSerialized(engine).get()
    assert(new String(serialized.utf8Bytes(), StandardCharsets.UTF_8).trim == raw)
  }
}
