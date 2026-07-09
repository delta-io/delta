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
 * End-to-end tests for `Checkpointer.readLastCheckpointFile` capturing the columnar fields of the
 * `_last_checkpoint` pointer (including the V2 checkpoint block) and re-serializing them via
 * `CheckpointMetaData.toJson`.
 *
 * Uses the real `DefaultJsonHandler` against the golden `_last_checkpoint` fixtures so the schema
 * projection, nested-struct/array parsing, and JSON serialization are all exercised together.
 *
 * Note: the top-level `checkpointSchema` field is a polymorphic schema-of-schema that the columnar
 * reader cannot project into a fixed schema. It is captured as raw JSON text and `toJson` splices
 * it back in as a JSON object, so it round-trips like every other field.
 */
class LastCheckpointHintSuite extends AnyFunSuite {

  private val engine = DefaultEngine.create(new Configuration())

  /** Parses two JSON strings and asserts semantic equality (object key order is irrelevant). */
  private def assertJsonEquals(expected: String, actual: String): Unit = {
    val expectedNode = JsonUtils.mapper().readTree(expected)
    val actualNode = JsonUtils.mapper().readTree(actual)
    assert(
      expectedNode == actualNode,
      s"JSON mismatch.\n expected: $expectedNode\n actual:   $actualNode")
  }

  private def logPathFor(goldenTable: String): Path =
    new Path(new Path(goldenTablePath(goldenTable)), "_delta_log")

  /** Reads the verbatim (trimmed) contents of the `_last_checkpoint` file under `logPath`. */
  private def readLastCheckpoint(logPath: Path): String = {
    val bytes = Files.readAllBytes(
      Paths.get(new Path(logPath, Checkpointer.LAST_CHECKPOINT_FILE_NAME).toString))
    new String(bytes, StandardCharsets.UTF_8).trim
  }

  test("readLastCheckpointFile captures the full V2 (json format) pointer and round-trips it") {
    val logPath = logPathFor("v2-checkpoint-json")
    val expectedJson = readLastCheckpoint(logPath)

    val cpmOpt = new Checkpointer(logPath).readLastCheckpointFile(engine)
    assert(cpmOpt.isPresent, "expected to read the _last_checkpoint pointer")
    val cpm = cpmOpt.get()

    assert(cpm.version == 2L)
    assert(cpm.size == 9L)
    assert(cpm.sizeInBytes == Optional.of(19554L))
    assert(cpm.numOfAddFiles == Optional.of(4L))
    assert(cpm.checksum.isPresent)
    assert(!cpm.parts.isPresent, "V2 pointer has no `parts`")
    assert(cpm.v2Checkpoint.isPresent, "V2 pointer must carry the v2Checkpoint block")
    assert(!cpm.checkpointSchema.isPresent, "this fixture carries no checkpointSchema")

    val actualJson = cpm.toJson()
    assert(actualJson.contains("v2Checkpoint"))
    assert(actualJson.contains("sidecarFiles"))
    assert(actualJson.contains("nonFileActions"))
    assert(actualJson.contains("checkpointMetadata"))
    assert(!actualJson.contains("checkpointSchema"))
    assertJsonEquals(expectedJson, actualJson)
  }

  test("V2 (parquet format) pointer round-trips every field including checkpointSchema") {
    val logPath = logPathFor("v2-checkpoint-parquet")
    val rawJson = readLastCheckpoint(logPath)

    val cpm = new Checkpointer(logPath).readLastCheckpointFile(engine).get()
    assert(cpm.version == 2L)
    assert(cpm.v2Checkpoint.isPresent)
    assert(cpm.sizeInBytes.isPresent)
    assert(cpm.numOfAddFiles.isPresent)
    assert(cpm.checkpointSchema.isPresent, "checkpointSchema must be captured")

    val parsedSchema = DataTypeJsonSerDe.deserializeStructType(cpm.checkpointSchema.get())
    assert(parsedSchema.length() > 0, "checkpointSchema should parse to a non-empty StructType")

    val actualJson = cpm.toJson()
    // Spliced back in as a JSON object, not an escaped string literal.
    assert(actualJson.contains("\"checkpointSchema\":{"))
    assert(!actualJson.contains("\"checkpointSchema\":\""))
    assertJsonEquals(rawJson, actualJson)
  }

  test("classic checkpoint (no v2Checkpoint) round-trips its columnar fields") {
    // A classic (non-V2) pointer that carries a top-level `checkpointSchema` but has no
    // `v2Checkpoint`, `parts`, or `tags`.
    val logPath = logPathFor("spark-variant-checkpoint")
    val raw = readLastCheckpoint(logPath)

    val cpm = new Checkpointer(logPath).readLastCheckpointFile(engine).get()
    assert(cpm.version == 2L)
    assert(cpm.size == 6L)
    assert(cpm.sizeInBytes == Optional.of(21929L))
    assert(cpm.numOfAddFiles == Optional.of(4L))
    assert(cpm.checksum == Optional.of("a8d400a03ead8a86dbb412f2a693e26e"))
    assert(!cpm.parts.isPresent, "classic pointer here has no `parts`")
    assert(!cpm.v2Checkpoint.isPresent, "classic pointer has no v2Checkpoint")
    assert(cpm.checkpointSchema.isPresent, "checkpointSchema must be captured")

    val parsedSchema = DataTypeJsonSerDe.deserializeStructType(cpm.checkpointSchema.get())
    assert(parsedSchema.length() > 0, "checkpointSchema should parse to a non-empty StructType")

    val actual = cpm.toJson()
    assert(actual.contains("\"checkpointSchema\":{"))
    assert(!actual.contains("\"checkpointSchema\":\""))
    // Reproduces the pointer in full.
    assertJsonEquals(raw, actual)
  }
}
