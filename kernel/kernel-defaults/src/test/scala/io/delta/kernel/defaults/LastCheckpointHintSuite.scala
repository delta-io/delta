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
import io.delta.kernel.internal.checkpoints.{Checkpointer, CheckpointMetaData}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.JsonUtils

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

/**
 * End-to-end tests for `Checkpointer.readLastCheckpointFile` capturing the full `_last_checkpoint`
 * pointer (including the V2 checkpoint block) and re-serializing it via `CheckpointMetaData.toJson`.
 *
 * Uses the real `DefaultJsonHandler` against the golden `_last_checkpoint` fixtures so the schema
 * projection, nested-struct/array parsing, and JSON serialization are all exercised together.
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

  test("readLastCheckpointFile captures the full V2 (json format) pointer and round-trips it") {
    val logPath = logPathFor("v2-checkpoint-json")
    val expectedJson = {
      val bytes = java.nio.file.Files.readAllBytes(
        java.nio.file.Paths.get(new Path(logPath, Checkpointer.LAST_CHECKPOINT_FILE_NAME).toString))
      new String(bytes, java.nio.charset.StandardCharsets.UTF_8).trim
    }

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

    val actualJson = cpm.toJson()
    assert(actualJson.contains("v2Checkpoint"))
    assert(actualJson.contains("sidecarFiles"))
    assert(actualJson.contains("nonFileActions"))
    assert(actualJson.contains("checkpointMetadata"))
    assertJsonEquals(expectedJson, actualJson)
  }

  test("full V2 (parquet format) pointer round-trips including checkpointSchema") {
    // The parquet-format golden also carries a top-level `checkpointSchema` (a recursive,
    // polymorphic schema-of-schema object). It cannot be projected by the columnar reader, so the
    // consumer extracts it from the raw bytes and attaches it via fromRow(row, checkpointSchema).
    val logPath = logPathFor("v2-checkpoint-parquet")
    val rawJson = {
      val bytes = java.nio.file.Files.readAllBytes(
        java.nio.file.Paths.get(new Path(logPath, Checkpointer.LAST_CHECKPOINT_FILE_NAME).toString))
      new String(bytes, java.nio.charset.StandardCharsets.UTF_8).trim
    }

    // Fast path (kernel's own): checkpointSchema is NOT captured.
    val fast = new Checkpointer(logPath).readLastCheckpointFile(engine)
    assert(fast.isPresent)
    assert(!fast.get().checkpointSchema.isPresent, "fast read must not capture checkpointSchema")

    // Full-pointer path (caching consumer): extract checkpointSchema from the raw bytes and attach.
    val row = new Checkpointer(logPath).readLastCheckpointFile(engine).get().toRow()
    val checkpointSchema = CheckpointMetaData.extractCheckpointSchema(rawJson)
    assert(checkpointSchema.isPresent, "parquet golden carries a checkpointSchema")
    val cpm = CheckpointMetaData.fromRow(row, checkpointSchema)

    assert(cpm.version == 2L)
    assert(cpm.v2Checkpoint.isPresent)
    assert(cpm.sizeInBytes.isPresent)
    assert(cpm.numOfAddFiles.isPresent)

    // Full round-trip: every field, INCLUDING checkpointSchema, reproduced (order-insensitive).
    val actualJson = cpm.toJson()
    assert(actualJson.contains("checkpointSchema"))
    assertJsonEquals(rawJson, actualJson)
  }

  test("extractCheckpointSchema returns empty when the field is absent") {
    // The json-format golden has no checkpointSchema.
    val logPath = logPathFor("v2-checkpoint-json")
    val rawJson = new String(
      java.nio.file.Files.readAllBytes(
        java.nio.file.Paths.get(new Path(
          logPath,
          Checkpointer.LAST_CHECKPOINT_FILE_NAME).toString)),
      java.nio.charset.StandardCharsets.UTF_8)
    assert(!CheckpointMetaData.extractCheckpointSchema(rawJson).isPresent)
  }

  test(
    "classic checkpoint with checkpointSchema and no v2Checkpoint round-trips byte-identically") {
    val raw = {
      val stream = getClass.getClassLoader
        .getResourceAsStream("last-checkpoint-classic-with-schema.json")
      assert(stream != null, "test resource not found")
      try new String(stream.readAllBytes(), StandardCharsets.UTF_8).trim
      finally stream.close()
    }

    // Lay the blob down as a real _last_checkpoint under a temp _delta_log and read it via the
    // real DefaultJsonHandler.
    val tmpLogDir = Files.createTempDirectory("last-checkpoint-classic-").resolve("_delta_log")
    Files.createDirectories(tmpLogDir)
    Files.write(
      Paths.get(tmpLogDir.toString, Checkpointer.LAST_CHECKPOINT_FILE_NAME),
      raw.getBytes(StandardCharsets.UTF_8))
    val logPath = new Path(tmpLogDir.toString)

    // Fast (kernel) path captures the scalar fields but not checkpointSchema.
    val fast = new Checkpointer(logPath).readLastCheckpointFile(engine).get()
    assert(fast.version == 2L)
    assert(fast.size == 6L)
    assert(fast.sizeInBytes == Optional.of(21929L))
    assert(fast.numOfAddFiles == Optional.of(4L))
    assert(fast.checksum == Optional.of("a8d400a03ead8a86dbb412f2a693e26e"))
    assert(!fast.parts.isPresent, "classic pointer here has no `parts`")
    assert(!fast.v2Checkpoint.isPresent, "classic pointer has no v2Checkpoint")
    assert(!fast.checkpointSchema.isPresent, "fast read must not capture checkpointSchema")

    // Full-pointer path: attach checkpointSchema extracted from the raw bytes.
    val checkpointSchema = CheckpointMetaData.extractCheckpointSchema(raw)
    assert(checkpointSchema.isPresent, "this pointer carries a checkpointSchema")
    val cpm = CheckpointMetaData.fromRow(fast.toRow(), checkpointSchema)

    val actual = cpm.toJson()
    // The whole pointer (dominated by checkpointSchema) reconstructs byte-for-byte.
    assert(actual == raw, s"NOT byte-identical\n expected: $raw\n actual:   $actual")
    assertJsonEquals(raw, actual)
  }
}
