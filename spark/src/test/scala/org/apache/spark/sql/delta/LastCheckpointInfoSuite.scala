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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.actions.{Checkpoint, ContentRoot, Metadata, Protocol}
import org.apache.spark.sql.delta.amt.{AMTLeafComparisons, AMTSingleAction, AMTWriteResult, DataManifestEntry, ManifestInfo, Tracking}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.google.common.io.{ByteStreams, Closeables}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.IOUtils

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

class LastCheckpointInfoSuite extends SharedSparkSession
  with DeltaSQLCommandTest {

  // same checkpoint schema for tests
  private val checkpointSchema = Some(new StructType().add("c1", IntegerType, nullable = false))

  private def jsonStringToChecksum(jsonStr: String): String = {
    val rootNode = JsonUtils.mapper.readValue(jsonStr, classOf[JsonNode])
    LastCheckpointInfo.treeNodeToChecksum(rootNode)
  }

  test("test json to checksum conversion with maps") {
    // test with different ordering and spaces, with different value data types
    val s1 = """{"k1":"v1","k4":"v4","k3":23.45,"k2":123}"""
    val s2 = """{"k1":"v1","k3":23.45,"k2":123,      "k4":"v4"}"""
    assert(jsonStringToChecksum(s1) === jsonStringToChecksum(s2))

    // test json with nested maps
    val s3 =
      """{"k1":"v1","k4":{"k41":"v41","k40":{"k401":401,"k402":"402"}},"k3":23.45,"k2":123}"""
    val s4 =
      """{"k1":"v1","k4":{"k40":{"k401":401,"k402":"402"},   "k41":"v41"},"k3":23.45,"k2":123}"""
    assert(jsonStringToChecksum(s3) === jsonStringToChecksum(s4))

    // test empty json
    val s5 = """{    }"""
    val s6 = """{}"""
    assert(jsonStringToChecksum(s5) === jsonStringToChecksum(s6))

    // negative test: value for a specific key k4 is not same.
    val s7 = """{"k1":"v1","k4":"v4","k3":23.45,"k2":123}"""
    val s8 = """{"k1":"v1","k4":"v1","k3":23.45,"k2":123}"""
    assert(jsonStringToChecksum(s7) != jsonStringToChecksum(s8))
  }

  test("test json to checksum conversion with array") {
    // has top level array and array values are json objects
    val s1 = """[{"id":"j1","stuff":"things"},{"stuff":"t2","id":"j2"}]"""
    val s2 = """[{"id" : "j1", "stuff" : "things"}, {"id" : "j2", "stuff" : "t2"}]"""
    assert(jsonStringToChecksum(s1) === jsonStringToChecksum(s2))

    // array as part of value for a json key and array value has single json object
    val s3 = """{"id":"j1","stuff":[{"hello": "world", "hello1": "world1"}]}"""
    val s4 = """{"id":   "j1","stuff":[{"hello1": "world1", "hello": "world"}]}"""
    assert(jsonStringToChecksum(s3) === jsonStringToChecksum(s4))

    // array as part of value for a json key and array values are multiple json objects
    val s5 = """{"id":"j1","stuff":[{"hello": "world"}, {"hello1": "world1"}]}"""
    val s6 = """{"id":   "j1","stuff":[{"hello":"world"},{"hello1":"world1"}]}"""
    assert(jsonStringToChecksum(s5) === jsonStringToChecksum(s6))

    // Negative case: array as part of value for a json key and array values are multiple json
    // objects with different order.
    val s7 = """{"id":"j1","stuff":[{"hello1": "world1"}, {"hello": "world"}]}"""
    val s8 = """{"id":   "j1","stuff":[{"hello":"world"},{"hello1":"world1"}]}"""
    assert(jsonStringToChecksum(s7) != jsonStringToChecksum(s8))

    // array has scalar string values
    val s9 = """{"id":"j1","stuff":["a", "b"]}"""
    val s10 = """{"stuff":["a","b"], "id":   "j1"}"""
    assert(jsonStringToChecksum(s9) === jsonStringToChecksum(s10))

    // array has scalar int values
    val s11 = """{"id":"j1","stuff":[1, 2]}"""
    val s12 = """{"stuff":[1,2], "id":   "j1"}"""
    assert(jsonStringToChecksum(s11) === jsonStringToChecksum(s12))

    // Negative case: array has scalar values in different order
    val s13 = """{"id":"j1","stuff":["a", "b", "c"]}"""
    val s14 = """{"id":"j1","stuff":["c", "a", "b"]}"""
    assert(jsonStringToChecksum(s13) != jsonStringToChecksum(s14))
  }

  // scalastyle:off line.size.limit
  test("test json normalization") {
    // test with different data types
    val s1 = """{"k1":"v1","k4":"v4","k3":23.45,"k2":123,"k6":null,"k5":true}"""
    val normalizedS1 = """"k1"="v1","k2"=123,"k3"=23.45,"k4"="v4","k5"=true,"k6"=null"""
    assert(jsonStringToChecksum(s1) === DigestUtils.md5Hex(normalizedS1))

    // test json with nested maps
    val s2 =
      """{"k1":"v1","k4":{"k41":"v41","k40":{"k401":401,"k402":"402"}},"k3":23.45,"k2":123}"""
    val normalizedS2 = """"k1"="v1","k2"=123,"k3"=23.45,"k4"+"k40"+"k401"=401,"k4"+"k40"+"k402"="402","k4"+"k41"="v41""""
    assert(jsonStringToChecksum(s2) === DigestUtils.md5Hex(normalizedS2))

    // test with arrays
    val s3 = """{"stuff":[{"hx": "wx","h1":"w1"}, {"h2": "w2"}],"id":1}"""
    val normalizedS3 = """"id"=1,"stuff"+0+"h1"="w1","stuff"+0+"hx"="wx","stuff"+1+"h2"="w2""""
    assert(jsonStringToChecksum(s3) === DigestUtils.md5Hex(normalizedS3))

    // test top level `checksum` key is ignored in canonicalization
    val s4 = """{"k1":"v1","checksum":"daswefdssfd","k3":23.45,"k2":123}"""
    val normalizedS4 = """"k1"="v1","k2"=123,"k3"=23.45"""
    assert(jsonStringToChecksum(s4) === DigestUtils.md5Hex(normalizedS4))

    // test empty json
    val s5 = """{    }"""
    val normalizedS5 = """"""
    assert(jsonStringToChecksum(s5) === DigestUtils.md5Hex(normalizedS5))

    // test with complex strings
    val s6 = """{"k0":"normal","k1":"'v1'","k4":"'v4","k3":":hello","k2":"\"double quote str\""}"""
    val normalizedS6 = """"k0"="normal","k1"="%27v1%27","k2"="%22double%20quote%20str%22","k3"="%3Ahello","k4"="%27v4""""
    assert(jsonStringToChecksum(s6) === DigestUtils.md5Hex(normalizedS6))

    // test covering different ASCII characters
    val s7 = """{"k0":"normal","k1":"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789%'`~!@#$%^&*()_+-={[}]|\\;:'\"\/?.>,<"}"""
    val normalizedS7 = """"k0"="normal","k1"="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789%25%27%60~%21%40%23%24%25%5E%26%2A%28%29_%2B-%3D%7B%5B%7D%5D%7C%5C%3B%3A%27%22%2F%3F.%3E%2C%3C""""
    assert(jsonStringToChecksum(s7) === DigestUtils.md5Hex(normalizedS7))

    // test with nested maps and arrays
    // This example is also part of Delta's PROTOCOL.md. We should keep these two in sync.
    val s8 = """{"k0":"'v 0'", "checksum": "adsaskfljadfkjadfkj", "k1":{"k2": 2, "k3": ["v3", [1, 2], {"k4": "v4", "k5": ["v5", "v6", "v7"]}]}}"""
    val normalizedS8 = """"k0"="%27v%200%27","k1"+"k2"=2,"k1"+"k3"+0="v3","k1"+"k3"+1+0=1,"k1"+"k3"+1+1=2,"k1"+"k3"+2+"k4"="v4","k1"+"k3"+2+"k5"+0="v5","k1"+"k3"+2+"k5"+1="v6","k1"+"k3"+2+"k5"+2="v7""""
    assert(jsonStringToChecksum(s8) === DigestUtils.md5Hex(normalizedS8))
    assert(jsonStringToChecksum(s8) === "6a92d155a59bf2eecbd4b4ec7fd1f875")

    // test non-ASCII character
    // scalastyle:off nonascii
    val s9 = s"""{"k0":"normal","k1":"a€+"}"""
    val normalizedS9 = """"k0"="normal","k1"="a%E2%82%AC%2B""""
    assert(jsonStringToChecksum(s9) === DigestUtils.md5Hex(normalizedS9))
    // scalastyle:on nonascii
  }
  // scalastyle:on line.size.limit

  test("test LastCheckpointInfo checksum") {
    val ci1 = LastCheckpointInfo(version = 1, size = 2, parts = Some(3),
      sizeInBytes = Some(20L), numOfAddFiles = Some(2L), checkpointSchema = checkpointSchema)
    val (stored1, actual1) =
      LastCheckpointInfo.getChecksums(LastCheckpointInfo.serializeToJson(ci1, addChecksum = true))
    assert(stored1 === Some(actual1))

    // checksum mismatch when version changes.
    val ci2 = LastCheckpointInfo(version = 2, size = 2, parts = Some(3),
      sizeInBytes = Some(20L), numOfAddFiles = Some(2L),
      checkpointSchema = checkpointSchema)
    val (stored2, actual2) =
      LastCheckpointInfo.getChecksums(LastCheckpointInfo.serializeToJson(ci2, addChecksum = true))
    assert(stored2 === Some(actual2))
    assert(stored2 != stored1)

    // `checksum` doesn't participate in `actualChecksum` calculation.
    val ci3 = LastCheckpointInfo(version = 1, size = 2, parts = Some(3),
      checksum = Some("XYZ"), sizeInBytes = Some(20L), numOfAddFiles = Some(2L),
      checkpointSchema = checkpointSchema)
    val (stored3, actual3) =
      LastCheckpointInfo.getChecksums(LastCheckpointInfo.serializeToJson(ci3, addChecksum = true))
    assert(stored3 === Some(actual3))
    assert(stored3 === stored1)

    // checksum doesn't depend on spaces and order of field
    val json1 = """{"version":1,"size":2,"parts":3}"""
    val json2 = """{"version":1 ,"parts":3,"size":2}"""
    assert(jsonStringToChecksum(json1) === jsonStringToChecksum(json2))
    // `checksum` is ignored while calculating json
    val json3 = """{"version":1 ,"parts":3,"size":2,"checksum":"xyz"}"""
    assert(jsonStringToChecksum(json1) === jsonStringToChecksum(json3))
    // Change in any value changes the checksum
    val json4 = """{"version":4,"size":2,"parts":3}"""
    assert(jsonStringToChecksum(json1) != jsonStringToChecksum(json4))

  }

  test("test backward compatibility - json without checksum is deserialized properly") {
    val jsonStr = """{"version":1,"size":2,"parts":3,"sizeInBytes":20,"numOfAddFiles":2,""" +
     """"checkpointSchema":{"type":"struct","fields":[{"name":"c1","type":"integer"""" +
     ""","nullable":false,"metadata":{}}]}}"""
    val expectedLastCheckpointInfo = LastCheckpointInfo(
      version = 1, size = 2, parts = Some(3), sizeInBytes = Some(20), numOfAddFiles = Some(2),
      checkpointSchema = Some(new StructType().add("c1", IntegerType, nullable = false)))
    assert(LastCheckpointInfo.deserializeFromJson(jsonStr, validate = true) ===
      expectedLastCheckpointInfo)
  }

  test("LastCheckpointInfo - serialize/deserialize") {
    val ci1 = LastCheckpointInfo(version = 1, size = 2, parts = Some(3),
      checksum = Some("XYZ"), sizeInBytes = Some(20L), numOfAddFiles = Some(2L),
      checkpointSchema = checkpointSchema)
    val ci2 = LastCheckpointInfo(version = 1, size = 2, parts = Some(3), checksum = None,
      sizeInBytes = Some(20L), numOfAddFiles = Some(2L),
      checkpointSchema = checkpointSchema)

    val actualChecksum = LastCheckpointInfo.getChecksums(
      LastCheckpointInfo.serializeToJson(ci1, addChecksum = true))._2
    val ciWithCorrectChecksum = ci1.copy(checksum = Some(actualChecksum))

    for(ci <- Seq(ci1, ci2)) {
      val json = LastCheckpointInfo.serializeToJson(ci, addChecksum = true)
      assert(LastCheckpointInfo.deserializeFromJson(json, validate = true)
        === ciWithCorrectChecksum)
      // The below assertion also validates that fields version/size/parts are in the beginning of
      // the json.
      assert(LastCheckpointInfo.serializeToJson(ci, addChecksum = true) ===
        """{"version":1,"size":2,"parts":3,"sizeInBytes":20,"numOfAddFiles":2,""" +
          s""""checkpointSchema":${JsonUtils.toJson(checkpointSchema)},""" +
          """"checksum":"524d4e2226f3c3f923df4ee42dae347e"}""")
    }

    assert(LastCheckpointInfo.serializeToJson(ci1, addChecksum = true)
      === LastCheckpointInfo.serializeToJson(ci2, addChecksum = true))
  }

  /** A [[Checkpoint]] action describing `version`, with a root manifest at `rootPath`. */
  private def sampleCheckpointAction(
      version: Long = 1L,
      rootPath: String = "metadata/root-abc.parquet",
      rootSizeInBytes: Long = 4096L): Checkpoint = Checkpoint(
    version = version,
    contentRoot = ContentRoot(path = rootPath, sizeInBytes = rootSizeInBytes, version = version),
    protocol = Protocol(minReaderVersion = 3, minWriterVersion = 7),
    metaData = Metadata(id = "metadata-id", name = "t"),
    domainMetadata = Nil,
    txns = Nil,
    sidecars = Nil)

  private def sampleAMTCheckpoint(
      version: Long = 1L,
      leaves: Seq[DataManifestEntry] =
        Seq(sampleLeaf("metadata/leaf-1.parquet"))): LastAMTCheckpoint =
    LastAMTCheckpoint(
      manifestCommitVersion = version,
      checkpoint = Some(sampleCheckpointAction(version)),
      leaves = Some(leaves))

  /**
   * A leaf pointer carrying `recordCount` entries and occupying `sizeInBytes` on disk. Every
   * optional field is populated with a distinct non-empty value so the round-trip tests actually
   * exercise their serialization -- in particular the byte-array fields (`key_metadata`,
   * `deleted_positions`, `dv`) whose base64 encoding is easy to get wrong.
   */
  private def sampleLeaf(
      location: String,
      recordCount: Long = 1L,
      sizeInBytes: Long = 1024L): DataManifestEntry = DataManifestEntry(
    location = location,
    file_format = AMTSingleAction.FileFormatParquet,
    tracking = Tracking(
      status = Tracking.Status.Added,
      snapshot_id = Some(11L),
      dv_snapshot_id = Some(12L),
      sequence_number = Some(13L),
      file_sequence_number = Some(14L),
      first_row_id = Some(15L),
      deleted_positions = Some(Array[Byte](1, 2, 3)),
      replaced_positions = Some(Array[Byte](4, 5))),
    record_count = recordCount,
    file_size_in_bytes = sizeInBytes,
    manifest_info = ManifestInfo(
      added_files_count = recordCount.toInt,
      existing_files_count = 2,
      deleted_files_count = 3,
      replaced_files_count = 4,
      modified_files_count = 5,
      added_rows_count = 100L,
      existing_rows_count = 200L,
      deleted_rows_count = 300L,
      replaced_rows_count = 400L,
      modified_rows_count = 500L,
      min_sequence_number = 7L,
      dv = Some(Array[Byte](9, 8, 7)),
      dv_cardinality = Some(5L)),
    partition = Some(Map("part_col" -> "part_val")),
    spec_id = Some(1),
    content_stats = Some("""{"minValues":{"col-1":1}}"""),
    key_metadata = Some(Array[Byte](16, 17, 18)),
    split_offsets = Some(Seq(0L, 128L, 256L)))

  private def sampleAMTWriteResult(
      contentRootVersion: Long = 1L,
      leaves: Seq[DataManifestEntry] = Seq(sampleLeaf("metadata/leaf-1.parquet"))): AMTWriteResult =
    AMTWriteResult(
      contentRootVersion = contentRootVersion,
      checkpoint = sampleCheckpointAction(contentRootVersion),
      leaves = leaves,
      includeActionsInCommitJson = true)

  /** Reads `_last_checkpoint` back as raw json. */
  private def readLastCheckpointFileAsJson(log: DeltaLog): String = {
    val fs = log.LAST_CHECKPOINT.getFileSystem(log.newDeltaHadoopConf())
    val is = fs.open(log.LAST_CHECKPOINT)
    try {
      IOUtils.toString(is, "UTF-8")
    } finally {
      is.close()
    }
  }

  ////////////////////////////////////////
  // writeLastCheckpointFileForAMT
  ////////////////////////////////////////

  test("writeLastCheckpointFileForAMT - writes an AMT-format _last_checkpoint") {
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)
      val log = DeltaLog.forTable(spark, dir)
      val writeResult = sampleAMTWriteResult(contentRootVersion = 3)

      log.writeLastCheckpointFileForAMT(manifestCommitVersion = 4, writeResult)

      val info = log.readLastCheckpointFile().getOrElse(
        fail("writeLastCheckpointFileForAMT must produce a readable _last_checkpoint."))
      // The pointer describes the manifest tree's content-root version, and records the leaf count
      // in `parts`.
      assert(info.version == 3)
      assert(info.parts == Some(writeResult.leaves.size))
      // `size` (action count) and `numOfAddFiles` are intentionally unavailable for AMT.
      assert(info.size == -1)
      assert(info.numOfAddFiles.isEmpty)
      // The embedded Checkpoint action survives the round trip, so a cold reader can locate the
      // root manifest without reading the commit json.
      val amt = info.amtCheckpoint.getOrElse(fail("The written info must carry an amtCheckpoint."))
      assert(amt.checkpoint == Some(writeResult.checkpoint))
      assert(amt.manifestCommitVersion == 4)
      assert(amt.checkpoint.map(_.contentRoot.path) == Some("metadata/root-abc.parquet"))
      AMTLeafComparisons.assertLeavesEqual(amt.leaves.get, writeResult.leaves)
    }
  }

  test("writeLastCheckpointFileForAMT - records the leaf count") {
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)
      val log = DeltaLog.forTable(spark, dir)
      val leaves = Seq(sampleLeaf("metadata/leaf-1.parquet"), sampleLeaf("metadata/leaf-2.parquet"))
      log.writeLastCheckpointFileForAMT(
        manifestCommitVersion = 4, sampleAMTWriteResult(contentRootVersion = 3, leaves = leaves))

      val info = log.readLastCheckpointFile().get
      assert(info.parts == Some(2), "`parts` carries the number of leaves for an AMT checkpoint.")
      assert(info.version == 3, "`version` carries the content root version.")
      assert(info.amtCheckpoint.map(_.manifestCommitVersion) == Some(4L))
    }
  }

  test("writeLastCheckpointFileForAMT - overwrites the previous pointer") {
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)
      val log = DeltaLog.forTable(spark, dir)

      log.writeLastCheckpointFileForAMT(
        manifestCommitVersion = 4, sampleAMTWriteResult(contentRootVersion = 3))
      assert(log.readLastCheckpointFile().map(_.version) == Some(3L))

      // A later manifest commit replaces the pointer wholesale: `_last_checkpoint` names only the
      // latest checkpoint, so the newer content root must win.
      val newer = AMTWriteResult(
        contentRootVersion = 5,
        checkpoint = sampleCheckpointAction(version = 5, rootPath = "metadata/root-def.parquet"),
        leaves = Seq(sampleLeaf("metadata/leaf-9.parquet")),
        includeActionsInCommitJson = true)
      log.writeLastCheckpointFileForAMT(manifestCommitVersion = 6, newer)

      val info = log.readLastCheckpointFile().get
      assert(info.version == 5)
      assert(info.amtCheckpoint.flatMap(_.checkpoint).map(_.contentRoot.path)
        == Some("metadata/root-def.parquet"))
      assert(info.amtCheckpoint.map(_.manifestCommitVersion) == Some(6))
    }
  }

  test("writeLastCheckpointFileForAMT - honors the checksum config") {
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)
      val log = DeltaLog.forTable(spark, dir)
      val writeResult = sampleAMTWriteResult(contentRootVersion = 1)

      withSQLConf(DeltaSQLConf.LAST_CHECKPOINT_CHECKSUM_ENABLED.key -> "true") {
        log.writeLastCheckpointFileForAMT(manifestCommitVersion = 1, writeResult)
        assert(readLastCheckpointFileAsJson(log).contains("checksum"))
      }

      withSQLConf(DeltaSQLConf.LAST_CHECKPOINT_CHECKSUM_ENABLED.key -> "false") {
        log.writeLastCheckpointFileForAMT(manifestCommitVersion = 1, writeResult)
        assert(!readLastCheckpointFileAsJson(log).contains("checksum"))
      }
    }
  }

  test("LastCheckpointInfo - amtCheckpoint serialize/deserialize round-trip") {
    val ci = LastCheckpointInfo(version = 1, size = -1, parts = None, sizeInBytes = None,
      numOfAddFiles = None, checkpointSchema = None,
      checkpointType = Some(LastCheckpointInfo.CheckpointType.AMT),
      amtCheckpoint = Some(sampleAMTCheckpoint()))
    val json = LastCheckpointInfo.serializeToJson(ci, addChecksum = true)
    val deserialized = LastCheckpointInfo.deserializeFromJson(json, validate = true)
    val amt = deserialized.amtCheckpoint.getOrElse(fail("round-trip dropped the amtCheckpoint."))
    val expectedAmt = ci.amtCheckpoint.get
    assert(amt.manifestCommitVersion == expectedAmt.manifestCommitVersion)
    assert(amt.checkpoint == expectedAmt.checkpoint) // Checkpoint carries no Array[Byte] fields.
    AMTLeafComparisons.assertLeavesEqual(amt.leaves.get, expectedAmt.leaves.get)
    assert(deserialized.checkpointType == Some(LastCheckpointInfo.CheckpointType.AMT))
    // Compare the rest non-array fields by `==`.
    assert(deserialized.copy(amtCheckpoint = None, checkpointType = None, checksum = None)
      == ci.copy(amtCheckpoint = None, checkpointType = None))
  }

  test("LastCheckpointInfo - checkpointType serializes as a bare string") {
    val ci = LastCheckpointInfo(version = 1, size = -1, parts = None, sizeInBytes = None,
      numOfAddFiles = None, checkpointSchema = None,
      checkpointType = Some(LastCheckpointInfo.CheckpointType.AMT),
      amtCheckpoint = Some(sampleAMTCheckpoint()))
    val json = LastCheckpointInfo.serializeToJson(ci, addChecksum = false)
    // The type must land as its bare `name`, not as a nested `{"name":...}` object. Jackson would
    // bean-serialize the sealed type without `@JsonValue`, which also changes the checksum shape
    // (one `"checkpointType"` entry instead of a `"checkpointType"+"name"` entry) and breaks
    // deserialization.
    assert(json.contains(s""""checkpointType":"${LastCheckpointInfo.CheckpointType.AMT.name}""""),
      s"Expected a bare-string checkpointType in: $json")
    assert(!json.contains(""""checkpointType":{"""),
      s"checkpointType must not serialize as an object: $json")

    // And it deserializes back to the same singleton, with the checksum validating over it.
    val withChecksum = LastCheckpointInfo.serializeToJson(ci, addChecksum = true)
    val deserialized = LastCheckpointInfo.deserializeFromJson(withChecksum, validate = true)
    assert(deserialized.checkpointType == Some(LastCheckpointInfo.CheckpointType.AMT))
  }

  test("LastCheckpointInfo - an unknown checkpointType decodes to UNKNOWN") {
    val ci = LastCheckpointInfo(version = 1, size = -1, parts = None, sizeInBytes = None,
      numOfAddFiles = None, checkpointSchema = None,
      checkpointType = Some(LastCheckpointInfo.CheckpointType.UNKNOWN),
      amtCheckpoint = Some(sampleAMTCheckpoint()))
    val json = LastCheckpointInfo.serializeToJson(ci, addChecksum = false)
    val forged = json.replace(
      s""""checkpointType":"${LastCheckpointInfo.CheckpointType.UNKNOWN.name}"""",
      """"checkpointType":"SomeFutureTree"""")
    val deserialized = LastCheckpointInfo.deserializeFromJson(forged, validate = false)
    assert(deserialized.checkpointType == Some(LastCheckpointInfo.CheckpointType.UNKNOWN),
      s"An unrecognized checkpointType must decode to UNKNOWN, got ${deserialized.checkpointType}.")
    // The amtCheckpoint alongside the unknown type must still be decoded, not dropped.
    val amt = deserialized.amtCheckpoint.getOrElse(
      fail("The amtCheckpoint must survive an unknown checkpointType."))
    assert(amt.manifestCommitVersion == ci.amtCheckpoint.get.manifestCommitVersion)
    assert(amt.checkpoint == ci.amtCheckpoint.get.checkpoint)
    AMTLeafComparisons.assertLeavesEqual(amt.leaves.get, ci.amtCheckpoint.get.leaves.get)
  }

  test("LastCheckpointInfo - json with duplicate keys should fail") {
    val jsonString =
      """{"version":1,"size":3,"parts":3,"checksum":"d84a0aa11c93304d57feca6acaceb7fb","size":2}"""
    intercept[MismatchedInputException] {
      LastCheckpointInfo.deserializeFromJson(jsonString, validate = true)
    }
    // Deserialization shouldn't fail when validate is false and the last `size` overrides the
    // previous size.
    assert(LastCheckpointInfo.deserializeFromJson(jsonString, validate = false).size === 2)
  }

  test("LastCheckpointInfo - test checksum is written only when config is enabled") {
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)
      val log = DeltaLog.forTable(spark, dir)

      withSQLConf(DeltaSQLConf.LAST_CHECKPOINT_CHECKSUM_ENABLED.key -> "true") {
        DeltaLog.forTable(spark, dir).checkpoint()
        assert(readLastCheckpointFileAsJson(log).contains("checksum"))
      }

      spark.range(10).write.mode("append").format("delta").save(dir.getAbsolutePath)
      withSQLConf(DeltaSQLConf.LAST_CHECKPOINT_CHECKSUM_ENABLED.key -> "false") {
        DeltaLog.forTable(spark, dir).checkpoint()
        assert(!readLastCheckpointFileAsJson(log).contains("checksum"))
      }
    }
  }

  test("Suppress optional fields in _last_checkpoint") {
    val expectedStr = """{"version":1,"size":2,"parts":3}"""
    val info = LastCheckpointInfo(
      version = 1, size = 2, parts = Some(3), sizeInBytes = Some(20), numOfAddFiles = Some(2),
      checkpointSchema = Some(new StructType().add("c1", IntegerType, nullable = false)))
    val serializedJson = LastCheckpointInfo.serializeToJson(
      info, addChecksum = true, suppressOptionalFields = true)
    assert(serializedJson === expectedStr)

    val expectedStrNoPart = """{"version":1,"size":2}"""
    val serializedJsonNoPart = LastCheckpointInfo.serializeToJson(
      info.copy(parts = None), addChecksum = true, suppressOptionalFields = true)
    assert(serializedJsonNoPart === expectedStrNoPart)
  }

  test("read and write _last_checkpoint with optional fields suppressed") {
    withTempDir { dir =>
      withSQLConf(DeltaSQLConf.SUPPRESS_OPTIONAL_LAST_CHECKPOINT_FIELDS.key -> "true") {
        // Create a Delta table with a checkpoint.
        spark.range(10).write.format("delta").save(dir.getAbsolutePath)
        DeltaLog.forTable(spark, dir).checkpoint()
        DeltaLog.clearCache()

        val log = DeltaLog.forTable(spark, dir)
        val metadata = log.readLastCheckpointFile().get
        val trimmed = metadata.productIterator.drop(3).forall {
          case o: Option[_] => o.isEmpty
        }
        assert(trimmed, s"Unexpected fields in _last_checkpoint: $metadata")
      }
    }
  }

}
