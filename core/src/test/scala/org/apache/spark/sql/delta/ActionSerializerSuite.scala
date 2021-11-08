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

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

// scalastyle:off: removeFile
class ActionSerializerSuite extends QueryTest with SharedSparkSession {

  roundTripCompare("Add",
    AddFile("test", Map.empty, 1, 1, dataChange = true))
  roundTripCompare("Add with partitions",
    AddFile("test", Map("a" -> "1"), 1, 1, dataChange = true))
  roundTripCompare("Add with stats",
    AddFile("test", Map.empty, 1, 1, dataChange = true, stats = "stats"))
  roundTripCompare("Add with tags",
    AddFile("test", Map.empty, 1, 1, dataChange = true, tags = Map("a" -> "1")))
  roundTripCompare("Add with empty tags",
    AddFile("test", Map.empty, 1, 1, dataChange = true, tags = Map.empty))

  roundTripCompare("Remove",
    RemoveFile("test", Some(2)))

  test("AddFile tags") {
    val action1 =
      AddFile(
        path = "a",
        partitionValues = Map.empty,
        size = 1,
        modificationTime = 2,
        dataChange = false,
        stats = null,
        tags = Map("key1" -> "val1", "key2" -> "val2"))
    val json1 =
      """{
        |  "add": {
        |    "path": "a",
        |    "partitionValues": {},
        |    "size": 1,
        |    "modificationTime": 2,
        |    "dataChange": false,
        |    "tags": {
        |      "key1": "val1",
        |      "key2": "val2"
        |    }
        |  }
        |}""".stripMargin
    assert(action1 === Action.fromJson(json1))
    assert(action1.json === json1.replaceAll("\\s", ""))

    val json2 =
      """{
        |  "add": {
        |    "path": "a",
        |    "partitionValues": {},
        |    "size": 1,
        |    "modificationTime": 2,
        |    "dataChange": false,
        |    "tags": {}
        |  }
        |}""".stripMargin
    val action2 =
      AddFile(
        path = "a",
        partitionValues = Map.empty,
        size = 1,
        modificationTime = 2,
        dataChange = false,
        stats = null,
        tags = Map.empty)
    assert(action2 === Action.fromJson(json2))
    assert(action2.json === json2.replaceAll("\\s", ""))
  }

  // This is the same test as "removefile" in OSS, but due to a Jackson library upgrade the behavior
  // has diverged between Spark 3.1 and Spark 3.2.
  // We don't believe this is a practical issue because all extant versions of Delta explicitly
  // write the dataChange field.
  test("remove file deserialization") {
    val removeJson = RemoveFile("a", Some(2L)).json
    assert(removeJson.contains(""""deletionTimestamp":2"""))
    assert(!removeJson.contains("""delTimestamp"""))
    val json1 = """{"remove":{"path":"a","deletionTimestamp":2,"dataChange":true}}"""
    val json2 = """{"remove":{"path":"a","dataChange":false}}"""
    val json4 = """{"remove":{"path":"a","deletionTimestamp":5}}"""
    assert(Action.fromJson(json1) === RemoveFile("a", Some(2L), dataChange = true))
    assert(Action.fromJson(json2) === RemoveFile("a", None, dataChange = false))
    assert(Action.fromJson(json4) === RemoveFile("a", Some(5L), dataChange = true))
  }

  roundTripCompare("SetTransaction",
    SetTransaction("a", 1, Some(1234L)))

  roundTripCompare("SetTransaction without lastUpdated",
    SetTransaction("a", 1, None))

  roundTripCompare("MetaData",
    Metadata(
      "id",
      "table",
      "testing",
      Format("parquet", Map.empty),
      new StructType().json,
      Seq("a")))

  test("extra fields") {
    // TODO reading from checkpoint
    Action.fromJson("""{"txn": {"test": 1}}""")
  }

  test("deserialization of CommitInfo without tags") {
    val expectedCommitInfo = CommitInfo(
      time = 123L,
      operation = "CONVERT",
      operationParameters = Map.empty,
      commandContext = Map.empty,
      readVersion = Some(23),
      isolationLevel = Some("SnapshotIsolation"),
      isBlindAppend = Some(true),
      operationMetrics = Some(Map("m1" -> "v1", "m2" -> "v2")),
      userMetadata = Some("123"),
      tags = None).copy(engineInfo = None)

    // json of commit info actions without tag or engineInfo field
    val json1 =
      """{"commitInfo":{"timestamp":123,"operation":"CONVERT",""" +
        """"operationParameters":{},"readVersion":23,""" +
        """"isolationLevel":"SnapshotIsolation","isBlindAppend":true,""" +
        """"operationMetrics":{"m1":"v1","m2":"v2"},"userMetadata":"123"}}""".stripMargin
    assert(Action.fromJson(json1) === expectedCommitInfo)
  }

  testActionSerDe(
    "Protocol - json serialization/deserialization",
    Protocol(minReaderVersion = 1, minWriterVersion = 2),
    expectedJson = """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}""".stripMargin)

  testActionSerDe(
    "SetTransaction (lastUpdated is None) - json serialization/deserialization",
    SetTransaction(appId = "app-1", version = 2L, lastUpdated = None),
    expectedJson = """{"txn":{"appId":"app-1","version":2}}""".stripMargin)

  testActionSerDe(
    "SetTransaction (lastUpdated is not None) - json serialization/deserialization",
    SetTransaction(appId = "app-2", version = 3L, lastUpdated = Some(4L)),
    expectedJson = """{"txn":{"appId":"app-2","version":3,"lastUpdated":4}}""".stripMargin)

  testActionSerDe(
    "AddFile (without tags) - json serialization/deserialization",
    AddFile("x=2/f1", partitionValues = Map("x" -> "2"),
      size = 10, modificationTime = 1, dataChange = true, stats = "{rowCount: 2}"),
    expectedJson = """{"add":{"path":"x=2/f1","partitionValues":{"x":"2"},"size":10,""" +
      """"modificationTime":1,"dataChange":true,"stats":"{rowCount: 2}"}}""".stripMargin)

  testActionSerDe(
    "AddFile (with tags) - json serialization/deserialization",
    AddFile("part=p1/f1", partitionValues = Map("x" -> "2"), size = 10, modificationTime = 1,
      dataChange = true, stats = "{rowCount: 2}", tags = Map("TAG1" -> "23")),
    expectedJson = """{"add":{"path":"part=p1/f1","partitionValues":{"x":"2"},"size":10""" +
      ""","modificationTime":1,"dataChange":true,"stats":"{rowCount: 2}",""" +
      """"tags":{"TAG1":"23"}}}"""
  )

  testActionSerDe(
    "RemoveFile (without tags) - json serialization/deserialization",
    AddFile("part=p1/f1", partitionValues = Map("x" -> "2"), size = 10, modificationTime = 1,
      dataChange = true, stats = "{rowCount: 2}").removeWithTimestamp(timestamp = 11),
    expectedJson = """{"remove":{"path":"part=p1/f1","deletionTimestamp":11,"dataChange":true,""" +
      """"extendedFileMetadata":true,"partitionValues":{"x":"2"},"size":10}}""".stripMargin)


  testActionSerDe(
    "AddCDCFile (without tags) - json serialization/deserialization",
    AddCDCFile("part=p1/f1", partitionValues = Map("x" -> "2"), size = 10),
    expectedJson = """{"cdc":{"path":"part=p1/f1","partitionValues":{"x":"2"},""" +
      """"size":10,"dataChange":false}}""".stripMargin)

  testActionSerDe(
    "AddCDCFile (with tags) - json serialization/deserialization",
    AddCDCFile("part=p2/f1", partitionValues = Map("x" -> "2"),
      size = 11, tags = Map("key1" -> "value1")),
    expectedJson = """{"cdc":{"path":"part=p2/f1","partitionValues":{"x":"2"},""" +
      """"size":11,"tags":{"key1":"value1"},"dataChange":false}}""".stripMargin)

  testActionSerDe(
    "Metadata (with all defaults) - json serialization/deserialization",
    Metadata(createdTime = Some(2222)),
    expectedJson = """{"metaData":{"id":"testId","format":{"provider":"parquet",""" +
    """"options":{}},"partitionColumns":[],"configuration":{},"createdTime":2222}}""")

  {
    val schemaStr = new StructType().add("a", "long").json
    testActionSerDe(
      "Metadata - json serialization/deserialization",
      Metadata(
        name = "t1",
        description = "desc",
        format = Format(provider = "parquet", options = Map("o1" -> "v1")),
        partitionColumns = Seq("a"),
        createdTime = Some(2222),
        configuration = Map("delta.enableXyz" -> "true"),
        schemaString = schemaStr),
      expectedJson = """{"metaData":{"id":"testId","name":"t1","description":"desc",""" +
        """"format":{"provider":"parquet","options":{"o1":"v1"}},""" +
        s""""schemaString":${JsonUtils.toJson(schemaStr)},"partitionColumns":["a"],""" +
        """"configuration":{"delta.enableXyz":"true"},"createdTime":2222}}""".stripMargin)
  }

  {
    // Test for CommitInfo
    val commitInfo = CommitInfo(
      time = 123L,
      operation = "CONVERT",
      operationParameters = Map.empty,
      commandContext = Map("clusterId" -> "23"),
      readVersion = Some(23),
      isolationLevel = Some("SnapshotIsolation"),
      isBlindAppend = Some(true),
      operationMetrics = Some(Map("m1" -> "v1", "m2" -> "v2")),
      userMetadata = Some("123"),
      tags = Some(Map("k1" -> "v1"))).copy(engineInfo = None)

    testActionSerDe(
      "CommitInfo (without operationParameters) - json serialization/deserialization",
      commitInfo,
      expectedJson = """{"commitInfo":{"timestamp":123,"operation":"CONVERT",""" +
        """"operationParameters":{},"clusterId":"23","readVersion":23,""" +
        """"isolationLevel":"SnapshotIsolation","isBlindAppend":true,""" +
        """"operationMetrics":{"m1":"v1","m2":"v2"},"userMetadata":"123",""" +
        """"tags":{"k1":"v1"}}}""".stripMargin)

    test("CommitInfo (with operationParameters) - json serialization/deserialization") {
      val operation = DeltaOperations.Convert(
        numFiles = 23L,
        partitionBy = Seq("a", "b"),
        collectStats = false,
        catalogTable = Some("t1"))
      val commitInfo1 = commitInfo.copy(operationParameters = operation.jsonEncodedValues)

      val expectedCommitInfoJson1 =
        """{"commitInfo":{"timestamp":123,"operation":"CONVERT",""" +
          """"operationParameters":{"numFiles":23,"partitionedBy":"[\"a\",\"b\"]",""" +
          """"collectStats":false,"catalogTable":"t1"},"clusterId":"23","readVersion":23,""" +
          """"isolationLevel":"SnapshotIsolation","isBlindAppend":true,""" +
          """"operationMetrics":{"m1":"v1","m2":"v2"},"userMetadata":"123",""" +
          """"tags":{"k1":"v1"}}}""".stripMargin
      assert(commitInfo1.json == expectedCommitInfoJson1)
      val newCommitInfo1 = Action.fromJson(expectedCommitInfoJson1).asInstanceOf[CommitInfo]
      // TODO: operationParameters serialization/deserialization is broken as it uses a custom
      //  serializer but a default deserializer and needs to be fixed.
      assert(newCommitInfo1.copy(operationParameters = Map.empty) ==
        commitInfo.copy(operationParameters = Map.empty))
    }

    testActionSerDe(
      "CommitInfo (with engineInfo) - json serialization/deserialization",
      commitInfo.copy(engineInfo = Some("Apache-Spark/3.1.1 Delta-Lake/10.1.0")),
      expectedJson = """{"commitInfo":{"timestamp":123,"operation":"CONVERT",""" +
        """"operationParameters":{},"clusterId":"23","readVersion":23,""" +
        """"isolationLevel":"SnapshotIsolation","isBlindAppend":true,""" +
        """"operationMetrics":{"m1":"v1","m2":"v2"},"userMetadata":"123",""" +
        """"tags":{"k1":"v1"},"engineInfo":"Apache-Spark/3.1.1 Delta-Lake/10.1.0"}}""".stripMargin)
  }

  private def roundTripCompare(name: String, actions: Action*) = {
    test(name) {
      val asJson = actions.map(_.json)
      val asObjects = asJson.map(Action.fromJson)

      assert(actions === asObjects)
    }
  }

  /** Test serialization/deserialization of [[Action]] by doing an actual commit */
  private def testActionSerDe(name: String, action: Action, expectedJson: String): Unit = {
    test(name) {
      withTempDir { tempDir =>
        val deltaLog = DeltaLog(spark, new Path(tempDir.getAbsolutePath))
        // Disable different delta validations so that the passed action can be committed in
        // all cases.
        withSQLConf(
          DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED.key -> "false",
          DeltaSQLConf.DELTA_STATE_RECONSTRUCTION_VALIDATION_ENABLED.key -> "false",
          DeltaSQLConf.DELTA_COMMIT_INFO_ENABLED.key -> "false") {

          // Do one empty commit so that protocol gets committed.
          deltaLog.startTransaction().commit(Seq(), ManualUpdate)

          // Commit the actual action.
          val version = deltaLog.startTransaction().commit(Seq(action), ManualUpdate)
          // Read the commit file and get the serialized committed actions
          val committedActions = deltaLog.store.read(
            FileNames.deltaFile(deltaLog.logPath, version),
            deltaLog.newDeltaHadoopConf())

          assert(committedActions.size == 1)
          val serializedJson = committedActions.head
          assert(serializedJson === expectedJson)
          val asObject = Action.fromJson(serializedJson)
          assert(action === asObject)
        }
      }
    }
  }
}
