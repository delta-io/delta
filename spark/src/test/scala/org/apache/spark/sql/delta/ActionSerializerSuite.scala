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

import java.util.UUID

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.{TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

// scalastyle:off: removeFile
class ActionSerializerSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

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
      tags = None,
      txnId = None).copy(engineInfo = None)

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
    expectedJson = """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}""")

  testActionSerDe(
    "Protocol - json serialization/deserialization with writer features",
    Protocol(minReaderVersion = 1, minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION)
      .withFeature(AppendOnlyTableFeature),
    expectedJson = """{"protocol":{"minReaderVersion":1,""" +
      s""""minWriterVersion":$TABLE_FEATURES_MIN_WRITER_VERSION,""" +
      """"writerFeatures":["appendOnly"]}}""")

  testActionSerDe(
    "Protocol - json serialization/deserialization with reader and writer features",
    Protocol(
      minReaderVersion = TABLE_FEATURES_MIN_READER_VERSION,
      minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION)
      .withFeature(TestLegacyReaderWriterFeature),
    expectedJson =
      s"""{"protocol":{"minReaderVersion":$TABLE_FEATURES_MIN_READER_VERSION,""" +
        s""""minWriterVersion":$TABLE_FEATURES_MIN_WRITER_VERSION,""" +
        """"readerFeatures":["testLegacyReaderWriter"],""" +
        """"writerFeatures":["testLegacyReaderWriter"]}}""")

  testActionSerDe(
    "Protocol - json serialization/deserialization with empty reader and writer features",
    Protocol(
      minReaderVersion = TABLE_FEATURES_MIN_READER_VERSION,
      minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION),
    expectedJson =
      s"""{"protocol":{"minReaderVersion":$TABLE_FEATURES_MIN_READER_VERSION,""" +
        s""""minWriterVersion":$TABLE_FEATURES_MIN_WRITER_VERSION,""" +
        """"readerFeatures":[],"writerFeatures":[]}}""")

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
      size = 10, modificationTime = 1, dataChange = true, stats = "{\"numRecords\": 2}"),
    expectedJson = """{"add":{"path":"x=2/f1","partitionValues":{"x":"2"},"size":10,""" +
      """"modificationTime":1,"dataChange":true,"stats":"{\"numRecords\": 2}"}}""".stripMargin)

  testActionSerDe(
    "AddFile (with tags) - json serialization/deserialization",
    AddFile("part=p1/f1", partitionValues = Map("x" -> "2"), size = 10, modificationTime = 1,
      dataChange = true, stats = "{\"numRecords\": 2}", tags = Map("TAG1" -> "23")),
    expectedJson = """{"add":{"path":"part=p1/f1","partitionValues":{"x":"2"},"size":10""" +
      ""","modificationTime":1,"dataChange":true,"stats":"{\"numRecords\": 2}",""" +
      """"tags":{"TAG1":"23"}}}"""
  )

  testActionSerDe(
    "RemoveFile (without tags) - json serialization/deserialization",
    AddFile("part=p1/f1", partitionValues = Map("x" -> "2"), size = 10, modificationTime = 1,
      dataChange = true, stats = "{\"numRecords\": 2}").removeWithTimestamp(timestamp = 11),
    expectedJson = """{"remove":{"path":"part=p1/f1","deletionTimestamp":11,"dataChange":true,""" +
      """"extendedFileMetadata":true,"partitionValues":{"x":"2"},"size":10,""" +
      """"stats":"{\"numRecords\": 2}"}}""")

  testActionSerDe(
    "RemoveFile (without tags and stats) - json serialization/deserialization",
    AddFile("part=p1/f1", partitionValues = Map("x" -> "2"), size = 10, modificationTime = 1,
        dataChange = true, stats = "{\"numRecords\": 2}")
      .removeWithTimestamp(timestamp = 11)
      .copy(stats = null),
    expectedJson = """{"remove":{"path":"part=p1/f1","deletionTimestamp":11,"dataChange":true,""" +
      """"extendedFileMetadata":true,"partitionValues":{"x":"2"},"size":10}}""")

  private def deletionVectorWithRelativePath: DeletionVectorDescriptor =
    DeletionVectorDescriptor.onDiskWithRelativePath(
      id = UUID.randomUUID(),
      randomPrefix = "a1",
      sizeInBytes = 10,
      cardinality = 2,
      offset = Some(10))

  private def deletionVectorWithAbsolutePath: DeletionVectorDescriptor =
    DeletionVectorDescriptor.onDiskWithAbsolutePath(
      path = "/test.dv",
      sizeInBytes = 10,
      cardinality = 2,
      offset = Some(10))

  private def deletionVectorInline: DeletionVectorDescriptor =
    DeletionVectorDescriptor.inlineInLog(Array(1, 2, 3, 4), 1)

  roundTripCompare("Add with deletion vector - relative path",
    AddFile(
      path = "test",
      partitionValues = Map.empty,
      size = 1,
      modificationTime = 1,
      dataChange = true,
      tags = Map.empty,
      deletionVector = deletionVectorWithRelativePath))
  roundTripCompare("Add with deletion vector - absolute path",
    AddFile(
      path = "test",
      partitionValues = Map.empty,
      size = 1,
      modificationTime = 1,
      dataChange = true,
      tags = Map.empty,
      deletionVector = deletionVectorWithAbsolutePath))
  roundTripCompare("Add with deletion vector - inline",
    AddFile(
      path = "test",
      partitionValues = Map.empty,
      size = 1,
      modificationTime = 1,
      dataChange = true,
      tags = Map.empty,
      deletionVector = deletionVectorInline))

  roundTripCompare("Remove with deletion vector - relative path",
    RemoveFile(
      path = "test",
      deletionTimestamp = Some(1L),
      extendedFileMetadata = Some(true),
      partitionValues = Map.empty,
      dataChange = true,
      size = Some(1L),
      tags = Map.empty,
      deletionVector = deletionVectorWithRelativePath))
  roundTripCompare("Remove with deletion vector - absolute path",
    RemoveFile(
      path = "test",
      deletionTimestamp = Some(1L),
      extendedFileMetadata = Some(true),
      partitionValues = Map.empty,
      dataChange = true,
      size = Some(1L),
      tags = Map.empty,
      deletionVector = deletionVectorWithAbsolutePath))
  roundTripCompare("Remove with deletion vector - inline",
    RemoveFile(
      path = "test",
      deletionTimestamp = Some(1L),
      extendedFileMetadata = Some(true),
      partitionValues = Map.empty,
      dataChange = true,
      size = Some(1L),
      tags = Map.empty,
      deletionVector = deletionVectorInline))

  // These make sure we don't accidentally serialise something we didn't mean to.
  testActionSerDe(
    name = "AddFile (with deletion vector) - json serialization/deserialization",
    action = AddFile(
      path = "test",
      partitionValues = Map.empty,
      size = 1,
      modificationTime = 1,
      dataChange = true,
      stats = """{"numRecords":3}""",
      tags = Map.empty,
      deletionVector = deletionVectorWithAbsolutePath),
    expectedJson =
      """
        |{"add":{
        |"path":"test",
        |"partitionValues":{},
        |"size":1,
        |"modificationTime":1,
        |"dataChange":true,
        |"stats":"{\"numRecords\":3}",
        |"tags":{},
        |"deletionVector":{
        |"storageType":"p",
        |"pathOrInlineDv":"/test.dv",
        |"offset":10,
        |"sizeInBytes":10,
        |"cardinality":2}}
        |}""".stripMargin.replaceAll("\n", ""),
    extraSettings = Seq(
      // Skip the table property check, so this write doesn't fail.
      DeltaSQLConf.DELETION_VECTORS_COMMIT_CHECK_ENABLED.key -> "false")
  )

  test("DomainMetadata action - json serialization/deserialization") {
    val table = "testTable"
    withTable(table) {
      sql(
        s"""
           | CREATE TABLE $table(id int) USING delta
           | tblproperties
           | ('${TableFeatureProtocolUtils.propertyKey(DomainMetadataTableFeature)}' = 'enabled')
           |""".stripMargin)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
      val domainMetadatas = DomainMetadata(
        domain = "testDomain",
        configuration = JsonUtils.toJson(Map("key1" -> "value1")),
        removed = false) :: Nil
      val version = deltaLog.startTransaction().commit(domainMetadatas, ManualUpdate)
      val committedActions = deltaLog.store.read(
        FileNames.deltaFile(deltaLog.logPath, version),
        deltaLog.newDeltaHadoopConf())
      assert(committedActions.size == 2)
      val serializedJson = committedActions.last
      val expectedJson =
        """
          |{"domainMetadata":{
          |"domain":"testDomain",
          |"configuration":
          |"{\"key1\":\"value1\"}",
          |"removed":false}
          |}""".stripMargin.replaceAll("\n", "")
      assert(serializedJson === expectedJson)
      val asObject = Action.fromJson(serializedJson)
      assert(domainMetadatas.head === asObject)
    }
  }

  test("CheckpointMetadata - serialize/deserialize") {
    val m1 = CheckpointMetadata(version = 1, tags = null) // tags are null
    val m2 = m1.copy(tags = Map()) // tags are empty
    val m3 = m1.copy( // tags are non empty
      tags = Map("k1" -> "v1", "schema" -> """{"type":"struct","fields":[]}""")
    )

    assert(m1.json === """{"checkpointMetadata":{"version":1}}""")
    assert(m2.json === """{"checkpointMetadata":{"version":1,"tags":{}}}""")
    assert(m3.json ===
      """{"checkpointMetadata":{"version":1,""" +
        """"tags":{"k1":"v1","schema":"{\"type\":\"struct\",\"fields\":[]}"}}}""")

    Seq(m1, m2, m3).foreach { metadata =>
      assert(metadata === JsonUtils.fromJson[SingleAction](metadata.json).unwrap)
    }
  }

  test("SidecarFile - serialize/deserialize") {
    val f1 = // tags are null
      SidecarFile(path = "/t1/p1", sizeInBytes = 1L, modificationTime = 3, tags = null)
    val f2 = f1.copy(tags = Map()) // tags are empty
    val f3 = f2.copy( // tags are non empty
      tags = Map("k1" -> "v1", "schema" -> """{"type":"struct","fields":[]}""")
    )

    assert(f1.json ===
      """{"sidecar":{"path":"/t1/p1","sizeInBytes":1,"modificationTime":3}}""")
    assert(f2.json ===
      """{"sidecar":{"path":"/t1/p1","sizeInBytes":1,""" +
        """"modificationTime":3,"tags":{}}}""")
    assert(f3.json ===
      """{"sidecar":{"path":"/t1/p1","sizeInBytes":1,"modificationTime":3,""" +
        """"tags":{"k1":"v1","schema":"{\"type\":\"struct\",\"fields\":[]}"}}}""".stripMargin)

    Seq(f1, f2, f3).foreach { file =>
      assert(file === JsonUtils.fromJson[SingleAction](file.json).unwrap)
    }
  }

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
    "AddCDCFile (without null value in partitionValues) - json serialization/deserialization",
    AddCDCFile("part=p1/f1", partitionValues = Map("x" -> null), size = 10),
    expectedJson = """{"cdc":{"path":"part=p1/f1","partitionValues":{"x":null},""" +
      """"size":10,"dataChange":false}}""".stripMargin)

  {
    // We want this metadata to be lazy so it is instantiated after `SparkFunSuite::beforeAll`.
    // This will ensure that `Utils.isTesting` returns true and that its id is set to 'testId'.
    lazy val metadata = Metadata(createdTime = Some(2222))
    testActionSerDe(
      "Metadata (with all defaults) - json serialization/deserialization",
      metadata,
      expectedJson = """{"metaData":{"id":"testId","format":{"provider":"parquet",""" +
        """"options":{}},"partitionColumns":[],"configuration":{},"createdTime":2222}}""")
  }

  {
    val schemaStr = new StructType().add("a", "long").json
    // We want this metadata to be lazy so it is instantiated after `SparkFunSuite::beforeAll`.
    // This will ensure that `Utils.isTesting` returns true and that its id is set to 'testId'.
    lazy val metadata = Metadata(
      name = "t1",
      description = "desc",
      format = Format(provider = "parquet", options = Map("o1" -> "v1")),
      partitionColumns = Seq("a"),
      createdTime = Some(2222),
      configuration = Map("delta.enableXyz" -> "true"),
      schemaString = schemaStr)
    testActionSerDe(
      "Metadata - json serialization/deserialization", metadata,
      expectedJson = """{"metaData":{"id":"testId","name":"t1","description":"desc",""" +
        """"format":{"provider":"parquet","options":{"o1":"v1"}},""" +
        s""""schemaString":${JsonUtils.toJson(schemaStr)},"partitionColumns":["a"],""" +
        """"configuration":{"delta.enableXyz":"true"},"createdTime":2222}}""".stripMargin)
    testActionSerDe(
      "Metadata with empty createdTime- json serialization/deserialization",
      metadata.copy(createdTime = None),
      expectedJson = """{"metaData":{"id":"testId","name":"t1","description":"desc",""" +
        """"format":{"provider":"parquet","options":{"o1":"v1"}},""" +
        s""""schemaString":${JsonUtils.toJson(schemaStr)},"partitionColumns":["a"],""" +
        """"configuration":{"delta.enableXyz":"true"}}}""".stripMargin)
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
      tags = Some(Map("k1" -> "v1")),
      txnId = Some("123")
    ).copy(engineInfo = None)

    testActionSerDe(
      "CommitInfo (without operationParameters) - json serialization/deserialization",
      commitInfo,
      expectedJson = """{"commitInfo":{"timestamp":123,"operation":"CONVERT",""" +
        """"operationParameters":{},"clusterId":"23","readVersion":23,""" +
        """"isolationLevel":"SnapshotIsolation","isBlindAppend":true,""" +
        """"operationMetrics":{"m1":"v1","m2":"v2"},"userMetadata":"123",""" +
        """"tags":{"k1":"v1"},"txnId":"123"}}""".stripMargin)

    test("CommitInfo (with operationParameters) - json serialization/deserialization") {
      val operation = DeltaOperations.Convert(
        numFiles = 23L,
        partitionBy = Seq("a", "b"),
        collectStats = false,
        catalogTable = Some("t1"),
        sourceFormat = Some("parquet"))
      val commitInfo1 = commitInfo.copy(operationParameters = operation.jsonEncodedValues)
      val expectedCommitInfoJson1 = // TODO JSON ordering differs between 2.12 and 2.13
        if (scala.util.Properties.versionNumberString.startsWith("2.13")) {
          """{"commitInfo":{"timestamp":123,"operation":"CONVERT","operationParameters"""" +
            """:{"catalogTable":"t1","numFiles":23,"partitionedBy":"[\"a\",\"b\"]",""" +
            """"sourceFormat":"parquet","collectStats":false},"clusterId":"23","readVersion"""" +
            """:23,"isolationLevel":"SnapshotIsolation","isBlindAppend":true,""" +
            """"operationMetrics":{"m1":"v1","m2":"v2"},""" +
            """"userMetadata":"123","tags":{"k1":"v1"},"txnId":"123"}}"""
        } else {
          """{"commitInfo":{"timestamp":123,"operation":"CONVERT","operationParameters"""" +
            """:{"catalogTable":"t1","numFiles":23,"partitionedBy":"[\"a\",\"b\"]",""" +
            """"sourceFormat":"parquet","collectStats":false},"clusterId":"23","readVersion""" +
            """":23,"isolationLevel":"SnapshotIsolation","isBlindAppend":true,""" +
            """"operationMetrics":{"m1":"v1","m2":"v2"},""" +
            """"userMetadata":"123","tags":{"k1":"v1"},"txnId":"123"}}"""
        }
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
        """"tags":{"k1":"v1"},"engineInfo":"Apache-Spark/3.1.1 Delta-Lake/10.1.0",""" +
        """"txnId":"123"}}""".stripMargin)
  }

  private def roundTripCompare(name: String, actions: Action*) = {
    test(name) {
      val asJson = actions.map(_.json)
      val asObjects = asJson.map(Action.fromJson)

      assert(actions === asObjects)
    }
  }

  /** Test serialization/deserialization of [[Action]] by doing an actual commit */
  private def testActionSerDe(
      name: String,
      action: => Action,
      expectedJson: String,
      extraSettings: Seq[(String, String)] = Seq.empty,
      testTags: Seq[org.scalatest.Tag] = Seq.empty): Unit = {
    import org.apache.spark.sql.delta.test.DeltaTestImplicits._
    test(name, testTags: _*) {
      withTempDir { tempDir =>
        val deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getAbsolutePath))
        // Disable different delta validations so that the passed action can be committed in
        // all cases.
        val settings = Seq(
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "1",
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "1",
          DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED.key -> "false") ++ extraSettings
        withSQLConf(settings: _*) {

          // Do one empty commit so that protocol gets committed.
          val protocol = Protocol(
            minReaderVersion = spark.conf.get(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION),
            minWriterVersion = spark.conf.get(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION))
          deltaLog.startTransaction().commitManually(protocol, Metadata())

          // Commit the actual action.
          val version = deltaLog.startTransaction().commit(Seq(action), ManualUpdate)
          // Read the commit file and get the serialized committed actions
          val committedActions = deltaLog.store.read(
            FileNames.deltaFile(deltaLog.logPath, version),
            deltaLog.newDeltaHadoopConf())

          assert(committedActions.size == 2)
          val serializedJson = committedActions.last
          assert(serializedJson === expectedJson)
          val asObject = Action.fromJson(serializedJson)
          assert(action === asObject)
        }
      }
    }
  }
}
