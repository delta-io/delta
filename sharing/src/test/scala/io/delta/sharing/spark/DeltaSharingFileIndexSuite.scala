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

package io.delta.sharing.spark

import java.io.File

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.sharing.client.{
  DeltaSharingClient,
  DeltaSharingFileSystem,
  DeltaSharingProfileProvider,
  DeltaSharingRestClient
}
import io.delta.sharing.client.model.{DeltaTableFiles, DeltaTableMetadata, Table}
import io.delta.sharing.client.util.JsonUtils
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.delta.sharing.{PreSignedUrlCache, PreSignedUrlFetcher}
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference => SqlAttributeReference,
  EqualTo => SqlEqualTo,
  Literal => SqlLiteral
}
import org.apache.spark.sql.delta.sharing.DeltaSharingTestSparkUtils
import org.apache.spark.sql.types.{FloatType, IntegerType}

private object TestUtils {
  val paths = Seq("http://path1", "http://path2")
  val refreshTokens = Seq("token1", "token2", "token3")

  val SparkConfForReturnExpTime = "spark.delta.sharing.fileindexsuite.returnexptime"
  val SparkConfForUrlExpirationMs = "spark.delta.sharing.fileindexsuite.urlExpirationMs"

  // 10 seconds
  val defaultUrlExpirationMs = 10000

  def getExpirationTimestampStr(urlExpirationMs: Option[Int]): String = {
    if (urlExpirationMs.isDefined) {
      s""""expirationTimestamp":${System.currentTimeMillis() + urlExpirationMs.get},"""
    } else {
      ""
    }
  }

  // scalastyle:off line.size.limit
  val protocolStr =
    """{"protocol":{"deltaProtocol":{"minReaderVersion": 1, "minWriterVersion": 1}}}"""
  val metaDataStr =
    """{"metaData":{"size":809,"deltaMetadata":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c2"],"configuration":{},"createdTime":1691734718560}}}"""
  val metaDataWithoutSizeStr =
    """{"metaData":{"deltaMetadata":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c2"],"configuration":{},"createdTime":1691734718560}}}"""
  def getAddFileStr1(path: String, urlExpirationMs: Option[Int] = None): String = {
    s"""{"file":{"id":"11d9b72771a72f178a6f2839f7f08528",${getExpirationTimestampStr(
      urlExpirationMs
    )}"deltaSingleAction":{"add":{"path":"${path}",""" + """"partitionValues":{"c2":"one"},"size":809,"modificationTime":1691734726073,"dataChange":true,"stats":"{\"numRecords\":2,\"minValues\":{\"c1\":1,\"c2\":\"one\"},\"maxValues\":{\"c1\":2,\"c2\":\"one\"},\"nullCount\":{\"c1\":0,\"c2\":0}}","tags":{"INSERTION_TIME":"1691734726073000","MIN_INSERTION_TIME":"1691734726073000","MAX_INSERTION_TIME":"1691734726073000","OPTIMIZE_TARGET_SIZE":"268435456"}}}}}"""
  }
  def getAddFileStr2(urlExpirationMs: Option[Int] = None): String = {
    s"""{"file":{"id":"22d9b72771a72f178a6f2839f7f08529",${getExpirationTimestampStr(
      urlExpirationMs
    )}""" + """"deltaSingleAction":{"add":{"path":"http://path2","partitionValues":{"c2":"two"},"size":809,"modificationTime":1691734726073,"dataChange":true,"stats":"{\"numRecords\":2,\"minValues\":{\"c1\":1,\"c2\":\"two\"},\"maxValues\":{\"c1\":2,\"c2\":\"two\"},\"nullCount\":{\"c1\":0,\"c2\":0}}","tags":{"INSERTION_TIME":"1691734726073000","MIN_INSERTION_TIME":"1691734726073000","MAX_INSERTION_TIME":"1691734726073000","OPTIMIZE_TARGET_SIZE":"268435456"}}}}}"""
  }
  // scalastyle:on line.size.limit
}

/**
 * A mocked delta sharing client for unit tests.
 */
class TestDeltaSharingClientForFileIndex(
    profileProvider: DeltaSharingProfileProvider,
    timeoutInSeconds: Int = 120,
    numRetries: Int = 10,
    maxRetryDuration: Long = Long.MaxValue,
    sslTrustAll: Boolean = false,
    forStreaming: Boolean = false,
    responseFormat: String = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA,
    readerFeatures: String = "",
    queryTablePaginationEnabled: Boolean = false,
    maxFilesPerReq: Int = 100000,
    enableAsyncQuery: Boolean = false,
    asyncQueryPollIntervalMillis: Long = 10000L,
    asyncQueryMaxDuration: Long = 600000L)
    extends DeltaSharingClient {

  import TestUtils._

  private lazy val returnExpirationTimestamp = SparkSession.active.sessionState.conf
    .getConfString(
      SparkConfForReturnExpTime,
      "false"
    )
    .toBoolean
  private lazy val urlExpirationMsOpt = if (returnExpirationTimestamp) {
    val urlExpirationMs = SparkSession.active.sessionState.conf
      .getConfString(
        SparkConfForUrlExpirationMs,
        defaultUrlExpirationMs.toString
      )
      .toInt
    Some(urlExpirationMs)
  } else {
    None
  }

  var numGetFileCalls: Int = -1

  var savedLimits = Seq.empty[Long]
  var savedJsonPredicateHints = Seq.empty[String]

  override def listAllTables(): Seq[Table] = throw new UnsupportedOperationException("not needed")

  override def getMetadata(
      table: Table,
      versionAsOf: Option[Long],
      timestampAsOf: Option[String]): DeltaTableMetadata = {
    throw new UnsupportedOperationException("getMetadata is not supported now.")
  }

  override def getTableVersion(table: Table, startingTimestamp: Option[String] = None): Long = {
    throw new UnsupportedOperationException("getTableVersion is not supported now.")
  }

  override def getFiles(
      table: Table,
      predicates: Seq[String],
      limit: Option[Long],
      versionAsOf: Option[Long],
      timestampAsOf: Option[String],
      jsonPredicateHints: Option[String],
      refreshToken: Option[String]
  ): DeltaTableFiles = {
    numGetFileCalls += 1
    limit.foreach(lim => savedLimits = savedLimits :+ lim)
    jsonPredicateHints.foreach(p => {
      savedJsonPredicateHints = savedJsonPredicateHints :+ p
    })
    if (numGetFileCalls > 0 && refreshToken.isDefined) {
      assert(refreshToken.get == refreshTokens(numGetFileCalls.min(2) - 1))
    }

    DeltaTableFiles(
      version = 0,
      lines = Seq[String](
        protocolStr,
        metaDataStr,
        getAddFileStr1(paths(numGetFileCalls.min(1)), urlExpirationMsOpt),
        getAddFileStr2(urlExpirationMsOpt)
      ),
      refreshToken = Some(refreshTokens(numGetFileCalls.min(2))),
      respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA
    )
  }

  override def getFiles(
      table: Table,
      startingVersion: Long,
      endingVersion: Option[Long]
  ): DeltaTableFiles = {
    throw new UnsupportedOperationException(s"getFiles with startingVersion($startingVersion)")
  }

  override def getCDFFiles(
      table: Table,
      cdfOptions: Map[String, String],
      includeHistoricalMetadata: Boolean
  ): DeltaTableFiles = {
    throw new UnsupportedOperationException(
      s"getCDFFiles with cdfOptions:[$cdfOptions], " +
      s"includeHistoricalMetadata:$includeHistoricalMetadata"
    )
  }

  override def getForStreaming(): Boolean = forStreaming

  override def getProfileProvider: DeltaSharingProfileProvider = profileProvider

  def clear() {
    savedLimits = Seq.empty[Long]
    savedJsonPredicateHints = Seq.empty[String]
  }
}

class DeltaSharingFileIndexSuite
    extends QueryTest
    with DeltaSQLCommandTest
    with DeltaSharingDataSourceDeltaTestUtils
    with DeltaSharingTestSparkUtils {

  import TestUtils._

  private def getMockedDeltaSharingMetadata(metaData: String): model.DeltaSharingMetadata = {
    JsonUtils.fromJson[model.DeltaSharingSingleAction](metaData).metaData
  }

  private def getMockedDeltaSharingFileAction(id: String): model.DeltaSharingFileAction = {
    if (id.startsWith("11")) {
      JsonUtils.fromJson[model.DeltaSharingSingleAction](getAddFileStr1(paths(0))).file
    } else {
      JsonUtils.fromJson[model.DeltaSharingSingleAction](getAddFileStr2()).file
    }
  }

  private val shareName = "share"
  private val schemaName = "default"
  private val sharedTableName = "table"

  private def prepareDeltaSharingFileIndex(
      profilePath: String,
      metaData: String): (Path, DeltaSharingFileIndex, DeltaSharingClient) = {
    val tablePath = new Path(s"$profilePath#$shareName.$schemaName.$sharedTableName")
    val client = DeltaSharingRestClient(profilePath, false, "delta")

    val spark = SparkSession.active
    val params = new DeltaSharingFileIndexParams(
      tablePath,
      spark,
      DeltaSharingUtils.DeltaSharingTableMetadata(
        version = 0,
        protocol = JsonUtils.fromJson[model.DeltaSharingSingleAction](protocolStr).protocol,
        metadata = getMockedDeltaSharingMetadata(metaData)
      ),
      new DeltaSharingOptions(Map("path" -> tablePath.toString))
    )
    val dsTable = Table(share = shareName, schema = schemaName, name = sharedTableName)
    (tablePath, new DeltaSharingFileIndex(params, dsTable, client, None), client)
  }

  test("basic functions works") {
    withTempDir { tempDir =>
      val profileFile = new File(tempDir, "foo.share")
      FileUtils.writeStringToFile(
        profileFile,
        s"""{
           |  "shareCredentialsVersion": 1,
           |  "endpoint": "https://localhost:12345/not-used-endpoint",
           |  "bearerToken": "mock"
           |}""".stripMargin,
        "utf-8"
      )
      withSQLConf(
        "spark.delta.sharing.client.class" -> classOf[TestDeltaSharingClientForFileIndex].getName,
        "fs.delta-sharing-log.impl" -> classOf[DeltaSharingLogFileSystem].getName,
        "spark.delta.sharing.profile.provider.class" ->
        "io.delta.sharing.client.DeltaSharingFileProfileProvider"
      ) {
        val (tablePath, fileIndex, _) =
          prepareDeltaSharingFileIndex(profileFile.getCanonicalPath, metaDataStr)

        assert(fileIndex.sizeInBytes == 809)
        assert(fileIndex.partitionSchema.toDDL == "c2 STRING")
        assert(fileIndex.rootPaths.length == 1)
        assert(fileIndex.rootPaths.head == tablePath)

        intercept[UnsupportedOperationException] {
          fileIndex.inputFiles
        }

        val partitionDirectoryList = fileIndex.listFiles(Seq.empty, Seq.empty)
        assert(partitionDirectoryList.length == 2)
        partitionDirectoryList.foreach { partitionDirectory =>
          assert(!partitionDirectory.values.anyNull)
          assert(
            partitionDirectory.values.getString(0) == "one" ||
            partitionDirectory.values.getString(0) == "two"
          )

          partitionDirectory.files.foreach { f =>
            // Verify that the path can be decoded
            val decodedPath = DeltaSharingFileSystem.decode(f.fileStatus.getPath)
            val dsFileAction = getMockedDeltaSharingFileAction(decodedPath.fileId)
            assert(decodedPath.tablePath.startsWith(tablePath.toString))
            assert(decodedPath.fileId == dsFileAction.id)
            assert(decodedPath.fileSize == dsFileAction.size)

            assert(f.fileStatus.getLen == dsFileAction.size)
            assert(f.fileStatus.getModificationTime == 0)
            assert(f.fileStatus.isDirectory == false)
          }
        }

        // Check exception is thrown when metadata doesn't have size
        val (_, fileIndex2, _) =
          prepareDeltaSharingFileIndex(profileFile.getCanonicalPath, metaDataWithoutSizeStr)
        val ex = intercept[IllegalStateException] {
          fileIndex2.sizeInBytes
        }
        assert(ex.toString.contains("size is null in the metadata"))
      }
    }
  }

  test("refresh works") {
    PreSignedUrlCache.registerIfNeeded(SparkEnv.get)

    withTempDir { tempDir =>
      val profileFile = new File(tempDir, "foo.share")
      FileUtils.writeStringToFile(
        profileFile,
        s"""{
           |  "shareCredentialsVersion": 1,
           |  "endpoint": "https://localhost:12345/not-used-endpoint",
           |  "bearerToken": "mock"
           |}""".stripMargin,
        "utf-8"
      )

      def test(): Unit = {
        val (_, fileIndex, _) =
          prepareDeltaSharingFileIndex(profileFile.getCanonicalPath, metaDataStr)
        val preSignedUrlCacheRef = PreSignedUrlCache.getEndpointRefInExecutor(SparkEnv.get)

        val partitionDirectoryList = fileIndex.listFiles(Seq.empty, Seq.empty)
        assert(partitionDirectoryList.length == 2)
        partitionDirectoryList.foreach { partitionDirectory =>
          partitionDirectory.files.foreach { f =>
            val decodedPath = DeltaSharingFileSystem.decode(f.fileStatus.getPath)
            if (decodedPath.fileId.startsWith("11")) {
              val fetcher = new PreSignedUrlFetcher(
                preSignedUrlCacheRef,
                decodedPath.tablePath,
                decodedPath.fileId,
                1000
              )
              // sleep for 25000ms to ensure that the urls are refreshed.
              Thread.sleep(25000)

              // Verify that the url is refreshed as paths(1), not paths(0) anymore.
              assert(fetcher.getUrl == paths(1))
            }
          }
        }
      }

      withSQLConf(
        "spark.delta.sharing.client.class" -> classOf[TestDeltaSharingClientForFileIndex].getName,
        "fs.delta-sharing-log.impl" -> classOf[DeltaSharingLogFileSystem].getName,
        "spark.delta.sharing.profile.provider.class" ->
        "io.delta.sharing.client.DeltaSharingFileProfileProvider",
        SparkConfForReturnExpTime -> "true"
      ) {
        test()
      }

      withSQLConf(
        "spark.delta.sharing.client.class" -> classOf[TestDeltaSharingClientForFileIndex].getName,
        "fs.delta-sharing-log.impl" -> classOf[DeltaSharingLogFileSystem].getName,
        "spark.delta.sharing.profile.provider.class" ->
        "io.delta.sharing.client.DeltaSharingFileProfileProvider",
        SparkConfForReturnExpTime -> "false"
      ) {
        test()
      }
    }
  }

  test("jsonPredicate test") {
    withTempDir { tempDir =>
      val profileFile = new File(tempDir, "foo.share")
      FileUtils.writeStringToFile(
        profileFile,
        s"""{
           |  "shareCredentialsVersion": 1,
           |  "endpoint": "https://localhost:12345/not-used-endpoint",
           |  "bearerToken": "mock"
           |}""".stripMargin,
        "utf-8"
      )
      withSQLConf(
        "spark.delta.sharing.client.class" -> classOf[TestDeltaSharingClientForFileIndex].getName,
        "fs.delta-sharing-log.impl" -> classOf[DeltaSharingLogFileSystem].getName,
        "spark.delta.sharing.profile.provider.class" ->
        "io.delta.sharing.client.DeltaSharingFileProfileProvider",
        SparkConfForReturnExpTime -> "true",
        SparkConfForUrlExpirationMs -> "3600000" // 1h
      ) {
        val (tablePath, fileIndex, client) =
          prepareDeltaSharingFileIndex(profileFile.getCanonicalPath, metaDataStr)
        val testClient = client.asInstanceOf[TestDeltaSharingClientForFileIndex]

        val spark = SparkSession.active
        spark.sessionState.conf
          .setConfString("spark.delta.sharing.jsonPredicateHints.enabled", "true")

        // We will send an equal op on partition filters as a SQL expression tree.
        val partitionSqlEq = SqlEqualTo(
          SqlAttributeReference("id", IntegerType)(),
          SqlLiteral(23, IntegerType)
        )
        // The client should get json for jsonPredicateHints.
        val expectedJson =
          """{"op":"equal",
             |"children":[
             |  {"op":"column","name":"id","valueType":"int"},
             |  {"op":"literal","value":"23","valueType":"int"}]
             |}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")

        fileIndex.listFiles(Seq(partitionSqlEq), Seq.empty)
        assert(testClient.savedJsonPredicateHints.size === 1)
        assert(expectedJson == testClient.savedJsonPredicateHints(0))
        testClient.clear()

        // We will send another equal op as a SQL expression tree for data filters.
        val dataSqlEq = SqlEqualTo(
          SqlAttributeReference("cost", FloatType)(),
          SqlLiteral(23.5.toFloat, FloatType)
        )

        // With V2 predicates disabled, the client should get json for partition filters only.
        fileIndex.listFiles(Seq(partitionSqlEq), Seq(dataSqlEq))
        assert(testClient.savedJsonPredicateHints.size === 1)
        assert(expectedJson == testClient.savedJsonPredicateHints(0))
        testClient.clear()

        // With V2 predicates enabled, the client should get json for partition and data filters
        // joined at the top level by an AND operation.
        val expectedJson2 =
          """{"op":"and","children":[
             |  {"op":"equal","children":[
             |    {"op":"column","name":"id","valueType":"int"},
             |    {"op":"literal","value":"23","valueType":"int"}]},
             |  {"op":"equal","children":[
             |    {"op":"column","name":"cost","valueType":"float"},
             |    {"op":"literal","value":"23.5","valueType":"float"}]}
             |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
        spark.sessionState.conf.setConfString(
          "spark.delta.sharing.jsonPredicateV2Hints.enabled",
          "true"
        )
        fileIndex.listFiles(Seq(partitionSqlEq), Seq(dataSqlEq))
        assert(testClient.savedJsonPredicateHints.size === 1)
        assert(expectedJson2 == testClient.savedJsonPredicateHints(0))
        testClient.clear()

        // With json predicates disabled, we should not get anything.
        spark.sessionState.conf
          .setConfString("spark.delta.sharing.jsonPredicateHints.enabled", "false")
        fileIndex.listFiles(Seq(partitionSqlEq), Seq.empty)
        assert(testClient.savedJsonPredicateHints.size === 0)
      }
    }
  }
}
