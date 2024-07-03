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
  DeltaSharingProfileProvider,
  DeltaSharingRestClient
}
import io.delta.sharing.client.model.{DeltaTableFiles, DeltaTableMetadata, Table}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.delta.sharing.{PreSignedUrlCache, PreSignedUrlFetcher}
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.delta.sharing.DeltaSharingTestSparkUtils
import org.apache.spark.sql.test.{SharedSparkSession}

private object CDFTesTUtils {
  val paths = Seq("http://path1", "http://path2")

  val SparkConfForReturnExpTime = "spark.delta.sharing.fileindexsuite.returnexptime"

  // 10 seconds
  val expirationTimeMs = 10000

  def getExpirationTimestampStr(returnExpTime: Boolean): String = {
    if (returnExpTime) {
      s""""expirationTimestamp":${System.currentTimeMillis() + expirationTimeMs},"""
    } else {
      ""
    }
  }

  // scalastyle:off line.size.limit
  val fileStr1Id = "11d9b72771a72f178a6f2839f7f08528"
  val metaDataStr =
    """{"metaData":{"size":809,"deltaMetadata":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c2"],"configuration":{"delta.enableChangeDataFeed":"true"},"createdTime":1691734718560}}}"""
  def getAddFileStr1(path: String, returnExpTime: Boolean = false): String = {
    s"""{"file":{"id":"11d9b72771a72f178a6f2839f7f08528",${getExpirationTimestampStr(
      returnExpTime
    )}"deltaSingleAction":{"add":{"path":"${path}",""" + """"partitionValues":{"c2":"one"},"size":809,"modificationTime":1691734726073,"dataChange":true,"stats":"{\"numRecords\":2,\"minValues\":{\"c1\":1,\"c2\":\"one\"},\"maxValues\":{\"c1\":2,\"c2\":\"one\"},\"nullCount\":{\"c1\":0,\"c2\":0}}","tags":{"INSERTION_TIME":"1691734726073000","MIN_INSERTION_TIME":"1691734726073000","MAX_INSERTION_TIME":"1691734726073000","OPTIMIZE_TARGET_SIZE":"268435456"}}}}}"""
  }
  def getAddFileStr2(returnExpTime: Boolean = false): String = {
    s"""{"file":{"id":"22d9b72771a72f178a6f2839f7f08529",${getExpirationTimestampStr(
      returnExpTime
    )}""" + """"deltaSingleAction":{"add":{"path":"http://path2","partitionValues":{"c2":"two"},"size":809,"modificationTime":1691734726073,"dataChange":true,"stats":"{\"numRecords\":2,\"minValues\":{\"c1\":1,\"c2\":\"two\"},\"maxValues\":{\"c1\":2,\"c2\":\"two\"},\"nullCount\":{\"c1\":0,\"c2\":0}}","tags":{"INSERTION_TIME":"1691734726073000","MIN_INSERTION_TIME":"1691734726073000","MAX_INSERTION_TIME":"1691734726073000","OPTIMIZE_TARGET_SIZE":"268435456"}}}}}"""
  }
  // scalastyle:on line.size.limit
}

/**
 * A mocked delta sharing client for unit tests.
 */
class TestDeltaSharingClientForCDFUtils(
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

  import CDFTesTUtils._

  private lazy val returnExpirationTimestamp = SparkSession.active.sessionState.conf
    .getConfString(
      SparkConfForReturnExpTime
    )
    .toBoolean

  var numGetFileCalls: Int = -1

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
    throw new UnsupportedOperationException("getFiles is not supported now.")
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
    numGetFileCalls += 1
    DeltaTableFiles(
      version = 0,
      lines = Seq[String](
        """{"protocol":{"deltaProtocol":{"minReaderVersion": 1, "minWriterVersion": 1}}}""",
        metaDataStr,
        getAddFileStr1(paths(numGetFileCalls.min(1)), returnExpirationTimestamp),
        getAddFileStr2(returnExpirationTimestamp)
      ),
      respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA
    )
  }

  override def getForStreaming(): Boolean = forStreaming

  override def getProfileProvider: DeltaSharingProfileProvider = profileProvider
}

class DeltaSharingCDFUtilsSuite
    extends QueryTest
    with DeltaSQLCommandTest
    with SharedSparkSession
    with DeltaSharingTestSparkUtils {

  import CDFTesTUtils._

  private val shareName = "share"
  private val schemaName = "default"
  private val sharedTableName = "table"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.delta.sharing.preSignedUrl.expirationMs", expirationTimeMs.toString)
      .set("spark.delta.sharing.driver.refreshCheckIntervalMs", "1000")
      .set("spark.delta.sharing.driver.refreshThresholdMs", "2000")
      .set("spark.delta.sharing.driver.accessThresholdToExpireMs", "60000")
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
        val profilePath = profileFile.getCanonicalPath
        val tablePath = new Path(s"$profilePath#$shareName.$schemaName.$sharedTableName")
        val client = DeltaSharingRestClient(profilePath, false, "delta")
        val dsTable = Table(share = shareName, schema = schemaName, name = sharedTableName)

        val options = new DeltaSharingOptions(Map("path" -> tablePath.toString))
        DeltaSharingCDFUtils.prepareCDFRelation(
          SparkSession.active.sqlContext,
          options,
          dsTable,
          client
        )

        val preSignedUrlCacheRef = PreSignedUrlCache.getEndpointRefInExecutor(SparkEnv.get)
        val path = options.options.getOrElse(
          "path",
          throw DeltaSharingErrors.pathNotSpecifiedException
        )
        val fetcher = new PreSignedUrlFetcher(
          preSignedUrlCacheRef,
          DeltaSharingUtils.getTablePathWithIdSuffix(
            path,
            DeltaSharingUtils.getQueryParamsHashId(options.cdfOptions)
          ),
          fileStr1Id,
          1000
        )
        // sleep for 25000ms to ensure that the urls are refreshed.
        Thread.sleep(25000)

        // Verify that the url is refreshed as paths(1), not paths(0) anymore.
        assert(fetcher.getUrl == paths(1))
      }

      withSQLConf(
        "spark.delta.sharing.client.class" -> classOf[TestDeltaSharingClientForCDFUtils].getName,
        "fs.delta-sharing-log.impl" -> classOf[DeltaSharingLogFileSystem].getName,
        "spark.delta.sharing.profile.provider.class" ->
        "io.delta.sharing.client.DeltaSharingFileProfileProvider",
        SparkConfForReturnExpTime -> "true"
      ) {
        test()
      }

      withSQLConf(
        "spark.delta.sharing.client.class" -> classOf[TestDeltaSharingClientForCDFUtils].getName,
        "fs.delta-sharing-log.impl" -> classOf[DeltaSharingLogFileSystem].getName,
        "spark.delta.sharing.profile.provider.class" ->
        "io.delta.sharing.client.DeltaSharingFileProfileProvider",
        SparkConfForReturnExpTime -> "false"
      ) {
        test()
      }
    }
  }
}
