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
import java.lang.ref.Reference

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaTestUtilsBase.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingRestClient}
import io.delta.sharing.client.model.{DeltaTableMetadata, Table => DeltaSharingTable}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.Level

import org.apache.spark.delta.sharing.CachedTableManager
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.delta.sharing.DeltaSharingTestSparkUtils
import org.apache.spark.sql.types.IntegerType

/**
 * Unit tests for [[DeltaSharingDeltaLogBuilder.buildSnapshotDeltaLogPath]], the FileIndex-free
 * snapshot-construction helper shared by the V1 path and (eventually) the DSV2 scan. The mocked
 * client and JSON fixtures are reused from [[DeltaSharingFileIndexSuite]] (`TestUtils` and
 * `TestDeltaSharingClientForFileIndex`, same package).
 */
class DeltaSharingDeltaLogBuilderSuite
    extends QueryTest
    with DeltaSQLCommandTest
    with DeltaSharingDataSourceDeltaTestUtils
    with DeltaSharingTestSparkUtils {

  private val shareName = "share"
  private val schemaName = "default"
  private val sharedTableName = "table"

  private val responseVersionKey =
    DeltaSQLConf.DELTA_SHARING_SNAPSHOT_RESPONSE_VERSION_ENABLED.key

  private def snapshotIndex(
      client: DeltaSharingClient,
      table: DeltaSharingTable,
      options: DeltaSharingOptions,
      path: Path,
      metadataVersion: Long): DeltaSharingFileIndex = {
    val metadata = DeltaSharingUtils.getDeltaSharingTableMetadata(
      table,
      DeltaTableMetadata(
        version = metadataVersion,
        lines = Seq(TestUtils.protocolStr, TestUtils.metaDataStr),
        respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA))
    DeltaSharingFileIndex(
      DeltaSharingFileIndexParams(path, spark, metadata, options), table, client, limitHint = None)
  }

  /** Build the inputs to `buildSnapshotDeltaLogPath` against the mocked client. */
  private def setup(
      profilePath: String): (DeltaSharingClient, DeltaSharingTable, DeltaSharingOptions, Path) = {
    val tablePath = new Path(s"$profilePath#$shareName.$schemaName.$sharedTableName")
    // Resolves to TestDeltaSharingClientForFileIndex via spark.delta.sharing.client.class.
    val client = DeltaSharingRestClient(profilePath, Map.empty, false, "delta")
    val options = new DeltaSharingOptions(Map("path" -> tablePath.toString))
    val dsTable = DeltaSharingTable(share = shareName, schema = schemaName, name = sharedTableName)
    (client, dsTable, options, tablePath)
  }

  /** Write a profile file and run `f` with the sharing test confs applied. */
  private def withProfile(f: String => Unit): Unit = {
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
        responseVersionKey -> "false",
        "spark.delta.sharing.client.class" -> classOf[TestDeltaSharingClientForFileIndex].getName,
        "fs.delta-sharing-log.impl" -> classOf[DeltaSharingLogFileSystem].getName,
        "spark.delta.sharing.profile.provider.class" ->
        "io.delta.sharing.client.DeltaSharingFileProfileProvider"
      ) {
        f(profileFile.getCanonicalPath)
      }
    }
  }

  test("constructs a readable version-0 delta log from the getFiles response") {
    withProfile { profilePath =>
      val (client, dsTable, options, tablePath) = setup(profilePath)
      val testClient = client.asInstanceOf[TestDeltaSharingClientForFileIndex]

      // A plain Object as anchor proves the helper does not depend on the FileIndex interface.
      val encodedPath = DeltaSharingDeltaLogBuilder.buildSnapshotDeltaLogPath(
        spark = spark,
        client = client,
        table = dsTable,
        options = options,
        tablePath = tablePath.toString,
        metadataVersion = 100L,
        queryParamsHashIdForVersion = version => {
          assert(version == 100L)
          "hashA"
        },
        jsonPredicateHints = None,
        limit = None,
        anchor = new Object(),
        logPrefix = "Test"
      )

      // Exactly one getFiles RPC was issued (numGetFileCalls starts at -1).
      assert(testClient.numGetFileCalls == 0)

      // The returned path is the synthetic log, openable as a classic version-0 DeltaLog (the V1
      // path). The two add files come from the mocked getFiles response.
      assert(encodedPath.toString.startsWith("delta-sharing-log:"))
      // The query-params hash is the table-path suffix (getTablePathWithIdSuffix appends `_<id>`),
      // so different query shapes on the same table get distinct synthetic log paths.
      assert(encodedPath.toString.endsWith("_hashA"))
      val snapshot = DeltaLog.forTable(SparkSession.active, encodedPath).update()
      assert(snapshot.version == 0)
      assert(snapshot.allFiles.count() == 2)
    }
  }

  test("distinguishes query shapes on the same table via the query-params hash") {
    withProfile { profilePath =>
      val (client, dsTable, options, tablePath) = setup(profilePath)
      CachedTableManager.INSTANCE.clear()
      val sizeBefore = CachedTableManager.INSTANCE.size()

      // Strong refs keep the registered WeakReference anchors alive until the size assertion.
      val anchorA = new Object()
      val anchorB = new Object()
      def build(hashId: String, anchor: AnyRef): Path =
        DeltaSharingDeltaLogBuilder.buildSnapshotDeltaLogPath(
          spark = spark,
          client = client,
          table = dsTable,
          options = options,
          tablePath = tablePath.toString,
          metadataVersion = 100L,
          queryParamsHashIdForVersion = _ => hashId,
          jsonPredicateHints = None,
          limit = None,
          anchor = anchor,
          logPrefix = "Test"
        )

      // Two query shapes on the same table, distinguished only by their hash callback.
      val pathA = build("hashA", anchorA)
      val pathB = build("hashB", anchorB)

      // The hash is the table-path suffix, so the two shapes get distinct synthetic log paths...
      assert(pathA.toString.endsWith("_hashA"))
      assert(pathB.toString.endsWith("_hashB"))
      assert(pathA.toString != pathB.toString)

      // ...and two distinct CachedTableManager entries, so the shapes don't clobber each other.
      assert(CachedTableManager.INSTANCE.size() == sizeBefore + 2)

      // Assert directly on the entry key. CachedTableManager has no containsKey, but its test hook
      // getQueryStateSize(tablePath) throws on a path that isn't cached. So the hash is part of the
      // cache key: each shape's hash-suffixed path is cached (size value is irrelevant -- only the
      // absence of a throw matters), while the bare un-suffixed table path is not.
      def cacheKey(hashId: String): String =
        client.getProfileProvider.getCustomTablePath(
          DeltaSharingUtils.getTablePathWithIdSuffix(tablePath.toString, hashId))
      assert(CachedTableManager.INSTANCE.getQueryStateSize(cacheKey("hashA")) >= 0)
      assert(CachedTableManager.INSTANCE.getQueryStateSize(cacheKey("hashB")) >= 0)
      intercept[IllegalStateException] {
        CachedTableManager.INSTANCE.getQueryStateSize(
          client.getProfileProvider.getCustomTablePath(tablePath.toString))
      }

      assert(anchorA != null && anchorB != null)
    }
  }

  BOOLEAN_DOMAIN.foreach { enabled =>
    test(s"V1 snapshot path version follows rollout flag: enabled=$enabled") {
      withProfile { profilePath =>
        withSQLConf(responseVersionKey -> enabled.toString) {
          val (client, table, options, path) = setup(profilePath)
          val testClient = client.asInstanceOf[TestDeltaSharingClientForFileIndex]
          CachedTableManager.INSTANCE.clear()
          testClient.responseVersion = 101L
          val firstIndex = snapshotIndex(client, table, options, path, metadataVersion = 100L)
          val first = firstIndex.fetchFilesAndConstructDeltaLog(Nil, Nil, None)

          testClient.responseVersion = 102L
          // A retained FileIndex still reuses its memoized DeltaLog without another getFiles RPC.
          assert(firstIndex.fetchFilesAndConstructDeltaLog(Nil, Nil, None) eq first)
          val secondIndex = snapshotIndex(client, table, options, path, metadataVersion = 100L)
          val second = secondIndex.fetchFilesAndConstructDeltaLog(Nil, Nil, None)
          assert(testClient.numGetFileCalls == 1) // The counter starts at -1.

          def expectedKey(version: Long): String = {
            val hash = DeltaSharingUtils.getQueryParamsHashId(options, "", "", "", "", version)
            client.getProfileProvider.getCustomTablePath(
              DeltaSharingUtils.getTablePathWithIdSuffix(path.toString, hash))
          }
          val firstKey = DeltaSharingLogFileSystem.decode(first.dataPath)
          val secondKey = DeltaSharingLogFileSystem.decode(second.dataPath)
          assert(firstKey == expectedKey(if (enabled) 101L else 100L))
          assert(secondKey == expectedKey(if (enabled) 102L else 100L))
          assert((first.dataPath != second.dataPath) == enabled)
          assert(CachedTableManager.INSTANCE.size() == (if (enabled) 2 else 1))
          val fileId = "11d9b72771a72f178a6f2839f7f08528"
          assert(CachedTableManager.INSTANCE.getPreSignedUrl(secondKey, fileId)._1.nonEmpty)
          if (enabled) {
            assert(CachedTableManager.INSTANCE.getPreSignedUrl(secondKey, fileId)._1 ==
              TestUtils.paths(1))
            assert(CachedTableManager.INSTANCE.getPreSignedUrl(firstKey, fileId)._1 ==
              TestUtils.paths.head)
          }

          // Only the selected version changes the hash; equal response versions need no new path.
          val thirdIndex = snapshotIndex(client, table, options, path, metadataVersion = 99L)
          val third = thirdIndex.fetchFilesAndConstructDeltaLog(Nil, Nil, None)
          assert((second.dataPath == third.dataPath) == enabled)
          Reference.reachabilityFence(firstIndex)
          Reference.reachabilityFence(secondIndex)
          Reference.reachabilityFence(thirdIndex)
        }
      }
    }
  }

  test("enabled V1 snapshot paths retain nonempty filter and hint inputs") {
    withProfile { profilePath =>
      withSQLConf(
        responseVersionKey -> "true",
        "spark.delta.sharing.jsonPredicateHints.enabled" -> "true",
        "spark.delta.sharing.jsonPredicateV2Hints.enabled" -> "true") {
        val (client, table, options, path) = setup(profilePath)
        client.asInstanceOf[TestDeltaSharingClientForFileIndex].responseVersion = 101L
        CachedTableManager.INSTANCE.clear()
        val filters = Seq(1, 2).map { value =>
          Seq(EqualTo(AttributeReference("c1", IntegerType)(), Literal(value, IntegerType)))
        }
        val indexes = filters.map { _ =>
          snapshotIndex(client, table, options, path, metadataVersion = 100L)
        }
        val logs = indexes.zip(filters).map { case (index, dataFilters) =>
          index.fetchFilesAndConstructDeltaLog(Nil, dataFilters, None)
        }
        val expectedKeys = filters.map { dataFilters =>
          val hints = DeltaSharingJsonPredicates.convert(Nil, dataFilters, spark.sessionState.conf)
          assert(hints.nonEmpty)
          val hash = DeltaSharingUtils.getQueryParamsHashId(
            options, "", dataFilters.map(_.sql).mkString(";"), hints.get, "", 101L)
          client.getProfileProvider.getCustomTablePath(
            DeltaSharingUtils.getTablePathWithIdSuffix(path.toString, hash))
        }
        assert(logs.map(_.dataPath).distinct.size == 2)
        assert(expectedKeys.distinct.size == 2)
        assert(logs.map(log => DeltaSharingLogFileSystem.decode(log.dataPath)) == expectedKeys)
        assert(CachedTableManager.INSTANCE.size() == 2)
        val fileId = "11d9b72771a72f178a6f2839f7f08528"
        expectedKeys.zip(TestUtils.paths).foreach { case (key, url) =>
          assert(CachedTableManager.INSTANCE.getPreSignedUrl(key, fileId)._1 == url)
        }
        indexes.foreach(index => Reference.reachabilityFence(index))
      }
    }
  }

  test("threads limit and jsonPredicateHints into the getFiles RPC") {
    withProfile { profilePath =>
      val (client, dsTable, options, tablePath) = setup(profilePath)
      val testClient = client.asInstanceOf[TestDeltaSharingClientForFileIndex]
      val jsonHint = """{"op":"equal","children":[]}"""

      DeltaSharingDeltaLogBuilder.buildSnapshotDeltaLogPath(
        spark = spark,
        client = client,
        table = dsTable,
        options = options,
        tablePath = tablePath.toString,
        metadataVersion = 100L,
        queryParamsHashIdForVersion = _ => "hashC",
        jsonPredicateHints = Some(jsonHint),
        limit = Some(5L),
        anchor = new Object(),
        logPrefix = "Test"
      )

      assert(testClient.savedLimits == Seq(5L))
      assert(testClient.savedJsonPredicateHints == Seq(jsonHint))
    }
  }

  test("tags the fetch log line with the caller's logPrefix") {
    withProfile { profilePath =>
      val (client, dsTable, options, tablePath) = setup(profilePath)
      val appender = new LogAppender("snapshot loader fetch log")
      withLogAppender(
        appender,
        loggerNames = Seq("io.delta.sharing.spark.DeltaSharingDeltaLogBuilder"),
        // Pin the level: withLogAppender's default level differs by Spark version (None on 4.0,
        // Some(INFO) on 4.1+), and without it the INFO line is filtered out under Spark 4.0.
        level = Some(Level.INFO)) {
        DeltaSharingDeltaLogBuilder.buildSnapshotDeltaLogPath(
          spark = spark,
          client = client,
          table = dsTable,
          options = options,
          tablePath = tablePath.toString,
          metadataVersion = 100L,
          queryParamsHashIdForVersion = _ => "hashD",
          jsonPredicateHints = None,
          limit = None,
          anchor = new Object(),
          logPrefix = "Delta Sharing (v1)"
        )
      }
      assert(appender.loggingEvents.exists(
        _.getMessage.getFormattedMessage.contains("Delta Sharing (v1): fetched")))
    }
  }
}
