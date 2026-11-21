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
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingRestClient}
import io.delta.sharing.client.model.{DeltaTableMetadata, Table => DeltaSharingTable}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.Level

import org.apache.spark.delta.sharing.CachedTableManager
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.delta.sharing.DeltaSharingTestSparkUtils

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

  private def withResponseVersion[T](enabled: Boolean)(f: => T): T = {
    require(enabled)
    f
  }

  private def snapshotHash(options: DeltaSharingOptions, version: Long): String =
    DeltaSharingUtils.getQueryParamsHashId(options, "", "", "", "", version)

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

  private def buildSnapshot(
      client: DeltaSharingClient,
      table: DeltaSharingTable,
      options: DeltaSharingOptions,
      path: Path,
      metadataVersion: Long): DeltaSharingDeltaLogBuilder.UnregisteredSnapshotDeltaLog = {
    DeltaSharingDeltaLogBuilder.buildSnapshotDeltaLog(
      spark = spark,
      client = client,
      table = table,
      options = options,
      tablePath = path.toString,
      queryParamsHashId = snapshotHash(options, metadataVersion),
      queryParamsHashIdForVersion = version => snapshotHash(options, version),
      jsonPredicateHints = None,
      limit = None,
      logPrefix = "Test")
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
        queryParamsHashId = "hashA",
        queryParamsHashIdForVersion = _ => "hashA",
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
          queryParamsHashId = hashId,
          queryParamsHashIdForVersion = _ => hashId,
          jsonPredicateHints = None,
          limit = None,
          anchor = anchor,
          logPrefix = "Test"
        )

      // Two query shapes on the same table, distinguished only by queryParamsHashId.
      val pathA = build("hashA", anchorA)
      val pathB = build("hashB", anchorB)

      // The two shapes get distinct synthetic log paths...
      assert(pathA.toString != pathB.toString)

      // ...and two distinct CachedTableManager entries, so the shapes don't clobber each other.
      assert(CachedTableManager.INSTANCE.size() == sizeBefore + 2)

      // Assert directly on the entry key. CachedTableManager has no containsKey, but its test hook
      // getQueryStateSize(tablePath) throws on a path that isn't cached. So the hash is part of the
      // cache key: each shape's hash-suffixed path is cached (size value is irrelevant -- only the
      // absence of a throw matters), while the bare un-suffixed table path is not.
      assert(CachedTableManager.INSTANCE.getQueryStateSize(
        DeltaSharingLogFileSystem.decode(pathA)) >= 0)
      assert(CachedTableManager.INSTANCE.getQueryStateSize(
        DeltaSharingLogFileSystem.decode(pathB)) >= 0)
      intercept[IllegalStateException] {
        CachedTableManager.INSTANCE.getQueryStateSize(
          client.getProfileProvider.getCustomTablePath(tablePath.toString))
      }

      Reference.reachabilityFence(anchorA)
      Reference.reachabilityFence(anchorB)
    }
  }

  Seq(true).foreach { enabled =>
    val behavior = if (enabled) "isolate response versions" else "reuse the metadata version"
    test(s"snapshot paths and URL entries $behavior") {
      withResponseVersion(enabled) {
        withProfile { profilePath =>
          val (client, dsTable, options, tablePath) = setup(profilePath)
          val testClient = client.asInstanceOf[TestDeltaSharingClientForFileIndex]
          CachedTableManager.INSTANCE.clear()
          val anchorA = new Object()
          val anchorB = new Object()

          testClient.responseVersion = 100L
          val first = buildSnapshot(client, dsTable, options, tablePath, metadataVersion = 100L)
          DeltaSharingDeltaLogBuilder.registerPreSignedUrls(first, anchorA)
          testClient.responseVersion = 101L
          val second = buildSnapshot(client, dsTable, options, tablePath, metadataVersion = 100L)
          DeltaSharingDeltaLogBuilder.registerPreSignedUrls(second, anchorB)

          assert((first.encodedPath != second.encodedPath) == enabled)
          assert((first.registrationTablePath != second.registrationTablePath) == enabled)
          assert(CachedTableManager.INSTANCE.size() == (if (enabled) 2 else 1))
          Seq(first, second).foreach { snapshot =>
            val cacheKey = client.getProfileProvider.getCustomTablePath(
              snapshot.registrationTablePath)
            assert(DeltaSharingLogFileSystem.decode(snapshot.encodedPath) == cacheKey)
          }
          if (enabled) {
            val fileId = "11d9b72771a72f178a6f2839f7f08528"
            def urlFor(snapshot: DeltaSharingDeltaLogBuilder.UnregisteredSnapshotDeltaLog): String =
              CachedTableManager.INSTANCE.getPreSignedUrl(
                client.getProfileProvider.getCustomTablePath(snapshot.registrationTablePath),
                fileId)._1
            assert(urlFor(first) == TestUtils.paths.head)
            assert(urlFor(second) == TestUtils.paths(1))
          } else {
            assert(first.registrationTablePath.endsWith(snapshotHash(options, 100L)))
          }
          Reference.reachabilityFence(anchorA)
          Reference.reachabilityFence(anchorB)
        }
      }
    }
  }

  test("equal getFiles versions reuse paths despite different metadata versions") {
    withResponseVersion(enabled = true) {
      withProfile { profilePath =>
        val (client, dsTable, options, tablePath) = setup(profilePath)
        client.asInstanceOf[TestDeltaSharingClientForFileIndex].responseVersion = 101L
        val first = buildSnapshot(client, dsTable, options, tablePath, metadataVersion = 99L)
        val second = buildSnapshot(client, dsTable, options, tablePath, metadataVersion = 100L)

        assert(first.encodedPath == second.encodedPath)
        assert(first.registrationTablePath == second.registrationTablePath)
        val snapshot = DeltaLog.forTable(spark, second.encodedPath).update()
        assert(snapshot.version == 0L)
        assert(snapshot.allFiles.count() == 2L)
      }
    }
  }

  test("V1 caller uses getFiles versions for its log path and URL registration") {
    withResponseVersion(enabled = true) {
      withProfile { profilePath =>
        val (client, table, options, path) = setup(profilePath)
        val testClient = client.asInstanceOf[TestDeltaSharingClientForFileIndex]
        CachedTableManager.INSTANCE.clear()
        testClient.responseVersion = 100L
        val firstIndex = snapshotIndex(client, table, options, path, metadataVersion = 100L)
        val first = firstIndex.fetchFilesAndConstructDeltaLog(Nil, Nil, None)
        testClient.responseVersion = 101L
        val secondIndex = snapshotIndex(client, table, options, path, metadataVersion = 100L)
        val second = secondIndex.fetchFilesAndConstructDeltaLog(Nil, Nil, None)

        assert(first.dataPath != second.dataPath)
        assert(CachedTableManager.INSTANCE.size() == 2)
        val fileId = "11d9b72771a72f178a6f2839f7f08528"
        val firstKey = DeltaSharingLogFileSystem.decode(first.dataPath)
        val secondKey = DeltaSharingLogFileSystem.decode(second.dataPath)
        assert(CachedTableManager.INSTANCE.getPreSignedUrl(firstKey, fileId)._1 ==
          TestUtils.paths.head)
        assert(CachedTableManager.INSTANCE.getPreSignedUrl(secondKey, fileId)._1 ==
          TestUtils.paths(1))
        assert(firstIndex ne secondIndex)
      }
    }
  }


  test("response version hashes include the effective V1 pushed limit") {
    withResponseVersion(enabled = true) {
      withProfile { profilePath =>
        val (client, dsTable, options, tablePath) = setup(profilePath)
        val metadata = DeltaSharingUtils.getDeltaSharingTableMetadata(
          dsTable,
          DeltaTableMetadata(
            version = 100L,
            lines = Seq(TestUtils.protocolStr, TestUtils.metaDataStr),
            respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA))
        val params = DeltaSharingFileIndexParams(tablePath, spark, metadata, options)
        val index = DeltaSharingFileIndex(params, dsTable, client, limitHint = Some(5L))
        val first = index.fetchFilesAndConstructDeltaLog(Nil, Nil, None)
        val second = index.copy(limitHint = Some(10L))
          .fetchFilesAndConstructDeltaLog(Nil, Nil, None)

        assert(first.dataPath != second.dataPath)
        assert(client.asInstanceOf[TestDeltaSharingClientForFileIndex].savedLimits == Seq(5L, 10L))
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
        queryParamsHashId = "hashC",
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
          queryParamsHashId = "hashD",
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
