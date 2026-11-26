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
import java.lang.ref.WeakReference

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames
import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingFileProfileProvider, DeltaSharingRestClient}
import io.delta.sharing.client.model.{Table => DeltaSharingTable}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.Level

import org.apache.spark.SparkEnv
import org.apache.spark.delta.sharing.{CachedTableManager, QuerySpecificCachedTable}
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.delta.sharing.DeltaSharingTestSparkUtils

/**
 * A file profile provider that also supplies a query id and a refresher wrapper, so that with the
 * Spark Parquet IO cache on, [[CachedTableManager]] keeps query-specific entries. The query id is
 * read from the companion at call time.
 */
private[sharing] class TestQuerySpecificFileProfileProvider(conf: Configuration, file: String)
    extends DeltaSharingFileProfileProvider(conf, file) {
  override def getCustomQueryId(): Option[String] =
    Some(TestQuerySpecificFileProfileProvider.queryId)

  override def getCustomRefresherWrapper(): Option[QuerySpecificCachedTable.RefresherWrapper] =
    Some((refreshToken, refresher) => refresher(refreshToken))
}

private[sharing] object TestQuerySpecificFileProfileProvider {
  @volatile var queryId: String = "query-1"

}

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

  /** The [[CachedTableManager]] key of the shared table's entry for `hashId`. */
  private def cacheKey(profilePath: String, hashId: String): String = {
    val (client, _, _, tablePath) = setup(profilePath)
    client.getProfileProvider.getCustomTablePath(
      DeltaSharingUtils.getTablePathWithIdSuffix(tablePath.toString, hashId))
  }

  /** Run `f` with reuse set to `reuseEnabled`, the IO cache on and a provider with a query id. */
  private def withQuerySpecificCache(reuseEnabled: Boolean)(f: String => Unit): Unit = {
    withProfile { profilePath =>
      withSQLConf(
        "spark.delta.sharing.profile.provider.class" ->
          classOf[TestQuerySpecificFileProfileProvider].getName,
        "spark.delta.sharing.client.sparkParquetIOCache.enabled" -> "true",
        DeltaSQLConf.DELTA_SHARING_SNAPSHOT_DELTA_LOG_REUSE_ENABLED.key -> reuseEnabled.toString
      ) {
        CachedTableManager.INSTANCE.clear()
        f(profilePath)
      }
    }
  }

  /**
   * Run one snapshot query with `queryId` and its own client, anchored to `anchor`. Returns the log
   * path and the number of getFiles RPCs it issued.
   */
  private def runQuery(
      profilePath: String,
      queryId: String,
      hashId: String,
      anchor: AnyRef): (Path, Int) = {
    TestQuerySpecificFileProfileProvider.queryId = queryId
    val (client, dsTable, options, tablePath) = setup(profilePath)
    val path = DeltaSharingDeltaLogBuilder.buildSnapshotDeltaLogPath(
      client = client,
      table = dsTable,
      options = options,
      tablePath = tablePath.toString,
      queryParamsHashId = hashId,
      jsonPredicateHints = None,
      limit = None,
      anchor = anchor,
      logPrefix = "Test"
    )
    // numGetFileCalls starts at -1 and counts up on every getFiles RPC.
    (path, client.asInstanceOf[TestDeltaSharingClientForFileIndex].numGetFileCalls + 1)
  }

  /** Runs GC until `ref` is cleared, for up to 10s. */
  private def gcUntilCleared(ref: WeakReference[_]): Unit = {
    val deadline = System.currentTimeMillis() + 10000
    while (ref.get != null && System.currentTimeMillis() < deadline) {
      System.gc()
      Thread.sleep(100)
    }
    assert(ref.get == null, "the anchor was not garbage collected")
  }

  test("constructs a readable version-0 delta log from the getFiles response") {
    withProfile { profilePath =>
      val (client, dsTable, options, tablePath) = setup(profilePath)
      val testClient = client.asInstanceOf[TestDeltaSharingClientForFileIndex]

      // A plain Object as anchor proves the helper does not depend on the FileIndex interface.
      val encodedPath = DeltaSharingDeltaLogBuilder.buildSnapshotDeltaLogPath(
        client = client,
        table = dsTable,
        options = options,
        tablePath = tablePath.toString,
        queryParamsHashId = "hashA",
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
          client = client,
          table = dsTable,
          options = options,
          tablePath = tablePath.toString,
          queryParamsHashId = hashId,
          jsonPredicateHints = None,
          limit = None,
          anchor = anchor,
          logPrefix = "Test"
        )

      // Two query shapes on the same table, distinguished only by queryParamsHashId.
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

  test("threads limit and jsonPredicateHints into the getFiles RPC") {
    withProfile { profilePath =>
      val (client, dsTable, options, tablePath) = setup(profilePath)
      val testClient = client.asInstanceOf[TestDeltaSharingClientForFileIndex]
      val jsonHint = """{"op":"equal","children":[]}"""

      DeltaSharingDeltaLogBuilder.buildSnapshotDeltaLogPath(
        client = client,
        table = dsTable,
        options = options,
        tablePath = tablePath.toString,
        queryParamsHashId = "hashC",
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
          client = client,
          table = dsTable,
          options = options,
          tablePath = tablePath.toString,
          queryParamsHashId = "hashD",
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

  test("isSnapshotDeltaLogReusable needs the cached urls and both log blocks") {
    withProfile { profilePath =>
      val (client, dsTable, options, tablePath) = setup(profilePath)
      CachedTableManager.INSTANCE.clear()
      // Keeps the registered anchor alive.
      val anchor = new Object()
      def buildLog(): Path = DeltaSharingDeltaLogBuilder.buildSnapshotDeltaLogPath(
        client = client,
        table = dsTable,
        options = options,
        tablePath = tablePath.toString,
        queryParamsHashId = "hashA",
        jsonPredicateHints = None,
        limit = None,
        anchor = anchor,
        logPrefix = "Test"
      )
      val path = buildLog()
      def logReusable: Boolean =
        DeltaSharingLogFileSystem.isSnapshotDeltaLogReusable(DeltaSharingLogFileSystem.decode(path))
      assert(logReusable)

      // Urls gone, blocks still there.
      CachedTableManager.INSTANCE.clear()
      assert(!logReusable)

      // Urls cached, one block gone: first the 0.json block, then the _delta_log listing block.
      val deltaLogPath = s"$path/_delta_log"
      Seq(FileNames.unsafeDeltaFile(new Path(deltaLogPath), 0).toString, deltaLogPath).foreach {
        blockPath =>
          buildLog()
          assert(logReusable)
          SparkEnv.get.blockManager.removeBlock(
            DeltaSharingLogFileSystem.getDeltaSharingLogBlockId(blockPath))
          assert(CachedTableManager.INSTANCE.size() == 1)
          assert(!logReusable)
      }
      assert(anchor != null)
    }
  }

  test("delta log reuse: an identical query reuses the log and urls, skipping getFiles") {
    withQuerySpecificCache(reuseEnabled = true) { profilePath =>
      val anchor1 = new Object()
      val (path1, getFiles1) = runQuery(profilePath, "query-1", "hashA", anchor1)
      assert(getFiles1 == 1)

      val anchor2 = new Object()
      val (path2, getFiles2) = runQuery(profilePath, "query-2", "hashA", anchor2)
      // Query 2 skipped getFiles and opens the same synthetic log...
      assert(getFiles2 == 0)
      assert(path2 == path1)
      val snapshot = DeltaLog.forTable(SparkSession.active, path2).update()
      assert(snapshot.version == 0)
      assert(snapshot.allFiles.count() == 2)
      // ...and has its own state in the cache entry.
      assert(CachedTableManager.INSTANCE.size() == 1)
      assert(CachedTableManager.INSTANCE.getQueryStateSize(cacheKey(profilePath, "hashA")) == 2)
      assert(anchor1 != null && anchor2 != null)
    }
  }

  test("delta log reuse: a later query reuses the urls after the first query is gone") {
    withQuerySpecificCache(reuseEnabled = true) { profilePath =>
      var anchor1: AnyRef = new Object()
      val (path1, _) = runQuery(profilePath, "query-1", "hashA", anchor1)

      // Query 1 ends: refresh() drops its state, but the entry keeps its urls.
      val anchor1Ref = new WeakReference(anchor1)
      anchor1 = null
      gcUntilCleared(anchor1Ref)
      CachedTableManager.INSTANCE.refresh()
      assert(CachedTableManager.INSTANCE.size() == 1)
      assert(CachedTableManager.INSTANCE.getQueryStateSize(cacheKey(profilePath, "hashA")) == 0)

      // A later query still reuses the log and urls.
      val anchor2 = new Object()
      val (path2, getFiles2) = runQuery(profilePath, "query-2", "hashA", anchor2)
      assert(getFiles2 == 0)
      assert(path2 == path1)
      assert(CachedTableManager.INSTANCE.getQueryStateSize(cacheKey(profilePath, "hashA")) == 1)
      assert(anchor2 != null)
    }
  }

  test("delta log reuse: when disabled, every query calls getFiles") {
    withQuerySpecificCache(reuseEnabled = false) { profilePath =>
      val anchor1 = new Object()
      val anchor2 = new Object()
      val (_, getFiles1) = runQuery(profilePath, "query-1", "hashA", anchor1)
      val (_, getFiles2) = runQuery(profilePath, "query-2", "hashA", anchor2)
      assert(getFiles1 == 1)
      assert(getFiles2 == 1)
      assert(anchor1 != null && anchor2 != null)
    }
  }

  test("delta log reuse: a different query shape does not reuse the log") {
    withQuerySpecificCache(reuseEnabled = true) { profilePath =>
      val anchorA = new Object()
      val anchorB = new Object()
      runQuery(profilePath, "query-1", "hashA", anchorA)
      val (pathB, getFilesB) = runQuery(profilePath, "query-2", "hashB", anchorB)
      assert(getFilesB == 1)
      assert(pathB.toString.endsWith("_hashB"))
      assert(CachedTableManager.INSTANCE.size() == 2)
      assert(anchorA != null && anchorB != null)
    }
  }

  test("delta log reuse: rebuilds the log when its blocks are gone") {
    withQuerySpecificCache(reuseEnabled = true) { profilePath =>
      val anchor1 = new Object()
      val (path1, _) = runQuery(profilePath, "query-1", "hashA", anchor1)
      // Drop the blocks but keep the urls: urls alone can't be reused. The cleanup takes the
      // `_delta_log` path, like its production callers, to match flat block ids too.
      DeltaSharingLogFileSystem.tryToCleanUpDeltaLog(s"$path1/_delta_log")
      assert(!DeltaSharingLogFileSystem.isSnapshotDeltaLogReusable(
        DeltaSharingLogFileSystem.decode(path1)))
      assert(CachedTableManager.INSTANCE.size() == 1)

      val anchor2 = new Object()
      val (path2, getFiles2) = runQuery(profilePath, "query-2", "hashA", anchor2)
      assert(getFiles2 == 1)
      assert(path2 == path1)
      assert(DeltaLog.forTable(SparkSession.active, path2).update().allFiles.count() == 2)
      assert(anchor1 != null && anchor2 != null)
    }
  }

  test("delta log reuse: rebuilds the log when its urls are no longer cached") {
    withQuerySpecificCache(reuseEnabled = true) { profilePath =>
      val anchor1 = new Object()
      runQuery(profilePath, "query-1", "hashA", anchor1)
      // Drop the urls but keep the blocks.
      CachedTableManager.INSTANCE.clear()

      val anchor2 = new Object()
      val (_, getFiles2) = runQuery(profilePath, "query-2", "hashA", anchor2)
      assert(getFiles2 == 1)
      assert(CachedTableManager.INSTANCE.getQueryStateSize(cacheKey(profilePath, "hashA")) == 1)
      assert(anchor1 != null && anchor2 != null)
    }
  }

  test("delta log reuse: no reuse with the Spark Parquet IO cache off") {
    // With the IO cache off, entries are plain even with a query id, so they can't be reused.
    withQuerySpecificCache(reuseEnabled = true) { profilePath =>
      withSQLConf("spark.delta.sharing.client.sparkParquetIOCache.enabled" -> "false") {
        val anchor1 = new Object()
        val anchor2 = new Object()
        runQuery(profilePath, "query-1", "hashA", anchor1)
        val (_, getFiles2) = runQuery(profilePath, "query-2", "hashA", anchor2)
        assert(getFiles2 == 1)
        assert(anchor1 != null && anchor2 != null)
      }
    }
  }

  test("delta log reuse: no reuse without a query-specific cache entry") {
    // The file profile provider has no query id, so the entry is plain and can't be reused.
    withProfile { profilePath =>
      withSQLConf(
        "spark.delta.sharing.client.sparkParquetIOCache.enabled" -> "true",
        DeltaSQLConf.DELTA_SHARING_SNAPSHOT_DELTA_LOG_REUSE_ENABLED.key -> "true"
      ) {
        CachedTableManager.INSTANCE.clear()
        val anchor1 = new Object()
        val anchor2 = new Object()
        runQuery(profilePath, "unused", "hashA", anchor1)
        val (_, getFiles2) = runQuery(profilePath, "unused", "hashA", anchor2)
        assert(getFiles2 == 1)
        assert(anchor1 != null && anchor2 != null)
      }
    }
  }

}
