package io.delta.unity

import java.util.{Comparator, Optional}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

import io.delta.kernel.internal.files.{ParsedCatalogCommitData, ParsedLogData}
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.CommitRangeBuilder.CommitBoundary
import io.delta.kernel.TableManager
import io.delta.storage.commit.Commit
import io.delta.storage.commit.uccommitcoordinator.UCClient
import io.delta.unity.InMemoryUCClient.TableData
import io.delta.unity.UCCatalogManagedClientSuite.{emptyLongOpt, InMemoryUCClientWithMetrics}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

class UCCatalogManagedClientSketchSuite extends AnyFunSuite with UCCatalogManagedTestUtils {

  private val emptyLongOpt = Optional.empty[java.lang.Long]()

  private def withUCClientAndTestTable(
    textFx: (UCClient, String, Long) => Unit): Unit = {
    val maxRatifiedVersion = 2L
    val tablePath = getTestResourceFilePath("catalog-owned-preview")
    val ucClient = new InMemoryUCClientWithMetrics("ucMetastoreId")
    val fs = FileSystem.get(new Configuration())
    val catalogCommits = Seq(
      // scalastyle:off line.size.limit
      getTestResourceFilePath("catalog-owned-preview/_delta_log/_staged_commits/00000000000000000001.4cb9708e-b478-44de-b203-53f9ba9b2876.json"),
      getTestResourceFilePath("catalog-owned-preview/_delta_log/_staged_commits/00000000000000000002.5b9bba4a-0085-430d-a65e-b0d38c1afbe9.json"))
      // scalastyle:on line.size.limit
      .map { path => fs.getFileStatus(new Path(path)) }
      .map { fileStatus =>
        new Commit(
          FileNames.deltaVersion(fileStatus.getPath.toString),
          fileStatus,
          fileStatus.getModificationTime)
      }
    val tableData = new TableData(maxRatifiedVersion, ArrayBuffer(catalogCommits: _*))
    ucClient.createTableIfNotExistsOrThrow("ucTableId", tableData)
    textFx(ucClient, tablePath, maxRatifiedVersion)
  }

  test("Option 1: Pass-through API") {
    withUCClientAndTestTable { (ucClient, tablePath, maxVersion) =>
      // ---- Query params ----
      val startVersionOpt = emptyLongOpt
      val startTimestampOpt: Optional[java.lang.Long] =
        Optional.of(1749830855993L) // TS of V0 in example table
      val endVersionOpt: Optional[java.lang.Long] = Optional.of(1L)
      val endTimestampOpt = emptyLongOpt

      // ---- Load the commit range using the UCCatalogManagedClient ----
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
      val commitRange = ucCatalogManagedClient.loadCommitRange(
        defaultEngine,
        "ucTableId",
        tablePath,
        startVersionOpt,
        startTimestampOpt,
        endVersionOpt,
        endTimestampOpt)

      // ---- Ta-da: query your commitRange ----
      assert(commitRange.getStartVersion == 0)
      assert(commitRange.getEndVersion == 1)
    }
  }

  test("Option 2: Provide static file-status conversion utils only") {
    withUCClientAndTestTable { (ucClient, tablePath, maxVersion) =>
      // ---- Query params ----
      val startVersionOpt = Optional.empty()
      val startTimestampOpt = Optional.of(1749830855993L) // TS of V0 in example table
      val endVersionOpt: Optional[java.lang.Long] = Optional.of(1L)
      val endTimestampOpt = Optional.empty()

      // ---- Get the commits from UC ----
      val hasTsQuery = startTimestampOpt.isPresent || endTimestampOpt.isPresent
      // If we have a TS boundary, we must load the latestSnapshot, and thus must ask for ALL the
      // commits
      val endVersionOptForGetCommits = if (hasTsQuery) emptyLongOpt else endVersionOpt
      val response = ucClient.getCommits(
        "ucTableId",
        new Path(tablePath).toUri(),
        Optional.empty(), /* startVersion */
        endVersionOptForGetCommits /* endVersion */)
      // Potentially validate maxRatifiedVersion?
      // Convert response commits to ParsedLogData
      val logData = response.getCommits().asScala.sortBy(_.getVersion).map { commit =>
        ParsedLogData.forFileStatus(
          UCCatalogManagedClient.hadoopFileStatusToKernelFileStatus(
            commit.getFileStatus()))
      }.asJava

      // ---- Build the commit range ----
      lazy val latestSnapshot = TableManager.loadSnapshot(tablePath)
        .withLogData(logData)
        .build(defaultEngine)
      val commitRange = TableManager.loadCommitRange(tablePath)
        .withStartBoundary(CommitBoundary.atTimestamp(startTimestampOpt.get, latestSnapshot))
        .withEndBoundary(CommitBoundary.atVersion(endVersionOpt.get))
        .withLogData(logData)
        .build(defaultEngine)

      // ---- Ta-da: query your commitRange ----
      assert(commitRange.getStartVersion == 0)
      assert(commitRange.getEndVersion == 1)
    }
  }

  test("Option 3: Provide getCommits utils") {
    withUCClientAndTestTable { (ucClient, tablePath, maxVersion) =>
      // ---- Query params ----
      val startVersionOpt = Optional.empty()
      val startTimestampOpt = Optional.of(1749830855993L) // TS of V0 in example table
      val endVersionOpt: Optional[java.lang.Long] = Optional.of(1L)
      val endTimestampOpt = Optional.empty()

      // ---- Get the commits from UC ----
      val hasTsQuery = startTimestampOpt.isPresent || endTimestampOpt.isPresent
      // If we have a TS boundary, we must load the latestSnapshot, and thus must ask for ALL the
      // commits
      val endVersionOptForGetCommits = if (hasTsQuery) emptyLongOpt else endVersionOpt
      // We could make this getCommits call a static-util
      val logData = new UCCatalogManagedClient(ucClient)
        .getCommits(defaultEngine, "ucTableId", tablePath, endVersionOptForGetCommits)

      // ---- Build the commit range ----
      lazy val latestSnapshot = TableManager.loadSnapshot(tablePath)
        .withLogData(logData)
        .build(defaultEngine)
      val commitRange = TableManager.loadCommitRange(tablePath)
        .withStartBoundary(CommitBoundary.atTimestamp(startTimestampOpt.get, latestSnapshot))
        .withEndBoundary(CommitBoundary.atVersion(endVersionOpt.get))
        .withLogData(logData)
        .build(defaultEngine)

      // ---- Ta-da: query your commitRange ----
      assert(commitRange.getStartVersion == 0)
      assert(commitRange.getEndVersion == 1)
    }
  }

}
