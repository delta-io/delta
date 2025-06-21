package io.delta.kernel.defaults.catalogManaged

import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.Optional

import io.delta.kernel.catalogmanaged.CatalogManagedUtils
import io.delta.kernel.commit.CommitPayload
import io.delta.kernel.defaults.catalogManaged.client.InMemoryCatalogManagedTestClient
import io.delta.kernel.defaults.catalogManaged.utils.CatalogManagedTestFixtures
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.commit.DefaultCommitPayload
import io.delta.kernel.internal.table.ResolvedTableInternal
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

/**
 * This suite tests that the [[InMemoryCatalogManagedTestClient]] is correct. It does not test
 * Kernel itself.
 */
class InMemoryCatalogManagedTestClientSuite
    extends AnyFunSuite
    with InMemoryCatalogManagedTestClient
    with CatalogManagedTestFixtures
    with TestUtils {

  override def engine: Engine = defaultEngine

  test("forceCommit: (commitType=STAGED) creates staged commit file and adds commit in catalog") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val logPath = s"$path/_delta_log"
      val commitVersion = 0L

      forceCommit(emptyCommitPayload(logPath, commitVersion), CommitType.STAGED)

      assert(tables.containsKey(logPath))
      val tableData = tables.get(logPath)

      assert(tableData.maxRatifiedVersion == commitVersion)
      assert(tableData.commits.containsKey(commitVersion))
      val commitData = tableData.commits.get(commitVersion)

      assert(commitData.fileStatusOpt.isDefined)
      val stagedFilePath = commitData.fileStatusOpt.get.getPath
      assert(Files.exists(Paths.get(stagedFilePath)))
    }
  }

  // TODO: test("forceCommit enforces sequential versions")

  test("publish: moves staged commit to published location and removes commit from catalog") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val logPath = s"$path/_delta_log"
      val commitVersion = 0L

      forceCommit(emptyCommitPayload(logPath, commitVersion), CommitType.STAGED)

      assert(tables.get(logPath).commits.containsKey(commitVersion))

      publish(logPath, commitVersion)

      assert(!tables.get(logPath).commits.containsKey(commitVersion))

      val expectedPublishedFilePath =
        CatalogManagedUtils.getPublishedDeltaFilePath(logPath, commitVersion)

      assert(Files.exists(Paths.get(expectedPublishedFilePath)))
    }
  }

  test("loadTable: correctly parses commits from the catalog into ParsedLogData") {
    withTempDir { dir =>
      val path = dir.toURI.toString.stripSuffix("/")
      val logPath = s"$path/_delta_log"

      forceCommit(
        new DefaultCommitPayload(
          logPath,
          0L,
          CloseableIterator.empty(),
          Optional.of(protocol),
          Optional.of(metadata)),
        CommitType.STAGED)

      publish(logPath, 0L)

      forceCommit(emptyCommitPayload(logPath, 1L), CommitType.STAGED)

      val table = loadTable(path, versionToLoadOpt = Some(1)).asInstanceOf[ResolvedTableInternal]

      val deltas = table.getLogSegment.getDeltas
      assert(deltas.size() == 2)
      assert(FileNames.isPublishedDeltaFile(deltas.get(0).getPath))
      assert(FileNames.isStagedDeltaFile(deltas.get(1).getPath))
    }
  }

  private def emptyCommitPayload(logPath: String, version: Long): CommitPayload = {
    new DefaultCommitPayload(
      logPath,
      version,
      CloseableIterator.empty(),
      Optional.empty(),
      Optional.empty())
  }
}
