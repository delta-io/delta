/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.unity

import java.io.IOException

import io.delta.kernel.commit.PublishFailedException
import io.delta.kernel.internal.files.ParsedCatalogCommitData
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.test.{BaseMockFileSystemClient, MockFileSystemClientUtils, TestFixtures, VectorTestUtils}

import org.scalatest.funsuite.AnyFunSuite

class UCPublishingSuite
    extends AnyFunSuite
    with UCCatalogManagedTestUtils
    with TestFixtures
    with VectorTestUtils
    with MockFileSystemClientUtils {

  private def createCommitter(tablePath: String): UCCatalogManagedCommitter = {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    new UCCatalogManagedCommitter(ucClient, "testUcTableId", tablePath)
  }

  private def toFile(path: String): java.io.File = {
    if (path.startsWith("file:")) {
      new java.io.File(new java.net.URI(path))
    } else {
      new java.io.File(path)
    }
  }

  private def readFile(path: String): String = {
    scala.io.Source.fromFile(toFile(path)).getLines().mkString("\n")
  }

  private def assertFileExists(path: String): Unit = {
    assert(toFile(path).exists(), s"File should exist: $path")
  }

  /**
   * Helper to create a staged commit file and return its ParsedCatalogCommitData.
   * Just writes the file directly without using the committer.
   */
  private def writeStagedCatalogCommit(
      logPath: String,
      version: Long,
      content: String = ""): ParsedCatalogCommitData = {
    val stagedPath = FileNames.stagedCommitFile(logPath, version)
    defaultEngine.getJsonHandler.writeJsonFileAtomically(
      stagedPath,
      getSingleElementRowIter(content),
      true /* overwrite */ )
    val fileStatus = defaultEngine.getFileSystemClient.getFileStatus(stagedPath)
    ParsedCatalogCommitData.forFileStatus(fileStatus)
  }

  test("publish: throws on null inputs") {
    val committer = createCommitter(baseTestTablePath)
    val publishMetadata = createPublishMetadata(
      snapshotVersion = 1,
      logPath = baseTestLogPath,
      catalogCommits = List(createStagedCatalogCommit(1, baseTestLogPath)))

    assertThrows[NullPointerException] {
      committer.publish(null, publishMetadata)
    }

    assertThrows[NullPointerException] {
      committer.publish(defaultEngine, null)
    }
  }

  test("publish: throws UnsupportedOperationException for inline commits") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val inlineCatalogCommit = ParsedCatalogCommitData.forInlineData(
        1L,
        emptyColumnarBatch)
      val committer = createCommitter(tablePath)
      val publishMetadata = createPublishMetadata(
        snapshotVersion = 1,
        logPath = logPath,
        catalogCommits = List(inlineCatalogCommit))

      // ===== WHEN =====
      val ex = intercept[UnsupportedOperationException] {
        committer.publish(defaultEngine, publishMetadata)
      }

      // ===== THEN =====
      assert(ex.getMessage.contains("Publishing inline catalog commits is not yet supported"))
    }
  }

  test("publish: multiple catalog commits successfully") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val catalogCommits = List(
        writeStagedCatalogCommit(logPath, 1, "COMMIT_V1"),
        writeStagedCatalogCommit(logPath, 2, "COMMIT_V2"),
        writeStagedCatalogCommit(logPath, 3, "COMMIT_V3"))
      val committer = createCommitter(tablePath)
      val publishMetadata = createPublishMetadata(
        snapshotVersion = 3,
        logPath = logPath,
        catalogCommits = catalogCommits)

      // ===== WHEN =====
      committer.publish(defaultEngine, publishMetadata)

      // ===== THEN =====
      assert(readFile(FileNames.deltaFile(logPath, 1)).contains("COMMIT_V1"))
      assert(readFile(FileNames.deltaFile(logPath, 2)).contains("COMMIT_V2"))
      assert(readFile(FileNames.deltaFile(logPath, 3)).contains("COMMIT_V3"))
    }
  }

  test("publish: does not overwrite existing published files") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val catalogCommit = writeStagedCatalogCommit(logPath, 1, "TEST_IDEMPOTENT_PUBLISH")
      val committer = createCommitter(tablePath)
      val publishMetadata = createPublishMetadata(
        snapshotVersion = 1,
        logPath = logPath,
        catalogCommits = List(catalogCommit))

      // ===== WHEN =====
      // Publish once
      committer.publish(defaultEngine, publishMetadata)
      val publishedTimestamp1 = defaultEngine
        .getFileSystemClient.getFileStatus(FileNames.deltaFile(logPath, 1)).getModificationTime

      // Publish again - should succeed but not overwrite existing file
      committer.publish(defaultEngine, publishMetadata)
      val publishedTimestamp2 = defaultEngine
        .getFileSystemClient.getFileStatus(FileNames.deltaFile(logPath, 1)).getModificationTime

      // ===== THEN =====
      assert(publishedTimestamp1 === publishedTimestamp2)
    }
  }

  test("publish: creates published file at correct location with identical content") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val catalogCommit = writeStagedCatalogCommit(logPath, 1, "VERSION_1")
      val committer = createCommitter(tablePath)
      val publishMetadata = createPublishMetadata(
        snapshotVersion = 1,
        logPath = logPath,
        catalogCommits = List(catalogCommit))

      // ===== WHEN =====
      committer.publish(defaultEngine, publishMetadata)

      // ===== THEN =====
      val stagedPath = catalogCommit.getFileStatus.getPath
      val publishedPath = FileNames.deltaFile(logPath, 1)

      // Verify staged file still exists (publish doesn't delete source)
      assertFileExists(stagedPath)

      // Verify published file exists at correct location
      assertFileExists(publishedPath)
      assert(FileNames.isPublishedDeltaFile(publishedPath))

      // Verify content was copied correctly
      assert(readFile(stagedPath) === readFile(publishedPath))
    }
  }

  test("publish: throws PublishFailedException on IOException") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val catalogCommit = writeStagedCatalogCommit(logPath, 1, "TEST_EXCEPTION")
      val throwingEngine = mockEngine(fileSystemClient = new BaseMockFileSystemClient {
        override def copyFileAtomically(
            srcPath: String,
            destPath: String,
            overwrite: Boolean): Unit = {
          throw new IOException("Network failure during copy")
        }
      })
      val committer = createCommitter(tablePath)
      val publishMetadata = createPublishMetadata(
        snapshotVersion = 1,
        logPath = logPath,
        catalogCommits = List(catalogCommit))

      // ===== WHEN =====
      val ex = intercept[PublishFailedException] {
        committer.publish(throwingEngine, publishMetadata)
      }

      // ===== THEN =====
      assert(ex.getMessage.contains("Failed to publish version 1"))
      assert(ex.getMessage.contains("Network failure during copy"))
      assert(ex.getCause.isInstanceOf[IOException])
    }
  }
}
