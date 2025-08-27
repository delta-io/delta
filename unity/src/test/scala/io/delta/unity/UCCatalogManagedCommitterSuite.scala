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
import java.nio.file.{FileAlreadyExistsException, Files}
import java.util.{Optional, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.commit.{CommitFailedException, CommitMetadata}
import io.delta.kernel.commit.CommitMetadata.CommitType
import io.delta.kernel.data.Row
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.internal.util.{Tuple2 => KernelTuple2}
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.test.{BaseMockJsonHandler, MockFileSystemClientUtils, VectorTestUtils}
import io.delta.kernel.utils.{CloseableIterator, FileStatus}
import io.delta.storage.commit.Commit
import io.delta.storage.commit.uccommitcoordinator.InvalidTargetTableException
import io.delta.unity.InMemoryUCClient.TableData

import org.apache.hadoop.shaded.org.apache.commons.io.FileUtils
import org.scalatest.funsuite.AnyFunSuite

class UCCatalogManagedCommitterSuite
    extends AnyFunSuite
    with UCCatalogManagedTestUtils
    with VectorTestUtils
    with MockFileSystemClientUtils {

  /**
   * Utility to create a temp table directory as well as the _delta_log and
   * _delta_log/_staged_commits subdirectories. Executes the provided function `f` with the table
   * and delta log paths.
   *
   * Note: Normal Kernel txn execution will create the staged commits directory. This method should
   * only be used for mock unit tests that don't perform a full txn execution.
   */
  private def withTempTableAndLogPathAndStagedCommitFolderCreated(
      f: (String, String) => Unit): Unit = {
    val tempDir = Files.createTempDirectory(UUID.randomUUID().toString).toFile
    val tablePath = tempDir.getAbsolutePath
    val logPath = s"$tablePath/_delta_log"

    // This also creates the _delta_log directory
    defaultEngine.getFileSystemClient.mkdirs(s"$logPath/_staged_commits")

    try f(tablePath, logPath)
    finally {
      FileUtils.deleteDirectory(tempDir)
    }
  }

  private def createCommitMetadata(
      version: Long,
      logPath: String = baseTestLogPath,
      readPandMOpt: Optional[KernelTuple2[Protocol, Metadata]] = Optional.empty(),
      newProtocolOpt: Optional[Protocol] = Optional.empty(),
      newMetadataOpt: Optional[Metadata] = Optional.empty()): CommitMetadata = new CommitMetadata(
    version,
    logPath,
    testCommitInfo(),
    readPandMOpt,
    newProtocolOpt,
    newMetadataOpt)

  private def catalogManagedWriteCommitMetadata(
      version: Long,
      logPath: String = baseTestLogPath): CommitMetadata = createCommitMetadata(
    version = version,
    logPath = logPath,
    readPandMOpt = Optional.of(
      new KernelTuple2[Protocol, Metadata](
        protocolWithCatalogManagedSupport,
        basicPartitionedMetadata)))

  private def getSingleElementRowIter(elem: String): CloseableIterator[Row] = {
    import io.delta.kernel.defaults.integration.DataBuilderUtils
    import io.delta.kernel.types.{StringType, StructField, StructType}

    val schema = new StructType().add(new StructField("testColumn", StringType.STRING, true))
    val simpleRow = DataBuilderUtils.row(schema, elem)
    singletonCloseableIterator(simpleRow)
  }

  // ============================================================
  // ===================== Misc. Unit Tests =====================
  // ============================================================

  test("constructor throws on null inputs") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")

    assertThrows[NullPointerException] {
      new UCCatalogManagedCommitter(null, "ucTableId", baseTestTablePath)
    }
    assertThrows[NullPointerException] {
      new UCCatalogManagedCommitter(ucClient, null, baseTestTablePath)
    }
    assertThrows[NullPointerException] {
      new UCCatalogManagedCommitter(ucClient, "ucTableId", null)
    }
  }

  test("commit throws on null inputs") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", baseTestTablePath)

    // Null engine
    assertThrows[NullPointerException] {
      committer.commit(null, emptyActionsIterator, catalogManagedWriteCommitMetadata(version = 1))
    }

    // Null finalizedActions
    assertThrows[NullPointerException] {
      committer.commit(defaultEngine, null, catalogManagedWriteCommitMetadata(version = 1))
    }

    // Null commitMetadata
    assertThrows[NullPointerException] {
      committer.commit(defaultEngine, emptyActionsIterator, null)
    }
  }

  test("commit throws if CommitMetadata is for a different table") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", baseTestTablePath)
    val badCommitMetadata = catalogManagedWriteCommitMetadata(
      version = 1,
      "/path/to/different/table/_delta_log")

    val exMsg = intercept[IllegalArgumentException] {
      committer.commit(defaultEngine, emptyActionsIterator, badCommitMetadata)
    }.getMessage

    assert(exMsg.contains("Delta log path '/path/to/table/_delta_log' does not match expected " +
      "'/path/to/different/table/_delta_log'"))
  }

  // ========== CommitType Tests START ==========

  case class CommitTypeTestCase(
      readPandMOpt: Optional[KernelTuple2[Protocol, Metadata]] = Optional.empty(),
      newProtocolOpt: Optional[Protocol] = Optional.empty(),
      newMetadataOpt: Optional[Metadata] = Optional.empty(),
      expectedCommitType: CommitType)

  private val protocol12 = new Protocol(1, 2)

  private val unsupportedCommitTypesTestCases = Seq(
    CommitTypeTestCase(
      readPandMOpt = Optional.empty(),
      newProtocolOpt = Optional.of(protocol12),
      newMetadataOpt = Optional.of(basicPartitionedMetadata),
      expectedCommitType = CommitType.FILESYSTEM_CREATE),
    CommitTypeTestCase(
      readPandMOpt = Optional.of(new KernelTuple2(protocol12, basicPartitionedMetadata)),
      expectedCommitType = CommitType.FILESYSTEM_WRITE),
    CommitTypeTestCase(
      readPandMOpt = Optional.of(new KernelTuple2(protocol12, basicPartitionedMetadata)),
      newProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
      expectedCommitType = CommitType.FILESYSTEM_UPGRADE_TO_CATALOG),
    CommitTypeTestCase(
      readPandMOpt = Optional.of(
        new KernelTuple2(protocolWithCatalogManagedSupport, basicPartitionedMetadata)),
      newProtocolOpt = Optional.of(protocol12),
      expectedCommitType = CommitType.CATALOG_DOWNGRADE_TO_FILESYSTEM))

  unsupportedCommitTypesTestCases.foreach { testCase =>
    test(s"commit throws UnsupportedOperationException for ${testCase.expectedCommitType}") {
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", baseTestTablePath)

      // version > 0 for updates, version = 0 for creates
      val version = if (testCase.readPandMOpt.isPresent) 1L else 0L

      val commitMetadata = new CommitMetadata(
        version,
        baseTestLogPath,
        testCommitInfo(),
        testCase.readPandMOpt,
        testCase.newProtocolOpt,
        testCase.newMetadataOpt)

      assert(commitMetadata.getCommitType == testCase.expectedCommitType)

      val exception = intercept[UnsupportedOperationException] {
        committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
      }
      assert(exception.getMessage == s"Unsupported commit type: ${testCase.expectedCommitType}")
    }
  }

  // ========== CommitType Tests END ==========

  test("kernelFileStatusToHadoopFileStatus converts kernel FileStatus to Hadoop FileStatus") {
    // ===== GIVEN =====
    val kernelFileStatus = FileStatus.of("/path/to/file.json", 1024L, 1234567890L)

    // ===== WHEN =====
    val hadoopFileStatus =
      UCCatalogManagedCommitter.kernelFileStatusToHadoopFileStatus(kernelFileStatus)

    // ===== THEN =====
    // These are the fields that we care about, taken from the Kernel FileStatus
    assert(hadoopFileStatus.getPath.toString == "/path/to/file.json")
    assert(hadoopFileStatus.getLen == 1024L)
    assert(hadoopFileStatus.getModificationTime == 1234567890L)

    // These are defaults that we set
    assert(hadoopFileStatus.getAccessTime == 1234567890L) // same as modification time
    assert(!hadoopFileStatus.isDirectory)
    assert(hadoopFileStatus.getReplication == 1)
    assert(hadoopFileStatus.getBlockSize == 128 * 1024 * 1024) // 128MB
    assert(hadoopFileStatus.getOwner == "unknown")
    assert(hadoopFileStatus.getGroup == "unknown")
    assert(hadoopFileStatus.getPermission ==
      org.apache.hadoop.fs.permission.FsPermission.getFileDefault)
  }

  // ===============================================================
  // ===================== CATALOG_WRITE Tests =====================
  // ===============================================================

  test("CATALOG_WRITE: protocol change is currently not implemented") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", baseTestTablePath)
    val protocolUpgrade = protocolWithCatalogManagedSupport
      .withFeature(TableFeatures.DELETION_VECTORS_RW_FEATURE)
    val commitMetadata = createCommitMetadata(
      version = 1,
      readPandMOpt = Optional.of(
        new KernelTuple2[Protocol, Metadata](
          protocolWithCatalogManagedSupport,
          basicPartitionedMetadata)),
      newProtocolOpt = Optional.of(protocolUpgrade))

    val exMsg = intercept[UnsupportedOperationException] {
      committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
    }.getMessage
    assert(exMsg.contains("Protocol change is not yet implemented"))
  }

  test("CATALOG_WRITE: metadata change is currently not implemented") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", baseTestTablePath)
    val metadataUpgrade = basicPartitionedMetadata
      .withMergedConfiguration(Map("foo" -> "bar").asJava)
    val commitMetadata = createCommitMetadata(
      version = 1,
      readPandMOpt = Optional.of(
        new KernelTuple2[Protocol, Metadata](
          protocolWithCatalogManagedSupport,
          basicPartitionedMetadata)),
      newMetadataOpt = Optional.of(metadataUpgrade))

    val exMsg = intercept[UnsupportedOperationException] {
      committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
    }.getMessage
    assert(exMsg.contains("Metadata change is not yet implemented"))
  }

  test("CATALOG_WRITE: writes staged commit file and invokes UC client commit API") {
    withTempTableAndLogPathAndStagedCommitFolderCreated { case (tablePath, logPath) =>
      // ===== GIVEN =====
      // Set up UC client with initial table with maxRatifiedVersion = -1, numCommits = 0. This
      // represents a table that was just created and at version 0. We will then commit version 1.
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val tableData = new TableData(-1, ArrayBuffer[Commit]())
      ucClient.createTableIfNotExistsOrThrow("ucTableId", tableData)

      val testValue = "TEST_COMMIT_DATA_12345"
      val actionsIterator = getSingleElementRowIter(testValue)
      val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", tablePath)
      val commitMetadata = catalogManagedWriteCommitMetadata(version = 1, logPath = logPath)

      // ===== WHEN =====
      val response = committer.commit(defaultEngine, actionsIterator, commitMetadata)

      // ===== THEN =====
      val stagedCommitFilePath = response.getCommitLogData.getFileStatus.getPath

      // Verify the staged commit file actually exists on disk
      val file = new java.io.File(stagedCommitFilePath)
      assert(file.exists())
      assert(file.isFile())

      // Read the file content and verify our test value was written
      val fileContent = scala.io.Source.fromFile(file).getLines().mkString("\n")
      assert(fileContent.contains(testValue))

      // Verify the file is in the correct location
      val expectedPattern =
        s"^$tablePath/_delta_log/_staged_commits/00000000000000000001\\.[^.]+\\.json$$"
      assert(stagedCommitFilePath.matches(expectedPattern))

      // Verify UC client was invoked and table was updated.
      val updatedTable = ucClient.getTablesCopy.get("ucTableId").get
      assert(updatedTable.getMaxRatifiedVersion == 1)
      assert(updatedTable.getCommits.size == 1)

      // Verify the new commit in UC has correct version
      val lastCommit = updatedTable.getCommits.last
      assert(lastCommit.getVersion == 1)
      assert(lastCommit.getFileStatus.getPath.toString == stagedCommitFilePath)
    }
  }

  test("CATALOG_WRITE: IOException writing staged commit => CFE(retryable=true, conflict=false)") {
    withTempTableAndLogPathAndStagedCommitFolderCreated { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val throwingEngine = mockEngine(jsonHandler = new BaseMockJsonHandler {
        override def writeJsonFileAtomically(
            path: String,
            data: CloseableIterator[Row],
            overwrite: Boolean): Unit =
          throw new IOException("Network error")
      })

      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val tableData = new TableData(maxRatifiedVersion = 1, commits = ArrayBuffer.empty[Commit])
      ucClient.createTableIfNotExistsOrThrow("ucTableId", tableData)
      val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", tablePath)
      val commitMetadata = catalogManagedWriteCommitMetadata(2, logPath = logPath)

      // ===== WHEN =====
      val ex = intercept[CommitFailedException] {
        committer.commit(throwingEngine, emptyActionsIterator, commitMetadata)
      }

      // ===== THEN =====
      assert(ex.isRetryable && !ex.isConflict)
      assert(ex.getMessage.contains("Failed to write staged commit file due to: Network error"))
    }
  }

  test("CATALOG_WRITE: i.d.s.c.CommitFailedException during UC commit => kernel CFE") {
    withTempTableAndLogPathAndStagedCommitFolderCreated { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId") {
        override def forceThrowInCommitMethod(): Unit =
          throw new io.delta.storage.commit.CommitFailedException(
            true, // retryable
            true, // conflict
            "Storage conflict",
            null)
      }
      val tableData = new TableData(maxRatifiedVersion = 1, commits = ArrayBuffer.empty[Commit])
      ucClient.createTableIfNotExistsOrThrow("ucTableId", tableData)
      val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", tablePath)
      val commitMetadata = catalogManagedWriteCommitMetadata(2, logPath = logPath)
      // ===== WHEN =====
      val ex = intercept[CommitFailedException] {
        committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
      }

      // ===== THEN =====
      assert(ex.isRetryable && ex.isConflict)
      assert(ex.getMessage.contains("Storage conflict"))
    }
  }

  test("CATALOG_WRITE: IOException during UC commit => CFE(retryable=true, conflict=false)") {
    withTempTableAndLogPathAndStagedCommitFolderCreated { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId") {
        override def forceThrowInCommitMethod(): Unit = throw new IOException("UC network error")
      }
      val tableData = new TableData(maxRatifiedVersion = 1, commits = ArrayBuffer.empty[Commit])
      ucClient.createTableIfNotExistsOrThrow("ucTableId", tableData)
      val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", tablePath)
      val commitMetadata = catalogManagedWriteCommitMetadata(2, logPath = logPath)

      // ===== WHEN =====
      val ex = intercept[CommitFailedException] {
        committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
      }

      // ===== THEN =====
      assert(ex.isRetryable && !ex.isConflict)
      assert(ex.getMessage.contains("UC network error"))
    }
  }

  test("CATALOG_WRITE: i.d.s.c.u.UCCCE during UC commit => CFE(retryable=false, conflict=false)") {
    withTempTableAndLogPathAndStagedCommitFolderCreated { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId") {
        override def forceThrowInCommitMethod(): Unit = {
          // A child type of UCCommitCoordinatorException
          throw new InvalidTargetTableException("Target table does not exist")
        }
      }
      val tableData = new TableData(maxRatifiedVersion = 1, commits = ArrayBuffer.empty[Commit])
      ucClient.createTableIfNotExistsOrThrow("ucTableId", tableData)
      val committer = new UCCatalogManagedCommitter(ucClient, "unknownTableId", tablePath)
      val commitMetadata = catalogManagedWriteCommitMetadata(2, logPath = logPath)

      // ===== WHEN =====
      val ex = intercept[CommitFailedException] {
        committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
      }

      // ===== THEN =====
      assert(ex.getCause.isInstanceOf[InvalidTargetTableException])
      assert(!ex.isRetryable && !ex.isConflict)
      assert(ex.getMessage.contains("Target table does not exist"))
    }
  }

  // ================================================================
  // ===================== CATALOG_CREATE Tests =====================
  // ================================================================

  test("CATALOG_CREATE: writes published delta file for version 0") {
    withTempTableAndLogPathAndStagedCommitFolderCreated { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val testValue = "CREATE_TABLE_DATA_12345"
      val actionsIterator = getSingleElementRowIter(testValue)
      val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", tablePath)

      val commitMetadata = createCommitMetadata(
        version = 0,
        logPath = logPath,
        newProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
        newMetadataOpt = Optional.of(basicPartitionedMetadata))

      // ===== WHEN =====
      val response = committer.commit(defaultEngine, actionsIterator, commitMetadata)

      // ===== THEN =====
      val publishedDeltaFilePath = response.getCommitLogData.getFileStatus.getPath

      // Verify the published delta file exists and is version 0
      val expectedFilePath = s"$logPath/00000000000000000000.json"
      assert(publishedDeltaFilePath == expectedFilePath)

      val file = new java.io.File(publishedDeltaFilePath)
      assert(file.exists())
      assert(file.isFile())

      // Read the file content and verify our test value was written
      val fileContent = scala.io.Source.fromFile(file).getLines().mkString("\n")
      assert(fileContent.contains(testValue))

      // Validate that UC was not updated for v0
      // TODO: [delta-io/delta#5118] If UC changes CREATE semantics, update logic here.
      assert(!ucClient.getTablesCopy.contains("ucTableId"))
    }
  }

  test("CATALOG_CREATE: FileAlreadyExistsException returns success") {
    withTempTableAndLogPathAndStagedCommitFolderCreated { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val throwingEngine = mockEngine(jsonHandler = new BaseMockJsonHandler {
        override def writeJsonFileAtomically(
            path: String,
            data: CloseableIterator[Row],
            overwrite: Boolean): Unit =
          throw new FileAlreadyExistsException("File already exists")
      })
      val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", tablePath)

      val commitMetadata = createCommitMetadata(
        version = 0,
        logPath = logPath,
        newProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
        newMetadataOpt = Optional.of(basicPartitionedMetadata))

      // ===== WHEN =====
      val response = committer.commit(throwingEngine, emptyActionsIterator, commitMetadata)

      // ===== THEN =====
      val publishedDeltaFilePath = response.getCommitLogData.getFileStatus.getPath
      val expectedFilePath = s"$logPath/00000000000000000000.json"
      assert(publishedDeltaFilePath == expectedFilePath)
    }
  }

  test("CATALOG_CREATE: IOException during write throws CFE(retryable=true, conflict=false)") {
    withTempTableAndLogPathAndStagedCommitFolderCreated { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val throwingEngine = mockEngine(jsonHandler = new BaseMockJsonHandler {
        override def writeJsonFileAtomically(
            path: String,
            data: CloseableIterator[Row],
            overwrite: Boolean): Unit =
          throw new IOException("Network hiccup")
      })
      val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", tablePath)

      val commitMetadata = createCommitMetadata(
        version = 0,
        logPath = logPath,
        newProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
        newMetadataOpt = Optional.of(basicPartitionedMetadata))

      // ===== WHEN =====
      val ex = intercept[CommitFailedException] {
        committer.commit(throwingEngine, emptyActionsIterator, commitMetadata)
      }

      // ===== THEN =====
      assert(ex.isRetryable && !ex.isConflict)
      assert(ex.getMessage.contains("Failed to write published delta file due to: Network hiccup"))
    }
  }

}
