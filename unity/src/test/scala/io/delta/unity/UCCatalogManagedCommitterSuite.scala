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

import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.commit.CommitMetadata
import io.delta.kernel.commit.CommitMetadata.CommitType
import io.delta.kernel.data.Row
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.utils.CloseableIterator
import io.delta.unity.InMemoryUCClient.TableData

import org.scalatest.funsuite.AnyFunSuite

class UCCatalogManagedCommitterSuite
    extends AnyFunSuite
    with UCCatalogManagedTestUtils
    with VectorTestUtils {

  private def createCommitMetadata(
      version: Long,
      logPath: String = baseTestLogPath,
      readProtocolOpt: Optional[Protocol] = Optional.empty(),
      readMetadataOpt: Optional[Metadata] = Optional.empty(),
      newProtocolOpt: Optional[Protocol] = Optional.empty(),
      newMetadataOpt: Optional[Metadata] = Optional.empty()): CommitMetadata = new CommitMetadata(
    version,
    logPath,
    testCommitInfo(),
    readProtocolOpt,
    readMetadataOpt,
    newProtocolOpt,
    newMetadataOpt)

  private def catalogManagedWriteCommitMetadata(
      version: Long,
      logPath: String = baseTestLogPath): CommitMetadata = createCommitMetadata(
    version = version,
    logPath = logPath,
    readProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
    readMetadataOpt = Optional.of(basicPartitionedMetadata))

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

  implicit class CommitMetadataOps(base: CommitMetadata) {
    def withCommitTypeOverride(commitType: CommitMetadata.CommitType): CommitMetadata = {
      new CommitMetadata(
        base.getVersion(),
        base.getDeltaLogDirPath(),
        base.getCommitInfo(),
        base.getReadProtocolOpt(),
        base.getReadMetadataOpt(),
        base.getNewProtocolOpt(),
        base.getNewMetadataOpt()) {
        override def getCommitType() = commitType
      }
    }
  }

  val unsupportedCommitTypes = Seq(
    CommitType.FILESYSTEM_CREATE,
    CommitType.CATALOG_CREATE,
    CommitType.FILESYSTEM_WRITE,
    CommitType.FILESYSTEM_UPGRADE_TO_CATALOG,
    CommitType.CATALOG_DOWNGRADE_TO_FILESYSTEM)

  unsupportedCommitTypes foreach { commitType =>
    test(s"commit throws UnsupportedOperationException for $commitType") {
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", baseTestTablePath)
      val version = commitType match {
        case CommitType.FILESYSTEM_CREATE | CommitType.CATALOG_CREATE => 0
        case _ => 1
      }
      val commitMetadata = catalogManagedWriteCommitMetadata(version = version)
        .withCommitTypeOverride(commitType)

      assert(commitMetadata.getCommitType == commitType)

      val exception = intercept[UnsupportedOperationException] {
        committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
      }
      assert(exception.getMessage == s"Unsupported commit type: $commitType")
    }
  }

  // ========== CommitType Tests END ==========

  test("commit protocol change is currently not implemented") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", baseTestTablePath)
    val protocolUpgrade = protocolWithCatalogManagedSupport
      .withFeature(TableFeatures.DELETION_VECTORS_RW_FEATURE)
    val commitMetadata = createCommitMetadata(
      version = 1,
      readProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
      readMetadataOpt = Optional.of(basicPartitionedMetadata),
      newProtocolOpt = Optional.of(protocolUpgrade))

    val exMsg = intercept[UnsupportedOperationException] {
      committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
    }.getMessage
    assert(exMsg.contains("Protocol change is not yet implemented"))
  }

  test("commit metadata change is currently not implemented") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val committer = new UCCatalogManagedCommitter(ucClient, "ucTableId", baseTestTablePath)
    val metadataUpgrade = basicPartitionedMetadata
      .withMergedConfiguration(Map("foo" -> "bar").asJava)
    val commitMetadata = createCommitMetadata(
      version = 1,
      readProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
      readMetadataOpt = Optional.of(basicPartitionedMetadata),
      newMetadataOpt = Optional.of(metadataUpgrade))

    val exMsg = intercept[UnsupportedOperationException] {
      committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
    }.getMessage
    assert(exMsg.contains("Metadata change is not yet implemented"))
  }

  test("commit writes staged commit file and invokes UC client commit API") {
    withTempDir { tempDir =>
      // ===== GIVEN =====
      val tablePath = tempDir.getAbsolutePath
      val logPath = s"$tablePath/_delta_log"

      // TODO do this in kernel
      defaultEngine.getFileSystemClient.mkdirs(s"$logPath/_staged_commits")

      // Setup UC client with an initial table at version 0
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val initialCommit = createCommit(0)
      val tableData = new TableData(0, ArrayBuffer(initialCommit))
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
      assert(file.length() > 0)

      // Read the file content and verify our test value was written
      val fileContent = scala.io.Source.fromFile(file).getLines().mkString("\n")
      assert(fileContent.contains(testValue))

      // Verify the file is in the correct location
      assert(stagedCommitFilePath.contains("_staged_commits"))
      assert(file.getName.contains("00000000000000000001"))
      assert(stagedCommitFilePath.startsWith(tablePath))

      // Verify UC client was invoked and table was updated
      val updatedTable = ucClient.getTablesCopy.get("ucTableId").get
      assert(updatedTable.getMaxRatifiedVersion == 1, "Max ratified version should be 1")
      assert(updatedTable.getCommits.size == 2, "Should have 2 commits (v0 and v1)")

      // Verify the new commit in UC has correct version
      val lastCommit = updatedTable.getCommits.last
      assert(lastCommit.getVersion == 1)
      assert(lastCommit.getFileStatus.getPath.toString == stagedCommitFilePath)
    }

  }

  private def getSingleElementRowIter(elem: String): CloseableIterator[Row] = {
    import io.delta.kernel.defaults.integration.DataBuilderUtils
    import io.delta.kernel.types.{StringType, StructField, StructType}

    val schema = new StructType().add(new StructField("testColumn", StringType.STRING, true))
    val simpleRow = DataBuilderUtils.row(schema, elem)
    singletonCloseableIterator(simpleRow)
  }
}
