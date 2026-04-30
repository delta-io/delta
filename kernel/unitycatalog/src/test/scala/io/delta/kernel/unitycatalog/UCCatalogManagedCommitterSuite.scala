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

package io.delta.kernel.unitycatalog

import java.io.IOException
import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.commit.{CommitFailedException, CommitMetadata}
import io.delta.kernel.commit.CommitMetadata.CommitType
import io.delta.kernel.data.Row
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.internal.util.{Tuple2 => KernelTuple2}
import io.delta.kernel.test.{BaseMockJsonHandler, MockFileSystemClientUtils, TestFixtures, VectorTestUtils}
import io.delta.kernel.unitycatalog.adapters.UniformAdapter
import io.delta.kernel.utils.{CloseableIterable, CloseableIterator, FileStatus}
import io.delta.storage.commit.{CommitFailedException => StorageCFE}
import io.delta.storage.commit.Commit
import io.delta.storage.commit.uccommitcoordinator.InvalidTargetTableException

import InMemoryUCClient.TableData
import org.scalatest.funsuite.AnyFunSuite

class UCCatalogManagedCommitterSuite
    extends AnyFunSuite
    with UCCatalogManagedTestUtils
    with TestFixtures
    with VectorTestUtils
    with MockFileSystemClientUtils {

  private val testUcTableId = "testUcTableId"

  // ============================================================
  // ===================== Misc. Unit Tests =====================
  // ============================================================

  test("constructor throws on null inputs") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")

    assertThrows[NullPointerException] {
      new UCCatalogManagedCommitter(null, testUcTableId, baseTestTablePath)
    }
    assertThrows[NullPointerException] {
      new UCCatalogManagedCommitter(ucClient, null, baseTestTablePath)
    }
    assertThrows[NullPointerException] {
      new UCCatalogManagedCommitter(ucClient, testUcTableId, null)
    }
  }

  test("commit throws on null inputs") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val committer = new UCCatalogManagedCommitter(ucClient, testUcTableId, baseTestTablePath)

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
    val committer = new UCCatalogManagedCommitter(ucClient, testUcTableId, baseTestTablePath)
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
      val committer = new UCCatalogManagedCommitter(ucClient, testUcTableId, baseTestTablePath)

      // version > 0 for updates, version = 0 for creates
      val version = if (testCase.readPandMOpt.isPresent) 1L else 0L

      val commitMetadata = createCommitMetadata(
        version = version,
        logPath = baseTestLogPath,
        readPandMOpt = testCase.readPandMOpt,
        newProtocolOpt = testCase.newProtocolOpt,
        newMetadataOpt = testCase.newMetadataOpt)

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

  test("writeDeltaFile returns real FileStatus") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val ucTableIdentifier = new UCTableIdentifier("cat", "sch", "tbl")
      val committer = new UCCatalogManagedCommitter(
        ucClient,
        testUcTableId,
        tablePath,
        ucTableIdentifier)
      val testValue = "TEST_FILE_STATUS_DATA"
      val actionsIterator = getSingleElementRowIter(testValue)

      val commitMetadata = createCommitMetadata(
        version = 0,
        logPath = logPath,
        newProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
        newMetadataOpt = Optional.of(basicPartitionedMetadata))

      // ===== WHEN =====
      val response = committer.commit(defaultEngine, actionsIterator, commitMetadata)

      // ===== THEN =====
      val fileStatus = response.getCommitLogData.getFileStatus
      assert(fileStatus.getSize > 0)
      assert(fileStatus.getModificationTime > 0)
    }
  }

  // ===============================================================
  // ===================== CATALOG_WRITE Tests =====================
  // ===============================================================

  test("CATALOG_WRITE: protocol and metadata changes are passed to UC client") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      ucClient.insertTableDataAfterCreate(testUcTableId)
      val committer = new UCCatalogManagedCommitter(ucClient, testUcTableId, tablePath)

      // ===== WHEN =====
      val protocolUpgrade = protocolWithCatalogManagedSupport
        .withFeature(TableFeatures.DELETION_VECTORS_RW_FEATURE)
      val metadataUpgrade = basicPartitionedMetadata
        .withMergedConfiguration(Map("foo" -> "bar").asJava)

      val commitMetadata = createCommitMetadata(
        version = 1,
        logPath = logPath,
        readPandMOpt = Optional.of(
          new KernelTuple2[Protocol, Metadata](
            protocolWithCatalogManagedSupport,
            basicPartitionedMetadata)),
        newProtocolOpt = Optional.of(protocolUpgrade),
        newMetadataOpt = Optional.of(metadataUpgrade))
      committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)

      // ===== THEN =====
      val updatedTableData = ucClient.getTablesCopy.get(testUcTableId).get
      val latestProtocol = updatedTableData.getCurrentProtocolOpt.get
      val latestMetadata = updatedTableData.getCurrentMetadataOpt.get
      assert(latestProtocol.getReaderFeatures === protocolUpgrade.getReaderFeatures)
      assert(latestProtocol.getWriterFeatures === protocolUpgrade.getWriterFeatures)
      assert(latestMetadata.getConfiguration === metadataUpgrade.getConfiguration)
    }
  }

  test("CATALOG_WRITE: Iceberg metadata is extracted and passed to UC client") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      ucClient.insertTableDataAfterCreate(testUcTableId)
      val committer = new UCCatalogManagedCommitter(ucClient, testUcTableId, tablePath)

      // ===== WHEN =====
      val icebergProperties = Map(
        UniformAdapter.ICEBERG_METADATA_LOCATION_KEY -> "s3://bucket/table/metadata/v1.json",
        UniformAdapter.ICEBERG_CONVERTED_DELTA_VERSION_KEY -> "1",
        UniformAdapter.ICEBERG_CONVERTED_DELTA_TIMESTAMP_KEY -> "2025-01-04T03:13:11.423").asJava

      val commitMetadata = createCommitMetadata(
        version = 1,
        logPath = logPath,
        committerProperties = () => icebergProperties,
        readPandMOpt = Optional.of(
          new KernelTuple2[Protocol, Metadata](
            protocolWithCatalogManagedSupport,
            basicPartitionedMetadata)))
      committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)

      // ===== THEN =====
      val updatedTableData = ucClient.getTablesCopy.get(testUcTableId).get
      val icebergOpt = updatedTableData.getCurrentIcebergOpt
      assert(icebergOpt.isDefined)
      val iceberg = icebergOpt.get
      assert(iceberg.getMetadataLocation === "s3://bucket/table/metadata/v1.json")
      assert(iceberg.getConvertedDeltaVersion === 1L)
      assert(iceberg.getConvertedDeltaTimestamp === "2025-01-04T03:13:11.423")
    }
  }

  test("CATALOG_WRITE: empty committer properties result in no Iceberg metadata") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      ucClient.insertTableDataAfterCreate(testUcTableId)
      val committer = new UCCatalogManagedCommitter(ucClient, testUcTableId, tablePath)

      // ===== WHEN =====
      val emptyProperties = Map.empty[String, String].asJava
      val commitMetadata = createCommitMetadata(
        version = 1,
        logPath = logPath,
        committerProperties = () => emptyProperties,
        readPandMOpt = Optional.of(
          new KernelTuple2[Protocol, Metadata](
            protocolWithCatalogManagedSupport,
            basicPartitionedMetadata)))
      committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)

      // ===== THEN =====
      val updatedTableData = ucClient.getTablesCopy.get(testUcTableId).get
      val icebergOpt = updatedTableData.getCurrentIcebergOpt
      assert(icebergOpt.isEmpty)
    }
  }

  test("CATALOG_WRITE: throws exception when convertedDeltaVersion " +
    "does not match commit version") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      ucClient.insertTableDataAfterCreate(testUcTableId)
      val committer = new UCCatalogManagedCommitter(ucClient, testUcTableId, tablePath)

      // ===== WHEN =====
      // Commit version is 1, but convertedDeltaVersion is 2 (mismatch)
      val icebergProperties = Map(
        UniformAdapter.ICEBERG_METADATA_LOCATION_KEY -> "s3://bucket/table/metadata/v2.json",
        UniformAdapter.ICEBERG_CONVERTED_DELTA_VERSION_KEY -> "2",
        UniformAdapter.ICEBERG_CONVERTED_DELTA_TIMESTAMP_KEY -> "2025-01-04T03:13:11.423").asJava

      val commitMetadata = createCommitMetadata(
        version = 1,
        logPath = logPath,
        committerProperties = () => icebergProperties,
        readPandMOpt = Optional.of(
          new KernelTuple2[Protocol, Metadata](
            protocolWithCatalogManagedSupport,
            basicPartitionedMetadata)))

      // ===== THEN =====
      val exception = intercept[IllegalStateException] {
        committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
      }
      assert(exception.getMessage.contains(
        "Uniform convertedDeltaVersion (2) must match commit version (1)"))
    }
  }

  test("CATALOG_WRITE: writes staged commit file and invokes UC client commit API (no P&M change") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      ucClient.insertTableDataAfterCreate(testUcTableId)

      val testValue = "TEST_COMMIT_DATA_12345"
      val actionsIterator = getSingleElementRowIter(testValue)
      val committer = new UCCatalogManagedCommitter(ucClient, testUcTableId, tablePath)
      val commitMetadata = catalogManagedWriteCommitMetadata(version = 1, logPath = logPath)

      // ===== WHEN =====
      val response = committer.commit(defaultEngine, actionsIterator, commitMetadata)

      // ===== THEN =====
      val stagedCommitFilePath = response.getCommitLogData.getFileStatus.getPath

      // Verify the staged commit file actually exists on disk
      val file = new java.io.File(new java.net.URI(stagedCommitFilePath))
      assert(file.exists())
      assert(file.isFile())

      // Read the file content and verify our test value was written
      val fileContent = scala.io.Source.fromFile(file).getLines().mkString("\n")
      assert(fileContent.contains(testValue))

      // Verify the file is in the correct location
      val expectedPattern =
        s"^file:$tablePath/_delta_log/_staged_commits/00000000000000000001\\.[^.]+\\.json$$"
      assert(stagedCommitFilePath.matches(expectedPattern))

      // Verify UC client was invoked and table was updated.
      val updatedTable = ucClient.getTablesCopy.get(testUcTableId).get
      assert(updatedTable.getMaxRatifiedVersion == 1)
      assert(updatedTable.getCommits.size == 1)

      // Assert that no P&M change in this txn => No P&M change sent to UC
      assert(updatedTable.getCurrentProtocolOpt.isEmpty)
      assert(updatedTable.getCurrentMetadataOpt.isEmpty)

      // Verify the new commit in UC has correct version
      val lastCommit = updatedTable.getCommits.last
      assert(lastCommit.getVersion == 1)
      assert(lastCommit.getFileStatus.getPath.toString == stagedCommitFilePath)
    }
  }

  test("CATALOG_WRITE: IOException writing staged commit => CFE(retryable=true, conflict=false)") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
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
      ucClient.insertTableData(testUcTableId, tableData)
      val committer = new UCCatalogManagedCommitter(ucClient, testUcTableId, tablePath)
      val commitMetadata = catalogManagedWriteCommitMetadata(2, logPath = logPath)

      // ===== WHEN =====
      val ex = intercept[CommitFailedException] {
        committer.commit(throwingEngine, emptyActionsIterator, commitMetadata)
      }

      // ===== THEN =====
      assert(ex.isRetryable && !ex.isConflict)
      assert(ex.getMessage.contains("Failed to write delta file due to: Network error"))
    }
  }

  test("CATALOG_WRITE: i.d.s.c.CommitFailedException during UC commit => kernel CFE") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
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
      ucClient.insertTableData(testUcTableId, tableData)
      val committer = new UCCatalogManagedCommitter(ucClient, testUcTableId, tablePath)
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
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId") {
        override def forceThrowInCommitMethod(): Unit = throw new IOException("UC network error")
      }
      val tableData = new TableData(maxRatifiedVersion = 1, commits = ArrayBuffer.empty[Commit])
      ucClient.insertTableData(testUcTableId, tableData)
      val committer = new UCCatalogManagedCommitter(ucClient, testUcTableId, tablePath)
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
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId") {
        override def forceThrowInCommitMethod(): Unit = {
          // A child type of UCCommitCoordinatorException
          throw new InvalidTargetTableException("Target table does not exist")
        }
      }
      val tableData = new TableData(maxRatifiedVersion = 1, commits = ArrayBuffer.empty[Commit])
      ucClient.insertTableData(testUcTableId, tableData)
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
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val testValue = "CREATE_TABLE_DATA_12345"
      val actionsIterator = getSingleElementRowIter(testValue)
      val ucTableIdentifier = new UCTableIdentifier("cat", "sch", "tbl")
      val committer = new UCCatalogManagedCommitter(
        ucClient,
        testUcTableId,
        tablePath,
        ucTableIdentifier)

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
      val expectedFilePath = s"file:$logPath/00000000000000000000.json"
      assert(publishedDeltaFilePath == expectedFilePath)

      val file = new java.io.File(new java.net.URI(publishedDeltaFilePath))
      assert(file.exists())
      assert(file.isFile())

      // Read the file content and verify our test value was written
      val fileContent = scala.io.Source.fromFile(file).getLines().mkString("\n")
      assert(fileContent.contains(testValue))
    }
  }

  test("CATALOG_CREATE: finalizes table in UC when UCTableIdentifier is provided") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)
      val ucTableIdentifier = new UCTableIdentifier(
        "cat",
        "sch",
        "tbl")

      // ===== WHEN =====
      val result = ucCatalogManagedClient
        .buildCreateTableTransaction(
          testUcTableId,
          tablePath,
          testSchema,
          "test-engine",
          ucTableIdentifier)
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())

      // ===== THEN =====
      // createImpl should have called finalizeCreate, registering the table
      assert(ucClient.getTablesCopy.contains("cat.sch.tbl"))

      // Verify the properties and columns sent to finalizeCreate
      val record = ucClient.getLastFinalizeCreateRecord.get
      assert(record.catalogName == "cat")
      assert(record.schemaName == "sch")
      assert(record.tableName == "tbl")
      assert(record.storageLocation.nonEmpty)

      // Verify Class B properties were computed from CommitMetadata
      val props = record.properties.asScala
      assert(props("delta.lastUpdateVersion") == "0")
      assert(props.contains("delta.lastCommitTimestamp"))
      assert(props("delta.minReaderVersion") == "3")
      assert(props("delta.minWriterVersion") == "7")
      assert(props("delta.feature.catalogManaged") == "supported")

      // Verify columns were derived from the schema
      val colNames = record.columns.asScala.map(_.getName)
      assert(colNames == testSchema.fieldNames().asScala)
    }
  }

  test("CATALOG_CREATE: skips finalization when UCTableIdentifier is absent") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      // 3-arg constructor: no UCTableIdentifier
      val committer = new UCCatalogManagedCommitter(ucClient, testUcTableId, tablePath)

      val commitMetadata = createCommitMetadata(
        version = 0,
        logPath = logPath,
        newProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
        newMetadataOpt = Optional.of(basicPartitionedMetadata))

      // ===== WHEN =====
      // Should succeed without throwing - finalization is skipped
      val response = committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)

      // ===== THEN =====
      // Delta file was written successfully
      assert(response != null)
      // No finalizeCreate was called
      assert(ucClient.getLastFinalizeCreateRecord.isEmpty)
    }
  }

  test("CATALOG_CREATE: CFE from finalizeCreate propagates as kernel CFE(retryable=true)") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      // Test the committer directly (not through TransactionImpl) to verify the exception type
      // before TransactionImpl's retry logic wraps it in MaxCommitRetryLimitReachedException.
      val ucClient = new InMemoryUCClient("ucMetastoreId") {
        override def forceThrowInFinalizeCreateMethod(): Unit =
          throw new StorageCFE(true, false, "UC server unreachable", null)
      }
      val ucTableIdentifier = new UCTableIdentifier("cat", "sch", "tbl")
      val committer = new UCCatalogManagedCommitter(
        ucClient,
        testUcTableId,
        tablePath,
        ucTableIdentifier)

      val commitMetadata = createCommitMetadata(
        version = 0,
        logPath = logPath,
        newProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
        newMetadataOpt = Optional.of(basicPartitionedMetadata))

      // ===== WHEN =====
      val ex = intercept[CommitFailedException] {
        committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)
      }

      // ===== THEN =====
      assert(ex.isRetryable && !ex.isConflict)
      assert(ex.getMessage.contains("UC server unreachable"))
    }
  }

  test("CATALOG_WRITE: does not finalize even when UCTableIdentifier is present") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      // A committer that has UCTableIdentifier AND would blow up if finalizeCreate were called.
      val ucClient = new InMemoryUCClient("ucMetastoreId") {
        // scalastyle:off throwerror
        override def forceThrowInFinalizeCreateMethod(): Unit =
          throw new AssertionError("finalizeCreate should not be called for CATALOG_WRITE")
        // scalastyle:on throwerror
      }
      ucClient.insertTableDataAfterCreate(testUcTableId)
      val ucTableIdentifier = new UCTableIdentifier("cat", "sch", "tbl")
      val committer = new UCCatalogManagedCommitter(
        ucClient,
        testUcTableId,
        tablePath,
        ucTableIdentifier)

      // CATALOG_WRITE at version 1 -- commit type routing sends this to writeImpl,
      // NOT createImpl, so finalization should never happen.
      val commitMetadata = catalogManagedWriteCommitMetadata(version = 1, logPath = logPath)

      // ===== WHEN =====
      // If finalizeCreate were called, the forceThrowInFinalizeCreateMethod override would
      // throw an AssertionError, causing this test to fail.
      committer.commit(defaultEngine, emptyActionsIterator, commitMetadata)

      // ===== THEN =====
      // The table was committed to UC (version 1 staged commit), but no finalization happened.
      val updatedTable = ucClient.getTablesCopy.get(testUcTableId).get
      assert(updatedTable.getMaxRatifiedVersion == 1)
      assert(!ucClient.getTablesCopy.contains("cat.sch.tbl"))
    }
  }

  test("CATALOG_CREATE: IOException during write throws CFE(retryable=true, conflict=false)") {
    withTempDirAndAllDeltaSubDirs { case (tablePath, logPath) =>
      // ===== GIVEN =====
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val throwingEngine = mockEngine(jsonHandler = new BaseMockJsonHandler {
        override def writeJsonFileAtomically(
            path: String,
            data: CloseableIterator[Row],
            overwrite: Boolean): Unit =
          throw new IOException("Network hiccup")
      })
      val ucTableIdentifier = new UCTableIdentifier("cat", "sch", "tbl")
      val committer = new UCCatalogManagedCommitter(
        ucClient,
        testUcTableId,
        tablePath,
        ucTableIdentifier)

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
      assert(ex.getMessage.contains("Failed to write delta file due to: Network hiccup"))
    }
  }

}
