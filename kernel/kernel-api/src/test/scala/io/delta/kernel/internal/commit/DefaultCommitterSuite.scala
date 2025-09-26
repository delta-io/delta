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

package io.delta.kernel.internal.commit

import java.io.IOException
import java.nio.file.FileAlreadyExistsException
import java.util.Optional

import io.delta.kernel.TableManager
import io.delta.kernel.commit.{CommitFailedException, CommitMetadata}
import io.delta.kernel.data.Row
import io.delta.kernel.exceptions.KernelEngineException
import io.delta.kernel.internal.actions.Protocol
import io.delta.kernel.internal.table.SnapshotBuilderImpl
import io.delta.kernel.internal.util.{Tuple2 => KernelTuple2}
import io.delta.kernel.test.{ActionUtils, BaseMockJsonHandler, MockFileSystemClientUtils, TestFixtures, VectorTestUtils}
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class DefaultCommitterSuite extends AnyFunSuite
    with MockFileSystemClientUtils
    with TestFixtures
    with VectorTestUtils {

  private val protocol12 = new Protocol(1, 2)

  private val basicFileSystemCommitMetadataNoPMChange = createCommitMetadata(
    version = 1L,
    commitInfo = testCommitInfo(ictEnabled = false),
    readPandMOpt = Optional.of(new KernelTuple2(protocol12, basicPartitionedMetadata)))

  Seq(
    (protocol12, protocolWithCatalogManagedSupport, "Upgrade"),
    (protocolWithCatalogManagedSupport, protocol12, "Downgrade"),
    (
      protocolWithCatalogManagedSupport,
      protocolWithCatalogManagedSupport,
      "CatalogManagedWrite")).foreach { case (readProtocol, newProtocol, testCase) =>
    test(s"default committer does not support committing to catalog-managed tables -- $testCase") {
      val emptyMockEngine = createMockFSListFromEngine(Nil)
      val schema = new StructType().add("col1", IntegerType.INTEGER)
      val metadata = testMetadata(schema, Seq[String]())
      val committer = TableManager.loadSnapshot(dataPath.toString)
        .asInstanceOf[SnapshotBuilderImpl]
        .withProtocolAndMetadata(readProtocol, metadata)
        .atVersion(1)
        .build(emptyMockEngine)
        .getCommitter

      assert(committer.isInstanceOf[DefaultFileSystemManagedTableOnlyCommitter])

      val exMsg = intercept[UnsupportedOperationException] {
        committer.commit(
          emptyMockEngine,
          emptyActionsIterator,
          createCommitMetadata(
            version = 3L,
            readPandMOpt = Optional.of(new KernelTuple2(readProtocol, metadata)),
            newProtocolOpt = Optional.of(newProtocol),
            newMetadataOpt = Optional.of(metadata)))
      }.getMessage

      assert(exMsg.contains("No io.delta.kernel.commit.Committer has been provided to Kernel, so " +
        "Kernel is using a default Committer that only supports committing to " +
        "filesystem-managed Delta tables, not catalog-managed Delta tables. Since this table " +
        "is catalog-managed, this commit operation is unsupported"))
    }
  }

  ////////////////////////////////////////////////////////
  // DefaultCommitter exception handling tests -- START //
  ////////////////////////////////////////////////////////

  case class ExceptionTestCase(
      description: String,
      exceptionToThrow: Exception,
      expectedRetryableOpt: Option[Boolean],
      expectedConflictOpt: Option[Boolean],
      expectedThrownType: Class[_],
      expectedCauseType: Class[_])

  private val exceptionTestCases = Seq(
    ExceptionTestCase(
      description = "FileAlreadyExistsException -> CFE(true, true)",
      exceptionToThrow = new FileAlreadyExistsException("_delta_log/001.json"),
      expectedRetryableOpt = Some(true),
      expectedConflictOpt = Some(true),
      expectedThrownType = classOf[CommitFailedException],
      expectedCauseType = classOf[FileAlreadyExistsException]),
    ExceptionTestCase(
      description = "IOException -> CFE(true, false)",
      exceptionToThrow = new IOException("Network timeout writing to _delta_log/001.json"),
      expectedRetryableOpt = Some(true),
      expectedConflictOpt = Some(false),
      expectedThrownType = classOf[CommitFailedException],
      expectedCauseType = classOf[IOException]),
    ExceptionTestCase(
      description = "RuntimeException wrapped and thrown as KernelEngineException",
      exceptionToThrow = new RuntimeException("Some runtime error"),
      expectedRetryableOpt = None,
      expectedConflictOpt = None,
      expectedThrownType = classOf[KernelEngineException],
      expectedCauseType = classOf[RuntimeException]))

  exceptionTestCases.foreach { testCase =>
    test(s"default committer handles ${testCase.description} correctly") {

      val throwingEngine = mockEngine(jsonHandler = new BaseMockJsonHandler {
        override def writeJsonFileAtomically(
            filePath: String,
            data: CloseableIterator[Row],
            overwrite: Boolean): Unit = {
          throw testCase.exceptionToThrow
        }
      })

      val ex = intercept[Exception] {
        DefaultFileSystemManagedTableOnlyCommitter.INSTANCE.commit(
          throwingEngine,
          emptyActionsIterator,
          basicFileSystemCommitMetadataNoPMChange)
      }

      assert(ex.getClass == testCase.expectedThrownType)
      assert(ex.getCause.getClass == testCase.expectedCauseType)

      testCase.expectedRetryableOpt.foreach { expectedRetryable =>
        assert(ex.isInstanceOf[CommitFailedException])
        val commitEx = ex.asInstanceOf[CommitFailedException]
        assert(commitEx.isRetryable == expectedRetryable)
      }

      testCase.expectedConflictOpt.foreach { expectedConflict =>
        assert(ex.isInstanceOf[CommitFailedException])
        val commitEx = ex.asInstanceOf[CommitFailedException]
        assert(commitEx.isConflict == expectedConflict)
      }
    }
  }

  //////////////////////////////////////////////////////
  // DefaultCommitter exception handling tests -- END //
  //////////////////////////////////////////////////////

  test("success commit returns ParsedLogData containing FileStatus for that commit file") {
    val noOpEngine = mockEngine(jsonHandler = new BaseMockJsonHandler {
      override def writeJsonFileAtomically(
          filePath: String,
          data: CloseableIterator[Row],
          overwrite: Boolean): Unit = {
        // what's important is that we do *not* throw here
      }
    })

    val commitResult = DefaultFileSystemManagedTableOnlyCommitter.INSTANCE.commit(
      noOpEngine,
      emptyActionsIterator,
      basicFileSystemCommitMetadataNoPMChange)

    val commitParsedLogData = commitResult.getCommitLogData

    assert(commitParsedLogData.isFile)
    assert(commitParsedLogData.getVersion == basicFileSystemCommitMetadataNoPMChange.getVersion)
  }

}
