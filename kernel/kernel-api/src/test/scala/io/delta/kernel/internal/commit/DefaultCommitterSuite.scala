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
import io.delta.kernel.test.{ActionUtils, BaseMockJsonHandler, MockFileSystemClientUtils, VectorTestUtils}
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class DefaultCommitterSuite extends AnyFunSuite
    with MockFileSystemClientUtils
    with ActionUtils
    with VectorTestUtils {

  private val protocol12 = new Protocol(1, 2)

  private val basicFileSystemNonUpdateCommitMetadata = new CommitMetadata(
    1L,
    "/fake/_delta_log",
    testCommitInfo,
    Optional.of(protocol12),
    Optional.of(basicPartitionedMetadata),
    Optional.empty(),
    Optional.empty())

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
        .withProtocolAndMetadata(readProtocol, metadata)
        .atVersion(1)
        .build(emptyMockEngine)
        .getCommitter

      assert(committer.isInstanceOf[DefaultFileSystemManagedTableOnlyCommitter])

      val exMsg = intercept[UnsupportedOperationException] {
        committer.commit(
          emptyMockEngine,
          emptyActionsIterator,
          new CommitMetadata(
            3L,
            "/fake/_delta_log",
            testCommitInfo,
            Optional.of(readProtocol),
            Optional.of(metadata),
            Optional.of(newProtocol),
            Optional.of(metadata)))
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
      thrownException: Exception,
      expectedRetryable: Boolean,
      expectedConflict: Boolean,
      expectedMessageFragment: String,
      expectedCauseType: Class[_])

  private val exceptionTestCases = Seq(
    ExceptionTestCase(
      description = "FileAlreadyExistsException",
      thrownException = new FileAlreadyExistsException("_delta_log/001.json"),
      expectedRetryable = true,
      expectedConflict = true,
      expectedMessageFragment = "Concurrent write detected for version 1",
      expectedCauseType = classOf[FileAlreadyExistsException]),
    ExceptionTestCase(
      description = "IOException",
      thrownException = new IOException("Network timeout writing to _delta_log/001.json"),
      expectedRetryable = true,
      expectedConflict = false,
      expectedMessageFragment = "Failed to write commit file due to I/O error",
      expectedCauseType = classOf[IOException]),
    ExceptionTestCase(
      description = "RuntimeException",
      thrownException = new RuntimeException("Unexpected error during write"),
      expectedRetryable = false,
      expectedConflict = false,
      expectedMessageFragment = "Failed to write commit file",
      expectedCauseType = classOf[RuntimeException]),
    ExceptionTestCase(
      description = "SecurityException",
      thrownException = new SecurityException("Access denied"),
      expectedRetryable = false,
      expectedConflict = false,
      expectedMessageFragment = "Failed to write commit file",
      // SecurityException will be wrapped
      expectedCauseType = classOf[KernelEngineException]),
    ExceptionTestCase(
      description = "IllegalArgumentException",
      thrownException = new IllegalArgumentException("Invalid argument"),
      expectedRetryable = false,
      expectedConflict = false,
      expectedMessageFragment = "Failed to write commit file",
      // IllegalArgumentException will be wrapped
      expectedCauseType = classOf[KernelEngineException]))

  exceptionTestCases.foreach { testCase =>
    test(s"default committer handles ${testCase.description} correctly " +
      s"(retryable=${testCase.expectedRetryable}, conflict=${testCase.expectedConflict})") {

      val throwingEngine = mockEngine(jsonHandler = new BaseMockJsonHandler {
        override def writeJsonFileAtomically(
            filePath: String,
            data: CloseableIterator[Row],
            overwrite: Boolean): Unit = {
          throw testCase.thrownException
        }
      })

      val ex = intercept[CommitFailedException] {
        DefaultFileSystemManagedTableOnlyCommitter.INSTANCE.commit(
          throwingEngine,
          emptyActionsIterator,
          basicFileSystemNonUpdateCommitMetadata)
      }

      assert(ex.isRetryable == testCase.expectedRetryable)
      assert(ex.isConflict == testCase.expectedConflict)
      assert(ex.getMessage.contains(testCase.expectedMessageFragment))
      assert(testCase.expectedCauseType.isInstance(ex.getCause))
    }
  }

  //////////////////////////////////////////////////////
  // DefaultCommitter exception handling tests -- END //
  //////////////////////////////////////////////////////

}
