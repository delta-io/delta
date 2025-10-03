/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.test

import java.io.ByteArrayInputStream
import java.util
import java.util.Optional

import io.delta.kernel.data.{ColumnarBatch, ColumnVector, FilteredColumnarBatch, Row}
import io.delta.kernel.engine._
import io.delta.kernel.expressions.{Column, Expression, ExpressionEvaluator, Predicate, PredicateEvaluator}
import io.delta.kernel.internal.actions.CommitInfo
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.{FileNames, Utils}
import io.delta.kernel.types.{DataType, StructType}
import io.delta.kernel.utils.{CloseableIterator, DataFileStatus, FileStatus}

/**
 * Contains broiler plate code for mocking [[Engine]] and its sub-interfaces.
 *
 * A concrete class is created for each sub-interface (e.g. [[FileSystemClient]]) with
 * default implementation (unsupported). Test suites can override a specific API(s)
 * in the sub-interfaces to mock the behavior as desired.
 *
 * Example:
 * {{{
 *   val myMockFileSystemClient = new BaseMockFileSystemClient() {
 *     override def listFrom(filePath: String): CloseableIterator[FileStatus] = {
 *        .. my mock code to return specific values for given file path ...
 *     }
 *   }
 *
 *   val myMockEngine = mockEngine(fileSystemClient = myMockFileSystemClient)
 * }}}
 */
trait MockEngineUtils {

  /**
   * Create a mock Engine with the given components. If a component is not provided, it will
   * throw an exception when accessed.
   */
  def mockEngine(
      fileSystemClient: FileSystemClient = null,
      jsonHandler: JsonHandler = null,
      parquetHandler: ParquetHandler = null,
      expressionHandler: ExpressionHandler = null): Engine = {
    new Engine() {
      override def getExpressionHandler: ExpressionHandler =
        Option(expressionHandler).getOrElse(
          throw new UnsupportedOperationException("not supported in this test suite"))

      override def getJsonHandler: JsonHandler =
        Option(jsonHandler).getOrElse(
          throw new UnsupportedOperationException("not supported in this test suite"))

      override def getFileSystemClient: FileSystemClient =
        Option(fileSystemClient).getOrElse(
          throw new UnsupportedOperationException("not supported in this test suite"))

      override def getParquetHandler: ParquetHandler =
        Option(parquetHandler).getOrElse(
          throw new UnsupportedOperationException("not supported in this test suite"))
    }
  }
}

/**
 * Base class for mocking [[JsonHandler]]
 */
trait BaseMockJsonHandler extends JsonHandler {
  override def parseJson(
      jsonStringVector: ColumnVector,
      outputSchema: StructType,
      selectionVector: Optional[ColumnVector]): ColumnarBatch =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def readJsonFiles(
      fileIter: CloseableIterator[FileStatus],
      physicalSchema: StructType,
      predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def writeJsonFileAtomically(
      filePath: String,
      data: CloseableIterator[Row],
      overwrite: Boolean): Unit =
    throw new UnsupportedOperationException("not supported in this test suite")
}

/**
 * Base class for mocking [[ParquetHandler]]
 */
trait BaseMockParquetHandler extends ParquetHandler with MockEngineUtils {
  override def readParquetFiles(
      fileIter: CloseableIterator[FileStatus],
      physicalSchema: StructType,
      predicate: Optional[Predicate]): CloseableIterator[FileReadResult] =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def writeParquetFiles(
      directoryPath: String,
      dataIter: CloseableIterator[FilteredColumnarBatch],
      statsColumns: util.List[Column]): CloseableIterator[DataFileStatus] =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def writeParquetFileAtomically(
      filePath: String,
      data: CloseableIterator[FilteredColumnarBatch]): Unit =
    throw new UnsupportedOperationException("not supported in this test suite")
}

/**
 * Base class for mocking [[ExpressionHandler]]
 */
trait BaseMockExpressionHandler extends ExpressionHandler {
  override def getPredicateEvaluator(
      inputSchema: StructType,
      predicate: Predicate): PredicateEvaluator =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def getEvaluator(
      inputSchema: StructType,
      expression: Expression,
      outputType: DataType): ExpressionEvaluator =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def createSelectionVector(values: Array[Boolean], from: Int, to: Int): ColumnVector =
    throw new UnsupportedOperationException("not supported in this test suite")
}

/**
 * Base class for [[FileSystemClient]]
 */
trait BaseMockFileSystemClient extends FileSystemClient {
  override def listFrom(filePath: String): CloseableIterator[FileStatus] =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def resolvePath(path: String): String =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def readFiles(
      readRequests: CloseableIterator[FileReadRequest]): CloseableIterator[ByteArrayInputStream] =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def mkdirs(path: String): Boolean =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def delete(path: String): Boolean =
    throw new UnsupportedOperationException("not supported in this test suite")

  override def getFileStatus(path: String): FileStatus =
    throw new UnsupportedOperationException("not supported in this test suite")
}

/**
 * A mock [[JsonHandler]] that reads a single file and returns a single [[ColumnarBatch]].
 * The columnar batch only contains the [[CommitInfo]] action with the `inCommitTimestamp`
 * column set to the value in the mapping.
 *
 * @param deltaVersionToICTMapping A mapping from delta version to inCommitTimestamp.
 */
class MockReadICTFileJsonHandler(
    deltaVersionToICTMapping: Map[Long, Long],
    add10ForStagedFiles: Boolean = false)
    extends BaseMockJsonHandler with VectorTestUtils {
  override def readJsonFiles(
      fileIter: CloseableIterator[FileStatus],
      physicalSchema: StructType,
      predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
    assert(fileIter.hasNext)
    val filePathStr = fileIter.next.getPath
    assert(FileNames.isCommitFile(filePathStr))
    val deltaVersion = FileNames.getFileVersion(new Path(filePathStr))
    assert(deltaVersionToICTMapping.contains(deltaVersion))

    var ict = deltaVersionToICTMapping(deltaVersion)
    // This enables us to have different ICT times for staged vs published delta files, which lets
    // us test that we use the correct file when both exist for a specific version
    if (add10ForStagedFiles && FileNames.isStagedDeltaFile(filePathStr)) {
      ict += 10
    }
    val schema = new StructType().add("commitInfo", CommitInfo.FULL_SCHEMA);
    Utils.singletonCloseableIterator(
      new ColumnarBatch {
        override def getSchema: StructType = schema

        override def getColumnVector(ordinal: Int): ColumnVector = {
          val struct = Seq(
            longVector(Seq(ict)), /* inCommitTimestamp */
            longVector(Seq(-1L)), /* timestamp */
            stringVector(Seq("engine")), /* engineInfo */
            stringVector(Seq("operation")), /* operation */
            mapTypeVector(Seq(Map("operationParameter" -> ""))), /* operationParameters */
            booleanVector(Seq(false)), /* isBlindAppend */
            stringVector(Seq("txnId")), /* txnId */
            mapTypeVector(Seq(Map("operationMetrics" -> ""))) /* operationMetrics */
          )
          ordinal match {
            case 0 => new ColumnVector {
                override def getDataType: DataType = schema

                override def getSize: Int = struct.head.getSize

                override def close(): Unit = {}

                override def isNullAt(rowId: Int): Boolean = false

                override def getChild(ordinal: Int): ColumnVector = struct(ordinal)
              }
          }
        }
        override def getSize: Int = 1
      })
  }
}
