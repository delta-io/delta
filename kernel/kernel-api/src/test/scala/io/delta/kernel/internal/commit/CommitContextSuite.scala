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

import java.util.Optional

import io.delta.kernel.{Operation, TableManager}
import io.delta.kernel.data.Row
import io.delta.kernel.internal.actions.{SetTransaction, SingleAction}
import io.delta.kernel.internal.table.{ResolvedTableBuilderImpl, ResolvedTableInternal}
import io.delta.kernel.internal.transaction.TransactionV2State
import io.delta.kernel.internal.util.Clock
import io.delta.kernel.test.{ActionUtils, MockEngineUtils, MockFileSystemClientUtils}
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class CommitContextSuite
    extends AnyFunSuite with ActionUtils with MockEngineUtils {

  private val dataPath = "/path/to/table"
  private val logPath = s"$dataPath/_delta_log"
  private val schema = new StructType()
    .add("part1", IntegerType.INTEGER).add("col1", IntegerType.INTEGER)
  private val partCols = Seq("part1")
  private val protocol = protocolWithCatalogManagedSupport
  private val metadata = testMetadata(schema, partCols)
  private val emptyDataActionsIterator = new CloseableIterator[Row] {
    override def hasNext: Boolean = false
    override def next(): Row = throw new NoSuchElementException("No more elements")
    override def close(): Unit = {}
  }
  private val emptyMockEngine = MockFileSystemClientUtils.createMockFSListFromEngine(Nil)

  private def createTestTxnState(
      isCreateOrReplace: Boolean,
      operation: Operation,
      readTableOpt: Optional[ResolvedTableInternal],
      isProtocolUpdate: Boolean,
      isMetadataUpdate: Boolean): TransactionV2State = {
    new TransactionV2State(
      isCreateOrReplace,
      "engineInfo",
      operation,
      dataPath,
      logPath,
      readTableOpt,
      protocol,
      metadata,
      isProtocolUpdate,
      isMetadataUpdate,
      new Clock {
        override def getTimeMillis: Long = System.currentTimeMillis()
      },
      Optional.of(new SetTransaction("appId", 123, Optional.empty() /* lastUpdated */ )))
  }

  private val createTableTxnState = createTestTxnState(
    isCreateOrReplace = true,
    Operation.CREATE_TABLE,
    readTableOpt = Optional.empty(),
    isProtocolUpdate = true,
    isMetadataUpdate = true)

  private val updateTableTxnState = createTestTxnState(
    isCreateOrReplace = false,
    Operation.WRITE,
    readTableOpt = Optional.of(
      TableManager
        .loadTable(dataPath)
        .asInstanceOf[ResolvedTableBuilderImpl]
        .atVersion(10L)
        .withProtocolAndMetadata(protocol, metadata)
        .build(emptyMockEngine)),
    isProtocolUpdate = false,
    isMetadataUpdate = false)

  test("getFinalizedActions can only be called once") {
    val commitContext = CommitContextImpl
      .forFirstCommitAttempt(emptyMockEngine, createTableTxnState, emptyDataActionsIterator)

    commitContext.getFinalizedActions()

    assertThrows[IllegalStateException] {
      commitContext.getFinalizedActions()
    }
  }

  test("getFinalizedActions metadata actions order: CommitInfo, Protocol, Metadata, SetTxn") {
    val commitContext = CommitContextImpl
      .forFirstCommitAttempt(emptyMockEngine, createTableTxnState, emptyDataActionsIterator)
    val finalizedActions = commitContext.getFinalizedActions

    assert(!finalizedActions.next().isNullAt(SingleAction.COMMIT_INFO_ORDINAL))
    assert(!finalizedActions.next().isNullAt(SingleAction.PROTOCOL_ORDINAL))
    assert(!finalizedActions.next().isNullAt(SingleAction.METADATA_ORDINAL))
    assert(!finalizedActions.next().isNullAt(SingleAction.TXN_ORDINAL))
  }

  test("CommitMetadata version = 0 for first commit attempt for CREATE") {
    val commitContext = CommitContextImpl
      .forFirstCommitAttempt(emptyMockEngine, createTableTxnState, emptyDataActionsIterator)

    assert(commitContext.getCommitMetadata.getVersion == 0L)
  }

  test("CommitMetadata version = readTable.version + 1 for first commit attempt for UPDATE") {
    val commitContext = CommitContextImpl
      .forFirstCommitAttempt(emptyMockEngine, updateTableTxnState, emptyDataActionsIterator)

    assert(commitContext.getCommitMetadata.getVersion == 11L)
  }

  // TODO: getFinalizedActions metadata actions are equal to the CommitMetadata actions
}
