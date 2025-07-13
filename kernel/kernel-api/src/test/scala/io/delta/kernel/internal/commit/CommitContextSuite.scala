package io.delta.kernel.internal.commit

import java.util.Optional

import io.delta.kernel.Operation
import io.delta.kernel.data.Row
import io.delta.kernel.internal.actions.{CommitInfo, SingleAction}
import io.delta.kernel.internal.transaction.{TransactionDataSource, TransactionV2State}
import io.delta.kernel.internal.util.{Clock, Utils}
import io.delta.kernel.test.{ActionUtils, MockEngineUtils}
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class CommitContextSuite extends AnyFunSuite with ActionUtils with MockEngineUtils {

  private val dataPath = "/path/to/table"
  private val logPath = s"$dataPath/_delta_log"
  private val schema = new StructType()
    .add("part1", IntegerType.INTEGER).add("col1", IntegerType.INTEGER)
  private val partCols = Seq("part1")
  private val protocol = catalogManagedSupportedProtocol
  private val metadata = testMetadata(schema, partCols)
  private val emptyDataActionsIterator = new CloseableIterator[Row] {
    override def hasNext: Boolean = false
    override def next(): Row = throw new NoSuchElementException("No more elements")
    override def close(): Unit = {}
  }
  private val uninvokableEngine = mockEngine()

  private def createTestTxnState(
      isCreateOrReplace: Boolean,
      operation: Operation,
      readTableOpt: Optional[TransactionDataSource],
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
      Optional.empty())
  }

  private val createTableTxnState = createTestTxnState(
    isCreateOrReplace = true,
    Operation.CREATE_TABLE,
    readTableOpt = Optional.empty(),
    isProtocolUpdate = true,
    isMetadataUpdate = true)

  test("getFinalizedActions can only be called once") {
    val commitContext = CommitContextImpl
      .forInitialCommit(uninvokableEngine, createTableTxnState, emptyDataActionsIterator)

    commitContext.getFinalizedActions()

    assertThrows[IllegalStateException] {
      commitContext.getFinalizedActions()
    }
  }

  test("getFinalizedActions metadata actions are in the order of: CommitInfo, Metadata, Protocol") {
    val commitContext = CommitContextImpl
      .forInitialCommit(uninvokableEngine, createTableTxnState, emptyDataActionsIterator)
    val finalizedActions = commitContext.getFinalizedActions

    assert(!finalizedActions.next().isNullAt(SingleAction.COMMIT_INFO_ORDINAL))
    assert(!finalizedActions.next().isNullAt(SingleAction.METADATA_ORDINAL))
    assert(!finalizedActions.next().isNullAt(SingleAction.PROTOCOL_ORDINAL))
  }

  // TODO: getFinalizedActions metadata actions are equal to the CommitMetadata actions
}
