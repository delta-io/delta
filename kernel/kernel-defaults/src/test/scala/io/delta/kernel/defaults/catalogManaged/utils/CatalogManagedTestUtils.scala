package io.delta.kernel.defaults.catalogManaged.utils

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.{Operation, Table, Transaction}
import io.delta.kernel.data.{FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.catalogManaged.AbstractCatalogMangedE2ESuite
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.TransactionImpl
import io.delta.kernel.internal.commit.DefaultCommitPayload
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.test.ActionUtils
import io.delta.kernel.utils.CloseableIterator

trait CatalogManagedTestUtils extends TestUtils with ActionUtils {
  self: AbstractCatalogMangedE2ESuite =>

  case class PartitionedData(partitionValues: Map[String, Literal], data: FilteredColumnarBatch)

  def stageData(txnStateRow: Row, perPartitionData: PartitionedData): CloseableIterator[Row] = {
    val partValues = perPartitionData.partitionValues.asJava

    val physicalDataIter = Transaction.transformLogicalData(
      engine,
      txnStateRow,
      toCloseableIterator(Seq(perPartitionData.data).toIterator.asJava),
      partValues)

    val writeContext = Transaction.getWriteContext(engine, txnStateRow, partValues)

    val writeResultIter = engine
      .getParquetHandler
      .writeParquetFiles(
        writeContext.getTargetDirectory,
        physicalDataIter,
        writeContext.getStatisticsColumns)

    Transaction.generateAppendActions(engine, txnStateRow, writeResultIter, writeContext)
  }

  def createDataForPartition(partValue: Long, startOffset: Int, numRows: Int): PartitionedData = {
    val baseValue = partValue * partitionSize
    val col1Values = (startOffset until (startOffset + numRows)).map(i => baseValue + i).toArray
    val part1Values = Array.fill(numRows)(partValue)

    val part1Vector = longVector(part1Values: _*)
    val col1Vector = longVector(col1Values: _*)
    val data = new DefaultColumnarBatch(numRows, schema, Array(part1Vector, col1Vector))
    val partValuesMap = Map("part1" -> Literal.ofLong(partValue))
    PartitionedData(partValuesMap, new FilteredColumnarBatch(data, Optional.empty()))
  }

  def createDataForFullPartition(partValue: Long): PartitionedData = {
    createDataForPartition(partValue, 0, partitionSize)
  }

  // TODO: update this with proper ResolvedTable-based transaction write APIs
  def createTable(tablePath: String): Unit = {
    val logPath = s"$tablePath/_delta_log"
    val legacyTable = Table.forPath(engine, tablePath)
    val finalizedActions = legacyTable
      .createTransactionBuilder(engine, "engineInfo", Operation.CREATE_TABLE)
      .withSchema(engine, schema)
      .withPartitionColumns(engine, partCols.asJava)
      .build(engine)
      .asInstanceOf[TransactionImpl]
      .getFinalizedActions(engine, CloseableIterator.empty())
    forceCommit(new DefaultCommitPayload(logPath, 0, finalizedActions))
  }

  // TODO: Fix the below.
  // Note: We are still using the *legacy* Transaction API. It requires that the table exist in the
  //       filesystem (else, it asks for the schema). So, for now, before our test code calls
  //       `appendData`, we must call `publish`. Further, the transaction readVersion is not correct
  //       since it only knows about the filesystem deltas. Thus, we have to pass in the commit
  //       version.
  def appendData(tablePath: String, commitVersion: Long, partitionValues: Seq[Long]): Unit = {
    val logPath = s"$tablePath/_delta_log"
    val legacyTable = Table.forPath(engine, tablePath)
    val txn = legacyTable
      .createTransactionBuilder(engine, "engineInfo", Operation.WRITE)
      .build(engine)
      .asInstanceOf[TransactionImpl]
    val txnStateRow = txn.getTransactionState(engine)
    val dataActions = partitionValues.map { partValue =>
      stageData(txnStateRow, createDataForFullPartition(partValue))
    }.reduceLeft(_ combine _)
    val finalizedActions = txn.getFinalizedActions(engine, dataActions)
    // TODO: txn.getCommitPayload or something similar
    forceCommit(new DefaultCommitPayload(logPath, commitVersion, finalizedActions))
  }
}
