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

package io.delta.kernel.defaults.catalogManaged

import scala.collection.JavaConverters._

import io.delta.kernel.{Operation, Table, Transaction}
import io.delta.kernel.data.{ColumnarBatch, Row}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.TransactionImpl
import io.delta.kernel.internal.actions.Protocol
import io.delta.kernel.internal.commit.DefaultCommitPayload
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.test.ActionUtils
import io.delta.kernel.types.{LongType, StructType}
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class InMemoryCatalogManagedE2ESuite
    extends AbstractCatalogMangedE2ESuite
    with InMemoryCatalogManagedTestClient {
  override val engine: Engine = defaultEngine
}

trait AbstractCatalogMangedE2ESuite
    extends AnyFunSuite
    with AbstractCatalogManagedTestClient
    with TestUtils
    with ActionUtils {

  case class PerPartitionData(partitionValues: Map[String, Literal], data: ColumnarBatch)

  val schema = new StructType().add("part1", LongType.LONG).add("col1", LongType.LONG)
  val partCols = Seq("part1")
  val metadata = testMetadata(schema, partCols)
  val protocol = new Protocol(3, 7)
  val partitionSize = 10

  def stageData(txnStateRow: Row, perPartitionData: PerPartitionData): CloseableIterator[Row] = {
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

  def createDataForPartition(partValue: Long, startOffset: Int, numRows: Int): PerPartitionData = {
    val baseValue = partValue * partitionSize
    val col1Values = (startOffset until (startOffset + numRows)).map(i => baseValue + i).toArray
    val part1Values = Array.fill(numRows)(partValue)

    val part1Vector = longVector(part1Values: _*)
    val col1Vector = longVector(col1Values: _*)
    val data = new DefaultColumnarBatch(numRows, schema, Array(part1Vector, col1Vector))
    val partValuesMap = Map("part1" -> Literal.ofLong(partValue))
    PerPartitionData(partValuesMap, data)
  }

  def createDataForFullPartition(partValue: Long): PerPartitionData = {
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

  def appendData(tablePath: String, partValues: Seq[Long]): Unit = {
    val logPath = s"$tablePath/_delta_log"
    val legacyTable = Table.forPath(engine, tablePath)
    val txn = legacyTable
      .createTransactionBuilder(engine, "engineInfo", Operation.WRITE)
      .build(engine)
      .asInstanceOf[TransactionImpl]
    val txnStateRow = txn.getTransactionState(engine)
    val dataActions = partValues.map { partValue =>
      stageData(txnStateRow, createDataForFullPartition(partValue))
    }.reduceLeft(_ combine _)
    val finalizedActions = txn.getFinalizedActions(engine, dataActions)
    // TODO: txn.getCommitPayload or something similar
    forceCommit(new DefaultCommitPayload(logPath, txn.getReadTableVersion + 1, finalizedActions))
  }

  test("basic read") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      createTable(path)
      appendData(path, Seq(0))
      appendData(path, Seq(1))
    }
  }
}
