package io.delta.kernel.defaults.ccv2

import java.util.Optional

import io.delta.kernel.Operation
import io.delta.kernel.ccv2.ResolvedTable
import io.delta.kernel.data.{ColumnVector, ColumnarBatch, FilteredColumnarBatch}
import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.defaults.ccv2.setup.{CCv2Client, InMemoryCatalogClient}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.utils.CloseableIterable
import org.scalatest.funsuite.AnyFunSuite

// scalastyle:off println
class InMemoryCCv2Suite extends AnyFunSuite
  with DeltaTableWriteSuiteBase
  with VectorTestUtils{

  private val catalogClient = new InMemoryCatalogClient()
  private val ccv2Client = new CCv2Client(defaultEngine, catalogClient)

  test("aaa") {
    createTableHelper("table_001", createData(1 to 10))

    for (min <- 11 to 131 by 10) {
      val max = min + 9
      println("=" * 50)
      appendDataHelper("table_001", createData(min to max))
    }
    println("=" * 50)
    readTableHelper("table_001")
  }

  private def createTableHelper(
      tableName: String,
      data: Seq[FilteredColumnarBatch]): Unit = {
    println(s"Creating staging table for $tableName")

    val stagingTableMetadata = ccv2Client.getStagingTableResolvedMetadata(
      tableName, defaultEngine, catalogClient)
    println(stagingTableMetadata.getPath)
    println(stagingTableMetadata.getVersion)

    val resolvedTable = ResolvedTable.fromResolvedMetadata(defaultEngine, stagingTableMetadata)

    val txn = resolvedTable
      .createTransactionBuilder(testEngineInfo, Operation.CREATE_TABLE)
      .withSchema(defaultEngine, testSchema)
      .build(defaultEngine)

    val txnState = txn.getTransactionState(defaultEngine)
    val stagedFiles = stageData(txnState, Map.empty, data.toList)
    val stagedActionsIterable = CloseableIterable.inMemoryIterable(stagedFiles)
    txn.commit(defaultEngine, stagedActionsIterable)
  }

  private def appendDataHelper(tableName: String, data: Seq[FilteredColumnarBatch]): Unit = {
    println(s"Appending data to $tableName")
    val resolvedMetadata = ccv2Client.getResolvedMetadata(tableName)
    val resolvedTable = ResolvedTable.fromResolvedMetadata(defaultEngine, resolvedMetadata)
    val txn = resolvedTable
      .createTransactionBuilder(testEngineInfo, Operation.WRITE)
      .build(defaultEngine)

    val txnState = txn.getTransactionState(defaultEngine)
    val stagedFiles = stageData(txnState, Map.empty, data.toList)
    val stagedActionsIterable = CloseableIterable.inMemoryIterable(stagedFiles)
    val txnCommitResult = txn.commit(defaultEngine, stagedActionsIterable)
    println(s"Successfully committed transaction with version ${txnCommitResult.getVersion}")

    // TODO: this is jenky -- the backfill might not yet be done
    txnCommitResult.getPostCommitHooks.forEach(hook => {
      println("=" * 25)
      println("Invoking post commit hook ...")
      hook.threadSafeInvoke(defaultEngine)
      println("=" * 25)
    })
  }

  private def readTableHelper(tableName: String): Unit = {
    val resolvedMetadata = ccv2Client.getResolvedMetadata(tableName)
    val resolvedTable = ResolvedTable.fromResolvedMetadata(defaultEngine, resolvedMetadata)
    val snapshot = resolvedTable.getSnapshot
    val readRows = readSnapshot(snapshot, engine = defaultEngine).map(TestRow(_))
    readRows.map(_.get(0).asInstanceOf[Int]).sorted.foreach(row => println(row))
  }

  private def createData(vals: Seq[Int]): Seq[FilteredColumnarBatch] = {
    Seq(columnarBatch(intVector(vals: _*)))
      .map(_.toFiltered)
      .toList // immutable
  }

  private def columnarBatch(vectors: ColumnVector*): ColumnarBatch = {
    val numRows = vectors.head.getSize
    vectors.tail.foreach(
      v => require(v.getSize == numRows, "All vectors should have the same size"))

    new DefaultColumnarBatch(numRows, testSchema, vectors.toArray)
  }

}
