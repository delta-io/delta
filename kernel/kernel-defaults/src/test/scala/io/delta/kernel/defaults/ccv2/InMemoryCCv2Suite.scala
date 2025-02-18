package io.delta.kernel.defaults.ccv2

import io.delta.kernel.{Operation, Transaction}
import io.delta.kernel.ccv2.{CommitResult, ResolvedMetadata, ResolvedTable}
import io.delta.kernel.data.{ColumnVector, ColumnarBatch, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.defaults.ccv2.setup.{CCv2Client, InMemoryCatalogClient}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.utils.CloseableIterator
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
      printDiv
      appendDataHelper("table_001", createData(min to max))
      printDiv
    }
    printDiv
    readTableHelper("table_001")
    printDiv
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

    doCommitLoop(stagingTableMetadata, txn, stagedFiles)
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

    doCommitLoop(resolvedMetadata, txn, stagedFiles)
  }

  private def doCommitLoop(
      rm: ResolvedMetadata, txn: Transaction, stagedFiles: CloseableIterator[Row]): Unit = {
    val result = rm.commit(
      txn.getCommitAsVersion,
      txn.finalizeActions(defaultEngine, stagedFiles),
      txn.getUpdatedProtocol,
      txn.getUpdatedMetadata
    )

    println("Commit result type: " + result.resultString())
    println("Commit result attempt version: " + result.getCommitAttemptVersion)

    result match {
      case success: CommitResult.Success =>
        println(s"Commit succeeded with version ${success.getCommitVersion}")
      case fail: CommitResult.NonRetryableFailure =>
        println(s"Commit failed (non-retryable) with: ${fail.getMessage}")
      case retryable: CommitResult.RetryableFailure =>
        println(s"Commit failed (retryable) with: ${retryable.getMessage}")
    }
  }

  private def printDiv(): Unit = {
    println("=" * 100)
    println("=" * 100)
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
