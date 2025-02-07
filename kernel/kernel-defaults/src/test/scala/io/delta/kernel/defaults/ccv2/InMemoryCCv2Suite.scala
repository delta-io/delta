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

  // DeltaTableWriteSuiteBase.testSchema is of INTEGER so that's what we produce here
  val smallDataBatch1 = Seq(columnarBatch(intVector((1 to 10) : _*)))
    .map(batch => new FilteredColumnarBatch(batch, Optional.empty()))
    .toList // immutable
  val smallDataBatch2 = Seq(columnarBatch(intVector((11 to 20) : _*)))
    .map(batch => new FilteredColumnarBatch(batch, Optional.empty()))
    .toList // immutable

  test("aaa") {

    {
      // ===== Step 1: Create a staging table (v0) =====
      val stagingTableMetadata = ccv2Client.getStagingTableResolvedMetadata(
        "table_000", defaultEngine, catalogClient)
      println(stagingTableMetadata.getPath)
      println(stagingTableMetadata.getVersion)

      val resolvedTable = ResolvedTable.fromResolvedMetadata(defaultEngine, stagingTableMetadata)

      val txn = resolvedTable
        .createTransactionBuilder(testEngineInfo, Operation.CREATE_TABLE)
        .withSchema(defaultEngine, testSchema)
        .build(defaultEngine)

      val txnState = txn.getTransactionState(defaultEngine)
      val stagedFiles = stageData(txnState, Map.empty, smallDataBatch1)
      val stagedActionsIterable = CloseableIterable.inMemoryIterable(stagedFiles)
      txn.commit(defaultEngine, stagedActionsIterable)
    }

    println("=" * 50)

    {
      // ===== Step 2: Read that table (v0) =====
      val resolvedMetadata = ccv2Client.getResolvedMetadata("table_000")
      assert(resolvedMetadata.getVersion === 0)

      println(resolvedMetadata.getPath)
      println(resolvedMetadata.getVersion)
      println(resolvedMetadata.getSchemaString)
      println(resolvedMetadata.getProtocol)
      println(resolvedMetadata.getMetadata)
      println(resolvedMetadata.getLogSegment)

      val resolvedTable = ResolvedTable.fromResolvedMetadata(defaultEngine, resolvedMetadata)
      val snapshot = resolvedTable.getSnapshot
      assert(snapshot.getVersion === 0)
      println(snapshot.getVersion)
      val readRows = readSnapshot(snapshot, engine = defaultEngine).map(TestRow(_))
      readRows.foreach(row => println(row))

      // ===== Step 3: Append to that resolved table (v1) =====
      val txn = resolvedTable
        .createTransactionBuilder(testEngineInfo, Operation.WRITE)
        .build(defaultEngine)

      val txnState = txn.getTransactionState(defaultEngine)
      val stagedFiles = stageData(txnState, Map.empty, smallDataBatch2)
      val stagedActionsIterable = CloseableIterable.inMemoryIterable(stagedFiles)
      txn.commit(defaultEngine, stagedActionsIterable)
    }

    println("=" * 50)

    {
      // ===== Step 5: Read that table (v1) =====
      val resolvedMetadata = ccv2Client.getResolvedMetadata("table_000")
      assert(resolvedMetadata.getVersion === 1)

      val resolvedTable = ResolvedTable.fromResolvedMetadata(defaultEngine, resolvedMetadata)
      val snapshot = resolvedTable.getSnapshot
      assert(snapshot.getVersion === 1)
      println(snapshot.getVersion)
      val readRows = readSnapshot(snapshot, engine = defaultEngine).map(TestRow(_))
      readRows.foreach(row => println(row))
    }

  }

  protected def columnarBatch(vectors: ColumnVector*): ColumnarBatch = {
    val numRows = vectors.head.getSize
    vectors.tail.foreach(
      v => require(v.getSize == numRows, "All vectors should have the same size"))

    new DefaultColumnarBatch(numRows, testSchema, vectors.toArray)
  }

}
