package io.delta.kernel

import java.util.Optional

import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.client.{DefaultTableClient, TableClient}
import io.delta.kernel.data.{ColumnarBatch, Row}
import io.delta.kernel.expressions.{Expression, Literal}
import io.delta.kernel.types.{LongType, StructType}
import io.delta.kernel.utils.{CloseableIterator, DefaultKernelTestUtils}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class DeltaCoreAPISuite extends AnyFunSuite {

  def getRows(batchIter: CloseableIterator[ColumnarBatch]): Seq[Row] = {
    var result = Seq.empty[Row]
    try {
      while (batchIter.hasNext) {
        val batch = batchIter.next()
        val rowIter = batch.getRows
        try {
          while (rowIter.hasNext) {
            result = result :+ rowIter.next
          }
        } finally {
          rowIter.close()
        }
      }
    } finally {
      batchIter.close()
    }
    result
  }

  // TODO: serialize and deserialize
  test("end-to-end usage: reading a table with dv") {
    val path = DefaultKernelTestUtils.getTestResourceFilePath("basic-dv-no-checkpoint")
    val tableClient = DefaultTableClient.create(new Configuration())
    val table = Table.forPath(path)
    val snapshot = table.getLatestSnapshot(tableClient)

    // Go through the tableSchema and select the columns interested in reading
    val readSchema = new StructType().add("id", LongType.INSTANCE)
    val filter = Literal.TRUE

    val scanObject = scan(tableClient, snapshot, readSchema, filter)

    val fileIter = scanObject.getScanFiles(tableClient)
    val scanState = scanObject.getScanState(tableClient);

    val actualValueColumnValues = ArrayBuffer[Long]()
    while(fileIter.hasNext) {
      val fileColumnarBatch = fileIter.next()
      val dataBatches = Scan.readData(
        tableClient,
        scanState,
        fileColumnarBatch.getRows(),
        Optional.empty()
      )

      while (dataBatches.hasNext) {
        val batch = dataBatches.next()
        val selectionVector = batch.getSelectionVector()
        val valueColVector = batch.getData.getColumnVector(0)
        (0 to valueColVector.getSize()-1).foreach { i =>
          if (!selectionVector.isPresent || selectionVector.get.getBoolean(i)) {
            actualValueColumnValues.append(valueColVector.getLong(i))
          }
        }
      }
    }
    assert(actualValueColumnValues.toSet === Seq.range(start = 2, end = 10).toSet)
  }

  // TODO: failing because cannot find protocol
  test("end-to-end usage: reading a table with dv with checkpoint") {
    val path = DefaultKernelTestUtils.getTestResourceFilePath("basic-dv-with-checkpoint")
    val tableClient = DefaultTableClient.create(new Configuration())
    val table = Table.forPath(path)
    val snapshot = table.getLatestSnapshot(tableClient)

    // Go through the tableSchema and select the columns interested in reading
    val readSchema = new StructType().add("id", LongType.INSTANCE)
    val filter = Literal.TRUE

    val scanObject = scan(tableClient, snapshot, readSchema, filter)

    val fileIter = scanObject.getScanFiles(tableClient)
    val scanState = scanObject.getScanState(tableClient);

    val actualValueColumnValues = ArrayBuffer[Long]()
    while(fileIter.hasNext) {
      val fileColumnarBatch = fileIter.next()
      val dataBatches = Scan.readData(
        tableClient,
        scanState,
        fileColumnarBatch.getRows(),
        Optional.empty()
      )

      while (dataBatches.hasNext) {
        val batch = dataBatches.next()
        val selectionVector = batch.getSelectionVector()
        val valueColVector = batch.getData.getColumnVector(0)
        (0 until valueColVector.getSize()).foreach { i =>
          val value = valueColVector.getLong(i)
          if (value%11 == 0) { // should be deleted
            assert(selectionVector.isPresent && !selectionVector.get.getBoolean(i))
          } else { // should be present
            assert(!selectionVector.isPresent || selectionVector.get.getBoolean(i))
          }
        }
      }
    }
  }

  test("reads DeletionVectorDescriptor from json files") {
    val path = DefaultKernelTestUtils.getTestResourceFilePath("basic-dv-no-checkpoint")

    val readSchema = new StructType().add("id", LongType.INSTANCE)

    val tableClient = DefaultTableClient.create(new Configuration())
    val table = Table.forPath(path)
    val snapshot = table.getLatestSnapshot(tableClient)
    val scan = snapshot.getScanBuilder(tableClient)
      .withReadSchema(tableClient, readSchema)
      .build()

    val scanFilesIter = scan.getScanFiles(tableClient)
    val rows = getRows(scanFilesIter)
    val dvs = rows.filter(!_.isNullAt(5)).map(_.getStruct(5))

    // there should be 1 deletion vector
    assert(dvs.length == 1)

    val dv = dvs.head
    // storageType should be 'u'
    assert(dv.getString(0) == "u")
    // cardinality should be 2
    assert(dv.getLong(4) == 2)
  }

  // TODO: test that log replay with DV works correctly

  private def scan(
    tableClient: TableClient,
    snapshot: Snapshot,
    readSchema: StructType,
    filter: Expression = null): Scan = {
    var builder =
      snapshot.getScanBuilder(tableClient).withReadSchema(tableClient, readSchema)
    if (filter != null) {
      builder = builder.withFilter(tableClient, filter)
    }
    builder.build()
  }
}