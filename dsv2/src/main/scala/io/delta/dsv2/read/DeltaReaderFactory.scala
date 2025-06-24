package io.delta.dsv2.read

import io.delta.dsv2.read.{KernelColumnarBatchToSparkColumnarBatchWrapper, KernelRowToSparkRowWrapper}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.{Scan => KernelScan}
import io.delta.kernel.defaults.internal.json.JsonUtils
import io.delta.kernel.internal.InternalScanFileUtils
import io.delta.kernel.internal.data.ScanStateRow
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.utils.CloseableIterator
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Serialized and sent from the Driver to the Executors */
class DeltaReaderFactory extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {

    require(partition.isInstanceOf[DeltaInputPartition])

    new DeltaPartitionReaderOfRows(partition.asInstanceOf[DeltaInputPartition])
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {

    require(partition.isInstanceOf[DeltaInputPartition])

    new DeltaPartitionReaderOfColumnarBatch(partition.asInstanceOf[DeltaInputPartition])
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    SparkSession.active.sparkContext.getConf
      .getBoolean("io.delta.kernel.spark.supportColumnarReads", defaultValue = true)
  }
}

object DeltaReaderFactory {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/** Created on Executor */
abstract class DeltaPartitionReader[T](deltaInputPartition: DeltaInputPartition)
    extends PartitionReader[T] {
  protected val engine = new DefaultEngine(new Configuration())

  protected val scanFileRow = JsonUtils.rowFromJson(
    deltaInputPartition.serializedScanFileRow,
    InternalScanFileUtils.SCAN_FILE_SCHEMA)

  protected val addFileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow)

  protected val scanStateRow =
    JsonUtils.rowFromJson(deltaInputPartition.serializedScanState, ScanStateRow.SCHEMA)

  protected val physicalRowDataIter = engine.getParquetHandler
    .readParquetFiles(
      Utils.singletonCloseableIterator(addFileStatus),
      ScanStateRow.getPhysicalDataReadSchema(engine, scanStateRow),
      java.util.Optional.empty() /* predicate */ )

  protected val logicalRowDataColumnarBatchIter =
    KernelScan.transformPhysicalData(engine, scanStateRow, scanFileRow, physicalRowDataIter)
}

/** Created on the executor. */
class DeltaPartitionReaderOfRows(deltaInputPartition: DeltaInputPartition)
    extends DeltaPartitionReader[InternalRow](deltaInputPartition) {
  import DeltaPartitionReaderOfRows._

  logger.info("DeltaPartitionReaderOfRows constructed")

  private var rowIter: CloseableIterator[io.delta.kernel.data.Row] = null
  private var curr: io.delta.kernel.data.Row = null
  private var closed = false

  override def close(): Unit = {
    logicalRowDataColumnarBatchIter.close()
    if (rowIter != null) {
      rowIter.close()
    }
    closed = true
  }

  override def next(): Boolean = {
    if (!closed) {
      // Check if there are remaining rows in the current row iterator
      if (rowIter != null && rowIter.hasNext) {
        curr = rowIter.next()
        true
      }
      // If current batch is exhausted, fetch the next batch and reset the row iterator
      else if (logicalRowDataColumnarBatchIter.hasNext) {
        rowIter = logicalRowDataColumnarBatchIter.next().getRows
        next() // Recursively call next to process the new batch
      } else {
        false
      }
    } else {
      false
    }
  }

  override def get(): InternalRow = {
    if (curr == null) {
      throw new NoSuchElementException("No current row available; call next() first.")
    }
    new KernelRowToSparkRowWrapper(curr)
  }
}

object DeltaPartitionReaderOfRows {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

class DeltaPartitionReaderOfColumnarBatch(deltaInputPartition: DeltaInputPartition)
    extends DeltaPartitionReader[ColumnarBatch](deltaInputPartition) {
  import DeltaPartitionReaderOfColumnarBatch._

  logger.info("DeltaPartitionReaderOfColumnarBatch constructed")

  private var currentBatch: KernelColumnarBatchToSparkColumnarBatchWrapper = null
  private var closed = false

  override def next(): Boolean = {
    if (!closed) {
      if (logicalRowDataColumnarBatchIter.hasNext) {
        val kernelBatch = logicalRowDataColumnarBatchIter.next()
        currentBatch = KernelColumnarBatchToSparkColumnarBatchWrapper(kernelBatch)
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  override def get(): ColumnarBatch = {
    if (currentBatch == null) {
      throw new NoSuchElementException("No current batch available; call next() first.")
    }
    currentBatch
  }

  override def close(): Unit = {
    if (!closed) {
      logicalRowDataColumnarBatchIter.close()
      if (currentBatch != null) {
        currentBatch.close()
      }
      closed = true
    }
  }
}

object DeltaPartitionReaderOfColumnarBatch {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}
