package io.delta.dsv2.read

import io.delta.kernel.{Scan => KernelScan}
import io.delta.kernel.defaults.internal.json.JsonUtils
import io.delta.kernel.engine.{Engine => KernelEngine}

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan => SparkScan}
import org.apache.spark.sql.types.{StructType => SparkStructType}
import org.apache.spark.sql.types.StructType

class DeltaScan(
    val kernelScan: KernelScan,
    tableEngine: KernelEngine,
    sparkReadSchema: SparkStructType)
    extends SparkScan
    with Batch {
  import DeltaScan._

  private val serializedScanState = JsonUtils.rowToJson(kernelScan.getScanState(tableEngine))

  /** Get the Kernel ScanFiles ColumnarBatchIter and convert to [[DeltaInputPartition]] array. */
  private lazy val planPartitions: Array[InputPartition] = {
    val scanFileAsInputPartitionBuffer = scala.collection.mutable.ArrayBuffer[DeltaInputPartition]()

    kernelScan
      .getScanFiles(tableEngine)
      .map { columnarBatch => columnarBatch.getRows }
      .forEachRemaining { rowIter =>
        rowIter.forEachRemaining { row =>
          val serializedScanFileRow = JsonUtils.rowToJson(row)
          logger.info(s"serializedScanFileRow: $serializedScanFileRow")
          val inputPartition = DeltaInputPartition(serializedScanFileRow, serializedScanState)
          scanFileAsInputPartitionBuffer += inputPartition
        }
      }

    scanFileAsInputPartitionBuffer.toArray
  }

  /////////////////////////
  // SparkScan Overrides //
  /////////////////////////

  override def readSchema(): StructType = sparkReadSchema

  override def toBatch: Batch = this

  /////////////////////
  // Batch Overrides //
  /////////////////////

  // A physical representation of a data source scan for batch queries. This interface is used to
  // provide physical information, like how many partitions the scanned data has, and how to read
  // records from the partitions.

  /**
   * Returns a list of input partitions. Each InputPartition represents a data split that can be
   * processed by one Spark task. The number of input partitions returned here is the same as the
   * number of RDD partitions this scan outputs.
   *
   * If the Scan supports filter pushdown, this Batch is likely configured with a filter and is
   * responsible for creating splits for that filter, which is not a full scan.
   *
   * This method will be called only once during a data source scan, to launch one Spark job.
   */
  override def planInputPartitions(): Array[InputPartition] = planPartitions

  override def createReaderFactory(): PartitionReaderFactory = {
    new DeltaReaderFactory()
  }
}

object DeltaScan {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////

/** Serialized and sent from the Driver to the Executors */
case class DeltaInputPartition(serializedScanFileRow: String, serializedScanState: String)
    extends InputPartition with Serializable
