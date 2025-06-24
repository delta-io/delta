package io.delta.dsv2.read

import io.delta.dsv2.utils.SchemaUtils
import io.delta.kernel.data.{ColumnVector => KernelColumnVector, FilteredColumnarBatch => KernelFilteredColumnarBatch}

import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.vectorized.{ColumnarArray, ColumnarBatch => SparkColumnarBatch, ColumnarMap, ColumnVector => SparkColumnVector}
import org.apache.spark.unsafe.types.UTF8String

class KernelColumnarBatchToSparkColumnarBatchWrapper(
    columns: Array[SparkColumnVector],
    numRows: Int)
    extends SparkColumnarBatch(columns, numRows)

object KernelColumnarBatchToSparkColumnarBatchWrapper {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def apply(kernelFilteredColumnarBatch: KernelFilteredColumnarBatch)
      : KernelColumnarBatchToSparkColumnarBatchWrapper = {
    val kernelColumnarBatch = kernelFilteredColumnarBatch.getData
    val numColumns = kernelColumnarBatch.getSchema.length()
    var numRows = kernelColumnarBatch.getSize

    val desiredToActualRowIdMapping: Option[Array[Int]] =
      if (kernelFilteredColumnarBatch.getSelectionVector.isPresent) {
        val selectionVector = kernelFilteredColumnarBatch.getSelectionVector.get
        val rowIdMapping = scala.collection.mutable.ArrayBuffer.empty[Int]
        for (actualRowId <- 0 until selectionVector.getSize) {
          if (!selectionVector.isNullAt(actualRowId) && selectionVector.getBoolean(actualRowId)) {
            rowIdMapping += actualRowId

            logger.info(s"rowIdMapping index ${rowIdMapping.size - 1} -> $actualRowId")
          } else {
            logger.info(s"skipping actual selection vector row $actualRowId")
          }
        }
        logger.info(s"rowIdMapping size: ${rowIdMapping.size}")
        logger.info(s"number of skipped rows: ${selectionVector.getSize - rowIdMapping.size}")
        numRows = rowIdMapping.size
        Some(rowIdMapping.toArray)
      } else None

    logger.info(
      s"kernelColumnarBatch: numRows $numRows, numColumns: $numColumns, " +
        s"getSchema ${kernelColumnarBatch.getSchema}")

    val columns: Array[SparkColumnVector] = (0 until numColumns).map { i =>
      logger.info(
        s"Creating SparkColumnVector for column $i: " +
          s"${kernelColumnarBatch.getColumnVector(i).getDataType}")
      new KernelColumnVectorToSparkColumnVectorWrapper(
        kernelColumnarBatch.getColumnVector(i),
        desiredToActualRowIdMapping)
    }.toArray

    new KernelColumnarBatchToSparkColumnarBatchWrapper(columns, numRows)
  }
}

class KernelColumnVectorToSparkColumnVectorWrapper(
    kernelColumnVector: KernelColumnVector,
    desiredToActualRowIdMapping: Option[Array[Int]])
    extends SparkColumnVector(
      SchemaUtils.convertKernelDataTypeToSparkDataType(kernelColumnVector.getDataType)) {

  import KernelColumnVectorToSparkColumnVectorWrapper._

  private def getActualRowId(desiredRowId: Int): Int = {
    if (desiredToActualRowIdMapping.isDefined) {
      val actualRowId = desiredToActualRowIdMapping.get(desiredRowId)
      logger.info(s"desiredRowId: $desiredRowId --> actualRowId $actualRowId")
      actualRowId
    } else {
      desiredRowId
    }
  }

  override def close(): Unit = kernelColumnVector.close()

  override def hasNull: Boolean = true

  override def numNulls(): Int = 0

  override def isNullAt(rowId: Int): Boolean = kernelColumnVector.isNullAt(getActualRowId(rowId))

  override def getBoolean(rowId: Int): Boolean =
    kernelColumnVector.getBoolean(getActualRowId(rowId))

  override def getByte(rowId: Int): Byte = kernelColumnVector.getByte(getActualRowId(rowId))

  override def getShort(rowId: Int): Short = kernelColumnVector.getShort(getActualRowId(rowId))

  override def getInt(rowId: Int): Int = kernelColumnVector.getInt(getActualRowId(rowId))

  override def getLong(rowId: Int): Long = kernelColumnVector.getLong(getActualRowId(rowId))

  override def getFloat(rowId: Int): Float = kernelColumnVector.getFloat(getActualRowId(rowId))

  override def getDouble(rowId: Int): Double = kernelColumnVector.getDouble(getActualRowId(rowId))

  override def getUTF8String(rowId: Int): UTF8String =
    UTF8String.fromString(kernelColumnVector.getString(getActualRowId(rowId)))

  override def getBinary(rowId: Int): Array[Byte] =
    kernelColumnVector.getBinary(getActualRowId(rowId))

  override def getArray(rowId: Int): ColumnarArray = throw new UnsupportedOperationException(
    "getArray is not supported")

  override def getMap(rowId: Int): ColumnarMap = throw new UnsupportedOperationException(
    "getMap is not supported")

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal =
    throw new UnsupportedOperationException("getDecimal is not supported")

  override def getChild(ordinal: Int): SparkColumnVector =
    throw new UnsupportedOperationException("getChild is not supported")
}

object KernelColumnVectorToSparkColumnVectorWrapper {
  private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
}
