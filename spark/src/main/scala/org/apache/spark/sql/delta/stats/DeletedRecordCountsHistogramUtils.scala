/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.stats

import org.apache.spark.sql.delta.{DeltaErrors, DeltaUDF}
import org.apache.spark.sql.delta.ClassicColumnConversions._

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, DataType, LongType}
import org.apache.spark.unsafe.Platform

/**
 * This object contains helper functionality related to [[DeletedRecordCountsHistogram]].
 */
private[delta] object DeletedRecordCountsHistogramUtils {
  val BUCKET_BOUNDARIES = IndexedSeq(
    0L, 1L, 10L, 100L, 1000L, 10000L, 100000L, 1000000L, 10000000L, Int.MaxValue, Long.MaxValue)
  val NUMBER_OF_BINS = BUCKET_BOUNDARIES.length - 1

  def getDefaultBins: Array[Long] = Array.fill(NUMBER_OF_BINS)(0L)

  def emptyHistogram: DeletedRecordCountsHistogram =
    DeletedRecordCountsHistogram.apply(getDefaultBins)

  def getHistogramBin(dvCardinality: Long): Int = {
    import scala.collection.Searching._

    require(dvCardinality >= 0)

    if (dvCardinality == Long.MaxValue) return NUMBER_OF_BINS - 1

    BUCKET_BOUNDARIES.search(dvCardinality) match {
      case Found(index) =>
        index
      case InsertionPoint(insertionPoint) =>
        insertionPoint - 1
    }
  }

  /**
   * An imperative aggregate implementation of DeletedRecordCountsHistogram.
   *
   * The return type of this Imperative Aggregate is of ArrayType(LongType). The array
   * represents a [[DeletedRecordCountsHistogram]].
   *
   */
  case class DeletedRecordCountsHistogramAgg(
      child: Expression,
      mutableAggBufferOffset: Int = 0,
      inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[Array[Long]]
    with UnaryLike[Expression] {
    override def createAggregationBuffer(): Array[Long] = getDefaultBins

    override val dataType: DataType = ArrayType(LongType)

    // This Aggregate doesn't return null.
    override val nullable: Boolean = false

    override protected def withNewChildInternal(
        newChild: Expression): DeletedRecordCountsHistogramAgg = copy(child = newChild)

    override def update(aggBuffer: Array[Long], input: InternalRow): Array[Long] = {
      val value = child.eval(input)

      if (value != null) {
        val dvCardinality = value.asInstanceOf[Long]
        val index = getHistogramBin(dvCardinality)
        aggBuffer(index) += 1
      }
      aggBuffer
    }

    override def merge(buffer: Array[Long], input: Array[Long]): Array[Long] = {
      require(buffer.length == input.length)
      for (index <- buffer.indices) {
        buffer(index) += input(index)
      }
      buffer
    }

    override def eval(buffer: Array[Long]): Any = new GenericArrayData(buffer)

    /** Serializes the aggregation buffer to Array[Byte]. */
    override def serialize(buffer: Array[Long]): Array[Byte] = {
      require(buffer.length < 128)
      val bytesPerLong = 8
      // One 8bit value stores the number of elements, the remaining are bucket values.
      val serializedByteSize = (buffer.length * bytesPerLong) + 1
      val byteArray = new Array[Byte](serializedByteSize)
      // Add buffer length for validation.
      Platform.putByte(byteArray, Platform.BYTE_ARRAY_OFFSET, buffer.length.toByte)

      for (index <- buffer.indices) {
        val offset = Platform.BYTE_ARRAY_OFFSET + 1 + index * bytesPerLong
        Platform.putLong(byteArray, offset, buffer(index))
      }
      byteArray
    }

    /** De-serializes the serialized format Array[Byte], and produces aggregation buffer. */
    override def deserialize(bytes: Array[Byte]): Array[Long] = {
      val bytesPerLong = 8
      // One 8bit value stores the number of elements, the remaining are bucket values.
      val numElementsFromSerializedByteSize = (bytes.length - 1) / bytesPerLong
      val aggBuffer = new Array[Long](numElementsFromSerializedByteSize)
      // At the first byte we store the length of the deserialized buffer for validation purposes.
      val numElementsFromSerializedState = Platform.getByte(bytes, Platform.BYTE_ARRAY_OFFSET).toInt
      if (numElementsFromSerializedByteSize != numElementsFromSerializedState) {
        throw DeltaErrors.deletedRecordCountsHistogramDeserializationException()
      }
      for (index <- aggBuffer.indices) {
        val offset = Platform.BYTE_ARRAY_OFFSET + 1 + index * bytesPerLong
        aggBuffer(index) = Platform.getLong(bytes, offset)
      }
      aggBuffer
    }

    override def withNewMutableAggBufferOffset(offset: Int): DeletedRecordCountsHistogramAgg =
      copy(mutableAggBufferOffset = offset)

    override def withNewInputAggBufferOffset(offset: Int): DeletedRecordCountsHistogramAgg =
      copy(inputAggBufferOffset = offset)
  }

  /**
   * A UDF to convert a long array (returned by [[DeletedRecordCountsHistogramAgg]]) to
   * [[DeletedRecordCountsHistogram]].
   */
  private lazy val HistogramAggrToHistogramUDF = {
    DeltaUDF.deletedRecordCountsHistogramFromArrayLong { deletedRecordCountsHistogramArray =>
      new DeletedRecordCountsHistogram(deletedRecordCountsHistogramArray) }
  }

  def histogramAggregate(dvCardinalityExpr: Column): Column = {
    val aggregate =
      Column(DeletedRecordCountsHistogramAgg(dvCardinalityExpr.expr).toAggregateExpression())
    DeletedRecordCountsHistogramUtils.HistogramAggrToHistogramUDF(aggregate)
  }
}
