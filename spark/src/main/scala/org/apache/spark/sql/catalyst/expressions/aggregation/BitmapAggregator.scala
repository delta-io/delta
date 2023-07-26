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

package org.apache.spark.sql.catalyst.expressions.aggregation

import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types._

/**
 * This function returns a bitmap representing the set of values of the underlying column.
 *
 * The bitmap is simply a compressed representation of the set of all integral values that
 * appear in the column being aggregated over.
 *
 * @param child child expression that can produce a column value with `child.eval(inputRow)`
 */
case class BitmapAggregator(
    child: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int,
    // Take the format as string instead of [[RoaringBitmapArrayFormat.Value]],
    // because String is safe to serialize.
    serializationFormatString: String)
  extends TypedImperativeAggregate[RoaringBitmapArray] with ImplicitCastInputTypes
  with UnaryLike[Expression] {

  def this(child: Expression, serializationFormat: RoaringBitmapArrayFormat.Value) =
    this(child, 0, 0, serializationFormat.toString)

  override def createAggregationBuffer(): RoaringBitmapArray = new RoaringBitmapArray()

  override def update(buffer: RoaringBitmapArray, input: InternalRow): RoaringBitmapArray = {
    val value = child.eval(input)
    // Ignore empty rows
    if (value != null) {
      buffer.add(value.asInstanceOf[Long])
    }
    buffer
  }

  override def merge(buffer: RoaringBitmapArray, input: RoaringBitmapArray): RoaringBitmapArray = {
    buffer.merge(input)
    buffer
  }

  /**
   * Return bitmap cardinality, last and serialized bitmap.
   */
  override def eval(bitmapIntegerSet: RoaringBitmapArray): GenericInternalRow = {
    // reduce the serialized size via RLE optimisation
    bitmapIntegerSet.runOptimize()
    new GenericInternalRow(Array(
      bitmapIntegerSet.cardinality,
      bitmapIntegerSet.last.getOrElse(null),
      serialize(bitmapIntegerSet)))
  }

  override def serialize(buffer: RoaringBitmapArray): Array[Byte] = {
    val serializationFormat = RoaringBitmapArrayFormat.withName(serializationFormatString)
    buffer.serializeAsByteArray(serializationFormat)
  }

  override def deserialize(storageFormat: Array[Byte]): RoaringBitmapArray = {
    RoaringBitmapArray.readFrom(storageFormat)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int)
    : ImperativeAggregate = copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int)
    : ImperativeAggregate = copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = false

  override def dataType: StructType = StructType(
    Seq(
      StructField("cardinality", LongType),
      StructField("last", LongType),
      StructField("bitmap", BinaryType)
    )
  )

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType)

  override protected def withNewChildInternal(newChild: Expression): BitmapAggregator =
    copy(child = newChild)
}
