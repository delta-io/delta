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
package io.delta.kernel.defaults.utils

import scala.collection.JavaConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.types._

/**
 * Corresponding Scala class for each Kernel data type:
 * - BooleanType --> boolean
 * - ByteType --> byte
 * - ShortType --> short
 * - IntegerType --> int
 * - LongType --> long
 * - FloatType --> float
 * - DoubleType --> double
 * - StringType --> String
 * - DateType --> int (number of days since the epoch)
 * - TimestampType --> long (number of microseconds since the unix epoch)
 * - DecimalType --> java.math.BigDecimal
 * - BinaryType --> Array[Byte]
 *
 * TODO: complex types
 * - StructType?
 * - ArrayType?
 * - MapType?
 */
class TestRow(val values: Array[Any]) {
  // TODO: we could make this extend Row and create a way to generate Seq(Any) from Rows but it
  //  would complicate a lot of the code for not much benefit

  def length: Int = values.length

  def get(i: Int): Any = values(i)

  def toSeq: Seq[Any] = values.clone()

  def mkString(start: String, sep: String, end: String): String = {
    val n = length
    val builder = new StringBuilder
    builder.append(start)
    if (n > 0) {
      builder.append(get(0))
      var i = 1
      while (i < n) {
        builder.append(sep)
        builder.append(get(i))
        i += 1
      }
    }
    builder.append(end)
    builder.toString()
  }

  override def toString: String = this.mkString("[", ",", "]")
}

object TestRow {

  /**
   * Construct a [[TestRow]] with the given values. See the docs for [[TestRow]] for
   * the scala type corresponding to each Kernel data type.
   */
  def apply(values: Any*): TestRow = {
    new TestRow(values.toArray)
  }

  /**
   * Construct a [[TestRow]] with the same values as a Kernel [[Row]].
   */
  def apply(row: Row): TestRow = {
    TestRow.fromSeq(row.getSchema.fields().asScala.zipWithIndex.map { case (field, i) =>
      field.getDataType match {
        case _ if row.isNullAt(i) => null
        case _: BooleanType => row.getBoolean(i)
        case _: ByteType => row.getByte(i)
        case _: IntegerType => row.getInt(i)
        case _: LongType => row.getLong(i)
        case _: ShortType => row.getShort(i)
        case _: DateType => row.getInt(i)
        case _: TimestampType => row.getLong(i)
        case _: FloatType => row.getFloat(i)
        case _: DoubleType => row.getDouble(i)
        case _: StringType => row.getString(i)
        case _: BinaryType => row.getBinary(i)
        case _: DecimalType => row.getDecimal(i)

        // TODO complex types
        // case _: StructType => row.getStruct(i)
        // case _: MapType => row.getMap(i)
        // case _: ArrayType => row.getArray(i)
        case _ => throw new UnsupportedOperationException("unrecognized data type")
      }
    })
  }

  /**
   * Construct a [[TestRow]] from the given seq of values. See the docs for [[TestRow]] for
   * the scala type corresponding to each Kernel data type.
   */
  def fromSeq(values: Seq[Any]): TestRow = {
    new TestRow(values.toArray)
  }

  /**
   * Construct a [[TestRow]] with the elements of the given tuple. See the docs for
   * [[TestRow]] for the scala type corresponding to each Kernel data type.
   */
  def fromTuple(tuple: Product): TestRow = fromSeq(tuple.productIterator.toSeq)
}
