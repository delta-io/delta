/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.data

import scala.collection.JavaConverters._

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue, Row}
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class StructRowSuite extends AnyFunSuite with VectorTestUtils {

  private val leafSchema = new StructType()
    .add("id", LongType.LONG, false)
    .add("name", StringType.STRING, true)
    .add("config", new MapType(StringType.STRING, StringType.STRING, false), true)

  private val nestedSchema = new StructType()
    .add("count", LongType.LONG, false)
    .add("items", new ArrayType(leafSchema, false), true)

  private def leafRow(id: Long, name: String, config: Map[String, String]): Row = {
    val values = new java.util.HashMap[Integer, Object]()
    values.put(0, java.lang.Long.valueOf(id))
    if (name != null) values.put(1, name)
    if (config != null) values.put(2, VectorUtils.stringStringMapValue(config.asJava))
    new GenericRow(leafSchema, values)
  }

  /**
   * A single-row struct column vector over `row`, whose backing `row` reference can be swapped.
   */
  private def singleStructVector(schemaType: StructType, rowHolder: () => Row): ColumnVector =
    new ColumnVector {
      override def getDataType: DataType = schemaType
      override def getSize: Int = 1
      override def close(): Unit = {}
      override def isNullAt(rowId: Int): Boolean = rowHolder() == null
      override def getChild(ordinal: Int): ColumnVector =
        childVector(schemaType.at(ordinal).getDataType, rowHolder, ordinal)
    }

  private def childVector(dt: DataType, rowHolder: () => Row, ordinal: Int): ColumnVector =
    new ColumnVector {
      override def getDataType: DataType = dt
      override def getSize: Int = 1
      override def close(): Unit = {}
      override def isNullAt(rowId: Int): Boolean = rowHolder().isNullAt(ordinal)
      override def getLong(rowId: Int): Long = rowHolder().getLong(ordinal)
      override def getString(rowId: Int): String = rowHolder().getString(ordinal)
      override def getMap(rowId: Int): MapValue = rowHolder().getMap(ordinal)
      override def getArray(rowId: Int): ArrayValue = rowHolder().getArray(ordinal)
      override def getChild(o: Int): ColumnVector = {
        val childType = dt.asInstanceOf[StructType].at(o).getDataType
        childVector(childType, () => rowHolder().getStruct(ordinal), o)
      }
    }

  test("deepCopy copies scalars, maps, and nested array-of-struct") {
    val leaf = leafRow(7L, "n", Map("k" -> "v"))
    val items = VectorUtils.buildArrayValue(java.util.Arrays.asList(leaf), leafSchema)
    val nestedValues = new java.util.HashMap[Integer, Object]()
    nestedValues.put(0, java.lang.Long.valueOf(3L))
    nestedValues.put(1, items)
    val src: Row = new GenericRow(nestedSchema, nestedValues)

    val copy = StructRow.deepCopy(src)

    assert(copy.getLong(0) == 3L)
    val copiedItems = copy.getArray(1)
    assert(copiedItems.getSize == 1)
    val copiedLeaf = copiedItems.getElements.getChild(0)
    assert(copiedLeaf.getLong(0) == 7L)
    val copiedName = copiedItems.getElements.getChild(1)
    assert(copiedName.getString(0) == "n")
    val copiedConfig = copiedItems.getElements.getChild(2)
    assert(VectorUtils.toJavaMap(copiedConfig.getMap(0)).asScala == Map("k" -> "v"))
  }

  test("deepCopy is detached: mutating the source afterwards does not affect the copy") {
    var backing: Row = leafRow(1L, "original", Map("a" -> "1"))
    val vector = singleStructVector(leafSchema, () => backing)
    val view = StructRow.fromStructVector(vector, 0)

    val copy = StructRow.deepCopy(view)

    // Simulate the batch being replaced/freed: point the holder at different data.
    backing = leafRow(999L, "mutated", Map("a" -> "changed"))

    assert(copy.getLong(0) == 1L)
    assert(copy.getString(1) == "original")
    assert(VectorUtils.toJavaMap(copy.getMap(2)).asScala == Map("a" -> "1"))
  }

  test("deepCopy preserves null fields") {
    val leaf = leafRow(5L, null, null)
    val copy = StructRow.deepCopy(leaf)
    assert(copy.getLong(0) == 5L)
    assert(copy.isNullAt(1))
    assert(copy.isNullAt(2))
  }

  test("deepCopy preserves array element order and nulls") {
    // items = [leaf(1), null, leaf(2)]
    val elems: java.util.List[Row] =
      java.util.Arrays.asList(leafRow(1L, "a", Map.empty), null, leafRow(2L, "b", Map.empty))
    val items = VectorUtils.buildArrayValue(elems, leafSchema)
    val values = new java.util.HashMap[Integer, Object]()
    values.put(0, java.lang.Long.valueOf(0L))
    values.put(1, items)
    val copy = StructRow.deepCopy(new GenericRow(nestedSchema, values))

    val copied = copy.getArray(1)
    assert(copied.getSize == 3)
    val ids = copied.getElements.getChild(0)
    assert(ids.getLong(0) == 1L)
    assert(ids.isNullAt(1), "the null element must remain null")
    assert(ids.getLong(2) == 2L)
  }

  test("deepCopy handles all top-level scalar types (symmetry with array-element path)") {
    // Covers the scalar types beyond long/int/string/boolean so a top-level field copies the same
    // way it would inside an array (which routes through VectorUtils.getValueAsObject).
    val schema = new StructType()
      .add("b", ByteType.BYTE, false)
      .add("sh", ShortType.SHORT, false)
      .add("f", FloatType.FLOAT, false)
      .add("d", DoubleType.DOUBLE, false)
      .add("dec", new DecimalType(10, 2), false)
      .add("bin", BinaryType.BINARY, false)
    val values = new java.util.HashMap[Integer, Object]()
    values.put(0, java.lang.Byte.valueOf(1.toByte))
    values.put(1, java.lang.Short.valueOf(2.toShort))
    values.put(2, java.lang.Float.valueOf(3.5f))
    values.put(3, java.lang.Double.valueOf(4.5d))
    values.put(4, new java.math.BigDecimal("12.34"))
    values.put(5, Array[Byte](7, 8, 9))

    val copy = StructRow.deepCopy(new GenericRow(schema, values))

    assert(copy.getByte(0) == 1.toByte)
    assert(copy.getShort(1) == 2.toShort)
    assert(copy.getFloat(2) == 3.5f)
    assert(copy.getDouble(3) == 4.5d)
    assert(copy.getDecimal(4) == new java.math.BigDecimal("12.34"))
    assert(copy.getBinary(5).sameElements(Array[Byte](7, 8, 9)))
  }
}
