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

package org.apache.spark.sql.delta.expressions

import java.util

import scala.collection.mutable

import org.apache.spark.sql.delta.expressions.HilbertUtils._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{AbstractDataType, DataType, DataTypes}

/**
 * Represents a hilbert index built from the provided columns.
 * The columns are expected to all be Ints and to have at most numBits individually.
 * The points along the hilbert curve are represented by Longs.
 */
private[sql] case class HilbertLongIndex(numBits: Int, children: Seq[Expression])
    extends Expression with ExpectsInputTypes with CodegenFallback {

  private val n: Int = children.size
  private val nullValue: Int = 0

  override def nullable: Boolean = false

  // pre-initialize working set array
  private val ints = new Array[Int](n)

  override def eval(input: InternalRow): Any = {
    var i = 0
    while (i < n) {
      ints(i) = children(i).eval(input) match {
        case null => nullValue
        case int: Integer => int
        case any => throw new IllegalArgumentException(
          s"${this.getClass.getSimpleName} expects only inputs of type Int, but got: " +
            s"$any of type${any.getClass.getSimpleName}")
      }
      i += 1
    }

    HilbertStates.getStateList(n).translateNPointToDKey(ints, numBits)
  }

  override def dataType: DataType = DataTypes.LongType

  override def inputTypes: Seq[AbstractDataType] = Seq.fill(n)(DataTypes.IntegerType)

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): HilbertLongIndex = copy(children = newChildren)
}

/**
 * Represents a hilbert index built from the provided columns.
 * The columns are expected to all be Ints and to have at most numBits.
 * The points along the hilbert curve are represented by Byte arrays.
 */
private[sql] case class HilbertByteArrayIndex(numBits: Int, children: Seq[Expression])
    extends Expression with ExpectsInputTypes with CodegenFallback {

  private val n: Int = children.size
  private val nullValue: Int = 0

  override def nullable: Boolean = false

  // pre-initialize working set array
  private val ints = new Array[Int](n)

  override def eval(input: InternalRow): Any = {
    var i = 0
    while (i < n) {
      ints(i) = children(i).eval(input) match {
        case null => nullValue
        case int: Integer => int
        case any => throw new IllegalArgumentException(
          s"${this.getClass.getSimpleName} expects only inputs of type Int, but got: " +
            s"$any of type${any.getClass.getSimpleName}")
      }
      i += 1
    }

    HilbertStates.getStateList(n).translateNPointToDKeyArray(ints, numBits)
  }

  override def dataType: DataType = DataTypes.BinaryType

  override def inputTypes: Seq[AbstractDataType] = Seq.fill(n)(DataTypes.IntegerType)

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): HilbertByteArrayIndex = copy(children = newChildren)
}

// scalastyle:off line.size.limit
/**
 * The following code is based on this paper:
 *   https://citeseerx.ist.psu.edu/document?repid=rep1&type=pdf&doi=bfd6d94c98627756989b0147a68b7ab1f881a0d6
 * with optimizations around matrix manipulation taken from this one:
 *   https://pdfs.semanticscholar.org/4043/1c5c43a2121e1bc071fc035e90b8f4bb7164.pdf
 *
 * At a high level you construct a GeneratorTable with the getStateGenerator method.
 * That represents the information necessary to construct a state list for a given number
 * of dimension, N.
 * Once you have the generator table for your dimension you can construct a state list.
 * You can then turn those state lists into compact state lists that store all the information
 * in one large array of longs.
 */
// scalastyle:on line.size.limit
object HilbertIndex {

  private type CompactStateList = HilbertCompactStateList

  val SIZE_OF_INT = 32

  /**
   * Construct the generator table for a space of dimension n.
   * This table consists of 2^n rows, each row containing Y, X1, X2, dY and TY.
   *   Y    The index in the array representing the table. (0 to (2^n - 1))
   *   X1   A coordinate representing points on the curve expressed as an n-point.
   *        These are arranged such that if two rows differ by 1 in Y then the binary
   *        representation of their X1 values differ by exactly one bit.
   *        These are the "Gray-codes" of their Y value.
   *   X2   A pair of n-points corresponding to the first and last points on the first
   *        order curve to which X1 transforms in the construction of a second order curve.
   *   dY   Represents the magnitude of difference between X2 values in this row.
   *   TY   A transformation matrix that transforms X2(1) to the X1 value where Y is zero and
   *        transforms X2(2) to the X1 value where Y is (2^n - 1)
   */
  def getStateGenerator(n: Int): GeneratorTable = {
    val x2s = getX2GrayCodes(n)

    val len = 1 << n
    val rows = (0 until len).map { i =>
      val x21 = x2s(i << 1)
      val x22 = x2s((i << 1) + 1)
      val dy = x21 ^ x22

      Row(
        y = i,
        x1 = i ^ (i >>> 1),
        x2 = (x21, x22),
        dy = dy,
        m = HilbertMatrix(n, x21, getSetColumn(n, dy))
      )
    }

    new GeneratorTable(n, rows)
  }

  // scalastyle:off line.size.limit
  /**
   * This will construct an x2-gray-codes sequence of order n as described in
   *  https://citeseerx.ist.psu.edu/document?repid=rep1&type=pdf&doi=bfd6d94c98627756989b0147a68b7ab1f881a0d6
   *
   *   Each pair of values corresponds to the first and last coordinates of points on a first
   *   order curve to which a point taken from column X1 transforms to at the second order.
   */
  // scalastyle:on line.size.limit
  private[this] def getX2GrayCodes(n: Int) : Array[Int] = {
    if (n == 1) {
      // hard code the base case
      return Array(0, 1, 0, 1)
    }
    val mask = 1 << (n - 1)
    val base = getX2GrayCodes(n - 1)
    base(base.length - 1) = base(base.length - 2) + mask
    val result = Array.fill(base.length * 2)(0)
    base.indices.foreach { i =>
      result(i) = base(i)
      result(result.length - 1 - i) = base(i) ^ mask
    }
    result
  }

  private[this] case class Row(y: Int, x1: Int, x2: (Int, Int), dy: Int, m: HilbertMatrix)

  private[this] case class PointState(y: Int, var x1: Int = 0, var state: Int = 0)

  private[this] case class State(id: Int, matrix: HilbertMatrix, var pointStates: Seq[PointState])

  private[sql] class StateList(n: Int, states: Map[Int, State]) {
    def getNPointToDKeyStateMap: CompactStateList = {
      val numNPoints = 1 << n
      val array = new Array[Long](numNPoints * states.size)

      states.foreach { case (stateIdx, state) =>
        val stateStartIdx = stateIdx * numNPoints

        state.pointStates.foreach { ps =>
          val psLong = (ps.y.toLong << SIZE_OF_INT) | ps.state.toLong
          array(stateStartIdx + ps.x1) = psLong
        }
      }
      new CompactStateList(n, array)
    }
    def getDKeyToNPointStateMap: CompactStateList = {
      val numNPoints = 1 << n
      val array = new Array[Long](numNPoints * states.size)

      states.foreach { case (stateIdx, state) =>
        val stateStartIdx = stateIdx * numNPoints

        state.pointStates.foreach { ps =>
          val psLong = (ps.x1.toLong << SIZE_OF_INT) | ps.state.toLong
          array(stateStartIdx + ps.y) = psLong
        }
      }
      new CompactStateList(n, array)
    }
  }

  private[sql] class GeneratorTable(n: Int, rows: Seq[Row]) {
    def generateStateList(): StateList = {
      val result = mutable.Map[Int, State]()
      val list = new util.LinkedList[State]()

      var nextStateNum = 1

      val initialState = State(0, HilbertMatrix.identity(n), rows.map(r => PointState(r.y, r.x1)))
      result.put(0, initialState)

      rows.foreach { row =>
        val matrix = row.m
        result.find { case (_, s) => s.matrix == matrix } match {
          case Some((_, s)) =>
            initialState.pointStates(row.y).state = s.id
          case _ =>
            initialState.pointStates(row.y).state = nextStateNum
            val newState = State(nextStateNum, matrix, Seq())
            result.put(nextStateNum, newState)
            list.addLast(newState)
            nextStateNum += 1
        }
      }

      while (!list.isEmpty) {
        val currentState = list.removeFirst()
        currentState.pointStates = rows.indices.map(r => PointState(r))

        rows.indices.foreach { i =>
          val j = currentState.matrix.transform(i)
          val p = initialState.pointStates.find(_.x1 == j).get
          val currentPointState = currentState.pointStates(p.y)
          currentPointState.x1 = i
          val tm = result(p.state).matrix.multiply(currentState.matrix)

          result.find { case (_, s) => s.matrix == tm } match {
            case Some((_, s)) =>
              currentPointState.state = s.id
            case _ =>
              currentPointState.state = nextStateNum
              val newState = State(nextStateNum, tm, Seq())
              result.put(nextStateNum, newState)
              list.addLast(newState)
              nextStateNum += 1
          }
        }
      }

      new StateList(n, result.toMap)
    }
  }
}

/**
 * Represents a compact state map. This is used in the mapping between n-points and d-keys.
 * [[array]] is treated as a Map(Int -> Map(Int -> (Int, Int)))
 *
 * Each values in the array will be a combination of two things, a point and the index of the
 * next state, in the most- and least- significant bits, respectively.
 *   state -> coord -> [point + nextState]
 */
private[sql] class HilbertCompactStateList(n: Int, array: Array[Long]) {
    private val maxNumN = 1 << n
    private val mask = maxNumN - 1
    private val intMask = (1L << HilbertIndex.SIZE_OF_INT) - 1

    // point and nextState
    @inline def transform(nPoint: Int, state: Int): (Int, Int) = {
      val value = array(state * maxNumN + nPoint)
      (
        (value >>> HilbertIndex.SIZE_OF_INT).toInt,
        (value & intMask).toInt
      )
    }

    // These while loops are to minimize overhead.
    // This method exists only for testing
    private[expressions] def translateDKeyToNPoint(key: Long, k: Int): Array[Int] = {
      val result = new Array[Int](n)
      var currentState = 0
      var i = 0
      while (i < k) {
        val h = (key >> ((k - 1 - i) * n)) & mask

        val (z, nextState) = transform(h.toInt, currentState)

        var j = 0
        while (j < n) {
          val v = (z >> (n - 1 - j)) & 1
          result(j) = (result(j) << 1) | v
          j += 1
        }

        currentState = nextState
        i += 1
      }
      result
    }

    // These while loops are to minimize overhead.
    // This method exists only for testing
    private[expressions] def translateDKeyArrayToNPoint(key: Array[Byte], k: Int): Array[Int] = {
      val result = new Array[Int](n)
      val initialOffset = (key.length * 8) - (k * n)
      var currentState = 0
      var i = 0
      while (i < k) {
        val offset = initialOffset + (i * n)
        val h = getBits(key, offset, n)

        val (z, nextState) = transform(h, currentState)

        var j = 0
        while (j < n) {
          val v = (z >> (n - 1 - j)) & 1
          result(j) = (result(j) << 1) | v
          j += 1
        }

        currentState = nextState
        i += 1
      }
      result
    }

    /**
     * Translate an n-dimensional point into it's corresponding position on the n-dimensional
     * hilbert curve.
     * @param point An n-dimensional point. (assumed to have n elements)
     * @param k     The number of meaningful bits in each value of the point.
     */
    def translateNPointToDKey(point: Array[Int], k: Int): Long = {
      var result = 0L
      var currentState = 0
      var i = 0
      while (i < k) {
        var z = 0
        var j = 0
        while (j < n) {
          z = (z << 1) | ((point(j) >> (k - 1 - i)) & 1)
          j += 1
        }
        val (h, nextState) = transform(z, currentState)
        result = (result << n) | h
        currentState = nextState
        i += 1
      }
      result
    }

    /**
     * Translate an n-dimensional point into it's corresponding position on the n-dimensional
     * hilbert curve. Returns the resulting integer as an array of bytes.
     * @param point An n-dimensional point. (assumed to have n elements)
     * @param k     The number of meaningful bits in each value of the point.
     */
    def translateNPointToDKeyArray(point: Array[Int], k: Int): Array[Byte] = {
      val numBits = k * n
      val numBytes = (numBits + 7) / 8
      val result = new Array[Byte](numBytes)
      val initialOffset = (numBytes * 8) - numBits
      var currentState = 0
      var i = 0
      while (i < k) {
        var z = 0
        var j = 0
        while (j < n) {
          z = (z << 1) | ((point(j) >> (k - 1 - i)) & 1)
          j += 1
        }
        val (h, nextState) = transform(z, currentState)
        setBits(result, initialOffset + (i * n), h, n)
        currentState = nextState
        i += 1
      }
      result
    }
}
