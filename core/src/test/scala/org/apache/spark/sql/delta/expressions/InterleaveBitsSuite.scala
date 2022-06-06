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

import java.nio.ByteBuffer

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckSuccess
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionEvalHelper, Literal}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.types.IntegerType


class InterleaveBitsSuite extends SparkFunSuite with ExpressionEvalHelper {

  def intToBinary(x: Int): Array[Byte] = {
    ByteBuffer.allocate(4).putInt(x).array()
  }

  def checkInterleaving(input: Seq[Expression], expectedOutput: Any): Unit = {
    Seq("true", "false").foreach { flag =>
      withSQLConf(DeltaSQLConf.FAST_INTERLEAVE_BITS_ENABLED.key -> flag) {
        checkEvaluation(InterleaveBits(input), expectedOutput)
      }
    }
  }

  test("0 inputs") {
    checkInterleaving(Seq.empty[Expression], Array.empty[Byte])
  }

  test("1 input") {
    for { i <- 1.to(10) } {
      val r = Random.nextInt()
      checkInterleaving(Seq(Literal(r)), intToBinary(r))
    }
  }

  test("2 inputs") {
    checkInterleaving(
      input = Seq(
        0x000ff0ff,
        0xfff00f00
      ).map(Literal(_)),
      expectedOutput =
        Array(0x55, 0x55, 0x55, 0xaa, 0xaa, 0x55, 0xaa, 0xaa)
          .map(_.toByte))
  }

  test("3 inputs") {
    checkInterleaving(
      input = Seq(
        0xff00,
        0x00ff,
        0x0000
      ).map(Literal(_)),
      expectedOutput =
        Array(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x92, 0x49, 0x24, 0x49, 0x24, 0x92)
          .map(_.toByte))
  }

  test("nulls") {
    val ones = 0xffffffff
    checkInterleaving(
      Seq(Literal(ones), Literal.create(null, IntegerType)), Array.fill(8)(0xaa.toByte))
    checkInterleaving(
      Seq(Literal.create(null, IntegerType), Literal(ones)), Array.fill(8)(0x55.toByte))

    for { i <- 0.to(6) } {
      checkInterleaving(
        Seq.fill(i)(Literal.create(null, IntegerType)), Array.fill(i * 4)(0x00.toByte))
    }
  }

  test("consistency") {
    for { num_inputs <- 1 to 10 } {
      checkConsistencyBetweenInterpretedAndCodegen(InterleaveBits(_), IntegerType, num_inputs)
    }
  }

  test("supported types") {
    // only int for now
    InterleaveBits(Seq(Literal(0))).checkInputDataTypes() == TypeCheckSuccess
    // nothing else
    InterleaveBits(Seq(Literal(false))).checkInputDataTypes() != TypeCheckSuccess
    InterleaveBits(Seq(Literal(0.toLong))).checkInputDataTypes() != TypeCheckSuccess
    InterleaveBits(Seq(Literal(0.toDouble))).checkInputDataTypes() != TypeCheckSuccess
    InterleaveBits(Seq(Literal(0.toString))).checkInputDataTypes() != TypeCheckSuccess
  }

  test("randomization interleave bits") {
    val numIters = sys.env
      .get("NUMBER_OF_ITERATIONS_TO_INTERLEAVE_BITS")
      .map(_.toInt)
      .getOrElse(100000000)
    var i = 0
    while (i < numIters) {
      // generate n columns where 1 <= n <= 8
      val numCols = Random.nextInt(8) + 1
      val input = new Array[Int](numCols)
      var j = 0
      while (j < numCols) {
        input(j) = Random.nextInt()
        j += 1
      }
      val r1 = InterleaveBits.interleaveBits(input, true)
      val r2 = InterleaveBits.interleaveBits(input, false)
      assert(java.util.Arrays.equals(r1, r2), s"input: ${input.mkString(",")}")
      i += 1
    }
  }
}
