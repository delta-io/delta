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
package io.delta.kernel.defaults.internal.json

import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.test.VectorTestUtils
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite

import scala.Double.NegativeInfinity
import scala.collection.JavaConverters._

class JsonUtilsSuite extends AnyFunSuite with TestUtils with VectorTestUtils {

  // Tests for round trip of each data type
  Seq(
    (
      BooleanType.BOOLEAN,
      s"""{"c0":false,"c1":true,"c2":null,"c3":false}""", // test JSON
      TestRow(false, true, null, false), // expected decoded row
      // expected row serialized as JSON, null values won't be in output
      s"""{"c0":false,"c1":true,"c3":false}"""
    ),
    (
      ByteType.BYTE,
      s"""{"c0":${Byte.MinValue},"c1":${Byte.MaxValue},"c2":null,"c3":4}""",
      TestRow(Byte.MinValue, Byte.MaxValue, null, 4.toByte),
      s"""{"c0":${Byte.MinValue},"c1":${Byte.MaxValue},"c3":4}"""
    ),
    (
      ShortType.SHORT,
      s"""{"c0":${Short.MinValue},"c1":${Short.MaxValue},"c2":null,"c3":44}""",
      TestRow(Short.MinValue, Short.MaxValue, null, 44.toShort),
      s"""{"c0":${Short.MinValue},"c1":${Short.MaxValue},"c3":44}"""
    ),
    (
      IntegerType.INTEGER,
      s"""{"c0":${Integer.MIN_VALUE},"c1":${Integer.MAX_VALUE},"c2":null,"c3":423423}""",
      TestRow(Integer.MIN_VALUE, Integer.MAX_VALUE, null, 423423),
      s"""{"c0":${Integer.MIN_VALUE},"c1":${Integer.MAX_VALUE},"c3":423423}"""
    ),
    (
      LongType.LONG,
      s"""{"c0":${Long.MinValue},"c1":${Long.MaxValue},"c2":null,"c3":423423}""",
      TestRow(Long.MinValue, Long.MaxValue, null, 423423.toLong),
      s"""{"c0":${Long.MinValue},"c1":${Long.MaxValue},"c3":423423}"""
    ),
    (
      FloatType.FLOAT,
      s"""{"c0":${Float.MinValue},"c1":${Float.MaxValue},"c2":null,"c3":"${Float.NaN}"}""",
      TestRow(Float.MinValue, Float.MaxValue, null, Float.NaN),
      s"""{"c0":${Float.MinValue},"c1":${Float.MaxValue},"c3":"NaN"}"""
    ),
    (
      DoubleType.DOUBLE,
      s"""{"c0":${Double.MinValue},"c1":${Double.MaxValue},"c2":null,"c3":"${NegativeInfinity}"}""",
      TestRow(Double.MinValue, Double.MaxValue, null, NegativeInfinity),
      s"""{"c0":${Double.MinValue},"c1":${Double.MaxValue},"c3":"-Infinity"}"""
    ),
    (
      StringType.STRING,
      s"""{"c0":"","c1":"ssdfsdf","c2":null,"c3":"123sdsd"}""",
      TestRow("", "ssdfsdf", null, "123sdsd"),
      s"""{"c0":"","c1":"ssdfsdf","c3":"123sdsd"}"""
    ),
    (
      new ArrayType(IntegerType.INTEGER, true /* containsNull */),
      """{"c0":[23,23],"c1":[1212,null,2332],"c2":null,"c3":[]}""",
      TestRow(Seq(23, 23), Seq(1212, null, 2332), null, Seq()),
      """{"c0":[23,23],"c1":[1212,null,2332],"c3":[]}"""
    ),
    (
      // array with complex element types
      new ArrayType(
        new StructType()
          .add("cn0", IntegerType.INTEGER)
          .add("cn1",
            new ArrayType(LongType.LONG, true /* containsNull */)),
        true /* containsNull */),
      """{
        |"c0":[{"cn0":24,"cn1":[23,232]},{"cn0":25,"cn1":[24,237]}],
        |"c1":[{"cn0":32,"cn1":[37,null,2323]},{"cn0":29,"cn1":[200,111237]}],
        |"c2":null,
        |"c3":[]}""".stripMargin,
      TestRow(
        Seq(TestRow(24, Seq(23L, 232L)), TestRow(25, Seq(24L, 237L))),
        Seq(TestRow(32, Seq(37L, null, 2323L)), TestRow(29, Seq(200L, 111237L))),
        null,
        Seq()
      ),
      """{
        |"c0":[{"cn0":24,"cn1":[23,232]},{"cn0":25,"cn1":[24,237]}],
        |"c1":[{"cn0":32,"cn1":[37,null,2323]},{"cn0":29,"cn1":[200,111237]}],
        |"c3":[]}""".stripMargin
    ),
    (
      new MapType(StringType.STRING, IntegerType.INTEGER, true /* valueContainsNull */),
      """{
        |"c0":{"24":200,"25":201},
        |"c1":{"27":null,"25":203},
        |"c2":null,
        |"c3":{}
        |}""".stripMargin,
      TestRow(
        Map("24" -> 200, "25" -> 201),
        Map("27" -> null, "25" -> 203),
        null,
        Map()
      ),
      """{
        |"c0":{"24":200,"25":201},
        |"c1":{"25":203},
        |"c3":{}
        |}""".stripMargin
    ),
    (
      new StructType()
        .add("cn0", IntegerType.INTEGER)
        .add("cn1",
          new ArrayType(LongType.LONG, true /* containsNull */)),
      """{
        |"c0":{"cn0":24,"cn1":[23,232]},
        |"c1":{"cn0":29,"cn1":[200,null,111237]},
        |"c2":null,
        |"c3":{}
        |}""".stripMargin,
      TestRow(
        TestRow(24, Seq(23L, 232L)),
        TestRow(29, Seq(200L, null, 111237L)),
        null,
        TestRow(null, null)
      ),
      """{
        |"c0":{"cn0":24,"cn1":[23,232]},
        |"c1":{"cn0":29,"cn1":[200,null,111237]},
        |"c3":{}
        |}""".stripMargin
    )
  ).foreach { case (dataType, testJson, expRow, expJson) =>
    test(s"JsonUtils.RowSerializer: $dataType") {
      val schema = new StructType(Seq.range(0, 4).map(colOrdinal =>
        new StructField(s"c$colOrdinal", dataType, true)).asJava)

      val actRow = JsonUtils.rowFromJson(testJson, schema)
      checkAnswer(Seq(actRow), Seq(expRow))
      assert(JsonUtils.rowToJson(actRow) === expJson.linesIterator.mkString)
    }
  }
}
