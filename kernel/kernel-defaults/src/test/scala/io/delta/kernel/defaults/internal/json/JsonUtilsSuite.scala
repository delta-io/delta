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

import io.delta.kernel.defaults.utils.{TestRow, TestUtils, VectorTestUtils}
import io.delta.kernel.internal.util.InternalUtils._
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite

import java.sql._
import java.time.temporal.ChronoUnit
import java.time.{Instant, OffsetDateTime}
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
      new DecimalType(10, 3),
      s"""{"c0":-2342342323.23,"c1":23423223424.23,"c2":null,"c3":423423.0}""",
      TestRow(BigDecimal("-2342342323.23"), BigDecimal("23423223424.23"),
        null, BigDecimal("423423.0")),
      s"""{"c0":-2342342323.23,"c1":23423223424.23,"c3":423423}"""
    ),
    (
      StringType.STRING,
      s"""{"c0":"","c1":"ssdfsdf","c2":null,"c3":"123sdsd"}""",
      TestRow("", "ssdfsdf", null, "123sdsd"),
      s"""{"c0":"","c1":"ssdfsdf","c3":"123sdsd"}"""
    ),
    (
      DateType.DATE,
      s"""{"c0":"1902-01-01","c1":"2500-12-31","c2":null,"c3":"2020-06-15"}""",
      TestRow(epochDays("1902-01-01"), epochDays("2500-12-31"), null, epochDays("2020-06-15")),
      s"""{"c0":"1902-01-01","c1":"2500-12-31","c3":"2020-06-15"}"""
    ),
    (
      TimestampType.TIMESTAMP,
      s"""
         |{"c0":"1902-01-01T00:00:00.000001-08:00",
         |"c1":"2200-12-31T23:59:59.999999Z",
         |"c2":null,
         |"c3":"2020-06-15T12:00:00Z"}""".stripMargin,
      TestRow(
        epochMicros("1902-01-01T00:00:00.000001-08:00"),
        epochMicros("2200-12-31T23:59:59.999999Z"),
        null,
        epochMicros("2020-06-15T12:00:00Z")
      ),
      s"""
         |{"c0":"1902-01-01T08:00:00.000001Z",
         |"c1":"2200-12-31T23:59:59.999999Z",
         |"c3":"2020-06-15T12:00:00Z"}""".stripMargin
    ),
    (
      TimestampNTZType.TIMESTAMP_NTZ,
      s"""
         |{"c0":"1902-01-01T00:00:00.000001-08:00",
         |"c1":"2200-12-31T23:59:59.999999Z",
         |"c2":null,
         |"c3":"2020-06-15T12:00:00Z"}""".stripMargin,
      TestRow(
        epochMicros("1902-01-01T00:00:00.000001-08:00"),
        epochMicros("2200-12-31T23:59:59.999999Z"),
        null,
        epochMicros("2020-06-15T12:00:00Z")
      ),
      s"""
         |{"c0":"1902-01-01T08:00:00.000001Z",
         |"c1":"2200-12-31T23:59:59.999999Z",
         |"c3":"2020-06-15T12:00:00Z"}""".stripMargin
    ),
    (
      new ArrayType(IntegerType.INTEGER, true /* containsNull */),
      """{"c0":[23,23],"c1":[1212,2323,2332],"c2":null,"c3":[]}""",
      TestRow(Seq(23, 23), Seq(1212, 2323, 2332), null, Seq()),
      """{"c0":[23,23],"c1":[1212,2323,2332],"c3":[]}"""
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
        |"c1":[{"cn0":32,"cn1":[37,2323]},{"cn0":29,"cn1":[200,111237]}],
        |"c2":null,
        |"c3":[]}""".stripMargin,
      TestRow(
        Seq(TestRow(24, Seq(23L, 232L)), TestRow(25, Seq(24L, 237L))),
        Seq(TestRow(32, Seq(37L, 2323L)), TestRow(29, Seq(200L, 111237L))),
        null,
        Seq()
      ),
      """{
        |"c0":[{"cn0":24,"cn1":[23,232]},{"cn0":25,"cn1":[24,237]}],
        |"c1":[{"cn0":32,"cn1":[37,2323]},{"cn0":29,"cn1":[200,111237]}],
        |"c3":[]}""".stripMargin
    ),
    (
      new MapType(StringType.STRING, DateType.DATE, true /* valueContainsNull */),
      """{
        |"c0":{"24":"2020-01-01","25":"2022-01-01"},
        |"c1":{"27":null,"25":"2022-01-01"},
        |"c2":null,
        |"c3":{}
        |}""".stripMargin,
      TestRow(
        Map("24" -> epochDays("2020-01-01"), "25" -> epochDays("2022-01-01")),
        Map("27" -> null, "25" -> epochDays("2022-01-01")),
        null,
        Map()
      ),
      """{
        |"c0":{"24":"2020-01-01","25":"2022-01-01"},
        |"c1":{"25":"2022-01-01"},
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
        |"c1":{"cn0":29,"cn1":[200,111237]},
        |"c2":null,
        |"c3":{}
        |}""".stripMargin,
      TestRow(
        TestRow(24, Seq(23L, 232L)),
        TestRow(29, Seq(200L, 111237L)),
        null,
        TestRow(null, null)
      ),
      """{
        |"c0":{"cn0":24,"cn1":[23,232]},
        |"c1":{"cn0":29,"cn1":[200,111237]},
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

  def epochDays(date: String): Int = {
    daysSinceEpoch(Date.valueOf(date))
  }

  def epochMicros(ts: String): Long = {
    val time = OffsetDateTime.parse(ts).toInstant
    ChronoUnit.MICROS.between(Instant.EPOCH, time)
  }
}
