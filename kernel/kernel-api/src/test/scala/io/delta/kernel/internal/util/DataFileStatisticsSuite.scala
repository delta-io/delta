/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.util

import java.util.Collections

import scala.collection.JavaConverters.mapAsJavaMapConverter

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.statistics.DataFileStatistics
import io.delta.kernel.types._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class DataFileStatisticsSuite extends AnyFunSuite with Matchers {

  val objectMapper = new ObjectMapper()

  def jsonToNode(json: String): JsonNode = {
    objectMapper.readTree(json)
  }

  def areJsonNodesEqual(json1: String, json2: String): Boolean = {
    val node1 = jsonToNode(json1)
    val node2 = jsonToNode(json2)
    node1 == node2
  }

  test("DataFileStatistics serialization with all types") {
    val nestedStructType = new StructType()
      .add("aa", StringType.STRING)
      .add("ac", new StructType().add("aca", IntegerType.INTEGER))

    val schema = new StructType()
      .add("ByteType", ByteType.BYTE)
      .add("ShortType", ShortType.SHORT)
      .add("IntegerType", IntegerType.INTEGER)
      .add("LongType", LongType.LONG)
      .add("FloatType", FloatType.FLOAT)
      .add("DoubleType", DoubleType.DOUBLE)
      .add("DecimalType", new DecimalType(10, 2))
      .add("StringType", StringType.STRING)
      .add("DateType", DateType.DATE)
      .add("TimestampType", TimestampType.TIMESTAMP)
      .add("TimestampNTZType", TimestampNTZType.TIMESTAMP_NTZ)
      .add("BinaryType", BinaryType.BINARY)
      .add("NestedStruct", nestedStructType)

    val minValues = Map(
      new Column("ByteType") -> Literal.ofByte(1.toByte),
      new Column("ShortType") -> Literal.ofShort(1.toShort),
      new Column("IntegerType") -> Literal.ofInt(1),
      new Column("LongType") -> Literal.ofLong(1L),
      new Column("FloatType") -> Literal.ofFloat(0.1f),
      new Column("DoubleType") -> Literal.ofDouble(0.1),
      new Column("DecimalType") -> Literal.ofDecimal(new java.math.BigDecimal("123.45"), 10, 2),
      new Column("StringType") -> Literal.ofString("a"),
      new Column("DateType") -> Literal.ofDate(1),
      new Column("TimestampType") -> Literal.ofTimestamp(1L),
      new Column("TimestampNTZType") -> Literal.ofTimestampNtz(1L),
      new Column("BinaryType") -> Literal.ofBinary("a".getBytes),
      new Column(Array("NestedStruct", "aa")) -> Literal.ofString("a"),
      new Column(Array("NestedStruct", "ac", "aca")) -> Literal.ofInt(1)).asJava

    val maxValues = Map(
      new Column("ByteType") -> Literal.ofByte(10.toByte),
      new Column("ShortType") -> Literal.ofShort(10.toShort),
      new Column("IntegerType") -> Literal.ofInt(10),
      new Column("LongType") -> Literal.ofLong(10L),
      new Column("FloatType") -> Literal.ofFloat(10.1f),
      new Column("DoubleType") -> Literal.ofDouble(10.1),
      new Column("DecimalType") -> Literal.ofDecimal(new java.math.BigDecimal("456.78"), 10, 2),
      new Column("StringType") -> Literal.ofString("z"),
      new Column("DateType") -> Literal.ofDate(10),
      new Column("TimestampType") -> Literal.ofTimestamp(10L),
      new Column("TimestampNTZType") -> Literal.ofTimestampNtz(10L),
      new Column("BinaryType") -> Literal.ofBinary("z".getBytes),
      new Column(Array("NestedStruct", "aa")) -> Literal.ofString("z"),
      new Column(Array("NestedStruct", "ac", "aca")) -> Literal.ofInt(10)).asJava

    val nullCounts = Map(
      new Column("ByteType") -> 1L,
      new Column("ShortType") -> 1L,
      new Column("IntegerType") -> 1L,
      new Column("LongType") -> 1L,
      new Column("FloatType") -> 1L,
      new Column("DoubleType") -> 1L,
      new Column("DecimalType") -> 1L,
      new Column("StringType") -> 1L,
      new Column("DateType") -> 1L,
      new Column("TimestampType") -> 1L,
      new Column("TimestampNTZType") -> 1L,
      new Column("BinaryType") -> 1L,
      new Column(Array("NestedStruct", "aa")) -> 1L,
      new Column(Array("NestedStruct", "ac", "aca")) -> 1L)

    val stats = new DataFileStatistics(
      schema,
      100,
      minValues,
      maxValues,
      nullCounts.map { case (k, v) => (k, java.lang.Long.valueOf(v)) }.asJava)

    val expectedJson =
      """{
        |  "numRecords": 100,
        |  "minValues": {
        |    "ByteType": 1,
        |    "ShortType": 1,
        |    "IntegerType": 1,
        |    "LongType": 1,
        |    "FloatType": 0.1,
        |    "DoubleType": 0.1,
        |    "DecimalType": 123.45,
        |    "StringType": "a",
        |    "DateType": "1970-01-02",
        |    "TimestampType": "1970-01-01T00:00:00.000Z",
        |    "TimestampNTZType": "1970-01-01T00:00:00.000Z",
        |    "BinaryType": "a",
        |    "NestedStruct": {
        |      "aa": "a",
        |      "ac": {
        |        "aca": 1
        |      }
        |    }
        |  },
        |  "maxValues": {
        |    "ByteType": 10,
        |    "ShortType": 10,
        |    "IntegerType": 10,
        |    "LongType": 10,
        |    "FloatType": 10.1,
        |    "DoubleType": 10.1,
        |    "DecimalType": 456.78,
        |    "StringType": "z",
        |    "DateType": "1970-01-11",
        |    "TimestampType": "1970-01-01T00:00:00.000Z",
        |    "TimestampNTZType": "1970-01-01T00:00:00.000Z",
        |    "BinaryType": "z",
        |    "NestedStruct": {
        |      "aa": "z",
        |      "ac": {
        |        "aca": 10
        |      }
        |    }
        |  },
        |  "nullCounts": {
        |    "ByteType": 1,
        |    "ShortType": 1,
        |    "IntegerType": 1,
        |    "LongType": 1,
        |    "FloatType": 1,
        |    "DoubleType": 1,
        |    "DecimalType": 1,
        |    "StringType": 1,
        |    "DateType": 1,
        |    "TimestampType": 1,
        |    "TimestampNTZType": 1,
        |    "BinaryType": 1,
        |    "NestedStruct": {
        |      "aa": 1,
        |      "ac": {
        |        "aca": 1
        |      }
        |    }
        |}
        |}""".stripMargin

    val json = stats.serializeAsJson()

    assert(areJsonNodesEqual(json, expectedJson))
  }

  test("serializeAsJson returns empty nested objects when nested map is empty") {
    val nestedSchema = new StructType()
      .add("field1", IntegerType.INTEGER)
      .add("field2", StringType.STRING)
    val schema = new StructType().add("nested", nestedSchema)
    val stats = new DataFileStatistics(
      schema,
      50L,
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap())
    val expectedJson =
      """{
        |  "numRecords": 50,
        |  "minValues": {"nested": {}},
        |  "maxValues": {"nested": {}},
        |  "nullCounts": {"nested": {}}
        |}""".stripMargin
    val json = stats.serializeAsJson()
    assert(areJsonNodesEqual(json, expectedJson))
  }

  test("serializeAsJson handles partially populated nested values") {
    val nestedSchema = new StructType()
      .add("field1", IntegerType.INTEGER)
      .add("field2", StringType.STRING)
    val schema = new StructType().add("nested", nestedSchema)
    val minValues = Map(
      new Column(Array("nested", "field1")) -> Literal.ofInt(10)).asJava
    val stats = new DataFileStatistics(
      schema,
      75L,
      minValues,
      Collections.emptyMap(),
      Collections.emptyMap())
    val expectedJson =
      """{
        |  "numRecords": 75,
        |  "minValues": {
        |    "nested": {
        |      "field1": 10
        |    }
        |  },
        |  "maxValues": {"nested":{}},
        |  "nullCounts": {"nested":{}}
        |}""".stripMargin
    val json = stats.serializeAsJson()
    assert(areJsonNodesEqual(json, expectedJson))
  }

  test("deserialize invalid JSON structure throws KernelException") {
    val malformedJson =
      """{
        |  "numRecords": "invalid_value",
        |}""".stripMargin

    val exception = intercept[KernelException] {
      DataFileStatistics.deserializeFromJson(malformedJson)
    }
    assert(exception.getMessage.contains("Failed to parse JSON string"))
  }

  test("serialization and deserialization of stats") {
    val numRecords = 123L
    // Currently we only support deserialization of numRecords.
    val stats = new DataFileStatistics(
      null,
      numRecords,
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap())
    val json = stats.serializeAsJson()
    val deserialized = DataFileStatistics.deserializeFromJson(json)
    assert(deserialized.get().getNumRecords.equals(stats.getNumRecords))
  }
}
