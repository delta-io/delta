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

    val json = stats.serializeAsJson(schema)

    assert(areJsonNodesEqual(json, expectedJson))
  }

  test("serializeAsJson handles NaN and Infinity correctly") {
    val schema = new StructType()
      .add("FloatType", FloatType.FLOAT)
      .add("DoubleType", DoubleType.DOUBLE)

    val minValues = Map(
      new Column("FloatType") -> Literal.ofFloat(Float.NaN),
      new Column("DoubleType") -> Literal.ofDouble(Double.NegativeInfinity)).asJava

    val maxValues = Map(
      new Column("FloatType") -> Literal.ofFloat(Float.PositiveInfinity),
      new Column("DoubleType") -> Literal.ofDouble(Double.NaN)).asJava

    val stats = new DataFileStatistics(
      1L,
      minValues,
      maxValues,
      Collections.emptyMap[Column, java.lang.Long]())

    val json = stats.serializeAsJson(schema)
    val expectedJson =
      """{
        |  "numRecords": 1,
        |  "minValues": {
        |    "FloatType": "NaN",
        |    "DoubleType": "-Infinity"
        |  },
        |  "maxValues": {
        |    "FloatType": "Infinity",
        |    "DoubleType": "NaN"
        |  },
        |  "nullCounts": {}
        |}""".stripMargin

    assert(areJsonNodesEqual(json, expectedJson))
  }

  test("serializeAsJson handles null values and null literals correctly") {
    val schema = new StructType()
      .add("col1", IntegerType.INTEGER)
      .add("col2", StringType.STRING)
      .add("col3", DoubleType.DOUBLE)
      .add(
        "nested",
        new StructType()
          .add("nestedCol1", IntegerType.INTEGER)
          .add("nestedCol2", StringType.STRING))

    val minValues = Map[Column, Literal](
      new Column("col1") -> Literal.ofInt(1),
      new Column("col2") -> null,
      new Column("col3") -> Literal.ofNull(DoubleType.DOUBLE),
      new Column(Array("nested", "nestedCol1")) -> Literal.ofInt(5),
      new Column(Array("nested", "nestedCol2")) -> Literal.ofNull(StringType.STRING)).asJava

    val maxValues = Map[Column, Literal](
      new Column("col2") -> Literal.ofString("z"),
      new Column("col3") -> null,
      new Column(Array("nested", "nestedCol1")) -> null,
      new Column(Array("nested", "nestedCol2")) -> Literal.ofString("zzz")).asJava

    val nullCounts = Map(
      new Column("col1") -> 5L,
      new Column("col2") -> 0L,
      new Column(Array("nested", "nestedCol1")) -> 2L).map { case (k, v) =>
      (k, java.lang.Long.valueOf(v))
    }.asJava

    val stats = new DataFileStatistics(
      100,
      minValues,
      maxValues,
      nullCounts)

    val expectedJson =
      """{
        |  "numRecords": 100,
        |  "minValues": {
        |    "col1": 1,
        |    "col3": null,
        |    "nested": {
        |      "nestedCol1": 5,
        |      "nestedCol2": null
        |    }
        |  },
        |  "maxValues": {
        |    "col2": "z",
        |    "nested": {
        |      "nestedCol2": "zzz"
        |    }
        |  },
        |  "nullCounts": {
        |    "col1": 5,
        |    "col2": 0,
        |    "nested": {
        |      "nestedCol1": 2
        |    }
        |  }
        |}""".stripMargin

    val json = stats.serializeAsJson(schema)
    assert(areJsonNodesEqual(json, expectedJson))
  }

  test("serializeAsJson returns empty nested objects when nested map is empty") {
    val nestedSchema = new StructType()
      .add("field1", IntegerType.INTEGER)
      .add("field2", StringType.STRING)
    val schema = new StructType().add("nested", nestedSchema)
    val stats = new DataFileStatistics(
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
    val json = stats.serializeAsJson(schema)
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
    val json = stats.serializeAsJson(schema)
    assert(areJsonNodesEqual(json, expectedJson))
  }

  test("deserialize invalid JSON structure throws KernelException") {
    val malformedJson =
      """{
        |  "numRecords": "invalid_value",
        |}""".stripMargin

    val exception = intercept[KernelException] {
      StatsUtils.deserializeFromJson(malformedJson)
    }
    assert(exception.getMessage.contains("Failed to parse JSON string"))
  }

  test("serialization and deserialization of stats") {
    val numRecords = 123L
    val dataSchema = new StructType().add("a", IntegerType.INTEGER)
    val stats = new DataFileStatistics(
      numRecords,
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap())

    val json = stats.serializeAsJson(dataSchema)
    val deserialized = StatsUtils.deserializeFromJson(json)

    assert(deserialized.get().getNumRecords == stats.getNumRecords)
  }

  test("test equals and hashCode work correctly for DataFileStatistics") {
    // Setup common test data
    val col1 = new Column("col1")
    val nestedField = new Column(Array("nested", "field"))

    // Create two identical stats objects
    val commonMaps = () => {
      val min = Map(col1 -> Literal.ofInt(10), nestedField -> Literal.ofString("value")).asJava
      val max = Map(col1 -> Literal.ofInt(100), nestedField -> Literal.ofString("zzzz")).asJava
      val nulls =
        Map(col1 -> java.lang.Long.valueOf(5L), nestedField -> java.lang.Long.valueOf(2L)).asJava
      (min, max, nulls)
    }

    val (min1, max1, nulls1) = commonMaps()
    val (min2, max2, nulls2) = commonMaps()

    val stats1 = new DataFileStatistics(100L, min1, max1, nulls1)
    val stats2 = new DataFileStatistics(100L, min2, max2, nulls2)

    // Stats with different value
    val differentMin =
      Map(col1 -> Literal.ofInt(20), nestedField -> Literal.ofString("value")).asJava
    val stats3 = new DataFileStatistics(100L, differentMin, max1, nulls1)

    // Stats with different structure
    val differentCol = new Column("col2")
    val structureMaps =
      Map(col1 -> Literal.ofInt(10), differentCol -> Literal.ofString("new")).asJava
    val stats4 = new DataFileStatistics(100L, structureMaps, structureMaps, nulls1)

    // Empty stats
    val emptyStats1 = new DataFileStatistics(
      50L,
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap())
    val emptyStats2 = new DataFileStatistics(
      50L,
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap())
    val emptyStats3 = new DataFileStatistics(
      60L,
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap())

    // Equality tests
    assert(
      stats1 == stats2 && stats1.hashCode() == stats2.hashCode(),
      "Identical stats should be equal with same hash")
    assert(stats1 != stats3, "Stats with different values should not be equal")
    assert(stats1 != stats4, "Stats with different structure should not be equal")
    assert(stats1 != null && stats1 != "string", "Stats should not equal null or different types")

    // Empty stats tests
    assert(
      emptyStats1 == emptyStats2 && emptyStats1.hashCode() == emptyStats2.hashCode(),
      "Empty stats with same records should be equal with same hash")
    assert(emptyStats1 != emptyStats3, "Empty stats with different records should not be equal")
  }

  test("serializeAsJson throws exception when literal type doesn't match schema data type") {
    val schema = new StructType()
      .add("intCol", IntegerType.INTEGER)
      .add("doubleCol", DoubleType.DOUBLE)
      .add(
        "nested",
        new StructType()
          .add("stringCol", StringType.STRING))

    val minValues = Map[Column, Literal](
      new Column("intCol") -> Literal.ofString("not an int"),
      new Column("doubleCol") -> Literal.ofDouble(1.23),
      new Column(Array("nested", "stringCol")) -> Literal.ofInt(42)).asJava

    val stats = new DataFileStatistics(
      100,
      minValues,
      Collections.emptyMap[Column, Literal](),
      Collections.emptyMap[Column, java.lang.Long]())

    val exception = intercept[KernelException] {
      stats.serializeAsJson(schema)
    }

    val expectedMessage = "Type mismatch for field 'intCol' when writing statistics" +
      ": expected integer, but found string"
    assert(exception.getMessage === expectedMessage)
  }
}
