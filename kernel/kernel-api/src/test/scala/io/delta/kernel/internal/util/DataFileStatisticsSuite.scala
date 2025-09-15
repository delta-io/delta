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

import java.util.{Collections, Optional}

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
      .add("VariantType", VariantType.VARIANT)

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
      new Column(Array("NestedStruct", "ac", "aca")) -> Literal.ofInt(1),
      new Column("VariantType") -> Literal.ofString(
        "0S&u501fk+ze0(tB98CpzF6vU0rJl95HpNdvjbtatpi(cu0wW^cTu")).asJava

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
      new Column(Array("NestedStruct", "ac", "aca")) -> Literal.ofInt(10),
      new Column("VariantType") -> Literal.ofString(
        "0S&u500&]LC42A9vqZe}wb#-i1}-a+cT!xdbWhT9cTx}7v<+K")).asJava

    val nullCount = Map(
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
      new Column(Array("NestedStruct", "ac", "aca")) -> 1L,
      new Column("VariantType") -> 1L)

    val tightBounds = false

    val stats = new DataFileStatistics(
      100,
      minValues,
      maxValues,
      nullCount.map { case (k, v) => (k, java.lang.Long.valueOf(v)) }.asJava,
      Optional.of(tightBounds))

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
        |    "TimestampNTZType": "1970-01-01T00:00:00",
        |    "BinaryType": "a",
        |    "NestedStruct": {
        |      "aa": "a",
        |      "ac": {
        |        "aca": 1
        |      }
        |    },
        |    "VariantType": "0S&u501fk+ze0(tB98CpzF6vU0rJl95HpNdvjbtatpi(cu0wW^cTu"
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
        |    "TimestampNTZType": "1970-01-01T00:00:00",
        |    "BinaryType": "z",
        |    "NestedStruct": {
        |      "aa": "z",
        |      "ac": {
        |        "aca": 10
        |      }
        |    },
        |    "VariantType": "0S&u500&]LC42A9vqZe}wb#-i1}-a+cT!xdbWhT9cTx}7v<+K"
        |  },
        |  "nullCount": {
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
        |    },
        |    "VariantType": 1
        |},
        |"tightBounds": false
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
      Collections.emptyMap[Column, java.lang.Long](),
      Optional.empty())

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
        |  "nullCount": {}
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

    val nullCount = Map(
      new Column("col1") -> 5L,
      new Column("col2") -> 0L,
      new Column(Array("nested", "nestedCol1")) -> 2L).map { case (k, v) =>
      (k, java.lang.Long.valueOf(v))
    }.asJava

    val tightBounds = true

    val stats = new DataFileStatistics(
      100,
      minValues,
      maxValues,
      nullCount,
      Optional.of(tightBounds))

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
        |  "nullCount": {
        |    "col1": 5,
        |    "col2": 0,
        |    "nested": {
        |      "nestedCol1": 2
        |    }
        |  },
        |  "tightBounds": true
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
      Collections.emptyMap(),
      Optional.empty())
    val expectedJson =
      """{
        |  "numRecords": 50,
        |  "minValues": {"nested": {}},
        |  "maxValues": {"nested": {}},
        |  "nullCount": {"nested": {}}
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
      Collections.emptyMap(),
      Optional.empty())
    val expectedJson =
      """{
        |  "numRecords": 75,
        |  "minValues": {
        |    "nested": {
        |      "field1": 10
        |    }
        |  },
        |  "maxValues": {"nested":{}},
        |  "nullCount": {"nested":{}}
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
      DataFileStatistics.deserializeFromJson(malformedJson, null)
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
      Collections.emptyMap(),
      Optional.empty())

    val json = stats.serializeAsJson(dataSchema)
    val deserialized = DataFileStatistics.deserializeFromJson(json, null)

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

    val stats1 = new DataFileStatistics(100L, min1, max1, nulls1, Optional.empty())
    val stats2 = new DataFileStatistics(100L, min2, max2, nulls2, Optional.empty())

    // Stats with different value
    val differentMin =
      Map(col1 -> Literal.ofInt(20), nestedField -> Literal.ofString("value")).asJava
    val stats3 = new DataFileStatistics(100L, differentMin, max1, nulls1, Optional.empty())

    // Stats with different structure
    val differentCol = new Column("col2")
    val structureMaps =
      Map(col1 -> Literal.ofInt(10), differentCol -> Literal.ofString("new")).asJava
    val stats4 =
      new DataFileStatistics(100L, structureMaps, structureMaps, nulls1, Optional.empty())

    // Empty stats
    val emptyStats1 = new DataFileStatistics(
      50L,
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      Optional.empty())
    val emptyStats2 = new DataFileStatistics(
      50L,
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      Optional.empty())
    val emptyStats3 = new DataFileStatistics(
      60L,
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      Optional.empty())

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
      Collections.emptyMap[Column, java.lang.Long](),
      Optional.empty())

    val exception = intercept[KernelException] {
      stats.serializeAsJson(schema)
    }

    val expectedMessage = "Type mismatch for field 'intCol' when writing statistics" +
      ": expected integer, but found string"
    assert(exception.getMessage === expectedMessage)
  }

  test("deserializeFromJson handles all data types correctly") {
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
      .add("BooleanType", BooleanType.BOOLEAN)
      .add("VariantType", VariantType.VARIANT)

    val json =
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
        |    "TimestampType": "1970-01-01T00:00:00.001Z",
        |    "TimestampNTZType": "1970-01-01T00:00:00.001",
        |    "BinaryType": "a",
        |    "BooleanType": true,
        |    "VariantType": "0S&u501fk+ze0(tB98CpzF6vU0rJl95HpNdvjbtatpi(cu0wW^cTu"
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
        |    "TimestampType": "1970-01-01T00:00:00.010Z",
        |    "TimestampNTZType": "1970-01-01T00:00:00.010",
        |    "BinaryType": "z",
        |    "BooleanType": false,
        |    "VariantType": "0S&u500&]LC42A9vqZe}wb#-i1}-a+cT!xdbWhT9cTx}7v<+K"
        |  },
        |  "nullCount": {
        |    "ByteType": 1,
        |    "StringType": 2,
        |    "DecimalType": 0,
        |    "BooleanType": 5
        |  },
        |  "tightBounds": true
        |}""".stripMargin

    val result = DataFileStatistics.deserializeFromJson(json, schema)
    assert(result.isPresent)

    val stats = result.get()
    assert(stats.getNumRecords == 100)

    val minValues = stats.getMinValues
    assert(minValues.get(new Column("ByteType")).getValue == 1.toByte)
    assert(minValues.get(new Column("IntegerType")).getValue == 1)
    assert(minValues.get(new Column("FloatType")).getValue == 0.1f)
    assert(minValues.get(new Column("StringType")).getValue == "a")
    assert(minValues.get(new Column("BooleanType")).getValue == true)
    assert(minValues.get(new Column("VariantType")).getValue ==
      "0S&u501fk+ze0(tB98CpzF6vU0rJl95HpNdvjbtatpi(cu0wW^cTu")

    val maxValues = stats.getMaxValues
    assert(maxValues.get(new Column("LongType")).getValue == 10L)
    assert(maxValues.get(new Column("DoubleType")).getValue == 10.1)
    assert(maxValues.get(new Column("BooleanType")).getValue == false)
    assert(maxValues.get(new Column("VariantType")).getValue ==
      "0S&u500&]LC42A9vqZe}wb#-i1}-a+cT!xdbWhT9cTx}7v<+K")

    val nullCount = stats.getNullCount
    assert(nullCount.get(new Column("ByteType")) == 1L)
    assert(nullCount.get(new Column("StringType")) == 2L)
    assert(nullCount.get(new Column("DecimalType")) == 0L)

    assert(stats.getTightBounds.isPresent && stats.getTightBounds.get)
  }

  test("deserializeFromJson handles nested structures correctly") {
    val schema = new StructType()
      .add("simple", StringType.STRING)
      .add(
        "nested",
        new StructType()
          .add("field1", IntegerType.INTEGER)
          .add(
            "deep",
            new StructType()
              .add("field2", StringType.STRING)
              .add(
                "deeper",
                new StructType()
                  .add("field3", IntegerType.INTEGER))))

    val json =
      """{
        |  "numRecords": 50,
        |  "minValues": {
        |    "simple": "value1",
        |    "nested": {
        |      "field1": 10,
        |      "deep": {
        |        "field2": "nested_value",
        |        "deeper": {
        |          "field3": 42
        |        }
        |      }
        |    }
        |  },
        |  "maxValues": {
        |    "simple": "value2",
        |    "nested": {
        |      "field1": 100,
        |      "deep": {
        |        "field2": "zzz_value"
        |      }
        |    }
        |  },
        |  "nullCount": {
        |    "simple": 0,
        |    "nested": {
        |      "field1": 5,
        |      "deep": {
        |        "field2": 2,
        |        "deeper": {
        |          "field3": 1
        |        }
        |      }
        |    }
        |  },
        |  "tightBounds": true
        |}""".stripMargin

    val result = DataFileStatistics.deserializeFromJson(json, schema)
    assert(result.isPresent)

    val stats = result.get()
    assert(stats.getNumRecords == 50)

    // Test simple column
    val minValues = stats.getMinValues
    assert(minValues.get(new Column("simple")).getValue == "value1")

    // Test nested columns with different path depths
    assert(minValues.get(new Column(Array("nested", "field1"))).getValue == 10)
    assert(minValues.get(new Column(Array("nested", "deep", "field2"))).getValue == "nested_value")
    assert(minValues.get(new Column(Array("nested", "deep", "deeper", "field3"))).getValue == 42)

    // Test that max values work for nested too
    val maxValues = stats.getMaxValues
    assert(maxValues.get(new Column(Array("nested", "field1"))).getValue == 100)
    assert(maxValues.get(new Column(Array("nested", "deep", "field2"))).getValue == "zzz_value")

    // Test null counts for nested
    val nullCount = stats.getNullCount
    assert(nullCount.get(new Column(Array("nested", "field1"))) == 5L)
    assert(nullCount.get(new Column(Array("nested", "deep", "deeper", "field3"))) == 1L)

    // Test tight bounds for nested columns
    assert(stats.getTightBounds.isPresent && stats.getTightBounds.get)
  }

  test("round-trip serialization and deserialization consistency") {
    val nestedStructType = new StructType()
      .add("aa", StringType.STRING)
      .add("ac", new StructType().add("aca", IntegerType.INTEGER))

    val schema = new StructType()
      .add("IntegerType", IntegerType.INTEGER)
      .add("StringType", StringType.STRING)
      .add("DoubleType", DoubleType.DOUBLE)
      .add("NestedStruct", nestedStructType)

    val minValues = Map(
      new Column("IntegerType") -> Literal.ofInt(1),
      new Column("StringType") -> Literal.ofString("a"),
      new Column("DoubleType") -> Literal.ofDouble(0.1),
      new Column(Array("NestedStruct", "aa")) -> Literal.ofString("nested_a"),
      new Column(Array("NestedStruct", "ac", "aca")) -> Literal.ofInt(5)).asJava

    val maxValues = Map(
      new Column("IntegerType") -> Literal.ofInt(100),
      new Column("StringType") -> Literal.ofString("z"),
      new Column("DoubleType") -> Literal.ofDouble(99.9),
      new Column(Array("NestedStruct", "aa")) -> Literal.ofString("nested_z"),
      new Column(Array("NestedStruct", "ac", "aca")) -> Literal.ofInt(50)).asJava

    val nullCount = Map(
      new Column("IntegerType") -> 2L,
      new Column("StringType") -> 0L,
      new Column(Array("NestedStruct", "aa")) -> 1L).map { case (k, v) =>
      (k, java.lang.Long.valueOf(v))
    }.asJava

    val tightBounds = false

    val originalStats = new DataFileStatistics(
      123L,
      minValues,
      maxValues,
      nullCount,
      Optional.of(tightBounds))

    // Serialize then deserialize
    val json = originalStats.serializeAsJson(schema)
    val deserializedOpt = DataFileStatistics.deserializeFromJson(json, schema)

    assert(deserializedOpt.isPresent)
    val deserializedStats = deserializedOpt.get()

    // Verify they are equal
    assert(deserializedStats.getNumRecords == originalStats.getNumRecords)
    assert(deserializedStats.getMinValues.size() == originalStats.getMinValues.size())
    assert(deserializedStats.getMaxValues.size() == originalStats.getMaxValues.size())
    assert(deserializedStats.getNullCount.size() == originalStats.getNullCount.size())

    // Verify specific values match
    assert(deserializedStats.getMinValues.get(new Column("IntegerType")).getValue == 1)
    assert(deserializedStats.getMaxValues.get(new Column(Array(
      "NestedStruct",
      "ac",
      "aca"))).getValue == 50)
    assert(deserializedStats.getNullCount.get(new Column("StringType")) == 0L)

    assert(deserializedStats.getTightBounds == originalStats.getTightBounds)

  }

  test("deserializeFromJson handles NaN and Infinity correctly") {
    val schema = new StructType()
      .add("FloatType", FloatType.FLOAT)
      .add("DoubleType", DoubleType.DOUBLE)

    val json =
      """{
        |  "numRecords": 10,
        |  "minValues": {
        |    "FloatType": "NaN",
        |    "DoubleType": "-Infinity"
        |  },
        |  "maxValues": {
        |    "FloatType": "Infinity",
        |    "DoubleType": "NaN"
        |  },
        |  "nullCount": {
        |    "FloatType": 1,
        |    "DoubleType": 2
        |  },
        |  "tightBounds": true
        |}""".stripMargin

    val result = DataFileStatistics.deserializeFromJson(json, schema)
    assert(result.isPresent)

    val stats = result.get()
    assert(stats.getNumRecords == 10)

    val minValues = stats.getMinValues
    val maxValues = stats.getMaxValues

    // Test NaN and Infinity values - Note: Float values will be stored as Float, not Double
    assert(
      java.lang.Float.isNaN(minValues.get(new Column("FloatType")).getValue.asInstanceOf[Float]))
    assert(minValues.get(new Column("DoubleType")).getValue == Double.NegativeInfinity)
    assert(maxValues.get(new Column("FloatType")).getValue == Float.PositiveInfinity)
    assert(
      java.lang.Double.isNaN(maxValues.get(new Column("DoubleType")).getValue.asInstanceOf[Double]))

    val nullCount = stats.getNullCount
    assert(nullCount.get(new Column("FloatType")) == 1L)
    assert(nullCount.get(new Column("DoubleType")) == 2L)

    assert(stats.getTightBounds.isPresent && stats.getTightBounds.get)
  }

  test("deserializeFromJson handles empty stats correctly") {
    val schema = new StructType() // Empty schema for empty stats

    val json =
      """{
        |  "numRecords": 42,
        |  "minValues": {},
        |  "maxValues": {},
        |  "nullCount": {}
        |}""".stripMargin

    val result = DataFileStatistics.deserializeFromJson(json, schema)
    assert(result.isPresent)

    val stats = result.get()
    assert(stats.getNumRecords == 42)
    assert(stats.getMinValues.isEmpty)
    assert(stats.getMaxValues.isEmpty)
    assert(stats.getNullCount.isEmpty)
    assert(!stats.getTightBounds.isPresent)
  }

  test("deserializeFromJson handles partial nested objects correctly") {
    // Schema should include all possible fields that appear in the JSON
    val schema = new StructType()
      .add("simple", StringType.STRING)
      .add(
        "nested",
        new StructType()
          .add("field1", IntegerType.INTEGER)
          .add("field2", StringType.STRING))
      .add("other", IntegerType.INTEGER)

    val json =
      """{
        |  "numRecords": 25,
        |  "minValues": {
        |    "simple": "value",
        |    "nested": {
        |      "field1": 10
        |    }
        |  },
        |  "maxValues": {
        |    "nested": {
        |      "field2": "different_field"
        |    },
        |    "other": 99
        |  },
        |  "nullCount": {
        |    "simple": 1,
        |    "nested": {
        |      "field1": 0,
        |      "field2": 5
        |    }
        |  },
        |"tightBounds": true
        |}""".stripMargin

    val result = DataFileStatistics.deserializeFromJson(json, schema)
    assert(result.isPresent)

    val stats = result.get()
    assert(stats.getNumRecords == 25)

    val minValues = stats.getMinValues
    val maxValues = stats.getMaxValues
    val nullCount = stats.getNullCount

    // minValues has simple + nested.field1
    assert(minValues.get(new Column("simple")).getValue == "value")
    assert(minValues.get(new Column(Array("nested", "field1"))).getValue == 10)
    assert(minValues.get(new Column(Array("nested", "field2"))) == null) // not present in minValues

    // maxValues has nested.field2 + other (different structure)
    assert(maxValues.get(new Column("simple")) == null) // not present in maxValues
    assert(maxValues.get(new Column(Array("nested", "field2"))).getValue == "different_field")
    assert(maxValues.get(new Column("other")).getValue == 99)

    // nullCount has both fields under nested
    assert(nullCount.get(new Column("simple")) == 1L)
    assert(nullCount.get(new Column(Array("nested", "field1"))) == 0L)
    assert(nullCount.get(new Column(Array("nested", "field2"))) == 5L)

    // tightBounds has simple + nested.field2 + other
    assert(stats.getTightBounds.isPresent && stats.getTightBounds.get)

  }

  test("withoutTightBounds removes tight bounds from DataFileStatistics") {
    val schema = new StructType()
      .add("col1", IntegerType.INTEGER)
      .add("col2", StringType.STRING)
      .add(
        "nested",
        new StructType()
          .add("field1", IntegerType.INTEGER)
          .add("field2", StringType.STRING))

    // stats with a mix of true and false tight bounds
    val minValues = Map(
      new Column("col1") -> Literal.ofInt(1),
      new Column("col2") -> Literal.ofString("a"),
      new Column(Array("nested", "field1")) -> Literal.ofInt(10),
      new Column(Array("nested", "field2")) -> Literal.ofString("nested_a")).asJava

    val maxValues = Map(
      new Column("col1") -> Literal.ofInt(100),
      new Column("col2") -> Literal.ofString("z"),
      new Column(Array("nested", "field1")) -> Literal.ofInt(200),
      new Column(Array("nested", "field2")) -> Literal.ofString("nested_z")).asJava

    val nullCount = Map(
      new Column("col1") -> 5L,
      new Column("col2") -> 0L,
      new Column(Array("nested", "field1")) -> 2L,
      new Column(Array("nested", "field2")) -> 3L).map { case (k, v) =>
      (k, java.lang.Long.valueOf(v))
    }.asJava

    val originalTightBounds = true

    val originalStats = new DataFileStatistics(
      100L,
      minValues,
      maxValues,
      nullCount,
      Optional.of(originalTightBounds))

    // Test that original stats has tight bounds
    assert(originalStats.getTightBounds.isPresent && originalStats.getTightBounds.get)

    // Apply withoutTightBounds
    val statsWithoutTightBounds = originalStats.withoutTightBounds()

    // Verify all other fields remain unchanged
    assert(statsWithoutTightBounds.getNumRecords == originalStats.getNumRecords)
    assert(statsWithoutTightBounds.getMinValues == originalStats.getMinValues)
    assert(statsWithoutTightBounds.getMaxValues == originalStats.getMaxValues)
    assert(statsWithoutTightBounds.getNullCount == originalStats.getNullCount)

    // Verify tight bounds is now false
    assert(statsWithoutTightBounds.getTightBounds.isPresent
      && !statsWithoutTightBounds.getTightBounds.get)

    // Verify serialization reflects the change
    val jsonAfter = statsWithoutTightBounds.serializeAsJson(schema)
    val expectedJsonWithFalseTightBounds =
      """{
        |  "numRecords": 100,
        |  "minValues": {
        |    "col1": 1,
        |    "col2": "a",
        |    "nested": {
        |      "field1": 10,
        |      "field2": "nested_a"
        |    }
        |  },
        |  "maxValues": {
        |    "col1": 100,
        |    "col2": "z",
        |    "nested": {
        |      "field1": 200,
        |      "field2": "nested_z"
        |    }
        |  },
        |  "nullCount": {
        |    "col1": 5,
        |    "col2": 0,
        |    "nested": {
        |      "field1": 2,
        |      "field2": 3
        |    }
        |  },
        |  "tightBounds": false
        |}""".stripMargin

    assert(areJsonNodesEqual(jsonAfter, expectedJsonWithFalseTightBounds))

    // Test edge case: stats with already false tight bounds

    val statsAlreadyFalse = new DataFileStatistics(
      50L,
      Map(new Column("col1") -> Literal.ofInt(1)).asJava,
      Map(new Column("col1") -> Literal.ofInt(10)).asJava,
      Map(new Column("col1") -> java.lang.Long.valueOf(0L)).asJava,
      Optional.of(false))

    val resultAlreadyFalse = statsAlreadyFalse.withoutTightBounds()
    assert(resultAlreadyFalse.getTightBounds.isPresent &&
      !resultAlreadyFalse.getTightBounds.get)

    // Test edge case: stats with empty tight bounds
    val emptyTightBoundsStats = new DataFileStatistics(
      25L,
      minValues,
      maxValues,
      nullCount,
      Optional.empty())

    val resultFromEmpty = emptyTightBoundsStats.withoutTightBounds()
    assert(resultFromEmpty.getTightBounds.isPresent &&
      !resultFromEmpty.getTightBounds.get)
  }

  test("deserializing invalid variant stats throws KernelException") {
    val schema = new StructType().add("VariantType", VariantType.VARIANT);
    val invalidVariantStats =
      """|{
         |  "numRecords": 100,
         |  "minValues": {
         |    "VariantType": 1234
         |  },
         |  "maxValues": {
         |    "VariantType": 5678
         |  }
         |}""".stripMargin

    val exception = intercept[KernelException] {
      DataFileStatistics.deserializeFromJson(invalidVariantStats, schema)
    }

    assert(exception.getMessage.contains("Expected variant as string value"))
  }
}
