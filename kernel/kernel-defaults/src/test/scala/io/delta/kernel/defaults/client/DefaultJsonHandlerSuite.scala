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
package io.delta.kernel.defaults.client

import java.math.{BigDecimal => JBigDecimal}
import java.util.Optional

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

import io.delta.kernel.types._
import io.delta.kernel.internal.util.InternalUtils.singletonStringColumnVector

import io.delta.kernel.defaults.utils.{TestRow, TestUtils}

// NOTE: currently tests are split across scala and java; additional tests are in
// TestDefaultJsonHandler.java
class DefaultJsonHandlerSuite extends AnyFunSuite with TestUtils {

  val jsonHandler = new DefaultJsonHandler(new Configuration());

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Tests for parseJson for statistics eligible types (additional in TestDefaultJsonHandler.java)
  /////////////////////////////////////////////////////////////////////////////////////////////////

  def testJsonParserWithSchema(
    jsonString: String,
    schema: StructType,
    expectedRow: TestRow): Unit = {
    val batchRows = jsonHandler.parseJson(
      singletonStringColumnVector(jsonString),
      schema,
      Optional.empty()
    ).getRows.toSeq
    checkAnswer(batchRows, Seq(expectedRow))
  }

  def testJsonParserForSingleType(
    jsonString: String,
    dataType: DataType,
    numColumns: Int,
    expectedRow: TestRow): Unit = {
    val schema = new StructType(
      (1 to numColumns).map(i => new StructField(s"col$i", dataType, true)).asJava)
    testJsonParserWithSchema(jsonString, schema, expectedRow)
  }

  test("parse byte type") {
    testJsonParserForSingleType(
      jsonString = """{"col1":0,"col2":-127,"col3":127}""",
      dataType = ByteType.BYTE,
      3,
      TestRow(0.toByte, -127.toByte, 127.toByte)
    )
  }

  test("parse short type") {
    testJsonParserForSingleType(
      jsonString = """{"col1":-32767,"col2":8,"col3":32767}""",
      dataType = ShortType.SHORT,
      3,
      TestRow(-32767.toShort, 8.toShort, 32767.toShort)
    )
  }

  test("parse integer type") {
    testJsonParserForSingleType(
      jsonString = """{"col1":-2147483648,"col2":8,"col3":2147483647}""",
      dataType = IntegerType.INTEGER,
      3,
      TestRow(-2147483648, 8, 2147483647)
    )
  }

  test("parse long type") {
    testJsonParserForSingleType(
      jsonString = """{"col1":-9223372036854775808,"col2":8,"col3":9223372036854775807}""",
      dataType = LongType.LONG,
      3,
      TestRow(-9223372036854775808L, 8L, 9223372036854775807L)
    )
  }

  test("parse float type") {
    testJsonParserForSingleType(
      jsonString =
      """{"col1":-9223.33,"col2":0.4,"col3":1.2E8,"col4":1.23E-7,"col5":0.004444444}""",
      dataType = FloatType.FLOAT,
      5,
      TestRow(-9223.33F, 0.4F, 120000000.0F, 0.000000123F, 0.004444444F)
    )
  }

  test("parse double type") {
    testJsonParserForSingleType(
      jsonString =
        """
          |{"col1":-9.2233333333E8,"col2":0.4,"col3":1.2E8,
          |"col4":1.234444444E-7,"col5":0.0444444444}""".stripMargin,
      dataType = DoubleType.DOUBLE,
      5,
      TestRow(-922333333.33D, 0.4D, 120000000.0D, 0.0000001234444444D, 0.0444444444D)
    )
  }

  test("parse string type") {
    testJsonParserForSingleType(
      jsonString = """{"col1": "foo", "col2": "", "col3": null}""",
      dataType = StringType.STRING,
      3,
      TestRow("foo", "", null)
    )
  }

  test("parse decimal type") {
    testJsonParserWithSchema(
      jsonString = """
      |{
      |  "col1":0,
      |  "col2":0.01234567891234567891234567891234567890,
      |  "col3":123456789123456789123456789123456789,
      |  "col4":1234567891234567891234567891.2345678900,
      |  "col5":1.23
      |}
      |""".stripMargin,
      schema = new StructType()
        .add("col1", DecimalType.USER_DEFAULT)
        .add("col2", new DecimalType(38, 38))
        .add("col3", new DecimalType(38, 0))
        .add("col4", new DecimalType(38, 10))
        .add("col5", new DecimalType(5, 2)),
      TestRow(
        new JBigDecimal(0),
        new JBigDecimal("0.01234567891234567891234567891234567890"),
        new JBigDecimal("123456789123456789123456789123456789"),
        new JBigDecimal("1234567891234567891234567891.2345678900"),
        new JBigDecimal("1.23")
      )
    )
  }

  test("parse date type") {
    testJsonParserForSingleType(
      jsonString = """{"col1":"2020-12-31"}""",
      dataType = DateType.DATE,
      1,
      TestRow(18627)
    )
  }

  test("parse timestamp type") {
    testJsonParserForSingleType(
      jsonString =
        """
          |{
          | "col1":"2050-01-01T00:00:00.000-08:00",
          | "col2":"1970-01-01T06:30:23.523Z"
          | }
          | """.stripMargin,
      dataType = TimestampType.TIMESTAMP,
      numColumns = 2,
      TestRow(2524636800000000L, 23423523000L)
    )
  }

  test("parse null input") {
    val schema = new StructType()
      .add("nested_struct", new StructType().add("foo", IntegerType.INTEGER))

    val batch = jsonHandler.parseJson(
      singletonStringColumnVector(null),
      schema,
      Optional.empty()
    )
    assert(batch.getColumnVector(0).getChild(0).isNullAt(0))
  }

  test("parse NaN and INF for float and double") {
    def testSpecifiedString(json: String, output: TestRow): Unit = {
      testJsonParserWithSchema(
        jsonString = json,
        schema = new StructType()
          .add("col1", FloatType.FLOAT)
          .add("col2", DoubleType.DOUBLE),
        output
      )
    }
    testSpecifiedString("""{"col1":"NaN","col2":"NaN"}""", TestRow(Float.NaN, Double.NaN))
    testSpecifiedString("""{"col1":"+INF","col2":"+INF"}""",
      TestRow(Float.PositiveInfinity, Double.PositiveInfinity))
    testSpecifiedString("""{"col1":"+Infinity","col2":"+Infinity"}""",
      TestRow(Float.PositiveInfinity, Double.PositiveInfinity))
    testSpecifiedString("""{"col1":"Infinity","col2":"Infinity"}""",
      TestRow(Float.PositiveInfinity, Double.PositiveInfinity))
    testSpecifiedString("""{"col1":"-INF","col2":"-INF"}""",
      TestRow(Float.NegativeInfinity, Double.NegativeInfinity))
    testSpecifiedString("""{"col1":"-Infinity","col2":"-Infinity"}""",
      TestRow(Float.NegativeInfinity, Double.NegativeInfinity))
  }

  //////////////////////////////////////////////////////////////////////////////////
  // END-TO-END TESTS FOR deserializeStructType (more tests in DataTypeParserSuite)
  //////////////////////////////////////////////////////////////////////////////////

  private def sampleMetadata: FieldMetadata = FieldMetadata.builder()
    .putNull("null")
    .putLong("long", 1000L)
    .putDouble("double", 2.222)
    .putBoolean("boolean", true)
    .putString("string", "value")
    .build()

  test("deserializeStructType: primitive type round trip") {
    val fields = BasePrimitiveType.getAllPrimitiveTypes().asScala.flatMap { dataType =>
      Seq(
        new StructField("col1" + dataType, dataType, true),
        new StructField("col1" + dataType, dataType, false),
        new StructField("col1" + dataType, dataType, false, sampleMetadata)
      )
    } ++ Seq(
      new StructField("col1decimal", new DecimalType(30, 10), true),
      new StructField("col2decimal", new DecimalType(38, 22), true),
      new StructField("col3decimal", new DecimalType(5, 2), true)
    )

    val expSchema = new StructType(fields.asJava);
    val serializedSchema = expSchema.toJson
    val actSchema = jsonHandler.deserializeStructType(serializedSchema)
    assert(expSchema == actSchema)
  }

  test("deserializeStructType: complex type round trip") {
    val arrayType = new ArrayType(IntegerType.INTEGER, true)
    val arrayArrayType = new ArrayType(arrayType, false)
    val mapType = new MapType(FloatType.FLOAT, BinaryType.BINARY, false)
    val mapMapType = new MapType(mapType, BinaryType.BINARY, true)
    val structType = new StructType().add("simple", DateType.DATE)
    val structAllType = new StructType()
      .add("prim", BooleanType.BOOLEAN)
      .add("arr", arrayType)
      .add("map", mapType)
      .add("struct", structType)

    val expSchema = new StructType()
      .add("col1", arrayType, true)
      .add("col2", arrayArrayType, false)
      .add("col3", mapType, false)
      .add("col4", mapMapType, false)
      .add("col5", structType, false)
      .add("col6", structAllType, false)

    val serializedSchema = expSchema.toJson
    val actSchema = jsonHandler.deserializeStructType(serializedSchema)
    assert(expSchema == actSchema)
  }

  test("deserializeStructType: not a StructType") {
    val e = intercept[IllegalArgumentException] {
      jsonHandler.deserializeStructType(new ArrayType(StringType.STRING, true).toJson())
    }
    assert(e.getMessage.contains("Could not parse the following JSON as a valid StructType"))
  }

  test("deserializeStructType: invalid JSON") {
    val e = intercept[RuntimeException] {
      jsonHandler.deserializeStructType(
        """
          |{
          |  "type" : "struct,
          |  "fields" : []
          |}
          |""".stripMargin
      )
    }
    assert(e.getMessage.contains("Could not parse JSON"))
  }

  // TODO we use toJson to serialize our physical and logical schemas in ScanStateRow, we should
  //  test DataType.toJson
}
