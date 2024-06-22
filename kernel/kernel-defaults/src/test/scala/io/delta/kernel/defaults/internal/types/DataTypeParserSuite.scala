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
package io.delta.kernel.defaults.internal.types

import scala.reflect.ClassTag

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite

import io.delta.kernel.types._

class DataTypeParserSuite extends AnyFunSuite {

  import DataTypeParserSuite._

  private val objectMapper = new ObjectMapper()

  private def parse(json: String): DataType = {
    DataTypeParser.parseDataType(objectMapper.readTree(json))
  }

  private def checkDataType(dataTypeString: String, expectedDataType: DataType): Unit = {
    test(s"parseDataType: parse ${dataTypeString.replace("\n", "")}") {
      assert(parse(dataTypeString) === expectedDataType)
    }
  }

  private def checkError[T <: Throwable](json: String, expectedErrorContains: String)
      (implicit classTag: ClassTag[T]): Unit = {
    val e = intercept[T] {
      parse(json)
    }
    assert(e.getMessage.contains(expectedErrorContains))
  }

  /* --------------- Primitive data types (stored as a string) ----------------- */

  checkDataType("\"string\"", StringType.STRING)
  checkDataType("\"long\"", LongType.LONG)
  checkDataType("\"short\"", ShortType.SHORT)
  checkDataType("\"integer\"", IntegerType.INTEGER)
  checkDataType("\"boolean\"", BooleanType.BOOLEAN)
  checkDataType("\"byte\"", ByteType.BYTE)
  checkDataType("\"float\"", FloatType.FLOAT)
  checkDataType("\"double\"", DoubleType.DOUBLE)
  checkDataType("\"binary\"", BinaryType.BINARY)
  checkDataType("\"date\"", DateType.DATE)
  checkDataType("\"timestamp\"", TimestampType.TIMESTAMP)
  checkDataType("\"decimal\"", DecimalType.USER_DEFAULT)
  checkDataType("\"decimal(10, 5)\"", new DecimalType(10, 5))

  test("parseDataType: invalid primitive string data type") {
    checkError[IllegalArgumentException]("\"foo\"", "foo is not a supported delta data type")
  }

  test("parseDataType: mis-formatted decimal  data type") {
    checkError[IllegalArgumentException](
      "\"decimal(1)\"",
      "decimal(1) is not a supported delta data type")
  }

  /* ---------------  Complex types ----------------- */

  test("parseDataType: array type") {
    for (containsNull <- Seq(true, false)) {
      for ((elementJson, elementType) <- SAMPLE_JSON_TO_TYPES) {
        val parsedType = parse(arrayTypeJson(elementJson, containsNull))
        val expectedType = new ArrayType(elementType, containsNull)
        assert(parsedType == expectedType)
      }
    }
  }

  test("parseDataType: map type") {
    for (valueContainsNull <- Seq(true, false)) {
      for ((keyJson, keyType) <- SAMPLE_JSON_TO_TYPES) {
        for ((valueJson, valueType) <- SAMPLE_JSON_TO_TYPES) {
          val parsedType = parse(mapTypeJson(keyJson, valueJson, valueContainsNull))
          val expectedType = new MapType(keyType, valueType, valueContainsNull)
          assert(parsedType == expectedType)
        }
      }
    }
  }

  test("parseDataType: struct type") {
    for ((col1Json, col1Type) <- SAMPLE_JSON_TO_TYPES) {
      for ((col2Json, col2Type) <- SAMPLE_JSON_TO_TYPES) {
        val fieldsJson = Seq(
          structFieldJson("col1", col1Json, false),
          structFieldJson("col2", col2Json, true, Some("{ \"int\" : 0 }"))
        )
        val parsedType = parse(structTypeJson(fieldsJson))
        val expectedType = new StructType()
          .add("col1", col1Type, false)
          .add("col2", col2Type, true, FieldMetadata.builder().putLong("int", 0).build())
        assert(parsedType == expectedType)
      }
    }
  }

  test("parseDataType: special characters for column name") {
    val parsedType = parse(structTypeJson(Seq(
      structFieldJson("@_! *c", "\"string\"", true)
    )))
    val expectedType = new StructType()
      .add("@_! *c", StringType.STRING, true)
    assert(parsedType == expectedType)
  }

  test("parseDataType: empty struct type") {
    val str =
      """
        |{
        |  "type" : "struct",
        |  "fields": []
        |}
        |""".stripMargin
    assert(parse(str) == new StructType())
  }

  test("parseDataType: parsing FieldMetadata") {
    def testFieldMetadata(fieldMetadataJson: String, expectedFieldMetadata: FieldMetadata): Unit = {
      val parsedMetadata = parse(structTypeJson(
        Seq(structFieldJson("testCol", "\"string\"", true, Some(fieldMetadataJson)))
      )).asInstanceOf[StructType].get("testCol").getMetadata

      assert(parsedMetadata == expectedFieldMetadata)
    }

    val fieldMetadataAllTypesJson =
      """
        |{
        |  "null" : null,
        |  "int" : 10,
        |  "long-1" : -16070400023423400,
        |  "long-2" : 16070400023423400,
        |  "double" : 2.22,
        |  "boolean" : true,
        |  "string" : "10",
        |  "metadata" : { "nestedInt" : 200 },
        |  "empty_arr" : [],
        |  "int_arr" : [1, 2, 0],
        |  "double_arr" : [1.0, 2.0, 3.0],
        |  "boolean_arr" : [true],
        |  "string_arr" : ["one", "two"],
        |  "metadata_arr" : [{ "one" : 1, "two" : true }, {}]
        |}
        |""".stripMargin
    val expectedFieldMetadataAllTypes = FieldMetadata.builder()
      .putNull("null")
      .putLong("int", 10)
      .putLong("long-1", -16070400023423400L)
      .putLong("long-2", 16070400023423400L)
      .putDouble("double", 2.22)
      .putBoolean("boolean", true)
      .putString("string", "10")
      .putFieldMetadata("metadata", FieldMetadata.builder().putLong("nestedInt", 200).build())
      .putLongArray("empty_arr", Array())
      .putLongArray("int_arr", Array(1, 2, 0))
      .putDoubleArray("double_arr", Array(1.0, 2.0, 3.0))
      .putBooleanArray("boolean_arr", Array(true))
      .putStringArray("string_arr", Array("one", "two"))
      .putFieldMetadataArray("metadata_arr",
        Array(
          FieldMetadata.builder().putLong("one", 1).putBoolean("two", true).build(),
          FieldMetadata.empty()))
      .build()

    testFieldMetadata(fieldMetadataAllTypesJson, expectedFieldMetadataAllTypes)
    testFieldMetadata("{}", FieldMetadata.empty())
  }

  test("parseDataType: invalid field for type") {
    checkError[IllegalArgumentException](
      """
        |{
        |  "type" : "foo",
        |  "two" : "val2"
        |}
        |""".stripMargin,
      "Could not parse the following JSON as a valid Delta data type"
    )
  }

  test("parseDataType: not a valid JSON node (not a string or object)") {
    checkError[IllegalArgumentException](
      "0",
      "Could not parse the following JSON as a valid Delta data type"
    )
  }
}

object DataTypeParserSuite {

  val SAMPLE_JSON_TO_TYPES = Seq(
    ("\"string\"", StringType.STRING),
    ("\"integer\"", IntegerType.INTEGER),
    (arrayTypeJson("\"string\"", true), new ArrayType(StringType.STRING, true)),
    (mapTypeJson("\"integer\"", "\"string\"", true),
      new MapType(IntegerType.INTEGER, StringType.STRING, true)),
    (structTypeJson(Seq(
      structFieldJson("col1", "\"string\"", true),
      structFieldJson("col2", "\"string\"", false, Some("{ \"int\" : 0 }")))),
      new StructType()
        .add("col1", StringType.STRING, true)
        .add("col2", StringType.STRING, false, FieldMetadata.builder().putLong("int", 0).build())
    )
  )

  def arrayTypeJson(elementJson: String, containsNull: Boolean): String = {
    s"""
       |{
       |  "type" : "array",
       |  "elementType" : $elementJson,
       |  "containsNull" : $containsNull
       |}
       |""".stripMargin
  }

  def mapTypeJson(keyJson: String, valueJson: String, valueContainsNull: Boolean): String = {
    s"""
       |{
       |  "type" : "map",
       |  "keyType" : $keyJson,
       |  "valueType" : $valueJson,
       |  "valueContainsNull" : $valueContainsNull
       |}
       |""".stripMargin
  }

  def structFieldJson(
      name: String,
      typeJson: String,
      nullable: Boolean,
      metadataJson: Option[String] = None): String = {
    metadataJson match {
      case Some(metadata) =>
        s"""
           |{
           |  "name" : "$name",
           |  "type" : $typeJson,
           |  "nullable" : $nullable,
           |  "metadata" : $metadata
           |}
           |""".stripMargin
      case None =>
        s"""
           |{
           |  "name" : "$name",
           |  "type" : $typeJson,
           |  "nullable" : $nullable
           |}
           |""".stripMargin
    }
  }

  def structTypeJson(fieldsJsons: Seq[String]): String = {
    s"""
      |{
      |  "type" : "struct",
      |  "fields": ${fieldsJsons.mkString("[\n", ",\n", "]\n")}
      |}
      |""".stripMargin
  }
}
