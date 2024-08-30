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
package io.delta.kernel.internal.types

import com.fasterxml.jackson.databind.ObjectMapper
import io.delta.kernel.types._
import io.delta.kernel.types.DataType.COLLATIONS_METADATA_KEY
import org.scalatest.funsuite.AnyFunSuite

import java.util.HashMap
import scala.reflect.ClassTag

class DataTypeJsonSerDeSuite extends AnyFunSuite {

  import DataTypeJsonSerDeSuite._

  private val objectMapper = new ObjectMapper()

  private def parse(json: String): DataType = {
    DataTypeJsonSerDe.parseDataType(objectMapper.readTree(json), "", new HashMap())
  }

  private def serialize(dataType: DataType): String = {
    DataTypeJsonSerDe.serializeDataType(dataType)
  }

  private def testRoundTrip(dataTypeJsonString: String, dataType: DataType): Unit = {
    // test deserialization
    assert(parse(dataTypeJsonString) === dataType)

    // test serialization
    val serializedJson = serialize(dataType)
    assert(parse(serializedJson) === dataType)
  }

  private def checkError[T <: Throwable](json: String, expectedErrorContains: String)
    (implicit classTag: ClassTag[T]): Unit = {
    val e = intercept[T] {
      parse(json)
    }
    assert(e.getMessage.contains(expectedErrorContains))
  }

  /* --------------- Primitive data types (stored as a string) ----------------- */

  Seq(
    ("\"string\"", StringType.STRING),
    ("\"long\"", LongType.LONG),
    ("\"short\"", ShortType.SHORT),
    ("\"integer\"", IntegerType.INTEGER),
    ("\"boolean\"", BooleanType.BOOLEAN),
    ("\"byte\"", ByteType.BYTE),
    ("\"float\"", FloatType.FLOAT),
    ("\"double\"", DoubleType.DOUBLE),
    ("\"binary\"", BinaryType.BINARY),
    ("\"date\"", DateType.DATE),
    ("\"timestamp\"", TimestampType.TIMESTAMP),
    ("\"decimal\"", DecimalType.USER_DEFAULT),
    ("\"decimal(10, 5)\"", new DecimalType(10, 5)),
    ("\"variant\"", VariantType.VARIANT)
  ).foreach { case (json, dataType) =>
    test("serialize/deserialize: " + dataType) {
      testRoundTrip(json, dataType)
    }
  }

  test("parseDataType: invalid primitive string data type") {
    checkError[IllegalArgumentException]("\"foo\"", "foo is not a supported delta data type")
  }

  test("parseDataType: mis-formatted decimal  data type") {
    checkError[IllegalArgumentException](
      "\"decimal(1)\"",
      "decimal(1) is not a supported delta data type")
  }

  /* ---------------  Complex types ----------------- */

  test("serialize/deserialize: array type") {
    for (containsNull <- Seq(true, false)) {
      for ((elementJson, elementType) <- SAMPLE_JSON_TO_TYPES) {
        // test deserialization
        val json = arrayTypeJson(elementJson, containsNull)
        val expectedType = new ArrayType(elementType, containsNull)

        testRoundTrip(json, expectedType)
      }
    }
  }

  test("serialize/deserialize: map type") {
    for (valueContainsNull <- Seq(true, false)) {
      for ((keyJson, keyType) <- SAMPLE_JSON_TO_TYPES) {
        for ((valueJson, valueType) <- SAMPLE_JSON_TO_TYPES) {
          val json = mapTypeJson(keyJson, valueJson, valueContainsNull)
          val expectedType = new MapType(keyType, valueType, valueContainsNull)

          testRoundTrip(json, expectedType)
        }
      }
    }
  }

  test("serialize/deserialize: struct type") {
    for ((col1Json, col1Type) <- SAMPLE_JSON_TO_TYPES) {
      for ((col2Json, col2Type) <- SAMPLE_JSON_TO_TYPES) {
        val fieldsJson = Seq(
          structFieldJson("col1", col1Json, false),
          structFieldJson("col2", col2Json, true, Some("{ \"int\" : 0 }"))
        )

        val json = structTypeJson(fieldsJson)
        val expectedType = new StructType()
          .add("col1", col1Type, false)
          .add("col2", col2Type, true, FieldMetadata.builder().putLong("int", 0).build())

        testRoundTrip(json, expectedType)
      }
    }
  }

  test("parseDataType: types with collated strings") {
    SAMPLE_JSON_TO_TYPES_WITH_COLLATION
      .foreach {
        case(json, structType) =>
          assert(parse(json) == structType)
      }
  }

  test("serialize/deserialize: special characters for column name") {
    val json = structTypeJson(Seq(
      structFieldJson("@_! *c", "\"string\"", true)
    ))
    val expectedType = new StructType()
      .add("@_! *c", StringType.STRING, true)

    testRoundTrip(json, expectedType)
  }

  test("serialize/deserialize: empty struct type") {
    val str =
      """
        |{
        |  "type" : "struct",
        |  "fields": []
        |}
        |""".stripMargin
    testRoundTrip(str, new StructType())
  }

  test("serialize/deserialize: parsing FieldMetadata") {
    def testFieldMetadata(fieldMetadataJson: String, expectedFieldMetadata: FieldMetadata): Unit = {
      val json = structTypeJson(Seq(
        structFieldJson("testCol", "\"string\"", true, Some(fieldMetadataJson))
      ))

      val dataType = new StructType().add("testCol", StringType.STRING, true, expectedFieldMetadata)

      testRoundTrip(json, dataType)
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
        |  "string" : "10\"@",
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
      .putString("string", "10\"@") // special characters
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

object DataTypeJsonSerDeSuite {

  val SAMPLE_JSON_TO_TYPES = Seq(
    ("\"string\"", StringType.STRING),
    ("\"integer\"", IntegerType.INTEGER),
    ("\"variant\"", VariantType.VARIANT),
    (arrayTypeJson("\"string\"", true), new ArrayType(StringType.STRING, true)),
    (mapTypeJson("\"integer\"", "\"string\"", true),
      new MapType(IntegerType.INTEGER, StringType.STRING, true)),
    (structTypeJson(Seq(
      structFieldJson("col1", "\"string\"", true),
      structFieldJson("col2", "\"string\"", false, Some("{ \"int\" : 0 }")),
      structFieldJson("col3", "\"variant\"", false))),
      new StructType()
        .add("col1", StringType.STRING, true)
        .add("col2", StringType.STRING, false, FieldMetadata.builder().putLong("int", 0).build())
        .add("col3", VariantType.VARIANT, false)
    )
  )

  val SAMPLE_JSON_TO_TYPES_WITH_COLLATION = Seq(
    (
      structTypeJson(Seq(
        structFieldJson("a1", "\"string\"", true,
          metadataJson = Some(s"""{"$COLLATIONS_METADATA_KEY" : {"a1" : "ICU.UNICODE"}}""")),
        structFieldJson("a2", "\"integer\"", false),
        structFieldJson("a3", "\"string\"", false,
          metadataJson = Some(s"""{"$COLLATIONS_METADATA_KEY" : {"a3" : "KERNEL.UTF8_LCASE"}}""")),
        structFieldJson("a4", "\"string\"", true))),
      new StructType()
        .add("a1", new StringType("ICU.UNICODE"), true)
        .add("a2", IntegerType.INTEGER, false)
        .add("a3", new StringType("KERNEL.UTF8_LCASE"), false)
        .add("a4", StringType.STRING, true)
    ),
    (
      structTypeJson(Seq(
        structFieldJson("a1", structTypeJson(Seq(
          structFieldJson("b1", "\"string\"", true,
            metadataJson = Some(
              s"""{"$COLLATIONS_METADATA_KEY"
                 | : {"b1" : "ICU.UNICODE"}}""".stripMargin)))), true),
        structFieldJson("a2", structTypeJson(Seq(
          structFieldJson("b1", arrayTypeJson("\"string\"", false), true,
            metadataJson = Some(
              s"""{"$COLLATIONS_METADATA_KEY"
                 | : {"b1.element" : "KERNEL.UTF8_LCASE"}}""".stripMargin)),
          structFieldJson("b2", mapTypeJson("\"string\"", "\"string\"", true), false,
            metadataJson = Some(
              s"""{"$COLLATIONS_METADATA_KEY"
                 | : {"b2.key" : "ICU.UNICODE_CI",
                 |  "b2.value" : "KERNEL.UTF8_LCASE"}}""".stripMargin)),
          structFieldJson("b3", arrayTypeJson("\"string\"", false), true),
          structFieldJson("b4", mapTypeJson("\"string\"", "\"string\"", false), false))), true),
        structFieldJson("a3", structTypeJson(Seq(
          structFieldJson("b1", "\"string\"", false),
          structFieldJson("b2", arrayTypeJson("\"integer\"", false), true))), false,
          metadataJson = Some(
            s"""{"$COLLATIONS_METADATA_KEY"
               | : {"b1" : "KERNEL.UTF8_LCASE"}}""".stripMargin)))),
      new StructType()
        .add("a1", new StructType()
          .add("b1", new StringType("ICU.UNICODE")), true)
        .add("a2", new StructType()
          .add("b1", new ArrayType(new StringType("KERNEL.UTF8_LCASE"), false))
          .add("b2", new MapType(
            new StringType("ICU.UNICODE_CI"), new StringType("KERNEL.UTF8_LCASE"), true), false)
          .add("b3", new ArrayType(StringType.STRING, false))
          .add("b4", new MapType(
            StringType.STRING, StringType.STRING, false), false), true)
        .add("a3", new StructType()
          .add("b1", StringType.STRING, false)
          .add("b2", new ArrayType(IntegerType.INTEGER, false), true), false)
    ),
    (
      structTypeJson(Seq(
        structFieldJson("a1", "\"string\"", true),
        structFieldJson("a2", structTypeJson(Seq(
          structFieldJson("b1", mapTypeJson(
            arrayTypeJson(arrayTypeJson("\"string\"", true), true),
            structTypeJson(Seq(
              structFieldJson("c1", "\"string\"", false,
                metadataJson = Some(
                  s"""{"$COLLATIONS_METADATA_KEY"
                     | : {"c1" : "KERNEL.UTF8_LCASE"}}""".stripMargin)),
              structFieldJson("c2", "\"string\"", true,
                metadataJson = Some(
                  s"""{"$COLLATIONS_METADATA_KEY"
                     | : {\"c1\" : \"ICU.UNICODE\"}}""".stripMargin)),
              structFieldJson("c3", "\"string\"", true))), true), true),
          structFieldJson("b2", "\"long\"", true))), true, metadataJson =
          Some(
            s"""{"$COLLATIONS_METADATA_KEY"
               | : {"b1.key.element.element" : \"KERNEL.UTF8_LCASE\"}}""".stripMargin)),
        structFieldJson("a3", arrayTypeJson(
          mapTypeJson(
            "\"string\"",
            structTypeJson(Seq(
              structFieldJson("b1", "\"string\"", false, metadataJson =
                Some(
                  s"""{"$COLLATIONS_METADATA_KEY"
                     | : {"a3.element.key" : "ICU.UNICODE_CI"}}""".stripMargin)))),
            false), false), true,
          metadataJson = Some(
            s"""{"$COLLATIONS_METADATA_KEY"
               | : {"a3.element.key" : "ICU.UNICODE_CI"}}""".stripMargin)))),
      new StructType()
        .add("a1", StringType.STRING, true)
        .add("a2", new StructType()
          .add("b1", new MapType(
            new ArrayType(
              new ArrayType(
                new StringType("KERNEL.UTF8_LCASE"), true), true),
            new StructType()
              .add("c1", new StringType("KERNEL.UTF8_LCASE"), false)
              .add("c2", new StringType("ICU.UNICODE"), true)
              .add("c3", StringType.STRING), true))
          .add("b2", LongType.LONG), true)
        .add("a3", new ArrayType(
          new MapType(
            new StringType("ICU.UNICODE_CI"),
            new StructType()
              .add("b1", new StringType("KERNEL.UTF8_LCASE"), false), false), false), true)
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
