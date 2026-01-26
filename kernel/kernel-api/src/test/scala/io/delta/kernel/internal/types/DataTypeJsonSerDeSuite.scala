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

import java.util.HashMap

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import io.delta.kernel.types._

import StructField.COLLATIONS_METADATA_KEY
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite

class DataTypeJsonSerDeSuite extends AnyFunSuite {

  import DataTypeJsonSerDeSuite._

  private val objectMapper = new ObjectMapper()

  private def parse(json: String): DataType = {
    DataTypeJsonSerDe.parseDataType(objectMapper.readTree(json))
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

  private def checkError[T <: Throwable](json: String, expectedErrorContains: String)(implicit
      classTag: ClassTag[T]): Unit = {
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
    ("\"variant\"", VariantType.VARIANT)).foreach { case (json, dataType) =>
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
          structFieldJson("col2", col2Json, true, Some("{ \"int\" : 0 }")))

        val json = structTypeJson(fieldsJson)
        val expectedType = new StructType()
          .add("col1", col1Type, false)
          .add("col2", col2Type, true, FieldMetadata.builder().putLong("int", 0).build())

        testRoundTrip(json, expectedType)
      }
    }
  }

  test("serialize/deserialize: complex types") {
    SAMPLE_COMPLEX_JSON_TO_TYPES
      .foreach {
        case (json, dataType) =>
          testRoundTrip(json, dataType)
      }
  }

  test("serialize/deserialize: types with collated strings") {
    SAMPLE_JSON_TO_TYPES_WITH_COLLATION
      .foreach {
        case (json, dataType) =>
          testRoundTrip(json, dataType)
      }
  }

  test("serialize/deserialize: parsed and original struct" +
    " type differing just in StringType collation") {
    val json = structTypeJson(Seq(
      structFieldJson(
        "a1",
        "\"string\"",
        true,
        metadataJson = Some(
          s"""{"$COLLATIONS_METADATA_KEY" :
             | {"a1" : "SPARK.UTF8_LCASE"}}""".stripMargin))))
    val dataType = new StructType()
      .add("a1", new StringType("ICU.UNICODE"), true)

    assert(!(parse(json) === dataType))
  }

  test("serialize/deserialize: round-trip type changes metadata") {
    // Test cases for various type changes based on Delta Protocol specification

    // Case 1: Simple type widening (short -> integer -> long)
    val simpleTypeChangeJson = structTypeJson(Seq(
      structFieldJson(
        "e",
        "\"long\"",
        true,
        metadataJson = Some(
          """{
            |"delta.typeChanges": [
            |  {
            |    "fromType": "short",
            |    "toType": "integer"
            |  },
            |  {
            |    "fromType": "integer",
            |    "toType": "long"
            |  }
            |]
            |}""".stripMargin))))

    val simpleTypeChangeDataType = new StructType()
      .add(new StructField("e", LongType.LONG, true).withTypeChanges(
        Seq(
          new TypeChange(ShortType.SHORT, IntegerType.INTEGER),
          new TypeChange(IntegerType.INTEGER, LongType.LONG)).asJava))

    testRoundTrip(simpleTypeChangeJson, simpleTypeChangeDataType)

    // Case 2: Map key type change (float -> double)
    val mapKeyTypeChangeJson = structTypeJson(Seq(
      structFieldJson(
        "e",
        mapTypeJson("\"double\"", "\"integer\"", true),
        true,
        metadataJson = Some(
          """{
            |"delta.typeChanges": [
            |  {
            |    "fromType": "float",
            |    "toType": "double",
            |    "fieldPath": "key"
            |  }
            |]
            |}""".stripMargin))))

    val mapKeyTypeChangeDataType = new StructType()
      .add(new StructField(
        "e",
        new MapType(
          new StructField("key", DoubleType.DOUBLE, false)
            .withTypeChanges(Seq(new TypeChange(FloatType.FLOAT, DoubleType.DOUBLE)).asJava),
          new StructField("value", IntegerType.INTEGER, true)),
        true))

    testRoundTrip(mapKeyTypeChangeJson, mapKeyTypeChangeDataType)

    // Case 3: Nested map value in array type change (decimal scale change)
    val nestedTypeChangeJson = structTypeJson(Seq(
      structFieldJson(
        "e",
        arrayTypeJson(
          mapTypeJson("\"string\"", "\"decimal(10,4)\"", true),
          true),
        true,
        metadataJson = Some(
          """{
            |"delta.typeChanges": [
            |  {
            |    "fromType": "decimal(6,2)",
            |    "toType": "decimal(10,4)",
            |    "fieldPath": "element.value"
            |  }
            |]
            |}""".stripMargin))))

    val nestedTypeChangeDataType = new StructType()
      .add(
        "e",
        new ArrayType(
          new MapType(
            new StructField("key", StringType.STRING, false),
            new StructField("value", new DecimalType(10, 4), true)
              .withTypeChanges(
                Seq(new TypeChange(new DecimalType(6, 2), new DecimalType(10, 4))).asJava)),
          true),
        true)

    testRoundTrip(nestedTypeChangeJson, nestedTypeChangeDataType)

    // Case 4: Combined type changes and collations
    val combinedJson = structTypeJson(Seq(
      structFieldJson(
        "tags",
        mapTypeJson("\"string\"", "\"string\"", false),
        true,
        metadataJson = Some(
          s"""{
             |"$COLLATIONS_METADATA_KEY": {
             |  "tags.value": "ICU.UNICODE"
             |},
             |"delta.typeChanges": [
             |  {
             |    "fromType": "binary",
             |    "toType": "string",
             |    "fieldPath": "value"
             |  }
             |]
             |}""".stripMargin))))

    val combinedDataType = new StructType()
      .add(
        "tags",
        new MapType(
          new StructField("key", new StringType("SPARK.UTF8_BINARY"), false),
          new StructField("value", new StringType("ICU.UNICODE"), false)
            .withTypeChanges(Seq(new TypeChange(BinaryType.BINARY, StringType.STRING)).asJava)),
        true)

    testRoundTrip(combinedJson, combinedDataType)

    // Case 5: Deeply nested maps
    val deeplyNestedMapJson = structTypeJson(Seq(
      structFieldJson(
        "tags",
        mapTypeJson(
          /* key= */ mapTypeJson("\"integer\"", "\"integer\"", false),
          /* value= */ mapTypeJson("\"long\"", "\"long\"", false),
          false),
        true,
        metadataJson = Some(
          s"""{
             |"delta.typeChanges": [
             |  {
             |    "fromType": "byte",
             |    "toType": "integer",
             |    "fieldPath": "key.key"
             |  },
             |  {
             |    "fromType": "byte",
             |    "toType": "short",
             |    "fieldPath": "key.value"
             |  },
             |  {
             |    "fromType": "short",
             |    "toType": "integer",
             |    "fieldPath": "key.value"
             |  },
             |  {
             |    "fromType": "short",
             |    "toType": "long",
             |    "fieldPath": "value.key"
             |  },
             |  {
             |    "fromType": "byte",
             |    "toType": "long",
             |    "fieldPath": "value.value"
             |  }]
             |}""".stripMargin))))

    val deeplyNestedMapType = new StructType()
      .add(
        "tags",
        new MapType(
          new StructField(
            "key",
            new MapType(
              new StructField("key", IntegerType.INTEGER, false)
                .withTypeChanges(Seq(new TypeChange(ByteType.BYTE, IntegerType.INTEGER)).asJava),
              new StructField("value", IntegerType.INTEGER, false)
                .withTypeChanges(
                  Seq(
                    new TypeChange(ByteType.BYTE, ShortType.SHORT),
                    new TypeChange(ShortType.SHORT, IntegerType.INTEGER)).asJava)),
            false),
          new StructField(
            "value",
            new MapType(
              new StructField("key", LongType.LONG, false)
                .withTypeChanges(Seq(new TypeChange(ShortType.SHORT, LongType.LONG)).asJava),
              new StructField("value", LongType.LONG, false)
                .withTypeChanges(Seq(new TypeChange(ByteType.BYTE, LongType.LONG)).asJava)),
            false)),
        true);

    testRoundTrip(deeplyNestedMapJson, deeplyNestedMapType)
  }

  test("serialize/deserialize: special characters for column name") {
    val json = structTypeJson(Seq(
      structFieldJson("@_! *c", "\"string\"", true)))
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
        structFieldJson("testCol", "\"string\"", true, Some(fieldMetadataJson))))

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
      .putFieldMetadataArray(
        "metadata_arr",
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
      "Could not parse the following JSON as a valid Delta data type")
  }

  test("parseDataType: not a valid JSON node (not a string or object)") {
    checkError[IllegalArgumentException](
      "0",
      "Could not parse the following JSON as a valid Delta data type")
  }
}

object DataTypeJsonSerDeSuite {

  val SAMPLE_JSON_TO_TYPES = Seq(
    ("\"string\"", StringType.STRING),
    ("\"integer\"", IntegerType.INTEGER),
    ("\"variant\"", VariantType.VARIANT),
    (arrayTypeJson("\"string\"", true), new ArrayType(StringType.STRING, true)),
    (
      mapTypeJson("\"integer\"", "\"string\"", true),
      new MapType(IntegerType.INTEGER, StringType.STRING, true)),
    (
      structTypeJson(Seq(
        structFieldJson("col1", "\"string\"", true),
        structFieldJson("col2", "\"string\"", false, Some("{ \"int\" : 0 }")),
        structFieldJson("col3", "\"variant\"", false))),
      new StructType()
        .add("col1", StringType.STRING, true)
        .add("col2", StringType.STRING, false, FieldMetadata.builder().putLong("int", 0).build())
        .add("col3", VariantType.VARIANT, false)))

  val SAMPLE_COMPLEX_JSON_TO_TYPES = Seq(
    (
      structTypeJson(Seq(
        structFieldJson("c1", "\"binary\"", true),
        structFieldJson("c2", "\"boolean\"", false),
        structFieldJson("c3", "\"byte\"", false),
        structFieldJson("c4", "\"date\"", true),
        structFieldJson("c5", "\"decimal(10,0)\"", false),
        structFieldJson("c6", "\"double\"", false),
        structFieldJson("c7", "\"float\"", false),
        structFieldJson("c8", "\"integer\"", true),
        structFieldJson("c9", "\"long\"", true),
        structFieldJson("c10", "\"short\"", true),
        structFieldJson("c11", "\"string\"", true),
        structFieldJson("c12", "\"timestamp_ntz\"", false),
        structFieldJson("c13", "\"timestamp\"", false),
        structFieldJson("c14", "\"variant\"", false))),
      new StructType()
        .add("c1", BinaryType.BINARY, true)
        .add("c2", BooleanType.BOOLEAN, false)
        .add("c3", ByteType.BYTE, false)
        .add("c4", DateType.DATE, true)
        .add("c5", DecimalType.USER_DEFAULT, false)
        .add("c6", DoubleType.DOUBLE, false)
        .add("c7", FloatType.FLOAT, false)
        .add("c8", IntegerType.INTEGER, true)
        .add("c9", LongType.LONG, true)
        .add("c10", ShortType.SHORT, true)
        .add("c11", StringType.STRING, true)
        .add("c12", TimestampNTZType.TIMESTAMP_NTZ, false)
        .add("c13", TimestampType.TIMESTAMP, false)
        .add("c14", VariantType.VARIANT, false)),
    (
      structTypeJson(Seq(
        structFieldJson("a1", "\"string\"", true),
        structFieldJson(
          "a2",
          structTypeJson(Seq(
            structFieldJson(
              "b1",
              mapTypeJson(
                arrayTypeJson(
                  arrayTypeJson(
                    "\"string\"",
                    true),
                  true),
                structTypeJson(Seq(
                  structFieldJson("c1", "\"string\"", false),
                  structFieldJson("c2", "\"string\"", true))),
                true),
              true),
            structFieldJson("b2", "\"long\"", true))),
          true),
        structFieldJson(
          "a3",
          arrayTypeJson(
            mapTypeJson(
              "\"string\"",
              structTypeJson(Seq(
                structFieldJson("b1", "\"date\"", false))),
              false),
            false),
          true))),
      new StructType()
        .add("a1", StringType.STRING, true)
        .add(
          "a2",
          new StructType()
            .add(
              "b1",
              new MapType(
                new ArrayType(
                  new ArrayType(StringType.STRING, true),
                  true),
                new StructType()
                  .add("c1", StringType.STRING, false)
                  .add("c2", StringType.STRING, true),
                true))
            .add("b2", LongType.LONG),
          true)
        .add(
          "a3",
          new ArrayType(
            new MapType(
              StringType.STRING,
              new StructType()
                .add("b1", DateType.DATE, false),
              false),
            false),
          true)))

  val SAMPLE_JSON_TO_TYPES_WITH_COLLATION = Seq(
    (
      structTypeJson(Seq(
        structFieldJson(
          "a1",
          "\"string\"",
          true,
          metadataJson = Some(s"""{"$COLLATIONS_METADATA_KEY" : {"a1" : "ICU.UNICODE"}}""")),
        structFieldJson("a2", "\"integer\"", false),
        structFieldJson(
          "a3",
          "\"string\"",
          false,
          metadataJson = Some(s"""{"$COLLATIONS_METADATA_KEY" : {"a3" : "SPARK.UTF8_LCASE"}}""")),
        structFieldJson("a4", "\"string\"", true))),
      new StructType()
        .add("a1", new StringType("ICU.UNICODE"), true)
        .add("a2", IntegerType.INTEGER, false)
        .add("a3", new StringType("SPARK.UTF8_LCASE"), false)
        .add("a4", StringType.STRING, true)),
    (
      structTypeJson(Seq(
        structFieldJson(
          "a1",
          structTypeJson(Seq(
            structFieldJson(
              "b1",
              "\"string\"",
              true,
              metadataJson = Some(
                s"""{"$COLLATIONS_METADATA_KEY"
                 | : {"b1" : "ICU.UNICODE"}}""".stripMargin)))),
          true),
        structFieldJson(
          "a2",
          structTypeJson(Seq(
            structFieldJson(
              "b1",
              arrayTypeJson("\"string\"", false),
              true,
              metadataJson = Some(
                s"""{"$COLLATIONS_METADATA_KEY"
                 | : {"b1.element" : "SPARK.UTF8_LCASE"}}""".stripMargin)),
            structFieldJson(
              "b2",
              mapTypeJson("\"string\"", "\"string\"", true),
              false,
              metadataJson = Some(
                s"""{"$COLLATIONS_METADATA_KEY" : {"b2.value" : "SPARK.UTF8_LCASE"}}""")),
            structFieldJson("b3", arrayTypeJson("\"string\"", false), true),
            structFieldJson("b4", mapTypeJson("\"string\"", "\"string\"", false), false))),
          true),
        structFieldJson(
          "a3",
          structTypeJson(Seq(
            structFieldJson("b1", "\"string\"", false),
            structFieldJson("b2", arrayTypeJson("\"integer\"", false), true))),
          false))),
      new StructType()
        .add(
          "a1",
          new StructType()
            .add("b1", new StringType("ICU.UNICODE")),
          true)
        .add(
          "a2",
          new StructType()
            .add("b1", new ArrayType(new StringType("SPARK.UTF8_LCASE"), false))
            .add(
              "b2",
              new MapType(
                StringType.STRING,
                new StringType("SPARK.UTF8_LCASE"),
                true),
              false)
            .add("b3", new ArrayType(StringType.STRING, false))
            .add(
              "b4",
              new MapType(
                StringType.STRING,
                StringType.STRING,
                false),
              false),
          true)
        .add(
          "a3",
          new StructType()
            .add("b1", StringType.STRING, false)
            .add("b2", new ArrayType(IntegerType.INTEGER, false), true),
          false)),
    (
      structTypeJson(Seq(
        structFieldJson("a1", "\"string\"", true),
        structFieldJson(
          "a2",
          structTypeJson(Seq(
            structFieldJson(
              "b1",
              mapTypeJson(
                arrayTypeJson(arrayTypeJson("\"string\"", true), true),
                structTypeJson(Seq(
                  structFieldJson(
                    "c1",
                    "\"string\"",
                    false,
                    metadataJson = Some(
                      s"""{"$COLLATIONS_METADATA_KEY"
                     | : {"c1" : "SPARK.UTF8_LCASE"}}""".stripMargin)),
                  structFieldJson(
                    "c2",
                    "\"string\"",
                    true,
                    metadataJson = Some(
                      s"""{"$COLLATIONS_METADATA_KEY"
                     | : {\"c2\" : \"ICU.UNICODE\"}}""".stripMargin)),
                  structFieldJson("c3", "\"string\"", true))),
                true),
              true),
            structFieldJson("b2", "\"long\"", true))),
          true),
        structFieldJson(
          "a3",
          arrayTypeJson(
            mapTypeJson(
              "\"string\"",
              structTypeJson(Seq(
                structFieldJson(
                  "b1",
                  "\"string\"",
                  false,
                  metadataJson = Some(
                    s"""{"$COLLATIONS_METADATA_KEY"
                     | : {"b1" : "SPARK.UTF8_LCASE"}}""".stripMargin)))),
              false),
            false),
          true),
        structFieldJson(
          "a4",
          arrayTypeJson(
            structTypeJson(Seq(
              structFieldJson(
                "b1",
                "\"string\"",
                false,
                metadataJson = Some(
                  s"""{"$COLLATIONS_METADATA_KEY"
                   | : {"b1" : "SPARK.UTF8_LCASE"}}""".stripMargin)))),
            false),
          false),
        structFieldJson(
          "a5",
          mapTypeJson(
            "\"string\"",
            structTypeJson(Seq(
              structFieldJson(
                "b1",
                "\"string\"",
                false,
                metadataJson = Some(
                  s"""{"$COLLATIONS_METADATA_KEY"
                 | : {"b1" : "SPARK.UTF8_LCASE"}}""".stripMargin)))),
            false),
          false))),
      new StructType()
        .add("a1", StringType.STRING, true)
        .add(
          "a2",
          new StructType()
            .add(
              "b1",
              new MapType(
                new ArrayType(
                  new ArrayType(
                    StringType.STRING,
                    true),
                  true),
                new StructType()
                  .add("c1", new StringType("SPARK.UTF8_LCASE"), false)
                  .add("c2", new StringType("ICU.UNICODE"), true)
                  .add("c3", StringType.STRING),
                true))
            .add("b2", LongType.LONG),
          true)
        .add(
          "a3",
          new ArrayType(
            new MapType(
              StringType.STRING,
              new StructType()
                .add("b1", new StringType("SPARK.UTF8_LCASE"), false),
              false),
            false),
          true)
        .add(
          "a4",
          new ArrayType(
            new StructType()
              .add("b1", new StringType("SPARK.UTF8_LCASE"), false),
            false),
          false)
        .add(
          "a5",
          new MapType(
            StringType.STRING,
            new StructType()
              .add("b1", new StringType("SPARK.UTF8_LCASE"), false),
            false),
          false)))

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
