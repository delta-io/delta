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

package io.delta.kernel.types

import java.util.ArrayList

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.types.StructField.{COLLATIONS_METADATA_KEY, DELTA_TYPE_CHANGES_KEY, FIELD_PATH_KEY, FROM_TYPE_KEY, TO_TYPE_KEY}

import collection.JavaConverters._
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for [[StructField]] class.
 */
class StructFieldSuite extends AnyFunSuite {

  // Test equality and hashcode
  test("equality and hashcode") {
    val field1 = new StructField(
      "field",
      LongType.LONG,
      true,
      FieldMetadata.empty(),
      Seq(new TypeChange(IntegerType.INTEGER, LongType.LONG)).asJava)
    val field2 = new StructField(
      "field",
      LongType.LONG,
      true,
      FieldMetadata.empty(),
      Seq(new TypeChange(IntegerType.INTEGER, LongType.LONG)).asJava)
    val field3 = new StructField("differentField", IntegerType.INTEGER, true)
    val field4 = new StructField("field", StringType.STRING, true)
    val field5 = new StructField("field", IntegerType.INTEGER, false)
    val field6 = new StructField(
      "field",
      IntegerType.INTEGER,
      true,
      FieldMetadata.builder().putBoolean("a", true).build(),
      Seq(new TypeChange(IntegerType.INTEGER, LongType.LONG)).asJava)
    val field7 = new StructField(
      "field",
      LongType.LONG,
      true,
      FieldMetadata.empty(),
      Seq(new TypeChange(IntegerType.INTEGER, StringType.STRING)).asJava)

    assert(field1 == field2)
    assert(field1.hashCode() == field2.hashCode())

    assert(field1 != field3)
    assert(field1 != field4)
    assert(field1 != field5)
    assert(field1 != field6)
    assert(field1 != field7)
  }

  Seq(
    new StructType(),
    new ArrayType(LongType.LONG, false),
    new MapType(LongType.LONG, LongType.LONG, false)).foreach { dataType =>
    test(s"withType should throw exception with change types for nested types $dataType") {
      val field = new StructField(
        "field",
        dataType,
        true)
      assertThrows[KernelException] {
        field.withTypeChanges(Seq(new TypeChange(IntegerType.INTEGER, LongType.LONG)).asJava)
      }
    }

    test(s"Constructor should throw exception with change types for nested types $dataType") {

      assertThrows[KernelException] {
        new StructField(
          "field",
          dataType,
          true,
          FieldMetadata.empty(),
          Seq(new TypeChange(IntegerType.INTEGER, LongType.LONG)).asJava)
      }
    }
  }

  // Test metadata column detection
  test("metadata column detection") {
    val regularField = new StructField("regularField", IntegerType.INTEGER, true)
    assert(!regularField.isMetadataColumn)
    assert(regularField.isDataColumn)

    // Create a metadata field
    val metadataFieldName = "_metadata.custom"
    val metadataBuilder = FieldMetadata.builder()
    metadataBuilder.putBoolean("isMetadataColumn", true)
    val metadataField =
      new StructField(metadataFieldName, LongType.LONG, false, metadataBuilder.build())

    assert(metadataField.isMetadataColumn)
    assert(!metadataField.isDataColumn)
  }

  // Test withNewMetadata method
  test("withNewMetadata") {
    val originalField = new StructField("field", IntegerType.INTEGER, true)
    assert(originalField.getMetadata() == FieldMetadata.empty())

    val newMetadataBuilder = FieldMetadata.builder()
    newMetadataBuilder.putString("key", "value")
    val newMetadata = newMetadataBuilder.build()

    val updatedField = originalField.withNewMetadata(newMetadata)

    assert(updatedField.getName == originalField.getName)
    assert(updatedField.getDataType == originalField.getDataType)
    assert(updatedField.isNullable == originalField.isNullable)
    assert(updatedField.getMetadata == newMetadata)
    assert(updatedField.getMetadata.getString("key") == "value")
  }

  // Test type changes
  test("type changes") {
    val originalField = new StructField(
      "field",
      IntegerType.INTEGER,
      true,
      FieldMetadata.builder().putString("a", "b").build())
    assert(originalField.getTypeChanges.isEmpty)

    val typeChanges = new ArrayList[TypeChange]()
    typeChanges.add(new TypeChange(IntegerType.INTEGER, LongType.LONG))

    val updatedField = originalField.withTypeChanges(typeChanges)

    assert(updatedField.getName == originalField.getName)
    assert(updatedField.getDataType == originalField.getDataType)
    assert(updatedField.isNullable == originalField.isNullable)
    assert(updatedField.getMetadata == FieldMetadata.builder()
      .putString("a", "b")
      .putFieldMetadataArray(
        "delta.typeChanges",
        Array(FieldMetadata.builder()
          .putString("fromType", "integer")
          .putString("toType", "long").build()))
      .build())
    assert(updatedField.getTypeChanges.size() == 1)

    val typeChange = updatedField.getTypeChanges.get(0)
    assert(typeChange.getFrom == IntegerType.INTEGER)
    assert(typeChange.getTo == LongType.LONG)
  }

  // Test TypeChange class
  test("TypeChange class") {
    val from = IntegerType.INTEGER
    val to = LongType.LONG
    val typeChange = new TypeChange(from, to)

    assert(typeChange.getFrom == from)
    assert(typeChange.getTo == to)

    // Test equals and hashCode
    val sameTypeChange = new TypeChange(IntegerType.INTEGER, LongType.LONG)
    val differentTypeChange = new TypeChange(IntegerType.INTEGER, StringType.STRING)

    assert(typeChange == sameTypeChange)
    assert(typeChange.hashCode() == sameTypeChange.hashCode())
    assert(typeChange != differentTypeChange)
  }

  // Sequence of tuples containing StructFields with type changes and their expected FieldMetadata
  Seq(
    // Simple primitive type change: Integer -> Long
    (
      new StructField(
        "intToLongField",
        LongType.LONG,
        true,
        FieldMetadata.empty(),
        Seq(new TypeChange(IntegerType.INTEGER, LongType.LONG)).asJava),
      FieldMetadata.builder()
        .putFieldMetadataArray(
          DELTA_TYPE_CHANGES_KEY,
          Array(
            FieldMetadata.builder()
              .putString(FROM_TYPE_KEY, "integer")
              .putString(TO_TYPE_KEY, "long")
              .build()))
        .build()),

    // Multiple type changes: Integer -> Long -> Decimal
    (
      new StructField(
        "multiTypeChangeField",
        new DecimalType(10, 2),
        true,
        FieldMetadata.empty(),
        Seq(
          new TypeChange(IntegerType.INTEGER, LongType.LONG),
          new TypeChange(LongType.LONG, new DecimalType(10, 2))).asJava),
      FieldMetadata.builder()
        .putFieldMetadataArray(
          DELTA_TYPE_CHANGES_KEY,
          Array(
            FieldMetadata.builder()
              .putString(FROM_TYPE_KEY, "integer")
              .putString(TO_TYPE_KEY, "long")
              .build(),
            FieldMetadata.builder()
              .putString(FROM_TYPE_KEY, "long")
              .putString(TO_TYPE_KEY, "decimal(10,2)")
              .build()))
        .build()),
    // Float -> Double type change with additional metadata
    (
      new StructField(
        "floatToDoubleField",
        DoubleType.DOUBLE,
        true,
        FieldMetadata.builder().putString("description", "A field with type change").build(),
        Seq(new TypeChange(FloatType.FLOAT, DoubleType.DOUBLE)).asJava),
      FieldMetadata.builder()
        .putString("description", "A field with type change")
        .putFieldMetadataArray(
          DELTA_TYPE_CHANGES_KEY,
          Array(
            FieldMetadata.builder()
              .putString(FROM_TYPE_KEY, "float")
              .putString(TO_TYPE_KEY, "double")
              .build()))
        .build()),
    // Type change in array element type
    (
      new StructField(
        "arrayField",
        new ArrayType(new StructField("element", LongType.LONG, true)
          .withTypeChanges(Seq(new TypeChange(ShortType.SHORT, LongType.LONG)).asJava)),
        true),
      FieldMetadata.builder()
        .putFieldMetadataArray(
          DELTA_TYPE_CHANGES_KEY,
          Array(
            FieldMetadata.builder()
              .putString(FROM_TYPE_KEY, "short")
              .putString(TO_TYPE_KEY, "long")
              .putString(FIELD_PATH_KEY, "element")
              .build()))
        .build()),
    // Type change in map value type
    (
      new StructField(
        "mapField",
        new MapType(
          new StructField("key", new StringType("ICU.DE_DE"), false),
          new StructField("value", LongType.LONG, true).withTypeChanges(
            Seq(
              new TypeChange(ShortType.SHORT, IntegerType.INTEGER),
              new TypeChange(IntegerType.INTEGER, LongType.LONG)).asJava)),
        false),
      FieldMetadata.builder()
        .putFieldMetadata(
          COLLATIONS_METADATA_KEY,
          FieldMetadata.builder()
            .putString("mapField.key", "ICU.DE_DE")
            .build())
        .putFieldMetadataArray(
          DELTA_TYPE_CHANGES_KEY,
          Array(
            FieldMetadata.builder()
              .putString(FROM_TYPE_KEY, "short")
              .putString(TO_TYPE_KEY, "integer")
              .putString(FIELD_PATH_KEY, "value")
              .build(),
            FieldMetadata.builder()
              .putString(FROM_TYPE_KEY, "integer")
              .putString(TO_TYPE_KEY, "long")
              .putString(FIELD_PATH_KEY, "value")
              .build()))
        .build()),
    // Complex nested type with multiple type changes
    (
      new StructField(
        "complexField",
        new ArrayType(new StructField(
          "element",
          new MapType(
            new StructField(
              "key",
              new ArrayType(new StructField("element", LongType.LONG, false)
                .withTypeChanges(Seq(new TypeChange(ShortType.SHORT, LongType.LONG)).asJava)),
              false),
            new StructField(
              "value",
              new MapType(
                new StructField("key", ShortType.SHORT, false),
                new StructField("value", LongType.LONG, true)
                  .withTypeChanges(Seq(new TypeChange(ByteType.BYTE, LongType.LONG)).asJava)),
              false)),
          false)),
        false),
      FieldMetadata.builder()
        .putFieldMetadataArray(
          DELTA_TYPE_CHANGES_KEY,
          Array(
            FieldMetadata.builder()
              .putString(FROM_TYPE_KEY, "short")
              .putString(TO_TYPE_KEY, "long")
              .putString(FIELD_PATH_KEY, "element.key.element")
              .build(),
            FieldMetadata.builder()
              .putString(FROM_TYPE_KEY, "byte")
              .putString(TO_TYPE_KEY, "long")
              .putString(FIELD_PATH_KEY, "element.value.value")
              .build()))
        .build())).foreach {

    case (structField, expectedMetadata) =>

      test(s"$structField has expected metadata") {
        assert(structField.getMetadata == expectedMetadata)
      }

      test(s"$structField does not leak field metadata if it is a child struct field.") {
        // Field metadata for type changes is stored at the nearest ancestor of the type
        // sho it shouldn't leak up.
        assert(new StructField("parent", new StructType().add(structField), false).getMetadata ==
          FieldMetadata.empty())
        assert(new StructField(
          "parent",
          new ArrayType(new StructType().add(structField), false),
          false).getMetadata ==
          FieldMetadata.empty())
        assert(new StructField(
          "parent",
          new MapType(new StructType().add(structField), new StructType().add(structField), false),
          false).getMetadata ==
          FieldMetadata.empty())

      }
  }
}
