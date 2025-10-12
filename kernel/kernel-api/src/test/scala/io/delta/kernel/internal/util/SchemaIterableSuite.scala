/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class SchemaIterableSuite extends AnyFunSuite {
  test("depth first traversal works with deeply nested types") {
    val schema: StructType = getDeeplyNestedSchema

    val iterable = new SchemaIterable(schema)

    // Track the path stack during traversal
    val fieldInfo = iterable.asScala.map {
      element =>
        (
          element.getNamePath(),
          element.getPathFromNearestStructFieldAncestor(
            element.getNearestStructFieldAncestor.getName),
          element.getPathFromNearestStructFieldAncestor(""),
          element.getField.getDataType.getClass.getSimpleName)
    }
      .toList

    // The expected traversal order with field types, showing the complete depth-first traversal
    val expectedOrder = List(
      // First branch: nested_array
      ("nested_array.element.id", "id", "", "IntegerType"),
      ("nested_array.element.tags.element", "tags.element", "element", "StringType"),
      ("nested_array.element.tags", "tags", "", "ArrayType"),
      ("nested_array.element", "nested_array.element", "element", "StructType"),
      ("nested_array", "nested_array", "", "ArrayType"),

      // Second branch: nested_map
      ("nested_map.key.element", "nested_map.key.element", "key.element", "StringType"),
      ("nested_map.key", "nested_map.key", "key", "ArrayType"),
      ("nested_map.value.points.element.x", "x", "", "DoubleType"),
      ("nested_map.value.points.element.y", "y", "", "DoubleType"),
      ("nested_map.value.points.element", "points.element", "element", "StructType"),
      ("nested_map.value.points", "points", "", "ArrayType"),
      ("nested_map.value.metadata.key", "metadata.key", "key", "StringType"),
      ("nested_map.value.metadata.value", "metadata.value", "value", "IntegerType"),
      ("nested_map.value.metadata", "metadata", "", "MapType"),
      ("nested_map.value", "nested_map.value", "value", "StructType"),
      ("nested_map", "nested_map", "", "MapType"),
      // Third branch
      (
        "double_nested.element.element.key.key.element",
        "double_nested.element.element.key.key.element",
        "element.element.key.key.element",
        "IntegerType"),
      (
        "double_nested.element.element.key.key",
        "double_nested.element.element.key.key",
        "element.element.key.key",
        "ArrayType"),
      (
        "double_nested.element.element.key.value",
        "double_nested.element.element.key.value",
        "element.element.key.value",
        "StringType"),
      (
        "double_nested.element.element.key",
        "double_nested.element.element.key",
        "element.element.key",
        "MapType"),
      (
        "double_nested.element.element.value.key",
        "double_nested.element.element.value.key",
        "element.element.value.key",
        "StringType"),
      (
        "double_nested.element.element.value.value",
        "double_nested.element.element.value.value",
        "element.element.value.value",
        "StringType"),
      (
        "double_nested.element.element.value",
        "double_nested.element.element.value",
        "element.element.value",
        "MapType"),
      (
        "double_nested.element.element",
        "double_nested.element.element",
        "element.element",
        "MapType"),
      ("double_nested.element", "double_nested.element", "element", "ArrayType"),
      ("double_nested", "double_nested", "", "ArrayType"),
      // fourth branch
      ("empty_struct", "empty_struct", "", "StructType"),
      // fifth branch
      ("empty_struct_array.element", "empty_struct_array.element", "element", "StructType"),
      ("empty_struct_array", "empty_struct_array", "", "ArrayType"),
      // sixth branch
      ("empty_map_struct.key", "empty_map_struct.key", "key", "StructType"),
      ("empty_map_struct.value", "empty_map_struct.value", "value", "StructType"),
      ("empty_map_struct", "empty_map_struct", "", "MapType"))

    fieldInfo.zip(expectedOrder).foreach {
      case (actual, expected) => assert(actual == expected)
    }
    assert(fieldInfo == expectedOrder)

  }

  Seq(
    (new StructType(), List()),
    (new StructType().add("empty", new StructType()), List("empty")),
    (
      new StructType().add("f1", new StructType().add("f2", IntegerType.INTEGER)),
      List("f1.f2", "f1")),
    (
      new StructType().add("f1", IntegerType.INTEGER).add("f2", IntegerType.INTEGER),
      List("f1", "f2")),
    (
      new StructType()
        .add("f1", IntegerType.INTEGER)
        .add(
          "s1",
          new StructType()
            .add("f1", IntegerType.INTEGER)
            .add("f2", IntegerType.INTEGER))
        .add("s2", new StructType())
        .add("f2", IntegerType.INTEGER),
      List("f1", "s1.f1", "s1.f2", "s1", "s2", "f2"))).foreach {
    case (schema, expected) =>
      test(s"check basic iteration ${schema.toString}") {
        val iterable = new SchemaIterable(schema)
        val fieldInfo = iterable.asScala.map {
          field => (field.getNamePath)
        }
          .toList
        assert(fieldInfo === expected)
      }
  }

  test("test update schema") {

    val schema: StructType = getDeeplyNestedSchema

    val iterable = new SchemaIterable(schema)

    val fieldMetadata = FieldMetadata.builder()
      .putString("k1", "v1")
      .build()
    val newTypes = Map(
      "nested_array.element.tags.element" -> IntegerType.INTEGER,
      "nested_map.value.metadata.value" -> StringType.STRING,
      "nested_map.value.points.element" -> new StructType().add(
        "x",
        DoubleType.DOUBLE,
        false).add("y", DoubleType.DOUBLE, false)
        .add("z", LongType.LONG, false))
    val newMetadata = Map("nested_array" -> fieldMetadata)

    iterable.newMutableIterator().asScala.foreach {
      element =>
        newTypes.get(element.getNamePath).foreach {
          t => element.updateField(element.getField.withDataType(t))
        }
        newMetadata.get(element.getNamePath).foreach {
          fm => element.updateField(element.getField.withNewMetadata(fm))
        }
    }

    val fieldInfo = iterable.asScala.map {
      element => (element.getNamePath, element.getField.getDataType.getClass.getSimpleName)
    }.toList

    // The expected traversal order with field types, showing the complete depth-first traversal
    val expectedOrder = List(
      // First branch: nested_array
      ("nested_array.element.id", "IntegerType"),
      ("nested_array.element.tags.element", "IntegerType"),
      ("nested_array.element.tags", "ArrayType"),
      ("nested_array.element", "StructType"),
      ("nested_array", "ArrayType"),

      // Second branch: nested_map
      ("nested_map.key.element", "StringType"),
      ("nested_map.key", "ArrayType"),
      ("nested_map.value.points.element.x", "DoubleType"),
      ("nested_map.value.points.element.y", "DoubleType"),
      ("nested_map.value.points.element.z", "LongType"),
      ("nested_map.value.points.element", "StructType"),
      ("nested_map.value.points", "ArrayType"),
      ("nested_map.value.metadata.key", "StringType"),
      ("nested_map.value.metadata.value", "StringType"),
      ("nested_map.value.metadata", "MapType"),
      ("nested_map.value", "StructType"),
      ("nested_map", "MapType"),
      // Third branch
      ("double_nested.element.element.key.key.element", "IntegerType"),
      ("double_nested.element.element.key.key", "ArrayType"),
      ("double_nested.element.element.key.value", "StringType"),
      ("double_nested.element.element.key", "MapType"),
      ("double_nested.element.element.value.key", "StringType"),
      ("double_nested.element.element.value.value", "StringType"),
      ("double_nested.element.element.value", "MapType"),
      ("double_nested.element.element", "MapType"),
      ("double_nested.element", "ArrayType"),
      ("double_nested", "ArrayType"),
      // fourth branch
      ("empty_struct", "StructType"),
      // fifth branch
      ("empty_struct_array.element", "StructType"),
      ("empty_struct_array", "ArrayType"),
      // sixth branch
      ("empty_map_struct.key", "StructType"),
      ("empty_map_struct.value", "StructType"),
      ("empty_map_struct", "MapType"))

    fieldInfo.zip(expectedOrder).foreach {
      case (actual, expected) => assert(actual == expected)
    }
    assert(iterable.getSchema.get("nested_array").getMetadata == fieldMetadata)

  }

  test("test set nearest ancestor field metadata") {

    val schema: StructType = getDeeplyNestedSchema

    val iterable = new SchemaIterable(schema)

    val newMetadata = Map(
      "nested_array.element.tags.element" -> newFieldMetadata("v1"),
      "nested_map.value.metadata" -> newFieldMetadata("v2"),
      "nested_map.value.metadata.value" -> newFieldMetadata("v3"),
      "nested_array" -> newFieldMetadata("v4"))

    val expected = Map(
      "nested_array.element.tags" -> newFieldMetadata("v1"),
      "nested_array.element.tags.element" -> FieldMetadata.empty(),
      "nested_map.value.metadata" ->
        FieldMetadata.builder
          .fromMetadata(newFieldMetadata("v2"))
          .fromMetadata(newFieldMetadata("v3")).build(),
      "nested_map.value.metadata.value" -> FieldMetadata.empty(),
      "nested_array" -> newFieldMetadata("v4"))

    val originalCount = iterable.asScala.count(_ => true)

    iterable.newMutableIterator().asScala.foreach {
      element =>
        newMetadata.get(element.getNamePath).foreach { fm =>
          val ancestorField = element.getNearestStructFieldAncestor
          val metadataBuilder = FieldMetadata.builder()
            .fromMetadata(ancestorField.getMetadata).fromMetadata(fm)
          element.setMetadataOnNearestStructFieldAncestor(metadataBuilder.build())
        }
    }

    iterable.asScala.foreach {
      element =>
        expected.get(element.getNamePath).foreach {
          fm =>
            assert(
              fm ==
                element.getField.getMetadata,
              s"Path: ${element.getNamePath}  ${iterable.getSchema} ")
        }
    }
    val newCount = iterable.asScala.count(_ => true)
    assert(newCount > 0)
    assert(originalCount == newCount)

  }

  val testCases = Seq(
    // Test case 1: Skip ArrayType
    (
      Seq(classOf[ArrayType]),
      List(
        "nested_array",
        "nested_map.key",
        "nested_map.value.points",
        "nested_map.value.metadata.key",
        "nested_map.value.metadata.value",
        "nested_map.value.metadata",
        "nested_map.value",
        "nested_map",
        "double_nested",
        "empty_struct",
        "empty_struct_array",
        "empty_map_struct.key",
        "empty_map_struct.value",
        "empty_map_struct")),

    // Test case 2: Skip MapType
    (
      Seq(classOf[MapType]),
      List(
        "nested_array.element.id",
        "nested_array.element.tags.element",
        "nested_array.element.tags",
        "nested_array.element",
        "nested_array",
        "nested_map",
        "double_nested.element.element",
        "double_nested.element",
        "double_nested",
        "empty_struct",
        "empty_struct_array.element",
        "empty_struct_array",
        "empty_map_struct")),

    // Test case 3: Skip StructType
    (
      Seq(classOf[StructType]),
      List(
        "nested_array.element",
        "nested_array",
        "nested_map.key.element",
        "nested_map.key",
        "nested_map.value",
        "nested_map",
        "double_nested.element.element.key.key.element",
        "double_nested.element.element.key.key",
        "double_nested.element.element.key.value",
        "double_nested.element.element.key",
        "double_nested.element.element.value.key",
        "double_nested.element.element.value.value",
        "double_nested.element.element.value",
        "double_nested.element.element",
        "double_nested.element",
        "double_nested",
        "empty_struct",
        "empty_struct_array.element",
        "empty_struct_array",
        "empty_map_struct.key",
        "empty_map_struct.value",
        "empty_map_struct")),
    // Test case 4: Skip multiple types (ArrayType and MapType)
    (
      Seq(classOf[ArrayType], classOf[MapType]),
      List(
        "nested_array",
        "nested_map",
        "double_nested",
        "empty_struct",
        "empty_struct_array",
        "empty_map_struct"))).foreach { case (typesToSkip, expectedFields) =>
    test(s"skip recursion for specified types $typesToSkip") {
      val schema: StructType = getDeeplyNestedSchema
      // Define test cases as a sequence of (types to skip, expected output) pairs

      val iterable =
        SchemaIterable.newSchemaIterableWithIgnoredRecursion(schema, typesToSkip.toArray)
      val visitedPaths = iterable.asScala.map(_.getNamePath).toList

      // Assert the results match expected output
      assert(
        visitedPaths === expectedFields,
        s"Failed for types: ${typesToSkip.map(_.getSimpleName).mkString(", ")}")
    }
  }

  test("update schema with type recursion skipping") {
    val schema: StructType = getDeeplyNestedSchema

    val iterable =
      SchemaIterable.newSchemaIterableWithIgnoredRecursion(schema, Array(classOf[ArrayType]))

    // Create a modified schema by skipping recursion into ArrayType
    // and modifying only the top-level fields
    iterable.newMutableIterator.asScala.foreach { element =>
      if (element.getNamePath.contains("nested_array")) {
        // Add metadata to the array field but don't recurse into it
        val fieldMetadata = FieldMetadata.builder()
          .putString("array_skipped", "true")
          .build()
        element.updateField(element.getField.withNewMetadata(fieldMetadata))
      }
    }

    // Verify the metadata was added to the array field
    assert(iterable.getSchema.get("nested_array").getMetadata
      .getString("array_skipped") == "true")

    // Verify that the array elements were not modified (recursion was skipped)
    val newIterable = new SchemaIterable(iterable.getSchema)
    var visited_count = 0
    newIterable.asScala.foreach(element => {
      if (element.getNamePath.startsWith("nested_array.")) {
        visited_count += 1
        assert(element.getField.getMetadata == FieldMetadata.empty())
      }
    })
    assert(visited_count > 0)

  }

  private def newFieldMetadata(v: String) = FieldMetadata.builder().putString(v, v).build()

  private def getDeeplyNestedSchema = {
    val intType = IntegerType.INTEGER
    val stringType = StringType.STRING
    val doubleType = DoubleType.DOUBLE

    // Create a deeply nested schema:
    // struct<
    //   nested_array: array<
    //     struct<
    //       id: int,
    //       tags: array<string>
    //     >
    //   >,
    //   nested_map: map<
    //     array<string>,
    //     struct<
    //       points: array<
    //         struct<x: double, y: double>
    //       >,
    //       metadata: map<string, int>
    //     >
    //   >
    //   double_nested:
    //     array<array<map<map<array<int>, string>, map<string, string>>>
    //   empty_struct: struct<>
    //   empty_struct_array:
    //     array<struct<>>>
    //   empty_map_struct:
    //    map<struct<>, struct<>>
    // >

    // Define the point struct inside the array
    val pointStruct = new StructType().add("x", doubleType).add("y", doubleType);

    // Define the inner struct containing tags array
    val innerStruct = new StructType().add("id", intType).add(
      "tags",
      new ArrayType(stringType, true));

    // Define the value struct for the nested map
    val valueStruct = new StructType().add("points", new ArrayType(pointStruct, false)).add(
      "metadata",
      new MapType(
        stringType,
        intType,
        /* valuesContainsNull = */ false));

    // Create the root schema
    val schema = new StructType().add("nested_array", new ArrayType(innerStruct, false))
      .add("nested_map", new MapType(new ArrayType(stringType, false), valueStruct, true))
      .add(
        "double_nested",
        new ArrayType(
          new ArrayType(
            new MapType(
              new MapType(new ArrayType(IntegerType.INTEGER, true), StringType.STRING, true),
              new MapType(StringType.STRING, StringType.STRING, true),
              true),
            false),
          false),
        false)
      .add("empty_struct", new StructType(), false)
      .add("empty_struct_array", new ArrayType(new StructType(), true), false)
      .add("empty_map_struct", new MapType(new StructType(), new StructType(), true), false)
    schema
  }
}
