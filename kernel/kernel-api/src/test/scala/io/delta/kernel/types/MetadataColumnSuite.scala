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
package io.delta.kernel.types

import io.delta.kernel.utils.MetadataColumnTestUtils

import org.scalatest.funsuite.AnyFunSuite

class MetadataColumnSuite extends AnyFunSuite with MetadataColumnTestUtils {
  test("add metadata columns to schema") {
    val schema = new StructType()
      .add("number", IntegerType.INTEGER)
      .add("name", StringType.STRING)
      .addMetadataColumn("_metadata.row_index", MetadataColumnSpec.ROW_INDEX)

    // We compare using addMetadataColumn() against manually adding the expected metadata columns
    // as provided by MetadataColumnTestUtils
    val expected = new StructType()
      .add("number", IntegerType.INTEGER)
      .add("name", StringType.STRING)
      .add(ROW_INDEX)

    assert(schema.equals(expected))
  }

  test("fail if metadata column already exists in schema") {
    val schema = new StructType()
      .add("number", IntegerType.INTEGER)
      .add("name", StringType.STRING)
      .addMetadataColumn("_metadata.row_index", MetadataColumnSpec.ROW_INDEX)

    // Adding the same metadata column should fail
    val e = intercept[IllegalArgumentException] {
      schema.addMetadataColumn("some other name", MetadataColumnSpec.ROW_INDEX)
    }
    assert(e.getMessage.contains("Duplicate metadata column row_index found in struct type"))

    // Adding a different metadata column should not fail
    val updated = schema.addMetadataColumn("_metadata.row_id", MetadataColumnSpec.ROW_ID)
    val expected = new StructType()
      .add("number", IntegerType.INTEGER)
      .add("name", StringType.STRING)
      .add(ROW_INDEX)
      .add(ROW_ID)
    assert(updated.equals(expected))
  }

  test("fail if metadata column is nested") {
    val schema = new StructType()

    // Adding a nested metadata column should fail
    val e1 = intercept[IllegalArgumentException] {
      schema.add(new StructField(
        "struct",
        new StructType().addMetadataColumn("row_index", MetadataColumnSpec.ROW_INDEX),
        false))
    }
    assert(
      e1.getMessage.contains("Metadata columns are only allowed at the top level of a schema."))

    // Verify two-level nesting fails
    val e2 = intercept[IllegalArgumentException] {
      schema.add(new StructField(
        "struct",
        new StructType().add(new StructField(
          "inner_struct",
          new StructType().addMetadataColumn("row_index", MetadataColumnSpec.ROW_INDEX),
          false)),
        false))
    }
    assert(
      e2.getMessage.contains("Metadata columns are only allowed at the top level of a schema."))

    // Verify metadata in map type fails
    val e3 = intercept[IllegalArgumentException] {
      schema.add(new StructField(
        "map",
        new MapType(
          StringType.STRING,
          new StructType().addMetadataColumn("row_index", MetadataColumnSpec.ROW_INDEX),
          false),
        false))
    }
    assert(
      e3.getMessage.contains("Metadata columns are only allowed at the top level of a schema."))

    // Verify metadata in array type fails
    val e4 = intercept[IllegalArgumentException] {
      schema.add(new StructField(
        "array",
        new ArrayType(
          new StructType().addMetadataColumn("row_index", MetadataColumnSpec.ROW_INDEX),
          false),
        false))
    }
    assert(
      e4.getMessage.contains("Metadata columns are only allowed at the top level of a schema."))
  }
}
