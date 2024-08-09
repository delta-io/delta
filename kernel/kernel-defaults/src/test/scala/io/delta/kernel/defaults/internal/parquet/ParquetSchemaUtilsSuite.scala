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
package io.delta.kernel.defaults.internal.parquet

import io.delta.kernel.defaults.internal.parquet.ParquetSchemaUtils.pruneSchema
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.internal.util.ColumnMapping
import io.delta.kernel.internal.util.ColumnMapping.PARQUET_FIELD_NESTED_IDS_METADATA_KEY
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.LongType.LONG
import io.delta.kernel.types.{ArrayType, DoubleType, FieldMetadata, MapType, StructType}
import org.apache.parquet.schema.MessageTypeParser
import org.scalatest.funsuite.AnyFunSuite

class ParquetSchemaUtilsSuite extends AnyFunSuite with TestUtils {
  // Test parquet schema type containing different types of columns with field ids
  private val testParquetFileSchema = MessageTypeParser.parseMessageType(
    """message fileSchema {
      |  required group f0 = 1 {
      |    optional int32 f00 = 2;
      |    optional int64 f01 = 3;
      |  }
      |  optional group f1 = 4 {
      |    repeated group list = 5 {
      |      optional int32 element = 6;
      |    }
      |  }
      |  required group f2 (MAP) = 7 {
      |    repeated group key_value = 8 {
      |      required group key = 9 {
      |        required int32 key_f0 = 10;
      |        required int64 key_f1 = 11;
      |      }
      |      required int32 value = 12;
      |    }
      |  }
      |  optional double f3 = 13;
      |}
      """.stripMargin)

  // Delta schema corresponding to the above test [[parquetSchema]]
  private val testParquetFileDeltaSchema = new StructType()
    .add("f0",
      new StructType()
        .add("f00", INTEGER, fieldMetadata(2))
        .add("f01", LONG, fieldMetadata(3)),
      fieldMetadata(1))
    .add("f1", new ArrayType(INTEGER, false), fieldMetadata(4))
    .add(
      "f2",
      new MapType(
        new StructType()
          .add("key_f0", INTEGER, fieldMetadata(10))
          .add("key_f1", INTEGER, fieldMetadata(11)),
        INTEGER,
        false
      ),
      fieldMetadata(7))
    .add("f3", DoubleType.DOUBLE, fieldMetadata(13))


  test("id mapping mode - delta reads all columns in the parquet file") {
    val prunedParquetSchema = pruneSchema(testParquetFileSchema, testParquetFileDeltaSchema)
    assert(prunedParquetSchema === testParquetFileSchema)
  }

  test("id mapping mode - delta selects a subset of columns in the parquet file") {
    val readDeltaSchema = new StructType()
      .add(testParquetFileDeltaSchema.get("f1"))
      .add( // nested column pruning
        "f0",
        new StructType()
          .add("f00", INTEGER, fieldMetadata(2)),
        fieldMetadata(1)
      )

    val expectedParquetSchema = MessageTypeParser.parseMessageType(
      """message fileSchema {
        |  optional group f1 = 4 {
        |    repeated group list = 5 {
        |      optional int32 element = 6;
        |    }
        |  }
        |  required group f0 = 1 {
        |    optional int32 f00 = 2;
        |  }
        |}
        """.stripMargin)

    val prunedParquetSchema = pruneSchema(testParquetFileSchema, readDeltaSchema)
    assert(prunedParquetSchema === expectedParquetSchema)
  }

  test("id mapping mode - delta tries to read a column not present in the parquet file") {
    val readDeltaSchema = new StructType()
      .add(testParquetFileDeltaSchema.get("f1"))
      .add( // nested column has extra column that is not present in the file
        "f0",
        new StructType()
          .add("f00", INTEGER, fieldMetadata(2))
          .add("f02", INTEGER, fieldMetadata(15)),
        fieldMetadata(1)
      )
      .add("f4", INTEGER, fieldMetadata(14))

    // pruned parquet file schema shouldn't have the column "f4"
    val expectedParquetSchema = MessageTypeParser.parseMessageType(
      """message fileSchema {
        |  optional group f1 = 4 {
        |    repeated group list = 5 {
        |      optional int32 element = 6;
        |    }
        |  }
        |  required group f0 = 1 {
        |    optional int32 f00 = 2;
        |  }
        |}
        """.stripMargin)

    val prunedParquetSchema = pruneSchema(testParquetFileSchema, readDeltaSchema)
    assert(prunedParquetSchema === expectedParquetSchema)
  }

  test("id mapping mode - combination of columns with and w/o field ids in delta read schema") {
    val readDeltaSchema = new StructType()
      .add(testParquetFileDeltaSchema.get("f1")) // with field id
      .add( // nested column has extra column that is not present in the file
        "f0",
        new StructType()
          .add("F00", INTEGER) // no field id and with case-insensitive column name
          .add("f01", INTEGER, fieldMetadata(3))
        // no field id for struct f0
      )

    val expectedParquetSchema = MessageTypeParser.parseMessageType(
      """message fileSchema {
        |  optional group f1 = 4 {
        |    repeated group list = 5 {
        |      optional int32 element = 6;
        |    }
        |  }
        |  required group f0 = 1 {
        |    optional int32 f00 = 2;
        |    optional int64 f01 = 3;
        |  }
        |}
        """.stripMargin)

    val prunedParquetSchema = pruneSchema(testParquetFileSchema, readDeltaSchema)
    assert(prunedParquetSchema === expectedParquetSchema)
  }

  test("id mapping mode - field id matches but not the column name") {
    val readDeltaSchema = new StructType()
       // physical name in the file is f3, but the same field id
      .add("f3_new", DoubleType.DOUBLE, fieldMetadata(13))
      .add(
        "f0",
        new StructType()
          // physical name in the file is f00, but the same field id
          .add("f00_new", INTEGER, fieldMetadata(2)),
        fieldMetadata(1)
      )

    val expectedParquetSchema = MessageTypeParser.parseMessageType(
      """message fileSchema {
        |  optional double f3 = 13;
        |  required group f0 = 1 {
        |    optional int32 f00 = 2;
        |  }
        |}
        """.stripMargin)

    val prunedParquetSchema = pruneSchema(testParquetFileSchema, readDeltaSchema)
    assert(prunedParquetSchema === expectedParquetSchema)
  }

  test("id mapping mode - duplicate id in file at the same level throws error") {
    val readDeltaSchema = new StructType()
      .add("f3", DoubleType.DOUBLE, fieldMetadata(13))

    val testParquetFileSchema = MessageTypeParser.parseMessageType(
      """message fileSchema {
        |  optional double f3 = 13;
        |  optional double f4 = 13;
        |}
        """.stripMargin)

    val ex = intercept[Exception] {
      pruneSchema(testParquetFileSchema, readDeltaSchema)
    }
    assert(ex.getMessage.contains(
      "Parquet file contains multiple columns (optional double f3 = 13, " +
        "optional double f4 = 13) with the same field id"))
  }

  test("id mapping mode - duplicate id in file at the same nested level throws error") {
    val readDeltaSchema = new StructType()
      .add(
        "f0",
        new StructType()
          .add("f00", INTEGER, fieldMetadata(2)),
        fieldMetadata(1)
      )

    val testParquetFileSchema = MessageTypeParser.parseMessageType(
      """message fileSchema {
        |  required group f0 = 1 {
        |    optional int32 f00 = 2;
        |    optional int64 f01 = 3;
        |    optional int64 f02 = 2;
        |  }
        |}
        """.stripMargin)

    val ex = intercept[Exception] {
      pruneSchema(testParquetFileSchema, readDeltaSchema)
    }
    assert(ex.getMessage.contains(
      "Parquet file contains multiple columns (optional int32 f00 = 2, " +
        "optional int64 f02 = 2) with the same field id"))
  }

  // icebergCompatV2 tests - nested field ids are converted correctly to parquet schema
  Seq(
    (
      "struct with array and map",
      // Delta schema - input
      new StructType()
        .add("f0",
          new StructType()
            .add("f00", new ArrayType(LONG, false), fieldMetadata(2, ("f00.element", 3)))
            .add("f01", new MapType(INTEGER, INTEGER, true),
              fieldMetadata(4, ("f01.key", 5), ("f01.value", 6))),
          fieldMetadata(1)),
      // Expected parquet schema
      MessageTypeParser.parseMessageType(
        """message DefaultKernelSchema {
          |  optional group f0 = 1 {
          |    optional group f00 (LIST) = 2 {
          |      repeated group list {
          |        required int64 element = 3;
          |      }
          |    }
          |    optional group f01 (MAP) = 4 {
          |      repeated group key_value {
          |        required int32 key = 5;
          |        optional int32 value = 6;
          |      }
          |    }
          |  }
          |}""".stripMargin)
    ),
    (
      "top-level array and map columns",
      // Delta schema - input
      new StructType()
        .add("f1",
          new ArrayType(INTEGER, true),
          fieldMetadata(1, ("f1.element", 2)))
        .add("f2",
          new MapType(
            new StructType()
              .add("key_f0", INTEGER, fieldMetadata(6))
              .add("key_f1", INTEGER, fieldMetadata(7)),
            INTEGER,
            true
          ),
          fieldMetadata(3, ("f2.key", 4), ("f2.value", 5))),
      // Expected parquet schema
      MessageTypeParser.parseMessageType("""message DefaultKernelSchema {
        |  optional group f1 (LIST) = 1 {
        |    repeated group list {
        |      optional int32 element = 2;
        |    }
        |  }
        |  optional group f2 (MAP) = 3 {
        |    repeated group key_value {
        |      required group key = 4 {
        |        optional int32 key_f0 = 6;
        |        optional int32 key_f1 = 7;
        |      }
        |      optional int32 value = 5;
        |    }
        |  }
        |}""".stripMargin)
    ),
    (
      "array/map inside array/map",
      // Delta schema - input
      new StructType()
        .add("f3",
          new ArrayType(new ArrayType(INTEGER, false), false),
          fieldMetadata(0, ("f3.element", 1), ("f3.element.element", 2)))
        .add("f4",
          new MapType(
            new MapType(
              new StructType()
                .add("key_f0", INTEGER, fieldMetadata(3))
                .add("key_f1", INTEGER, fieldMetadata(4)),
              INTEGER,
              false
            ),
            INTEGER,
            false
          ),
          fieldMetadata(5,
            ("f4.key", 6), ("f4.value", 7), ("f4.key.key", 8), ("f4.key.value", 9))),
      // Expected parquet schema
      MessageTypeParser.parseMessageType("""message DefaultKernelSchema {
        |  optional group f3 (LIST) = 0 {
        |    repeated group list {
        |      required group element (LIST) = 1 {
        |        repeated group list {
        |          required int32 element = 2;
        |        }
        |      }
        |    }
        |  }
        |  optional group f4 (MAP) = 5 {
        |    repeated group key_value {
        |      required group key (MAP) = 6 {
        |        repeated group key_value {
        |          required group key = 8 {
        |            optional int32 key_f0 = 3;
        |            optional int32 key_f1 = 4;
        |          }
        |          required int32 value = 9;
        |        }
        |      }
        |      required int32 value = 7;
        |    }
        |  }
        |}""".stripMargin)
    )
  ).foreach { case (testName, deltaSchema, expectedParquetSchema) =>
    test(s"icebergCompatV2 - nested fields are converted to parquet schema - $testName") {
      val actParquetSchema = ParquetSchemaUtils.toParquetSchema(deltaSchema)
      assert(actParquetSchema === expectedParquetSchema)
    }
  }

  Seq(
    (
      "field id validation: no negative field id",
      // Delta schema - input
      new StructType()
        .add("f0",
          new StructType()
            .add("f00", new ArrayType(LONG, false), fieldMetadata(-1))
            .add("f01", new MapType(INTEGER, INTEGER, true), fieldMetadata(4)),
          fieldMetadata(1)),
      // Expected error message
      "Field id should be non-negative."
    ),
    (
      "field id validation: no negative nested field id",
      // Delta schema - input
      new StructType()
        .add("f0",
          new StructType()
            .add("f00", new ArrayType(LONG, false), fieldMetadata(1, ("f00.element", -1)))
            .add("f01", new MapType(INTEGER, INTEGER, true), fieldMetadata(4)),
          fieldMetadata(0)),
      // Expected error message
      "Field id should be non-negative."
    ),
    (
      "field id validation: no duplicate field id",
      // Delta schema - input
      new StructType()
        .add("f0",
          new StructType()
            .add("f00", new ArrayType(LONG, false), fieldMetadata(1, ("f00.element", 1)))
            .add("f01", new MapType(INTEGER, INTEGER, true), fieldMetadata(4)),
          fieldMetadata(1)),
      // Expected error message
      "Field id should be unique."
    ),
    (
      "field id validation: no duplicate nested field id",
      // Delta schema - input
      new StructType()
        .add("f0",
          new StructType()
            .add("f00", new ArrayType(LONG, false), fieldMetadata(1, ("f00.element", 2)))
            .add("f01", new MapType(INTEGER, INTEGER, true), fieldMetadata(2)),
          fieldMetadata(1)),
      // Expected error message
      "Field id should be unique."
    ),
    (
      "field id validation: missing field ids",
      // Delta schema - input
      new StructType()
        .add("f0",
          new StructType()
            .add("f00", new ArrayType(LONG, false))
            .add("f01", new MapType(INTEGER, INTEGER, true), fieldMetadata(4)),
          fieldMetadata(1)),
      // Expected error message
      "Some of the fields are missing field ids."
    ),
    (
      "field id validation: missing nested field ids",
      // Delta schema - input
      new StructType()
        .add("f0",
          new StructType()
            .add("f00", new ArrayType(LONG, false), fieldMetadata(1, ("f00.element", 2)))
            .add("f01", new MapType(INTEGER, INTEGER, true), fieldMetadata(4)), // missing nested id
          fieldMetadata(0)),
      // Expected error message
      "Some of the fields are missing field ids."
    ),
    (
      "field id validation: missing field ids but have nested fields",
      // Delta schema - input
      new StructType()
        .add("f0",
          new StructType()
            .add("f00", new ArrayType(LONG, false), fieldMetadata(1, ("f00.element", 2)))
            .add("f01", new MapType(INTEGER, INTEGER, true), fieldMetadata(4, ("f01.key", 5)))
        ), // missing field id for f0
      // Expected error message
      "Some of the fields are missing field ids."
    )
  ).foreach { case (testName, deltaSchema, expectedErrorMsg) =>
    test(testName) {
      val ex = intercept[IllegalArgumentException] {
        ParquetSchemaUtils.toParquetSchema(deltaSchema)
      }
      assert(ex.getMessage.contains(expectedErrorMsg))
    }
  }

  private def fieldMetadata(id: Int, nestedFieldIds: (String, Int) *): FieldMetadata = {
    val builder = FieldMetadata.builder().putLong(ColumnMapping.PARQUET_FIELD_ID_KEY, id)

    val nestedFiledMetadata = FieldMetadata.builder()
    nestedFieldIds.foreach { case (nestedColPath, nestedId) =>
      nestedFiledMetadata.putLong(nestedColPath, nestedId)
    }
    builder
      .putFieldMetadata(PARQUET_FIELD_NESTED_IDS_METADATA_KEY, nestedFiledMetadata.build())
      .build()
  }
}
