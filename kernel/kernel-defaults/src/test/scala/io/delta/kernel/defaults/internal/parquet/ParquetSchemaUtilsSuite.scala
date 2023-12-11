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
import io.delta.kernel.types.{ArrayType, DoubleType, FieldMetadata, IntegerType, LongType, MapType, StructType}
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
        .add("f00", IntegerType.INTEGER, fieldMetadata(2))
        .add("f01", LongType.LONG, fieldMetadata(3)),
      fieldMetadata(1))
    .add("f1", new ArrayType(IntegerType.INTEGER, false), fieldMetadata(4))
    .add(
      "f2",
      new MapType(
        new StructType()
          .add("key_f0", IntegerType.INTEGER, fieldMetadata(10))
          .add("key_f1", IntegerType.INTEGER, fieldMetadata(11)),
        IntegerType.INTEGER,
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
          .add("f00", IntegerType.INTEGER, fieldMetadata(2)),
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
          .add("f00", IntegerType.INTEGER, fieldMetadata(2))
          .add("f02", IntegerType.INTEGER, fieldMetadata(15)),
        fieldMetadata(1)
      )
      .add("f4", IntegerType.INTEGER, fieldMetadata(14))

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
          .add("F00", IntegerType.INTEGER) // no field id and with case-insensitive column name
          .add("f01", IntegerType.INTEGER, fieldMetadata(3)),
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
          .add("f00_new", IntegerType.INTEGER, fieldMetadata(2)),
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
          .add("f00", IntegerType.INTEGER, fieldMetadata(2)),
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

  private def fieldMetadata(id: Int): FieldMetadata = {
    FieldMetadata.builder()
      .putLong(ColumnMapping.PARQUET_FIELD_ID_KEY, id)
      .build()
  }
}
