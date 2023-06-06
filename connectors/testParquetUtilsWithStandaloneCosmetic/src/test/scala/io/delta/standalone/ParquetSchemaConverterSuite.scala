/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone

import org.apache.parquet.schema.MessageTypeParser
import org.scalatest.FunSuite

import io.delta.standalone.types._
import io.delta.standalone.util.ParquetSchemaConverter

class ParquetSchemaConverterSuite extends FunSuite {

  private def testCatalystToParquet(
      testName: String,
      sqlSchema: StructType,
      parquetSchema: String,
      writeLegacyParquetFormat: Boolean,
      outputTimestampType: ParquetSchemaConverter.ParquetOutputTimestampType =
        ParquetSchemaConverter.ParquetOutputTimestampType.INT96): Unit = {

    test(s"sql => parquet: $testName") {
      val actual = ParquetSchemaConverter.deltaToParquet(
        sqlSchema,
        writeLegacyParquetFormat,
        outputTimestampType)
      val expected = MessageTypeParser.parseMessageType(parquetSchema)
      actual.checkContains(expected)
      expected.checkContains(actual)
    }
  }

  // =======================================================
  // Tests for converting Catalyst ArrayType to Parquet LIST
  // =======================================================

  testCatalystToParquet(
    "Backwards-compatibility: LIST with nullable element type - 1 - standard",
    new StructType(Array(
      new StructField(
        "f1",
        new ArrayType(new IntegerType(), true),
        true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group list {
      |      optional int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "Backwards-compatibility: LIST with nullable element type - 2 - prior to 1.4.x",
    new StructType(Array(
      new StructField(
        "f1",
        new ArrayType(new IntegerType(), true),
        true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group bag {
      |      optional int32 array;
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  testCatalystToParquet(
    "Backwards-compatibility: LIST with non-nullable element type - 1 - standard",
    new StructType(Array(
      new StructField(
        "f1",
        new ArrayType(new IntegerType(), false),
        true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated group list {
      |      required int32 element;
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "Backwards-compatibility: LIST with non-nullable element type - 2 - prior to 1.4.x",
    new StructType(Array(
      new StructField(
        "f1",
        new ArrayType(new IntegerType(), false),
        true))),
    """message root {
      |  optional group f1 (LIST) {
      |    repeated int32 array;
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  // ====================================================
  // Tests for converting Catalyst MapType to Parquet Map
  // ====================================================

  testCatalystToParquet(
    "Backwards-compatibility: MAP with non-nullable value type - 1 - standard",
    new StructType(Array(
      new StructField(
        "f1",
        new MapType(new IntegerType(), new StringType(), false),
        true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      required binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "Backwards-compatibility: MAP with non-nullable value type - 2 - prior to 1.4.x",
    new StructType(Array(
      new StructField(
        "f1",
        new MapType(new IntegerType(), new StringType(), false),
        true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value (MAP_KEY_VALUE) {
      |      required int32 key;
      |      required binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  testCatalystToParquet(
    "Backwards-compatibility: MAP with nullable value type - 1 - standard",
    new StructType(Array(
      new StructField(
        "f1",
        new MapType(new IntegerType(), new StringType(), true),
        true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "Backwards-compatibility: MAP with nullable value type - 3 - prior to 1.4.x",
    new StructType(Array(
      new StructField(
        "f1",
        new MapType(new IntegerType(), new StringType(), true),
       true))),
    """message root {
      |  optional group f1 (MAP) {
      |    repeated group key_value (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  // =================================
  // Tests for conversion for decimals
  // =================================

  testCatalystToParquet(
    "DECIMAL(1, 0) - standard",
    new StructType(Array(new StructField("f1", new DecimalType(1, 0)))),
    """message root {
      |  optional int32 f1 (DECIMAL(1, 0));
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "DECIMAL(8, 3) - standard",
    new StructType(Array(new StructField("f1", new DecimalType(8, 3)))),
    """message root {
      |  optional int32 f1 (DECIMAL(8, 3));
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "DECIMAL(9, 3) - standard",
    new StructType(Array(new StructField("f1", new DecimalType(9, 3)))),
    """message root {
      |  optional int32 f1 (DECIMAL(9, 3));
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "DECIMAL(18, 3) - standard",
    new StructType(Array(new StructField("f1", new DecimalType(18, 3)))),
    """message root {
      |  optional int64 f1 (DECIMAL(18, 3));
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "DECIMAL(19, 3) - standard",
    new StructType(Array(new StructField("f1", new DecimalType(19, 3)))),
    """message root {
      |  optional fixed_len_byte_array(9) f1 (DECIMAL(19, 3));
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)

  testCatalystToParquet(
    "DECIMAL(1, 0) - prior to 1.4.x",
    new StructType(Array(new StructField("f1", new DecimalType(1, 0)))),
    """message root {
      |  optional fixed_len_byte_array(1) f1 (DECIMAL(1, 0));
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  testCatalystToParquet(
    "DECIMAL(8, 3) - prior to 1.4.x",
    new StructType(Array(new StructField("f1", new DecimalType(8, 3)))),
    """message root {
      |  optional fixed_len_byte_array(4) f1 (DECIMAL(8, 3));
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  testCatalystToParquet(
    "DECIMAL(9, 3) - prior to 1.4.x",
    new StructType(Array(new StructField("f1", new DecimalType(9, 3)))),
    """message root {
      |  optional fixed_len_byte_array(5) f1 (DECIMAL(9, 3));
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  testCatalystToParquet(
    "DECIMAL(18, 3) - prior to 1.4.x",
    new StructType(Array(new StructField("f1", new DecimalType(18, 3)))),
    """message root {
      |  optional fixed_len_byte_array(8) f1 (DECIMAL(18, 3));
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true)

  testCatalystToParquet(
    "Timestamp written and read as INT64 with TIMESTAMP_MILLIS",
    new StructType(Array(new StructField("f1", new TimestampType()))),
    """message root {
      |  optional INT64 f1 (TIMESTAMP_MILLIS);
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true,
    outputTimestampType = ParquetSchemaConverter.ParquetOutputTimestampType.TIMESTAMP_MILLIS)

  testCatalystToParquet(
    "Timestamp written and read as INT64 with TIMESTAMP_MICROS",
    new StructType(Array(new StructField("f1", new TimestampType()))),
    """message root {
      |  optional INT64 f1 (TIMESTAMP_MICROS);
      |}
    """.stripMargin,
    writeLegacyParquetFormat = true,
    outputTimestampType = ParquetSchemaConverter.ParquetOutputTimestampType.TIMESTAMP_MICROS)

  testCatalystToParquet(
    "SPARK-36825: Year-month interval written and read as INT32",
    new StructType(Array(new StructField("f1", new DateType()))),
    """message root {
      |  optional INT32 f1;
      |}
    """.stripMargin,
    writeLegacyParquetFormat = false)
}
