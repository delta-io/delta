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

package io.delta.kernel.internal.columndefaults

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.test.ActionUtils
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class ColumnDefaultsSuite extends AnyFunSuite with ActionUtils {
  val validProtocol = new Protocol(
    TableFeatures.TABLE_FEATURES_MIN_READER_VERSION,
    TableFeatures.TABLE_FEATURES_MIN_WRITER_VERSION,
    Set.empty[String].asJava,
    Set(
      TableFeatures.ALLOW_COLUMN_DEFAULTS_W_FEATURE.featureName(),
      TableFeatures.ICEBERG_COMPAT_V3_W_FEATURE.featureName()).asJava)
  test("validate") {
    def metadataForDefault(value: String): FieldMetadata =
      FieldMetadata.builder().putString("CURRENT_DEFAULT", value).build()
    val correctSchema = new StructType()
      .add("id", IntegerType.INTEGER)
      .add("int", IntegerType.INTEGER, metadataForDefault("\"123\""))
      .add("short", ShortType.SHORT, metadataForDefault("123"))
      .add("long", LongType.LONG, metadataForDefault("1231341"))
      .add("decimal", new DecimalType(10, 5), metadataForDefault("1231.34155"))
      .add("float", FloatType.FLOAT, metadataForDefault("123.1341"))
      .add("double", DoubleType.DOUBLE, metadataForDefault("123.7774"))
      .add("double2", DoubleType.DOUBLE, metadataForDefault("'123.7774'"))
      .add("name", StringType.STRING, metadataForDefault("\"tom\""))
      .add("name2", StringType.STRING, metadataForDefault("'tom'"))
      .add("date", DateType.DATE, metadataForDefault("'2025-01-01'"))
      .add("date2", DateType.DATE, metadataForDefault("2025-01-01"))
      .add("ts", TimestampType.TIMESTAMP, metadataForDefault("\"2025-01-01T00:00:00Z\""))
      .add("ts2", TimestampType.TIMESTAMP, metadataForDefault("\"2025-01-01T00:00:00+01:00\""))
      .add("ts3", TimestampType.TIMESTAMP, metadataForDefault("2025-01-01T00:00:00Z"))
      .add("tsntz", TimestampNTZType.TIMESTAMP_NTZ, metadataForDefault("'2025-01-01T00:00:00'"))
      .add("tsntz2", TimestampNTZType.TIMESTAMP_NTZ, metadataForDefault("2025-01-01T00:00:00"))
      .add(
        "childStruct",
        new StructType()
          .add("childId", IntegerType.INTEGER, metadataForDefault("100"))
          .add(
            "grandChildList",
            new ArrayType(
              new StructType().add("nestedId", IntegerType.INTEGER, metadataForDefault("120")),
              false)))
      .add(
        "grandChildMap",
        new MapType(
          new StructType().add("mapKeyId", IntegerType.INTEGER, metadataForDefault("220")),
          new StructType().add("mapValueId", IntegerType.INTEGER, metadataForDefault("330")),
          false))
      .add(
        "childList",
        new ArrayType(
          new StructType().add("clid", IntegerType.INTEGER, metadataForDefault("300")),
          false))
    ColumnDefaults.validate(correctSchema, true)
    intercept[KernelException] {
      ColumnDefaults.validate(correctSchema, false)
    }

    val unsupportedCases = Seq(
      new StructType().add("sub", IntegerType.INTEGER),
      new ArrayType(IntegerType.INTEGER, false),
      new MapType(IntegerType.INTEGER, IntegerType.INTEGER, false),
      VariantType.VARIANT)
    unsupportedCases.foreach { dataType =>
      val schemaWithUnsupportedType = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("col1", dataType, metadataForDefault("120"))
      intercept[KernelException] {
        ColumnDefaults.validate(schemaWithUnsupportedType, true)
      }
    }

    val badCases = Seq(
      (StringType.STRING, "string with no quotes"),
      (BinaryType.BINARY, "string with no quotes"),
      (ShortType.SHORT, "1248.995"),
      (IntegerType.INTEGER, "1248.995"),
      (LongType.LONG, "1248.995"),
      (FloatType.FLOAT, "michael"),
      (DoubleType.DOUBLE, "jordan"),
      (new DecimalType(10, 0), "1248.995"),
      (new DecimalType(10, 5), "12480031341.995"),
      (new DecimalType(10, 5), "1248.995031"),
      (DateType.DATE, "1248.995031"),
      (DateType.DATE, "\"2025/09/01\""),
      (DateType.DATE, "09/01/2025"),
      (TimestampType.TIMESTAMP, "2025-01-01"),
      (TimestampType.TIMESTAMP, "2025-01-01"),
      (TimestampType.TIMESTAMP, "2025-01-01 00:00:00"),
      (TimestampType.TIMESTAMP, "'2025-01-01T00:00:00'"),
      (TimestampType.TIMESTAMP, "2025-01-01T00:00:00+2:00"),
      (TimestampNTZType.TIMESTAMP_NTZ, "2025-01-01"),
      (TimestampNTZType.TIMESTAMP_NTZ, "'2025-01-01 00:00:00'"))

    badCases.foreach { case (dataType, defaultValue) =>
      val badSchema1 = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("col", dataType, metadataForDefault(defaultValue))

      val badSchema2 = new StructType()
        .add("id", IntegerType.INTEGER)
        .add(
          "childStruct",
          new StructType()
            .add("childId", IntegerType.INTEGER, metadataForDefault("100"))
            .add("badcol", dataType, metadataForDefault(defaultValue)))
      val badSchema3 = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("name", StringType.STRING, metadataForDefault("tom"))
        .add(
          "childList",
          new ArrayType(
            new StructType()
              .add("clid", IntegerType.INTEGER, metadataForDefault("300"))
              .add("badcol", dataType, metadataForDefault(defaultValue)),
            false))
      val badSchema4 = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("name", StringType.STRING, metadataForDefault("tom"))
        .add(
          "grandChildMap",
          new MapType(
            new StructType().add("mapKeyId", IntegerType.INTEGER, metadataForDefault("220")),
            new StructType().add("mapValueId", dataType, metadataForDefault(defaultValue)),
            false))
      Seq(badSchema1, badSchema2, badSchema3, badSchema4).foreach(schema => {
        intercept[KernelException] {
          ColumnDefaults.validate(schema, true)
        }
      })
    }
  }
}
