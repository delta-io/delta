/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.standalone.internal.util

import org.apache.parquet.schema.{ConversionPatterns, MessageType, Type, Types}
import org.apache.parquet.schema.OriginalType._
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.schema.Type.Repetition._

import io.delta.standalone.types._
import io.delta.standalone.util.ParquetSchemaConverter.ParquetOutputTimestampType

/**
 * This converter class is used to convert Spark SQL [[StructType]] to Parquet [[MessageType]].
 *
 * @param writeLegacyParquetFormat  Whether to use legacy Parquet format compatible with Spark 1.4
 *        and prior versions when converting a [[StructType]] to a Parquet [[MessageType]].
 *        When set to false, use standard format defined in parquet-format spec.  This argument only
 *        affects Parquet write path.
 * @param outputTimestampType  which parquet timestamp type to use when writing.
 */
private[standalone] class SparkToParquetSchemaConverter(
    writeLegacyParquetFormat: Boolean,
    outputTimestampType: ParquetOutputTimestampType) {

  /**
   * Converts a Spark SQL [[StructType]] to a Parquet [[MessageType]].
   */
  def convert(schema: StructType): MessageType = {
    Types
      .buildMessage()
      .addFields(schema.getFields.map(convertField): _*)
      .named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)
  }

  /**
   * Converts a Spark SQL [[StructField]] to a Parquet [[Type]].
   */
  private def convertField(field: StructField): Type = {
    convertField(field, if (field.isNullable) OPTIONAL else REQUIRED)
  }

  private def convertField(field: StructField, repetition: Type.Repetition): Type = {
    ParquetSchemaConverter.checkFieldName(field.getName)

    field.getDataType match {
      // ===================
      // Simple atomic types
      // ===================

      case _: BooleanType =>
        Types.primitive(BOOLEAN, repetition).named(field.getName)

      case _: ByteType =>
        Types.primitive(INT32, repetition).as(INT_8).named(field.getName)

      case _: ShortType =>
        Types.primitive(INT32, repetition).as(INT_16).named(field.getName)

      case _: IntegerType =>
        Types.primitive(INT32, repetition).named(field.getName)

      case _: LongType =>
        Types.primitive(INT64, repetition).named(field.getName)

      case _: FloatType =>
        Types.primitive(FLOAT, repetition).named(field.getName)

      case _: DoubleType =>
        Types.primitive(DOUBLE, repetition).named(field.getName)

      case _: StringType =>
        Types.primitive(BINARY, repetition).as(UTF8).named(field.getName)

      case _: DateType =>
        Types.primitive(INT32, repetition).as(DATE).named(field.getName)

      // NOTE: Spark SQL can write timestamp values to Parquet using INT96, TIMESTAMP_MICROS or
      // TIMESTAMP_MILLIS. TIMESTAMP_MICROS is recommended but INT96 is the default to keep the
      // behavior same as before.
      //
      // As stated in PARQUET-323, Parquet `INT96` was originally introduced to represent nanosecond
      // timestamp in Impala for some historical reasons.  It's not recommended to be used for any
      // other types and will probably be deprecated in some future version of parquet-format spec.
      // That's the reason why parquet-format spec only defines `TIMESTAMP_MILLIS` and
      // `TIMESTAMP_MICROS` which are both logical types annotating `INT64`.
      //
      // Originally, Spark SQL uses the same nanosecond timestamp type as Impala and Hive.  Starting
      // from Spark 1.5.0, we resort to a timestamp type with microsecond precision so that we can
      // store a timestamp into a `Long`.  This design decision is subject to change though, for
      // example, we may resort to nanosecond precision in the future.
      case _: TimestampType =>
        outputTimestampType match {
          case ParquetOutputTimestampType.INT96 =>
            Types.primitive(INT96, repetition).named(field.getName)
          case ParquetOutputTimestampType.TIMESTAMP_MICROS =>
            Types.primitive(INT64, repetition).as(TIMESTAMP_MICROS).named(field.getName)
          case ParquetOutputTimestampType.TIMESTAMP_MILLIS =>
            Types.primitive(INT64, repetition).as(TIMESTAMP_MILLIS).named(field.getName)
        }

      case _: BinaryType =>
        Types.primitive(BINARY, repetition).named(field.getName)

      // ======================
      // Decimals (legacy mode)
      // ======================

      // Spark 1.4.x and prior versions only support decimals with a maximum precision of 18 and
      // always store decimals in fixed-length byte arrays.  To keep compatibility with these older
      // versions, here we convert decimals with all precisions to `FIXED_LEN_BYTE_ARRAY` annotated
      // by `DECIMAL`.
      case decimalType: DecimalType if writeLegacyParquetFormat =>
        Types
          .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(DECIMAL)
          .precision(decimalType.getPrecision)
          .scale(decimalType.getScale)
          .length(computeMinBytesForPrecision(decimalType.getPrecision))
          .named(field.getName)

      // ========================
      // Decimals (standard mode)
      // ========================

      // Uses INT32 for 1 <= precision <= 9
      case decimalType: DecimalType
        if decimalType.getPrecision <= 9 && !writeLegacyParquetFormat =>
        Types
          .primitive(INT32, repetition)
          .as(DECIMAL)
          .precision(decimalType.getPrecision)
          .scale(decimalType.getScale)
          .named(field.getName)

      // Uses INT64 for 1 <= precision <= 18
      case decimalType: DecimalType
        if decimalType.getPrecision <= 18 && !writeLegacyParquetFormat =>
        Types
          .primitive(INT64, repetition)
          .as(DECIMAL)
          .precision(decimalType.getPrecision)
          .scale(decimalType.getScale)
          .named(field.getName)

      // Uses FIXED_LEN_BYTE_ARRAY for all other precisions
      case decimalType: DecimalType if !writeLegacyParquetFormat =>
        Types
          .primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(DECIMAL)
          .precision(decimalType.getPrecision)
          .scale(decimalType.getScale)
          .length(computeMinBytesForPrecision(decimalType.getPrecision))
          .named(field.getName)

      // ===================================
      // ArrayType and MapType (legacy mode)
      // ===================================

      // Spark 1.4.x and prior versions convert `ArrayType` with nullable elements into a 3-level
      // `LIST` structure.  This behavior is somewhat a hybrid of parquet-hive and parquet-avro
      // (1.6.0rc3): the 3-level structure is similar to parquet-hive while the 3rd level element
      // field name "array" is borrowed from parquet-avro.
      case arrayType: ArrayType if arrayType.containsNull && writeLegacyParquetFormat =>
        // <list-repetition> group <name> (LIST) {
        //   optional group bag {
        //     repeated <element-type> array;
        //   }
        // }

        // This should not use `listOfElements` here because this new method checks if the
        // element name is `element` in the `GroupType` and throws an exception if not.
        // As mentioned above, Spark prior to 1.4.x writes `ArrayType` as `LIST` but with
        // `array` as its element name as below. Therefore, we build manually
        // the correct group type here via the builder. (See SPARK-16777)
        Types
          .buildGroup(repetition).as(LIST)
          .addField(Types
            .buildGroup(REPEATED)
            // "array" is the name chosen by parquet-hive (1.7.0 and prior version)
            .addField(convertField(
              new StructField("array", arrayType.getElementType, arrayType.containsNull)))
            .named("bag"))
          .named(field.getName)

      // Spark 1.4.x and prior versions convert ArrayType with non-nullable elements into a 2-level
      // LIST structure.  This behavior mimics parquet-avro (1.6.0rc3).  Note that this case is
      // covered by the backwards-compatibility rules implemented in `isElementType()`.
      case arrayType: ArrayType if !arrayType.containsNull && writeLegacyParquetFormat =>
        // <list-repetition> group <name> (LIST) {
        //   repeated <element-type> element;
        // }

        // Here too, we should not use `listOfElements`. (See SPARK-16777)
        Types
          .buildGroup(repetition).as(LIST)
          // "array" is the name chosen by parquet-avro (1.7.0 and prior version)
          .addField(convertField(
            new StructField("array", arrayType.getElementType, arrayType.containsNull), REPEATED))
          .named(field.getName)

      // Spark 1.4.x and prior versions convert MapType into a 3-level group annotated by
      // MAP_KEY_VALUE.  This is covered by `convertGroupField(field: GroupType): DataType`.
      case mapType: MapType if writeLegacyParquetFormat =>
        // <map-repetition> group <name> (MAP) {
        //   repeated group map (MAP_KEY_VALUE) {
        //     required <key-type> key;
        //     <value-repetition> <value-type> value;
        //   }
        // }
        ConversionPatterns.mapType(
          repetition,
          field.getName,
          "key_value",
          convertField(new StructField("key", mapType.getKeyType, false)),
          convertField(new StructField("value", mapType.getValueType, mapType.valueContainsNull)))

      // =====================================
      // ArrayType and MapType (standard mode)
      // =====================================

      case arrayType: ArrayType if !writeLegacyParquetFormat =>
        // <list-repetition> group <name> (LIST) {
        //   repeated group list {
        //     <element-repetition> <element-type> element;
        //   }
        // }
        Types
          .buildGroup(repetition).as(LIST)
          .addField(
            Types.repeatedGroup()
              .addField(convertField(
                new StructField("element", arrayType.getElementType, arrayType.containsNull)))
              .named("list"))
          .named(field.getName)

      case mapType: MapType if !writeLegacyParquetFormat =>
        // <map-repetition> group <name> (MAP) {
        //   repeated group key_value {
        //     required <key-type> key;
        //     <value-repetition> <value-type> value;
        //   }
        // }
        Types
          .buildGroup(repetition).as(MAP)
          .addField(
            Types
              .repeatedGroup()
              .addField(convertField(new StructField("key", mapType.getKeyType, false)))
              .addField(convertField(
                new StructField("value", mapType.getValueType, mapType.valueContainsNull())))
              .named("key_value"))
          .named(field.getName)

      // ===========
      // Other types
      // ===========

      case structType: StructType =>
        structType.getFields.foldLeft(Types.buildGroup(repetition)) { (builder, field) =>
          builder.addField(convertField(field))
        }.named(field.getName)

      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported data type ${field.getDataType.getTypeName}")
    }
  }

  // Returns the minimum number of bytes needed to store a decimal with a given `precision`.
  private def computeMinBytesForPrecision(precision: Int) : Int = {
    var numBytes = 1
    while (math.pow(2.0, 8 * numBytes - 1) < math.pow(10.0, precision)) {
      numBytes += 1
    }
    numBytes
  }
}

private[internal] object ParquetSchemaConverter {
  val SPARK_PARQUET_SCHEMA_NAME = "spark_schema"

  val EMPTY_MESSAGE: MessageType =
    Types.buildMessage().named(ParquetSchemaConverter.SPARK_PARQUET_SCHEMA_NAME)

  def checkFieldName(name: String): Unit = {
    // ,;{}()\n\t= and space are special characters in Parquet schema
    checkConversionRequirement(
      !name.matches(".*[ ,;{}()\n\t=].*"),
      s"""Attribute name "$name" contains invalid character(s) among " ,;{}()\\n\\t=".
         |Please use alias to rename it.
       """.stripMargin.split("\n").mkString(" ").trim)
  }

  def checkFieldNames(names: Seq[String]): Unit = {
    names.foreach(checkFieldName)
  }

  def checkConversionRequirement(f: => Boolean, message: String): Unit = {
    if (!f) {
      throw new IllegalArgumentException(message)
    }
  }
}
