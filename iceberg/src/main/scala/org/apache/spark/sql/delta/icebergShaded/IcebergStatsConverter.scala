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

package org.apache.spark.sql.delta.icebergShaded

import java.lang.{Long => JLong}
import java.nio.ByteBuffer

import org.apache.spark.sql.delta.DeltaColumnMapping
import org.apache.spark.sql.delta.stats.{DeltaStatistics, SkippingEligibleDataType}
import shadedForDelta.org.apache.iceberg.types.Conversions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Converts Delta stats to Iceberg stats given an Internal Row representing Delta stats and the
 * row's schema.
 *
 * Iceberg stores stats as a map from column ID to the statistic. For example, lower/upper bound
 * statistics are represented as a map from column ID to byte buffer where the byte buffer stores
 * any type.
 *
 * For example, given the following Delta stats schema with column IDs:
 * | -- id(0): INT
 * | -- person(1): STRUCT
 *     | name(2): STRUCT
 *         | -- first(3): STRING
 *         | -- last(4): STRING
 *     | height(5): LONG
 *
 * Iceberg's upper bound statistic map will be:
 * {0 -> MAX_ID, 3 -> MAX_FIRST, 4 -> MAX_LAST, 5 -> MAX_HEIGHT}
 *
 * Iceberg requires the "record count" stat while the "upper bounds", "lower bounds", and
 * "null value counts" are optional. See iceberg/DataFile.java.
 * Iceberg's "record count" metric is set in `convertFileAction` before the stats conversion.
 * If additional metrics are attached to the Iceberg data file, the "record count" metric must be
 * left non-null.
 */
case class IcebergStatsConverter(statsRow: InternalRow, statsSchema: StructType) {

  val numRecordsStat: JLong = statsSchema.getFieldIndex(DeltaStatistics.NUM_RECORDS) match {
    case Some(fieldIndex) => new JLong(statsRow.getLong(fieldIndex))
    case None => throw new IllegalArgumentException("Delta is missing the 'num records' stat. " +
    "Iceberg requires this stat when attaching statistics to the output data file.")
  }

  val lowerBoundsStat: Option[Map[Integer, ByteBuffer]] =
    getByteBufferBackedColStats(DeltaStatistics.MIN)

  val upperBoundsStat: Option[Map[Integer, ByteBuffer]] =
    getByteBufferBackedColStats(DeltaStatistics.MAX)

  val nullValueCountsStat: Option[Map[Integer, JLong]] =
    statsSchema.getFieldIndex(DeltaStatistics.NULL_COUNT) match {
      case Some(nullCountFieldIdx) =>
        val nullCountStatSchema =
          statsSchema.fields(nullCountFieldIdx).dataType.asInstanceOf[StructType]
        Some(
          generateIcebergLongMetricMap(
            statsRow.getStruct(nullCountFieldIdx, nullCountStatSchema.fields.length),
            nullCountStatSchema
          )
        )
      case None => None
    }

  /**
   * Generates Iceberg's metric representation by recursively flattening the Delta stat struct
   * (represented as an internal row) and converts the column's physical name to its ID.
   *
   * Ignores null Delta stats.
   *
   * @param stats An internal row holding the `ByteBuffer`-based Delta column stats
   *              (i.e. lower bound).
   * @param statsSchema The schema of the `stats` internal row.
   * @return Iceberg's ByteBuffer-backed metric representation.
   */
  private def generateIcebergByteBufferMetricMap(
      stats: InternalRow,
      statsSchema: StructType): Map[Integer, ByteBuffer] = {
    statsSchema.fields.zipWithIndex.flatMap { case (field, idx) =>
      field.dataType match {
        // Iceberg statistics cannot be null.
        case _ if stats.isNullAt(idx) => Map[Integer, ByteBuffer]().empty
        // If the stats schema contains a struct type, there is a corresponding struct in the data
        // schema. The struct's per-field stats are also stored in the Delta stats struct. See the
        // `StatisticsCollection` trait comment for more.
        case st: StructType =>
          generateIcebergByteBufferMetricMap(stats.getStruct(idx, st.fields.length), st)
        // Ignore the Delta statistic if the conversion doesn't support the given data type or the
        // column ID for this field is missing.
        case dt if !DeltaColumnMapping.hasColumnId(field) ||
            !IcebergStatsConverter.isMinMaxStatTypeSupported(dt) => Map[Integer, ByteBuffer]().empty
        case b: ByteType =>
          // Iceberg stores bytes using integers.
          val statVal = stats.getByte(idx).toInt
          Map[Integer, ByteBuffer](Integer.valueOf(DeltaColumnMapping.getColumnId(field)) ->
            Conversions.toByteBuffer(IcebergSchemaUtils.convertAtomic(b), statVal))
        case s: ShortType =>
          // Iceberg stores shorts using integers.
          val statVal = stats.getShort(idx).toInt
          Map[Integer, ByteBuffer](Integer.valueOf(DeltaColumnMapping.getColumnId(field)) ->
            Conversions.toByteBuffer(IcebergSchemaUtils.convertAtomic(s), statVal))
        case dt if IcebergStatsConverter.isMinMaxStatTypeSupported(dt) =>
          val statVal = stats.get(idx, dt)

          // Iceberg's `Conversions.toByteBuffer` method expects the Java object representation
          // for string and decimal types.
          // Other types supported by Delta's min/max stat such as int, long, boolean, etc., do not
          // require a different representation.
          val compatibleStatsVal = statVal match {
            case u: UTF8String => u.toString
            case d: Decimal => d.toJavaBigDecimal
            case _ => statVal
          }
          Map[Integer, ByteBuffer](Integer.valueOf(DeltaColumnMapping.getColumnId(field)) ->
            Conversions.toByteBuffer(IcebergSchemaUtils.convertAtomic(dt), compatibleStatsVal))
      }
    }.toMap
  }

  /**
   * Generates Iceberg's metric representation by recursively flattening the Delta stat struct
   * (represented as an internal row) and converts the column's physical name to its ID.
   *
   * @param stats An internal row holding the long-backed Delta column stats (i.e. null counts).
   * @param statsSchema The schema of the `stats` internal row.
   * @return a map in Iceberg's metric representation.
   */
  private def generateIcebergLongMetricMap(
      stats: InternalRow,
      statsSchema: StructType): Map[Integer, JLong] = {
    statsSchema.fields.zipWithIndex.flatMap { case (field, idx) =>
      field.dataType match {
        // If the stats schema contains a struct type, there is a corresponding struct in the data
        // schema. The struct's per-field stats are also stored in the Delta stats struct. See the
        // `StatisticsCollection` trait comment for more.
        case st: StructType =>
          generateIcebergLongMetricMap(stats.getStruct(idx, st.fields.length), st)
        case lt: LongType =>
          if (DeltaColumnMapping.hasColumnId(field)) {
            Map[Integer, JLong](Integer.valueOf(DeltaColumnMapping.getColumnId(field)) ->
              new JLong(stats.getLong(idx)))
          } else {
            Map[Integer, JLong]().empty
          }
        case _ => throw new UnsupportedOperationException("Expected metric to be a long type.")
      }
    }.toMap
  }

  /**
   * @param statName The name of the Delta stat that is being converted. Must be one of the field
   *                 names in the `DeltaStatistics` object.
   * @return An option holding Iceberg's statistic representation. Returns `None` if the output
   *         would otherwise be empty.
   */
  private def getByteBufferBackedColStats(statName: String): Option[Map[Integer, ByteBuffer]] = {
    statsSchema.getFieldIndex(statName) match {
      case Some(statFieldIdx) =>
        val colStatSchema = statsSchema.fields(statFieldIdx).dataType.asInstanceOf[StructType]
        val icebergMetricsMap = generateIcebergByteBufferMetricMap(
          statsRow.getStruct(statFieldIdx, colStatSchema.fields.length),
          colStatSchema
        )
        if (icebergMetricsMap.nonEmpty) {
          Some(icebergMetricsMap)
        } else {
          // The iceberg metrics map may be empty when all Delta stats are null.
          None
        }
      case None => None
    }
  }
}

object IcebergStatsConverter {
  /**
   * Returns true if a min/max statistic of the given Delta data type can be converted into an
   * Iceberg metric of equivalent data type.
   *
   * Currently, nested types and null types are unsupported.
   */
  def isMinMaxStatTypeSupported(dt: DataType): Boolean = {
    if (!SkippingEligibleDataType(dt)) return false

    dt match {
      case _: StringType | _: IntegerType | _: FloatType | _: DoubleType |
        _: DoubleType | _: DecimalType | _: BooleanType | _: DateType | _: TimestampType |
        // _: LongType TODO: enable after https://github.com/apache/spark/pull/42083 is released
        _: TimestampNTZType | _: ByteType | _: ShortType => true
      case _ => false
    }
  }
}
