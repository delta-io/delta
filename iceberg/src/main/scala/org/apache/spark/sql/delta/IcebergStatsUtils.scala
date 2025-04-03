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

package org.apache.spark.sql.delta.commands.convert

import java.lang.{Integer => JInt, Long => JLong}
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaStatistics._
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.iceberg.{DataFile, PartitionData, PartitionField, Schema, StructLike, Table}
import org.apache.iceberg.types.{Conversions, Type => IcebergType}
import org.apache.iceberg.types.Type.{PrimitiveType => IcebergPrimitiveType, TypeID}
import org.apache.iceberg.types.Types.{
  DateType => IcebergDateType,
  ListType => IcebergListType,
  MapType => IcebergMapType,
  NestedField,
  StringType => IcebergStringType,
  StructType => IcebergStructType,
  TimestampType => IcebergTimestampType
}
import org.apache.iceberg.util.DateTimeUtil

import org.apache.spark.sql.SparkSession

object IcebergStatsUtils extends DeltaLogging {

  // Types that are currently supported for converting stats to delta
  // The stats for these types will be converted to Delta stats
  // except for following types:
  //  DECIMAL (decided by DeltaSQLConf.DELTA_CONVERT_ICEBERG_DECIMAL_STATS)
  //  DATE (decided by DeltaSQLConf.DELTA_CONVERT_ICEBERG_DATE_STATS)
  //  TIMESTAMP (decided by DeltaSQLConf.DELTA_CONVERT_ICEBERG_TIMESTAMP_STATS)
  // which are decided by spark configs dynamically
  private val STATS_ALLOW_TYPES = Set[TypeID](
    TypeID.BOOLEAN,
    TypeID.INTEGER,
    TypeID.LONG,
    TypeID.FLOAT,
    TypeID.DOUBLE,
    TypeID.DATE,
//    TypeID.TIME,
    TypeID.TIMESTAMP,
//    TypeID.TIMESTAMP_NANO,
    TypeID.STRING,
//    TypeID.UUID,
//    TypeID.FIXED,
    TypeID.BINARY,
    TypeID.DECIMAL
  )

  private val CONFIGS_TO_STATS_ALLOW_TYPES = Map(
    DeltaSQLConf.DELTA_CONVERT_ICEBERG_DATE_STATS -> TypeID.DATE,
    DeltaSQLConf.DELTA_CONVERT_ICEBERG_TIMESTAMP_STATS -> TypeID.TIMESTAMP,
    DeltaSQLConf.DELTA_CONVERT_ICEBERG_DECIMAL_STATS -> TypeID.DECIMAL
  )

  def typesAllowStatsConversion(spark: SparkSession): Set[TypeID] = {
    val statsDisallowTypes = CONFIGS_TO_STATS_ALLOW_TYPES.filter {
      case (conf, _) => !spark.sessionState.conf.getConf(conf)
    }.values.toSet

    typesAllowStatsConversion(statsDisallowTypes)
  }

  def typesAllowStatsConversion(statsDisallowTypes: Set[TypeID]): Set[TypeID] = {
    STATS_ALLOW_TYPES -- statsDisallowTypes
  }

  /**
   * Convert Iceberg DataFile stats into a Json string containing Delta stats.
   * We will abandon conversion if Iceberg DataFile has a null or empty stats for
   * any criteria used in the conversion.
   *
   * @param icebergSchema            Iceberg table schema
   * @param dataFile                 Iceberg DataFile that contains stats info
   * @return None if stats is missing on the DataFile or error occurs during conversion
   */
  def icebergStatsToDelta(
      icebergSchema: Schema,
      dataFile: DataFile,
      statsAllowTypes: Set[TypeID]): Option[String] = {
    try {
      // Any empty or null fields means Iceberg has disabled column stats
      if (dataFile.upperBounds == null ||
        dataFile.upperBounds.isEmpty ||
        dataFile.lowerBounds == null ||
        dataFile.lowerBounds.isEmpty ||
        dataFile.nullValueCounts == null ||
        dataFile.nullValueCounts.isEmpty
      ) {
        return None
      }
      Some(icebergStatsToDelta(
        icebergSchema,
        dataFile.recordCount,
        dataFile.upperBounds.asScala.toMap,
        dataFile.lowerBounds.asScala.toMap,
        dataFile.nullValueCounts.asScala.toMap,
        statsAllowTypes
      ))
    } catch {
      case NonFatal(e) =>
        logError("Exception while converting Iceberg stats to Delta format", e)
        None
    }
  }

  /**
   * Convert Iceberg DataFile stats into Delta stats.
   *
   * Iceberg stats consist of multiple maps from field_id to value. The maps include
   * max_value, min_value and null_counts.
   * Delta stats is a Json string.
   *
   **********************************************************
   * Example:
   **********************************************************
   * Assume we have an Iceberg table of schema
   * ( col1: int, field_id = 1, col2: string, field_id = 2 )
   *
   * The following Iceberg stats:
   *    numRecords 100
   *    max_value { 1 -> 200, 2 -> "max value" }
   *    min_value { 1 -> 10, 2 -> "min value" }
   *    null_counts { 1 -> 0, 2 -> 20 }
   * will be converted into the following Delta style stats as a Json str
   *
   * {
   *    numRecords: 100,
   *    maxValues: {
   *      "col1": 200,
   *      "col2" "max value"
   *    },
   *    minValues: {
   *      "col1": 10,
   *      "col2": "min value"
   *    },
   *    nullCount: {
   *      "col1": 0,
   *      "col2": 20
   *    }
   * }
   **********************************************************
   *
   * See also [[org.apache.spark.sql.delta.stats.StatsCollectionUtils]] for more
   * about Delta stats.
   *
   * @param icebergSchema          Iceberg table schema
   * @param numRecords             Iceberg stats of numRecords
   * @param maxMap                 Iceberg stats of max value ( field_id -> value )
   * @param minMap                 Iceberg stats of min value ( field_id -> value )
   * @param nullCountMap           Iceberg stats of null count ( field_id -> value )
   * @param statsAllowTypes        dataTypes that will convert stats to Delta
   * @return json string representing Delta stats
   */
  private[convert] def icebergStatsToDelta(
      icebergSchema: Schema,
      numRecords: Long,
      maxMap: Map[JInt, ByteBuffer],
      minMap: Map[JInt, ByteBuffer],
      nullCountMap: Map[JInt, JLong],
      statsAllowTypes: Set[TypeID]): String = {

    def deserialize(ftype: IcebergType, value: Any): Any = {
      (ftype, value) match {
        case (_, null) => null
        case (_: IcebergStringType, bb: ByteBuffer) =>
          Conversions.fromByteBuffer(ftype, bb).toString
        case (_: IcebergDateType, bb: ByteBuffer) =>
          val daysFromEpoch = Conversions.fromByteBuffer(ftype, bb).asInstanceOf[Int]
          DateTimeUtil.dateFromDays(daysFromEpoch).toString
        case (tsType: IcebergTimestampType, bb: ByteBuffer) =>
          val microts = Conversions.fromByteBuffer(tsType, bb).asInstanceOf[JLong]
          microTimestampToString(microts, tsType)
        case (_, bb: ByteBuffer) =>
          Conversions.fromByteBuffer(ftype, bb)
        case _ => throw new IllegalArgumentException("unable to deserialize unknown values")
      }
    }

    // Recursively collect stats from the given fields list and values and
    // use the given deserializer to format the value.
    // The result is a map of ( delta column physical name -> value )
    def collectStats(
        fields: java.util.List[NestedField],
        valueMap: Map[JInt, Any],
        deserializer: (IcebergType, Any) => Any,
        statsAllowTypes: Set[TypeID]): Map[String, Any] = {
      fields.asScala.flatMap { field =>
        field.`type`() match {
          // Both Iceberg and Delta do not maintain stats for List/Map. Ignore them
          case st: IcebergStructType =>
            Some(field.name ->
              collectStats(st.fields, valueMap, deserializer, statsAllowTypes))
          case pt: IcebergPrimitiveType
            if valueMap.contains(field.fieldId) && statsAllowTypes.contains(pt.typeId) =>
            Option(deserializer(pt, valueMap(field.fieldId))).map(field.name -> _)
          case _ => None
        }
      }.toMap
    }

    JsonUtils.toJson(
      Map(
        NUM_RECORDS -> numRecords,
        MAX -> collectStats(icebergSchema.columns, maxMap, deserialize, statsAllowTypes),
        MIN -> collectStats(icebergSchema.columns, minMap, deserialize, statsAllowTypes),
        NULL_COUNT -> collectStats(
          icebergSchema.columns, nullCountMap, (_: IcebergType, v: Any) => v, statsAllowTypes
        )
      )
    )
  }

  private def microTimestampToString(
      microTS: JLong, tsType: IcebergTimestampType): String = {
    // iceberg timestamptz will have shouldAdjustToUTC() as true
    if (tsType.shouldAdjustToUTC()) {
      DateTimeUtil.microsToIsoTimestamptz(microTS)
    } else {
    // iceberg timestamp doesn't need to adjust to UTC
      DateTimeUtil.microsToIsoTimestamp(microTS)
    }
  }
}
