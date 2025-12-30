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

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.time.{LocalDateTime, OffsetDateTime}
import java.time.format._
import java.util.{Base64, List => JList}

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DeltaConfig, DeltaConfigs, IcebergCompat, NoMapping, Snapshot, SnapshotDescriptor}
import org.apache.spark.sql.delta.DeltaConfigs.{LOG_RETENTION, TOMBSTONE_RETENTION}
import org.apache.spark.sql.delta.actions.{AddFile, FileAction}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.JsonUtils
import shadedForDelta.org.apache.iceberg.{FileMetadata, PartitionData, PartitionSpec, Schema => IcebergSchema, StructLike, TableProperties => IcebergTableProperties}
import shadedForDelta.org.apache.iceberg.expressions.Literal
import shadedForDelta.org.apache.iceberg.types.{Conversions, Type => IcebergType, Types => IcebergTypes}

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Generate Iceberg table metadata (schema, partition, etc.) from a Delta [[Snapshot]]
 */
class DeltaToIcebergConverter(val snapshot: SnapshotDescriptor, val catalogTable: CatalogTable) {

  private val schemaUtils: IcebergSchemaUtils =
    IcebergSchemaUtils(snapshot.metadata.columnMappingMode == NoMapping)

  def maxFieldId: Int = schemaUtils.maxFieldId(snapshot)

  val schema: IcebergSchema = IcebergCompat
    .getEnabledVersion(snapshot.metadata)
    .orElse(Some(0))
    .map { compatVersion =>
      val icebergStruct = schemaUtils.convertStruct(snapshot.schema)(compatVersion)
      new IcebergSchema(icebergStruct.fields())
    }.getOrElse(throw new IllegalArgumentException("No IcebergCompat available"))

  val partition: PartitionSpec = IcebergTransactionUtils
    .createPartitionSpec(schema, snapshot.metadata.partitionColumns)

  val properties: Map[String, String] =
    DeltaToIcebergConvert.TableProperties(snapshot.metadata.configuration)
}
/**
 * Utils for converting a Delta Table to Iceberg Table
 */
object DeltaToIcebergConvert
  extends DeltaLogging
  {
  object Action
    extends DeltaLogging
    {
      def buildPartitionValues(
          builder: FileMetadata.Builder,
          fileAction: FileAction,
          partitionSpec: PartitionSpec,
          snapshot: Snapshot,
          logicalToPhysicalPartitionNames: Map[String, String]): Unit = {
        if (partitionSpec.isPartitioned) {
            builder.withPartition(
              DeltaToIcebergConvert.Partition.convertPartitionValues(
                snapshot,
                partitionSpec,
                fileAction.partitionValues,
                logicalToPhysicalPartitionNames))
        }
      }
    }
  /**
   * Utils used when converting Delta schema to Iceberg
   */
  object Schema {
    /**
     * Extract Delta Column Default values in Iceberg Literal format
     * @param field column
     * @return Right(Some(Literal)) if the column contains a literal default
     *         Right(None) if the column does not have a default
     *         Left(errorMessage) if the column contains a non-literal default
     */
    def extractLiteralDefault(field: StructField): Either[String, Option[Literal[_]]] = {
      if (field.metadata.contains(CURRENT_DEFAULT_COLUMN_METADATA_KEY)) {
        val defaultValueStr = field.metadata.getString(CURRENT_DEFAULT_COLUMN_METADATA_KEY)
        try {
            Right(Some(stringToLiteral(defaultValueStr, field.dataType)))
        } catch {
          case NonFatal(e) =>
            Left("Unsupported default value:" +
              s"${field.dataType.typeName}:$defaultValueStr:${e.getMessage}")
          case unknown: Throwable => throw unknown
        }
      } else {
        Right(None)
      }
    }
    /**
     * Convert Delta default value string to an Iceberg Literal based on data type.
     * @param str default value in Delta column metadata
     * @param dataType Delta column data type
     * @return converted Literal
     */
    def stringToLiteral(str: String, dataType: DataType): Literal[_] = {
      def parseString(input: String) = {
        if (input.length > 1 && ((input.head == '\'' && input.last == '\'')
          || (input.head == '"' && input.last == '"'))) {
          Literal.of(input.substring(1, input.length - 1))
        } else {
          throw new UnsupportedOperationException(s"String missing quotation marks: $input")
        }
      }
      // Parse either hex encoded literal x'....' or string literal(utf8) into binary
      def parseBinary(input: String) = {
        if (input.startsWith("x") || input.startsWith("X")) {
          // Hex encoded literal
          Literal.of(BigInt(parseString(input.substring(1))
              .value().toString, 16).toByteArray.dropWhile(_ == 0))
        } else {
          Literal.of(parseString(input).value().toString
            .getBytes(java.nio.charset.StandardCharsets.UTF_8))
        }
      }
      // Parse timestamp string without time zone info
      def parseLocalTimestamp(input: String) = {
        val formats = Seq(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
          DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        val stripped = parseString(input).value()
        val parsed = formats.flatMap { format =>
          try {
            Some(Literal.of(Timestamp.valueOf(LocalDateTime.parse(stripped, format)).getTime))
          } catch {
            case NonFatal(_) => None
          }
        }
        if (parsed.nonEmpty) {
          parsed.head
        } else {
          throw new IllegalArgumentException(input)
        }
      }
      // Parse string with time zone info. If the input has no time zone, assume its UTC.
      def parseTimestamp(input: String) = {
        val stripped = parseString(input).value()
        try {
          Literal.of(OffsetDateTime.parse(stripped, DateTimeFormatter.ISO_DATE_TIME)
            .toInstant.toEpochMilli)
        } catch {
          case NonFatal(_) => parseLocalTimestamp(input)
        }
      }

      dataType match {
        case StringType => parseString(str)
        case LongType => Literal.of(java.lang.Long.valueOf(str.replaceAll("[lL]$", "")))
        case IntegerType | ShortType | ByteType => Literal.of(Integer.valueOf(str))
        case FloatType => Literal.of(java.lang.Float.valueOf(str))
        case DoubleType => Literal.of(java.lang.Double.valueOf(str))
        // The number should be correctly formatted without need to rounding
        case d: DecimalType => Literal.of(
          new java.math.BigDecimal(str, new java.math.MathContext(d.precision)).setScale(d.scale)
        )
        case BooleanType => Literal.of(java.lang.Boolean.valueOf(str))
        case BinaryType => parseBinary(str)
        case DateType => parseString(str).to(IcebergTypes.DateType.get())
        case TimestampType => parseTimestamp(str)
        case TimestampNTZType => parseLocalTimestamp(str)
        case _ =>
          throw new UnsupportedOperationException(
            s"Could not convert default value: $dataType: $str")
      }
    }
  }

  object TableProperties
  {
    /**
     * We generate Iceberg Table properties from Delta table properties
     * using two methods.
     * 1. If a Delta property key starts with "delta.universalformat.config.iceberg"
     * we strip the prefix from the key and include the property pair.
     * Note the key is already normalized to lower case.
     * 2. We compute Iceberg properties from Delta using custom logic
     * This now includes
     * a) Iceberg format version
     * b) Iceberg snapshot retention
     */
    def apply(deltaProperties: Map[String, String]): Map[String, String] = {
      val prefix = DeltaConfigs.DELTA_UNIVERSAL_FORMAT_ICEBERG_CONFIG_PREFIX
      val copiedFromDelta =
        deltaProperties
          .filterKeys(_.startsWith(prefix))
          .map { case (key, value) => key.stripPrefix(prefix) -> value }
          .toSeq
          .toMap
      val computers = Seq(FormatVersionComputer, RetentionPeriodComputer)
      val computed: Map[String, String] = computers
        .map(_.apply(deltaProperties ++ copiedFromDelta))
        .reduce((a, b) => a ++ b)

      copiedFromDelta ++ computed
    }

    private trait IcebergPropertiesComputer {
      /**
       * Compute Iceberg properties from Delta properties.
       */
      def apply(deltaProperties: Map[String, String]): Map[String, String]
    }

    /**
     * Compute Iceberg FORMAT_VERSION from IcebergCompat
     */
    private object FormatVersionComputer extends IcebergPropertiesComputer {
      override def apply(deltaProperties: Map[String, String]): Map[String, String] =
        IcebergCompat
          .anyEnabled(deltaProperties)
          .map(IcebergTableProperties.FORMAT_VERSION -> _.icebergFormatVersion.toString)
          .toMap
    }

    /**
     * Compute Iceberg MAX_SNAPSHOT_AGE_MS as the minimal of
     * Delta's LOG_RETENTION and TOMBSTONE_RETENTION.
     * If users explicitly provide a MAX_SNAPSHOT_AGE_MS, also ensure the provided
     * value is no larger than Delta's retention.
     */
    private object RetentionPeriodComputer extends IcebergPropertiesComputer {
      override def apply(deltaProperties: Map[String, String]): Map[String, String] = {
        def getAsMilliSeconds(conf: DeltaConfig[CalendarInterval],
                              properties: Map[String, String],
                              useDefault: Boolean = false): Option[Long] =
          properties.get(conf.key)
            .orElse(if (useDefault) Some(conf.defaultValue) else None)
            .map(conf.fromString)
            .map(DeltaConfigs.getMilliSeconds)

        // Set Iceberg max snapshot age as minimal of Delta log retention and tombstone retention
        val deltaRetention = (
          getAsMilliSeconds(LOG_RETENTION, deltaProperties),
          getAsMilliSeconds(TOMBSTONE_RETENTION, deltaProperties)
        ) match {
          case (Some(a), Some(b)) => Some(a min b)
          case (a, b) => a orElse b
        }

        // If user provided max snapshot age, check that it is smaller than Delta's retention
        lazy val maxAllowedRetention =
          getAsMilliSeconds(LOG_RETENTION, deltaProperties, useDefault = true).get min
          getAsMilliSeconds(TOMBSTONE_RETENTION, deltaProperties, useDefault = true).get

        deltaProperties.get(IcebergTableProperties.MAX_SNAPSHOT_AGE_MS)
          .foreach { providedRetention =>
            if (providedRetention.toLong > maxAllowedRetention) {
              throw new IllegalArgumentException(
                s"""Uniform iceberg's ${IcebergTableProperties.MAX_SNAPSHOT_AGE_MS} should be
                    | no less than the min of delta's ${LOG_RETENTION.key} and
                    | ${TOMBSTONE_RETENTION.key}.
                    | Current delta retention min in MS: $maxAllowedRetention.
                    | Proposed iceberg retention in Ms: $providedRetention""".stripMargin)
            }
          }

        deltaRetention
          .filter(_ < IcebergTableProperties.MAX_SNAPSHOT_AGE_MS_DEFAULT)
          .map { IcebergTableProperties.MAX_SNAPSHOT_AGE_MS -> _.toString }
          .toMap
      }
    }
  }

  object Partition {

    private[delta] def convertPartitionValues(
        snapshot: Snapshot,
        partitionSpec: PartitionSpec,
        partitionValues: Map[String, String],
        logicalToPhysicalPartitionNames: Map[String, String]): StructLike = {
      val schema = snapshot.schema
      val ICEBERG_NULL_PARTITION_VALUE = "__HIVE_DEFAULT_PARTITION__"
      val partitionPath = partitionSpec.fields()
      val partitionVals = new Array[Any](partitionSpec.fields().size())
      val nameToDataTypes: Map[String, DataType] =
        schema.fields.map(f => f.name -> f.dataType).toMap
      for (i <- partitionVals.indices) {
        val logicalPartCol = partitionPath.get(i).name()
        val physicalPartKey = logicalToPhysicalPartitionNames(logicalPartCol)
        // ICEBERG_NULL_PARTITION_VALUE is referred in Iceberg lib to mark NULL partition value
        val partValue = Option(partitionValues.getOrElse(physicalPartKey, null))
          .getOrElse(ICEBERG_NULL_PARTITION_VALUE)
        val partitionColumnDataType = nameToDataTypes(logicalPartCol)
        val icebergPartitionValue =
          IcebergTransactionUtils.stringToIcebergPartitionValue(
            partitionColumnDataType, partValue, snapshot.version)
        partitionVals(i) = icebergPartitionValue
      }
      new IcebergTransactionUtils.Row(partitionVals)
    }
  }
}
