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

package org.apache.iceberg.transforms

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.DeltaColumnMapping
import org.apache.spark.sql.delta.sources.DeltaSourceUtils.GENERATION_EXPRESSION_METADATA_KEY
import org.apache.spark.sql.delta.util.{DateFormatter, TimestampFormatter}
import org.apache.iceberg.{PartitionField, PartitionSpec, Schema, StructLike}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types

import org.apache.spark.sql.types.{DateType, IntegerType, MetadataBuilder, StringType, StructField}

/**
 * Utils to translate Iceberg's partition expressions to Delta generated column expressions.
 */
object IcebergPartitionUtil {

  // scalastyle:off line.size.limit
  /**
   * Convert the partition values stored in Iceberg metadata to string values, which we will
   * directly use in the partitionValues field of AddFiles. Here is how we generate the string
   * value from the Iceberg stored partition value for each of the transforms:
   *
   * Identity
   *   - Iceberg source code: https://github.com/apache/iceberg/blob/4c98a0f6408d4ccd0d47b076b2f7743d836d28ec/api/src/main/java/org/apache/iceberg/transforms/Identity.java
   *   - Source column type: any
   *   - Stored partition value type: same as source type
   *   - String value generation: for timestamp and date, use our Spark formatter; other types use toString
   *
   * Timestamps (year, month, day, hour)
   *  - Iceberg source code: https://github.com/apache/iceberg/blob/4c98a0f6408d4ccd0d47b076b2f7743d836d28ec/api/src/main/java/org/apache/iceberg/transforms/Timestamps.java
   *  - Source column type: timestamp
   *  - Stored partition value type: integer
   *  - String value generation: use Iceberg's Timestamps.toHumanString (which uses yyyy-MM-dd-HH format)
   *
   * Dates (year, month, day)
   *  - Iceberg source code: https://github.com/apache/iceberg/blob/4c98a0f6408d4ccd0d47b076b2f7743d836d28ec/api/src/main/java/org/apache/iceberg/transforms/Dates.java
   *  - Source column type: date
   *  - Stored partition value type: integer
   *  - String value generation: use Iceberg's Dates.toHumanString (which uses yyyy-MM-dd format)
   *
   * Truncate
   *  - Iceberg source code: https://github.com/apache/iceberg/blob/4c98a0f6408d4ccd0d47b076b2f7743d836d28ec/api/src/main/java/org/apache/iceberg/transforms/Truncate.java
   *  - Source column type: string, long and int
   *  - Stored partition value type: string, long and int
   *  - String value generation: directly use toString
   */
  // scalastyle:on line.size.limit
  def partitionValueToString(
      partField: PartitionField,
      partValue: Object,
      schema: Schema,
      dateFormatter: DateFormatter,
      timestampFormatter: TimestampFormatter): String = {
    if (partValue == null) return null
    partField.transform() match {
      case _: Identity[_] =>
        // Identity transform
        // We use our own date and timestamp formatter for date and timestamp types, while simply
        // use toString for other input types.
        val sourceField = schema.findField(partField.sourceId())
        val sourceType = sourceField.`type`()
        if (sourceType.typeId() == TypeID.DATE) {
          // convert epoch days to Spark date formatted string
          dateFormatter.format(partValue.asInstanceOf[Int])
        } else if (sourceType.typeId == TypeID.TIMESTAMP) {
          // convert timestamps to Spark timestamp formatted string
          timestampFormatter.format(partValue.asInstanceOf[Long])
        } else {
          // all other types can directly toString
          partValue.toString
        }
      case ts: Timestamps =>
        // Matches all transforms on Timestamp input type: YEAR, MONTH, DAY, HOUR
        // We directly use Iceberg's toHumanString(), which takes a timestamp type source column and
        // generates the partition value in the string format as follows:
        //  - YEAR: yyyy
        //  - MONTH: yyyy-MM
        //  - DAY: yyyy-MM-dd
        //  - HOUR: yyyy-MM-dd-HH
        ts.toHumanString(Types.TimestampType.withoutZone(), partValue.asInstanceOf[Int])
      case dt: Dates =>
        // Matches all transform on Date input type: YEAR, MONTH, DAY
        // We directly use Iceberg's toHumanString(), which takes a date type source column and
        // generates the partition value in the string format as follows:
        //  - YEAR: yyyy
        //  - MONTH: yyyy-MM
        //  - DAY: yyyy-MM-dd
        dt.toHumanString(Types.DateType.get(), partValue.asInstanceOf[Int])
      case _: Truncate[_] =>
        // Truncate transform
        // While Iceberg Truncate transform supports multiple input types, our converter
        // only supports string and block all other input types. So simply toString suffices.
        partValue.toString
      case other =>
        throw new UnsupportedOperationException(
          s"unsupported partition transform expression when converting to Delta: $other")
    }
  }

  def getPartitionFields(partSpec: PartitionSpec, schema: Schema): Seq[StructField] = {
    // Skip removed partition fields due to partition evolution.
    partSpec.fields.asScala.toSeq.collect {
      case partField if !partField.transform().isInstanceOf[VoidTransform[_]] =>
        val sourceColumnName = schema.findColumnName(partField.sourceId())
        val sourceField = schema.findField(partField.sourceId())
        val sourceType = sourceField.`type`()

        val metadataBuilder = new MetadataBuilder()

        // TODO: Support truncate[Decimal] in partition
        val (transformExpr, targetType) = partField.transform() match {
          // binary partition values are problematic in Delta, so we block converting if the iceberg
          // table has a binary type partition column
          case _: Identity[_] if sourceType.typeId() != TypeID.BINARY =>
            // copy id only for identity transform because source id will be the converted column id
            // ids for other columns will be assigned later automatically during schema evolution
            metadataBuilder
              .putLong(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY, sourceField.fieldId())
            ("", SparkSchemaUtil.convert(sourceType))

          case Timestamps.YEAR | Dates.YEAR =>
            (s"year($sourceColumnName)", IntegerType)

          case Timestamps.DAY | Dates.DAY =>
            (s"cast($sourceColumnName as date)", DateType)

          case t: Truncate[_] if sourceType.typeId() == TypeID.STRING =>
            (s"substring($sourceColumnName, 0, ${t.width()})", StringType)

          case t: Truncate[_]
            if sourceType.typeId() == TypeID.LONG || sourceType.typeId() == TypeID.INTEGER =>
            (icebergNumericTruncateExpression(sourceColumnName, t.width().toLong),
              SparkSchemaUtil.convert(sourceType))

          case Timestamps.MONTH | Dates.MONTH =>
            (s"date_format($sourceColumnName, 'yyyy-MM')", StringType)

          case Timestamps.HOUR =>
            (s"date_format($sourceColumnName, 'yyyy-MM-dd-HH')", StringType)

          case other =>
            throw new UnsupportedOperationException(
              s"Unsupported partition transform expression when converting to Delta: " +
                s"transform: $other, source data type: ${sourceType.typeId()}")
        }

        if (transformExpr != "") {
          metadataBuilder.putString(GENERATION_EXPRESSION_METADATA_KEY, transformExpr)
        }

        Option(sourceField.doc()).foreach { comment =>
          metadataBuilder.putString("comment", comment)
        }

        val metadata = metadataBuilder.build()

        StructField(partField.name(),
          targetType,
          nullable = sourceField.isOptional(),
          metadata = metadata)
    }
  }

  /**
   * Returns the iceberg transform function of truncate[Integer] and truncate[Long] as an
   * expression string, please check the iceberg documents for more details:
   *
   *    https://iceberg.apache.org/spec/#truncate-transform-details
   *
   * TODO: make this partition expression optimizable.
   */
  private def icebergNumericTruncateExpression(colName: String, width: Long): String =
    s"$colName - (($colName % $width) + $width) % $width"
}
