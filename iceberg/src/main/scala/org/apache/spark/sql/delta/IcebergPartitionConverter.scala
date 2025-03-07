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

import org.apache.spark.sql.delta.DeltaColumnMapping
import org.apache.spark.sql.delta.util.{DateFormatter, TimestampFormatter}
import org.apache.iceberg.{PartitionData, PartitionField, PartitionSpec, Schema, StructLike, Table}
import org.apache.iceberg.transforms.IcebergPartitionUtil
import org.apache.iceberg.types.{Conversions, Type => IcebergType}
import org.apache.iceberg.types.Type.{PrimitiveType => IcebergPrimitiveType, TypeID}
import org.apache.iceberg.types.Types.{
  ListType => IcebergListType,
  MapType => IcebergMapType,
  NestedField,
  StringType => IcebergStringType,
  StructType => IcebergStructType
}

import org.apache.spark.sql.types.StructType


object IcebergPartitionConverter {
  // we must use field id to look up the partition value; consider scenario with iceberg
  // behavior chance since 1.4.0:
  // 1) create table with partition schema (a[col_name]: 1[field_id]), add file1;
  //    The partition data for file1 is (a:1:some_part_value)
  // 2) add new partition col b and the partition schema becomes (a: 1, b: 2), add file2;
  //    the partition data for file2 is (a:1:some_part_value, b:2:some_part_value)
  // 3) remove partition col a, then add file3;
  //    for iceberg < 1.4.0: the partFields is (a:1(void), b:2); the partition data for
  //                         file3 is (a:1(void):null, b:2:some_part_value);
  //    for iceberg 1.4.0:   the partFields is (b:2); When it reads file1 (a:1:some_part_value),
  //                         it must use the field_id instead of index to look up the partition
  //                         value, as the partField and partitionData from file1 have different
  //                         ordering and thus same index indicates different column.
  def physicalNameToPartitionField(
      table: Table, partitionSchema: StructType): Map[String, PartitionField] =
    table.spec().fields().asScala.collect {
      case field if field.transform().toString != "void" &&
          !field.transform().toString.contains("bucket") =>
        DeltaColumnMapping.getPhysicalName(partitionSchema(field.name)) -> field
    }.toMap
}

case class IcebergPartitionConverter(
    icebergSchema: Schema,
    physicalNameToPartitionField: Map[String, PartitionField]) {

  val dateFormatter: DateFormatter = DateFormatter()
  val timestampFormatter: TimestampFormatter =
      TimestampFormatter(ConvertUtils.timestampPartitionPattern, java.util.TimeZone.getDefault)

  def this(table: Table, partitionSchema: StructType, partitionEvolutionEnabled: Boolean) =
    this(table.schema(),
      // We only allow empty partition when partition evolution happened
      // This is an extra safety mechanism as we should have already passed
      // a non-bucket partitionSchema when table has >1 specs
      if (table.specs().size() > 1 && !partitionEvolutionEnabled) {
        Map.empty[String, PartitionField]
      } else {
        IcebergPartitionConverter.physicalNameToPartitionField(table, partitionSchema)
      }
    )

  /**
   * Convert an Iceberg [[PartitionData]] into a Map of (columnID -> partitionValue) used by Delta
   */
  def toDelta(partition: StructLike): Map[String, String] = {
    val icebergPartitionData = partition.asInstanceOf[PartitionData]
    val fieldIdToIdx = icebergPartitionData.getPartitionType
      .fields()
      .asScala
      .zipWithIndex
      .map(kv => kv._1.fieldId() -> kv._2)
      .toMap
    val physicalNameToPartValueMap = physicalNameToPartitionField
      .map {
        case (physicalName, field) =>
          val fieldIndex = fieldIdToIdx.get(field.fieldId())
          val partValueAsString = fieldIndex
            .map { idx =>
              val partValue = icebergPartitionData.get(idx)
              IcebergPartitionUtil.partitionValueToString(
                field,
                partValue,
                icebergSchema,
                dateFormatter,
                timestampFormatter
              )
            }
            .getOrElse(null)
          physicalName -> partValueAsString
      }
    physicalNameToPartValueMap
  }
}
