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
import java.math.BigDecimal
import java.util.{List => JList}

import scala.collection.JavaConverters._

import org.apache.iceberg.{PartitionData, PartitionSpec, Schema}
import org.apache.iceberg.transforms._
import org.apache.iceberg.types.Conversions
import org.apache.iceberg.types.Types._

import org.apache.spark.SparkFunSuite

class IcebergPartitionConverterSuite extends SparkFunSuite {

  test("convert partition simple case, including empty and null") {
    val icebergSchema = new Schema(10, Seq[NestedField](
      NestedField.required(1, "col_int", IntegerType.get),
      NestedField.required(2, "col_long", LongType.get),
      NestedField.required(3, "col_st", StringType.get)
    ).asJava)

    val icebergPartSpec = PartitionSpec
      .builderFor(icebergSchema)
      .identity("col_int")
      .truncate("col_st", 3)
      .identity("col_long")
      .build

    val physicalNameToField = Map(
      "pname1" -> icebergPartSpec.fields().get(0),
      "pname2" -> icebergPartSpec.fields().get(1),
      "pname3" -> icebergPartSpec.fields().get(2)
    )

    val partitionConverter = IcebergPartitionConverter(icebergSchema, physicalNameToField)

    val partData = new PartitionData(
      StructType.of(
        NestedField.required(1000, "col_int", IntegerType.get),
        NestedField.required(1001, "col_st", StringType.get)
      )
    )
    partData.put(0, 100)
    partData.put(1, "alo")
    assertResult("Map(pname1 -> 100, pname2 -> alo, pname3 -> null)")(
      partitionConverter.toDelta(partData).toString)

    val partData2 = new PartitionData(
      StructType.of(
        NestedField.required(1000, "col_int", IntegerType.get),
        NestedField.required(1001, "col_long", LongType.get),
        NestedField.required(1002, "col_st", StringType.get)
      )
    )
    partData2.put(2, 100000000000000L)
    partData2.put(1, null)
    assertResult("Map(pname1 -> null, pname2 -> null, pname3 -> 100000000000000)")(
      partitionConverter.toDelta(partData2).toString)
  }

  test("convert partition with complex types") {
    val icebergSchema = new Schema(10, Seq[NestedField](
      NestedField.required(4, "col_date", DateType.get),
      NestedField.required(5, "col_ts", TimestampType.withZone),
      NestedField.required(6, "col_tsnz", TimestampType.withoutZone)
    ).asJava)

    val icebergPartSpec = PartitionSpec
      .builderFor(icebergSchema)
      .identity("col_date")
      .identity("col_ts")
      .identity("col_tsnz")
      .build

    val physicalNameToField = Map(
      "pname1" -> icebergPartSpec.fields().get(0),
      "pname2" -> icebergPartSpec.fields().get(1),
      "pname3" -> icebergPartSpec.fields().get(2)
    )

    val partitionConverter = IcebergPartitionConverter(icebergSchema, physicalNameToField)

    val partData = new PartitionData(
      StructType.of(
        NestedField.required(1000, "col_date", DateType.get),
        NestedField.required(1001, "col_ts", TimestampType.withZone),
        NestedField.required(1002, "col_tsnz", TimestampType.withoutZone)
      )
    )
    partData.put(0, 12800)
    partData.put(1, 1790040414914000L)
    partData.put(2, 1790040414914000L)
    assertResult("Map(pname1 -> 2005-01-17, " +
      "pname2 -> 2026-09-21 18:26:54.9, pname3 -> 2026-09-21 18:26:54.9)")(
      partitionConverter.toDelta(partData).toString)
  }
}
