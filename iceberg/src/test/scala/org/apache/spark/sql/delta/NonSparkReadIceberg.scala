/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.uniform.UniFormE2ETest
import org.apache.iceberg.data.{GenericRecord, IcebergGenerics, Record}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types.{StructType => IcebergStructType}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

trait NonSparkReadIceberg extends UniFormE2ETest {
  implicit class IcebergRecordConvert(record: Record) {
    def convertRecordToRow(struct: IcebergStructType): Row = {
      val values = struct.fields().asScala.map { field =>
        convertValue(record.getField(field.name), field.`type`)
      }.toSeq
      Row.fromSeq(values)
    }

    def convertValue(value: Any, tpe: Type): Any = {
      if (value == null) return null
      tpe.typeId() match {
        case TypeID.INTEGER | TypeID.LONG | TypeID.FLOAT | TypeID.DOUBLE | TypeID.BOOLEAN |
             TypeID.STRING | TypeID.BINARY | TypeID.DATE | TypeID.TIME |
             TypeID.UUID | TypeID.DECIMAL =>
          value
        case TypeID.TIMESTAMP =>
          java.sql.Timestamp.from(value.asInstanceOf[java.time.OffsetDateTime].toInstant())
        case TypeID.STRUCT =>
          val struct = tpe.asStructType()
          val rec = value.asInstanceOf[Record]
          rec.convertRecordToRow(struct)

        case TypeID.LIST =>
          val listType = tpe.asListType()
          val elemType = listType.elementType()
          value.asInstanceOf[java.util.List[_]].asScala.map(elem => convertValue(elem, elemType))

        case TypeID.MAP =>
          val mapType = tpe.asMapType()
          val keyType = mapType.keyType()
          val valueType = mapType.valueType()
          value
            .asInstanceOf[java.util.Map[_, _]]
            .asScala
            .map {
              case (k, v) => convertValue(k, keyType) -> convertValue(v, valueType)
            }
            .toMap

        case other =>
          throw new UnsupportedOperationException(s"Unsupported Iceberg type: $other")
      }
    }
  }

  protected def readIcebergByPath(
      icebergMetadataPath: String, orderBy: String, schema: StructType): DataFrame = {
    // scalastyle:off deltahadoopconfiguration
    val hadoopTables = new HadoopTables(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    val hadoopTable = hadoopTables.load(icebergMetadataPath)

    val result = IcebergGenerics
      .read(hadoopTable)
      .build()
      .asScala
      .toList

    val sorted = result.head.getField(orderBy) match {
      case _: Integer => result.sortBy(_.getField(orderBy).asInstanceOf[Integer])
      case _: java.lang.Long => result.sortBy(_.getField(orderBy).asInstanceOf[java.lang.Long])
      case _: BigDecimal => result.sortBy(_.getField(orderBy).asInstanceOf[BigDecimal])
      case _: String => result.sortBy(_.getField(orderBy).asInstanceOf[String])
      case input => throw new UnsupportedOperationException(input.getClass.getSimpleName)
    }

    spark.createDataFrame(
      sorted.map(_.convertRecordToRow(hadoopTable.schema().asStruct())).asJava,
      schema
    )
  }

  protected def verifyReadByPath(
      icebergMetadataPath: String,
      schema: StructType,
      fields: String,
      orderBy: String,
      expect: Seq[Row]): Unit = {
    val fieldCols = fields.split("\\s*,\\s*").map(col)
    checkAnswer(
      readIcebergByPath(icebergMetadataPath, orderBy, schema).select(fieldCols: _*),
      expect
    )
  }
}
