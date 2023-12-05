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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import java.io.PrintWriter

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.schema.{DeltaInvariantViolationException, InvariantViolationException, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils.GENERATION_EXPRESSION_METADATA_KEY
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import io.delta.tables.DeltaTableBuilder

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getZoneId, stringToDate, stringToTimestamp, toJavaDate, toJavaTimestamp}
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, DateType, IntegerType, MetadataBuilder, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

trait GeneratedColumnTest extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {


  protected def sqlDate(date: String): java.sql.Date = {
    toJavaDate(stringToDate(UTF8String.fromString(date)).get)
  }

  protected def sqlTimestamp(timestamp: String): java.sql.Timestamp = {
    toJavaTimestamp(stringToTimestamp(
      UTF8String.fromString(timestamp),
      getZoneId(SQLConf.get.sessionLocalTimeZone)).get)
  }

  protected def withTableName[T](tableName: String)(func: String => T): Unit = {
    withTable(tableName) {
      func(tableName)
    }
  }

  /** Create a new field with the given generation expression. */
  def withGenerationExpression(field: StructField, expr: String): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(field.metadata)
      .putString(GENERATION_EXPRESSION_METADATA_KEY, expr)
      .build()
    field.copy(metadata = newMetadata)
  }

  protected def buildTable(
      builder: DeltaTableBuilder,
      tableName: String,
      path: Option[String],
      schemaString: String,
      generatedColumns: Map[String, String],
      partitionColumns: Seq[String],
      notNullColumns: Set[String],
      comments: Map[String, String],
      properties: Map[String, String]): DeltaTableBuilder = {
    val schema = if (schemaString.nonEmpty) {
      StructType.fromDDL(schemaString)
    } else {
      new StructType()
    }
    val cols = schema.map(field => (field.name, field.dataType))
    if (tableName != null) {
      builder.tableName(tableName)
    }
    cols.foreach(col => {
      val (colName, dataType) = col
      val nullable = !notNullColumns.contains(colName)
      var columnBuilder = io.delta.tables.DeltaTable.columnBuilder(spark, colName)
      columnBuilder.dataType(dataType.sql)
      columnBuilder.nullable(nullable)
      if (generatedColumns.contains(colName)) {
        columnBuilder.generatedAlwaysAs(generatedColumns(colName))
      }
      if (comments.contains(colName)) {
        columnBuilder.comment(comments(colName))
      }
      builder.addColumn(columnBuilder.build())
    })
    if (partitionColumns.nonEmpty) {
      builder.partitionedBy(partitionColumns: _*)
    }
    if (path.nonEmpty) {
      builder.location(path.get)
    }
    properties.foreach { case (key, value) =>
      builder.property(key, value)
    }
    builder
  }

  protected def createTable(
      tableName: String,
      path: Option[String],
      schemaString: String,
      generatedColumns: Map[String, String],
      partitionColumns: Seq[String],
      notNullColumns: Set[String] = Set.empty,
      comments: Map[String, String] = Map.empty,
      properties: Map[String, String] = Map.empty): Unit = {
    var tableBuilder = io.delta.tables.DeltaTable.create(spark)
    buildTable(tableBuilder, tableName, path, schemaString,
      generatedColumns, partitionColumns, notNullColumns, comments, properties)
      .execute()
  }
}


