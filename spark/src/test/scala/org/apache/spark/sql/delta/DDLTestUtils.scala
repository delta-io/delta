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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, LongType, StructField}

/**
 * Base trait for specifying column definitions in tests in an API agnostic way.
 *
 * Note: we don't use StructField because StructField is defined in Spark. It's easier to
 * write tests with flexible helpers in our own project.
 */
trait ColumnSpec {
  /** Name of the column. */
  def colName: String

  /** Spark logical type for the column. */
  def dataType: DataType

  /** Returns a String which can be used to define the column in SQL. */
  def ddl: String

  /** Return the specification as a StructField */
  def structField(spark: SparkSession): StructField
}

case class GeneratedColumnSpec(
    colName: String,
    dataType: DataType,
    generatedExpression: String)
  extends ColumnSpec {

  override def ddl: String =
    s"$colName ${dataType.sql} GENERATED ALWAYS AS ($generatedExpression)"

  override def structField(spark: SparkSession): StructField = {
    io.delta.tables.DeltaTable.columnBuilder(spark, colName)
      .dataType(dataType)
      .generatedAlwaysAs(generatedExpression)
      .build()
  }
}

case class TestColumnSpec(
    colName: String,
    dataType: DataType)
  extends ColumnSpec {

  override def ddl: String = {
    s"$colName ${dataType.sql}"
  }

  override def structField(spark: SparkSession): StructField = {
    io.delta.tables.DeltaTable.columnBuilder(spark, colName)
      .dataType(dataType)
      .build()
  }
}

object GeneratedAsIdentityType extends Enumeration {
  type GeneratedAsIdentityType = Value
  val GeneratedAlways, GeneratedByDefault = Value
}

case class IdentityColumnSpec(
    generatedAsIdentityType: GeneratedAsIdentityType.GeneratedAsIdentityType,
    startsWith: Option[Long] = None,
    incrementBy: Option[Long] = None,
    colName: String = "id",
    dataType: DataType = LongType,
    comment: Option[String] = None)
  extends ColumnSpec {

  override def ddl: String = {
    throw new UnsupportedOperationException(
      "DDL generation is not supported for identity columns yet")
  }

  override def structField(spark: SparkSession): StructField = {
    var col = io.delta.tables.DeltaTable.columnBuilder(spark, colName)
      .dataType(dataType)
    val start = startsWith.getOrElse(IdentityColumn.defaultStart.toLong)
    val step = incrementBy.getOrElse(IdentityColumn.defaultStep.toLong)
    col = generatedAsIdentityType match {
      case GeneratedAsIdentityType.GeneratedAlways =>
        col.generatedAlwaysAsIdentity(start, step)
      case GeneratedAsIdentityType.GeneratedByDefault =>
        col.generatedByDefaultAsIdentity(start, step)
    }

    comment.foreach { c =>
      col = col.comment(c)
    }

    col.build()
  }
}

trait DDLTestUtils extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {
  protected object DDLType extends Enumeration {
    val CREATE, REPLACE, CREATE_OR_REPLACE = Value
  }

  /** Interface (SQL, Scala) agnostic helper to execute the DDL statement. */
  protected def runDDL(
      ddlType: DDLType.Value,
      tableName: String,
      columnSpecs: Seq[ColumnSpec],
      partitionedBy: Seq[String],
      tblProperties: Map[String, String]): Unit

  def createTable(
      tableName: String,
      columnSpecs: Seq[ColumnSpec],
      partitionedBy: Seq[String] = Nil,
      tblProperties: Map[String, String] = Map.empty): Unit = {
    runDDL(DDLType.CREATE, tableName, columnSpecs, partitionedBy, tblProperties)
  }

  def replaceTable(
      tableName: String,
      columnSpecs: Seq[ColumnSpec],
      partitionedBy: Seq[String] = Nil,
      tblProperties: Map[String, String] = Map.empty): Unit = {
    runDDL(DDLType.REPLACE, tableName, columnSpecs, partitionedBy, tblProperties)
  }

  def createOrReplaceTable(
      tableName: String,
      columnSpecs: Seq[ColumnSpec],
      partitionedBy: Seq[String] = Nil,
      tblProperties: Map[String, String] = Map.empty): Unit = {
    runDDL(DDLType.CREATE_OR_REPLACE, tableName, columnSpecs, partitionedBy, tblProperties)
  }
}


trait SQLDDLTestUtils extends DDLTestUtils {
  private def getPartitionByClause(partitionedBy: Seq[String]): String = {
    if (partitionedBy.nonEmpty) {
      s"PARTITIONED BY (${partitionedBy.mkString(", ")})"
    } else {
      ""
    }
  }

  protected def runDDL(
      ddlType: DDLType.Value,
      tableName: String,
      columnSpecs: Seq[ColumnSpec],
      partitionedBy: Seq[String],
      tblProperties: Map[String, String]): Unit = {
    val columnDefinitions = columnSpecs.map(_.ddl).mkString(",\n")
    val ddlClause = ddlType match {
      case DDLType.CREATE =>
        "CREATE TABLE"
      case DDLType.REPLACE =>
        "REPLACE TABLE"
      case DDLType.CREATE_OR_REPLACE =>
        "CREATE OR REPLACE TABLE"
    }

    val tblPropertiesClause = if (tblProperties.nonEmpty) {
      val tblPropertiesStr =
        tblProperties.map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")
      s"TBLPROPERTIES ($tblPropertiesStr)"
    } else {
      ""
    }

    sql(
      s"""
         |$ddlClause $tableName(
         |  $columnDefinitions
         |) USING delta
         |${getPartitionByClause(partitionedBy)}
         |$tblPropertiesClause
         |""".stripMargin)
  }
}

trait ScalaDDLTestUtils extends DDLTestUtils {
  protected def runDDL(
      ddlType: DDLType.Value,
      tableName: String,
      columnSpecs: Seq[ColumnSpec],
      partitionedBy: Seq[String],
      tblProperties: Map[String, String]): Unit = {
    val builder = ddlType match {
      case DDLType.CREATE =>
        io.delta.tables.DeltaTable.create(spark)
      case DDLType.REPLACE =>
        io.delta.tables.DeltaTable.replace(spark)
      case DDLType.CREATE_OR_REPLACE =>
        io.delta.tables.DeltaTable.createOrReplace(spark)
    }

    builder.tableName(tableName)

    columnSpecs.foreach { columnSpec =>
      val colAsStructField = columnSpec.structField(spark)
      builder.addColumn(colAsStructField)
    }

    if (partitionedBy.nonEmpty) {
      builder.partitionedBy(partitionedBy: _*)
    }

    for ((key, value) <- tblProperties) {
      builder.property(key, value)
    }

    builder.execute()
  }
}
