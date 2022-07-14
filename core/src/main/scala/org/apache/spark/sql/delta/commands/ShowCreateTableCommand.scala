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

package org.apache.spark.sql.delta.commands

import org.apache.hadoop.fs.Path

import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Table, TableCatalog}
import org.apache.spark.sql.catalyst.util.escapeSingleQuotedString
import org.apache.spark.sql.delta.DeltaErrors.cannotShowCreateGeneratedColumnsProperty
import org.apache.spark.sql.delta.DeltaTableIdentifier
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * A command for describing the details of a table such as the format, name, and size.
 * Implementation partially reused
 * from [https://github.com/apache/spark/blob/branch-3.3/
 * sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/ShowCreateTableExec.scala]
 */
case class ShowCreateTableCommand(tableID: DeltaTableIdentifier)
  extends LeafRunnableCommand with DeltaCommand {

  case class ShowCreateTableOutput(createtab_stmt: String)

  override val output: Seq[Attribute] = ExpressionEncoder[ShowCreateTableOutput]().
    schema.toAttributes

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val builder = new mutable.StringBuilder
    val md = sparkSession.sessionState.catalog.getTableMetadata(tableID.table.get)
    val dt = DeltaTableV2(sparkSession, new Path(md.location), Option(md))
    showCreateTable(dt, builder)
    Seq(Row(UTF8String.fromString(builder.toString)))
  }

  private def showCreateTable(table: Table, builder: mutable.StringBuilder): Unit = {
    builder ++= s"CREATE TABLE ${table.name()} "

    showTableDataColumns(table, builder)
    showTableUsing(table, builder)

    val tableOptions = table.properties.asScala
      .filterKeys(_.startsWith(TableCatalog.OPTION_PREFIX)).map {
      case (k, v) => k.drop(TableCatalog.OPTION_PREFIX.length) -> v
    }.toMap
    showTableOptions(builder, tableOptions)
    showTablePartitioning(table, builder)
    showTableComment(table, builder)
    showTableLocation(table, builder)
    showTableProperties(table, builder, tableOptions)
  }

  private def showTableDataColumns(table: Table, builder: mutable.StringBuilder): Unit = {
    val tableSchema = table.asInstanceOf[DeltaTableV2].deltaLog.snapshot.metadata.schema
    if (tableSchema.exists(column => column.metadata.contains(
      DeltaSourceUtils.GENERATION_EXPRESSION_METADATA_KEY))) {
      throw cannotShowCreateGeneratedColumnsProperty(table.name())
    }
    val columns = CharVarcharUtils.getRawSchema(table.schema()).fields.map(_.toDDL)
    builder ++= concatByMultiLines(columns)
  }

  private def showTableUsing(table: Table, builder: mutable.StringBuilder): Unit = {
    builder.append("USING delta\n")
  }

  private def showTableOptions(
                                builder: mutable.StringBuilder,
                                tableOptions: Map[String, String]): Unit = {
    if (tableOptions.nonEmpty) {
      val props = conf.redactOptions(tableOptions).toSeq.sortBy(_._1).map {
        case (key, value) =>
          s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }
      builder ++= "OPTIONS "
      builder ++= concatByMultiLines(props)
    }
  }

  private def showTablePartitioning(table: Table, builder: mutable.StringBuilder): Unit = {
    if (!table.partitioning.isEmpty) {
      val transforms = new ArrayBuffer[String]
      table.partitioning.map { t =>
        transforms += t.describe()
      }
      if (transforms.nonEmpty) {
        builder ++= s"PARTITIONED BY ${transforms.mkString("(", ", ", ")")}\n"
      }
    }
  }

  private def showTableLocation(table: Table, builder: mutable.StringBuilder): Unit = {
    val isExternalOption = Option(table.properties().get(DeltaTableV2.PROP_TYPE))
    // Only generate LOCATION clause if it's not managed.
    if (isExternalOption.forall(_.equalsIgnoreCase("EXTERNAL"))) {
      Option(table.properties.get(DeltaTableV2.PROP_LOCATION))
        .map("LOCATION '" + escapeSingleQuotedString(_) + "'\n")
        .foreach(builder.append)
    }
  }

  private def showTableProperties(
                                   table: Table,
                                   builder: mutable.StringBuilder,
                                   tableOptions: Map[String, String]): Unit = {

    val showProps = table.properties.asScala
      .filterKeys(key => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(key)
        && !key.startsWith(TableCatalog.OPTION_PREFIX)
        && !tableOptions.contains(key))
    if (showProps.nonEmpty) {
      val props = showProps.toSeq.sortBy(_._1).map {
        case (key, value) =>
          s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      builder ++= "TBLPROPERTIES "
      builder ++= concatByMultiLines(props)
    }
  }

  private def showTableComment(table: Table, builder: mutable.StringBuilder): Unit = {
    Option(table.properties.get(TableCatalog.PROP_COMMENT))
      .map("COMMENT '" + escapeSingleQuotedString(_) + "'\n")
      .foreach(builder.append)
  }

  private def concatByMultiLines(iter: Iterable[String]): String = {
    iter.mkString("(\n  ", ",\n  ", ")\n")
  }
}
