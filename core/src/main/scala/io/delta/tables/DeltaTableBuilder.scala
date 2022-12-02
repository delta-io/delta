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

package io.delta.tables

import scala.collection.mutable

import org.apache.spark.sql.delta.{DeltaErrors, DeltaTableUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.tables.execution._

import org.apache.spark.annotation._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{CreateTable, LogicalPlan, ReplaceTable}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * :: Evolving ::
 *
 * Builder to specify how to create / replace a Delta table.
 * You must specify the table name or the path before executing the builder.
 * You can specify the table columns, the partitioning columns, the location of the data,
 * the table comment and the property, and how you want to create / replace the Delta table.
 *
 * After executing the builder, an instance of [[DeltaTable]] is returned.
 *
 * Scala example to create a Delta table with generated columns, using the table name:
 * {{{
 *   val table: DeltaTable = DeltaTable.create()
 *     .tableName("testTable")
 *     .addColumn("c1",  dataType = "INT", nullable = false)
 *     .addColumn(
 *       DeltaTable.columnBuilder("c2")
 *         .dataType("INT")
 *         .generatedAlwaysAs("c1 + 10")
 *         .build()
 *     )
 *     .addColumn(
 *       DeltaTable.columnBuilder("c3")
 *         .dataType("INT")
 *         .comment("comment")
 *         .nullable(true)
 *         .build()
 *     )
 *     .partitionedBy("c1", "c2")
 *     .execute()
 * }}}
 *
 * Scala example to create a delta table using the location:
 * {{{
 *   val table: DeltaTable = DeltaTable.createIfNotExists(spark)
 *     .location("/foo/`bar`")
 *     .addColumn("c1", dataType = "INT", nullable = false)
 *     .addColumn(
 *       DeltaTable.columnBuilder(spark, "c2")
 *         .dataType("INT")
 *         .generatedAlwaysAs("c1 + 10")
 *         .build()
 *     )
 *     .addColumn(
 *       DeltaTable.columnBuilder(spark, "c3")
 *         .dataType("INT")
 *         .comment("comment")
 *         .nullable(true)
 *         .build()
 *     )
 *     .partitionedBy("c1", "c2")
 *     .execute()
 * }}}
 *
 * Java Example to replace a table:
 * {{{
 *   DeltaTable table = DeltaTable.replace()
 *     .tableName("db.table")
 *     .addColumn("c1",  "INT", false)
 *     .addColumn(
 *       DeltaTable.columnBuilder("c2")
 *         .dataType("INT")
 *         .generatedAlwaysBy("c1 + 10")
 *         .build()
 *     )
 *     .execute();
 * }}}
 *
 * @since 1.0.0
 */
@Evolving
class DeltaTableBuilder private[tables](
    spark: SparkSession,
    builderOption: DeltaTableBuilderOptions) {
  private var identifier: String = null
  private var partitioningColumns: Option[Seq[String]] = None
  private var columns: mutable.Seq[StructField] = mutable.Seq.empty
  private var location: Option[String] = None
  private var tblComment: Option[String] = None
  private var properties =
    if (spark.sessionState.conf.getConf(DeltaSQLConf.TABLE_BUILDER_FORCE_TABLEPROPERTY_LOWERCASE)) {
      CaseInsensitiveMap(Map.empty[String, String])
    } else {
      Map.empty[String, String]
    }


  private val FORMAT_NAME: String = "delta"

  /**
   * :: Evolving ::
   *
   * Specify the table name, optionally qualified with a database name [database_name.] table_name
   *
   * @param identifier string the table name
   * @since 1.0.0
   */
  @Evolving
  def tableName(identifier: String): DeltaTableBuilder = {
    this.identifier = identifier
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify the table comment to describe the table.
   *
   * @param comment string table comment
   * @since 1.0.0
   */
  @Evolving
  def comment(comment: String): DeltaTableBuilder = {
    tblComment = Option(comment)
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify the path to the directory where table data is stored,
   * which could be a path on distributed storage.
   *
   * @param location string the data location
   * @since 1.0.0
   */
  @Evolving
  def location(location: String): DeltaTableBuilder = {
    this.location = Option(location)
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify a column.
   *
   * @param colName string the column name
   * @param dataType string the DDL data type
   * @since 1.0.0
   */
  @Evolving
  def addColumn(colName: String, dataType: String): DeltaTableBuilder = {
    addColumn(
      DeltaTable.columnBuilder(spark, colName).dataType(dataType).build
    )
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify a column.
   *
   * @param colName string the column name
   * @param dataType dataType the DDL data type
   * @since 1.0.0
   */
  @Evolving
  def addColumn(colName: String, dataType: DataType): DeltaTableBuilder = {
    addColumn(
      DeltaTable.columnBuilder(spark, colName).dataType(dataType).build
    )
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify a column.
   *
   * @param colName string the column name
   * @param dataType string the DDL data type
   * @param nullable boolean whether the column is nullable
   * @since 1.0.0
   */
  @Evolving
  def addColumn(colName: String, dataType: String, nullable: Boolean): DeltaTableBuilder = {
    addColumn(
      DeltaTable.columnBuilder(spark, colName).dataType(dataType).nullable(nullable).build
    )
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify a column.
   *
   * @param colName string the column name
   * @param dataType dataType the DDL data type
   * @param nullable boolean whether the column is nullable
   * @since 1.0.0
   */
  @Evolving
  def addColumn(colName: String, dataType: DataType, nullable: Boolean): DeltaTableBuilder = {
    addColumn(
      DeltaTable.columnBuilder(spark, colName).dataType(dataType).nullable(nullable).build
    )
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify a column.
   *
   * @param col structField the column struct
   * @since 1.0.0
   */
  @Evolving
  def addColumn(col: StructField): DeltaTableBuilder = {
    columns = columns :+ col
    this
  }


  /**
   * :: Evolving ::
   *
   * Specify columns with an existing schema.
   *
   * @param cols structType the existing schema for columns
   * @since 1.0.0
   */
  @Evolving
  def addColumns(cols: StructType): DeltaTableBuilder = {
    columns = columns ++ cols.toSeq
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify the columns to partition the output on the file system.
   *
   * Note: This should only include table columns already defined in schema.
   *
   * @param colNames string* column names for partitioning
   * @since 1.0.0
   */
  @Evolving
  @scala.annotation.varargs
  def partitionedBy(colNames: String*): DeltaTableBuilder = {
    partitioningColumns = Option(colNames)
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify a key-value pair to tag the table definition.
   *
   * @param key string the table property key
   * @param value string the table property value
   * @since 1.0.0
   */
  @Evolving
  def property(key: String, value: String): DeltaTableBuilder = {
    this.properties = this.properties + (key -> value)
    this
  }

  /**
   * :: Evolving ::
   *
   * Execute the command to create / replace a Delta table and returns a instance of [[DeltaTable]].
   *
   * @since 1.0.0
   */
  @Evolving
  def execute(): DeltaTable = {
    if (identifier == null && location.isEmpty) {
      throw DeltaErrors.analysisException("Table name or location has to be specified")
    }

    if (this.identifier == null) {
      identifier = s"delta.`${location.get}`"
    }

    // Return DeltaTable Object.
    val tableId: TableIdentifier = spark.sessionState.sqlParser.parseTableIdentifier(identifier)

    if (DeltaTableUtils.isValidPath(tableId) && location.nonEmpty
        && tableId.table != location.get) {
      throw DeltaErrors.analysisException(
        s"Creating path-based Delta table with a different location isn't supported. "
          + s"Identifier: $identifier, Location: ${location.get}")
    }

    val table = spark.sessionState.sqlParser.parseMultipartIdentifier(identifier)

    val partitioning = partitioningColumns.map { colNames =>
      colNames.map(name => DeltaTableUtils.parseColToTransform(name))
    }.getOrElse(Seq.empty[Transform])

    val tableSpec = org.apache.spark.sql.catalyst.plans.logical.TableSpec(
      properties = properties,
      provider = Some(FORMAT_NAME),
      options = Map.empty,
      location = location,
      serde = None,
      comment = tblComment,
      external = false
    )

    val stmt = builderOption match {
      case CreateTableOptions(ifNotExists) =>
        val unresolvedTable: LogicalPlan =
          org.apache.spark.sql.catalyst.analysis.UnresolvedDBObjectName(table, isNamespace = false)
        CreateTable(
          unresolvedTable,
          StructType(columns.toSeq),
          partitioning,
          tableSpec,
          ifNotExists)
      case ReplaceTableOptions(orCreate) =>
        val unresolvedTable: LogicalPlan =
          org.apache.spark.sql.catalyst.analysis.UnresolvedDBObjectName(table, isNamespace = false)
        ReplaceTable(
          unresolvedTable,
          StructType(columns.toSeq),
          partitioning,
          tableSpec,
          orCreate)
    }
    val qe = spark.sessionState.executePlan(stmt)
    // call `QueryExecution.toRDD` to trigger the execution of commands.
    SQLExecution.withNewExecutionId(qe, Some("create delta table"))(qe.toRdd)

    // Return DeltaTable Object.
    if (DeltaTableUtils.isValidPath(tableId)) {
      DeltaTable.forPath(spark, location.get)
    } else {
      DeltaTable.forName(spark, this.identifier)
    }
  }
}
