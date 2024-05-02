/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.delta.connect.proto
import io.delta.tables.execution.{CreateTableOptions, DeltaTableBuilderOptions, ReplaceTableOptions}

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
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
 * @since 2.5.0
 */
@Evolving
class DeltaTableBuilder private[tables](
    spark: SparkSession,
    builderOption: DeltaTableBuilderOptions) {
  private var identifier: Option[String] = None
  private var partitioningColumns: Seq[String] = Nil
  private var clusteringColumns: Seq[String] = Nil
  private var columns: mutable.Seq[StructField] = mutable.Seq.empty
  private var location: Option[String] = None
  private var tblComment: Option[String] = None
  private var properties = Map.empty[String, String]

  /**
   * :: Evolving ::
   *
   * Specify the table name, optionally qualified with a database name [database_name.] table_name
   *
   * @param identifier string the table name
   * @since 2.5.0
   */
  @Evolving
  def tableName(identifier: String): DeltaTableBuilder = {
    this.identifier = Some(identifier)
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify the table comment to describe the table.
   *
   * @param comment string table comment
   * @since 2.5.0
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
   * @since 2.5.0
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
   * @since 2.5.0
   */
  @Evolving
  def addColumn(colName: String, dataType: String): DeltaTableBuilder = {
    addColumn(
      DeltaTable.columnBuilder(spark, colName).dataType(dataType).build()
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
   * @since 2.5.0
   */
  @Evolving
  def addColumn(colName: String, dataType: DataType): DeltaTableBuilder = {
    addColumn(
      DeltaTable.columnBuilder(spark, colName).dataType(dataType).build()
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
   * @since 2.5.0
   */
  @Evolving
  def addColumn(colName: String, dataType: String, nullable: Boolean): DeltaTableBuilder = {
    addColumn(
      DeltaTable.columnBuilder(spark, colName).dataType(dataType).nullable(nullable).build()
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
   * @since 2.5.0
   */
  @Evolving
  def addColumn(colName: String, dataType: DataType, nullable: Boolean): DeltaTableBuilder = {
    addColumn(
      DeltaTable.columnBuilder(spark, colName).dataType(dataType).nullable(nullable).build()
    )
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify a column.
   *
   * @param col structField the column struct
   * @since 2.5.0
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
   * @since 2.5.0
   */
  @Evolving
  def addColumns(cols: StructType): DeltaTableBuilder = {
    columns = columns ++ cols
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
   * @since 2.5.0
   */
  @Evolving
  @scala.annotation.varargs
  def partitionedBy(colNames: String*): DeltaTableBuilder = {
    partitioningColumns = colNames
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify the columns to use as clustering keys for liquid clustering.
   *
   * Note: This should only include table columns already defined in schema.
   *
   * @param colNames string* column names for liquid clustering
   * @since 3.x
   */
  @Evolving
  @scala.annotation.varargs
  def clusterBy(colNames: String*): DeltaTableBuilder = {
    clusteringColumns = colNames
    this
  }

  /**
   * :: Evolving ::
   *
   * Specify a key-value pair to tag the table definition.
   *
   * @param key string the table property key
   * @param value string the table property value
   * @since 2.5.0
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
   * @since 2.5.0
   */
  @Evolving
  def execute(): DeltaTable = {
    if (identifier.isEmpty && location.isEmpty) {
      // throw new AnalysisException("Table name or location has to be specified")
    }

    val mode = builderOption match {
      case CreateTableOptions(ifNotExists) =>
        if (ifNotExists) proto.CreateDeltaTable.Mode.MODE_CREATE_IF_NOT_EXISTS
        else proto.CreateDeltaTable.Mode.MODE_CREATE
      case ReplaceTableOptions(orCreate) =>
        if (orCreate) proto.CreateDeltaTable.Mode.MODE_CREATE_OR_REPLACE
        else proto.CreateDeltaTable.Mode.MODE_REPLACE
    }

    val createDeltaTable = proto.CreateDeltaTable
      .newBuilder()
      .setMode(mode)
      .addAllPartitioningColumns(partitioningColumns.asJava)
      .addAllClusteringColumns(clusteringColumns.asJava)
      .putAllProperties(properties.asJava)
    identifier.foreach(createDeltaTable.setTableName)
    location.foreach(createDeltaTable.setLocation)
    tblComment.foreach(createDeltaTable.setComment)

    val protoColumns = columns.map { f =>
      val builder = proto.CreateDeltaTable.Column
        .newBuilder()
        .setName(f.name)
        .setDataType(DataTypeProtoConverter.toConnectProtoType(f.dataType))
        .setNullable(f.nullable)
      if (f.metadata.contains("delta.generationExpression")) {
        builder.setGeneratedAlwaysAs(f.metadata.getString("delta.generationExpression"))
      }
      if (f.metadata.contains("comment")) {
        builder.setComment(f.metadata.getString("comment"))
      }
      builder.build()
    }
    createDeltaTable.addAllColumns(protoColumns.asJava)

    val command = proto.DeltaCommand.newBuilder().setCreateDeltaTable(createDeltaTable).build()
    val extension = com.google.protobuf.Any.pack(command)
    spark.execute(extension)

    if (location.isDefined) {
      DeltaTable.forPath(spark, location.get)
    } else {
      DeltaTable.forName(spark, identifier.get)
    }
  }
}
