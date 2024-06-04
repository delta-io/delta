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

import org.apache.iceberg
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Type.PrimitiveType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, PhysicalWriteInfo}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

import java.util.UUID

/**
 * Test utils for create iceberg tables to test reading iceberg as Delta via Uniform.
 */
object IcebergTestUtils {

  /** Wrapper classes used to write data into Iceberg table through the scala interface. */
  case class TestLogicalWriteInfo(
      queryId: String,
      schema: StructType,
      options: CaseInsensitiveStringMap) extends LogicalWriteInfo

  case class TestPhysicalWriteInfo(numPartitions: Int) extends PhysicalWriteInfo

  /** helper trait to achieve the generic iceberg schema update */
  sealed trait IcebergSchemaUpdateAction

  /** add a group of columns of the iceberg table */
  case class AddColumns(colTypeMap: Map[String, String]) extends IcebergSchemaUpdateAction

  /** delete a group of columns of the iceberg table */
  case class DeleteColumns(cols: Seq[String]) extends IcebergSchemaUpdateAction

  /** rename a group of columns of the iceberg table */
  case class RenameColumns(colNameMap: Map[String, String]) extends IcebergSchemaUpdateAction

  /** update a group of columns of the iceberg table */
  case class UpdateColumns(colTypeMap: Map[String, String]) extends IcebergSchemaUpdateAction

  /**
   * Helper function to get the `PrimitiveType` based on the column type string.
   *
   * @param colTypeString the type string representing a new column of a iceberg table.
   * @return the corresponding `PrimitiveType` of the `colTypeString`.
   */
  private def getColumnType(colTypeString: String): PrimitiveType = colTypeString match {
    case "int" => Types.IntegerType.get()
    case "long" => Types.LongType.get()
    case "string" => Types.StringType.get()
    case "date" => Types.DateType.get()
    case "bool" => Types.BooleanType.get()
    case "float" => Types.FloatType.get()
    case "double" => Types.DoubleType.get()
    // TODO: add more types, e.g., nested type, etc.
    case _ => throw new IllegalArgumentException(s"Unsupported column type: $colTypeString")
  }

  /**
   * Get an *existing* iceberg table based on the provided parameters.
   *
   * @param spark the spark session to use.
   * @param tableName the name of the existing iceberg table.
   * @param warehousePath the path of the iceberg table.
   * @return the `iceberg.Table` ready for use, e.g., update the table schema, etc.
   */
  private def getIcebergTableInternal(
      spark: SparkSession,
      tableName: String,
      warehousePath: String): iceberg.Table = {
    val tableIdent = iceberg.catalog.TableIdentifier.of("db", tableName)
    // scalastyle:off deltahadoopconfiguration
    // load the existing table from hadoop catalog
    new HadoopCatalog(spark.sessionState.newHadoopConf(), warehousePath).loadTable(tableIdent)
    // scalastyle:on deltahadoopconfiguration
  }

  def updateIcebergTableSchema(
      spark: SparkSession,
      warehousePath: String,
      tableName: String,
      actions: IcebergSchemaUpdateAction*): iceberg.Table = {
    // get the existing iceberg table
    val table = getIcebergTableInternal(spark, tableName, warehousePath)
    // the schema to be updated
    val updatedSchema = table.updateSchema()

    def getTypeMap(colTypeMap: Map[String, String]): Map[String, PrimitiveType] = {
      colTypeMap.map { case (colName, colTypeString) => (colName, getColumnType(colTypeString)) }
    }

    // iterate through the actions to update the iceberg table schema accordingly
    actions.foreach {
      case AddColumns(colTypeMap) =>
        getTypeMap(colTypeMap).foreach {
          case (colName, colType) => updatedSchema.addColumn(colName, colType)
        }
      case DeleteColumns(cols) =>
        cols.foreach { col => updatedSchema.deleteColumn(col) }
      case RenameColumns(colNameMap) =>
        colNameMap.foreach {
          case (oldName, newName) => updatedSchema.renameColumn(oldName, newName)
        }
      case UpdateColumns(colTypeMap) =>
        getTypeMap(colTypeMap).foreach {
          // TODO: update doc string for the column?
          case (colName, colType) => updatedSchema.updateColumn(colName, colType)
        }
    }

    // commit all update actions to the schema
    updatedSchema.commit()
    table
  }

  /**
   * Creates an Iceberg table from a Dataframe with provided configs using the scala interface,
   * this works in the UC environment, where SQL interface is not available.
   *
   * @param spark: the spark session to use.
   * @param warehousePath: the warehouse path, where to create the table.
   * @param tableName: the table name used to create the table.
   * @param data: the Dataframe used to create the table.
   * @param partitionNames: the list of partition column names, can be empty.
   * @return the created iceberg table object.
   */
  def createIcebergTable(
      spark: SparkSession,
      warehousePath: String,
      tableName: String,
      data: DataFrame,
      partitionNames: Seq[String] = Seq.empty[String]): iceberg.Table = {
    val table = createIcebergTable(spark, warehousePath, tableName, data.schema, partitionNames)
    writeIcebergTable(table, data)
    table
  }

  /**
   * Similar as above, except creating an empty Iceberg table from the schema.
   *
   * @param spark: the spark session to use.
   * @param warehousePath: the warehouse path, where to create the table.
   * @param tableName: the table name used to create the table.
   * @param schema: the schema used to create the table.
   * @param partitionNames: the list of partition column names, can be empty.
   * @return the created iceberg table object.
   */
  def createIcebergTable(
      spark: SparkSession,
      warehousePath: String,
      tableName: String,
      schema: StructType,
      partitionNames: Seq[String]): iceberg.Table = {
    val tableIdent = iceberg.catalog.TableIdentifier.of("db", tableName)
    val icebergSchema = iceberg.spark.SparkSchemaUtil.convert(schema)
    val specBuilder = iceberg.PartitionSpec.builderFor(icebergSchema)
    partitionNames.foreach(specBuilder.identity)

    // scalastyle:off deltahadoopconfiguration
    new HadoopCatalog(spark.sessionState.newHadoopConf(), warehousePath)
      .createTable(tableIdent, icebergSchema, specBuilder.build())
    // scalastyle:on deltahadoopconfiguration
  }

  /**
   * Drops the Iceberg table using the scala interface, this works in the UC environment, where SQL
   * interface is not available.
   *
   * @param spark: the spark session to use.
   * @param warehousePath: the warehouse path, where the table exists.
   * @param tableName: the table name to drop.
   */
  def dropIcebergTable(
      spark: SparkSession,
      warehousePath: String,
      tableName: String): Unit = {
    val tableIdent = iceberg.catalog.TableIdentifier.of("db", tableName)
    // scalastyle:off deltahadoopconfiguration
    new HadoopCatalog(spark.sessionState.newHadoopConf(), warehousePath)
      .dropTable(tableIdent)
    // scalastyle:on deltahadoopconfiguration
  }

  /**
   * Inserts data into the Iceberg table using the scala interface, this works in the UC
   * environment, where SQL interface is not available.
   *
   * Please note: caller needs to match the schema of provided data to the Iceberg table.
   *
   * @param table: the target Iceberg table.
   * @param dataFrame: the data to be added.
   */
  def writeIcebergTable(table: iceberg.Table, dataFrame: DataFrame): Unit = {
    val sparkTable = new iceberg.spark.source.SparkTable(table, true)
    val batchWrite = sparkTable.newWriteBuilder(
        TestLogicalWriteInfo(
          "rid_" + UUID.randomUUID.toString,
          dataFrame.schema,
          CaseInsensitiveStringMap.empty))
      .build()
      .toBatch
    val writer = batchWrite.createBatchWriterFactory(TestPhysicalWriteInfo(1)).createWriter(0, 0)

    // Iceberg table expects UTF8 for string column.
    dataFrame.collect().foreach { row =>
      val values = row.toSeq.map {
        case s: String => UTF8String.fromString(s)
        case other => other
      }
      writer.write(InternalRow.fromSeq(values))
    }

    val commitMessage = writer.commit()
    batchWrite.commit(Array(commitMessage))
  }
}