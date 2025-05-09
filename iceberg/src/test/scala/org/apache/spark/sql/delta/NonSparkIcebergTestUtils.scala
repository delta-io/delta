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

import scala.collection.JavaConverters._

import org.apache.iceberg.{DataFile, DataFiles, Files, PartitionSpec, Schema, Table}
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.io.FileAppender
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.NestedField

import org.apache.spark.sql.SparkSession

object NonSparkIcebergTestUtils {

  /**
   * Create an Iceberg table with formats/data types not supported by Spark.
   * This is primarily used for compatibility tests. It includes the following features
   * * TIME data type that is not supported by Spark.
   * @param location Iceberg table root path
   * @param schema   Iceberg table schema
   * @param rows     Data rows we write into the table
   * @param dataFileIdx index of the parquet file going to be written in the data folder
   */
  def createIcebergTable(
       spark: SparkSession,
       location: String,
       schema: Schema,
       rows: Seq[Map[String, Any]],
       dataFileIdx: Int = 1): Table = {
    // scalastyle:off deltahadoopconfiguration
    val tables = new HadoopTables(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    val table = tables.create(
      schema,
      PartitionSpec.unpartitioned(),
      location
    )

    writeIntoIcebergTable(table, rows, dataFileIdx)
    table
  }

  /**
   * Writes into an Iceberg table with formats/data types not supported by Spark.
   * This is primarily used for compatibility tests. It includes the following features
   * * TIME data type that is not supported by Spark.
   * @param table Iceberg table
   * @param rows  Data rows we write into the table
   * @param dataFileIdx index of the parquet file going to be written in the data folder
   */
  def writeIntoIcebergTable(
      table: Table,
      rows: Seq[Map[String, Any]],
      dataFileIdx: Int): Unit = {
    val schema = table.schema()
    val records = rows.map { row =>
      val record = GenericRecord.create(schema)
      row.foreach {
        case (key, value) => record.setField(key, value)
      }
      record
    }

    val parquetLocation = table.location() + s"/data/$dataFileIdx.parquet"

    val fileAppender: FileAppender[GenericRecord] = Parquet
      .write(table.io().newOutputFile(parquetLocation))
      .schema(schema)
      .createWriterFunc(GenericParquetWriter.buildWriter)
      .overwrite()
      .build();
    try {
      fileAppender.addAll(records.asJava)
    } finally {
      fileAppender.close
    }

    val dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
      .withInputFile(table.io().newInputFile(parquetLocation))
      .withMetrics(fileAppender.metrics())
      .build();

    table
      .newAppend
      .appendFile(dataFile)
      .commit
  }
}
