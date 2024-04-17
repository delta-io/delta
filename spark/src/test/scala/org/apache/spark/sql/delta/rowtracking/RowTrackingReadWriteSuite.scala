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

package org.apache.spark.sql.delta.rowtracking

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, RowCommitVersion, RowId}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.rowid.RowIdTestUtils

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StructType}

class RowTrackingReadWriteSuite extends RowIdTestUtils
  with ParquetTest {

  private val testTableName = "target"

  private val testDataColumnName = "test_data"

  test("select star does not read materialized columns") {
    withAllParquetReaders {
      withTable(testTableName) {
        writeWithMaterializedRowCommitVersionColumns(
          spark.range(100).toDF(testDataColumnName),
          rowIdColumn = lit(1L),
          rowCommitVersionColumn = lit(2L))

        withAllParquetReaders {
          val df = sql(s"SELECT * FROM $testTableName")
          assert(df.schema.size === 1)
          assert(df.schema.head.name === testDataColumnName)
        }
      }
    }
  }

  test("write and read table without materialized columns") {
    withTable(testTableName) {
      withRowTrackingEnabled(enabled = true) {
        val numRows = 5L
        spark.range(numRows).toDF(testDataColumnName)
          .write.format("delta").saveAsTable(testTableName)

        try {
          // Confirm that the materialized columns are not present in the Parquet file(s).
            val deltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))
            val parquetDataFrame = spark.read.parquet(deltaLog.dataPath.toString)
            checkAnswer(parquetDataFrame, (0L until numRows).map(Row(_)))
        } catch {
          case e: Exception => Thread.sleep(1000 * 1000)
        }

        withAllParquetReaders {
          val df = spark.read.table(testTableName)
            .select(
              testDataColumnName,
              RowId.QUALIFIED_COLUMN_NAME,
              RowCommitVersion.QUALIFIED_COLUMN_NAME)
          checkAnswer(df, (0L until numRows).map(i => Row(i, i, 0)))
        }
      }
    }
  }

  test("write and read table with all-null materialized columns") {
    withTable(testTableName) {
      val numRows = 5L
      writeWithMaterializedRowCommitVersionColumns(
        spark.range(numRows).toDF(testDataColumnName),
        rowIdColumn = lit(null).cast("long"),
        rowCommitVersionColumn = lit(null).cast("long"))

      // Confirm that the materialized columns are present in the Parquet file(s).
      withSQLConf(
        // The function writeWithMaterializedRowCommitVersionColumns initially creates
        // an empty table with only the testDataColumnName column. After that, we write
        // some data to the Delta table, appending some Parquet files that include
        // both the testDataColumnName and the materialized Row Tracking Columns.
        //
        // PARQUET_SCHEMA_MERGING_ENABLED by default is disabled, so what then happens
        // is we pick a random data file to infer the schema of the underlying parquet
        // DataFrame of the Delta table in [[ParquetUtils]].
        //
        // Thus, that inferred schema could either contains only the testDataColumnName
        // column or all three columns, the former leads to wrong result for the
        // checkAnswer below.
        //
        // This is the intended behavior as Delta doesn't give any guarantee
        // that the Parquet files will all have the same schema, and normally we should
        // not read raw Parquet files from a Delta table, we are only doing this for
        // testing purpose.
        SQLConf.PARQUET_SCHEMA_MERGING_ENABLED.key -> "true"
      ) {
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))
        val parquetDataFrame = spark.read.parquet(deltaLog.dataPath.toString)
        checkAnswer(parquetDataFrame, (0L until numRows).map(Row(_, null, null)))
      }

      withAllParquetReaders {
        val df = spark.read.table(testTableName)
          .select(
            testDataColumnName,
            RowId.QUALIFIED_COLUMN_NAME,
            RowCommitVersion.QUALIFIED_COLUMN_NAME)
        checkAnswer(df, (0L until 5L).map(i => Row(i, i, 1)))
      }
    }
  }

  test("write and read table with no-nulls materialized columns") {
    withTable(testTableName) {
      val numRows = 100
      writeWithMaterializedRowCommitVersionColumns(
        spark.range(numRows).toDF(testDataColumnName),
        rowIdColumn = col(testDataColumnName) + 10,
        rowCommitVersionColumn = col(testDataColumnName) + 100)

      withAllParquetReaders {
        val df = spark.table(testTableName)
          .select(
            testDataColumnName,
            RowId.QUALIFIED_COLUMN_NAME,
            RowCommitVersion.QUALIFIED_COLUMN_NAME)
        val expectedAnswer = (0L until numRows).map(i => Row(i, i + 10, i + 100))
        checkAnswer(df, expectedAnswer)
      }
    }
  }

  test("write and read table with mixed materialized columns") {
    withTable(testTableName) {
      val numRows = 100L
      writeWithMaterializedRowCommitVersionColumns(
        spark.range(numRows).toDF(testDataColumnName),
        rowIdColumn =
          when(col(testDataColumnName) % 2 === 0, col(testDataColumnName) + 10)
            .otherwise(null),
        rowCommitVersionColumn =
          when(col(testDataColumnName) % 3 === 0, col(testDataColumnName) + 100)
            .otherwise(null))

      withAllParquetReaders {
        val df = spark.table(testTableName)
          .select(
            testDataColumnName,
            RowId.QUALIFIED_COLUMN_NAME,
            RowCommitVersion.QUALIFIED_COLUMN_NAME)
        val expectedAnswer = (0L until numRows).map { i =>
          Row(i, if (i % 2 == 0) i + 10 else i, if (i % 3 == 0) i + 100 else 1)
        }
        checkAnswer(df, expectedAnswer)
      }
    }
  }

  test("read mixed materialized columns with filter") {
    withTable(testTableName) {
      val numRows = 100L
      writeWithMaterializedRowCommitVersionColumns(
        spark.range(numRows).toDF(testDataColumnName),
        rowIdColumn =
          when(col(testDataColumnName) % 2 === 0, lit(1000L))
            .otherwise(null),
        rowCommitVersionColumn =
          when(col(testDataColumnName) % 3 === 0, lit(2000L))
            .otherwise(null))

      withAllParquetReaders {
        // Read the table with a filter that does not match any of the materialized row ids and
        // row commit versions, but that does match the default-generated ids and versions.
        val df = spark.table(testTableName)
          .select(
            testDataColumnName,
            RowId.QUALIFIED_COLUMN_NAME,
            RowCommitVersion.QUALIFIED_COLUMN_NAME)
          .filter(col(RowId.QUALIFIED_COLUMN_NAME) <= 100 &&
            col(RowCommitVersion.QUALIFIED_COLUMN_NAME) <= 100)
        val expectedAnswer = (0L until numRows)
          .filter(i => i % 2 != 0 && i % 3 != 0)
          .map { i => Row(i, i, 1) }
        checkAnswer(df, expectedAnswer)
      }
    }
  }

  test("writing to materialized column requires correct metadata") {
    withTable(testTableName) {
      writeWithMaterializedRowCommitVersionColumns(
        spark.range(100).toDF("id"),
        rowIdColumn = lit(null),
        rowCommitVersionColumn = lit(null))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))
      val materializedRowIdColumnName =
        extractMaterializedRowIdColumnName(deltaLog).get
      val materializedRowCommitVersionColumnName =
        extractMaterializedRowCommitVersionColumnName(deltaLog).get

      val insertStmt1 = s"INSERT INTO $testTableName (id, `$materializedRowIdColumnName`)"
      val errorRowIds = intercept[AnalysisException](sql(insertStmt1 + " VALUES(1, 2)"))
      checkError(
        errorRowIds,
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = errorRowIds.messageParameters,
        queryContext = Array(ExpectedContext(insertStmt1, 0, insertStmt1.length - 1)))

      val insertStmt2 =
        s"INSERT INTO $testTableName (id, `$materializedRowCommitVersionColumnName`)"
      val errorRowCommitVersions = intercept[AnalysisException](sql(insertStmt2 + " VALUES(1, 2)"))
      checkError(
        errorRowCommitVersions,
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = errorRowCommitVersions.messageParameters,
        queryContext = Array(ExpectedContext(insertStmt2, 0, insertStmt2.length - 1)))
    }
  }

  test("writing to materialized column requires correct name") {
    withRowTrackingEnabled(enabled = true) {
      withTable(testTableName) {
        writeWithMaterializedRowCommitVersionColumns(
          spark.range(1).toDF(testDataColumnName),
          rowIdColumn = lit(1L),
          rowCommitVersionColumn = lit(2L))

        checkWritingToMaterializedColumnsRequiresCorrectName()
      }
    }
  }

  test("writing to materialized column requires row tracking to be enabled") {
    withTable(testTableName) {
      writeWithMaterializedRowCommitVersionColumns(
        spark.range(1).toDF(testDataColumnName),
        rowIdColumn = lit(1L),
        rowCommitVersionColumn = lit(2L))

      spark.sql(s"ALTER TABLE $testTableName " +
        s"SET TBLPROPERTIES ('${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = 'false')")

      checkWritingToMaterializedColumnsRequiresCorrectName()
    }
  }

  private def checkWritingToMaterializedColumnsRequiresCorrectName(): Unit = {
    val columnNames = Seq(
      RowId.QUALIFIED_COLUMN_NAME,
      s"`${FileFormat.METADATA_NAME}`.`${RowId.ROW_ID}`",
      s"`${RowId.QUALIFIED_COLUMN_NAME}`",
      RowId.QUALIFIED_COLUMN_NAME,
      s"`${FileFormat.METADATA_NAME}`.`${RowId.ROW_ID}`",
      s"`${RowId.QUALIFIED_COLUMN_NAME}`")

    for (columnName <- columnNames) {
      // Throw an error because only using the materialized column name is valid.
      val error = intercept[AnalysisException] {
        spark
          .range(end = 1)
          .toDF(testDataColumnName)
          .withMaterializedRowIdColumn(columnName, lit(1L))
          .write
          .format("delta")
          .mode("append")
          .saveAsTable(testTableName)
      }
      checkError(
        error,
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = error.messageParameters)
    }

    for (columnName <- columnNames) {
      // Throw an error because only using the materialized column name is valid.
      val error = intercept[AnalysisException] {
        spark
          .range(end = 1)
          .toDF(testDataColumnName)
          .withMaterializedRowCommitVersionColumn(columnName, lit(2L))
          .write
          .format("delta")
          .mode("append")
          .saveAsTable(testTableName)
      }
      checkError(
        error,
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = error.messageParameters)
    }
  }

  test("write and read with column names similar to row tracking columns in the table schema") {
    val columnNames = Seq(
      RowId.QUALIFIED_COLUMN_NAME,
      RowCommitVersion.QUALIFIED_COLUMN_NAME)
    for (columnName <- columnNames) {
      withTable(testTableName) {
        val numRows = 10L
        writeWithMaterializedRowCommitVersionColumns(
          spark.range(numRows).toDF(columnName),
          rowIdColumn = lit(1L),
          rowCommitVersionColumn = lit(2L))

        withAllParquetReaders {
          val df = spark.read.table(testTableName).select(
            s"`$columnName`",
            RowId.QUALIFIED_COLUMN_NAME,
            RowCommitVersion.QUALIFIED_COLUMN_NAME)
          val expectedAnswer = (0L until numRows).map(Row(_, 1L, 2L))
          checkAnswer(df, expectedAnswer)
        }
      }
    }
  }

  test("write and read with conflicting columns") {
    withTable(testTableName) {
      val tableSchema = new StructType()
        .add("id", LongType)
        .add(FileFormat.METADATA_NAME, new StructType()
          .add(RowId.ROW_ID, LongType)
          .add(RowCommitVersion.METADATA_STRUCT_FIELD_NAME, LongType))

      writeWithMaterializedRowCommitVersionColumns(
        spark.createDataFrame(
          Seq(Row(1L, (11L, 111L)), Row(2L, (22L, 222L)), Row(3L, (33L, 333L))).asJava,
          tableSchema),
        rowIdColumn = lit(-1L),
        rowCommitVersionColumn = lit(-2L))

      withAllParquetReaders {
        val table = spark.read.table(testTableName)
        val metadataCol = table.metadataColumn(FileFormat.METADATA_NAME)
        val userCol = table.col(FileFormat.METADATA_NAME)
        val df = table.select(
          col("id"),
          metadataCol.getField(RowId.ROW_ID),
          metadataCol.getField(RowCommitVersion.METADATA_STRUCT_FIELD_NAME),
          userCol.getField(RowId.ROW_ID),
          userCol.getField(RowCommitVersion.METADATA_STRUCT_FIELD_NAME))
        val expectedAnswer =
          Seq(Row(1, -1, -2, 11, 111), Row(2, -1, -2, 22, 222), Row(3, -1, -2, 33, 333))
        checkAnswer(df, expectedAnswer)
      }
    }
  }

  private def writeWithMaterializedRowCommitVersionColumns(
      df: DataFrame,
      rowIdColumn: Column,
      rowCommitVersionColumn: Column): Unit = {
    withRowTrackingEnabled(enabled = true) {
      // Create the table if it does not exist already.
      df.limit(n = 0)
        .write.format("delta").mode("append").saveAsTable(testTableName)

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))
      val materializedRowIdColumnName =
        extractMaterializedRowIdColumnName(deltaLog).get
      val materializedRowCommitVersionName =
        extractMaterializedRowCommitVersionColumnName(deltaLog).get

      df.withMaterializedRowIdColumn(
          materializedRowIdColumnName, rowIdColumn)
        .withMaterializedRowCommitVersionColumn(
          materializedRowCommitVersionName, rowCommitVersionColumn)
        .write
        .mode("append")
        .format("delta")
        .saveAsTable(testTableName)
    }
  }
}
