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
package io.delta.kernel.defaults.internal.parquet

import java.lang.{Double => DoubleJ, Float => FloatJ}

import io.delta.golden.GoldenTableUtils.{goldenTableFile, goldenTablePath}
import io.delta.kernel.data.{ColumnarBatch, FilteredColumnarBatch}
import io.delta.kernel.defaults.internal.DefaultKernelUtils
import io.delta.kernel.defaults.utils.{DefaultVectorTestUtils, ExpressionTestUtils, TestRow}
import io.delta.kernel.expressions.{Column, Literal, Predicate}
import io.delta.kernel.internal.util.ColumnMapping
import io.delta.kernel.internal.util.ColumnMapping.convertToPhysicalSchema
import io.delta.kernel.types._
import io.delta.kernel.utils.DataFileStatus
import org.apache.spark.sql.{functions => sparkfn}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test strategy for [[ParquetFileWriter]]
 * <p>
 * Golden tables already have Parquet files containing various supported
 * data types and variations (null, non-nulls, decimal types, nested nested types etc.).
 * We will use these files to simplify the tests for ParquetFileWriter. Alternative is to
 * generate the test data in the tests and try to write as Parquet files, but that would be a lot
 * of test code to cover all the combinations.
 * <p>
 * Using the golden Parquet files in combination with Kernel Parquet reader and Spark Parquet
 * reader we will reduce the test code and also test the inter-working of the Parquet writer with
 * the Parquet readers.
 * <p>
 * High level steps in the test:
 * 1) read data using the Kernel Parquet reader to generate the data in [[ColumnarBatch]]es
 * 2) Optional: filter the data from (1) and generate [[FilteredColumnarBatch]]es
 * 3) write the data back to new Parquet file(s) using the ParquetFileWriter that we are
 * testing. We will test the following variations:
 * 3.1) change target file size and stats collection columns etc.
 * 4) verification
 * 4.1) read the new Parquet file(s) using the Kernel Parquet reader and compare with (2)
 * 4.2) read the new Parquet file(s) using the Spark Parquet reader and compare with (2)
 * 4.3) verify the stats returned in (3) are correct using the Spark Parquet reader
 */
class ParquetFileWriterSuite extends AnyFunSuite
  with ParquetSuiteBase with DefaultVectorTestUtils with ExpressionTestUtils {

  Seq(
    // Test cases reading and writing all types of data with or without stats collection
    Seq((200, 67), (1024, 16), (1048576, 1)).map {
      case (targetFileSize, expParquetFileCount) =>
        (
          "write all types (no stats)", // test name
          "parquet-all-types", // input table where the data is read and written
          targetFileSize,
          expParquetFileCount,
          200, /* expected number of rows written to Parquet files */
          Option.empty[Predicate], // predicate for filtering what rows to write to parquet files
          Seq.empty[Column], // list of columns to collect stats as part of the Parquet file write
          0 // how many columns have the stats collected from given list above
        )
    },
    // Test cases reading and writing decimal types data with different precisions
    // They trigger different paths in the Parquet writer as how decimal types are stored in Parquet
    // based on the precision and scale.
    Seq((1048576, 3), (2048576, 2)).map {
      case (targetFileSize, expParquetFileCount) =>
        (
          "write decimal all types (with stats)", // test name
          "parquet-decimal-type",
          targetFileSize,
          expParquetFileCount,
          99998, /* expected number of rows written to Parquet files */
          Option.empty[Predicate], // predicate for filtering what rows to write to parquet files
          leafLevelPrimitiveColumns(
            Seq.empty, tableSchema(goldenTablePath("parquet-decimal-type"))),
          4 // how many columns have the stats collected from given list above
        )
    },
    // Test cases reading and writing data with field ids. This is for column mapping mode ID.
    Seq((200, 3), (1024, 1)).map {
      case (targetFileSize, expParquetFileCount) =>
        (
          "write data with field ids (no stats)", // test name
          "table-with-columnmapping-mode-id",
          targetFileSize,
          expParquetFileCount,
          6, /* expected number of rows written to Parquet files */
          Option.empty[Predicate], // predicate for filtering what rows to write to parquet files
          Seq.empty[Column], // list of columns to collect stats as part of the Parquet file write
          0 // how many columns have the stats collected from given list above
        )
    },
    // Test cases reading and writing only a subset of data passing a predicate.
    Seq((200, 26), (1024, 6), (1048576, 1)).map {
      case (targetFileSize, expParquetFileCount) =>
        (
          "write filtered all types (no stats)", // test name
          "parquet-all-types", // input table where the data is read and written
          targetFileSize,
          expParquetFileCount,
          77, /* expected number of rows written to Parquet files */
          // predicate for filtering what input rows to write to parquet files
          Some(greaterThanOrEqual(col("ByteType"), Literal.ofInt(50))),
          Seq.empty[Column], // list of columns to collect stats as part of the Parquet file write
          0 // how many columns have the stats collected from given list above
        )
    },
    // Test cases reading and writing all types of data WITH stats collection
    Seq((200, 67), (1024, 16), (1048576, 1)).map {
      case (targetFileSize, expParquetFileCount) =>
        (
          "write all types (with stats for all leaf-level columns)", // test name
          "parquet-all-types", // input table where the data is read and written
          targetFileSize,
          expParquetFileCount,
          200, /* expected number of rows written to Parquet files */
          Option.empty[Predicate], // predicate for filtering what rows to write to parquet files
          leafLevelPrimitiveColumns(Seq.empty, tableSchema(goldenTablePath("parquet-all-types"))),
          15 // how many columns have the stats collected from given list above
        )
    },
    // Test cases reading and writing all types of data with a partial column set stats collection
    Seq((200, 67), (1024, 16), (1048576, 1)).map {
      case (targetFileSize, expParquetFileCount) =>
        (
          "write all types (with stats for a subset of leaf-level columns)", // test name
          "parquet-all-types", // input table where the data is read and written
          targetFileSize,
          expParquetFileCount,
          200, /* expected number of rows written to Parquet files */
          Option.empty[Predicate], // predicate for filtering what rows to write to parquet files
          Seq(
            new Column("ByteType"),
            new Column("DateType"),
            new Column(Array("nested_struct", "aa")),
            new Column(Array("nested_struct", "ac", "aca")),
            new Column(Array("nested_struct", "ac")), // stats are not collected for struct types
            new Column("nested_struct"), // stats are not collected for struct types
            new Column("array_of_prims"), // stats are not collected for array types
            new Column("map_of_prims") // stats are not collected for map types
          ),
          4 // how many columns have the stats collected from given list above
        )
    },
    // Decimal types with various precision and scales
    Seq((10000, 1)).map {
      case (targetFileSize, expParquetFileCount) =>
        (
          "write decimal various scales and precision (with stats)", // test name
          "decimal-various-scale-precision",
          targetFileSize,
          expParquetFileCount,
          3, /* expected number of rows written to Parquet files */
          Option.empty[Predicate], // predicate for filtering what rows to write to parquet files
          leafLevelPrimitiveColumns(
            Seq.empty, tableSchema(goldenTablePath("decimal-various-scale-precision"))),
          29 // how many columns have the stats collected from given list above
        )
    }
  ).flatten.foreach {
    case (name, input, fileSize, expFileCount, expRowCount, predicate, statsCols, expStatsColCnt) =>
      test(s"$name: targetFileSize=$fileSize, predicate=$predicate") {
        withTempDir { tempPath =>
          val targetDir = tempPath.getAbsolutePath

          val inputLocation = goldenTablePath(input)
          val schema = tableSchema(inputLocation)

          val physicalSchema = if (hasColumnMappingId(inputLocation)) {
            convertToPhysicalSchema(schema, schema, ColumnMapping.COLUMN_MAPPING_MODE_ID)
          } else {
            schema
          }

          val dataToWrite =
            readParquetUsingKernelAsColumnarBatches(inputLocation, physicalSchema) // read data
              // Convert the schema of the data to the physical schema with field ids
              .map(_.withNewSchema(physicalSchema))
              // convert the data to filtered columnar batches
              .map(_.toFiltered(predicate))

          val writeOutput =
            writeToParquetUsingKernel(dataToWrite, targetDir, fileSize, statsCols)

          assert(parquetFileCount(targetDir) === expFileCount)
          assert(parquetFileRowCount(targetDir) == expRowCount)

          verifyContent(targetDir, dataToWrite)
          verifyStatsUsingSpark(targetDir, writeOutput, schema, statsCols, expStatsColCnt)
        }
      }
  }

  test("columnar batches containing different schema") {
    withTempDir { tempPath =>
      val targetDir = tempPath.getAbsolutePath

      // First batch with one column
      val batch1 = columnarBatch(testColumnVector(10, IntegerType.INTEGER))

      // Batch with two columns
      val batch2 = columnarBatch(
        testColumnVector(10, IntegerType.INTEGER),
        testColumnVector(10, LongType.LONG))

      // Batch with one column as first batch but different data type
      val batch3 = columnarBatch(testColumnVector(10, LongType.LONG))

      Seq(Seq(batch1, batch2), Seq(batch1, batch3)).foreach { dataToWrite =>
        val e = intercept[IllegalArgumentException] {
          writeToParquetUsingKernel(dataToWrite.map(_.toFiltered), targetDir)
        }
        assert(e.getMessage.contains("Input data has columnar batches with different schemas:"))
      }
    }
  }

  /**
   * Tests to cover floating point comparison special cases in Parquet.
   * - https://issues.apache.org/jira/browse/PARQUET-1222
   * - Parquet doesn't collect stats if NaN is present in the column values
   * - Min is written as -0.0 instead of 0.0 and max is written as 0.0 instead of -0.0
   */
  test("float/double type column stats collection") {
    // Try writing different set of floating point values and verify the stats are correct
    // (float values, double values, exp rowCount in files, exp stats (min, max, nullCount)
    Seq(
      ( // no stats collection as NaN is present
        Seq(Float.NegativeInfinity, Float.MinValue, -1.0f,
          -0.0f, 0.0f, 1.0f, null, Float.MaxValue, Float.PositiveInfinity, Float.NaN),
        Seq(Double.NegativeInfinity, Double.MinValue, -1.0d,
          -0.0d, 0.0d, 1.0d, null, Double.MaxValue, Double.PositiveInfinity, Double.NaN),
        10,
        (null, null, null),
        (null, null, null)
      ),
      ( // Min and max are infinities
        Seq(Float.NegativeInfinity, Float.MinValue, -1.0f,
          -0.0f, 0.0f, 1.0f, null, Float.MaxValue, Float.PositiveInfinity),
        Seq(Double.NegativeInfinity, Double.MinValue, -1.0d,
          -0.0d, 0.0d, 1.0d, null, Double.MaxValue, Double.PositiveInfinity),
        9,
        (Float.NegativeInfinity, Float.PositiveInfinity, 1L),
        (Double.NegativeInfinity, Double.PositiveInfinity, 1L)
      ),
      ( // no infinities or NaN - expect stats collected
        Seq(Float.MinValue, -1.0f, -0.0f, 0.0f, 1.0f, null, Float.MaxValue),
        Seq(Double.MinValue, -1.0d, -0.0d, 0.0d, 1.0d, null, Double.MaxValue),
        7,
        (Float.MinValue, Float.MaxValue, 1L),
        (Double.MinValue, Double.MaxValue, 1L)
      ),
      ( // Only negative numbers. Max is 0.0 instead of -0.0 to avoid PARQUET-1222
        Seq(Float.NegativeInfinity, Float.MinValue, -1.0f, -0.0f, null),
        Seq(Double.NegativeInfinity, Double.MinValue, -1.0d, -0.0d, null),
        5,
        (Float.NegativeInfinity, 0.0f, 1L),
        (Double.NegativeInfinity, 0.0d, 1L)
      ),
      ( // Only positive numbers. Min is  -0.0 instead of 0.0 to avoid PARQUET-1222
        Seq(0.0f, 1.0f, null, Float.MaxValue, Float.PositiveInfinity),
        Seq(0.0d, 1.0d, null, Double.MaxValue, Double.PositiveInfinity),
        5,
        (-0.0f, Float.PositiveInfinity, 1L),
        (-0.0d, Double.PositiveInfinity, 1L)
      )
    ).foreach {
      case (floats: Seq[FloatJ], doubles: Seq[DoubleJ], expRowCount, expFltStats, expDblStats) =>
        withTempDir { tempPath =>
          val targetDir = tempPath.getAbsolutePath
          val testBatch = columnarBatch(floatVector(floats), doubleVector(doubles))
          val dataToWrite = Seq(testBatch.toFiltered)

          val writeOutput =
            writeToParquetUsingKernel(
              dataToWrite,
              targetDir,
              statsColumns = Seq(col("col_0"), col("col_1")))

          assert(parquetFileRowCount(targetDir) == expRowCount)
          verifyContent(targetDir, dataToWrite)

          val stats = writeOutput.head.getStatistics.get()

          def getStats(column: String): (Object, Object, Object) =
            (
              Option(stats.getMinValues.get(col(column))).map(_.getValue).orNull,
              Option(stats.getMaxValues.get(col(column))).map(_.getValue).orNull,
              Option(stats.getNullCounts.get(col(column))).orNull
            )

          assert(getStats("col_0") === expFltStats)
          assert(getStats("col_1") === expDblStats)
        }
    }
  }

  test(s"invalid target file size") {
    withTempDir { tempPath =>
      val targetDir = tempPath.getAbsolutePath
      val inputLocation = goldenTableFile("parquet-all-types").toString
      val schema = tableSchema(inputLocation)

      val dataToWrite =
        readParquetUsingKernelAsColumnarBatches(inputLocation, schema)
          .map(_.toFiltered)

      Seq(-1, 0).foreach { targetFileSize =>
        val e = intercept[IllegalArgumentException] {
          writeToParquetUsingKernel(dataToWrite, targetDir, targetFileSize)
        }
        assert(e.getMessage.contains("Invalid target Parquet file size: " + targetFileSize))
      }
    }
  }

  def verifyStatsUsingSpark(
    actualFileDir: String,
    actualFileStatuses: Seq[DataFileStatus],
    fileDataSchema: StructType,
    statsColumns: Seq[Column],
    expStatsColCount: Int): Unit = {

    if (statsColumns.isEmpty) return

    val actualStatsOutput = actualFileStatuses
      .map { fileStatus =>
        // validate there are no more the expected number of stats columns
        assert(fileStatus.getStatistics.isPresent)
        assert(fileStatus.getStatistics.get().getMinValues.size() === expStatsColCount)
        assert(fileStatus.getStatistics.get().getMaxValues.size() === expStatsColCount)
        assert(fileStatus.getStatistics.get().getNullCounts.size() === expStatsColCount)

        // Convert to TestRow for comparison with the actual values computing using Spark.
        fileStatus.toTestRow(statsColumns)
      }

    if (expStatsColCount == 0) return

    // Use spark to fetch the stats from the parquet files use them as the expected statistics
    // Compare them with the actual stats returned by the Kernel's Parquet writer.
    val df = spark.read
      .format("parquet")
      .parquet(actualFileDir)
      .to(fileDataSchema.toSpark)
      .select(
        sparkfn.col("*"), // select all columns from the parquet files
        sparkfn.col("_metadata.file_path").as("path"), // select file path
        sparkfn.col("_metadata.file_size").as("size"), // select file size
        // select mod time and convert to millis
        sparkfn.unix_timestamp(
          sparkfn.col("_metadata.file_modification_time")).as("modificationTime")
      )
      .groupBy("path", "size", "modificationTime")

    val nullStats = Seq(sparkfn.lit(null), sparkfn.lit(null), sparkfn.lit(null))

    // Add the row count aggregation
    val aggs = Seq(sparkfn.count(sparkfn.col("*")).as("rowCount")) ++
      // add agg for each stats column to get min, max and null count
      statsColumns
        .flatMap { statColumn =>
          val dataType = DefaultKernelUtils.getDataType(fileDataSchema, statColumn)
          dataType match {
            case _: StructType => nullStats // no concept of stats for struct types
            case _: ArrayType => nullStats // no concept of stats for array types
            case _: MapType => nullStats // no concept of stats for map types
            case _ => // for all other types
              val colName = statColumn.toPath
              Seq(
                sparkfn.min(colName).as("min_" + colName),
                sparkfn.max(colName).as("max_" + colName),
                sparkfn.sum(sparkfn.when(
                  sparkfn.col(colName).isNull, 1).otherwise(0)).as("nullCount_" + colName))
          }
        }

    val expectedStatsOutput = df.agg(aggs.head, aggs.tail: _*).collect().map(TestRow(_))

    checkAnswer(actualStatsOutput, expectedStatsOutput)
  }
}
