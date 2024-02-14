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
package io.delta.kernel.defaults.internal.parquet;

import io.delta.golden.GoldenTableUtils.goldenTableFile
import io.delta.kernel.Table
import io.delta.kernel.data.{ColumnarBatch, FilteredColumnarBatch}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.expressions.{Column, Literal, Predicate}
import io.delta.kernel.internal.util.ColumnMapping
import io.delta.kernel.internal.util.ColumnMapping.convertToPhysicalSchema
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.types._
import io.delta.kernel.utils.{DataFileStatus, FileStatus}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Paths}
import java.util.Optional
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

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
class ParquetFileWriterSuite extends AnyFunSuite with TestUtils {
  import ParquetFileWriterSuite._

  Seq(200, 1000, 1048576).foreach { targetFileSize =>
    test(s"write all types - no stats - targetFileSize: $targetFileSize") {
      withTempDir { tempPath =>
        val targetDir = tempPath.getAbsolutePath

        val dataToWrite =
          readParquetUsingKernelAsColumnarBatches(ALL_TYPES_DATA, ALL_TYPES_FILE_SCHEMA)
            .map(_.toFiltered)

        writeToParquetUsingKernel(dataToWrite, targetDir, targetFileSize)

        val expectedNumParquetFiles = targetFileSize match {
          case 200 => 100
          case 1000 => 29
          case 1048576 => 1
          case _ => throw new IllegalArgumentException(s"Invalid targetFileSize: $targetFileSize")
        }
        assert(parquetFileCount(targetDir) === expectedNumParquetFiles)

        verify(targetDir, dataToWrite)
      }
    }
  }

  Seq(1048576, 2048576).foreach { targetFileSize =>
    test(s"decimal all types - no stats - targetFileSize: $targetFileSize") {
      withTempDir { tempPath =>
        val targetDir = tempPath.getAbsolutePath

        val dataToWrite =
          readParquetUsingKernelAsColumnarBatches(DECIMAL_TYPES_DATA, DECIMAL_TYPES_FILE_SCHEMA)
            .map(_.toFiltered)

        writeToParquetUsingKernel(dataToWrite, targetDir, targetFileSize)

        val expectedNumParquetFiles = targetFileSize match {
          case 1048576 => 3
          case 2048576 => 2
          case _ => throw new IllegalArgumentException(s"Invalid targetFileSize: $targetFileSize")
        }
        assert(parquetFileCount(targetDir) === expectedNumParquetFiles)

        verify(targetDir, dataToWrite)
      }
    }
  }

  Seq(200, 1000, 1048576).foreach { targetFileSize =>
    test(s"write all types - filtered dataset, targetFileSize: $targetFileSize") {
      withTempDir { tempPath =>
        val targetDir = tempPath.getAbsolutePath

        // byteValue is in the range [-72, 127] with null at every (value % 72 == 0) row
        // File has total of 200 rows.
        val predicate = new Predicate(">=", new Column("byteType"), Literal.ofInt(50))
        val expectedRowCount = 128 /* no. of positive values */ - 1 /* one */ - 50 /* val < 50 */
        val dataToWrite =
          readParquetUsingKernelAsColumnarBatches(ALL_TYPES_DATA, ALL_TYPES_FILE_SCHEMA)
            .map(_.toFiltered(predicate))

        writeToParquetUsingKernel(dataToWrite, targetDir, targetFileSize = targetFileSize)

        val expectedNumParquetFiles = targetFileSize match {
          case 200 => 39
          case 1000 => 11
          case 1048576 => 1
          case _ => throw new IllegalArgumentException(s"Invalid targetFileSize: $targetFileSize")
        }
        assert(parquetFileCount(targetDir) === expectedNumParquetFiles)
        assert(parquetFileRowCount(targetDir) === expectedRowCount)

        verify(targetDir, dataToWrite)
      }
    }
  }

  test("columnar batches containing different schema") {
    withTempDir { tempPath =>
      val targetDir = tempPath.getAbsolutePath

      // First batch with one column
      val batch1 = new DefaultColumnarBatch(
        /* size */ 10,
        new StructType().add("col1", IntegerType.INTEGER),
        Array(testColumnVector(10, IntegerType.INTEGER)))

      // Batch with two columns
      val batch2 = new DefaultColumnarBatch(
        /* size */ 10,
        new StructType()
          .add("col1", IntegerType.INTEGER)
          .add("col2", LongType.LONG),
        Array(testColumnVector(10, IntegerType.INTEGER), testColumnVector(10, LongType.LONG)))

      // Batch with one column as first batch but different data type
      val batch3 = new DefaultColumnarBatch(
        /* size */ 10,
        new StructType().add("col1", LongType.LONG),
        Array(testColumnVector(10, LongType.LONG)))

      Seq(Seq(batch1, batch2), Seq(batch1, batch3)).foreach {
        dataToWrite =>
          val e = intercept[IllegalArgumentException] {
            writeToParquetUsingKernel(dataToWrite.map(_.toFiltered), targetDir)
          }
          assert(e.getMessage.contains("Input data has columnar batches with different schemas:"))
      }
    }
  }

  test("write data with field ids") {
    withTempDir { tempPath =>
      val targetDir = tempPath.getAbsolutePath

      val cmGoldenTable = goldenTableFile("table-with-columnmapping-mode-id").toString
      val schema = tableSchema(cmGoldenTable)

      val dataToWrite =
        readParquetUsingKernelAsColumnarBatches(cmGoldenTable, schema)
          .map(_.toFiltered)

      // From the Delta schema, generate the physical schema that has field ids.
      val physicalSchema =
        convertToPhysicalSchema(schema, schema, ColumnMapping.COLUMN_MAPPING_MODE_ID)

      writeToParquetUsingKernel(
        // Convert the schema of the data to the physical schema with field ids
        dataToWrite.map(_.getData).map(_.withNewSchema(physicalSchema)).map(_.toFiltered),
        targetDir)

      verifyFieldIds(targetDir, schema)
      verify(targetDir, dataToWrite)
    }
  }

  test(s"invalid target file size") {
    withTempDir { tempPath =>
      val targetDir = tempPath.getAbsolutePath
      val dataToWrite =
        readParquetUsingKernelAsColumnarBatches(DECIMAL_TYPES_DATA, DECIMAL_TYPES_FILE_SCHEMA)
          .map(_.toFiltered)

      Seq(-1, 0).foreach { targetFileSize =>
        val e = intercept[IllegalArgumentException] {
          writeToParquetUsingKernel(dataToWrite, targetDir, targetFileSize)
        }
        assert(e.getMessage.contains("Invalid target Parquet file size: " + targetFileSize))
      }
    }
  }

  def verify(actualFileDir: String, expected: Seq[FilteredColumnarBatch]): Unit = {
    verifyFileMetadata(actualFileDir)
    verifyUsingKernelReader(actualFileDir, expected)
    verifyUsingSparkReader(actualFileDir, expected)
  }

  /**
   * Verify the data in the Parquet files located in `actualFileDir` matches the expected data.
   * Use Kernel Parquet reader to read the data from the Parquet files.
   */
  def verifyUsingKernelReader(
    actualFileDir: String,
    expected: Seq[FilteredColumnarBatch]): Unit = {

    val dataSchema = expected.head.getData.getSchema

    val expectedTestRows = expected
      .map(fb => fb.getRows)
      .flatMap(_.toSeq)
      .map(TestRow(_))

    val actualTestRows = readParquetFilesUsingKernel(actualFileDir, dataSchema)

    checkAnswer(actualTestRows, expectedTestRows)
  }

  /**
   * Verify the data in the Parquet files located in `actualFileDir` matches the expected data.
   * Use Spark Parquet reader to read the data from the Parquet files.
   */
  def verifyUsingSparkReader(
    actualFileDir: String,
    expected: Seq[FilteredColumnarBatch]): Unit = {

    val dataSchema = expected.head.getData.getSchema;

    val expectedTestRows = expected
      .map(fb => fb.getRows)
      .flatMap(_.toSeq)
      .map(TestRow(_))

    val actualTestRows = readParquetFilesUsingSpark(actualFileDir, dataSchema)

    checkAnswer(actualTestRows, expectedTestRows)
  }

  /**
   * Verify the metadata of the Parquet files in `targetDir` matches says it is written by Kernel.
   */
  def verifyFileMetadata(targetDir: String): Unit = {
    parquetFiles(targetDir).foreach { file =>
      footer(file).getFileMetaData
        .getKeyValueMetaData.containsKey("io.delta.kernel.default-parquet-writer")
    }
  }

  /** Verify the field ids in Parquet files match the corresponding field ids in the Delta schema */
  def verifyFieldIds(targetDir: String, deltaSchema: StructType): Unit = {
    parquetFiles(targetDir).map(footer(_)).foreach {
      footer =>
        val parquetSchema = footer.getFileMetaData.getSchema

        def verifyFieldId(deltaFieldId: Long, columnPath: Array[String]): Unit = {
          val parquetFieldId = parquetSchema.getType(columnPath: _*).getId
          assert(parquetFieldId != null)
          assert(deltaFieldId === parquetFieldId.intValue())
        }

        def visitDeltaType(basePath: Array[String], deltaType: DataType): Unit = {
          deltaType match {
            case struct: StructType =>
              visitStructType(basePath, struct)
            case array: ArrayType =>
              // Arrays are stored as three-level structure in Parquet. There are two elements
              // between the array element and array itself. So in order to
              // search for  the array element field id, we need to append "list, element"
              // to the path.
              // optional group col-b89fd303-7352-4044-842e-87f428ee80be (LIST) = 19 {
              //  repeated group list {
              //   optional group element {
              //    optional int64 col-e983d1fc-d588-46a7-a0ad-2f63a6834ea6 = 20;
              //   }
              //  }
              // }
              visitDeltaType(basePath :+ "list" :+ "element", array.getElementType)
            case map: MapType =>
              // reason for appending the "key_value" is same as the array type (see above)
              visitDeltaType(basePath :+ "key_value" :+ "key", map.getKeyType)
              visitDeltaType(basePath :+ "key_value" :+ "value", map.getValueType)
            case _ => // Primitive type - continue
          }
        }

        def visitStructType(basePath: Array[String], structType: StructType): Unit = {
          structType.fields.forEach { field =>
            val deltaFieldId = field.getMetadata
              .get(ColumnMapping.COLUMN_MAPPING_ID_KEY).asInstanceOf[Long]
            val physicalName = field.getMetadata
              .get(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY).asInstanceOf[String]

            verifyFieldId(deltaFieldId, basePath :+ physicalName)
            visitDeltaType(basePath :+ physicalName, field.getDataType)
          }
        }

        visitStructType(Array.empty, deltaSchema)
    }
  }

  /**
   * Write the [[FilteredColumnarBatch]]es to Parquet files using the ParquetFileWriter and
   * verify the data using the Kernel Parquet reader and Spark Parquet reader.
   */
  def writeToParquetUsingKernel(
      filteredData: Seq[FilteredColumnarBatch],
      location: String,
      targetFileSize: Long = 1024 * 1024,
      statsColumns: Seq[Column] = Seq.empty): Seq[DataFileStatus] = {
    val parquetWriter = new ParquetFileWriter(
      configuration, new Path(location), targetFileSize, statsColumns.asJava)

    parquetWriter.write(toCloseableIterator(filteredData.asJava.iterator())).toSeq
  }

  def readParquetFilesUsingKernel(
    actualFileDir: String, readSchema: StructType): Seq[TestRow] = {
    val columnarBatches = readParquetUsingKernelAsColumnarBatches(actualFileDir, readSchema)
    columnarBatches.map(_.getRows).flatMap(_.toSeq).map(TestRow(_))
  }

  def readParquetUsingKernelAsColumnarBatches(
    actualFileDir: String, readSchema: StructType): Seq[ColumnarBatch] = {
    val parquetFiles = Files.list(Paths.get(actualFileDir))
      .iterator().asScala
      .map(_.toString)
      .filter(path => path.endsWith(".parquet"))
      .map(path => FileStatus.of(path, 0L, 0L))

    val data = defaultTableClient.getParquetHandler.readParquetFiles(
      toCloseableIterator(parquetFiles.asJava),
      readSchema,
      Optional.empty())

    data.asScala.toSeq
  }

  def readParquetFilesUsingSpark(
    actualFileDir: String, readSchema: StructType): Seq[TestRow] = {
    // Read the parquet files in actionFileDir using Spark Parquet reader
    spark.read
      .format("parquet")
      .parquet(actualFileDir)
      .to(readSchema.toSpark)
      .collect()
      .map(TestRow(_))
  }

  def parquetFileCount(path: String): Long = parquetFiles(path).size

  def parquetFileRowCount(path: String): Long = {
    val files = parquetFiles(path)

    var rowCount = 0L
    files.foreach { file =>
      // read parquet file using spark and count.
      rowCount = rowCount + spark.read.parquet(file).count()
    }
    rowCount
  }

  def parquetFiles(path: String): Seq[String] = {
    Files.list(Paths.get(path))
      .iterator().asScala
      .map(_.toString)
      .filter(path => path.endsWith(".parquet"))
      .toSeq
  }

  def footer(path: String): ParquetMetadata = {
    try {
      ParquetFileReader.readFooter(configuration, new Path(path))
    } catch {
      case NonFatal(e) => fail(s"Failed to read footer for file: $path", e)
    }
  }

  def tableSchema(path: String): StructType = {
    Table.forPath(defaultTableClient, path)
      .getLatestSnapshot(defaultTableClient)
      .getSchema(defaultTableClient)
  }
}

object ParquetFileWriterSuite {
  // Parquet file containing data of all supported types and variations
  val ALL_TYPES_DATA = goldenTableFile("parquet-all-types").toString
  // Schema of the data in `ALL_TYPES_DATA`
  val ALL_TYPES_FILE_SCHEMA = new StructType()
    .add("byteType", ByteType.BYTE)
    .add("shortType", ShortType.SHORT)
    .add("integerType", IntegerType.INTEGER)
    .add("longType", LongType.LONG)
    .add("floatType", FloatType.FLOAT)
    .add("doubleType", DoubleType.DOUBLE)
    .add("decimal", new DecimalType(10, 2))
    .add("booleanType", BooleanType.BOOLEAN)
    .add("stringType", StringType.STRING)
    .add("binaryType", BinaryType.BINARY)
    .add("dateType", DateType.DATE)
    .add("timestampType", TimestampType.TIMESTAMP)
    .add("nested_struct",
      new StructType()
        .add("aa", StringType.STRING)
        .add("ac",
          new StructType()
            .add("aca", IntegerType.INTEGER)))
    .add("array_of_prims", new ArrayType(IntegerType.INTEGER, true))
    .add("array_of_arrays", new ArrayType(new ArrayType(IntegerType.INTEGER, true), true))
    .add("array_of_structs",
      new ArrayType(
        new StructType()
          .add("ab", LongType.LONG), true))
    .add("map_of_prims", new MapType(IntegerType.INTEGER, LongType.LONG, true))
    .add("map_of_rows",
      new MapType(
        IntegerType.INTEGER,
        new StructType().add("ab", LongType.LONG),
        true))
    .add("map_of_arrays",
      new MapType(
        LongType.LONG,
        new ArrayType(IntegerType.INTEGER, true),
        true))

  // Parquet file containing all variations (int, long and fixed binary) Decimal type data
  val DECIMAL_TYPES_DATA = goldenTableFile("parquet-decimal-type").toString
  // Schema of the data in `DECIMAL_TYPES_DATA`
  val DECIMAL_TYPES_FILE_SCHEMA = new StructType()
    .add("id", IntegerType.INTEGER)
    .add("col1", new DecimalType(5, 1)) // stored as int
    .add("col2", new DecimalType(10, 5)) // stored as long
    .add("col3", new DecimalType(20, 5)) // stored as fixed binary
}
