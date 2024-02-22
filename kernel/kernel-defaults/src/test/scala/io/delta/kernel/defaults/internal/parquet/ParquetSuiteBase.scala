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
package io.delta.kernel.defaults.internal.parquet

import java.nio.file.{Files, Paths}
import java.util.Optional

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import io.delta.kernel.data.{ColumnarBatch, FilteredColumnarBatch}
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.util.ColumnMapping
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.types.{ArrayType, DataType, MapType, StructType}
import io.delta.kernel.utils.{DataFileStatus, FileStatus}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata

trait ParquetSuiteBase extends TestUtils {

<<<<<<< HEAD
  /**
   * Verify the contents of the Parquet files located in `actualFileDir` matches the
   * `expected` data. Does two types of verifications.
   * 1) Verify the data using the Kernel Parquet reader
   * 2) Verify the data using the Spark Parquet reader
   */
  def verifyContent(actualFileDir: String, expected: Seq[FilteredColumnarBatch]): Unit = {
=======
  def verify(actualFileDir: String, expected: Seq[FilteredColumnarBatch]): Unit = {
>>>>>>> 86b911d2 ([Kernel][TEST-ONLY] Refactors the test parquet suite)
    verifyFileMetadata(actualFileDir)
    verifyContentUsingKernelReader(actualFileDir, expected)
    verifyContentUsingSparkReader(actualFileDir, expected)
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

  /**
   * Verify the data in the Parquet files located in `actualFileDir` matches the expected data.
   * Use Kernel Parquet reader to read the data from the Parquet files.
   */
  def verifyContentUsingKernelReader(
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
  def verifyContentUsingSparkReader(
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

  // Read the parquet files in actionFileDir using Spark Parquet reader
  private def readParquetFilesUsingSpark(
    actualFileDir: String, readSchema: StructType): Seq[TestRow] = {
    spark.read
      .format("parquet")
      .parquet(actualFileDir)
      .to(readSchema.toSpark)
      .collect()
      .map(TestRow(_))
  }
}
<<<<<<< HEAD
=======

object ParquetSuiteBase {
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
    .add("nested_struct", new StructType()
      .add("aa", StringType.STRING)
      .add("ac", new StructType().add("aca", IntegerType.INTEGER)))
    .add("array_of_prims", new ArrayType(IntegerType.INTEGER, true))
    .add("array_of_arrays", new ArrayType(new ArrayType(IntegerType.INTEGER, true), true))
    .add("array_of_structs", new ArrayType(new StructType().add("ab", LongType.LONG), true))
    .add("map_of_prims", new MapType(IntegerType.INTEGER, LongType.LONG, true))
    .add("map_of_rows",
      new MapType(IntegerType.INTEGER, new StructType().add("ab", LongType.LONG), true))
    .add("map_of_arrays",
      new MapType(LongType.LONG, new ArrayType(IntegerType.INTEGER, true), true))

  // Parquet file containing all variations (int, long and fixed binary) Decimal type data
  val DECIMAL_TYPES_DATA = goldenTableFile("parquet-decimal-type").toString
  // Schema of the data in `DECIMAL_TYPES_DATA`
  val DECIMAL_TYPES_FILE_SCHEMA = new StructType()
    .add("id", IntegerType.INTEGER)
    .add("col1", new DecimalType(5, 1)) // stored as int
    .add("col2", new DecimalType(10, 5)) // stored as long
    .add("col3", new DecimalType(20, 5)) // stored as fixed binary
}
>>>>>>> 86b911d2 ([Kernel][TEST-ONLY] Refactors the test parquet suite)
