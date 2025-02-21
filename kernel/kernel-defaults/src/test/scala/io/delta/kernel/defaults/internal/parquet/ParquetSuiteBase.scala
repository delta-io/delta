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
import io.delta.kernel.expressions.{Column, Predicate}
import io.delta.kernel.internal.util.ColumnMapping
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.types.{ArrayType, DataType, MapType, StructField, StructType}
import io.delta.kernel.utils.{DataFileStatus, FileStatus}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.{ColumnPath, ParquetMetadata}

trait ParquetSuiteBase extends TestUtils {

  implicit class DataFileStatusOps(dataFileStatus: DataFileStatus) {

    /**
     * Convert the [[DataFileStatus]] to a [[TestRow]].
     * (path, size, modification time, numRecords,
     * min_col1, max_col1, nullCount_col1 (..repeated for every stats column)
     * )
     */
    def toTestRow(statsColumns: Seq[Column]): TestRow = {
      val statsOpt = dataFileStatus.getStatistics
      val record: Seq[Any] = {
        dataFileStatus.getPath +:
          dataFileStatus.getSize +:
          // convert to seconds, Spark returns in seconds and we can compare at second level
          (dataFileStatus.getModificationTime / 1000) +:
          // Add the row count to the stats literals
          (if (statsOpt.isPresent) statsOpt.get().getNumRecords else null) +:
          statsColumns.flatMap { column =>
            if (statsOpt.isPresent) {
              val stats = statsOpt.get()
              Seq(
                Option(stats.getMinValues.get(column)).map(_.getValue).orNull,
                Option(stats.getMaxValues.get(column)).map(_.getValue).orNull,
                Option(stats.getNullCounts.get(column)).orNull)
            } else {
              Seq(null, null, null)
            }
          }
      }
      TestRow(record: _*)
    }
  }

  /**
   * Verify the contents of the Parquet files located in `actualFileDir` matches the
   * `expected` data. Does two types of verifications.
   * 1) Verify the data using the Kernel Parquet reader
   * 2) Verify the data using the Spark Parquet reader
   */
  def verifyContent(actualFileDir: String, expected: Seq[FilteredColumnarBatch]): Unit = {
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

  /**
   * Verify the field ids in Parquet files match the corresponding field ids in the Delta schema.
   * If [[expectListMapEntryIds]] is true, verifies the array and map elements also have field ids
   * the match the fields in nearest ancestor struct field (i.e array or map)
   */
  def verifyFieldIds(
      targetDir: String,
      deltaSchema: StructType,
      expectNestedFiledIds: Boolean): Unit = {
    parquetFiles(targetDir).map(footer(_)).foreach {
      footer =>
        val parquetSchema = footer.getFileMetaData.getSchema

        def verifyFieldId(deltaFieldId: Long, parquetColumnPath: Array[String]): Unit = {
          val parquetFieldId = parquetSchema.getType(parquetColumnPath: _*).getId
          assert(parquetFieldId != null)
          assert(deltaFieldId === parquetFieldId.intValue())
        }

        def verifyNestedFieldId(
            nearestAncestorStructField: StructField,
            relativeNestedFieldPath: Array[String], // relative to the nearest ancestor struct field
            columnPath: Array[String]): Unit = {
          val deltaFieldId = nearestAncestorStructField.getMetadata
            .getMetadata(ColumnMapping.COLUMN_MAPPING_NESTED_IDS_KEY)
            .getLong(relativeNestedFieldPath.mkString("."))
            .toInt
          val parquetFieldId = parquetSchema.getType(columnPath: _*).getId
          assert(parquetFieldId != null)
          assert(deltaFieldId === parquetFieldId.intValue())
        }

        def visitDeltaType(
            basePathInParquet: Array[String],
            nearestAncestorStructField: StructField,
            baseRelativePathToAncestor: Array[String],
            deltaType: DataType): Unit = {
          deltaType match {
            case struct: StructType =>
              visitStructType(basePathInParquet, struct)
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
              val elemPathInParquet = basePathInParquet :+ "list" :+ "element"
              val relativePathToNearestAncestor = baseRelativePathToAncestor :+ "element"
              if (expectNestedFiledIds) {
                verifyNestedFieldId(
                  nearestAncestorStructField,
                  relativePathToNearestAncestor,
                  elemPathInParquet)
              }
              visitDeltaType(
                elemPathInParquet,
                nearestAncestorStructField,
                relativePathToNearestAncestor,
                array.getElementType)
            case map: MapType =>
              // reason for appending the "key_value" is same as the array type (see above)
              val keyPathInParquet = basePathInParquet :+ "key_value" :+ "key"
              val valuePathInParquet = basePathInParquet :+ "key_value" :+ "value"
              val keyRelativePathToNearestAncestor = baseRelativePathToAncestor :+ "key"
              val valueRelativePathToNearestAncestor = baseRelativePathToAncestor :+ "value"

              if (expectNestedFiledIds) {
                verifyNestedFieldId(
                  nearestAncestorStructField,
                  keyRelativePathToNearestAncestor,
                  keyPathInParquet)
                verifyNestedFieldId(
                  nearestAncestorStructField,
                  valueRelativePathToNearestAncestor,
                  valuePathInParquet)
              }

              visitDeltaType(
                keyPathInParquet,
                nearestAncestorStructField,
                keyRelativePathToNearestAncestor,
                map.getKeyType)

              visitDeltaType(
                valuePathInParquet,
                nearestAncestorStructField,
                valueRelativePathToNearestAncestor,
                map.getValueType)
            case _ => // Primitive type - continue
          }
        }

        def visitStructType(basePathInParquet: Array[String], structType: StructType): Unit = {
          structType.fields.forEach { field =>
            val deltaFieldId = field.getMetadata
              .getLong(ColumnMapping.COLUMN_MAPPING_ID_KEY)
            val physicalName = field.getMetadata
              .getString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)

            verifyFieldId(deltaFieldId, basePathInParquet :+ physicalName)
            visitDeltaType(
              basePathInParquet :+ physicalName,
              nearestAncestorStructField = field,
              baseRelativePathToAncestor = Array(physicalName),
              field.getDataType)
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
    val conf = new Configuration(configuration);
    conf.setLong(ParquetFileWriter.TARGET_FILE_SIZE_CONF, targetFileSize)
    val parquetWriter = new ParquetFileWriter(
      conf,
      new Path(location),
      statsColumns.asJava)

    parquetWriter.write(toCloseableIterator(filteredData.asJava.iterator())).toSeq
  }

  def readParquetFilesUsingKernel(
      actualFileDir: String,
      readSchema: StructType,
      predicate: Optional[Predicate] = Optional.empty()): Seq[TestRow] = {
    val columnarBatches =
      readParquetUsingKernelAsColumnarBatches(actualFileDir, readSchema, predicate)
    columnarBatches.map(_.getRows).flatMap(_.toSeq).map(TestRow(_))
  }

  def readParquetUsingKernelAsColumnarBatches(
      inputFileOrDir: String,
      readSchema: StructType,
      predicate: Optional[Predicate] = Optional.empty()): Seq[ColumnarBatch] = {
    val parquetFileList = parquetFiles(inputFileOrDir)
      .map(FileStatus.of(_, 0, 0))

    val data = defaultEngine.getParquetHandler.readParquetFiles(
      toCloseableIterator(parquetFileList.asJava.iterator()),
      readSchema,
      predicate)

    data.asScala.toSeq
  }

  def parquetFileCount(fileOrDir: String): Long = parquetFiles(fileOrDir).size

  def parquetFileRowCount(fileOrDir: String): Long = {
    val files = parquetFiles(fileOrDir)

    var rowCount = 0L
    files.foreach { file =>
      // read parquet file using spark and count.
      rowCount = rowCount + spark.read.parquet(file).count()
    }

    rowCount
  }

  def parquetFiles(fileOrDir: String): Seq[String] = {
    val fileOrDirPath = Paths.get(fileOrDir)
    if (Files.isDirectory(fileOrDirPath)) {
      Files.list(fileOrDirPath)
        .iterator().asScala
        .map(_.toString)
        .filter(path => path.endsWith(".parquet"))
        .toSeq
    } else {
      Seq(fileOrDir)
    }
  }

  def footer(path: String): ParquetMetadata = {
    try {
      org.apache.parquet.hadoop.ParquetFileReader.readFooter(configuration, new Path(path))
    } catch {
      case NonFatal(e) => fail(s"Failed to read footer for file: $path", e)
    }
  }

  // Read the parquet files in actionFileDir using Spark Parquet reader
  def readParquetFilesUsingSpark(
      actualFileDir: String,
      readSchema: StructType): Seq[TestRow] = {
    spark.read
      .format("parquet")
      .parquet(actualFileDir)
      .to(readSchema.toSpark)
      .collect()
      .map(TestRow(_))
  }
}
