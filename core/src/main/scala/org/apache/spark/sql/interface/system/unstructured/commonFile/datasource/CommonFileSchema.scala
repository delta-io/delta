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
package org.apache.spark.sql.interface.system.unstructured.commonFile.datasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.interface.system.unstructured.commonFile.util.{CommonFileUtil, IOUtils}
import org.apache.spark.sql.interface.system.util.PropertiesUtil
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.{ArrayBuffer, ListBuffer}



/**
 * Author: CHEN ZHI LING
 * Date: 2021/8/17
 * Description:
 */
object CommonFileSchema {



  private[commonFile] val SPLIT_NUMBER: Int =
    PropertiesUtil.chooseProperties("file", "file.split.number").toInt
  private[commonFile] val FILE_NAME = "fileName"
  private[commonFile] val PATH = "path"
  private[commonFile] val SUFFIX = "suffix"
  private[commonFile] val SIZE = "size"
  private[commonFile] val COMMON_FILE = "commonFile"
  private[commonFile] val BYTE_ARRAY = "byteArray"
  private[commonFile] val INITIAL_BUFFER_SIZE = 4096



  /**
   * Schema for the commonFile data source.
   *
   * Schema:
   *  - fileName(StringType):The name of the file.
   *  - path (StringType): The path of the file.
   *  - suffix (StringType): The suffix of the file.
   *  - size (LongType): The length of the file in bytes.
   *  - commonFile (StructType): The raw content of the file.
   *  it is a row composed by a series of BinaryType
   */
  val schema: StructType = StructType(
    StructField(FILE_NAME, StringType, nullable = true) ::
      StructField(PATH, StringType, nullable = true) ::
      StructField(SUFFIX, StringType, nullable = true) ::
      StructField(SIZE, LongType, nullable = true) ::
      StructField(COMMON_FILE, getSubSchema):: Nil)



  def getFileName(row: InternalRow): String = row.getString(0)



  def getFilePath(row: Row): String = row.getString(1)



  def getFileSuffix(row: Row): String = row.getString(2)



  def getFileSize(row: Row): Long = row.getLong(3)



  /**
   * get the raw content from row
   * @param row row
   * @return
   */
  def getCommonFile(row: InternalRow): Array[Byte] = {
    val nestRow: InternalRow = row.getStruct(4, SPLIT_NUMBER)
    val list = new ListBuffer[Array[Byte]]
    for(i <- 0 until nestRow.numFields) { list.append(nestRow.getBinary(i)) }
    if (list.toList.size == 1) {
      return list.head
    }
    IOUtils.joinArray(list.toList)
  }



  /**
   * the number of field is determined by reading th conf file
   * @return StructType
   */
  def getSubSchema: StructType = {
    val fields = new ArrayBuffer[StructField]
    for (i <- 1 to SPLIT_NUMBER) {
      fields.append(StructField(new StringBuilder(BYTE_ARRAY + i).toString(),
        BinaryType, nullable = false))
    }
    StructType(fields.toArray)
  }



  def fileToRowBatch(fileName : String,
                path: String,
                size: Long,
                suffix: String,
                iterator: Iterator[Array[Byte]]
               ) : Iterator[InternalRow] = {
    val writer = new UnsafeRowWriter(schema.length, INITIAL_BUFFER_SIZE)
    iterator.map{ (row: Array[Byte]) =>
      writer.reset()
      SchemaFilled(fileName, path, size, suffix, row, writer)
      writer.getRow
    }
  }



  def fileToRowStream(fileName : String,
                path: String,
                size: Long,
                suffix: String,
                iterator: Iterator[Array[Byte]]
               ) : Iterator[InternalRow] = {
    if (iterator.isEmpty) {
      val writer = new UnsafeRowWriter(schema.length, INITIAL_BUFFER_SIZE)
      writer.resetRowWriter()
      SchemaFilled(fileName, path, size, suffix, new Array[Byte](0), writer)
      return Iterator(writer.getRow)
    }
    iterator.map{ (row: Array[Byte]) =>
      val writer = new UnsafeRowWriter(schema.length, INITIAL_BUFFER_SIZE)
      writer.resetRowWriter()
      SchemaFilled(fileName, path, size, suffix, row, writer)
      writer.getRow
    }
  }



  /**
   * use UseSafeRowWriter to fill the row
   */
  def SchemaFilled(fileName: String, path: String, size: Long,
                   suffix: String, row: Array[Byte], writer: UnsafeRowWriter): Unit = {
    schema.fieldNames.zipWithIndex.foreach{
      case (FILE_NAME, i) => writer.write(i, UTF8String.fromString(fileName))
      case (PATH, i) => writer.write(i, UTF8String.fromString(path))
      case (SIZE, i) => writer.write(i, size)
      case (SUFFIX, i) => writer.write(i,
        UTF8String.fromString(CommonFileUtil.getFileSuffix(suffix)))
      case (COMMON_FILE, i) =>
        val list: List[Array[Byte]] = IOUtils.splitArrayByNumber(row, SPLIT_NUMBER)
        val childType: StructType = schema(COMMON_FILE).dataType.asInstanceOf[StructType]
        val childWriter = new UnsafeRowWriter(childType.length, INITIAL_BUFFER_SIZE)
        childWriter.resetRowWriter()
        for(j <- list.indices) { childWriter.write(j, list(j))}
        writer.write(i, childWriter.getRow)
      case (other, _) =>
        throw new RuntimeException(s"Unsupported field name: $other")
    }
  }
}
