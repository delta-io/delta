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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.interface.system.unstructured.commonFile.util.{CommonFileUtil, IOUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

import scala.collection.mutable.ArrayBuffer



/**
 * Author: CHEN ZHI LING
 * Date: 2021/8/17
 * Description:
 */
class CommonFileStream(sqlContext: SQLContext, path: String) extends Source with Logging {



  private val configuration: Configuration = sqlContext.sparkContext.hadoopConfiguration
  private val directoryPath = new Path(path)
  var currentOffset: Map[String, Array[String]] =
    Map[String, Array[String]](path -> new Array[String](0))
  private var _fs: FileSystem = _



  override def schema: StructType = CommonFileSchema.schema



  override def getOffset: Option[Offset] = {
    val latest: Map[String, Array[String]] = getLatestOffset
    Option(FileSourceOffset(latest))
  }



  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {

    var offset: Array[String] = new Array[String](0)
    if (start.isDefined) {
      offset = offsetToMap(start.get)(path)
    }
    /**
     * user diff function to get the new file path in latest polling
     */
    val files: Array[String] = offsetToMap(end)(path).diff(offset)
    val rows = new ArrayBuffer[InternalRow]
    if (null != files) {
      // change every file to row
      files.foreach((file: String) => {
        val path = new Path(file)
        val status: FileStatus = _fs.getFileStatus(path)
        val fileName: String = status.getPath.getName
        val stream: FSDataInputStream = fs.open(status.getPath)
        val size: Long = status.getLen
        // use iterator to avoid java heap space
        val iterator: Iterator[Array[Byte]] = IOUtils.splitStream(stream, size)
        val suffix: UTF8String = UTF8String.fromString(CommonFileUtil.getFileSuffix(fileName))
        // fill the row
        val internalRows: Iterator[InternalRow] = CommonFileSchema.fileToRowStream(
          fileName, path.toString, size, suffix.toString, iterator)
        while (internalRows.hasNext) {
          rows.append(internalRows.next())
        }
      })
    }
    val list: List[InternalRow] = rows.toList
    val rdd: RDD[InternalRow] = sqlContext.sparkContext.parallelize(list)
    // change to RDD[Row]
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }



  override def stop(): Unit = if (null != _fs) _fs.close()



  /**
   * get the latestOffset
   * it contains all of file path by Recursive traversal
   */
  def getLatestOffset: Map[String, Array[String]] = {
    val fileSystem: FileSystem = fs
    val statuses: Array[String] = CommonFileUtil.listFiles(directoryPath, fileSystem)
    Map[String, Array[String]](path -> statuses)
  }



  private def fs: FileSystem = {
    if (_fs == null) _fs = directoryPath.getFileSystem(configuration)
    _fs
  }



  def offsetToMap(offset: Offset): Map[String, Array[String]] = {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    Serialization.read[Map[String, Array[String]]](offset.json())
  }



case class FileSourceOffset(offset: Map[String, Array[String]]) extends Offset {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    override def json(): String = Serialization.write(offset)
  }
}
