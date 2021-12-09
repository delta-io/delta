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
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.interface.system.unstructured.commonFile.util.{CommonFileUtil, IOUtils}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

import java.net.URI



/**
 * The commonFile data source.
 * likes the spark binary file data source.
 * It reads binary files and converts each file into a single record that contains the raw content
 * and metadata of the file, but the schema is different.
 * It uses the StructType composed by a series of BinaryType to contain raw content,
 * not only a single BinaryType and it also supports spark structStreaming
 * Example:
 * {{{
 *   // Scala
 *   val df = spark.read.format("commonFile")
 *     .load("/path/to/fileDir")
 *
 *   // Java
 *   Dataset<Row> df = spark.read().format("commonFile")
 *     .load("/path/to/fileDir");
 * }}}
 */

/**
 * Author: CHEN ZHI LING
 * Date: 2021/8/17
 * Description:
 */
class CommonFileFormat
  extends FileFormat
    with StreamSourceProvider
    with StreamSinkProvider
    with DataSourceRegister{
  import CommonFileSchema._



  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path",
      throw new IllegalArgumentException("path must be specified."))
  }



  override def inferSchema(sparkSession: SparkSession,
                           options: Map[String, String],
                           files: Seq[FileStatus]):
  Option[StructType] = Some(CommonFileSchema.schema)



  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    // check path
    val writePath: String = checkPath(options)
    new CommonFileWriterFactory(writePath)
  }



  override def isSplitable(sparkSession: SparkSession,
                           options: Map[String, String],
                           path: Path): Boolean = {
    false
  }



  override def shortName(): String = COMMON_FILE



  override protected def buildReader(sparkSession: SparkSession,
                                     dataSchema: StructType,
                                     partitionSchema: StructType,
                                     requiredSchema: StructType,
                                     filters: Seq[Filter],
                                     options: Map[String, String],
                                     hadoopConf: Configuration):
  PartitionedFile => Iterator[InternalRow] = {
    val broadCastedHadoopConf: Broadcast[SerializableConfiguration] =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    val filterFunction: Seq[FileStatus => Boolean] =
      filters.map((filter: Filter) => createFilterFunction(filter))
    file: PartitionedFile => {
      val path = new Path(new URI(file.filePath))
      val fs: FileSystem = path.getFileSystem(broadCastedHadoopConf.value.value)
      val status: FileStatus = fs.getFileStatus(path)
      val size: Long = status.getLen
      val stream: FSDataInputStream = fs.open(status.getPath)
      // user iterator to avoid java heap space
      val iterator: Iterator[Array[Byte]] = IOUtils.splitStream(stream, size)
      val fileName: String = status.getPath.getName
      val suffix: UTF8String = UTF8String.fromString(CommonFileUtil.getFileSuffix(fileName))
      if (filterFunction.forall((_: FileStatus => Boolean).apply(status))) {
        // fill the row
        CommonFileSchema.fileToRowBatch(fileName, path.toString, size, suffix.toString, iterator)
      } else {
        Iterator.empty
      }
    }
  }



  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    (shortName(), CommonFileSchema.schema)
  }



  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {
    val path: String = checkPath(parameters)
    new CommonFileStream(sqlContext, path)
  }



  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    val path: String = checkPath(parameters)
    new CommonFileSink(sqlContext, path)
  }



  private[commonFile] def createFilterFunction(filter: Filter): FileStatus => Boolean = {
    filter match {
      case And(left, right) =>
        (s: FileStatus) => createFilterFunction(left)(s) && createFilterFunction(right)(s)
      case Or(left, right) =>
        (s: FileStatus) => createFilterFunction(left)(s) || createFilterFunction(right)(s)
      case Not(child) =>
        (s: FileStatus) => !createFilterFunction(child)(s)
      case LessThan(SIZE, value: Long) =>
        (_: FileStatus).getLen < value
      case LessThanOrEqual(SIZE, value: Long) =>
        (_: FileStatus).getLen <= value
      case GreaterThan(SIZE, value: Long) =>
        (_: FileStatus).getLen > value
      case GreaterThanOrEqual(SIZE, value: Long) =>
        (_: FileStatus).getLen >= value
      case EqualTo(SIZE, value: Long) =>
        (_: FileStatus).getLen == value
      case _ => (_: FileStatus) => true
    }
  }
}
