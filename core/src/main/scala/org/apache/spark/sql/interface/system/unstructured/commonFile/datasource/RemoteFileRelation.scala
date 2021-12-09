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

import com.sshtools.sftp.{SftpClient, SftpFile, SftpStatusException}
import com.sshtools.ssh2.Ssh2Client
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.interface.system.unstructured.commonFile.datasource.CommonFileSchema.SPLIT_NUMBER
import org.apache.spark.sql.interface.system.unstructured.commonFile.util.RemoteFileUtil.{createConnect, getSftpClient}
import org.apache.spark.sql.interface.system.unstructured.commonFile.util.{CommonFileUtil, IOUtils}
import org.apache.spark.sql.interface.system.util.MultiSourceException
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

import java.io.InputStream



/**
 * Author: CHEN ZHI LING
 * Date: 2021/8/17
 * Description:
 */
class RemoteFileRelation(override val sqlContext: SQLContext, parameter: Map[String, String])
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation
    with Serializable {


  private[commonFile] val PATH: String = "path"



  override def schema: StructType = CommonFileSchema.schema


  /**
   * scan all row
   */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    var fileFilters: Map[String, List[RemoteFileFilter]] = Map[String, List[RemoteFileFilter]]()
    filters.foreach{
      case IsNotNull(attr) => Array(attr)
      case EqualTo(attr, value) =>
        fileFilters = fileFilters ++ Map(attr -> {
          fileFilters.getOrElse(
            attr, List[RemoteFileFilter]()) :+ new RemoteFileFilter(attr, value, "equalTo")
        })

      case _ => throw new MultiSourceException("only support EqualTo")
    }
    val ssh: Ssh2Client = createConnect(parameter)
    val sftpClient: SftpClient = getSftpClient(ssh)
    var list: Array[SftpFile] = null
    var path: SftpFile = null
    // open the sftpFile
    try{
      path = sftpClient.openFile(parameter(PATH))
    } catch {
      case _: SftpStatusException =>
        throw new MultiSourceException(parameter(PATH) + " is not existed")
    }
    if (path.isFile) list = Array(path) else list =
      CommonFileUtil.listRemoteFiles(parameter(PATH), sftpClient)
    val rows: Array[Row] = list.flatMap((file: SftpFile) => sftpFileToRow(file, sftpClient))
    val rowToRdd: RDD[Row] = sqlContext.sparkSession.sparkContext.parallelize(rows)
    // do filter
    val rowAfterFilter: RDD[Row] = rowToRdd.map((row: Row) => if (fileFilters.nonEmpty) {
      var includeInResultSet = true
      row.toSeq.zipWithIndex.foreach {
        case (value, index) =>
          val attr: String = schema(index).name
          val filtersList: List[RemoteFileFilter] = fileFilters.getOrElse(attr, List())
          if (filtersList.nonEmpty) {
            if (RemoteFileFilter.applyFilters(filtersList, value, schema)) {}
            else {includeInResultSet = false}
          }
      }
      if (includeInResultSet) row else null
    }
    else row)
    // filter null type
    val rowFilterNone: RDD[Row] = rowAfterFilter.filter((row: Row) => null != row)
    val tmp: RDD[Seq[Option[Any]]] = rowFilterNone.map((row: Row) => {
      row.toSeq.zipWithIndex.map {
        case (value, index) =>
          val colName: String = schema(index).name
          val castedValue: Any = RemoteFileFilter.castTo(value, schema(index).dataType)
          if (requiredColumns.contains(colName)) Some(castedValue) else None
      }
    })
    // remove None type in seq
    val tmpRemoveNone: RDD[Seq[Any]] =
      tmp.map((seq: Seq[Option[Any]]) =>
        seq.filter((_: Option[Any]).isDefined).map((value: Option[Any]) => value.get))

    val rowsFinish: RDD[Row] = tmpRemoveNone.map((seq: Seq[Any]) => Row.fromSeq(seq))
    sftpClient.isClosed
    ssh.disconnect()
    rowsFinish
  }



  /**
   * return iterator after writing
   */
  private def sftpFileToRow(file: SftpFile, sftpClient: SftpClient): Iterator[Row] = {
    val path: String = file.getAbsolutePath
    val stream: InputStream = sftpClient.getInputStream(file.getAbsolutePath)
    val fileName: String = file.getFilename
    val size: Long = file.getAttributes.getSize.longValue()
    val iterator: Iterator[Array[Byte]] = IOUtils.splitRemoteStream(stream, size)
    val suffix: String = CommonFileUtil.getFileSuffix(fileName)
    if (iterator.isEmpty) {
      val row: Row = filledSchema(fileName, path, suffix, size, new Array[Byte](0))
      return Iterator(row)
    }
    val rows: Iterator[Row] =
      iterator.map((array: Array[Byte]) =>
      filledSchema(fileName, path, suffix, size, array))
    rows
  }



  /**
   * write the row
   */
  private def filledSchema(fileName: String, path: String, suffix: String,
                           size: Long, array: Array[Byte]): Row = {
    val list: List[Array[Byte]] = IOUtils.splitArrayByNumber(array, SPLIT_NUMBER)
    val byteContent: Row = Row.fromSeq(list)
    Row(fileName, path, suffix, size, byteContent)
  }



  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val path: String = parameter(PATH)
    data.write.format("delta")
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append).save(path)
  }
}
