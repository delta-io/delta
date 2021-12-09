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

import com.sshtools.sftp.{SftpClient, SftpFile}
import com.sshtools.ssh2.Ssh2Client
import org.apache.spark.sql.interface.system.unstructured.commonFile.util.RemoteFileUtil
import org.apache.spark.sql.interface.system.unstructured.commonFile.util.RemoteFileUtil.{PATH, createConnect}
import org.apache.spark.sql.interface.system.util.MultiSourceException
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}

import java.io.ByteArrayInputStream
import java.util

/**
 * The remoteFile data source.
 * It reads remote computer's binary files and
 * converts each file into a single record that contains the raw content
 * and metadata of the file.
 * The schema is same to the commonFile data source
 * Example:
 * {{{
 *   // Scala
 *   val df = spark.read.format("remoteFile")
 *     .load("/path/to/fileDir")
 *
 *   // Java
 *   Dataset<Row> df = spark.read().format("remoteFile")
 *     .load("/path/to/fileDir");
 * }}}
 */

/**
 * Author: CHEN ZHI LING
 * Date: 2021/8/17
 * Description:
 */
private [commonFile]class RemoteFileSource
  extends RelationProvider
  with CreatableRelationProvider
  with DataSourceRegister{



  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val params: Map[String, String] = initParams(parameters)
    new RemoteFileRelation(sqlContext, params)
  }


  override def shortName(): String = "remoteFile"


  /**
   * write raw content to remote computer which is design by your
   */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val params: Map[String, String] = initParams(parameters)
    val writePath: String = params(PATH)
    val ssh: Ssh2Client = createConnect(params)
    val sftpClient = new SftpClient(ssh)
    // user binary mode to transfer data
    sftpClient.setTransferMode(SftpClient.MODE_BINARY)
    val file: SftpFile = sftpClient.openFile(writePath)
    if(file.isFile) {
      throw new MultiSourceException(file.toString + " is not a directory")
    }
    val iterator: util.Iterator[Row] = data.toLocalIterator()
    while (iterator.hasNext) {
      val row: Row = iterator.next()
      val nestRow: Row = RemoteFileUtil.getNestRow(row)
      val fileName: String = RemoteFileUtil.getFileName(row)
      val content: Array[Byte] = RemoteFileUtil.getRemoteFile(nestRow)
      val stream = new ByteArrayInputStream(content)
      val builder = new StringBuilder(writePath + fileName)
      sftpClient.put(stream, builder.toString())
    }
    sftpClient.isClosed
    ssh.disconnect()
    new RemoteFileRelation(sqlContext, parameters)
  }


  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path",
      throw new IllegalArgumentException("path must be specified."))
  }


  /**
   * initial the params
   * ip,port,username,password is necessary to connect the remote computer
   */
  def initParams(parameters: Map[String, String]): Map[String, String] = {
    val ip: String = parameters.getOrElse(
      "ip", throw new IllegalArgumentException("ip must be specified."))
    val port: String = parameters.getOrElse(
      "port", throw new IllegalArgumentException("port must be specified."))
    val username: String = parameters.getOrElse(
      "username", throw new IllegalArgumentException("username must be specified."))
    val password: String = parameters.getOrElse(
      "password", throw new IllegalArgumentException("password must be specified."))
    val remotePath: String = checkPath(parameters)
    val params: Map[String, String] = Map[String, String](
      "ip" -> ip,
      "port" -> port,
      "username" -> username,
      "password" -> password,
      "path" -> remotePath)
    params
  }
}
