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
package org.apache.spark.sql.interface.system.unstructured.commonFile.util

import com.sshtools.net.SocketTransport
import com.sshtools.sftp.SftpClient
import com.sshtools.ssh.{PasswordAuthentication, SshAuthentication, SshConnector}
import com.sshtools.ssh2.Ssh2Client
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer



/**
 * Author: CHEN ZHI LING
 * Date: 2021/8/17
 * Description:
 */
object RemoteFileUtil {



  private[commonFile] val IP: String = "ip"
  private[commonFile] val PORT: String = "port"
  private[commonFile] val USERNAME: String = "username"
  private[commonFile] val PASSWORD: String = "password"
  private[commonFile] val PATH: String = "path"


  /**
   * create connection to remote computer
   */
  def createConnect(parameter: Map[String, String]): Ssh2Client = {
    val connector: SshConnector = SshConnector.createInstance()
    val transport = new SocketTransport(parameter(IP), parameter(PORT).toInt)
    val ssh: Ssh2Client = connector.connect(transport, parameter(USERNAME))
    val authentication = new PasswordAuthentication()
    authentication.setPassword(parameter(PASSWORD))
    val i: Int = ssh.authenticate(authentication)
    if(i != SshAuthentication.COMPLETE) {
      throw new RuntimeException("connection failed")
    }
    ssh
  }



  def getSftpClient(ssh: Ssh2Client): SftpClient = {
    val sftpClient = new SftpClient(ssh)
    sftpClient.setTransferMode(SftpClient.MODE_BINARY)
    sftpClient
  }

  def getRemoteFile(nestRow: Row): Array[Byte] = {
    val list = new ListBuffer[Array[Byte]]
    for(i <- 0 until nestRow.length) { list.append(nestRow.getAs[Array[Byte]](i)) }

    if (list.toList.size == 1) {
      return list.head
    }
    IOUtils.joinArray(list.toList)
  }



  def getNestRow(row: Row): Row = {
    row.getStruct(4)
  }



  def getFileName(row: Row): String = {
    row.getString(0)
  }
}
