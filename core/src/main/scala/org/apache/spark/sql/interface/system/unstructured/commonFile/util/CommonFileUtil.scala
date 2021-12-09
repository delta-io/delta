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

import com.sshtools.sftp.{SftpClient, SftpFile}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}


/**
 * Author: CHEN ZHI LING
 * Date: 2021/12/1
 * Description:
 */
object CommonFileUtil {



  def listFiles(path: Path, fs: FileSystem): Array[String] = {
    fs.listStatus(path).filter((_: FileStatus).isFile).map((_: FileStatus).getPath.toString) ++
      fs.listStatus(path).filter((_: FileStatus).isDirectory).flatMap((subPath: FileStatus) =>
        listFiles(subPath.getPath, fs))
  }



  def listRemoteFiles(remotePath: String, client: SftpClient): Array[SftpFile] = {
    client.ls(remotePath).filter((_: SftpFile).isFile) ++
      client.ls(remotePath).filter(!(_: SftpFile).toString.endsWith("."))
        .filter((_: SftpFile).isDirectory ).flatMap((subPath: SftpFile) =>
        listRemoteFiles(subPath.toString, client))
  }


  def getFileSuffix(fileName: String): String = {
    if (fileName.nonEmpty) {
      val dot: Int = fileName.lastIndexOf(".")
      if ((dot > -1) && (dot < (fileName.length - 1))) {
        return fileName.substring(dot + 1)
      }
    }
    fileName
  }
}
