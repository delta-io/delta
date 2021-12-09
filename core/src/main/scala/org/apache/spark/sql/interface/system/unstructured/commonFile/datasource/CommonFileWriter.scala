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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.interface.system.unstructured.commonFile.datasource.CommonFileSchema.INITIAL_BUFFER_SIZE
import org.apache.spark.sql.interface.system.unstructured.commonFile.util.IOUtils

import java.io.{ByteArrayInputStream, FileOutputStream}



/**
 * Author: CHEN ZHI LING
 * Date: 2021/8/17
 * Description:
 */
class CommonFileWriter(path: String) extends OutputWriter{



  var inputStream: ByteArrayInputStream = _
  var outputStream: FileOutputStream = _


  /**
   * get the raw content from row and write to local path
   */
  override def write(row: InternalRow): Unit = {
    val fileName: String = CommonFileSchema.getFileName(row)
    val builder: String = new StringBuilder(path + fileName).toString()
    val bytes: Array[Byte] = CommonFileSchema.getCommonFile(row)
    inputStream = new ByteArrayInputStream(bytes)
    outputStream = new FileOutputStream(builder, true)
    IOUtils.writeBytes(inputStream, outputStream, INITIAL_BUFFER_SIZE)
  }



  override def close(): Unit = {
    if (null != inputStream)inputStream.close()
    if (null != outputStream) outputStream.close()
  }

  override def path(): String = path
}
