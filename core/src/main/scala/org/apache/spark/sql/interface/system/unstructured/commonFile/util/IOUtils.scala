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

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.spark.sql.interface.system.util.PropertiesUtil

import java.io._
import java.nio.ByteBuffer
import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.language.postfixOps


/**
 * Author: CHEN ZHI LING
 * Date: 2021/8/17
 * Description: IO function
 */
object IOUtils {



  private val ONE_ROW_MAX_ARRAY_SIZE: Int =
    PropertiesUtil.chooseProperties("file", "file.one.row.max.size").toInt




  @throws[IOException]
  def writeBytes(in: ByteArrayInputStream, out: FileOutputStream, buffSize: Int): Unit = {
    val buffer = new Array[Byte](buffSize)
    var len = 0
    while({len = in.read(buffer); len } != -1) {
      out.write(buffer, 0, len)
    }
    out.flush()
    out.close()
    in.close()
  }



  def splitArrayByNumber(array: Array[Byte], number: Int): List[Array[Byte]] = {
    val list = new ListBuffer[Array[Byte]]
    val length: Int = array.length
    val arraySize: Int = length / number
    val remainder: Int = length % number
    val buffer: ByteBuffer = ByteBuffer.wrap(array)
    for (_ <- 1 until number) {
      val subArray = new Array[Byte](arraySize)
      list.append(subArray)
      buffer.get(subArray, 0, subArray.length)
    }
    val last = new Array[Byte](arraySize + remainder)
    buffer.get(last, 0, last.length)
    list.append(last)
    list.toList
  }



  @tailrec
  def joinArray(list: List[Array[Byte]]): Array[Byte] = {
    val length: Int = list.length
    if (2 == length) {
      return joinTwoArray(list.head, list.last)
    }
    val bytes: Array[Byte] = joinTwoArray(list.head, list(1))
    val tail: List[Array[Byte]] = list.tail.tail
    val newList: List[Array[Byte]] = bytes :: tail
    joinArray(newList)
  }



  def joinTwoArray(arrayOne: Array[Byte], arrayTwo: Array[Byte]): Array[Byte] = {
    val bytes: Array[Byte] = ByteBuffer.allocate(arrayOne.length + arrayTwo.length)
      .put(arrayOne)
      .put(arrayTwo).array()
    bytes
  }



  def splitStream(stream: FSDataInputStream, fileSize: Long): Iterator[Array[Byte]] = {

    val numOfChunks: Long = Math.ceil(fileSize.toDouble / ONE_ROW_MAX_ARRAY_SIZE).toInt
    val result = new ArrayBuffer[Array[Byte]]()
    for (i <- 0L until numOfChunks) {
      val start: Long = i * ONE_ROW_MAX_ARRAY_SIZE
      val length: Long = Math.min(fileSize - start, ONE_ROW_MAX_ARRAY_SIZE)
      val subArray: Array[Byte] = splitStream(stream, length.toInt)
      result.append(subArray)
    }
    result.toIterator
  }



  def splitStream(stream: FSDataInputStream, size: Int): Array[Byte] = {
    val bytes = new Array[Byte](size)
    var fileNameReadLength = 0
    var hasReadLength = 0
    while ({fileNameReadLength = stream.read(bytes, hasReadLength, size - hasReadLength)
      ; fileNameReadLength } > 0) {
      hasReadLength = hasReadLength + fileNameReadLength
    }
    bytes
  }



  def splitRemoteStream(stream: InputStream, fileSize: Long): Iterator[Array[Byte]] = {
    val numOfChunks: Long = Math.ceil(fileSize.toDouble / ONE_ROW_MAX_ARRAY_SIZE).toInt
    val result = new ArrayBuffer[Array[Byte]]()
    for (i <- 0L until numOfChunks) {
      val start: Long = i * ONE_ROW_MAX_ARRAY_SIZE
      val length: Long = Math.min(fileSize - start, ONE_ROW_MAX_ARRAY_SIZE)
      val subArray: Array[Byte] = splitRemoteStream(stream, length.toInt)
      result.append(subArray)
    }
    result.toIterator
  }



  def splitRemoteStream(stream: InputStream, size: Int): Array[Byte] = {
    val bytes: Array[Byte] = new Array[Byte](size)
    var fileNameReadLength: Int = 0
    var hasReadLength: Int = 0
    while ({fileNameReadLength = stream.read(bytes, hasReadLength, size - hasReadLength)
      ;fileNameReadLength} > 0) {
      hasReadLength = hasReadLength + fileNameReadLength
    }
    bytes
  }
}
