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

package io.delta.storage.cephobjectstore

import org.apache.hadoop.fs.{PositionedReadable, Seekable}
import java.io.{ByteArrayInputStream, IOException}

/**
 * The input stream for Ceph RGW.
 * The class uses downloading to read data from the object content
 * stream.
 */
class CephStoreInputStream(buf: Array[Byte])
  extends ByteArrayInputStream(buf) with Seekable with PositionedReadable {
  @throws[IOException]
  override def read(position: Long, buffer: Array[Byte], offset: Int, length: Int): Int = {
    System.arraycopy(buf, position.toInt, buffer, offset, length)
    length
  }

  override def readFully(position: Long, buffer: Array[Byte], offset: Int, length: Int): Unit = {
    read(position, buffer, offset, length)
  }

  @throws[IOException]
  override def readFully(position: Long, buffer: Array[Byte]): Unit = {
    read(position, buffer, 0, buffer.length)
  }

  @throws[IOException]
  override def seek(pos: Long): Unit = {
    reset()
    skip(pos)
  }

  @throws[IOException]
  override def getPos: Long = pos

  @throws[IOException]
  override def seekToNewSource(targetPos: Long): Boolean = {
    false
  }
  }
