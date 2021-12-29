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

import com.google.common.util.concurrent.{ListeningExecutorService, MoreExecutors}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalDirAllocator, Path}
import org.apache.log4j.Logger
import org.javaswift.joss.model.Account
import java.io._
import java.util

/**
 * Asynchronous multi-part based uploading mechanism to support huge file
 *  Data will be buffered on local disk, then uploaded
 */
class CephStoreOutputStream extends OutputStream {
  private val LOG = Logger.getLogger(this.getClass)
  private var account: Account = null
  private var conf: Configuration = null
  private var closed = false
  private var key: String = null
  private var blockFile: File = null
  private val blockFiles = new util.HashMap[Integer, File]
  private var blockSize = 0L
  private var blockId = 0
  private var blockWritten = 0L
  private var blockStream: OutputStream = null
  private val singleByte = new Array[Byte](1)
  private var directoryAllocator: LocalDirAllocator = null
  private val boundedThreadPool: ListeningExecutorService = null
  private val blockOutputActiveBlocks: Integer = null
  private var bucketName: String = null

  def this(conf: Configuration, store: Account, key: String, blockSize: Long, bucketName: String) {
    this()
    this.bucketName = bucketName
    this.account = store
    this.conf = conf
    this.key = key
    this.blockSize = blockSize
    this.blockFile = newBlockFile
    this.blockStream = new BufferedOutputStream(new FileOutputStream(blockFile))
  }


  private def newBlockFile: File = {
    if (conf.get("fs.ceph.tmp.dir") == null) conf.set(
      "fs.ceph.tmp.dir", conf.get("hadoop.tmp.dir") + "/oss")
    if (directoryAllocator == null) directoryAllocator = new LocalDirAllocator("fs.ceph.tmp.dir")
    directoryAllocator.createTmpFileForWrite(
      String.format("ceph-block-%04d-", Int.box(blockId)), blockSize, conf)
  }


  override def flush(): Unit = {
    blockStream.flush()
  }

  override def close(): Unit = {
    if (closed) return
    blockStream.flush()
    blockStream.close()
    if (!blockFiles.values.contains(blockFile)) {
      blockId += 1
      blockFiles.put(blockId, blockFile)
    }
    try if (blockFiles.size == 1) {
      val storeObject = blockFile.getAbsoluteFile
      val fis = new FileInputStream(storeObject)
      val hdfsPath = new Path(key)
      val objectPath = hdfsPath.toUri.getPath
      account.getContainer(bucketName).getObject(objectPath.substring(1)).uploadObject(fis)
    }
    removeTemporaryFiles()
    closed = true

  }

  @throws[IOException]
  override def write(b: Int): Unit = {
    singleByte(0) = b.toByte
    write(singleByte, 0, 1)
  }

  @throws[IOException]
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    if (closed) throw new IOException("Stream closed.")
    blockStream.write(b, off, len)
    blockWritten += len
    if (blockWritten >= blockSize) {
      blockWritten = 0L
    }
  }

  private def removeTemporaryFiles(): Unit = {
    blockFiles.values().forEach(file => {
      if (file != null && file.exists && !file.delete) LOG.warn("Failed to delete temporary file")
    })
  }

}
