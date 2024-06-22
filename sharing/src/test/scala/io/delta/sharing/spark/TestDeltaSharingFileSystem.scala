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

package io.delta.sharing.spark

import java.io.FileNotFoundException
import java.net.{URI, URLDecoder, URLEncoder}
import java.util.concurrent.TimeUnit

import io.delta.sharing.client.DeltaSharingFileSystem
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import org.apache.spark.SparkEnv
import org.apache.spark.delta.sharing.{PreSignedUrlCache, PreSignedUrlFetcher}
import org.apache.spark.storage.BlockId

/**
 * Read-only file system for DeltaSharingDataSourceDeltaSuite.
 * To replace DeltaSharingFileSystem and return the content for parquet files.
 */
private[spark] class TestDeltaSharingFileSystem extends FileSystem {
  import TestDeltaSharingFileSystem._

  private lazy val preSignedUrlCacheRef = PreSignedUrlCache.getEndpointRefInExecutor(SparkEnv.get)

  override def getScheme: String = SCHEME

  override def getUri(): URI = URI.create(s"$SCHEME:///")

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    val path = DeltaSharingFileSystem.decode(f)
    val fetcher =
      new PreSignedUrlFetcher(
        preSignedUrlCacheRef,
        path.tablePath,
        path.fileId,
        TimeUnit.MINUTES.toMillis(10)
      )
    val (tableName, parquetFilePath) = decode(fetcher.getUrl())
    val arrayBuilder = Array.newBuilder[Byte]
    val iterator = SparkEnv.get.blockManager
      .get[Byte](getBlockId(tableName, parquetFilePath))
      .map(
        _.data.asInstanceOf[Iterator[Byte]]
      )
      .getOrElse {
        throw new FileNotFoundException(f.toString)
      }
    while (iterator.hasNext) {
      arrayBuilder += iterator.next()
    }
    new FSDataInputStream(new SeekableByteArrayInputStream(arrayBuilder.result()))
  }

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream =
    throw new UnsupportedOperationException("create")

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
    throw new UnsupportedOperationException("append")

  override def rename(src: Path, dst: Path): Boolean =
    throw new UnsupportedOperationException("rename")

  override def delete(f: Path, recursive: Boolean): Boolean =
    throw new UnsupportedOperationException("delete")

  override def listStatus(f: Path): Array[FileStatus] =
    throw new UnsupportedOperationException("listStatus")

  override def setWorkingDirectory(new_dir: Path): Unit =
    throw new UnsupportedOperationException("setWorkingDirectory")

  override def getWorkingDirectory: Path = new Path(getUri)

  override def mkdirs(f: Path, permission: FsPermission): Boolean =
    throw new UnsupportedOperationException("mkdirs")

  override def getFileStatus(f: Path): FileStatus = {
    val resolved = makeQualified(f)
    new FileStatus(DeltaSharingFileSystem.decode(resolved).fileSize, false, 0, 1, 0, f)
  }

  override def close(): Unit = {
    super.close()
  }
}

private[spark] object TestDeltaSharingFileSystem {
  val SCHEME = "delta-sharing"

  def getBlockId(tableName: String, parquetFilePath: String): BlockId = {
    BlockId(
      s"${DeltaSharingUtils.DELTA_SHARING_BLOCK_ID_PREFIX}_" +
      s"{$tableName}_$parquetFilePath"
    )
  }

  // The encoded string is purely for testing purpose to contain the table name and file path,
  // which will be decoded and used to find block in block manager.
  // In real traffic, it will be a pre-signed url.
  def encode(tableName: String, parquetFilePath: String): String = {
    val encodedTableName = URLEncoder.encode(tableName, "UTF-8")
    val encodedParquetFilePath = URLEncoder.encode(parquetFilePath, "UTF-8")
    // SCHEME:/// is needed for making this path an absolute path
    s"$SCHEME:///$encodedTableName/$encodedParquetFilePath"
  }

  def decode(encodedPath: String): (String, String) = {
    val Array(tableName, parquetFilePath) = encodedPath
      .stripPrefix(s"$SCHEME:///")
      .stripPrefix(s"$SCHEME:/")
      .split("/")
      .map(
        URLDecoder.decode(_, "UTF-8")
      )
    (tableName, parquetFilePath)
  }
}
