/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.storage

import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileAlreadyExistsException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf

/**
 * LogStore implementation for IBM Cloud Object Storage.
 *
 * We assume the following from COS's [[FileSystem]] implementations:
 * - Write on COS is all-or-nothing, whether overwrite or not.
 * - Write is atomic.
 *   Note: Write is atomic when using the Stocator v1.1.2+ - Storage Connector for Apache Spark
 *   (https://github.com/CODAIT/stocator) by setting the configuration `fs.cos.atomic.write` to true
 *   (for more info see the documentation for Stocator)
 * - List-after-write is consistent.
 */
class COSLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
  extends HadoopFileSystemLogStore(sparkConf, hadoopConf) {
  assert(hadoopConf.getBoolean("fs.cos.atomic.write", false) == true,
    "'fs.cos.atomic.write' must be set to true to use COSLogStore")

  override def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val fs = path.getFileSystem(hadoopConf)

    val exists = fs.exists(path)
    if (exists && overwrite == false) {
      throw new FileAlreadyExistsException(path.toString)
    } else {
      // create is atomic
      val stream = fs.create(path, overwrite)
      try {
        var writeSize = 0L
        actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(action => {
          stream.write(action)
          writeSize += action.length
        })
        stream.close()
      } catch {
          case e: IOException =>
            if (fs.exists(path)) {
              throw new FileAlreadyExistsException(path.toString)
            } else {
              throw new IllegalStateException(s"Failed due to concurrent write", e)
            }
        }
      }
}

  override def invalidateCache(): Unit = {}

  override def isPartialWriteVisible(path: Path): Boolean = false
}
