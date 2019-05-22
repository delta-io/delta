/*
 * Copyright 2019 Databricks, Inc.
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

import java.io.{BufferedReader, FileNotFoundException, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.JavaConverters._

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * Default implementation of [[LogStore]] for the [[FileSystem]] implementations.
 */
abstract class FileSystemLogStore(
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends LogStore {

  def this(sc: SparkContext) = this(sc.getConf, sc.hadoopConfiguration)

  protected def getHadoopConfiguration: Configuration = {
    SparkSession.getActiveSession.map(_.sessionState.newHadoopConf()).getOrElse(hadoopConf)
  }

  override def read(path: Path): Seq[String] = {
    val fs = path.getFileSystem(getHadoopConfiguration)
    val stream = fs.open(path)
    try {
      val reader = new BufferedReader(new InputStreamReader(stream, UTF_8))
      IOUtils.readLines(reader).asScala.map(_.trim)
    } finally {
      stream.close()
    }
  }

  override def listFrom(path: Path): Iterator[FileStatus] = {
    val fs = path.getFileSystem(getHadoopConfiguration)
    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
    }
    val files = fs.listStatus(path.getParent)
    files.filter(_.getPath.getName >= path.getName).sortBy(_.getPath.getName).iterator
  }

  override def resolvePathOnPhysicalStorage(path: Path): Path = {
    path.getFileSystem(getHadoopConfiguration).makeQualified(path)
  }
}
