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

package org.apache.spark.sql.delta

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.util.Utils

/**
 * Information about a manifest.
 */
case class DeltaManifest (path: String)

/**
 * General interface writing manifest files for a delta table.
 */
trait DeltaManifestWriter {
  def write(fs: FileSystem, snapshot: Snapshot, path: String): DeltaManifest = write(
    fs, snapshot, Some(path)
  )

  def write(fs: FileSystem, snapshot: Snapshot): DeltaManifest = write(
    fs, snapshot, None
  )

  def write(fs: FileSystem, snapshot: Snapshot, path: Option[String]): DeltaManifest
}

/**
 * Writes SymlinkTextInputFormat manifests.
 */
class SymlinkTextInputFormatWriter extends DeltaManifestWriter
  with BaseManifestFileWriter
  with ManifestFileWriter {

  private def createManifestPath(dataPath: Path): Path = {
    new Path(dataPath, "_symlink_format_manifest")
  }

  private def createFragmentPath(manifestPath: Path, fragment: Path): Path = {
    new Path(new Path(manifestPath, fragment), "manifest.txt")
  }

  private def createManifestContent(dataPath: Path, files: Iterable[AddFile]): String = {
    files
      .map(f => new Path(dataPath, f.path))
      .mkString("\n")
  }

  override def write(fs: FileSystem, snapshot: Snapshot, path: Option[String]): DeltaManifest = {
    val groups = groupByPartitions(snapshot)
    val dataPath = snapshot.deltaLog.dataPath
    val manifestPath = path.map(new Path(_)).getOrElse(createManifestPath(dataPath))

    groups
      .foreach(e => {
        val path = createFragmentPath(manifestPath, e._1)
        val content = createManifestContent(dataPath, e._2)

        writeFile(fs, path, content)
      })

    DeltaManifest(manifestPath.toString)
  }

}

protected trait BaseManifestFileWriter {
  protected def groupByPartitions(snapshot: Snapshot): Map[Path, Iterable[AddFile]] = {
    snapshot.allFiles.collect().seq
      .groupBy(r => new Path(r.path).getParent)
  }
}

protected trait ManifestFileWriter {
  protected def writeFile(fs: FileSystem, path: Path, content: String): Unit = {
    var streamClosed = false
    val stream = fs.create(path, false)

    try {
      if (!fs.exists(path.getParent)) {
        fs.mkdirs(path.getParent)
      }

      stream.writeBytes(content)
      stream.close()

      streamClosed = true

    } finally {
      if (!streamClosed) {
        stream.close()
      }
    }
  }
}

trait ManifestWriterProvider {
  val writerClassConfKey: String = "spark.delta.manifest.writer"
  val defaultWriterClassName: String = classOf[SymlinkTextInputFormatWriter].getName

  def createManifestWriter(spark: SparkSession): DeltaManifestWriter = {
    val context = spark.sparkContext
    val sparkConf = context.getConf

    createManifestWriter(sparkConf)
  }

  def createManifestWriter(sparkConf: SparkConf): DeltaManifestWriter = {
    val writerClassName = sparkConf.get(writerClassConfKey, defaultWriterClassName)
    val writerClass = Utils.classForName(writerClassName).asInstanceOf[Class[DeltaManifestWriter]]

    createManifestWriter(writerClass)
  }

  def createManifestWriter(writerClassName: String): DeltaManifestWriter = {
    val classForName = Utils.classForName(writerClassName)
    val writerClass = classForName.asInstanceOf[Class[DeltaManifestWriter]]

    createManifestWriter(writerClass)
  }

  def createManifestWriter(writerClass: Class[DeltaManifestWriter]): DeltaManifestWriter = {
    writerClass.getConstructor()
      .newInstance()
      .asInstanceOf[DeltaManifestWriter]
  }
}
