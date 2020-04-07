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

package org.apache.spark.sql.delta.hooks

import org.apache.spark.SparkEnv
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.{DataFrame, Dataset, Encoders}
import org.apache.spark.util.SerializableConfiguration
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

case class JsonManifestEntryMetadata(content_length: Long)
case class JsonManifestEntry(url: String, meta: JsonManifestEntryMetadata)
case class JsonManifest(entries: Seq[JsonManifestEntry])

/**
 * Post commit hook to generate Redshift spectrum style manifests for Delta table.
 * This is useful for compatibility with Redshift.
 */
object GenerateJsonManifest extends GenerateManifestImpl {

  override def manifestType(): ManifestType = JsonManifestType
  override type T = JsonManifestRawEntry

  override protected def getManifestContent(ds: DataFrame): Dataset[JsonManifestRawEntry] = {
    implicit val encoder = Encoders.product[JsonManifestRawEntry]
    ds.select(RELATIVE_PARTITION_DIR_COL_NAME, "path", "size").as[JsonManifestRawEntry]
  }

  override protected def writeSingleManifestFile(manifestDirAbsPath: String,
      manifestRawEntries:
      Iterator[JsonManifestRawEntry],
      tableAbsPathForManifest: String,
      hadoopConf: SerializableConfiguration): Unit = {
    val manifestFilePath = createManifestDir(manifestDirAbsPath, hadoopConf)

    val entries = manifestRawEntries
      .map(
        rawEntry =>
          JsonManifestEntry(
            DeltaFileOperations.absolutePath(
              tableAbsPathForManifest, rawEntry.path
            ).toString,
            JsonManifestEntryMetadata(rawEntry.size))
      )
      .toSeq

    val manifest = JsonManifest(entries)
    implicit val formats = Serialization.formats(NoTypeHints)
    val manifestContent = write(manifest)

    val logStore = LogStore(SparkEnv.get.conf, hadoopConf.value)
    logStore.write(manifestFilePath, Iterator(manifestContent), overwrite = true)
    manifestFilePath.toString
  }

}

case class JsonManifestRawEntry(override val relativePartitionDir: String,
     override val path: String, size: Long) extends ManifestRawEntry
