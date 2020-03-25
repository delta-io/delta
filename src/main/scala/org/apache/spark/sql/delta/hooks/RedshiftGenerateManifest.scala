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

case class RedshiftManifestEntryMetadata(content_length: Long)
case class RedshiftManifestEntry(url: String, meta: RedshiftManifestEntryMetadata)
case class RedshiftManifest(entries: Seq[RedshiftManifestEntry])

object RedshiftGenerateManifest extends GenerateSymlinkManifest {
  override def manifestType(): ManifestType = RedshiftManifestType

  override type T = RedshiftManifestRawEntry

  override protected def getManifestContent(ds: DataFrame): Dataset[RedshiftManifestRawEntry] = {
    implicit val encoder = Encoders.product[RedshiftManifestRawEntry]
    ds.select(RELATIVE_PARTITION_DIR_COL_NAME, "path", "size").as[RedshiftManifestRawEntry]
  }

  override protected def writeSingleManifestFile(manifestDirAbsPath: String,
                                                 manifestRawEntries:
                                                 Iterator[RedshiftManifestRawEntry],
                                                 tableAbsPathForManifest: String,
                                                 hadoopConf: SerializableConfiguration): Unit = {
    val manifestFilePath = createManifestDir(manifestDirAbsPath, hadoopConf)

    val entries = manifestRawEntries
      .map(
        rawEntry =>
          RedshiftManifestEntry(
            DeltaFileOperations.absolutePath(
              tableAbsPathForManifest, rawEntry.path
            ).toString,
            RedshiftManifestEntryMetadata(rawEntry.size))
      )
      .toSeq

    val manifest = RedshiftManifest(entries)
    implicit val formats = Serialization.formats(NoTypeHints)
    val manifestContent = write(manifest)

    val logStore = LogStore(SparkEnv.get.conf, hadoopConf.value)
    logStore.write(manifestFilePath, Iterator(manifestContent), overwrite = true)
    manifestFilePath.toString
  }

}

case class RedshiftManifestRawEntry(override val relativePartitionDir: String,
                                    override val path: String,
                                    size: Long) extends ManifestRawEntry
