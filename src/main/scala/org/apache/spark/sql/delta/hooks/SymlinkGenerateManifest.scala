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

/**
 * Post commit hook to generate hive-style manifests for Delta table. This is useful for
 * compatibility with Presto / Athena.
 */
object SymlinkGenerateManifest extends GenerateManifest {

  override type T = PrestoManifestRawEntry
  override def manifestType(): ManifestType = SymlinkManifestType

  override protected def getManifestContent(ds: DataFrame): Dataset[PrestoManifestRawEntry] = {
    implicit val encoder = Encoders.product[PrestoManifestRawEntry]
    ds.select(RELATIVE_PARTITION_DIR_COL_NAME, "path").as[PrestoManifestRawEntry]
  }

  override protected def writeSingleManifestFile(manifestDirAbsPath: String,
      manifestRawEntries:
      Iterator[PrestoManifestRawEntry],
      tableAbsPathForManifest: String,
      hadoopConf: SerializableConfiguration) = {
    val manifestFilePath = createManifestDir(manifestDirAbsPath, hadoopConf)

    val manifestContent = manifestRawEntries.map { rawEntry =>
      DeltaFileOperations.absolutePath(tableAbsPathForManifest, rawEntry.path).toString
    }

    val logStore = LogStore(SparkEnv.get.conf, hadoopConf.value)
    logStore.write(manifestFilePath, manifestContent, overwrite = true)
  }

}

case class PrestoManifestRawEntry(override val relativePartitionDir: String,
  override val path: String) extends ManifestRawEntry
