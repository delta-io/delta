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

package org.apache.spark.sql.delta.files

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile}
import org.apache.spark.sql.delta.commands.cdc.CDCReader.CDCDataSpec
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}

/**
 * A [[TahoeFileIndex]] for scanning a sequence of CDC files. Similar to [[TahoeBatchFileIndex]],
 * the equivalent for reading [[AddFile]] actions.
 */
class TahoeChangeFileIndex(
                            spark: SparkSession,
                            filesByVersion: Seq[CDCDataSpec[AddCDCFile]],
                            deltaLog: DeltaLog,
                            path: Path,
                            snapshot: Snapshot)
  extends TahoeCDCBaseFileIndex(spark, filesByVersion, deltaLog, path, snapshot) {
}
