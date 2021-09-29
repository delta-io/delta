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

package io.delta.standalone.internal

import java.io.File

import io.delta.standalone.{DeltaLog, OptimisticTransaction}
import io.delta.standalone.internal.actions.Metadata
import io.delta.standalone.internal.util.{ConversionUtils, FileNames}
import org.apache.hadoop.fs.Path

trait DeltaRetentionSuiteBase {

  protected def getDeltaFiles(dir: File): Seq[File] =
    dir.listFiles().filter(_.getName.endsWith(".json"))

  protected def getCheckpointFiles(dir: File): Seq[File] =
    dir.listFiles().filter(f => FileNames.isCheckpointFile(new Path(f.getCanonicalPath)))

  /**
   * Start a txn that disables automatic log cleanup. Some tests may need to manually clean up logs
   * to get deterministic behaviors.
   */
  // TODO: this is dependent on withGlobalConfigDefaults
  //  protected def startTxnWithManualLogCleanup(log: DeltaLog): OptimisticTransaction = {
  //    val txn = log.startTransaction()
  //    txn.updateMetadata(ConversionUtils.convertMetadata(Metadata()))
  //    txn
  //  }
}
