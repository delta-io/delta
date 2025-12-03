/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import io.delta.standalone.{DeltaLog, Operation, OptimisticTransaction}
import io.delta.standalone.actions.{Metadata => MetadataJ}
import io.delta.standalone.types.{StringType, StructType}

import io.delta.standalone.internal.util.{ConversionUtils, FileNames}
import io.delta.standalone.internal.util.TestUtils._

trait DeltaRetentionSuiteBase extends FunSuite {

  val metadataJ = MetadataJ.builder().schema(new StructType().add("part", new StringType())).build()
  val metadata = ConversionUtils.convertMetadataJ(metadataJ)

  protected def hadoopConf: Configuration = {
    val conf = new Configuration()
    conf.set(
      DeltaConfigs.hadoopConfPrefix +
        DeltaConfigs.ENABLE_EXPIRED_LOG_CLEANUP.key.stripPrefix("delta."),
      "false")
    conf
  }

  protected def getDeltaFiles(dir: File): Seq[File] =
    dir.listFiles().filter(_.getName.endsWith(".json"))

  protected def getCheckpointFiles(dir: File): Seq[File] =
    dir.listFiles().filter(f => FileNames.isCheckpointFile(new Path(f.getCanonicalPath)))

  /**
   * Start a txn that disables automatic log cleanup. Some tests may need to manually clean up logs
   * to get deterministic behaviors.
   */
  protected def startTxnWithManualLogCleanup(log: DeltaLog): OptimisticTransaction = {
    val txn = log.startTransaction()
    txn.updateMetadata(metadataJ)
    txn
  }

  test("startTxnWithManualLogCleanup") {
    withTempDir { dir =>
      val log = DeltaLogImpl.forTable(hadoopConf, dir.getCanonicalPath)
      startTxnWithManualLogCleanup(log)
        .commit(Nil, new Operation(Operation.Name.MANUAL_UPDATE), "test-writer-id")
      assert(!log.enableExpiredLogCleanup)
    }
  }
}
