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

package org.apache.spark.sql.delta.commands.backfill

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{AddFile, Protocol, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * This command re-commits all AddFiles in the current snapshot that do not have a base row IDs.
 * After the backfill command finishes, the snapshot has row IDs for all files. All commits
 * afterwards must include row IDs.
 *
 * First, we will add the table feature support, if necessary.
 * Then, the command will lazily materialize AllFiles and split them into multiple backfill commits
 * if the number of files exceeds the threshold set by
 * [[DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT]].
 *
 * Note: We expect Backfill to be called before the table property is set. Furthermore, we do not
 * set the table property [[DeltaConfigs.ROW_TRACKING_ENABLED]] as part of backfill. The metadata
 * update needs to be handled by the caller.
 */
case class RowTrackingBackfillCommand(
    override val deltaLog: DeltaLog,
    override val nameOfTriggeringOperation: String,
    override val catalogTable: Option[CatalogTable])
  extends BackfillCommand {

  override def getBackfillExecutor(
      spark: SparkSession,
      txn: OptimisticTransaction,
      fileMaterializationTracker: FileMetadataMaterializationTracker,
      maxNumBatchesInParallel: Int,
      backfillStats: BackfillCommandStats): BackfillExecutor = {
    new RowTrackingBackfillExecutor(
      spark, txn, fileMaterializationTracker, maxNumBatchesInParallel, backfillStats)
  }

  override def filesToBackfill(txn: OptimisticTransaction): Dataset[AddFile] =
    // Get a new snapshot after the protocol upgrade.
    RowTrackingBackfillExecutor.getCandidateFilesToBackfill(deltaLog.update())

  override def opType: String = "delta.rowTracking.backfill"

  override def constructBatch(files: Seq[AddFile]): BackfillBatch =
    RowTrackingBackfillBatch(files)

 /**
  * Add Row tracking table feature support. This will also upgrade the minWriterVersion if
  * the current protocol cannot support write table feature.
  */
  private def upgradeProtocolIfRequired(): Unit = {
    val snapshot = deltaLog.update()
    if (!snapshot.protocol.isFeatureSupported(RowTrackingFeature)) {
      val minProtocolAllowingWriteTableFeature = Protocol(
        snapshot.protocol.minReaderVersion,
        TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION)
      val newProtocol = snapshot.protocol
        .merge(minProtocolAllowingWriteTableFeature.withFeature(RowTrackingFeature))
      deltaLog.upgradeProtocol(catalogTable, snapshot, newProtocol)
    }
  }

  override def run(spark: SparkSession): Seq[Row] = {
    if (!spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_ROW_TRACKING_BACKFILL_ENABLED)) {
      throw new UnsupportedOperationException("Cannot enable Row IDs on an existing table.")
    }

    // Upgrade the protocol to support the table feature if it isn't already supported.
    // This steps bounds the number of files the command must commit, since all actions after
    // we support the table feature must have base row IDs.
    upgradeProtocolIfRequired()

    super.run(spark)
  }
}
