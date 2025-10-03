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
 * This command cleans up tracking metadata from a delta table. In particular, it removes
 * `baseRowId` and `defaultRowCommitVersion`. This is achieved by re-commiting addFiles with
 * `dataChance = false`. This requires to commit the AddFiles of the entire table.
 *
 * Similarly to all backfilling operations, the relevant files are commited in multiple batches.
 * Each batch contains [[DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT]]. This is to avoid
 * generating large commits in big tables.
 */
case class RowTrackingUnBackfillCommand(
    override val deltaLog: DeltaLog,
    override val nameOfTriggeringOperation: String,
    override val catalogTableOpt: Option[CatalogTable])
  extends BackfillCommand {

  override def getBackfillExecutor(
      spark: SparkSession,
      deltaLog: DeltaLog,
      catalogTableOpt: Option[CatalogTable],
      backfillId: String,
      backfillStats: BackfillCommandStats): BackfillExecutor = {
    new RowTrackingUnBackfillExecutor(spark, deltaLog, catalogTableOpt, backfillId, backfillStats)
  }

  override def opType: String = "delta.rowTracking.unbackfill"
}
