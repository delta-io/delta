/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.{CheckpointProvider, DeltaLog}
import org.apache.spark.sql.delta.actions.{Action, Checkpoint, DomainMetadata, SetTransaction, SidecarFile, SidecarType, SingleAction}
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, lit, raise_error, when}

/**
 * Reads the non-content (txn and domain metadata) sidecars of an AMT checkpoint (see
 * [[AMTNonContentSidecars]]).
 *
 * Reading is always on, so a checkpoint with such sidecars is readable whatever writer produced
 * it. Two read paths exist:
 *  - [[loadSidecarActions]]: a lazy [[Dataset]] for snapshot state reconstruction.
 *  - [[readTxnSidecars]] / [[readDomainMetadataSidecars]]: a driver read for the incremental AMT
 *    writer.
 * On both paths, a row that does not carry the action of its sidecar type fails the read.
 */
object AMTNonContentSidecarReader extends DeltaLogging {
  import AMTNonContentSidecars._

  /**
   * Helper to read bunch of sidecar files of `sidecarType` as `DataSet[SingleAction]`.
   * Also validates that all the rows in the given sidecars have data for given sidecarType.
   */
  private def sidecarRows(
      deltaLog: DeltaLog,
      sidecars: Seq[SidecarFile],
      sidecarType: String): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    val column = singleActionColumnName(sidecarType)
    val index =
      CheckpointProvider.checkpointFileIndex(sidecars.map(_.toFileStatus(deltaLog.logPath)))
    val validatedColumnExpr = when(col(column).isNull, raise_error(lit(
        s"AMT non-content sidecar of type $sidecarType holds a row without a $column action.")))
      .otherwise(col(column))
    // Project exactly the log schema columns (the log scan may add others, like the commit
    // version), in encoder order, so the result unions by position with other SingleActions.
    val columns = Action.logSchema.fieldNames.toSeq.map { name =>
      if (name == column) validatedColumnExpr.as(name) else col(name)
    }
    deltaLog.loadIndex(index, Action.logSchema)
      .select(columns: _*)
      .as[SingleAction]
  }

  /**
   * The sidecar-carried non-content actions of `checkpoint` as a lazy [[Dataset]], for snapshot
   * state reconstruction; None when the checkpoint has no non-content sidecars.
   */
  def loadSidecarActions(
      spark: SparkSession,
      deltaLog: DeltaLog,
      checkpoint: Checkpoint): Option[Dataset[SingleAction]] = {
    if (!hasNonContentSidecars(checkpoint)) return None
    val dataframesForSidecars = SidecarType.all.toSeq.sorted.flatMap { sidecarType =>
      val sidecarsOfCurrentType = sidecarsOfType(checkpoint, sidecarType)
      if (sidecarsOfCurrentType.isEmpty) {
        None
      } else {
        Some(sidecarRows(deltaLog, sidecarsOfCurrentType, sidecarType))
      }
    }
    dataframesForSidecars.reduceOption(_ union _)
  }

  /** Reads the rows of the sidecars of one type on the driver. */
  private def collectSidecars(
      deltaLog: DeltaLog,
      sidecars: Seq[SidecarFile],
      sidecarType: String): Seq[SingleAction] = {
    if (sidecars.isEmpty) return Seq.empty
    sidecarRows(deltaLog, sidecars, sidecarType).collect().toSeq
  }

  /** Reads the txn sidecars among `sidecars` on the driver. */
  def readTxnSidecars(deltaLog: DeltaLog, sidecars: Seq[SidecarFile]): Seq[SetTransaction] =
    collectSidecars(deltaLog, sidecarsOfType(sidecars, SidecarType.Type.Txn), SidecarType.Type.Txn)
      .map(_.txn)

  /** Reads the domain metadata sidecars among `sidecars` on the driver. */
  def readDomainMetadataSidecars(
      deltaLog: DeltaLog,
      sidecars: Seq[SidecarFile]): Seq[DomainMetadata] =
    collectSidecars(deltaLog,
      sidecarsOfType(sidecars, SidecarType.Type.DomainMetadata), SidecarType.Type.DomainMetadata)
      .map(_.domainMetadata)

  /** The keys that appear more than once, sorted. */
  def duplicateKeys(keys: Seq[String]): Seq[String] =
    keys.groupBy(identity).collect { case (k, vs) if vs.size > 1 => k }.toSeq.sorted
}
