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

package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, Checkpoint, SingleAction}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat, lit, raise_error, udf}

/**
 * An [[AMTCheckpointProvider]] whose state reconstruction builds each `AddFile` field as a column
 * over the persisted manifest rows (see [[addFileColumn]]). Enabled by
 * [[DeltaSQLConf.AMT_COLUMNAR_STATE_RECONSTRUCTION_ENABLED]].
 *
 * @param rootInfo The root's leaf pointers and optional prefetched rows.
 */
final class ColumnarAMTCheckpointProvider(
    manifestCommitVersion: Long,
    checkpointAction: Checkpoint,
    tableRoot: Path,
    private val rootInfo: AMTRootInfo)
  extends AMTCheckpointProvider(
    manifestCommitVersion, checkpointAction, rootInfo.leaves, tableRoot) {

  override protected def rootLiveAddSingleActions(
      spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction] = {
    // Reuse the prefetched root if possible.
    val entries = rootInfo.toDF(spark).map { root =>
      // The shared back-reference expression requires a non-null pos, unused for root entries.
      withRootEntryContext(dataEntries(root.withColumn("pos", lit(0))))
    }.getOrElse(rootEntries(deltaLog))
    toAddSingleActions(
      withStatuses(entries, Tracking.Status.liveEntryStatuses),
      baseRowId = col("tracking.first_row_id"))
  }

  override protected def leafLiveAddSingleActions(deltaLog: DeltaLog): Dataset[SingleAction] =
    toAddSingleActions(
      withStatuses(
        leafEntriesWithRowIdPrefix(deltaLog, liveLeavesWithManifestFilters),
        Tracking.Status.liveEntryStatuses),
      baseRowId = inheritedBaseRowId)

  /** Reuses prefetched root for incremental writes as well as snapshot state reconstruction. */
  override private[amt] def readLiveRootDataEntries(
      spark: SparkSession, deltaLog: DeltaLog): Seq[AddFile] =
    rootLiveAddSingleActions(spark, deltaLog).collect().toSeq.map(_.add)
}

object ColumnarAMTCheckpointProvider {
  /** RFC-2396 encoding of an AMT `location`, as [[DataEntry.toAddFile]] does. */
  private[amt] val encodePath =
    udf((location: String) => SparkPath.fromPathString(location).urlEncoded)

  /** Fails the read of an entry row with `reason`, naming the entry's data file. */
  private[amt] def malformedEntry(reason: Column*): Column =
    raise_error(concat(
      Seq(lit("Malformed AMT data entry '"), col("location"), lit("': ")) ++ reason: _*))

  /** An AMT deletion vector struct -> Delta's descriptor, as [[DeletionVector.toDescriptor]]. */
  private[amt] def toDescriptor(tableRoot: Path) = udf { (dv: Row) =>
    Option(dv).map { dv =>
      DeletionVector.toDescriptor(
        DeletionVector(
          location = dv.getAs[String]("location"),
          offset = dv.getAs[Long]("offset"),
          size_in_bytes = dv.getAs[Long]("size_in_bytes"),
          cardinality = dv.getAs[Long]("cardinality")),
        tableRoot)
    }.orNull
  }
}
