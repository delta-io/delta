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

import org.apache.spark.sql.delta.{CheckpointPolicy, CheckpointProvider, DeltaLog, DeltaLogFileIndex, Snapshot}
import org.apache.spark.sql.delta.DeltaLogFileIndex.COMMIT_VERSION_COLUMN
import org.apache.spark.sql.delta.actions.{BackReference, Checkpoint, ContentRoot, SingleAction}
import org.apache.spark.sql.delta.util.DeltaEncoder
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.execution.datasources.FileFormat.{FILE_PATH, METADATA_NAME}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.StructType

/**
 * A [[CheckpointProvider]] backed by an AMT (Adaptive Metadata Tree) manifest tree.
 *
 * This provider is only for inline manifest-commit checkpoints, and this is not intended for
 * standalone checkpoint which also refers to an AMT.
 *
 * @param checkpointAction The inline-emitted Checkpoint action this tree was committed with;
 *                         carries the version, contentRoot, and inline non-file state.
 * @param leaves           Pointer metadata for each leaf reachable from the root.
 */
final class AMTCheckpointProvider(
    val checkpointAction: Checkpoint,
    val leaves: Seq[AMTCheckpointProvider.LeafInfo])
  extends CheckpointProvider {

  /** The table version the manifest tree describes. */
  def checkpointVersion: Long = checkpointAction.version

  /** Pointer to the root manifest parquet. */
  private def contentRoot: ContentRoot = checkpointAction.contentRoot

  override def version: Long = checkpointAction.version

  override def topLevelFiles: Seq[FileStatus] = {
    val rootPath = new Path(contentRoot.path)
    Seq(new FileStatus(
      /* length = */ contentRoot.sizeInBytes,
      /* isdir = */ false,
      /* block_replication = */ 0,
      /* blocksize = */ 0L,
      // modificationTime is not tracked on the ContentRoot, so report 0.
      // This should not impact readers.
      /* modification_time = */ 0L,
      rootPath))
  }

  override def effectiveCheckpointSizeInBytes(): Long =
    contentRoot.sizeInBytes + leaves.map(_.sizeInBytes).sum

  override def checkpointPolicyForLogging: Option[CheckpointPolicy.Policy] = None

  // Protocol and Metadata are carried inline on the Checkpoint action (not in the AMT), so convert
  // them to a DataFrame with the required schema.
  override def loadProtocolMetadataActions(
      spark: SparkSession, deltaLog: DeltaLog): Option[DataFrame] = {
    import org.apache.spark.sql.delta.implicits._
    val rows = Seq(
      SingleAction(protocol = checkpointAction.protocol),
      SingleAction(metaData = checkpointAction.metaData))
    val df = spark.createDataset(rows).toDF()
      .select(Snapshot.pAndMQuerySchema.fieldNames.toIndexedSeq.map(col): _*)
      .withColumn(COMMIT_VERSION_COLUMN, lit(version))
    Some(df)
  }

  override def loadActionsForStateReconstruction(
      spark: SparkSession, deltaLog: DeltaLog): Option[DataFrame] = {
    val df = allActions(spark, deltaLog).toDF()
      .withColumn(COMMIT_VERSION_COLUMN, lit(version))
      .withColumn(Snapshot.ADD_STATS_TO_USE_COL_NAME, col("add.stats"))
    Some(df)
  }
  /**
   * The full action set of this checkpoint as a distributed [[Dataset]] of [[SingleAction]]: the
   * live file `AddFile`s reconstructed from the AMT(root + leaves), unioned with the inline
   * non-content actions (protocol, metadata, domain metadata, txns) built on the driver.
   *
   * Note: Iceberg metadata inheritance (manifest entries inheriting fields such as partition
   * values, sequence numbers, or snapshot id from the parent manifest) is not supported yet;
   * entries are read as fully materialized rows.
   */
  private def allActions(spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    val nonFileActions = spark.createDataset(nonContentSingleActions)
    liveAddSingleActions(spark, deltaLog).union(nonFileActions)
  }

  /** The inline, non-content actions carried directly on the [[Checkpoint]] action. */
  private def nonContentSingleActions: Seq[SingleAction] =
    Seq(
      SingleAction(protocol = checkpointAction.protocol),
      SingleAction(metaData = checkpointAction.metaData)) ++
    checkpointAction.domainMetadata.map(dm => SingleAction(domainMetadata = dm)) ++
    checkpointAction.txns.map(txn => SingleAction(txn = txn))

  /**
   * Reconstructs the live-file AddFile actions from the AMT as a [[Dataset]].
   */
  private def liveAddSingleActions(
      spark: SparkSession, deltaLog: DeltaLog): Dataset[SingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    val tableRoot = deltaLog.dataPath
    val paths = new Path(contentRoot.path) +: leaves.map(l => new Path(l.path))
    val encodedRootPath = SparkPath.fromPathString(contentRoot.path).urlEncoded
    AMTCheckpointProvider.loadEntriesWithLocation(spark, deltaLog, paths)
      .where(col("entry.content_type") === lit(AMTSingleAction.ContentType.Type.Data))
      .map { entryWithLoc =>
        entryWithLoc.entry.unwrap match {
          case data: DataEntry =>
            val backReference = if (entryWithLoc.leafPath == encodedRootPath) {
              None
            } else {
              Some(BackReference(
                SparkPath.fromUrlString(entryWithLoc.leafPath).toPath.toString, entryWithLoc.pos))
            }
            val add = data.toAddFile(tableRoot).copy(backReference = backReference)
            SingleAction(add = add)
          case other => throw new IllegalStateException(
            s"Expected a DATA entry after filtering, got ${other.getClass.getSimpleName}.")
        }
      }
  }
}

object AMTCheckpointProvider {

  /**
   * Pointer-only view of a single leaf, derived from the `DATA_MANIFEST` row.
   *
   * @param path        Path to the leaf parquet file (the root entry's `location`).
   * @param sizeInBytes On-disk size of the leaf parquet file (`file_size_in_bytes`).
   * @param numEntries  Number of content entries the leaf holds (`record_count`).
   */
  case class LeafInfo(path: String, sizeInBytes: Long, numEntries: Long)

  /**
   * An [[AMTSingleAction]] entry paired with its physical read location in its manifest parquet.
   *
   * @param entry    The manifest content entry.
   * @param leafPath The URL-encoded absolute path of the manifest parquet the entry was read from
   *                 (Spark's `_metadata.file_path`).
   * @param pos      The 0-based position of the entry inside the manifest (Spark's
   *                 `_metadata.row_index`).
   */
  case class AMTDataEntryWithLocation(entry: AMTSingleAction, leafPath: String, pos: Long)

  private lazy val amtDataEntryWithLocationEncoder: Encoder[AMTDataEntryWithLocation] =
    new DeltaEncoder[AMTDataEntryWithLocation].get

  /**
   * Builds a provider from an emitted [[Checkpoint]] action by reading the leaf pointers out of the
   * root manifest parquet.
   *
   * @param spark      Active SparkSession used to read the root parquet.
   * @param deltaLog   The table's DeltaLog, used to read the root via `loadIndex` (which bypasses
   *                   the path-based Delta format check the root file under the table root would
   *                   otherwise trip).
   * @param checkpoint The inline-emitted checkpoint action carrying the `contentRoot`.
   */
  def fromCheckpoint(
      spark: SparkSession,
      deltaLog: DeltaLog,
      checkpoint: Checkpoint): AMTCheckpointProvider = {
    // The root manifest is small (one row per leaf), so collect it to the driver to enumerate the
    // leaf pointers.
    val rootPath = new Path(checkpoint.contentRoot.path)
    val leaves = loadEntries(spark, deltaLog, Seq(rootPath)).collect().toSeq
      .filter(_.content_type == AMTSingleAction.ContentType.Type.DataManifest)
      .map(row => LeafInfo(
        path = row.location,
        sizeInBytes = row.file_size_in_bytes,
        numEntries = row.record_count))
    new AMTCheckpointProvider(checkpointAction = checkpoint, leaves = leaves)
  }

  /**
   * Reads AMT manifest parquet files (root or leaves) into a [[Dataset]] of
   * [[AMTSingleAction]].
   */
  private def loadEntries(
      spark: SparkSession, deltaLog: DeltaLog, paths: Seq[Path]): Dataset[AMTSingleAction] = {
    import org.apache.spark.sql.delta.implicits._
    val fs = paths.head.getFileSystem(deltaLog.newDeltaHadoopConf())
    val index = DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, fs, paths)
    deltaLog.loadIndex(index, spark.emptyDataset[AMTSingleAction].schema)
      .as[AMTSingleAction]
  }

  /**
   * Like [[loadEntries]], but also captures each row's physical read location.
   */
  private def loadEntriesWithLocation(
      spark: SparkSession,
      deltaLog: DeltaLog,
      paths: Seq[Path]): Dataset[AMTDataEntryWithLocation] = {
    import org.apache.spark.sql.delta.implicits._
    implicit val entryLocEncoder: Encoder[AMTDataEntryWithLocation] =
      amtDataEntryWithLocationEncoder
    val fs = paths.head.getFileSystem(deltaLog.newDeltaHadoopConf())
    val index = DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, fs, paths)
    val amtSchema = spark.emptyDataset[AMTSingleAction].schema
    deltaLog.loadIndex(index, amtSchema)
      .select(
        struct(amtSchema.fieldNames.toIndexedSeq.map(col): _*).as("entry"),
        col(s"$METADATA_NAME.$FILE_PATH").as("leafPath"),
        col(s"$METADATA_NAME.${ParquetFileFormat.ROW_INDEX}").as("pos"))
      .as[AMTDataEntryWithLocation]
  }
}
