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

package org.apache.spark.sql.delta.icebergShaded

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.DeltaFileProviderUtils
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import shadedForDelta.org.apache.iceberg.{DataFile, ManifestFile, ManifestFiles, ManifestWriter, PartitionSpec}
import shadedForDelta.org.apache.iceberg.hadoop.HadoopFileIO

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * Serializable context bundling all dependencies needed to write Iceberg manifests on executors.
 * None of these fields reference Snapshot or SparkSession, ensuring serializability.
 */
case class ManifestWriteContext(
    tablePath: String,
    partitionSpec: PartitionSpec,
    logicalToPhysicalPartitionNames: Map[String, String],
    statsSchema: StructType,
    tableSchema: StructType,
    sessionLocalTimeZone: String,
    tableVersion: Long,
    metadataOutputLocation: String,
    formatVersion: Int,
    targetManifestSizeBytes: Long,
    hadoopConfBroadcast: Broadcast[SerializableConfiguration]
) extends Serializable

/**
 * Distributes Iceberg manifest writing across Spark executors using mapPartitions.
 * Each partition writes one or more manifest files and returns ManifestFile metadata
 * to the driver. This avoids collecting all AddFile objects to the driver and
 * accumulating DataFiles in memory.
 */
object DistributedManifestWriter extends DeltaLogging {

  /** Default target manifest file size (8 MB). */
  val DEFAULT_TARGET_MANIFEST_SIZE_BYTES: Long = 8L * 1024 * 1024

  /**
   * Write Iceberg manifest files distributed across Spark executors.
   *
   * @param spark        SparkSession
   * @param addFiles     Dataset of AddFile actions (stays distributed, never collected)
   * @param ctx          ManifestWriteContext with all serializable dependencies
   * @param numPartitions number of partitions to repartition into
   * @return Seq of ManifestFile references (small metadata, O(numManifests))
   */
  def writeManifestsDistributed(
      spark: SparkSession,
      addFiles: Dataset[AddFile],
      ctx: ManifestWriteContext,
      numPartitions: Int): Seq[ManifestFile] = {

    // Repartition to distribute work across executors, then use mapPartitions
    // to write manifests. Each partition writes one or more manifest files.
    val repartitioned = addFiles.repartition(numPartitions)

    implicit val manifestFileEncoder = Encoders.javaSerialization(classOf[ManifestFile])

    repartitioned.mapPartitions { addFileIter =>
      writeManifestsForPartition(addFileIter, ctx)
    }.collect().toSeq
  }

  /**
   * Write manifests for a single partition's AddFiles. Runs on executors.
   */
  private[delta] def writeManifestsForPartition(
      addFileIter: Iterator[AddFile],
      ctx: ManifestWriteContext): Iterator[ManifestFile] = {
    if (!addFileIter.hasNext) {
      return Iterator.empty
    }

    val hadoopConf = ctx.hadoopConfBroadcast.value.value
    val fileIO = new HadoopFileIO(hadoopConf)
    val tablePath = new Path(ctx.tablePath)

    // Recreate statsParser on executor (JsonToStructs is not serializable)
    val statsParser = DeltaFileProviderUtils.createJsonStatsParser(
      ctx.statsSchema, ctx.sessionLocalTimeZone)

    val manifests = new java.util.ArrayList[ManifestFile]()
    var writer: ManifestWriter[DataFile] = null
    var currentManifestSize: Long = 0

    try {
      addFileIter.foreach { addFile =>
        if (writer == null || currentManifestSize >= ctx.targetManifestSizeBytes) {
          // Close current writer if exists
          if (writer != null) {
            writer.close()
            manifests.add(writer.toManifestFile)
            currentManifestSize = 0
          }
          // Start a new manifest
          writer = newManifestWriter(ctx, fileIO)
        }

        val dataFile = IcebergTransactionUtils.convertDeltaAddFileToIcebergDataFile(
          addFile,
          tablePath,
          ctx.partitionSpec,
          ctx.logicalToPhysicalPartitionNames,
          statsParser,
          ctx.tableSchema,
          ctx.tableVersion,
          ctx.statsSchema)

        writer.add(dataFile)
        currentManifestSize += addFile.size
      }

      // Close the last writer
      if (writer != null) {
        writer.close()
        manifests.add(writer.toManifestFile)
      }
    } catch {
      case e: Exception =>
        // Close writer on failure; orphan manifest files will never be referenced
        if (writer != null) {
          try { writer.close() } catch { case _: Exception => }
        }
        throw e
    }

    manifests.asScala.iterator
  }

  /**
   * Create a new ManifestWriter for writing DataFile entries.
   */
  private def newManifestWriter(
      ctx: ManifestWriteContext,
      fileIO: HadoopFileIO): ManifestWriter[DataFile] = {
    val manifestPath = s"${ctx.metadataOutputLocation}/uniform-m-${UUID.randomUUID()}.avro"
    val outputFile = fileIO.newOutputFile(manifestPath)
    // snapshotId=null so Iceberg assigns at commit time
    ManifestFiles.write(ctx.formatVersion, ctx.partitionSpec, outputFile, null)
  }

  /**
   * Clean up manifest files on commit failure.
   * Best-effort: logs errors but does not throw.
   */
  def cleanupManifests(
      manifests: Seq[ManifestFile],
      hadoopConf: Configuration): Unit = {
    val fileIO = new HadoopFileIO(hadoopConf)
    manifests.foreach { manifest =>
      try {
        fileIO.deleteFile(manifest.path())
      } catch {
        case e: Exception =>
          logWarning(s"Failed to delete orphan manifest: ${manifest.path()}", e)
      }
    }
  }
}
