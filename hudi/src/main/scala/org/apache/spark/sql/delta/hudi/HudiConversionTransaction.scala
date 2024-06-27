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

package org.apache.spark.sql.delta.hudi

import org.apache.avro.Schema

import scala.util.control.NonFatal
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.hudi.HudiSchemaUtils._
import org.apache.spark.sql.delta.hudi.HudiTransactionUtils._
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hudi.avro.model.HoodieActionInstant
import org.apache.hudi.avro.model.HoodieCleanFileInfo
import org.apache.hudi.avro.model.HoodieCleanerPlan
import org.apache.hudi.client.HoodieJavaWriteClient
import org.apache.hudi.client.HoodieTimelineArchiver
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.client.common.HoodieJavaEngineContext
import org.apache.hudi.common.HoodieCleanStat
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.model.{HoodieAvroPayload, HoodieBaseFile, HoodieCleaningPolicy}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieInstantTimeGenerator, HoodieTimeline, TimelineMetadataUtils}
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.{MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH, SECS_INSTANT_ID_LENGTH, SECS_INSTANT_TIMESTAMP_FORMAT}
import org.apache.hudi.common.util.CleanerUtils
import org.apache.hudi.common.util.ExternalFilePathUtil
import org.apache.hudi.common.util.{Option => HudiOption}
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.config.HoodieArchivalConfig
import org.apache.hudi.config.HoodieCleanConfig
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY
import org.apache.hudi.table.HoodieJavaTable
import org.apache.hudi.table.action.clean.CleanPlanner

import java.io.{IOException, UncheckedIOException}
import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.{DateTimeFormatterBuilder, DateTimeParseException}
import java.time.temporal.{ChronoField, ChronoUnit}
import java.util
import java.util.stream.Collectors
import java.util.{Collections, Properties}
import collection.mutable._
import scala.collection.JavaConverters._

/**
 * Used to prepare (convert) and then commit a set of Delta actions into the Hudi table located
 * at the same path as [[postCommitSnapshot]]
 *
 *
 * @param conf Configuration for Hudi Hadoop interactions.
 * @param postCommitSnapshot Latest Delta snapshot associated with this Hudi commit.
 */
class HudiConversionTransaction(
    protected val conf: Configuration,
    protected val postCommitSnapshot: Snapshot,
    protected val providedMetaClient: HoodieTableMetaClient,
    protected val lastConvertedDeltaVersion: Option[Long] = None) extends DeltaLogging {

  //////////////////////
  // Member variables //
  //////////////////////

  private val tablePath = postCommitSnapshot.deltaLog.dataPath
  private val hudiSchema: Schema =
    convertDeltaSchemaToHudiSchema(postCommitSnapshot.metadata.schema)
  private var metaClient = providedMetaClient
  private val instantTime = convertInstantToCommit(
    Instant.ofEpochMilli(postCommitSnapshot.timestamp))
  private var writeStatuses: util.List[WriteStatus] = Collections.emptyList[WriteStatus]
  private var partitionToReplacedFileIds: util.Map[String, util.List[String]] =
    Collections.emptyMap[String, util.List[String]]

  private val version = postCommitSnapshot.version
  /** Tracks if this transaction has already committed. You can only commit once. */
  private var committed = false

  /////////////////
  // Public APIs //
  /////////////////

  def setCommitFileUpdates(actions: scala.collection.Seq[Action]): Unit = {
    // for all removed files, group by partition path and then map to
    // the file group ID (name in this case)
    partitionToReplacedFileIds = actions
      .map(_.wrap)
      .filter(action => action.remove != null)
      .map(_.remove)
      .map(remove => {
        val path = remove.toPath
        val partitionPath = getPartitionPath(tablePath, path)
        (partitionPath, path.getName)})
      .groupBy(_._1).map(v => (v._1, v._2.map(_._2).asJava))
      .asJava
    // Convert the AddFiles to write statuses for the commit
    writeStatuses = actions
      .map(_.wrap)
      .filter(action => action.add != null)
      .map(_.add)
      .map(add => {
        convertAddFile(add, tablePath, instantTime)
      })
      .asJava
  }

  def commit(): Unit = {
    assert(!committed, "Cannot commit. Transaction already committed.")
    val writeConfig = getWriteConfig(hudiSchema, getNumInstantsToRetain, 10, 7*24)
    val engineContext: HoodieEngineContext = new HoodieJavaEngineContext(metaClient.getStorageConf)
    val writeClient = new HoodieJavaWriteClient[AnyRef](engineContext, writeConfig)
    try {
      writeClient.startCommitWithTime(instantTime, HoodieTimeline.REPLACE_COMMIT_ACTION)
      metaClient.getActiveTimeline.transitionReplaceRequestedToInflight(
        new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION,
          instantTime),
        HudiOption.empty[Array[Byte]])
      val syncMetadata: Map[String, String] = Map(
        HudiConverter.DELTA_VERSION_PROPERTY -> version.toString,
        HudiConverter.DELTA_TIMESTAMP_PROPERTY -> postCommitSnapshot.timestamp.toString)
      writeClient.commit(instantTime,
        writeStatuses,
        HudiOption.of(syncMetadata.asJava),
        HoodieTimeline.REPLACE_COMMIT_ACTION,
        partitionToReplacedFileIds)
      // if the metaclient was created before the table's first commit, we need to reload it to
      // pick up the metadata table context
      if (!metaClient.getTableConfig.isMetadataTableAvailable) {
        metaClient = HoodieTableMetaClient.reload(metaClient)
      }
      val table = HoodieJavaTable.create(writeClient.getConfig, engineContext, metaClient)
      // clean up old commits and archive them
      markInstantsAsCleaned(table, writeClient.getConfig, engineContext)
      runArchiver(table, writeClient.getConfig, engineContext)
    } catch {
      case NonFatal(e) =>
        recordHudiCommit(Some(e))
        throw e
    } finally {
      if (writeClient != null) writeClient.close()
      recordHudiCommit()
    }
    committed = true
  }

  ////////////////////
  // Helper Methods //
  ////////////////////

  private def getNumInstantsToRetain = {
    val commitCutoff = convertInstantToCommit(
      parseFromInstantTime(instantTime).minus(7*24, ChronoUnit.HOURS))
    // count number of completed commits after the cutoff
    metaClient
      .getActiveTimeline
      .filterCompletedInstants
      .findInstantsAfter(commitCutoff)
      .countInstants
  }

  private def markInstantsAsCleaned(table: HoodieJavaTable[_],
      writeConfig: HoodieWriteConfig, engineContext: HoodieEngineContext): Unit = {
    val planner = new CleanPlanner(engineContext, table, writeConfig)
    val earliestInstant = planner.getEarliestCommitToRetain
    // since we're retaining based on time, we should exit early if earliestInstant is empty
    if (!earliestInstant.isPresent) return
    var partitionsToClean: util.List[String] = null
    try partitionsToClean = planner.getPartitionPathsToClean(earliestInstant)
    catch {
      case ex: IOException =>
        throw new UncheckedIOException("Unable to get partitions to clean", ex)
    }
    if (partitionsToClean.isEmpty) return
    val activeTimeline = metaClient.getActiveTimeline
    val fsView = table.getHoodieView
    val cleanInfoPerPartition = partitionsToClean.asScala.map(partition =>
        Pair.of(partition, planner.getDeletePaths(partition, earliestInstant)))
      .filter(deletePaths => !deletePaths.getValue.getValue.isEmpty)
      .map(deletePathsForPartition => deletePathsForPartition.getKey -> {
          val partition = deletePathsForPartition.getKey
          // we need to manipulate the path to properly clean from the metadata table,
          // so we map the file path to the base file
          val baseFiles = fsView.getAllReplacedFileGroups(partition)
            .flatMap(fileGroup => fileGroup.getAllBaseFiles)
            .collect(Collectors.toList[HoodieBaseFile])
          val baseFilesByPath = baseFiles.asScala
            .map(baseFile => baseFile.getPath -> baseFile).toMap
          deletePathsForPartition.getValue.getValue.asScala.map(cleanFileInfo => {
            val baseFile = baseFilesByPath.getOrElse(cleanFileInfo.getFilePath, null)
            new HoodieCleanFileInfo(ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(
                baseFile.getFileName, baseFile.getCommitTime), false)
          }).asJava
    }).toMap.asJava
    // there is nothing to clean, so exit early
    if (cleanInfoPerPartition.isEmpty) return
    // create a clean instant write after this latest commit
    val cleanTime = convertInstantToCommit(parseFromInstantTime(instantTime)
      .plus(1, ChronoUnit.SECONDS))
    // create a metadata table writer in order to mark files as deleted in the table
    // the deleted entries are cleaned up in the metadata table during compaction to control the
    // growth of the table
    val hoodieTableMetadataWriter = table.getMetadataWriter(cleanTime).get
    try {
      val earliestInstantToRetain = earliestInstant
        .map[HoodieActionInstant]((earliestInstantToRetain: HoodieInstant) =>
          new HoodieActionInstant(
            earliestInstantToRetain.getTimestamp,
            earliestInstantToRetain.getAction,
            earliestInstantToRetain.getState.name))
        .orElse(null)
      val cleanerPlan = new HoodieCleanerPlan(earliestInstantToRetain, instantTime,
        writeConfig.getCleanerPolicy.name, Collections.emptyMap[String, util.List[String]],
        CleanPlanner.LATEST_CLEAN_PLAN_VERSION, cleanInfoPerPartition,
        Collections.emptyList[String], Collections.emptyMap[String, String])
      // create a clean instant and mark it as requested with the clean plan
      val requestedCleanInstant = new HoodieInstant(HoodieInstant.State.REQUESTED,
        HoodieTimeline.CLEAN_ACTION, cleanTime)
      activeTimeline.saveToCleanRequested(
        requestedCleanInstant, TimelineMetadataUtils.serializeCleanerPlan(cleanerPlan))
      val inflightClean = activeTimeline
        .transitionCleanRequestedToInflight(requestedCleanInstant, HudiOption.empty[Array[Byte]])
      val cleanStats = cleanInfoPerPartition.entrySet.asScala.map(entry => {
        val partitionPath = entry.getKey
        val deletePaths = entry.getValue.asScala.map(_.getFilePath).asJava
        new HoodieCleanStat(HoodieCleaningPolicy.KEEP_LATEST_COMMITS, partitionPath, deletePaths,
          deletePaths, Collections.emptyList[String], earliestInstant.get.getTimestamp, instantTime)
      }).toSeq.asJava
      val cleanMetadata =
        CleanerUtils.convertCleanMetadata(cleanTime, HudiOption.empty[java.lang.Long], cleanStats,
          java.util.Collections.emptyMap[String, String])
      // update the metadata table with the clean metadata so the files' metadata are marked for
      // deletion
      hoodieTableMetadataWriter.performTableServices(HudiOption.empty[String])
      hoodieTableMetadataWriter.update(cleanMetadata, cleanTime)
      // mark the commit as complete on the table timeline
      activeTimeline.transitionCleanInflightToComplete(inflightClean,
        TimelineMetadataUtils.serializeCleanMetadata(cleanMetadata))
    } catch {
      case ex: IOException =>
        throw new UncheckedIOException("Unable to clean Hudi timeline", ex)
    } finally if (hoodieTableMetadataWriter != null) hoodieTableMetadataWriter.close()
  }

  private def runArchiver(table: HoodieJavaTable[_ <: HoodieAvroPayload],
      config: HoodieWriteConfig, engineContext: HoodieEngineContext): Unit = {
    // trigger archiver manually
    val archiver = new HoodieTimelineArchiver(config, table)
    archiver.archiveIfRequired(engineContext, true)
  }

  private def getWriteConfig(schema: Schema, numCommitsToKeep: Int,
      maxNumDeltaCommitsBeforeCompaction: Int, timelineRetentionInHours: Int) = {
    val properties = new Properties
    properties.setProperty(HoodieMetadataConfig.AUTO_INITIALIZE.key, "false")
    HoodieWriteConfig.newBuilder
      .withIndexConfig(HoodieIndexConfig.newBuilder.withIndexType(INMEMORY).build)
      .withPath(metaClient.getBasePathV2.toString)
      .withPopulateMetaFields(metaClient.getTableConfig.populateMetaFields)
      .withEmbeddedTimelineServerEnabled(false)
      .withSchema(if (schema == null) "" else schema.toString)
      .withArchivalConfig(HoodieArchivalConfig.newBuilder
        .archiveCommitsWith(Math.max(0, numCommitsToKeep - 1), Math.max(1, numCommitsToKeep))
        .withAutoArchive(false)
        .build)
      .withCleanConfig(
        HoodieCleanConfig.newBuilder
          .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS)
          .cleanerNumHoursRetained(timelineRetentionInHours)
          .withAutoClean(false)
          .build)
      .withMetadataConfig(HoodieMetadataConfig.newBuilder
        .enable(true)
        .withProperties(properties)
        .withMetadataIndexColumnStats(true)
        .withMaxNumDeltaCommitsBeforeCompaction(maxNumDeltaCommitsBeforeCompaction)
        .build)
      .build
  }

  /**
   * Copied mostly from {@link
   * org.apache.hudi.common.table.timeline.HoodieActiveTimeline#parseDateFromInstantTime(String)}
   * but forces the timestamp to use UTC unlike the Hudi code.
   *
   * @param timestamp input commit timestamp
   * @return timestamp parsed as Instant
   */
  private def parseFromInstantTime(timestamp: String): Instant = {
    try {
      var timestampInMillis: String = timestamp
      if (isSecondGranularity(timestamp)) {
        timestampInMillis = timestamp + "999"
      }
      else {
        if (timestamp.length > MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH) {
          timestampInMillis = timestamp.substring(0, MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH)
        }
      }
      val dt: LocalDateTime = LocalDateTime.parse(timestampInMillis, MILLIS_INSTANT_TIME_FORMATTER)
      dt.atZone(ZoneId.of("UTC")).toInstant
    } catch {
      case ex: DateTimeParseException =>
        throw new RuntimeException("Unable to parse date from commit timestamp: " + timestamp, ex)
    }
  }

  private def isSecondGranularity(instant: String) = instant.length == SECS_INSTANT_ID_LENGTH

  private def convertInstantToCommit(instant: Instant): String = {
    val instantTime = instant.atZone(ZoneId.of("UTC")).toLocalDateTime
    HoodieInstantTimeGenerator.getInstantFromTemporalAccessor(instantTime)
  }

  private def recordHudiCommit(errorOpt: Option[Throwable] = None): Unit = {

    val errorData = errorOpt.map { e =>
      Map(
        "exception" -> ExceptionUtils.getMessage(e),
        "stackTrace" -> ExceptionUtils.getStackTrace(e)
      )
    }.getOrElse(Map.empty)


    recordDeltaEvent(
      postCommitSnapshot.deltaLog,
      s"delta.hudi.conversion.commit.${if (errorOpt.isEmpty) "success" else "error"}",
      data = Map(
        "version" -> postCommitSnapshot.version,
        "timestamp" -> postCommitSnapshot.timestamp,
        "prevConvertedDeltaVersion" -> lastConvertedDeltaVersion
      ) ++ errorData
    )
  }

  private val MILLIS_INSTANT_TIME_FORMATTER = new DateTimeFormatterBuilder()
    .appendPattern(SECS_INSTANT_TIMESTAMP_FORMAT)
    .appendValue(ChronoField.MILLI_OF_SECOND, 3)
    .toFormatter
    .withZone(ZoneId.of("UTC"))
}
