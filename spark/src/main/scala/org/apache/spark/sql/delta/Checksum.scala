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

package org.apache.spark.sql.delta

import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets.UTF_8
import java.util.TimeZone

// scalastyle:off import.ordering.noEmptyLine
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeletedRecordCountsHistogram
import org.apache.spark.sql.delta.stats.FileSizeHistogram
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * Stats calculated within a snapshot, which we store along individual transactions for
 * verification.
 *
 * @param txnId Optional transaction identifier
 * @param tableSizeBytes The size of the table in bytes
 * @param numFiles Number of `AddFile` actions in the snapshot
 * @param numDeletedRecordsOpt  The number of deleted records with Deletion Vectors.
 * @param numDeletionVectorsOpt The number of Deletion Vectors present in the snapshot.
 * @param numMetadata Number of `Metadata` actions in the snapshot
 * @param numProtocol Number of `Protocol` actions in the snapshot
 * @param histogramOpt Optional file size histogram
 * @param deletedRecordCountsHistogramOpt A histogram of the deleted records count distribution
 *                                        for all the files in the snapshot.
 */
case class VersionChecksum(
    txnId: Option[String],
    tableSizeBytes: Long,
    numFiles: Long,
    @JsonDeserialize(contentAs = classOf[Long])
    numDeletedRecordsOpt: Option[Long],
    @JsonDeserialize(contentAs = classOf[Long])
    numDeletionVectorsOpt: Option[Long],
    numMetadata: Long,
    numProtocol: Long,
    @JsonDeserialize(contentAs = classOf[Long])
    inCommitTimestampOpt: Option[Long],
    setTransactions: Option[Seq[SetTransaction]],
    domainMetadata: Option[Seq[DomainMetadata]],
    metadata: Metadata,
    protocol: Protocol,
    histogramOpt: Option[FileSizeHistogram],
    deletedRecordCountsHistogramOpt: Option[DeletedRecordCountsHistogram],
    allFiles: Option[Seq[AddFile]])

/**
 * Record the state of the table as a checksum file along with a commit.
 */
trait RecordChecksum extends DeltaLogging {
  val deltaLog: DeltaLog
  protected def spark: SparkSession

  private lazy val writer =
    CheckpointFileManager.create(deltaLog.logPath, deltaLog.newDeltaHadoopConf())

  private def getChecksum(snapshot: Snapshot): VersionChecksum = snapshot.computeChecksum

  protected def writeChecksumFile(txnId: String, snapshot: Snapshot): Unit = {
    // scalastyle:off println
    if (!spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED)) {
      return
    }

    val version = snapshot.version
    val checksumWithoutTxnId = getChecksum(snapshot)
    val checksum = checksumWithoutTxnId.copy(txnId = Some(txnId))
    val eventData = mutable.Map[String, Any]("operationSucceeded" -> false)
    eventData("numAddFileActions") = checksum.allFiles.map(_.size).getOrElse(-1)
    eventData("numSetTransactionActions") = checksum.setTransactions.map(_.size).getOrElse(-1)
    val startTimeMs = System.currentTimeMillis()
    try {
      val toWrite = JsonUtils.toJson(checksum) + "\n"
      eventData("jsonSerializationTimeTakenMs") = System.currentTimeMillis() - startTimeMs
      eventData("checksumLength") = toWrite.length
      val stream = writer.createAtomic(
        FileNames.checksumFile(deltaLog.logPath, version),
        overwriteIfPossible = false)
      try {
        stream.write(toWrite.getBytes(UTF_8))
        stream.close()
        eventData("overallTimeTakenMs") = System.currentTimeMillis() - startTimeMs
        eventData("operationSucceeded") = true
      } catch {
        case NonFatal(e) =>
          logWarning(log"Failed to write the checksum for version: " +
            log"${MDC(DeltaLogKeys.VERSION, version)}", e)
          stream.cancel()
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to write the checksum for version: " +
          log"${MDC(DeltaLogKeys.VERSION, version)}", e)
    }
    recordDeltaEvent(
      deltaLog,
      opType = "delta.checksum.write",
      data = eventData)
  }

  /**
   * Incrementally derive checksum for the just-committed or about-to-be committed snapshot.
   * @param spark The SparkSession
   * @param deltaLog The DeltaLog
   * @param versionToCompute The version for which we want to compute the checksum
   * @param actions The actions corresponding to the version `versionToCompute`
   * @param metadataOpt The metadata corresponding to the version `versionToCompute` (if known)
   * @param protocolOpt The protocol corresponding to the version `versionToCompute` (if known)
   * @param operationName The operation name corresponding to the version `versionToCompute`
   * @param txnIdOpt The transaction identifier for the version `versionToCompute`
   * @param previousVersionState Contains either the versionChecksum corresponding to
   *                             `versionToCompute - 1` or a snapshot. Note that the snapshot may
   *                             belong to any version and this method will only use the snapshot if
   *                             it corresponds to `versionToCompute - 1`.
   * @param includeAddFilesInCrc True if the new checksum should include a [[AddFile]]s.
   * @return Either the new checksum or an error code string if the checksum could not be computed.
   */
  // scalastyle:off argcount
  def incrementallyDeriveChecksum(
      spark: SparkSession,
      deltaLog: DeltaLog,
      versionToCompute: Long,
      actions: Seq[Action],
      metadataOpt: Option[Metadata],
      protocolOpt: Option[Protocol],
      operationName: String,
      txnIdOpt: Option[String],
      previousVersionState: Either[Snapshot, VersionChecksum],
      includeAddFilesInCrc: Boolean
  ): Either[String, VersionChecksum] = {
    // scalastyle:on argcount
    if (!deltaLog.incrementalCommitEnabled) {
      return Left("INCREMENTAL_COMMITS_DISABLED")
    }

    // Do not incrementally derive checksum for ManualUpdate operations since it may
    // include actions that violate delta protocol invariants.
    if (operationName == DeltaOperations.ManualUpdate.name) {
      return Left("INVALID_OPERATION_MANUAL_UPDATE")
    }

    // Try to incrementally compute a VersionChecksum for the just-committed snapshot.
    val expectedVersion = versionToCompute - 1
    val (oldVersionChecksum, oldSnapshot) = previousVersionState match {
      case Right(checksum) => checksum -> None
      case Left(snapshot) if snapshot.version == expectedVersion =>
        // The original snapshot is still fresh so use it directly. Note this could trigger
        // a state reconstruction if there is not an existing checksumOpt in the snapshot
        // or if the existing checksumOpt contains missing information e.g.
        // a null valued metadata or protocol. However, if we do not obtain a checksum here,
        // then we cannot incrementally derive a new checksum for the new snapshot.
        logInfo(log"Incremental commit: starting with snapshot version " +
          log"${MDC(DeltaLogKeys.VERSION, expectedVersion)}")
        getChecksum(snapshot).copy(numMetadata = 1, numProtocol = 1) -> Some(snapshot)
      case _ =>
        previousVersionState.swap.foreach { snapshot =>
          // Occurs when snapshot is no longer fresh due to concurrent writers.
          // Read CRC file and validate checksum information is complete.
          recordDeltaEvent(deltaLog, opType = "delta.commit.snapshotAgedOut", data = Map(
            "snapshotVersion" -> snapshot.version,
            "commitAttemptVersion" -> versionToCompute
          ))
        }
        val oldCrcOpt = deltaLog.readChecksum(expectedVersion)
        if (oldCrcOpt.isEmpty) {
          return Left("MISSING_OLD_CRC")
        }
        val oldCrcFiltered = oldCrcOpt
          .filterNot(_.metadata == null)
          .filterNot(_.protocol == null)

        val oldCrc = oldCrcFiltered.getOrElse {
          return Left("OLD_CRC_INCOMPLETE")
        }
        oldCrc -> None
    }

    // Incrementally compute the new version checksum, if the old one is available.
    val ignoreAddFilesInOperation =
      RecordChecksum.operationNamesWhereAddFilesIgnoredForIncrementalCrc.contains(operationName)
    val ignoreRemoveFilesInOperation =
      RecordChecksum.operationNamesWhereRemoveFilesIgnoredForIncrementalCrc.contains(operationName)
    // Retrieve protocol/metadata in order of precedence:
    // 1. Use provided protocol/metadata if available
    // 2. Look for a protocol/metadata action in the incremental set of actions to be applied
    // 3. Use protocol/metadata from previous version's checksum
    // 4. Return PROTOCOL_MISSING/METADATA_MISSING error if all attempts fail
    val protocol = protocolOpt
      .orElse(actions.collectFirst { case p: Protocol => p })
      .orElse(Option(oldVersionChecksum.protocol))
      .getOrElse {
        return Left("PROTOCOL_MISSING")
      }
    val metadata = metadataOpt
      .orElse(actions.collectFirst { case m: Metadata => m })
      .orElse(Option(oldVersionChecksum.metadata))
      .getOrElse {
        return Left("METADATA_MISSING")
      }
    val persistentDVsOnTableReadable =
      DeletionVectorUtils.deletionVectorsReadable(protocol, metadata)
    val persistentDVsOnTableWritable =
      DeletionVectorUtils.deletionVectorsWritable(protocol, metadata)

    computeNewChecksum(
      versionToCompute,
      operationName,
      txnIdOpt,
      oldVersionChecksum,
      oldSnapshot,
      actions,
      ignoreAddFilesInOperation,
      ignoreRemoveFilesInOperation,
      includeAddFilesInCrc,
      persistentDVsOnTableReadable,
      persistentDVsOnTableWritable
    )
  }

  /**
   * Incrementally derive new checksum from old checksum + actions.
   *
   * @param attemptVersion commit attempt version for which we want to generate CRC.
   * @param operationName operation name for the attempted commit.
   * @param txnId transaction identifier.
   * @param oldVersionChecksum from previous commit (attemptVersion - 1).
   * @param oldSnapshot snapshot representing previous commit version (i.e. attemptVersion - 1),
   *                    None if not available.
   * @param actions used to incrementally compute new checksum.
   * @param ignoreAddFiles for transactions whose add file actions refer to already-existing files
   *                       e.g., [[DeltaOperations.ComputeStats]] transactions.
   * @param ignoreRemoveFiles for transactions that generate RemoveFiles for auxiliary files
   *                          e.g., [[DeltaOperations.AddDeletionVectorsTombstones]].
   * @param persistentDVsOnTableReadable Indicates whether commands modifying this table are allowed
   *                                      to read deletion vectors.
   * @param persistentDVsOnTableWritable Indicates whether commands modifying this table are allowed
   *                                      to create new deletion vectors.
   * @return Either the new checksum or error code string if the checksum could not be computed
   *         incrementally due to some reason.
   */
  // scalastyle:off argcount
  private[delta] def computeNewChecksum(
      attemptVersion: Long,
      operationName: String,
      txnIdOpt: Option[String],
      oldVersionChecksum: VersionChecksum,
      oldSnapshot: Option[Snapshot],
      actions: Seq[Action],
      ignoreAddFiles: Boolean,
      ignoreRemoveFiles: Boolean,
      includeAllFilesInCRC: Boolean,
      persistentDVsOnTableReadable: Boolean,
      persistentDVsOnTableWritable: Boolean
  ) : Either[String, VersionChecksum] = {
    // scalastyle:on argcount
    oldSnapshot.foreach(s => require(s.version == (attemptVersion - 1)))
    var tableSizeBytes = oldVersionChecksum.tableSizeBytes
    var numFiles = oldVersionChecksum.numFiles
    var protocol = oldVersionChecksum.protocol
    var metadata = oldVersionChecksum.metadata

    // In incremental computation, tables initialized with DVs disabled contain None DV
    // statistics. DV statistics remain None even if DVs are enabled at a random point
    // during the lifecycle of a table. That can only change if a full snapshot recomputation
    // is invoked while DVs are enabled for the table.
    val conf = spark.sessionState.conf
    val isFirstVersion = oldSnapshot.forall(_.version == -1)
    val checksumDVMetricsEnabled = conf.getConf(DeltaSQLConf.DELTA_CHECKSUM_DV_METRICS_ENABLED)
    val deletedRecordCountsHistogramEnabled =
      conf.getConf(DeltaSQLConf.DELTA_DELETED_RECORD_COUNTS_HISTOGRAM_ENABLED)

    // For tables where DVs were disabled later on in the table lifecycle we want to maintain DV
    // statistics.
    val computeDVMetricsWhenDVsNotWritable = persistentDVsOnTableReadable &&
      oldVersionChecksum.numDeletionVectorsOpt.isDefined && !isFirstVersion

    val computeDVMetrics = checksumDVMetricsEnabled &&
      (persistentDVsOnTableWritable || computeDVMetricsWhenDVsNotWritable)

    // DV-related metrics. When the old checksum does not contain DV statistics, we attempt to
    // pick them up from the old snapshot.
    var numDeletedRecordsOpt = if (computeDVMetrics) {
      oldVersionChecksum.numDeletedRecordsOpt
        .orElse(oldSnapshot.flatMap(_.numDeletedRecordsOpt))
    } else None
    var numDeletionVectorsOpt = if (computeDVMetrics) {
      oldVersionChecksum.numDeletionVectorsOpt
        .orElse(oldSnapshot.flatMap(_.numDeletionVectorsOpt))
    } else None
    val deletedRecordCountsHistogramOpt =
      if (computeDVMetrics && deletedRecordCountsHistogramEnabled) {
        oldVersionChecksum.deletedRecordCountsHistogramOpt
          .orElse(oldSnapshot.flatMap(_.deletedRecordCountsHistogramOpt))
          .map(h => DeletedRecordCountsHistogram(h.deletedRecordCounts.clone()))
      } else None

    var inCommitTimestamp : Option[Long] = None
    actions.foreach {
      case a: AddFile if !ignoreAddFiles =>
        tableSizeBytes += a.size
        numFiles += 1

        // Only accumulate DV statistics when base stats are not None.
        val (dvCount, dvCardinality) =
          Option(a.deletionVector).map(1L -> _.cardinality).getOrElse(0L -> 0L)
        numDeletedRecordsOpt = numDeletedRecordsOpt.map(_ + dvCardinality)
        numDeletionVectorsOpt = numDeletionVectorsOpt.map(_ + dvCount)
        deletedRecordCountsHistogramOpt.foreach(_.insert(dvCardinality))

      case _: RemoveFile if ignoreRemoveFiles => ()

      // extendedFileMetadata == true implies fields partitionValues, size, and tags are present
      case r: RemoveFile if r.extendedFileMetadata == Some(true) =>
        val size = r.size.get
        tableSizeBytes -= size
        numFiles -= 1

        // Only accumulate DV statistics when base stats are not None.
        val (dvCount, dvCardinality) =
          Option(r.deletionVector).map(1L -> _.cardinality).getOrElse(0L -> 0L)
        numDeletedRecordsOpt = numDeletedRecordsOpt.map(_ - dvCardinality)
        numDeletionVectorsOpt = numDeletionVectorsOpt.map(_ - dvCount)
        deletedRecordCountsHistogramOpt.foreach(_.remove(dvCardinality))

      case r: RemoveFile =>
        // Report the failure to usage logs.
        val msg = s"A remove action with a missing file size was detected in file ${r.path} " +
          "causing incremental commit to fallback to state reconstruction."
        recordDeltaEvent(
          this.deltaLog,
          "delta.checksum.compute",
          data = Map("error" -> msg))
        return Left("ENCOUNTERED_REMOVE_FILE_MISSING_SIZE")
      case p: Protocol =>
        protocol = p
      case m: Metadata =>
        metadata = m
      case ci: CommitInfo =>
        inCommitTimestamp = ci.inCommitTimestamp
      case _ =>
    }

    val setTransactions = incrementallyComputeSetTransactions(
      oldSnapshot, oldVersionChecksum, attemptVersion, actions)

    val domainMetadata = incrementallyComputeDomainMetadatas(
      oldSnapshot, oldVersionChecksum, attemptVersion, actions)

    val computeAddFiles = if (includeAllFilesInCRC) {
      incrementallyComputeAddFiles(
        oldSnapshot = oldSnapshot,
        oldVersionChecksum = oldVersionChecksum,
        attemptVersion = attemptVersion,
        numFilesAfterCommit = numFiles,
        actionsToCommit = actions)
    } else if (numFiles == 0) {
      // If the table becomes empty after the commit, addFiles should be empty.
      Option(Nil)
    } else {
      None
    }

    val allFiles = computeAddFiles.filter { files =>
      val computedNumFiles = files.size
      val computedTableSizeBytes = files.map(_.size).sum
      // Validate checksum of Incrementally computed files against the computed checksum from
      // incremental commits.
      if (computedNumFiles != numFiles || computedTableSizeBytes != tableSizeBytes) {
        val filePathsFromPreviousVersion = oldVersionChecksum.allFiles
          .orElse {
            recordFrameProfile("Delta", "VersionChecksum.computeNewChecksum.allFiles") {
              oldSnapshot.map(_.allFiles.collect().toSeq)
            }
          }
          .getOrElse(Seq.empty)
          .map(_.path)
        val addFilePathsInThisCommit = actions.collect { case af: AddFile => af.path }
        val removeFilePathsInThisCommit = actions.collect { case rf: RemoveFile => rf.path }
        logWarning(log"Incrementally computed files does not match the incremental checksum " +
          log"for commit attempt: ${MDC(DeltaLogKeys.VERSION, attemptVersion)}. " +
          log"addFilePathsInThisCommit: [${MDC(DeltaLogKeys.PATHS,
            addFilePathsInThisCommit.mkString(","))}], " +
          log"removeFilePathsInThisCommit: [${MDC(DeltaLogKeys.PATHS2,
            removeFilePathsInThisCommit.mkString(","))}], " +
          log"filePathsFromPreviousVersion: [${MDC(DeltaLogKeys.PATHS3,
            filePathsFromPreviousVersion.mkString(","))}], " +
          log"computedFiles: [${MDC(DeltaLogKeys.PATHS4,
            files.map(_.path).mkString(","))}]")
        val eventData = Map(
          "attemptVersion" -> attemptVersion,
          "expectedNumFiles" -> numFiles,
          "expectedTableSizeBytes" -> tableSizeBytes,
          "computedNumFiles" -> computedNumFiles,
          "computedTableSizeBytes" -> computedTableSizeBytes,
          "numAddFilePathsInThisCommit" -> addFilePathsInThisCommit.size,
          "numRemoveFilePathsInThisCommit" -> removeFilePathsInThisCommit.size,
          "numFilesInPreviousVersion" -> filePathsFromPreviousVersion.size,
          "operationName" -> operationName,
          "addFilePathsInThisCommit" -> JsonUtils.toJson(addFilePathsInThisCommit.take(10)),
          "removeFilePathsInThisCommit" -> JsonUtils.toJson(removeFilePathsInThisCommit.take(10)),
          "filePathsFromPreviousVersion" -> JsonUtils.toJson(filePathsFromPreviousVersion.take(10)),
          "computedFiles" -> JsonUtils.toJson(files.take(10))
        )
        recordDeltaEvent(
          deltaLog,
          opType = "delta.allFilesInCrc.checksumMismatch.aggregated",
          data = eventData)
        if (DeltaUtils.isTesting) {
          throw new IllegalStateException("Incrementally Computed State failed checksum check" +
            s" for commit $attemptVersion [$eventData]")
        }
        false
      } else {
        true
      }
    }

    Right(VersionChecksum(
      txnId = txnIdOpt,
      tableSizeBytes = tableSizeBytes,
      numFiles = numFiles,
      numDeletedRecordsOpt = numDeletedRecordsOpt,
      numDeletionVectorsOpt = numDeletionVectorsOpt,
      numMetadata = 1,
      numProtocol = 1,
      inCommitTimestampOpt = inCommitTimestamp,
      metadata = metadata,
      protocol = protocol,
      setTransactions = setTransactions,
      domainMetadata = domainMetadata,
      allFiles = allFiles,
      deletedRecordCountsHistogramOpt = deletedRecordCountsHistogramOpt,
      histogramOpt = None
    ))
  }

  /**
   * Incrementally compute [[Snapshot.setTransactions]] for the commit `attemptVersion`.
   *
   * @param oldSnapshot - snapshot corresponding to `attemptVersion` - 1
   * @param oldVersionChecksum - [[VersionChecksum]] corresponding to `attemptVersion` - 1
   * @param attemptVersion - version which we want to commit
   * @param actionsToCommit - actions for commit `attemptVersion`
   * @return Optional sequence of incrementally computed [[SetTransaction]]s for commit
   *         `attemptVersion`.
   */
  private def incrementallyComputeSetTransactions(
      oldSnapshot: Option[Snapshot],
      oldVersionChecksum: VersionChecksum,
      attemptVersion: Long,
      actionsToCommit: Seq[Action]): Option[Seq[SetTransaction]] = {
    // Check-1: check conf
    if (!spark.conf.get(DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC)) {
      return None
    }

    // Check-2: check `minSetTransactionRetentionTimestamp` is not set
    val newMetadataToCommit = actionsToCommit.collectFirst { case m: Metadata => m }
    // TODO: Add support for incrementally computing [[SetTransaction]]s even when
    //  `minSetTransactionRetentionTimestamp` is set.
    // We don't incrementally compute [[SetTransaction]]s when user has configured
    // `minSetTransactionRetentionTimestamp` as it makes verification non-deterministic.
    // Check all places to figure out whether `minSetTransactionRetentionTimestamp` is set:
    // 1. oldSnapshot corresponding to `attemptVersion - 1`
    // 2. old VersionChecksum's MetaData (corresponding to `attemptVersion-1`)
    // 3. new VersionChecksum's MetaData (corresponding to `attemptVersion`)
    val setTransactionRetentionTimestampConfigured =
      (oldSnapshot.map(_.metadata) ++ Option(oldVersionChecksum.metadata) ++ newMetadataToCommit)
        .exists(DeltaLog.minSetTransactionRetentionInterval(_).nonEmpty)
    if (setTransactionRetentionTimestampConfigured) return None

    // Check-3: Check old setTransactions are available so that we can incrementally compute new.
    val oldSetTransactions = oldVersionChecksum.setTransactions
      .getOrElse { return None }

    // Check-4: old/new setTransactions are within the threshold.
    val setTransactionsToCommit = actionsToCommit.filter(_.isInstanceOf[SetTransaction])
    val threshold = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_MAX_SET_TRANSACTIONS_IN_CRC)
    if (Math.max(setTransactionsToCommit.size, oldSetTransactions.size) > threshold) return None

    // We currently don't attempt incremental [[SetTransaction]] when
    // `minSetTransactionRetentionTimestamp` is set. So passing this as None here explicitly.
    // We can also ignore file retention because that only affects [[RemoveFile]] actions.
    val logReplay = new InMemoryLogReplay(
      minFileRetentionTimestamp = 0,
      minSetTransactionRetentionTimestamp = None)

    logReplay.append(attemptVersion - 1, oldSetTransactions.toIterator)
    logReplay.append(attemptVersion, setTransactionsToCommit.toIterator)
    Some(logReplay.getTransactions.toSeq).filter(_.size <= threshold)
  }

  /**
   * Incrementally compute [[Snapshot.domainMetadata]] for the commit `attemptVersion`.
   *
   * @param oldVersionChecksum - [[VersionChecksum]] corresponding to `attemptVersion` - 1
   * @param attemptVersion - version which we want to commit
   * @param actionsToCommit - actions for commit `attemptVersion`
   * @return Sequence of incrementally computed [[DomainMetadata]]s for commit
   *         `attemptVersion`.
   */
  private def incrementallyComputeDomainMetadatas(
      oldSnapshot: Option[Snapshot],
      oldVersionChecksum: VersionChecksum,
      attemptVersion: Long,
      actionsToCommit: Seq[Action]): Option[Seq[DomainMetadata]] = {
    // Check old DomainMetadatas are available so that we can incrementally compute new.
    val oldDomainMetadatas = oldVersionChecksum.domainMetadata
      .getOrElse { return None }
    val newDomainMetadatas = actionsToCommit.filter(_.isInstanceOf[DomainMetadata])

    // We only work with DomainMetadata, so RemoveFile and SetTransaction retention don't matter.
    val logReplay = new InMemoryLogReplay(
      minFileRetentionTimestamp = 0,
      minSetTransactionRetentionTimestamp = None)

    val threshold = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_MAX_DOMAIN_METADATAS_IN_CRC)

    logReplay.append(attemptVersion - 1, oldDomainMetadatas.iterator)
    logReplay.append(attemptVersion, newDomainMetadatas.iterator)
    // We don't truncate the set of DomainMetadata actions. Instead, we either store all of them or
    // none of them. The advantage of this is that you can then determine presence based on the
    // checksum, i.e. if the checksum contains domain metadatas but it doesn't contain the one you
    // are looking for, then it's not there.
    //
    // It's also worth noting that we can distinguish "no domain metadatas" versus
    // "domain metadatas not stored" as [[Some]] vs. [[None]].
    Some(logReplay.getDomainMetadatas.toSeq).filter(_.size <= threshold)
  }

  /**
   * Incrementally compute [[Snapshot.allFiles]] for the commit `attemptVersion`.
   *
   * @param oldSnapshot - snapshot corresponding to `attemptVersion` - 1
   * @param oldVersionChecksum - [[VersionChecksum]] corresponding to `attemptVersion` - 1
   * @param attemptVersion - version which we want to commit
   * @param numFilesAfterCommit - number of files in the table after the attemptVersion commit.
   * @param actionsToCommit - actions for commit `attemptVersion`
   * @return Optional sequence of AddFiles which represents the incrementally computed state for
   *         commit `attemptVersion`
   */
  private def incrementallyComputeAddFiles(
      oldSnapshot: Option[Snapshot],
      oldVersionChecksum: VersionChecksum,
      attemptVersion: Long,
      numFilesAfterCommit: Long,
      actionsToCommit: Seq[Action]): Option[Seq[AddFile]] = {

    // We must enumerate both the pre- and post-commit file lists; give up if they are too big.
    val incrementalAllFilesThreshold =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_THRESHOLD_FILES)
    val numFilesBeforeCommit = oldVersionChecksum.numFiles
    if (Math.max(numFilesAfterCommit, numFilesBeforeCommit) > incrementalAllFilesThreshold) {
      return None
    }

    // We try to get files for `attemptVersion - 1` from the old CRC first. If the old CRC doesn't
    // have those files, then we will try to get that info from the oldSnapshot (corresponding to
    // attemptVersion - 1). Note that oldSnapshot might not be present if another concurrent commits
    // have happened in between. In this case we return and not store incrementally computed state
    // to crc.
    val oldAllFiles = oldVersionChecksum.allFiles
      .orElse {
        recordFrameProfile("Delta", "VersionChecksum.incrementallyComputeAddFiles") {
          oldSnapshot.map(_.allFiles.collect().toSeq)
        }
      }
      .getOrElse { return None }

    val canonicalPath = new DeltaLog.CanonicalPathFunction(() => deltaLog.newDeltaHadoopConf())
    def normalizePath(action: Action): Action = action match {
      case af: AddFile => af.copy(path = canonicalPath(af.path))
      case rf: RemoveFile => rf.copy(path = canonicalPath(rf.path))
      case others => others
    }

    // We only work with AddFile, so RemoveFile and SetTransaction retention don't matter.
    val logReplay = new InMemoryLogReplay(
      minFileRetentionTimestamp = 0,
      minSetTransactionRetentionTimestamp = None)

    logReplay.append(attemptVersion - 1, oldAllFiles.map(normalizePath).toIterator)
    logReplay.append(attemptVersion, actionsToCommit.map(normalizePath).toIterator)
    Some(logReplay.allFiles)
  }
}

object RecordChecksum {
  // Operations where we should ignore AddFiles in the incremental checksum computation.
  private[delta] val operationNamesWhereAddFilesIgnoredForIncrementalCrc = Set(
    // The transaction that computes stats is special -- it re-adds files that already exist, in
    // order to update their min/max stats. We should not count those against the totals.
    DeltaOperations.ComputeStats(Seq.empty).name,
    // Backfill/Tagging re-adds existing AddFiles without changing the underlying data files.
    // Incremental commits should ignore backfill commits.
    DeltaOperations.RowTrackingBackfill().name
  )

  // Operations where we should ignore RemoveFiles in the incremental checksum computation.
  private[delta] val operationNamesWhereRemoveFilesIgnoredForIncrementalCrc = Set(
    // Deletion vector tombstones are only required to protect DVs from vacuum. They should be
    // ignored in checksum calculation.
    DeltaOperations.AddDeletionVectorsTombstones.name
  )
}

/**
 * Read checksum files.
 */
trait ReadChecksum extends DeltaLogging { self: DeltaLog =>

  val logPath: Path
  private[delta] def store: LogStore

  private[delta] def readChecksum(
      version: Long,
      checksumFileStatusHintOpt: Option[FileStatus] = None): Option[VersionChecksum] = {
    recordDeltaOperation(self, "delta.readChecksum") {
      val checksumFilePath = FileNames.checksumFile(logPath, version)
      val verifiedChecksumFileStatusOpt =
        checksumFileStatusHintOpt.filter(_.getPath == checksumFilePath)
      var exception: Option[String] = None
      val content = try Some(
        verifiedChecksumFileStatusOpt
          .map(store.read(_, newDeltaHadoopConf()))
          .getOrElse(store.read(checksumFilePath, newDeltaHadoopConf()))
      ) catch {
        case NonFatal(e) =>
          // We expect FileNotFoundException; if it's another kind of exception, we still catch them
          // here but we log them in the checksum error event below.
          if (!e.isInstanceOf[FileNotFoundException]) {
            exception = Some(Utils.exceptionString(e))
          }
          None
      }

      if (content.isEmpty) {
        // We may not find the checksum file in two cases:
        //  - We just upgraded our Spark version from an old one
        //  - Race conditions where we commit a transaction, and before we can write the checksum
        //    this reader lists the new version, and uses it to create the snapshot.
        recordDeltaEvent(
          this,
          "delta.checksum.error.missing",
          data = Map("version" -> version) ++ exception.map("exception" -> _))

        return None
      }
      val checksumData = content.get
      if (checksumData.isEmpty) {
        recordDeltaEvent(
          this,
          "delta.checksum.error.empty",
          data = Map("version" -> version))
        return None
      }
      try {
        Option(JsonUtils.mapper.readValue[VersionChecksum](checksumData.head))
      } catch {
        case NonFatal(e) =>
          recordDeltaEvent(
            this,
            "delta.checksum.error.parsing",
            data = Map("exception" -> Utils.exceptionString(e)))
          None
      }
    }
  }
}

/**
 * Verify the state of the table using the checksum information.
 */
trait ValidateChecksum extends DeltaLogging { self: Snapshot =>

  /**
   * Validate checksum (if any) by comparing it against the snapshot's state reconstruction.
   * @param contextInfo caller context that will be added to the logging if validation fails
   * @return True iff validation succeeded.
   * @throws IllegalStateException if validation failed and corruption is configured as fatal.
   */
  def validateChecksum(contextInfo: Map[String, String] = Map.empty): Boolean = {
    val contextSuffix = contextInfo.get("context").map(c => s".context-$c").getOrElse("")
    val computedStateAccessor = s"ValidateChecksum.checkMismatch$contextSuffix"
    val computedStateToCompareAgainst = computedState
    val (mismatchErrorMap, detailedErrorMapForUsageLogs) = checksumOpt
        .map(checkMismatch(_, computedStateToCompareAgainst))
        .getOrElse((Map.empty[String, String], Map.empty[String, String]))
    logAndThrowValidationFailure(mismatchErrorMap, detailedErrorMapForUsageLogs, contextInfo)
  }

  private def logAndThrowValidationFailure(
      mismatchErrorMap: Map[String, String],
      detailedErrorMapForUsageLogs: Map[String, String],
      contextInfo: Map[String, String]): Boolean = {
    if (mismatchErrorMap.isEmpty) return true
    val mismatchString = mismatchErrorMap.values.mkString("\n")

    // We get the active SparkSession, which may be different than the SparkSession of the
    // Snapshot that was created, since we cache `DeltaLog`s.
    val sparkOpt = SparkSession.getActiveSession

    // Report the failure to usage logs.
    recordDeltaEvent(
      this.deltaLog,
      "delta.checksum.invalid",
      data = Map(
        "error" -> mismatchString,
        "mismatchingFields" -> mismatchErrorMap.keys.toSeq,
        "detailedErrorMap" -> detailedErrorMapForUsageLogs,
        "v2CheckpointEnabled" ->
          CheckpointProvider.isV2CheckpointEnabled(this),
        "checkpointProviderCheckpointPolicy" ->
          checkpointProvider.checkpointPolicy.map(_.name).getOrElse("")
      ) ++ contextInfo)

    val spark = sparkOpt.getOrElse {
      throw DeltaErrors.sparkSessionNotSetException()
    }
    if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CHECKSUM_MISMATCH_IS_FATAL)) {
      throw DeltaErrors.logFailedIntegrityCheck(version, mismatchString)
    }
    false
  }

  /**
   * Validate [[Snapshot.allFiles]] against given checksum.allFiles.
   * Returns true if validation succeeds, else return false.
   * In Unit Tests, this method throws [[IllegalStateException]] so that issues can be caught during
   * development.
   */
  def validateFileListAgainstCRC(checksum: VersionChecksum, contextOpt: Option[String]): Boolean = {
    val fileSortKey = (f: AddFile) => (f.path, f.modificationTime, f.size)
    val filesFromCrc = checksum.allFiles.map(_.sortBy(fileSortKey)).getOrElse { return true }
    val filesFromStateReconstruction = recordFrameProfile("Delta", "snapshot.allFiles") {
      allFilesViaStateReconstruction.collect().toSeq.sortBy(fileSortKey)
    }
    if (filesFromCrc == filesFromStateReconstruction) return true

    val filesFromCrcWithoutStats = filesFromCrc.map(_.copy(stats = ""))
    val filesFromStateReconstructionWithoutStats =
      filesFromStateReconstruction.map(_.copy(stats = ""))
    val mismatchWithStatsOnly =
      filesFromCrcWithoutStats == filesFromStateReconstructionWithoutStats

    if (mismatchWithStatsOnly) {
      // Normalize stats in CRC as per the table schema
      val filesFromStateReconstructionMap =
        filesFromStateReconstruction.map(af => (af.path, af)).toMap
      val parser = DeltaFileProviderUtils.createJsonStatsParser(statsSchema)
      var normalizedStatsDiffer = false
      filesFromCrc.foreach { addFile =>
        val statsFromSR = filesFromStateReconstructionMap(addFile.path).stats
        val statsFromSRParsed = parser(statsFromSR)
        val statsFromCrcParsed = parser(addFile.stats)
        if (statsFromSRParsed != statsFromCrcParsed) {
          normalizedStatsDiffer = true
        }
      }
      if (!normalizedStatsDiffer) return true
    }
    // If incremental all-files-in-crc validation fails, then there is a possibility that the
    // issue is not just with incremental all-files-in-crc computation but with overall incremental
    // commits. So run the incremental commit crc validation and find out whether that is also
    // failing.
    val contextForIncrementalCommitCheck = contextOpt.map(c => s"$c.").getOrElse("") +
      "delta.allFilesInCrc.checksumMismatch.validateFileListAgainstCRC"
    var errorForIncrementalCommitCrcValidation = ""
    val incrementalCommitCrcValidationPassed = try {
      validateChecksum(Map("context" -> contextForIncrementalCommitCheck))
    } catch {
      case NonFatal(e) =>
        errorForIncrementalCommitCrcValidation += e.getMessage
        false
    }
    val eventData = Map(
      "version" -> version,
      "mismatchWithStatsOnly" -> mismatchWithStatsOnly,
      "filesCountFromCrc" -> filesFromCrc.size,
      "filesCountFromStateReconstruction" -> filesFromStateReconstruction.size,
      "filesFromCrc" -> JsonUtils.toJson(filesFromCrc),
      "incrementalCommitCrcValidationPassed" -> incrementalCommitCrcValidationPassed,
      "errorForIncrementalCommitCrcValidation" -> errorForIncrementalCommitCrcValidation,
      "context" -> contextOpt.getOrElse("")
    )
    val message = s"Incremental state reconstruction validation failed for version " +
      s"$version [${eventData.mkString(",")}]"
    logInfo(message)
    recordDeltaEvent(
      this.deltaLog,
      opType = "delta.allFilesInCrc.checksumMismatch.differentAllFiles",
      data = eventData)
    if (DeltaUtils.isTesting) throw new IllegalStateException(message)
    false
  }
  /**
   * Validates the given `checksum` against [[Snapshot.computedState]].
   * Returns an tuple of Maps:
   *  - first Map contains fieldName to user facing errorMessage mapping.
   *  - second Map is just for usage logs purpose and contains more details for different fields.
   *    Adding info to this map is optional.
   */
  private def checkMismatch(
      checksum: VersionChecksum,
      computedStateToCheckAgainst: SnapshotState
  ): (Map[String, String], Map[String, String]) = {
    var errorMap = ListMap[String, String]()
    var detailedErrorMapForUsageLogs = ListMap[String, String]()

    def compare(expected: Long, found: Long, title: String, field: String): Unit = {
      if (expected != found) {
        errorMap += (field -> s"$title - Expected: $expected Computed: $found")
      }
    }
    def compareAction(expected: Action, found: Action, title: String, field: String): Unit = {
      // only compare when expected is not null for being backward compatible to the checksum
      // without protocol and metadata
      Option(expected).filterNot(_.equals(found)).foreach { expected =>
        errorMap += (field -> s"$title - Expected: $expected Computed: $found")
      }
    }

    def compareSetTransactions(
        setTransactionsInCRC: Seq[SetTransaction],
        setTransactionsComputed: Seq[SetTransaction]): Unit = {
      val appIdsFromCrc = setTransactionsInCRC.map(_.appId)
      val repeatedEntriesForSameAppId = appIdsFromCrc.size != appIdsFromCrc.toSet.size
      val setTransactionsInCRCSet = setTransactionsInCRC.toSet
      val setTransactionsFromComputeStateSet = setTransactionsComputed.toSet
      val exactMatchFailed = setTransactionsInCRCSet != setTransactionsFromComputeStateSet
      if (repeatedEntriesForSameAppId || exactMatchFailed) {
        val repeatedAppIds = appIdsFromCrc.groupBy(identity).filter(_._2.size > 1).keySet.toSeq
        val matchedActions = setTransactionsInCRCSet.intersect(setTransactionsFromComputeStateSet)
        val unmatchedActionsInCrc = setTransactionsInCRCSet -- matchedActions
        val unmatchedActionsInComputed = setTransactionsFromComputeStateSet -- matchedActions
        val eventData = Map(
          "unmatchedSetTransactionsCRC" -> unmatchedActionsInCrc,
          "unmatchedSetTransactionsComputedState" -> unmatchedActionsInComputed,
          "version" -> version,
          "minSetTransactionRetentionTimestamp" -> minSetTransactionRetentionTimestamp,
          "repeatedEntriesForSameAppId" -> repeatedAppIds,
          "exactMatchFailed" -> exactMatchFailed)
        errorMap += ("setTransactions" -> s"SetTransaction mismatch")
        detailedErrorMapForUsageLogs += ("setTransactions" -> JsonUtils.toJson(eventData))
      }
    }

    def compareDomainMetadata(
        domainMetadataInCRC: Seq[DomainMetadata],
        domainMetadataComputed: Seq[DomainMetadata]): Unit = {
      val domainMetadataInCRCSet = domainMetadataInCRC.toSet
      // Remove any tombstones from the reconstructed set before comparison.
      val domainMetadataInComputeStateSet = domainMetadataComputed.filterNot(_.removed).toSet
      val exactMatchFailed = domainMetadataInCRCSet != domainMetadataInComputeStateSet
      if (exactMatchFailed) {
        val matchedActions = domainMetadataInCRCSet.intersect(domainMetadataInComputeStateSet)
        val unmatchedActionsInCRC = domainMetadataInCRCSet -- matchedActions
        val unmatchedActionsInComputed = domainMetadataInComputeStateSet -- matchedActions
        val eventData = Map(
          "unmatchedDomainMetadataInCRC" -> unmatchedActionsInCRC,
          "unmatchedDomainMetadataInComputedState" -> unmatchedActionsInComputed,
          "version" -> version)
        errorMap += ("domainMetadata" -> "domainMetadata mismatch")
        detailedErrorMapForUsageLogs += ("domainMetadata" -> JsonUtils.toJson(eventData))
      }
    }
    // Deletion vectors metrics.
    if (DeletionVectorUtils.deletionVectorsReadable(self)) {
      (checksum.numDeletedRecordsOpt zip computedState.numDeletedRecordsOpt).foreach {
        case (a, b) => compare(a, b, "Number of deleted records", "numDeletedRecordsOpt")
      }
      (checksum.numDeletionVectorsOpt zip computedState.numDeletionVectorsOpt).foreach {
        case (a, b) => compare(a, b, "Number of deleted vectors", "numDeletionVectorsOpt")
      }
    }

    compareAction(checksum.metadata, computedStateToCheckAgainst.metadata, "Metadata", "metadata")
    compareAction(checksum.protocol, computedStateToCheckAgainst.protocol, "Protocol", "protocol")
    compare(
      checksum.tableSizeBytes,
      computedStateToCheckAgainst.sizeInBytes,
      title = "Table size (bytes)",
      field = "tableSizeBytes")
    compare(
      checksum.numFiles,
      computedStateToCheckAgainst.numOfFiles,
      title = "Number of files",
      field = "numFiles")
    compare(
      checksum.numMetadata,
      computedStateToCheckAgainst.numOfMetadata,
      title = "Metadata updates",
      field = "numOfMetadata")
    compare(
      checksum.numProtocol,
      computedStateToCheckAgainst.numOfProtocol,
      title = "Protocol updates",
      field = "numOfProtocol")

    checksum.setTransactions.foreach { setTransactionsInCRC =>
      compareSetTransactions(setTransactionsInCRC, computedStateToCheckAgainst.setTransactions)
    }

    checksum.domainMetadata.foreach(
      compareDomainMetadata(_, computedStateToCheckAgainst.domainMetadata))

    (errorMap, detailedErrorMapForUsageLogs)
  }
}
