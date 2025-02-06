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

package org.apache.spark.sql.delta.sources

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{Action, Metadata, Protocol}
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.storage.ClosableIterator
import org.apache.spark.sql.delta.storage.ClosableIterator._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.sql.types.StructType

/**
 * Helper functions for metadata evolution related handling for DeltaSource.
 * A metadata change is one of:
 * 1. Schema change
 * 2. Delta table configuration change
 * 3. Delta protocol change
 * The documentation below will use schema change as example throughout.
 *
 * To achieve schema evolution, we intercept in different stages of the normal streaming process to:
 * 1. Capture all schema changes inside a stream
 * 2. Stop the latestOffset from crossing the schema change boundary
 * 3. Ensure the batch prior to the schema change can still be served correctly
 * 4. Ensure the stream fails if and only if the prior batch is served successfully
 * 5. Write the new schema to the schema tracking log prior to stream failure, so that next time
      when it restarts we will use the updated schema.
 *
 * Specifically,
 * 1. During latestOffset calls, if we detect schema change at version V, we generate a special
 *    barrier [[DeltaSourceOffset]] X that has ver=V and index=INDEX_METADATA_CHANGE.
 *    (We first generate an [[IndexedFile]] at this index, and that gets converted into an
 *    equivalent [[DeltaSourceOffset]].)
 *    [[INDEX_METADATA_CHANGE]] comes after [[INDEX_VERSION_BASE]] (the first
 *    offset index that exists for any reservoir version) and before the offsets that represent data
 *    changes. This ensures that we apply the schema change before processing the data
 *    that uses that schema.
 * 2. When we see a schema change offset X, then this is treated as a barrier that ends the
 *    current batch. The remaining data is effectively unavailable until all the source data before
 *    the schema change has been committed.
 * 3. Then, when a [[commit]] is invoked on the offset schema change barrier offset X, we can
 *    then officially write the new schema into the schema tracking log and fail the stream.
 *    [[commit]] is only called after this batch ending at X is completed, so it would be safe to
 *    fail there.
 * 4. In between when offset X is generated and when it is committed, there could be arbitrary
 *    number of calls to [[latestOffset]], attempting to fetch new latestOffset. These calls mustn't
 *    generate new offsets until the schema change barrier offset has been committed, the new schema
 *    has been written to the schema tracking log, and the stream has been aborted and restarted.
 *    A nuance here - streaming engine won't [[commit]] until it sees a new offset that is
 *    semantically different, which is why we first generate an offset X with index
 *    INDEX_METADATA_CHANGE, but another second barrier offset X' immediately following
 *    it with index INDEX_POST_SCHEMA_CHANGE.
  *    In this way, we could ensure:
 *    a) Offset with index INDEX_METADATA_CHANGE is always committed (typically)
 *    b) Even if streaming engine changed its behavior and ONLY offset with index
 *       INDEX_POST_SCHEMA_CHANGE is committed, we can still see this is a
 *       schema change barrier with a schema change ready to be evolved.
 *    c) Whenever [[latestOffset]] sees a startOffset with a schema change barrier index, we can
 *       easily tell that we should not progress past the schema change, unless the schema change
 *       has actually happened.
 * When a stream is restarted post a schema evolution (not initialization), it is guaranteed to have
 * >= 2 entries in the schema log. To prevent users from shooting themselves in the foot while
 * blindly restart stream without considering implications to downstream tables, by default we would
 * not allow stream to restart without a magic SQL conf that user has to set to allow non-additive
 * schema changes to propagate. We detect such non-additive schema changes during stream start by
 * comparing the last schema log entry with the current one.
 */
trait DeltaSourceMetadataEvolutionSupport extends DeltaSourceBase { base: DeltaSource =>

  /**
   * Whether this DeltaSource is utilizing a schema log entry as its read schema.
   *
   * If user explicitly turn on the flag to fall back to using latest schema to read (i.e. the
   * legacy mode), we will ignore the schema log.
   */
  protected def trackingMetadataChange: Boolean =
    !allowUnsafeStreamingReadOnColumnMappingSchemaChanges &&
      metadataTrackingLog.flatMap(_.getCurrentTrackedMetadata).nonEmpty

  /**
   * Whether a schema tracking log is provided (and is empty), so we could initialize eagerly.
   * This should only be used for the first write to the schema log, after then, schema tracking
   * should not rely on this state any more.
   */
  protected def readyToInitializeMetadataTrackingEagerly: Boolean =
    !allowUnsafeStreamingReadOnColumnMappingSchemaChanges &&
      metadataTrackingLog.exists { log =>
        log.getCurrentTrackedMetadata.isEmpty && log.initMetadataLogEagerly
      }


  /**
   * This is called from getFileChangesWithRateLimit() during latestOffset().
   */
  protected def stopIndexedFileIteratorAtSchemaChangeBarrier(
      fileActionScanIter: ClosableIterator[IndexedFile]): ClosableIterator[IndexedFile] = {
    fileActionScanIter.withClose { iter =>
      val (untilSchemaChange, fromSchemaChange) = iter.span { i =>
        i.index != DeltaSourceOffset.METADATA_CHANGE_INDEX
      }
      // This will end at the schema change indexed file (inclusively)
      // If there are no schema changes, this is an no-op.
      untilSchemaChange ++ fromSchemaChange.take(1)
    }
  }

  /**
   * Check the table metadata or protocol changed since the initial read snapshot. We make sure:
   * 1. The schema is the same, except for internal metadata, AND
   * 2. The delta related table configurations are strictly equal, AND
   * 3. The incoming metadata change should not be considered a failure-causing change if we have
   *    marked the persisted schema and the stream progress is behind that schema version.
   *    This could happen when we've already merged consecutive schema changes during the analysis
   *    phase and we are using the merged schema as the read schema. All the schema changes in
   *    between can be safely ignored because they won't contribute any data.
   */
  private def hasMetadataOrProtocolChangeComparedToStreamMetadata(
      metadataChangeOpt: Option[Metadata],
      protocolChangeOpt: Option[Protocol],
      newSchemaVersion: Long): Boolean = {
    if (persistedMetadataAtSourceInit.exists(_.deltaCommitVersion >= newSchemaVersion)) {
      false
    } else {
      protocolChangeOpt.exists(_ != readProtocolAtSourceInit) ||
      metadataChangeOpt.exists { newMetadata =>
         hasSchemaChangeComparedToStreamMetadata(newMetadata.schema) ||
           newMetadata.partitionSchema != readPartitionSchemaAtSourceInit ||
           newMetadata.configuration.filterKeys(_.startsWith("delta.")).toMap !=
             readConfigurationsAtSourceInit.filterKeys(_.startsWith("delta.")).toMap
      }
    }
  }

  /**
   * Check that the give schema is the same as the schema from the initial read snapshot.
   */
  private def hasSchemaChangeComparedToStreamMetadata(newSchema: StructType): Boolean =
    if (spark.conf.get(DeltaSQLConf.DELTA_STREAMING_IGNORE_INTERNAL_METADATA_FOR_SCHEMA_CHANGE)) {
      DeltaTableUtils.removeInternalWriterMetadata(spark, newSchema) !=
        DeltaTableUtils.removeInternalWriterMetadata(spark, readSchemaAtSourceInit)
    } else {
      newSchema != readSchemaAtSourceInit
    }

  /**
   * If the current stream metadata is not equal to the metadata change in [[metadataChangeOpt]],
   * return a metadata change barrier [[IndexedFile]].
   * Only returns something if [[trackingMetadataChange]]is true.
   */
  protected def getMetadataOrProtocolChangeIndexedFileIterator(
      metadataChangeOpt: Option[Metadata],
      protocolChangeOpt: Option[Protocol],
      version: Long): ClosableIterator[IndexedFile] = {
    if (trackingMetadataChange && hasMetadataOrProtocolChangeComparedToStreamMetadata(
        metadataChangeOpt, protocolChangeOpt, version)) {
      // Create an IndexedFile with metadata change
      Iterator.single(IndexedFile(version, DeltaSourceOffset.METADATA_CHANGE_INDEX, null))
        .toClosable
    } else {
      Iterator.empty.toClosable
    }
  }

  /**
   * Collect all actions between start and end version, both inclusive
   */
  private def collectActions(
      startVersion: Long,
      endVersion: Long
  ): ClosableIterator[(Long, Action)] = {
    deltaLog.getChangeLogFiles(startVersion, options.failOnDataLoss).takeWhile {
      case (version, _) => version <= endVersion
    }.flatMapWithClose { case (version, fileStatus) =>
      DeltaSource.createRewindableActionIterator(spark, deltaLog, fileStatus)
        .map((version, _))
        .toClosable
    }
  }

  /**
   * Given the version range for an ALREADY fetched batch, check if there are any
   * read-incompatible schema changes or protocol changes.
   * In this case, the streaming engine wants to getBatch(X,Y) on an existing Y that is already
   * loaded and saved in the offset log in the past before requesting new offsets. Therefore we
   * should verify if we could find a schema or protocol that is safe to read this constructed batch
   * , which then can be used to initialize the metadata log.
   * If not, there's not much we could do, even with metadata log, because unlike finding new
   * offsets, we don't have a chance to "split" this batch at schema change boundaries any more. The
   * streaming engine is not able to change the ranges of a batch after it has created it.
   * If there are no non-additive schema changes, or incompatible protocol changes, it is safe to
   * mark the metadata and protocol safe to read for all data files between startVersion and
   * endVersion.
   */
  private def validateAndResolveMetadataForLogInitialization(
      startVersion: Long, endVersion: Long): (Metadata, Protocol) = {
    val metadataChanges = collectMetadataActions(startVersion, endVersion).map(_._2)
    val startSnapshot = getSnapshotFromDeltaLog(startVersion)
    val startMetadata = startSnapshot.metadata

    // Try to find rename or drop columns in between, or nullability/datatype changes by using
    // the last schema as the read schema and if so we cannot find a good read schema.
    // Otherwise, the most recent metadata change will be the most encompassing schema as well.
    val mostRecentMetadataChangeOpt = metadataChanges.lastOption
    mostRecentMetadataChangeOpt.foreach { mostRecentMetadataChange =>
      val otherMetadataChanges = Seq(startMetadata) ++ metadataChanges.dropRight(1)
      otherMetadataChanges.foreach { potentialSchemaChangeMetadata =>
        if (!DeltaColumnMapping.hasNoColumnMappingSchemaChanges(
          newMetadata = mostRecentMetadataChange,
          oldMetadata = potentialSchemaChangeMetadata) ||
          !SchemaUtils.isReadCompatible(
            existingSchema = potentialSchemaChangeMetadata.schema,
            readSchema = mostRecentMetadataChange.schema,
            forbidTightenNullability = true)) {
          throw DeltaErrors.streamingMetadataLogInitFailedIncompatibleMetadataException(
            startVersion, endVersion)
        }
      }
    }

    // Check protocol changes and use the most supportive protocol
    val startProtocol = startSnapshot.protocol
    val protocolChanges = collectProtocolActions(startVersion, endVersion).map(_._2)

    var mostSupportiveProtocol = startProtocol
    protocolChanges.foreach { p =>
      if (mostSupportiveProtocol.readerAndWriterFeatureNames
          .subsetOf(p.readerAndWriterFeatureNames)) {
        mostSupportiveProtocol = p
      } else {
        // TODO: or use protocol union instead?
        throw DeltaErrors.streamingMetadataLogInitFailedIncompatibleMetadataException(
          startVersion, endVersion)
      }
    }

    (mostRecentMetadataChangeOpt.getOrElse(startMetadata), mostSupportiveProtocol)
  }

  /**
   * Collect a metadata action at the commit version if possible.
   */
  private def collectMetadataAtVersion(version: Long): Option[Metadata] = {
    collectActions(version, version).processAndClose { iter =>
      iter.map(_._2).collectFirst {
        case a: Metadata => a
      }
    }
  }

  protected def collectMetadataActions(
      startVersion: Long,
      endVersion: Long): Seq[(Long, Metadata)] = {
    collectActions(startVersion, endVersion).processAndClose { iter =>
      iter.collect {
        case (version, a: Metadata) => (version, a)
      }.toSeq
    }
  }

  /**
   * Collect a protocol action at the commit version if possible.
   */
  private def collectProtocolAtVersion(version: Long): Option[Protocol] = {
    collectActions(version, version).processAndClose { iter =>
      iter.map(_._2).collectFirst {
        case a: Protocol => a
      }
    }
  }

  protected def collectProtocolActions(
      startVersion: Long,
      endVersion: Long): Seq[(Long, Protocol)] = {
    collectActions(startVersion, endVersion).processAndClose { iter =>
      iter.collect {
        case (version, a: Protocol) => (version, a)
      }.toSeq
    }
  }


  /**
   * If the given previous Delta source offset is a schema change offset, returns the appropriate
   * next offset. This should be called before trying any other means of determining the next
   * offset.
   * If this returns None, then there is no schema change, and the caller should determine the next
   * offset in the normal way.
   */
  protected def getNextOffsetFromPreviousOffsetIfPendingSchemaChange(
      previousOffset: DeltaSourceOffset): Option[DeltaSourceOffset] = {
    // Check if we've generated a previous offset with schema change (i.e. offset X in class doc)
    // Then, we will generate offset X' as mentioned in the class doc.
    if (previousOffset.index == DeltaSourceOffset.METADATA_CHANGE_INDEX) {
      return Some(previousOffset.copy(index = DeltaSourceOffset.POST_METADATA_CHANGE_INDEX))
    }
    // If the previous offset is already POST the schema change and schema evolution has not
    // occurred, simply block as no-op.
    if (previousOffset.index == DeltaSourceOffset.POST_METADATA_CHANGE_INDEX &&
      hasMetadataOrProtocolChangeComparedToStreamMetadata(
        collectMetadataAtVersion(previousOffset.reservoirVersion),
        collectProtocolAtVersion(previousOffset.reservoirVersion),
        previousOffset.reservoirVersion)) {
      return Some(previousOffset)
    }

    // Otherwise, no special handling
    None
  }

  /**
   * Initialize the schema tracking log if an empty schema tracking log is provided.
   * This method also checks the range between batchStartVersion and batchEndVersion to ensure we
   * a safe schema to be initialized in the log.
   * @param batchStartVersion Start version of the batch of data to be proceed, it should typically
   *                          be the schema that is safe to process incoming data.
   * @param batchEndVersionOpt Optionally, if we are looking at a constructed batch with existing
   *                           end offset, we need to double verify to ensure no read-incompatible
   *                           within the batch range.
   * @param alwaysFailUponLogInitialized Whether we should always fail with the schema evolution
   *                                     exception.
   */
  protected def initializeMetadataTrackingAndExitStream(
      batchStartVersion: Long,
      batchEndVersionOpt: Option[Long] = None,
      alwaysFailUponLogInitialized: Boolean = false): Unit = {
    // If possible, initialize the metadata log with the desired start metadata instead of failing.
    // If a `batchEndVersion` is provided, we also need to verify if there are no incompatible
    // schema changes in a constructed batch, if so, we cannot find a proper schema to init the
    // schema log.
    val (version, metadata, protocol) = batchEndVersionOpt.map { endVersion =>
      val (validMetadata, validProtocol) =
        validateAndResolveMetadataForLogInitialization(batchStartVersion, endVersion)
      // `endVersion` should be valid for initialization
      (endVersion, validMetadata, validProtocol)
    }.getOrElse {
      val startSnapshot = getSnapshotFromDeltaLog(batchStartVersion)
      (startSnapshot.version, startSnapshot.metadata, startSnapshot.protocol)
    }

    val newMetadata = PersistedMetadata(tableId, version, metadata, protocol, metadataPath)
    // Always initialize the metadata log
    metadataTrackingLog.get.writeNewMetadata(newMetadata)
    if (hasMetadataOrProtocolChangeComparedToStreamMetadata(
        Some(metadata), Some(protocol), version) || alwaysFailUponLogInitialized) {
      // But trigger evolution exception when there's a difference
      throw DeltaErrors.streamingMetadataEvolutionException(
        newMetadata.dataSchema,
        newMetadata.tableConfigurations.get,
        newMetadata.protocol.get
      )
    }
  }

  /**
   * Update the current stream schema in the schema tracking log and fail the stream.
   * This is called during commit().
   * It's ok to fail during commit() because in streaming's semantics, the batch with offset ending
   * at `end` should've already being processed completely.
   */
  protected def updateMetadataTrackingLogAndFailTheStreamIfNeeded(end: Offset): Unit = {
    val offset = DeltaSourceOffset(tableId, end)
    if (trackingMetadataChange &&
      (offset.index == DeltaSourceOffset.METADATA_CHANGE_INDEX ||
        offset.index == DeltaSourceOffset.POST_METADATA_CHANGE_INDEX)) {
      // The offset must point to a metadata or protocol change action
      val changedMetadataOpt = collectMetadataAtVersion(offset.reservoirVersion)
      val changedProtocolOpt = collectProtocolAtVersion(offset.reservoirVersion)

      // Evolve the schema when the schema is indeed different from the current stream schema. We
      // need to check this because we could potentially generate two offsets before schema
      // evolution each with different indices.
      // Typically streaming engine will commit the first one and evolve the schema log, however,
      // to be absolutely safe, we also consider the case when the first is skipped and only the
      // second one is committed.
      // If the first one is committed (typically), the stream will fail and restart with the
      // evolved schema, then we should NOT fail/evolve again when we commit the second offset.
      updateMetadataTrackingLogAndFailTheStreamIfNeeded(
        changedMetadataOpt, changedProtocolOpt, offset.reservoirVersion)
    }
  }

  /**
   * Write a new potentially changed metadata into the metadata tracking log. Then fail the stream
   * to allow reanalysis if there are changes.
   * @param changedMetadataOpt Potentially changed metadata action
   * @param changedProtocolOpt Potentially changed protocol action
   * @param version The version of change
   */
  protected def updateMetadataTrackingLogAndFailTheStreamIfNeeded(
      changedMetadataOpt: Option[Metadata],
      changedProtocolOpt: Option[Protocol],
      version: Long,
      replace: Boolean = false): Unit = {
    if (hasMetadataOrProtocolChangeComparedToStreamMetadata(
        changedMetadataOpt, changedProtocolOpt, version)) {

      val schemaToPersist = PersistedMetadata(
        deltaLog.tableId,
        version,
        changedMetadataOpt.getOrElse(readSnapshotDescriptor.metadata),
        changedProtocolOpt.getOrElse(readSnapshotDescriptor.protocol),
        metadataPath
      )
      // Update schema log
      if (replace) {
        metadataTrackingLog.get.writeNewMetadata(schemaToPersist, replaceCurrent = true)
      } else {
        metadataTrackingLog.get.writeNewMetadata(schemaToPersist)
      }
      // Fail the stream with schema evolution exception
      throw DeltaErrors.streamingMetadataEvolutionException(
        schemaToPersist.dataSchema,
        schemaToPersist.tableConfigurations.get,
        schemaToPersist.protocol.get
      )
    }
  }
}

object DeltaSourceMetadataEvolutionSupport {
  /** SQL configs that allow unblocking each type of schema changes. */
  private val SQL_CONF_PREFIX = s"${DeltaSQLConf.SQL_CONF_PREFIX}.streaming"

  private final val SQL_CONF_UNBLOCK_RENAME_DROP =
    SQL_CONF_PREFIX + ".allowSourceColumnRenameAndDrop"
  private final val SQL_CONF_UNBLOCK_RENAME = SQL_CONF_PREFIX + ".allowSourceColumnRename"
  private final val SQL_CONF_UNBLOCK_DROP = SQL_CONF_PREFIX + ".allowSourceColumnDrop"
  private final val SQL_CONF_UNBLOCK_TYPE_CHANGE = SQL_CONF_PREFIX + ".allowSourceColumnTypeChange"

  /**
   * Defining the different combinations of non-additive schema changes to detect them and allow
   * users to vet and unblock them using a corresponding SQL conf:
   * - dropping columns
   * - renaming columns
   * - widening data types
   */
  private sealed trait SchemaChangeType {
    val name: String
    val isRename: Boolean
    val isDrop: Boolean
    val isTypeWidening: Boolean
    val sqlConfsUnblock: Seq[String]
    val readerOptionsUnblock: Seq[String]
  }

  // Single types of schema change, typically caused by a single ALTER TABLE operation.
  private case object SchemaChangeRename extends SchemaChangeType {
    override val name = "RENAME COLUMN"
    override val (isRename, isDrop, isTypeWidening) = (true, false, false)
    override val sqlConfsUnblock: Seq[String] = Seq(SQL_CONF_UNBLOCK_RENAME)
    override val readerOptionsUnblock: Seq[String] = Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_RENAME)
  }
  private case object SchemaChangeDrop extends SchemaChangeType {
    override val name = "DROP COLUMN"
    override val (isRename, isDrop, isTypeWidening) = (false, true, false)
    override val sqlConfsUnblock: Seq[String] = Seq(SQL_CONF_UNBLOCK_DROP)
    override val readerOptionsUnblock: Seq[String] = Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_DROP)
  }
  private case object SchemaChangeTypeWidening extends SchemaChangeType {
    override val name = "TYPE WIDENING"
    override val (isRename, isDrop, isTypeWidening) = (false, false, true)
    override val sqlConfsUnblock: Seq[String] = Seq(SQL_CONF_UNBLOCK_TYPE_CHANGE)
    override val readerOptionsUnblock: Seq[String] =
      Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_TYPE_CHANGE)
  }

  // Combinations of rename, drop and type change -> can be caused by a complete overwrite.
  private case object SchemaChangeRenameAndDrop extends SchemaChangeType {
    override val name = "RENAME AND DROP COLUMN"
    override val (isRename, isDrop, isTypeWidening) = (true, true, false)
    override val sqlConfsUnblock: Seq[String] = Seq(SQL_CONF_UNBLOCK_RENAME_DROP)
    override val readerOptionsUnblock: Seq[String] =
      Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_RENAME, DeltaOptions.ALLOW_SOURCE_COLUMN_DROP)
  }
  private case object SchemaChangeRenameAndTypeWidening extends SchemaChangeType {
    override val name = "RENAME AND TYPE WIDENING"
    override val (isRename, isDrop, isTypeWidening) = (true, false, true)
    override val sqlConfsUnblock: Seq[String] =
      Seq(SQL_CONF_UNBLOCK_RENAME, SQL_CONF_UNBLOCK_TYPE_CHANGE)
    override val readerOptionsUnblock: Seq[String] =
      Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_RENAME, DeltaOptions.ALLOW_SOURCE_COLUMN_DROP)
  }
  private case object SchemaChangeDropAndTypeWidening extends SchemaChangeType {
    override val name = "DROP AND TYPE WIDENING"
    override val (isRename, isDrop, isTypeWidening) = (false, true, true)
    override val sqlConfsUnblock: Seq[String] =
      Seq(SQL_CONF_UNBLOCK_DROP, SQL_CONF_UNBLOCK_TYPE_CHANGE)
    override val readerOptionsUnblock: Seq[String] =
      Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_DROP, DeltaOptions.ALLOW_SOURCE_COLUMN_TYPE_CHANGE)
  }
  private case object SchemaChangeRenameAndDropAndTypeWidening extends SchemaChangeType {
    override val name = "RENAME, DROP AND TYPE WIDENING"
    override val (isRename, isDrop, isTypeWidening) = (true, true, true)
    override val sqlConfsUnblock: Seq[String] =
      Seq(SQL_CONF_UNBLOCK_RENAME_DROP, SQL_CONF_UNBLOCK_TYPE_CHANGE)
    override val readerOptionsUnblock: Seq[String] =
      Seq(DeltaOptions.ALLOW_SOURCE_COLUMN_DROP, DeltaOptions.ALLOW_SOURCE_COLUMN_TYPE_CHANGE)
  }

  private final val allSchemaChangeTypes = Seq(
    SchemaChangeDrop,
    SchemaChangeRename,
    SchemaChangeTypeWidening,
    SchemaChangeRenameAndDrop,
    SchemaChangeRenameAndTypeWidening,
    SchemaChangeDropAndTypeWidening,
    SchemaChangeRenameAndDropAndTypeWidening
  )

  /**
   * Determine the non-additive schema change type for an incoming schema change. None if it's
   * additive.
   */
  private def determineNonAdditiveSchemaChangeType(
      spark: SparkSession,
      newSchema: StructType, oldSchema: StructType): Option[SchemaChangeType] = {
    val isRenameColumn = DeltaColumnMapping.isRenameColumnOperation(newSchema, oldSchema)
    val isDropColumn = DeltaColumnMapping.isDropColumnOperation(newSchema, oldSchema)
    // Use physical column names to identify type changes. Dropping a column and adding a new column
    // with a different type is historically allowed and is not considered a type change.
    val oldPhysicalSchema = DeltaColumnMapping.renameColumns(oldSchema)
    val newPhysicalSchema = DeltaColumnMapping.renameColumns(newSchema)
    // Check if there are widening type changes. This assumes [[checkIncompatibleSchemaChange]] was
    // already called before and failed if there were any non-widening type changes. The type change
    // checks - both widening and non-widening - can be disabled by flag to revert to historical
    // behavior where type changes are not considered a non-additive schema change and are allowed
    // to propagate without user action.
    val isTypeWidening = allowTypeWidening(spark) && !bypassTypeChangeCheck(spark) &&
      TypeWidening.containsWideningTypeChanges(from = oldPhysicalSchema, to = newPhysicalSchema)
    allSchemaChangeTypes.find { c =>
      (c.isDrop, c.isRename, c.isTypeWidening) == (isDropColumn, isRenameColumn, isTypeWidening)
    }
  }

  /**
   * Returns whether the given type of non-additive schema change was unblocked by setting one of
   * the corresponding SQL confs.
   */
  private def isChangeUnblocked(
      spark: SparkSession,
      change: SchemaChangeType,
      options: DeltaOptions,
      checkpointHash: Int,
      schemaChangeVersion: Long): Boolean = {

    def isUnblockedBySQLConf(sqlConf: String): Boolean = {
      def getConf(key: String): Option[String] =
        Option(spark.sessionState.conf.getConfString(key, null))
          .map(_.toLowerCase(Locale.ROOT))
      val validConfKeysValuePair = Seq(
        (sqlConf, "always"),
        (s"$sqlConf.ckpt_$checkpointHash", "always"),
        (s"$sqlConf.ckpt_$checkpointHash", schemaChangeVersion.toString)
      )
      validConfKeysValuePair.exists(p => getConf(p._1).contains(p._2))
    }

    val isBlockedRename = change.isRename && !options.allowSourceColumnRename &&
      !isUnblockedBySQLConf(SQL_CONF_UNBLOCK_RENAME) &&
      !isUnblockedBySQLConf(SQL_CONF_UNBLOCK_RENAME_DROP)
    val isBlockedDrop = change.isDrop && !options.allowSourceColumnDrop &&
      !isUnblockedBySQLConf(SQL_CONF_UNBLOCK_DROP) &&
      !isUnblockedBySQLConf(SQL_CONF_UNBLOCK_RENAME_DROP)
    val isBlockedTypeChange = change.isTypeWidening && !options.allowSourceColumnTypeChange &&
      !isUnblockedBySQLConf(SQL_CONF_UNBLOCK_TYPE_CHANGE)

    !isBlockedRename && !isBlockedDrop && !isBlockedTypeChange
  }

  def getCheckpointHash(path: String): Int = path.hashCode

  /**
   * Whether to accept widening type changes:
   *   - when true, widening type changes cause the stream to fail, requesting user to review and
   *     unblock them via a SQL conf.
   *   - when false, widening type changes are rejected without possibility to unblock, similar to
   *     any other arbitrary type change.
   */
  def allowTypeWidening(spark: SparkSession): Boolean = {
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_ALLOW_TYPE_WIDENING_STREAMING_SOURCE)
  }

  /**
   * We historically allowed any type changes to go through when schema tracking was enabled. This
   * config allows reverting to that behavior.
   */
  def bypassTypeChangeCheck(spark: SparkSession): Boolean =
    spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_TYPE_WIDENING_BYPASS_STREAMING_TYPE_CHANGE_CHECK)

  // scalastyle:off
  /**
   * Given a non-additive operation type from a previous schema evolution, check we can process
   * using the new schema given any SQL conf users have explicitly set to unblock.
   * The SQL conf can take one of following formats:
   * 1. spark.databricks.delta.streaming.allowSourceColumn$action = "always"
   *    -> allows non-additive schema change to propagate for all streams.
   * 2. spark.databricks.delta.streaming.allowSourceColumn$action.$checkpointHash = "always"
   *    -> allows non-additive schema change to propagate for this particular stream.
   * 3. spark.databricks.delta.streaming.allowSourceColumn$action.$checkpointHash = $deltaVersion
   *     -> allow non-additive schema change to propagate only for this particular stream source
   *        table version.
   * where `allowSourceColumn$action` is one of:
   * 1. `allowSourceColumnRename` to allow column renames.
   * 2. `allowSourceColumnDrop` to allow column drops.
   * 3. `allowSourceColumnRenameAndDrop` to allow both column drops and renames.
   * 4. `allowSourceColumnTypeChange` to allow widening type changes.
   *
   * We will check for any of these configs given the non-additive operation, and throw a proper
   * error message to instruct the user to set the SQL conf if they would like to unblock.
   *
   * @param metadataPath The path to the source-unique metadata location under checkpoint
   * @param currentSchema The current persisted schema
   * @param previousSchema The previous persisted schema
   */
  // scalastyle:on
  protected[sources] def validateIfSchemaChangeCanBeUnblockedWithSQLConf(
      spark: SparkSession,
      parameters: Map[String, String],
      metadataPath: String,
      currentSchema: PersistedMetadata,
      previousSchema: PersistedMetadata): Unit = {
    val options = new DeltaOptions(parameters, spark.sessionState.conf)
    val checkpointHash = getCheckpointHash(metadataPath)

    // The start version of a possible series of consecutive schema changes.
    val previousSchemaChangeVersion = previousSchema.deltaCommitVersion
    // The end version of a possible series of consecutive schema changes.
    val currentSchemaChangeVersion = currentSchema.deltaCommitVersion

    // Fail with a non-retryable exception if there are any type changes that we don't allow
    // unblocking, i.e. non-widening type changes. We do allow changes caused by columns being
    // dropped/renamed, e.g. dropping a column and adding it back with a different type. These were
    // historically allowed and will be surfaced to the user as column drop/rename.
    checkIncompatibleSchemaChange(
      spark,
      previousSchema = previousSchema.dataSchema,
      currentSchema = currentSchema.dataSchema,
      currentSchemaChangeVersion
    )

    determineNonAdditiveSchemaChangeType(
      spark, currentSchema.dataSchema, previousSchema.dataSchema).foreach { change =>
        if (!isChangeUnblocked(
            spark, change, options, checkpointHash, currentSchemaChangeVersion)) {
          // Throw error to prompt user to set the correct confs
          change match {
            case SchemaChangeTypeWidening =>
              val wideningTypeChanges = TypeWideningMetadata.collectTypeChanges(
                from = previousSchema.dataSchema,
                to = currentSchema.dataSchema
              )
              throw DeltaErrors.cannotContinueStreamingTypeWidening(
                previousSchemaChangeVersion,
                currentSchemaChangeVersion,
                checkpointHash,
                change.readerOptionsUnblock,
                change.sqlConfsUnblock,
                wideningTypeChanges)

            case _ =>
              throw DeltaErrors.cannotContinueStreamingPostSchemaEvolution(
                change.name,
                previousSchemaChangeVersion,
                currentSchemaChangeVersion,
                checkpointHash,
                change.readerOptionsUnblock,
                change.sqlConfsUnblock)
          }
        }
    }
  }

  /**
   * Checks that the new schema only contains column rename/drop and widening type changes compared
   * to the previous schema. That is, rejects any non-widening type changes.
   */
  private def checkIncompatibleSchemaChange(
      spark: SparkSession,
      previousSchema: StructType,
      currentSchema: StructType,
      currentSchemaChangeVersion: Long): Unit = {
    if (bypassTypeChangeCheck(spark)) return

    val incompatibleSchema =
      !SchemaUtils.isReadCompatible(
        // We want to ignore renamed/dropped columns here and let the check for non-additive
        // schema changes handle them: we only check if an actual physical column had an
        // incompatible type change.
        existingSchema = DeltaColumnMapping.renameColumns(previousSchema),
        readSchema = DeltaColumnMapping.renameColumns(currentSchema),
        forbidTightenNullability = true,
        allowMissingColumns = true,
        typeWideningMode =
          if (allowTypeWidening(spark)) TypeWideningMode.AllTypeWidening
          else TypeWideningMode.NoTypeWidening
      )
    if (incompatibleSchema) {
      throw DeltaErrors.schemaChangedException(
        previousSchema,
        currentSchema,
        retryable = false,
        Some(currentSchemaChangeVersion),
        includeStartingVersionOrTimestampMessage = false)
    }
  }
}
