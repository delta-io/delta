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
import org.apache.spark.sql.delta.actions.{Action, Metadata}
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.storage.ClosableIterator
import org.apache.spark.sql.delta.storage.ClosableIterator._

import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.sql.types.StructType

/**
 * Helper functions for schema evolution related handling for DeltaSource.
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
 *    barrier [[DeltaSourceOffset]] X that has ver=V and index=INDEX_SCHEMA_CHANGE.
 *    (We first generate an [[IndexedFile]] at this index, and that gets converted into an
 *    equivalent [[DeltaSourceOffset]].)
 *    [[INDEX_SCHEMA_CHANGE]] comes after [[INDEX_VERSION_BASE]] (the first
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
 *    INDEX_SCHEMA_CHANGE, but another second barrier offset X' immediately following
 *    it with index INDEX_POST_SCHEMA_CHANGE.
  *    In this way, we could ensure:
 *    a) Offset with index INDEX_SCHEMA_CHANGE is always committed (typically)
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
trait DeltaSourceSchemaEvolutionSupport extends DeltaSourceBase {
  base: DeltaSource =>

  /**
   * Whether this DeltaSource is utilizing a schema log entry as its read schema.
   *
   * If user explicitly turn on the flag to fall back to using latest schema to read (i.e. the
   * legacy mode), we will ignore the schema log.
   */
  protected def trackingSchemaChange: Boolean =
    !forceEnableStreamingReadOnColumnMappingSchemaChanges &&
      schemaTrackingLog.flatMap(_.getCurrentTrackedSchema).nonEmpty

  /**
   * Whether a schema tracking log is provided (and is empty), so we could initialize.
   * This should only be used for the first write to the schema log, after then, schema tracking
   * should not rely on this state any more.
   */
  protected def readyToInitializeSchemaTracking: Boolean =
    !forceEnableStreamingReadOnColumnMappingSchemaChanges &&
      schemaTrackingLog.exists(_.getCurrentTrackedSchema.isEmpty)

  /**
   * This is called from getFileChangesWithRateLimit() during latestOffset().
   */
  protected def stopIndexedFileIteratorAtSchemaChangeBarrier(
      fileActionScanIter: ClosableIterator[IndexedFile]): ClosableIterator[IndexedFile] = {
    fileActionScanIter.withClose { iter =>
      val (untilSchemaChange, fromSchemaChange) = iter.span { i =>
        i.index != DeltaSourceOffset.SCHEMA_CHANGE_INDEX
      }
      // This will end at the schema change indexed file (inclusively)
      // If there are no schema changes, this is an no-op.
      untilSchemaChange ++ fromSchemaChange.take(1)
    }
  }

  /**
   * Check if a schema change is different from the stream read schema.
   * A strict equality check on the schemas should be safest to capture all schema changes.
   */
  protected def hasSchemaChangeComparedToStreamSchema(s: StructType): Boolean =
    s != readSchemaAtSourceInit

  /**
   * If the current stream schema is not equal to the schema change in [[metadataChangeOpt]], return
   * a schema change barrier [[IndexedFile]].
   * Only returns something if [[trackingSchemaChange]]is true.
   */
  protected def getSchemaChangeIndexedFileIterator(
      metadataChangeOpt: Option[Metadata], version: Long): ClosableIterator[IndexedFile] = {
    // It would be nice to capture partition schema change as well.
    if (trackingSchemaChange &&
        metadataChangeOpt.exists(m => hasSchemaChangeComparedToStreamSchema(m.schema))) {
      // Create an IndexedFile with metadata change
      Iterator.single(IndexedFile(version, DeltaSourceOffset.SCHEMA_CHANGE_INDEX, null)).toClosable
    } else {
      Iterator.empty.toClosable
    }
  }

  /**
   * Collect all metadata actions between start and end version, both inclusive
   */
  private def collectMetadataActions(
      startVersion: Long, endVersion: Long): Seq[(Long, Metadata)] = {
    val metadataActions = mutable.ArrayBuffer[(Long, Metadata)]()
    deltaLog.getChangeLogFiles(startVersion, options.failOnDataLoss).takeWhile {
      case (version, _) => version <= endVersion
    }.foreach { case (version, fileStatus) =>
      val fileIterator = deltaLog.store.readAsIterator(
        fileStatus,
        deltaLog.newDeltaHadoopConf())
      try {
        fileIterator.map(Action.fromJson)
          .collectFirst { case m: Metadata => m }
          .foreach { m => metadataActions.append((version, m)) }
      } finally {
        fileIterator.close()
      }
    }
    metadataActions.toSeq
  }

  /**
   * Given the version range for an ALREADY fetched batch, check if there are any
   * read-incompatible schema changes.
   * In this case, the streaming engine wants to getBatch(X,Y) on an existing Y that is already
   * loaded and saved in the offset log in the past before requesting new offsets. Therefore we
   * should verify if we could find a schema that is safe to read this constructed batch, which
   * then can be used to initialize the schema log.
   * If not, there's not much we could do, even with schema log, because unlike finding new offsets,
   * we don't have a chance to "split" this batch at schema change boundaries any more. The
   * streaming engine is not able to change the ranges of a batch after it has created it.
   * If there are no non-additive schema changes, the latest schema in the range should be the best
   * schema to read this batch as it must be a superset of all schema changes in between.
   */
  private def resolveValidSchemaOfConstructedBatchForSchemaTrackingInitialization(
      startVersion: Long, endVersion: Long): (Long, Metadata) = {
    assert(readyToInitializeSchemaTracking)
    val schemaChanges = collectMetadataActions(startVersion, endVersion)
    // If no schema changes in between, just serve the start version
    val startSchemaMetadata = getSnapshotFromDeltaLog(startVersion).metadata
    if (schemaChanges.isEmpty) {
      return (startVersion, startSchemaMetadata)
    }

    // Try to find rename or drop columns in between, or nullability/datatype changes by using
    // the last schema as the read schema and if so we cannot find a good read schema.
    val (mostRecentSchemaVersion, mostRecentSchemaChange) = schemaChanges.last
    val otherSchemaChanges = Seq((startVersion, startSchemaMetadata)) ++ schemaChanges.dropRight(1)
    otherSchemaChanges.foreach { case (_, potentialSchemaChangeMetadata) =>
      if (!DeltaColumnMapping.hasNoColumnMappingSchemaChanges(
        newMetadata = mostRecentSchemaChange,
        oldMetadata = potentialSchemaChangeMetadata) ||
        !SchemaUtils.isReadCompatible(
          existingSchema = potentialSchemaChangeMetadata.schema,
          readSchema = mostRecentSchemaChange.schema,
          forbidTightenNullability = true)) {
        throw DeltaErrors.streamingSchemaLogInitFailedIncompatibleSchemaException(
          startVersion, endVersion)
      }
    }

    // If not, the schema changes must be additive, we can use the most recent schema change.
    (mostRecentSchemaVersion, mostRecentSchemaChange)
  }

  /**
   * If we know there must exist a schema change at `version`, we read it out directly.
   */
  private def collectSchemaChangeAtVersion(version: Long): Metadata = {
    val Seq((_, metadata)) = collectMetadataActions(version, version)
    metadata
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
    if (previousOffset.index == DeltaSourceOffset.SCHEMA_CHANGE_INDEX) {
      return Some(previousOffset.copy(index = DeltaSourceOffset.POST_SCHEMA_CHANGE_INDEX))
    }
    // If the previous offset is already POST the schema change and schema evolution has not
    // occurred, simply block as no-op.
    if (previousOffset.index == DeltaSourceOffset.POST_SCHEMA_CHANGE_INDEX &&
      hasSchemaChangeComparedToStreamSchema(
        collectSchemaChangeAtVersion(previousOffset.reservoirVersion).schema)) {
      return Some(previousOffset)
    }

    // Otherwise, no special handling
    None
  }

  /**
   * Initialize the schema tracking log if an empty schema tracking log is provided.
   * @param batchStartVersion Start version of the batch of data to be proceed, it should typically
   *                          be the schema that is safe to process incoming data.
   * @param batchEndVersionOpt Optionally, if we are looking at a constructed batch with existing
   *                           end offset, we need to double verify to ensure no read-incompatible
   *                           within the batch range.
   */
  protected def initializeSchemaTrackingAndExitStreamIfNeeded(
      batchStartVersion: Long, batchEndVersionOpt: Option[Long] = None): Unit = {
    // If possible, initialize the schema log with the desired start schema instead of failing.
    // If a `batchEndVersion` is provided, we also need to verify if there are no incompatible
    // schema changes in a constructed batch, if so, we cannot find a proper schema to init the
    // schema log.
    if (readyToInitializeSchemaTracking) {
      val (version, metadata) = batchEndVersionOpt.map(
        resolveValidSchemaOfConstructedBatchForSchemaTrackingInitialization(batchStartVersion, _))
        .getOrElse {
          val startSnapshot = getSnapshotFromDeltaLog(batchStartVersion)
          (startSnapshot.version, startSnapshot.metadata)
        }
      val schemaToUse = PersistedSchema(tableId, version, metadata.schema, metadata.partitionSchema)
      // Always initialize the schema log
      schemaTrackingLog.get.evolveSchema(schemaToUse)
      if (hasSchemaChangeComparedToStreamSchema(metadata.schema)) {
        // But trigger schema evolution exception when there's a difference
        throw DeltaErrors.streamingSchemaEvolutionException(schemaToUse.dataSchema)
      }
    }
  }

  /**
   * Update the current stream schema in the schema tracking log and fail the stream.
   * This is called during commit().
   * It's ok to fail during commit() because in streaming's semantics, the batch with offset ending
   * at `end` should've already being processed completely.
   */
  protected def updateSchemaTrackingLogAndFailTheStreamIfNeeded(end: Offset): Unit = {
    val offset = DeltaSourceOffset(tableId, end)
    if (trackingSchemaChange &&
        (offset.index == DeltaSourceOffset.SCHEMA_CHANGE_INDEX ||
          offset.index == DeltaSourceOffset.POST_SCHEMA_CHANGE_INDEX)) {

      // The offset must point to a schema change metadata action
      val schemaChange = collectSchemaChangeAtVersion(offset.reservoirVersion)
      val schemaToPersist = PersistedSchema(
        deltaLog.tableId,
        offset.reservoirVersion,
        schemaChange.schema,
        schemaChange.partitionSchema
      )

      // Evolve the schema when the schema is indeed different from the current stream schema. We
      // need to check this because we could potentially generate two offsets before schema
      // evolution each with different indices.
      // Typically streaming engine will commit the first one and evolve the schema log, however,
      // to be absolutely safe, we also consider the case when the first is skipped and only the
      // second one is committed.
      // If the first one is committed (typically), the stream will fail and restart with the
      // evolved schema, then we should NOT fail/evolve again when we commit the second offset.
      if (hasSchemaChangeComparedToStreamSchema(schemaChange.schema)) {
        // Update schema log
        schemaTrackingLog.get.evolveSchema(schemaToPersist)
        // Fail the stream with schema evolution exception
        throw DeltaErrors.streamingSchemaEvolutionException(schemaChange.schema)
      }
    }
  }

  // scalastyle:off
  /**
   * Given a non-additive operation type from a previous schema evolution, check we can process
   * using the new schema given any SQL conf users have explicitly set to unblock.
   * The SQL conf can take one of following formats:
   * 1. spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop = true
   *    -> allows all non-additive schema changes to propagate.
   * 2. spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop.$checkpointHash = true
   *    -> allows all non-additive schema changes to propagate for this particular stream
   * 3. spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop.$checkpointHash = $deltaVersion
   *
   * The `allowSourceColumnRenameAndDrop` can be replaced with:
   * 1. `allowSourceColumnRename` to just allow column rename
   * 2. `allowSourceColumnDrop` to just allow column drops
   *
   * We will check for any of these configs given the non-additive operation, and throw a proper
   * error message to instruct the user to set the SQL conf if they would like to unblock.
   *
   * @param currentSchema The current persisted schema
   * @param previousSchema The previous persisted schema
   */
  // scalastyle:on
  protected def validateIfSchemaChangeCanBeUnblockedWithSQLConf(
      currentSchema: PersistedSchema,
      previousSchema: PersistedSchema): Unit = {
    val sqlConfPrefix = s"${DeltaSQLConf.SQL_CONF_PREFIX}.streaming"
    val checkpointHash = metadataPath.hashCode
    val allowAll = "allowSourceColumnRenameAndDrop"
    val allowRename = "allowSourceColumnRename"
    val allowDrop = "allowSourceColumnDrop"

    def getConf(key: String): Option[String] =
      Option(spark.sessionState.conf.getConfString(key, null))
        .map(_.toLowerCase(Locale.ROOT))

    def getConfPairsToAllowSchemaChange(
        allowSchemaChange: String, schemaChangeVersion: Long): Seq[(String, String)] =
      Seq(
        (s"$sqlConfPrefix.$allowSchemaChange", "always"),
        (s"$sqlConfPrefix.$allowSchemaChange.ckpt_$checkpointHash", "always"),
        (s"$sqlConfPrefix.$allowSchemaChange.ckpt_$checkpointHash", schemaChangeVersion.toString)
      )

    val schemaChangeVersion = currentSchema.deltaCommitVersion
    val confPairsToAllowAllSchemaChange =
      getConfPairsToAllowSchemaChange(allowAll, schemaChangeVersion)

    determineNonAdditiveSchemaChangeType(
      currentSchema.dataSchema, previousSchema.dataSchema).foreach {
      case NonAdditiveSchemaChangeTypes.SCHEMA_CHANGE_DROP =>
        val validConfKeysValuePair =
          getConfPairsToAllowSchemaChange(allowDrop, schemaChangeVersion) ++
            confPairsToAllowAllSchemaChange
        if (!validConfKeysValuePair.exists(p => getConf(p._1).contains(p._2))) {
          // Throw error to prompt user to set the correct confs
          throw DeltaErrors.cannotContinueStreamingPostSchemaEvolution(
            NonAdditiveSchemaChangeTypes.SCHEMA_CHANGE_DROP,
            schemaChangeVersion,
            checkpointHash,
            allowAll, allowDrop)
        }
      case NonAdditiveSchemaChangeTypes.SCHEMA_CHANGE_RENAME =>
        val validConfKeysValuePair =
          getConfPairsToAllowSchemaChange(allowRename, schemaChangeVersion) ++
            confPairsToAllowAllSchemaChange
        if (!validConfKeysValuePair.exists(p => getConf(p._1).contains(p._2))) {
          // Throw error to prompt user to set the correct confs
          throw DeltaErrors.cannotContinueStreamingPostSchemaEvolution(
            NonAdditiveSchemaChangeTypes.SCHEMA_CHANGE_RENAME,
            schemaChangeVersion,
            checkpointHash,
            allowAll, allowRename)
        }
      case NonAdditiveSchemaChangeTypes.SCHEMA_CHANGE_RENAME_AND_DROP =>
        val validConfKeysValuePair = confPairsToAllowAllSchemaChange
        if (!validConfKeysValuePair.exists(p => getConf(p._1).contains(p._2))) {
          // Throw error to prompt user to set the correct confs
          throw DeltaErrors.cannotContinueStreamingPostSchemaEvolution(
            NonAdditiveSchemaChangeTypes.SCHEMA_CHANGE_DROP,
            schemaChangeVersion,
            checkpointHash,
            allowAll, allowAll)
        }
    }
  }

  /**
   * Determine the non-additive schema change type for an incoming schema change. None if it's
   * additive.
   */
  private def determineNonAdditiveSchemaChangeType(
      newSchema: StructType, oldSchema: StructType): Option[String] = {
    val isRenameColumn = DeltaColumnMapping.isRenameColumnOperation(newSchema, oldSchema)
    val isDropColumn = DeltaColumnMapping.isDropColumnOperation(newSchema, oldSchema)
    if (isRenameColumn && isDropColumn) {
      Some(NonAdditiveSchemaChangeTypes.SCHEMA_CHANGE_RENAME_AND_DROP)
    } else if (isRenameColumn) {
      Some(NonAdditiveSchemaChangeTypes.SCHEMA_CHANGE_RENAME)
    } else if (isDropColumn) {
      Some(NonAdditiveSchemaChangeTypes.SCHEMA_CHANGE_DROP)
    } else {
      None
    }
  }
}

object NonAdditiveSchemaChangeTypes {
  // Rename -> caused by a single column rename
  val SCHEMA_CHANGE_RENAME = "RENAME COLUMN"
  // Drop -> caused by a single column drop
  val SCHEMA_CHANGE_DROP = "DROP COLUMN"
  // A combination of rename and drop columns -> can be caused by a complete overwrite
  val SCHEMA_CHANGE_RENAME_AND_DROP = "RENAME AND DROP COLUMN"
}
