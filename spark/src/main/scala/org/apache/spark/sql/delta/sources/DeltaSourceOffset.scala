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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog}
import org.apache.spark.sql.delta.util.JsonUtils
import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2}
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}

/**
 * Tracks how far we processed in when reading changes from the [[DeltaLog]].
 *
 * Note this class retains the naming of `Reservoir` to maintain compatibility
 * with serialized offsets from the beta period.
 *
 * @param sourceVersion     The version of serialization that this offset is encoded with.
 *                          It should not be set manually!
 * @param reservoirId       The id of the table we are reading from. Used to detect
 *                          misconfiguration when restarting a query.
 * @param reservoirVersion  The version of the table that we are current processing.
 * @param index             The index in the sequence of AddFiles in this version. Used to
 *                          break large commits into multiple batches. This index is created by
 *                          sorting on modificationTimestamp and path.
 * @param isStartingVersion Whether this offset denotes a query that is starting rather than
 *                          processing changes. When starting a new query, we first process
 *                          all data present in the table at the start and then move on to
 *                          processing new data that has arrived.
 */
case class DeltaSourceOffset private(
    sourceVersion: Long,
    reservoirId: String,
    reservoirVersion: Long,
    index: Long,
    isStartingVersion: Boolean
  ) extends Offset with Comparable[DeltaSourceOffset] {

  import DeltaSourceOffset._

  override def json: String = {
    // We handle a few backward compatibility scenarios during Serialization here:
    // 1. [Backward compatibility] If the source index is a schema changing base index, then replace
    //    it with index = -1 and use VERSION_1.This allows older Delta to at least be able to read
    //    the non-schema-changes stream offsets.
    //    This needs to happen during serialization time so we won't be looking at a downgraded
    //    index right away when we need to utilize this offset in memory.
    // 2. [Backward safety] If the source index is a new schema changing index, then use
    //    VERSION_3. Older Delta would explode upon seeing this, but that's the safe thing to do.
    val minVersion = {
      if (DeltaSourceOffset.isSchemaChangeIndex(index)) {
        VERSION_3
      }
      else {
        VERSION_1
      }
    }
    val downgradedIndex = if (index == BASE_INDEX) {
      BASE_INDEX_V1
    } else {
      index
    }
    val objectToSerialize = this.copy(sourceVersion = minVersion, index = downgradedIndex)
    JsonUtils.toJson(objectToSerialize)
  }

  /**
   * Compare two DeltaSourceOffsets which are on the same table.
   * @return 0 for equivalent offsets. negative if this offset is less than `otherOffset`. Positive
   *         if this offset is greater than `otherOffset`
   */
  def compare(otherOffset: DeltaSourceOffset): Int = {
    assert(reservoirId == otherOffset.reservoirId, "Comparing offsets that do not refer to the" +
      " same table is disallowed.")
    implicitly[Ordering[(Long, Long)]].compare((reservoirVersion, index),
      (otherOffset.reservoirVersion, otherOffset.index))
  }
  override def compareTo(o: DeltaSourceOffset): Int = {
    compare(o)
  }
}

object DeltaSourceOffset extends Logging {

  private[DeltaSourceOffset] val VERSION_1 = 1
  private[DeltaSourceOffset] val VERSION_2 = 2 // reserved
  // Serialization version 3 adds support for schema change index values.
  private[DeltaSourceOffset] val VERSION_3 = 3

  private[DeltaSourceOffset] val CURRENT_VERSION = VERSION_3

  // The base index within each reservoirVersion. This offset indicates the offset before all
  // changes in the reservoirVersion. All other offsets within the reservoirVersion have an index
  // that is higher than the base index.
  //
  // This index is for VERSION_3+. Unless there are other fields that force the version to be >=3,
  // it should NOT be serialized into offset log for backward compatibility. Instead, we serialize
  // this as INDEX_VERSION_BASE_V1, and set source version lower accordingly. It gets converted back
  // to the VERSION_3 value at deserialization time, so that we only use the V3 value in memory.
  private[DeltaSourceOffset] val BASE_INDEX_V3: Long = -100

  // The V1 base index that should be serialized into the offset log
  private[DeltaSourceOffset] val BASE_INDEX_V1: Long = -1

  // The base index version clients of DeltaSourceOffset should use
  val BASE_INDEX: Long = BASE_INDEX_V3

  // The index for an IndexedFile that also contains a schema change. (from VERSION_3)
  val SCHEMA_CHANGE_INDEX: Long = -20
  // The index for an IndexedFile that is right after a schema change. (from VERSION_3)
  val POST_SCHEMA_CHANGE_INDEX: Long = -19

  /**
   * The ONLY external facing constructor to create a DeltaSourceOffset in memory.
   * @param reservoirId Table id
   * @param reservoirVersion Table commit version
   * @param index File action index in the commit version
   * @param isStartingVersion Whether this offset is still in initial snapshot
   */
  def apply(
      reservoirId: String,
      reservoirVersion: Long,
      index: Long,
      isStartingVersion: Boolean
  ): DeltaSourceOffset = {
    // TODO should we detect `reservoirId` changes when a query is running?
    new DeltaSourceOffset(
      CURRENT_VERSION,
      reservoirId,
      reservoirVersion,
      index,
      isStartingVersion
    )
  }

  /**
   * Validate and parse a DeltaSourceOffset from its serialized format
   * @param reservoirId Table id
   * @param offset Raw streaming offset
   */
  def apply(reservoirId: String, offset: OffsetV2): DeltaSourceOffset = {
    offset match {
      case o: DeltaSourceOffset => o
      case s =>
        validateSourceVersion(s.json)
        val o = JsonUtils.mapper.readValue[DeltaSourceOffset](s.json)
        if (o.reservoirId != reservoirId) {
          throw DeltaErrors.nonExistentDeltaTableStreaming(o.reservoirId)
        }
        // Always upgrade to use the current latest INDEX_VERSION_BASE
        val offsetIndex = if (o.sourceVersion < VERSION_3 && o.index == BASE_INDEX_V1) {
          logDebug(s"upgrading offset to use latest version base index")
          BASE_INDEX
        } else {
          o.index
        }
        // Leverage the only external facing constructor to initialize with latest sourceVersion
        DeltaSourceOffset(
          o.reservoirId,
          o.reservoirVersion,
          offsetIndex,
          o.isStartingVersion
        )
    }
  }

  private def validateSourceVersion(json: String): Unit = {
    val parsedJson = parse(json)
    val versionJValueOpt = jsonOption(parsedJson \ "sourceVersion")
    val versionOpt = versionJValueOpt.map {
      case i: JInt => i.num.longValue
      case other => throw DeltaErrors.invalidSourceVersion(other)
    }
    if (versionOpt.isEmpty) {
      throw DeltaErrors.cannotFindSourceVersionException(json)
    }

    if (versionOpt.get == VERSION_2) {
      // Version 2 is reserved.
      throw DeltaErrors.invalidSourceVersion(versionJValueOpt.get)
    }

    val maxVersion = VERSION_3

    if (versionOpt.get > maxVersion) {
      throw DeltaErrors.invalidFormatFromSourceVersion(versionOpt.get, maxVersion)
    }
  }

  /** Return an option that translates JNothing to None */
  private def jsonOption(json: JValue): Option[JValue] = {
    json match {
      case JNothing => None
      case value: JValue => Some(value)
    }
  }

  /**
   * Validate offsets to make sure we always move forward. Moving backward may make the query
   * re-process data and cause data duplication.
   */
  def validateOffsets(previousOffset: DeltaSourceOffset, currentOffset: DeltaSourceOffset): Unit = {
    if (!previousOffset.isStartingVersion && currentOffset.isStartingVersion) {
      throw new IllegalStateException(
        s"Found invalid offsets: 'isStartingVersion' fliped incorrectly. " +
          s"Previous: $previousOffset, Current: $currentOffset")
    }
    if (previousOffset.reservoirVersion > currentOffset.reservoirVersion) {
      throw new IllegalStateException(
        s"Found invalid offsets: 'reservoirVersion' moved back. " +
          s"Previous: $previousOffset, Current: $currentOffset")
    }
    if (previousOffset.reservoirVersion == currentOffset.reservoirVersion &&
      previousOffset.index > currentOffset.index) {
      throw new IllegalStateException(
        s"Found invalid offsets. 'index' moved back. " +
          s"Previous: $previousOffset, Current: $currentOffset")
    }
  }

  def isSchemaChangeIndex(index: Long): Boolean =
    index == SCHEMA_CHANGE_INDEX || index == POST_SCHEMA_CHANGE_INDEX
}
