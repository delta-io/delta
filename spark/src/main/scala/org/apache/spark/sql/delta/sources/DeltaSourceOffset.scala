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
import java.io.IOException

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog}
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.core.{JsonGenerator, JsonParseException, JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2}
import org.apache.spark.sql.execution.streaming.Offset

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
 * @param isInitialSnapshot Whether this offset points into an initial full table snapshot at the
 *                          provided reservoir version rather than into the changes at that version.
 *                          When starting a new query, we first process all data present in the
 *                          table at the start and then move on to processing new data that has
 *                          arrived.
 */
@JsonDeserialize(using = classOf[DeltaSourceOffset.Deserializer])
@JsonSerialize(using = classOf[DeltaSourceOffset.Serializer])
case class DeltaSourceOffset private(
    sourceVersion: Long,
    reservoirId: String,
    reservoirVersion: Long,
    index: Long,
    isInitialSnapshot: Boolean
  ) extends Offset with Comparable[DeltaSourceOffset] {

  import DeltaSourceOffset._

  override def json: String = {
    JsonUtils.toJson(this)
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

  // The index for an IndexedFile that also contains a metadata change. (from VERSION_3)
  val METADATA_CHANGE_INDEX: Long = -20
  // The index for an IndexedFile that is right after a metadata change. (from VERSION_3)
  val POST_METADATA_CHANGE_INDEX: Long = -19

  /**
   * The ONLY external facing constructor to create a DeltaSourceOffset in memory.
   * @param reservoirId Table id
   * @param reservoirVersion Table commit version
   * @param index File action index in the commit version
   * @param isInitialSnapshot Whether this offset is still in initial snapshot
   */
  def apply(
      reservoirId: String,
      reservoirVersion: Long,
      index: Long,
      isInitialSnapshot: Boolean
  ): DeltaSourceOffset = {
    // TODO should we detect `reservoirId` changes when a query is running?
    new DeltaSourceOffset(
      CURRENT_VERSION,
      reservoirId,
      reservoirVersion,
      index,
      isInitialSnapshot
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
        val o = JsonUtils.mapper.readValue[DeltaSourceOffset](s.json)
        if (o.reservoirId != reservoirId) {
          throw DeltaErrors.differentDeltaTableReadByStreamingSource(
            newTableId = reservoirId, oldTableId = o.reservoirId)
        }
        o
    }
  }

  /**
   * Validate offsets to make sure we always move forward. Moving backward may make the query
   * re-process data and cause data duplication.
   */
  def validateOffsets(previousOffset: DeltaSourceOffset, currentOffset: DeltaSourceOffset): Unit = {
    if (!previousOffset.isInitialSnapshot && currentOffset.isInitialSnapshot) {
      throw new IllegalStateException(
        s"Found invalid offsets: 'isInitialSnapshot' fliped incorrectly. " +
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

  def isMetadataChangeIndex(index: Long): Boolean =
    index == METADATA_CHANGE_INDEX || index == POST_METADATA_CHANGE_INDEX

  /**
   * This is a 1:1 copy of [[DeltaSourceOffset]] used for JSON serialization. Our serializers only
   * want to adjust some field values and then serialize in the normal way. But we cannot access the
   * "default" serializers once we've overridden them. So instead, we use a separate case class that
   * gets serialized "as-is".
   */
  private case class DeltaSourceOffsetForSerialization private(
      sourceVersion: Long,
      reservoirId: String,
      reservoirVersion: Long,
      index: Long,
      // This stores isInitialSnapshot.
      // This was confusingly called "starting version" in earlier versions, even though enabling
      // the option "startingVersion" actually causes this to be disabled. We still have to
      // serialize it using the old name for backward compatibility.
      isStartingVersion: Boolean
    )

  class Deserializer
    extends StdDeserializer[DeltaSourceOffset](classOf[DeltaSourceOffset]) {
    @throws[IOException]
    @throws[JsonProcessingException]
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): DeltaSourceOffset = {
      val o = try {
        p.readValueAs(classOf[DeltaSourceOffsetForSerialization])
      } catch {
        case _: JsonParseException =>
          // The version may be there with a different format, or something else might be off.
          throw DeltaErrors.invalidSourceOffsetFormat()
      }

      if (o.sourceVersion < VERSION_1) {
        throw DeltaErrors.invalidSourceVersion(o.sourceVersion.toString)
      }
      if (o.sourceVersion > CURRENT_VERSION) {
        throw DeltaErrors.invalidFormatFromSourceVersion(o.sourceVersion, CURRENT_VERSION)
      }
      if (o.sourceVersion == VERSION_2) {
        // Version 2 is reserved.
        throw DeltaErrors.invalidSourceVersion(o.sourceVersion.toString)
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
        reservoirId = o.reservoirId,
        reservoirVersion = o.reservoirVersion,
        index = offsetIndex,
        isInitialSnapshot = o.isStartingVersion
      )
    }
  }

  class Serializer
    extends StdSerializer[DeltaSourceOffset](classOf[DeltaSourceOffset]) {

    @throws[IOException]
    override def serialize(
        o: DeltaSourceOffset,
        gen: JsonGenerator,
        provider: SerializerProvider): Unit = {
      // We handle a few backward compatibility scenarios during Serialization here:
      // 1. [Backward compatibility] If the source index is a schema changing base index, then
      //    replace it with index = -1 and use VERSION_1. This allows older Delta to at least be
      //    able to read the non-schema-changes stream offsets.
      //    This needs to happen during serialization time so we won't be looking at a downgraded
      //    index right away when we need to utilize this offset in memory.
      // 2. [Backward safety] If the source index is a new schema changing index, then use
      //    VERSION_3. Older Delta would explode upon seeing this, but that's the safe thing to do.
      val minVersion = {
        if (DeltaSourceOffset.isMetadataChangeIndex(o.index)) {
          VERSION_3
        }
        else {
          VERSION_1
        }
      }
      val downgradedIndex = if (o.index == BASE_INDEX) {
        BASE_INDEX_V1
      } else {
        o.index
      }
      gen.writeObject(DeltaSourceOffsetForSerialization(
        sourceVersion = minVersion,
        reservoirId = o.reservoirId,
        reservoirVersion = o.reservoirVersion,
        index = downgradedIndex,
        isStartingVersion = o.isInitialSnapshot
      ))
    }
  }
}
