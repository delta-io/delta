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

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog}
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.json4s._
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}

/**
 * Tracks how far we processed in when reading changes from the [[DeltaLog]].
 *
 * Note this class retains the naming of `Reservoir` to maintain compatibility
 * with serialized offsets from the beta period.
 *
 * @param sourceVersion     The version of serialization that this offset is encoded with.
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
case class DeltaSourceOffset(
    sourceVersion: Long,
    reservoirId: String,
    reservoirVersion: Long,
    index: Long,
    isStartingVersion: Boolean
  ) extends Offset {

  override def json: String = JsonUtils.toJson(this)

  /**
   * Compare two DeltaSourceOffsets which are on the same table and source version.
   * @return 0 for equivalent offsets. negative if this offset is less than `otherOffset`. Positive
   *         if this offset is greater than `otherOffset`
   */
  def compare(otherOffset: DeltaSourceOffset): Int = {
    assert(reservoirId == otherOffset.reservoirId &&
      sourceVersion == otherOffset.sourceVersion, "Comparing offsets that do not refer to the" +
      " same table is disallowed.")
    implicitly[Ordering[(Long, Long)]].compare((reservoirVersion, index),
      (otherOffset.reservoirVersion, otherOffset.index))
  }
}

object DeltaSourceOffset {

  val VERSION_1 = 1

  def apply(
      sourceVersion: Long,
      reservoirId: String,
      reservoirVersion: Long,
      index: Long,
      isStartingVersion: Boolean
  ): DeltaSourceOffset = {
    // TODO should we detect `reservoirId` changes when a query is running?
    new DeltaSourceOffset(
      sourceVersion,
      reservoirId,
      reservoirVersion,
      index,
      isStartingVersion
    )
  }

  def apply(reservoirId: String, offset: Offset): DeltaSourceOffset = {
    offset match {
      case o: DeltaSourceOffset => o
      case s: SerializedOffset =>
        validateSourceVersion(s.json)
        val o = JsonUtils.mapper.readValue[DeltaSourceOffset](s.json)
        if (o.reservoirId != reservoirId) {
          throw DeltaErrors.nonExistentDeltaTable(o.reservoirId)
        }
        o
    }
  }

  private def validateSourceVersion(json: String): Unit = {
    val parsedJson = parse(json)
    val versionOpt = jsonOption(parsedJson \ "sourceVersion").map {
      case i: JInt => i.num.longValue
      case other => throw DeltaErrors.invalidSourceVersion(other)
    }
    if (versionOpt.isEmpty) {
      throw DeltaErrors.cannotFindSourceVersionException(json)
    }

    var maxVersion = VERSION_1

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
}
