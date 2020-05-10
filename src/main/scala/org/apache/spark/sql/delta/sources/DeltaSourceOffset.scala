/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.util.JsonUtils
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
    isStartingVersion: Boolean) extends Offset {

  override def json: String = JsonUtils.toJson(this)
}

object DeltaSourceOffset {

  val VERSION = 1

  def apply(
      reservoirId: String,
      reservoirVersion: Long,
      index: Long,
      isStartingVersion: Boolean): DeltaSourceOffset = {
    // TODO should we detect `reservoirId` changes when a query is running?
    new DeltaSourceOffset(VERSION, reservoirId, reservoirVersion, index, isStartingVersion)
  }

  def apply(reservoirId: String, offset: Offset): DeltaSourceOffset = {
    offset match {
      case o: DeltaSourceOffset => o
      case s: SerializedOffset =>
        validateSourceVersion(s.json)
        val o = JsonUtils.mapper.readValue[DeltaSourceOffset](s.json)
        if (o.reservoirId != reservoirId) {
          throw new IllegalStateException(s"Delta table ${o.reservoirId} doesn't exist. " +
              s"Please delete your streaming query checkpoint and restart.")
        }
        o
    }
  }

  private def validateSourceVersion(json: String): Unit = {
    val parsedJson = parse(json)
    val versionOpt = jsonOption(parsedJson \ "sourceVersion").map {
      case i: JInt => i.num.longValue
      case other => throw new IllegalStateException(s"sourceVersion($other) is invalid")
    }
    if (versionOpt.isEmpty) {
      throw new IllegalStateException(s"Cannot find 'sourceVersion' in $json")
    }
    if (versionOpt.get > VERSION) {
      throw new IllegalStateException(
        s"Unsupported format. Expected version is $VERSION " +
            s"but was ${versionOpt.get}. Please upgrade your Spark.")
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
