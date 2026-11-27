/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import scala.util.Try

import org.apache.spark.sql.delta.actions.{Checkpoint, SidecarFile, SidecarType}

import org.apache.spark.SparkException

/**
 * The on-disk contract of the non-content sidecars of an AMT checkpoint: parquet files under
 * `_delta_log/_sidecars/` that hold the set transactions (`type = txn`) or the spillable domain
 * metadata (`type = domainMetadata`) of the checkpoint, one [[SingleAction]] row per entry with
 * only the column of the sidecar type populated.
 *
 * Each key (txn `appId`, domain name) is inline or in exactly one sidecar, never both. A kind may
 * be split across inline entries and sidecars, so readers always take the union of the two.
 */
object AMTNonContentSidecars {

  /**
   * Optional tag on a non-content [[SidecarFile]] holding the number of entries in the file.
   */
  val NUM_ENTRIES_TAG = "numEntries"

  /** The sidecars of `checkpoint` of the given type. */
  def sidecarsOfType(checkpoint: Checkpoint, sidecarType: String): Seq[SidecarFile] =
    sidecarsOfType(checkpoint.sidecars, sidecarType)

  /** The sidecars of the given type among `sidecars`. */
  def sidecarsOfType(sidecars: Seq[SidecarFile], sidecarType: String): Seq[SidecarFile] =
    sidecars.filter(_.`type`.contains(sidecarType))

  /**
   * Entry count of a non-content sidecar from its [[NUM_ENTRIES_TAG]] tag, or None when the tag
   * is missing or not a non-negative number.
   */
  def numEntries(sidecar: SidecarFile): Option[Long] =
    Option(sidecar.tags)
      .flatMap(_.get(NUM_ENTRIES_TAG))
      .flatMap(value => Try(value.toLong).toOption)
      .filter(_ >= 0)

  /** Whether `checkpoint` carries any non-content (typed) sidecar. */
  def hasNonContentSidecars(checkpoint: Checkpoint): Boolean =
    checkpoint.sidecars.exists(_.`type`.isDefined)

  /** The [[SingleAction]] column that a sidecar of `sidecarType` populates. */
  private[amt] def singleActionColumnName(sidecarType: String): String = sidecarType match {
    case SidecarType.Type.Txn => "txn"
    case SidecarType.Type.DomainMetadata => "domainMetadata"
    case other => throw SparkException.internalError(s"Unknown non-content sidecar type: $other")
  }
}
