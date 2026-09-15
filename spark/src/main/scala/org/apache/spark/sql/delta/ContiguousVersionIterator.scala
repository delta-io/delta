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

import org.apache.spark.sql.delta.ContiguousVersionIterator.GapEvent
import org.apache.spark.sql.util.ScalaExtensions._

/**
 * Wraps an arbitrary iterator of `T`s and verifies that the version extracted from each
 * successive item via `getVersionFromItem` is exactly `prev + 1`. The version of the first
 * emitted item is not constrained, since the underlying iterator may legitimately start at any
 * version (e.g. when the caller asked for `startVersion` but the earliest available item is
 * later).
 *
 * Behaviour on a gap (`logGap` is invoked in both modes so telemetry is consistent):
 *   - `treatGapAsFatal = true`  -> invoke `logGap`, then throw an [[IllegalStateException]].
 *   - `treatGapAsFatal = false` -> invoke `logGap` and continue.
 */
private[delta] class ContiguousVersionIterator[T](
    underlying: Iterator[T],
    getVersionFromItem: T => Long,
    treatGapAsFatal: Boolean,
    logGap: GapEvent[T] => Unit)
  extends Iterator[T] {

  private var prevItem: Option[T] = None

  override def hasNext: Boolean = underlying.hasNext

  override def next(): T = {
    val item = underlying.next()
    val version = getVersionFromItem(item)
    prevItem.ifDefined { prev =>
      val prevVersion = getVersionFromItem(prev)
      val expectedVersion = prevVersion + 1
      if (version != expectedVersion) {
        val gap = GapEvent(
          prevItem = prev,
          prevVersion = prevVersion,
          nextItem = item,
          nextVersion = version)
        logGap(gap)
        if (treatGapAsFatal) {
          throw new IllegalStateException(
            s"Non-contiguous versions: expected $expectedVersion, got $version " +
              s"(prevItem=$prev, nextItem=$item).")
        }
      }
    }
    prevItem = Some(item)
    item
  }
}

object ContiguousVersionIterator {
  /**
   * Describes a single non-contiguity event seen by [[ContiguousVersionIterator]].
   * `prevItem` / `nextItem` carry the surrounding items so that the `logGap` callback can
   * extract type-specific metadata (e.g. file name and modification time for a `FileStatus`).
   */
  case class GapEvent[T](prevItem: T, prevVersion: Long, nextItem: T, nextVersion: Long)
}
