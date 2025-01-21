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

package org.apache.spark.sql.delta.util

// scalastyle:off import.ordering.noEmptyLine
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * A [[Dataset]] reference cache to automatically create new [[Dataset]] objects when the active
 * [[SparkSession]] changes. This is useful when sharing objects holding [[Dataset]] references
 * cross multiple sessions. Without this, using a [[Dataset]] that holds a stale session may change
 * the active session and cause multiple issues (e.g., if we switch to a stale session coming from a
 * notebook that has been detached, we may not be able to use built-in functions because those are
 * cleaned up).
 *
 * The `creator` function will be called to create a new [[Dataset]] object when the old one has a
 * different session than the current active session. Note that one MUST use SparkSession.active
 * in the creator() if creator() needs to use Spark session.
 *
 * Unlike [[StateCache]], this class only caches the [[Dataset]] reference and doesn't cache the
 * underlying `RDD`.
 *
 * WARNING: If there are many concurrent Spark sessions and each session calls 'get' multiple times,
 *          then the cost of creator becomes more noticeable as everytime it switch the active
 *          session, the older session needs to call creator again when it becomes active.
 *
 * @param creator a function to create [[Dataset]].
 */
class DatasetRefCache[T] private[util](creator: () => Dataset[T]) {

  private val holder = new AtomicReference[Dataset[T]]

  private[delta] def invalidate() = holder.set(null)

  def get: Dataset[T] = Option(holder.get())
    .filter(_.sparkSession eq SparkSession.active)
    .getOrElse {
      val df = creator()
      holder.set(df)
      df
    }
}
