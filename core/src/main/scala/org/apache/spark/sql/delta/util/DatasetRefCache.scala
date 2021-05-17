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

package org.apache.spark.sql.delta.util

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * A [[Dataset]] reference cache to automatically create new [[Dataset]] objects when the active
 * [[SparkSession]] changes. This is useful when sharing objects holding [[Dataset]] references
 * cross multiple sessions. Without this, using a [[Dataset]] that holds a stale session may change
 * the active session and cause multiple issues (e.g., if we switch to a stale session coming from a
 * notebook that has been detached, we may not be able to use built-in functions because those are
 * cleaned up).
 *
 * The `creator` function will be called to create a new [[Dataset]] object when the old one has a
 * different session than the current active session.
 *
 * Unlike [[StateCache]], this class only caches the [[Dataset]] reference and doesn't cache the
 * underlying `RDD`.
 *
 * @param creator a function to create [[Dataset]].
 */
class DatasetRefCache[T](creator: () => Dataset[T]) {

  private val holder = new AtomicReference[Dataset[T]]

  def get: Dataset[T] = holder.updateAndGet { ref =>
    if (ref == null || (ref.sparkSession ne SparkSession.active)) {
      creator()
    } else {
      ref
    }
  }
}
