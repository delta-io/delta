/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.v2.interop

/**
 * Abstract trait for commit info actions in Delta. This trait provides a common
 * abstraction that can be implemented by both Spark's V1 CommitInfo and Kernel's CommitInfo
 * in V2 connector. The V2 connector will implement adapters for reusing V1 utilities.
 */
trait AbstractCommitInfo {

  /**
   * Get the in-commit timestamp of the commit as milliseconds after the epoch.
   * This is the timestamp recorded in the commit itself, used for time travel.
   */
  def getCommitTimestamp: Long
}

