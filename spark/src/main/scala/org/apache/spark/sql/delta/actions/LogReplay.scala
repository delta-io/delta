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

package org.apache.spark.sql.delta.actions

/**
 * Replays a history of actions, resolving them to produce the current state
 * of the table.
 */
trait LogReplay {
  /** Append these `actions` to the state. Must only be called in ascending order of `version`. */
  def append(version: Long, actions: Iterator[Action]): Unit

  /** Returns the current state of the Table as an iterator of actions. */
  def checkpoint: Iterator[Action]
}
