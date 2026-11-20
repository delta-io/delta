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

package org.apache.spark.sql.delta.v2.exception

/** Exception thrown when no Delta table exists at the requested path. */
class TableNotFoundException(path: String)
    extends RuntimeException(String.format("Delta table does not exist at path: %s.", path)) {

  // Preserve Java initialization order: getTablePath returns null during super construction.
  private[this] val tablePath: String = path

  def getTablePath: String = tablePath
}
