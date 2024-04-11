/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package org.apache.spark.sql.catalyst.analysis

object UnresolvedTableImplicits {

  /**
   * Handles a breaking change in [[UnresolvedTable]] constructor between Spark 3.5 and 4.0:
   * - Spark 3.5: requires `relationTypeMismatchHint` param
   * - Spark 4.0: gets rid of `relationTypeMismatchHint`param
   */
  implicit class UnresolvedTableShim(self: UnresolvedTable.type) {
    def apply(
        tableNameParts: Seq[String],
        commandName: String): UnresolvedTable = {
      UnresolvedTable(tableNameParts, commandName, relationTypeMismatchHint = None)
    }
  }
}
