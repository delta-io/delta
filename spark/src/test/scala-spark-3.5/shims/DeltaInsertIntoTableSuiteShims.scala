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

package org.apache.spark.sql.delta

object DeltaInsertIntoTableSuiteShims {
  val INSERT_INTO_TMP_VIEW_ERROR_MSG = "Inserting into a view is not allowed"

  val INVALID_COLUMN_DEFAULT_VALUE_ERROR_MSG = "INVALID_DEFAULT_VALUE.UNRESOLVED_EXPRESSION"
}
