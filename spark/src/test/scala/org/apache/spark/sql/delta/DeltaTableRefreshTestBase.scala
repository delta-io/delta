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

import io.delta.tables.shared.DeltaTableRefreshSharedBase
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Classic wiring for [[DeltaRepeatedAccessRefreshTests]]. The only classic-specific piece is the
 * arity-mismatch assertion (Spark's checkError with parameter matching); setup and external-write
 * simulation come from [[DeltaTableRefreshSharedBase]], and `spark` / `checkAnswer` / `withTable` /
 * `withTempPath` from the self-type.
 */
trait DeltaTableRefreshTestBase extends DeltaTableRefreshSharedBase {
  self: QueryTest with SharedSparkSession with DeltaSQLCommandTest =>

  override protected def assertArityMismatchError(f: => Unit): Unit = {
    checkError(
      exception = intercept[AnalysisException] { f },
      condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
      parameters = Map(
        "tableName" -> ".*", "tableColumns" -> ".*", "dataColumns" -> ".*"),
      matchPVals = true)
  }
}
