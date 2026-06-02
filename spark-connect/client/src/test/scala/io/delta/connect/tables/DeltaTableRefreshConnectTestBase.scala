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

package io.delta.tables

import io.delta.tables.shared.DeltaTableRefreshSharedBase

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.test.DeltaQueryTest

/**
 * Connect wiring for [[DeltaRepeatedAccessRefreshTests]]. The only Connect-specific piece is the
 * arity-mismatch assertion: Connect can wrap the error, so the condition may surface in the message
 * rather than as the error condition.
 */
trait DeltaTableRefreshConnectTestBase extends DeltaTableRefreshSharedBase {
  self: DeltaQueryTest with RemoteSparkSession =>

  /** Asserts a SparkThrowable carries the expected error condition, tolerating Connect wrapping. */
  protected def checkError(exception: SparkThrowable, condition: String): Unit = {
    val cond = exception.getCondition
    if (cond != condition) {
      assert(exception.asInstanceOf[Exception].getMessage.contains(condition),
        s"Expected error condition '$condition' but got '$cond' " +
        s"and message does not contain it either")
    }
  }

  override protected def assertArityMismatchError(f: => Unit): Unit = {
    checkError(
      exception = intercept[AnalysisException] { f },
      condition = "INSERT_COLUMN_ARITY_MISMATCH")
  }
}
