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

package org.apache.spark.sql.delta.hooks

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.Action

import org.apache.spark.sql.SparkSession

/**
 * A hook which can be executed after a transaction. These hooks are registered to a
 * [[OptimisticTransaction]], and are executed after a *successful* commit takes place.
 */
trait PostCommitHook {

  /** A user friendly name for the hook for error reporting purposes. */
  val name: String

  /** Executes the hook. */
  def run(spark: SparkSession, txn: OptimisticTransactionImpl, committedActions: Seq[Action]): Unit

  /**
   * Handle any error caused while running the hook. By default, all errors are ignored as
   * default policy should be to not let post-commit hooks to cause failures in the operation.
   */
  def handleError(error: Throwable, version: Long): Unit = {}
}
