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

package org.apache.spark.sql.delta.schema

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

/** Thrown when the given data doesn't match the rules defined on the table. */
case class InvariantViolationException(msg: String) extends IllegalArgumentException(msg)

object InvariantViolationException {
  def apply(
      invariant: Invariant,
      msg: String): InvariantViolationException = {
    new InvariantViolationException(s"Invariant ${invariant.rule.name} violated for column: " +
      s"${UnresolvedAttribute(invariant.column).name}.\n$msg")
  }
}
