/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.utils

import io.delta.kernel.expressions.{Column, Expression, Predicate}

/** Useful helper functions for creating expressions in tests */
trait ExpressionTestUtils {

  def equals(e1: Expression, e2: Expression): Predicate = {
    new Predicate("=", e1, e2)
  }

  def lessThan(e1: Expression, e2: Expression): Predicate = {
    new Predicate("<", e1, e2)
  }

  def greaterThan(e1: Expression, e2: Expression): Predicate = {
    new Predicate(">", e1, e2)
  }

  def greaterThanOrEqual(e1: Expression, e2: Expression): Predicate = {
    new Predicate(">=", e1, e2)
  }

  def lessThanOrEqual(e1: Expression, e2: Expression): Predicate = {
    new Predicate("<=", e1, e2)
  }

  def not(pred: Predicate): Predicate = {
    new Predicate("NOT", pred)
  }

  def isNotNull(e1: Expression): Predicate = {
    new Predicate("IS_NOT_NULL", e1)
  }

  def col(name: String): Column = new Column(name)

  def nestedCol(name: String): Column = {
    new Column(name.split("\\."))
  }

  /* ---------- NOT-YET SUPPORTED EXPRESSIONS ----------- */

  /*
  These expressions are used in ScanSuite to test data skipping. For unsupported expressions
  no skipping filter will be generated and they should just be returned as part of the remaining
  predicate to evaluate. As we add support for these expressions we'll adjust the tests that use
  them to expect skipped files. If they are ever actually evaluated they will throw an exception.
   */

  def nullSafeEquals(e1: Expression, e2: Expression): Predicate = {
    new Predicate("<=>", e1, e2)
  }

  def notEquals(e1: Expression, e2: Expression): Predicate = {
    new Predicate("<>", e1, e2)
  }

  def startsWith(e1: Expression, e2: Expression): Predicate = {
    new Predicate("STARTS_WITH", e1, e2)
  }

  def isNull(e1: Expression): Predicate = {
    new Predicate("IS_NULL", e1)
  }
}
