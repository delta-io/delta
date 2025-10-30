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
package io.delta.kernel.test

import java.util.Optional

import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.internal.skipping.StatsSchemaHelper.STATS_WITH_COLLATION
import io.delta.kernel.types.CollationIdentifier

/** Utility functions for tests. */
trait TestUtils {
  def col(name: String): Column = new Column(name)

  def nestedCol(name: String): Column = {
    new Column(name.split("\\."))
  }

  def collatedStatsCol(
      collation: CollationIdentifier,
      statName: String,
      fieldName: String): Column = {
    val columnPath =
      Array(STATS_WITH_COLLATION, collation.toString, statName) ++ fieldName.split('.')
    new Column(columnPath)
  }

  def literal(value: Any): Literal = {
    value match {
      case v: String => Literal.ofString(v)
      case v: Int => Literal.ofInt(v)
      case v: Long => Literal.ofLong(v)
      case v: Float => Literal.ofFloat(v)
      case v: Double => Literal.ofDouble(v)
      case v: Boolean => Literal.ofBoolean(v)
      case _ => throw new IllegalArgumentException(s"Unsupported literal type: ${value}")
    }
  }

  implicit class ScalaOptionOps[T](option: Option[T]) {
    def toJava: Optional[T] = option match {
      case Some(value) => Optional.of(value)
      case None => Optional.empty()
    }
  }

  implicit class JavaOptionalOps[T](optional: Optional[T]) {
    def toScala: Option[T] =
      if (optional.isPresent) Some(optional.get()) else None
  }
}
