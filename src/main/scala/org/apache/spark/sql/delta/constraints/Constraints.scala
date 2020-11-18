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

package org.apache.spark.sql.delta.constraints

import java.util.Locale

import org.apache.spark.sql.delta.actions.Metadata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * A constraint defined on a Delta table, which writers must verify before writing.
 */
sealed trait Constraint {
  val name: String
}

/**
 * Utilities for handling constraints. Right now this includes:
 * - Column-level invariants delegated to [[Invariants]], including both NOT NULL constraints and
 *   an old style of CHECK constraint specified in the column metadata
 * - Table-level CHECK constraints
 */
object Constraints {
  /**
   * A constraint that the specified column must not be NULL. Note that when the column is nested,
   * this implies its parents must also not be NULL.
   */
  case class NotNull(column: Seq[String]) extends Constraint {
    override val name: String = "NOT NULL"
  }

  /** A SQL expression to check for when writing out data. */
  case class Check(name: String, expression: Expression) extends Constraint

  /**
   * Extract CHECK constraints from the table properties. Note that some CHECK constraints may also
   * come from schema metadata; these constraints were never released in a public API but are
   * maintained for protocol compatibility.
   */
  def getCheckConstraints(metadata: Metadata, spark: SparkSession): Seq[Constraint] = {
    metadata.configuration.collect {
      case (key, constraintText) if key.toLowerCase(Locale.ROOT).startsWith("delta.constraints.") =>
        val name = key.stripPrefix("delta.constraints.")
        val expression = spark.sessionState.sqlParser.parseExpression(constraintText)
        Check(name, expression)
    }.toSeq
  }

  /** Extract all constraints from the given Delta table metadata. */
  def getAll(metadata: Metadata, spark: SparkSession): Seq[Constraint] = {
    val checkConstraints = getCheckConstraints(metadata, spark)
    val constraintsFromSchema = Invariants.getFromSchema(metadata.schema, spark)

    (checkConstraints ++ constraintsFromSchema).toSeq
  }

  /** Get the expression text for a constraint with the given name, if present. */
  def getExprTextByName(
      name: String,
      metadata: Metadata,
      spark: SparkSession): Option[String] = {
    metadata.configuration.get(checkConstraintPropertyName(name))
  }

  def checkConstraintPropertyName(constraintName: String): String = {
    "delta.constraints." + constraintName.toLowerCase(Locale.ROOT)
  }
}
