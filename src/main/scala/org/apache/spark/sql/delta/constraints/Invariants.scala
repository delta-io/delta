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

import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType

/**
 * List of invariants that can be defined on a Delta table that will allow us to perform
 * validation checks during changes to the table.
 */
object Invariants {
  sealed trait Rule {
    val name: String
  }

  /** Used for columns that should never be null. */
  case object NotNull extends Rule { override val name: String = "NOT NULL" }

  sealed trait RulePersistedInMetadata {
    def wrap: PersistedRule
    def json: String = JsonUtils.toJson(wrap)
  }

  /** Rules that are persisted in the metadata field of a schema. */
  case class PersistedRule(expression: PersistedExpression = null) {
    def unwrap: RulePersistedInMetadata = {
      if (expression != null) {
        expression
      } else {
        null
      }
    }
  }

  /** A SQL expression to check for when writing out data. */
  case class ArbitraryExpression(expression: Expression) extends Rule {
    override val name: String = s"EXPRESSION($expression)"
  }

  object ArbitraryExpression {
    def apply(sparkSession: SparkSession, exprString: String): ArbitraryExpression = {
      val expr = sparkSession.sessionState.sqlParser.parseExpression(exprString)
      ArbitraryExpression(expr)
    }
  }

  /** Persisted companion of the ArbitraryExpression rule. */
  case class PersistedExpression(expression: String) extends RulePersistedInMetadata {
    override def wrap: PersistedRule = PersistedRule(expression = this)
  }

  /** Extract invariants from the given schema */
  def getFromSchema(schema: StructType, spark: SparkSession): Seq[Constraint] = {
    val columns = SchemaUtils.filterRecursively(schema, checkComplexTypes = false) { field =>
      !field.nullable || field.metadata.contains(INVARIANTS_FIELD)
    }
    columns.map {
      case (parents, field) if !field.nullable =>
        Constraints.NotNull(parents :+ field.name)
      case (parents, field) =>
        val rule = field.metadata.getString(INVARIANTS_FIELD)
        val invariant = Option(JsonUtils.mapper.readValue[PersistedRule](rule).unwrap) match {
          case Some(PersistedExpression(exprString)) =>
            ArbitraryExpression(spark, exprString)
          case _ =>
            throw new UnsupportedOperationException(
              "Unrecognized invariant. Please upgrade your Spark version.")
        }
        Constraints.Check(invariant.name, invariant.expression)
    }
  }

  val INVARIANTS_FIELD = "delta.invariants"
}

/** A rule applied on a column to ensure data hygiene. */
case class Invariant(column: Seq[String], rule: Invariants.Rule)
