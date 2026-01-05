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

package org.apache.spark.sql.delta.constraints

import java.util.Locale

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{AllowedUserProvidedExpressions, DeltaErrors, DeltaLog}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf.ValidateCheckConstraintsMode

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, GetArrayItem, GetMapValue, GetStructField, IsNotNull, UserDefinedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.types.{BooleanType, StructType}

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
object Constraints extends DeltaLogging {
  /**
   * A constraint that the specified column must not be NULL. Note that when the column is nested,
   * this implies its parents must also not be NULL.
   */
  case class NotNull(column: Seq[String]) extends Constraint {
    override val name: String = "NOT NULL"
  }

  /** A SQL expression to check for when writing out data. */
  case class Check(name: String, expression: Expression) extends Constraint

  def getCheckConstraintNames(metadata: Metadata): Seq[String] = {
    metadata.configuration.keys.collect {
      case key if key.toLowerCase(Locale.ROOT).startsWith("delta.constraints.") =>
        key.stripPrefix("delta.constraints.")
    }.toSeq
  }

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
    val charVarcharLengthChecks = if (spark.sessionState.conf.charVarcharAsString) {
      Nil
    } else {
      CharVarcharConstraint.stringConstraints(metadata.schema)
    }

    (checkConstraints ++ constraintsFromSchema ++ charVarcharLengthChecks).toSeq
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

  /**
   * Find all the check constraints that reference the given column name. Returns a map of
   * constraint names to their corresponding expression.
   */
  def findDependentConstraints(
      sparkSession: SparkSession,
      columnName: Seq[String],
      metadata: Metadata): Map[String, String] = {
    metadata.configuration.filter {
      case (key, constraint) if key.toLowerCase(Locale.ROOT).startsWith("delta.constraints.") =>
        SchemaUtils.containsDependentExpression(
          sparkSession,
          columnName,
          constraint,
          metadata.schema,
          sparkSession.sessionState.conf.resolver)
      case _ => false
    }
  }

  /**
   * Validates check constraints with rollout logic for safe deployment.
   * This wrapper handles feature flag checks, error logging, and mode-based error handling.
   */
  def validateCheckConstraints(
      spark: SparkSession,
      constraints: Seq[Constraint],
      deltaLog: DeltaLog,
      schema: StructType): Unit = {
    val validateCheckConstraints = ValidateCheckConstraintsMode.fromConf(spark.sessionState.conf)
    if (validateCheckConstraints == ValidateCheckConstraintsMode.OFF) return
    if (constraints.isEmpty) return

    try {
      validateCheckConstraintsInternal(spark, constraints, schema)
    } catch {
      case NonFatal(e) =>
        val errorClassName = e match {
          case sparkEx: SparkThrowable => sparkEx.getErrorClass
          case _ => e.getClass
        }
        recordDeltaEvent(
          deltaLog,
          "delta.checkConstraints.validationFailure",
          data = Map(
            "errorClassName" -> errorClassName,
            "errorMessage" -> e.getMessage
          )
        )
        if (validateCheckConstraints == ValidateCheckConstraintsMode.ASSERT) {
          throw e
        }
    }
  }

  /**
   * Internal validation logic for check constraints.
   */
  private def validateCheckConstraintsInternal(
      spark: SparkSession,
      constraints: Seq[Constraint],
      schema: StructType): Unit = {
    // Create NamedExpressions for analysis (type checking will happen after resolution)
    // We unresolve expressions to validate against the `LocalRelation` we later create
    val selectExprs = constraints.map {
      case Check(name, expression) =>
        Alias(expression, name)()
      case NotNull(columnPath) =>
        // Create an IsNotNull expression to validate the column exists
        val columnRef = UnresolvedAttribute(columnPath)
        val isNotNullExpr = IsNotNull(columnRef)
        Alias(isNotNullExpr, s"NOT NULL ${columnPath.mkString(".")}")()
    }

    // Analyze all constraint expressions to ensure they can be properly resolved
    // Use LocalRelation with the table schema to ensure column references can be validated
    val analyzed = try {
      val analyzer = spark.sessionState.analyzer
      val relation = LocalRelation(DataTypeUtils.toAttributes(schema))
      val plan = analyzer.execute(Project(selectExprs, relation))
      analyzer.checkAnalysis(plan)
      plan
    } catch {
      case e: SparkThrowable
        if e.getErrorClass != null && e.getErrorClass.startsWith("UNRESOLVED_COLUMN") =>
          // Check if this is an unresolved column/field error by examining the error class
          throw DeltaErrors.checkConstraintReferToWrongColumns(
            e.getMessageParameters.getOrDefault("objectName", "")
          )
      case e => throw e
    }

    // Check that all Check constraints return boolean type
    analyzed match {
      case Project(projectList, _) =>
        projectList.foreach {
          case a: Alias =>
            if (a.dataType != BooleanType) {
              throw DeltaErrors.checkConstraintNotBoolean(a.name, a.child.sql)
            }
          case _ => // We should only the Aliases we previously created.
        }
      case _ => // We should only have a single projection
    }

    // Validate the analyzed expressions
    analyzed.transformAllExpressions {
      case expr: Alias =>
        // Alias will be non deterministic if it points to a non deterministic expression.
        // Skip `Alias` to provide a better error for a non deterministic expression.
        expr
      case expr@(_: GetStructField | _: GetArrayItem | _: GetMapValue) =>
        // The complex type extractors don't have a function name, so we need to check them
        // separately. Unlike generated columns we do allow `GetMapValue`.
        expr
      case expr: UserDefinedExpression =>
        throw DeltaErrors.checkConstraintUDF(expr)
      case expr if !expr.deterministic =>
        throw DeltaErrors.checkConstraintNonDeterministicExpression(expr)
      case expr if expr.isInstanceOf[AggregateExpression] =>
        throw DeltaErrors.checkConstraintAggregateExpression(expr)
      case expr if
        !AllowedUserProvidedExpressions.expressions.contains(expr.getClass) &&
          !AllowedUserProvidedExpressions
            .checkConstraintExpressions.contains(expr.getClass) =>
        throw DeltaErrors.checkConstraintUnsupportedExpression(expr)
    }
  }
}
