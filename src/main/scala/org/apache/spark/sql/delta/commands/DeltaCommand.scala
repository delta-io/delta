/*
 * Copyright 2019 Databricks, Inc.
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

package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.parser.ParseException

/**
 * Helper trait for all delta commands.
 */
trait DeltaCommand extends DeltaLogging {
  /**
   * Converts string predicates into [[Expression]]s relative to a transaction.
   *
   * @throws AnalysisException if a non-partition column is referenced.
   */
  protected def parsePartitionPredicates(
      spark: SparkSession,
      predicate: String): Seq[Expression] = {
    try {
      spark.sessionState.sqlParser.parseExpression(predicate) :: Nil
    } catch {
      case e: ParseException =>
        throw new AnalysisException(s"Cannot recognize the predicate '$predicate'", cause = Some(e))
    }
  }

  protected def verifyPartitionPredicates(
      spark: SparkSession,
      partitionColumns: Seq[String],
      predicates: Seq[Expression]): Unit = {

    predicates.foreach { pred =>
      if (SubqueryExpression.hasSubquery(pred)) {
        throw new AnalysisException("Subquery is not supported in partition predicates.")
      }

      pred.references.foreach { col =>
        val nameEquality = spark.sessionState.conf.resolver
        partitionColumns.find(f => nameEquality(f, col.name)).getOrElse {
          throw new AnalysisException(
            s"Predicate references non-partition column '${col.name}'. " +
              "Only the partition columns may be referenced: " +
              s"[${partitionColumns.mkString(", ")}]")
        }
      }
    }
  }
}
