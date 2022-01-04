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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.files.{PinnedTahoeFileIndex, TahoeLogFileIndex}


/**
 * This rule tries to find if we attempt to read the same table when there is an active transaction.
 * If so, reading the same table should use the same snapshot of the transaction, and the
 * transaction should also record the filters that are used to read the table.
 *
 */
class HiddenPartitioningRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {

    // We need to first prepare the scans in the subqueries of a node. Otherwise, because of the
    // short-circuiting nature of the pattern matching in the transform method, if a
    // PhysicalOperation node is matched, its subqueries that may contain other PhysicalOperation
    // nodes will be skipped.
    def transformSubqueries(plan: LogicalPlan): LogicalPlan =
      plan transformAllExpressions {
        case subquery: SubqueryExpression =>
          subquery.withNewPlan(transform(subquery.plan))
      }

    def transform(plan: LogicalPlan): LogicalPlan =
      transformSubqueries(plan) transform {
        case p @ PhysicalOperation(_, filters, DeltaTable(fileIndex: TahoeLogFileIndex))
            if fileIndex.deltaLog.snapshot.hiddenPartitioningColumns.nonEmpty &&
            filters.nonEmpty =>

          val hiddenPartitioningColumns = fileIndex.deltaLog.snapshot.hiddenPartitioningColumns
          val generatedColumnSources = hiddenPartitioningColumns.flatMap { field =>
            GeneratedColumn.getGenerationExpression(field).collect {
              case UnresolvedAttribute(nameParts) =>
                nameParts -> HiddenPartition(field.name, false, value => value)
              case e @ Cast(UnresolvedAttribute(nameParts), _, _, _) =>
                nameParts -> HiddenPartition(field.name, true, value => e.copy(child = value))
            }
          }.toMap

          val resolver = spark.sessionState.conf.resolver

          val hiddenPartitioningFilters = filters.flatMap(_ match {
            case EqualTo(left, right) =>
              extractNameAndValue(left, right).flatMap { case (name, value) =>
                generatedColumnSources.get(name).flatMap { hiddenPartition =>
                  p.resolve(Seq(hiddenPartition.fieldName), resolver).map { attr =>
                    EqualTo(attr, hiddenPartition.generator(value))
                  }
                }
              }
            case LessThanOrEqual(left, right) =>
              extractNameAndValue(left, right).flatMap { case (name, value) =>
                generatedColumnSources.get(name).flatMap { hiddenPartition =>
                  p.resolve(Seq(hiddenPartition.fieldName), resolver).map { attr =>
                    LessThanOrEqual(attr, hiddenPartition.generator(value))
                  }
                }
              }
            case LessThan(left, right) =>
              extractNameAndValue(left, right).flatMap { case (name, value) =>
                generatedColumnSources.get(name).flatMap { hiddenPartition =>
                  p.resolve(Seq(hiddenPartition.fieldName), resolver).map { attr =>
                    if (hiddenPartition.lostPrecision) {
                      LessThanOrEqual(attr, hiddenPartition.generator(value))
                    } else {
                      LessThan(attr, hiddenPartition.generator(value))
                    }
                  }
                }
              }
            case GreaterThanOrEqual(left, right) =>
              extractNameAndValue(left, right).flatMap { case (name, value) =>
                generatedColumnSources.get(name).flatMap { hiddenPartition =>
                  p.resolve(Seq(hiddenPartition.fieldName), resolver).map { attr =>
                    GreaterThanOrEqual(attr, hiddenPartition.generator(value))
                  }
                }
              }
            case GreaterThan(left, right) =>
              extractNameAndValue(left, right).flatMap { case (name, value) =>
                generatedColumnSources.get(name).flatMap { hiddenPartition =>
                  p.resolve(Seq(hiddenPartition.fieldName), resolver).map { attr =>
                    if (hiddenPartition.lostPrecision) {
                      GreaterThanOrEqual(attr, hiddenPartition.generator(value))
                    } else {
                      GreaterThan(attr, hiddenPartition.generator(value))
                    }
                  }
                }
              }
            case _ => None
          })

          var newFilters = hiddenPartitioningFilters.filter(!filters.contains(_))
          if (newFilters.nonEmpty) {
            p.transformUp {
              case d @ DeltaTable(_: TahoeLogFileIndex) =>
                Filter(newFilters.reduce(And), d)
            }
          } else {
            p
          }
      }

    transform(plan)
  }

  private def getNestedFieldName(expr: Expression): Seq[String] = {
    expr match {
      case g: GetStructField =>
        getNestedFieldName(g.child).toSeq ++ Seq(g.extractFieldName)
      case a: AttributeReference =>
        Seq(a.name)
      case _ =>
        throw new Exception("Bad path")
    }
  }

  private def extractNameAndValue(left: Expression, right: Expression
      ): Option[(Seq[String], Expression)] = {
    (left, right) match {
      case (attr: AttributeReference, value: Literal) => Some((Seq(attr.name), value))
      case (attr: GetStructField, value: Literal) =>
        Some((getNestedFieldName(attr), value))
      case (value: Literal, attr: AttributeReference) => Some((Seq(attr.name), value))
      case (value: Literal, attr: GetStructField) =>
        Some((getNestedFieldName(attr), value))
      case _ => None
    }
  }
}

case class HiddenPartition(fieldName: String, lostPrecision: Boolean,
    generator: Expression => Expression)
