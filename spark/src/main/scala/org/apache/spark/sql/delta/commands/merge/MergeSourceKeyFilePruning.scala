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

package org.apache.spark.sql.delta.commands.merge

import org.apache.spark.sql.delta.stats.{SkippingEligibleColumn, SkippingEligibleDataType}

import org.apache.spark.sql.catalyst.expressions.{AttributeSet, EqualTo, Expression, InSet, Literal, PredicateHelper}
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, StringType}

/**
 * Helpers to prune the target files of a MERGE by the join key values of a small source.
 *
 * For each conjunct `target_column = source_expression` of the ON condition, a target row can only
 * match a source row if its `target_column` equals one of the source's values of
 * `source_expression`. When the source is small, MERGE collects those values and adds
 * `target_column IN (v1, v2, ...)` to the target-only predicates it uses to select the target
 * files. Data skipping then drops the files whose min/max statistics (or partition values)
 * exclude every value (see `skipping.inList.exactMaxValues`). The predicate only selects files:
 * the join still uses the ON condition, so the result of the MERGE does not change.
 */
object MergeSourceKeyFilePruning extends PredicateHelper {

  /**
   * Returns the `(target, source)` expression pairs of the `target = source` conjuncts of
   * `condition` that can prune files: the target side is a skipping-eligible column of the target,
   * the source side is a deterministic expression over the source, and both sides have the same
   * type, whose equality is the same as the equality used by data skipping. This excludes
   * FLOAT/DOUBLE (`-0.0 = 0.0`, NaN) and strings with a non-binary collation. Null-safe equality
   * is not matched, because a NULL source key would match NULL target keys.
   */
  def equiJoinKeys(
      condition: Expression,
      targetOutput: AttributeSet,
      sourceOutput: AttributeSet): Seq[(Expression, Expression)] = {
    def keyPair(t: Expression, s: Expression): Option[(Expression, Expression)] = {
      val isTargetColumn = SkippingEligibleColumn.unapply(t).isDefined &&
        t.references.nonEmpty && t.references.subsetOf(targetOutput)
      val isSourceExpression = s.deterministic &&
        s.references.nonEmpty && s.references.subsetOf(sourceOutput)
      if (isTargetColumn && isSourceExpression && t.dataType == s.dataType &&
          hasBinaryEquality(t.dataType)) {
        Some((t, s))
      } else {
        None
      }
    }
    splitConjunctivePredicates(condition).flatMap {
      case EqualTo(l, r) => keyPair(l, r).orElse(keyPair(r, l))
      case _ => None
    }
  }

  /**
   * Returns a predicate that is true when `targetKey` is one of `values` (Catalyst values of
   * `targetKey.dataType`, without nulls), or false when there are no values: then no target row
   * can match, since a NULL key never satisfies `=`.
   */
  def keyIn(targetKey: Expression, values: Seq[Any]): Expression =
    if (values.isEmpty) Literal.FalseLiteral else InSet(targetKey, values.toSet)

  private def hasBinaryEquality(dataType: DataType): Boolean = dataType match {
    case _: FloatType | _: DoubleType => false
    case st: StringType => st.supportsBinaryOrdering
    case dt => SkippingEligibleDataType(dt)
  }
}
