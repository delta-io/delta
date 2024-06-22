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

package org.apache.spark.sql.delta.implicits

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.{RuleId, UnknownRuleId}
import org.apache.spark.sql.catalyst.trees.{AlwaysProcess, TreePatternBits}
import org.apache.spark.sql.delta.util.DeltaEncoders
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}

trait RichSparkClasses {

  /**
   * This implicit class is used to provide helpful methods used throughout the code that are not
   * provided by Spark-Catalyst's StructType.
   */
  implicit class RichStructType(structType: StructType) {

    /**
     * Returns a field in this struct and its child structs, case insensitively.
     *
     * If includeCollections is true, this will return fields that are nested in maps and arrays.
     *
     * @param fieldNames The path to the field, in order from the root. For example, the column
     *                   nested.a.b.c would be Seq("nested", "a", "b", "c").
     */
    def findNestedFieldIgnoreCase(
        fieldNames: Seq[String],
        includeCollections: Boolean = false): Option[StructField] = {
      val fieldOption = fieldNames.headOption.flatMap {
        fieldName => structType.find(_.name.equalsIgnoreCase(fieldName))
      }
      fieldOption match {
        case Some(field) =>
          (fieldNames.tail, field.dataType, includeCollections) match {
            case (Seq(), _, _) =>
              Some(field)

            case (names, struct: StructType, _) =>
              struct.findNestedFieldIgnoreCase(names, includeCollections)

            case (_, _, false) =>
              None // types nested in maps and arrays are not used

            case (Seq("key"), MapType(keyType, _, _), true) =>
              // return the key type as a struct field to include nullability
              Some(StructField("key", keyType, nullable = false))

            case (Seq("key", names @ _*), MapType(struct: StructType, _, _), true) =>
              struct.findNestedFieldIgnoreCase(names, includeCollections)

            case (Seq("value"), MapType(_, valueType, isNullable), true) =>
              // return the value type as a struct field to include nullability
              Some(StructField("value", valueType, nullable = isNullable))

            case (Seq("value", names @ _*), MapType(_, struct: StructType, _), true) =>
              struct.findNestedFieldIgnoreCase(names, includeCollections)

            case (Seq("element"), ArrayType(elementType, isNullable), true) =>
              // return the element type as a struct field to include nullability
              Some(StructField("element", elementType, nullable = isNullable))

            case (Seq("element", names @ _*), ArrayType(struct: StructType, _), true) =>
              struct.findNestedFieldIgnoreCase(names, includeCollections)

            case _ =>
              None
          }
        case _ =>
          None
      }
    }
  }

  /**
   * This implicit class is used to provide helpful methods used throughout the code that are not
   * provided by Spark-Catalyst's LogicalPlan.
   */
  implicit class RichLogicalPlan(plan: LogicalPlan) {
    /**
     * Returns the result of running QueryPlan.transformExpressionsUpWithPruning on this node
     * and all its children.
     */
    def transformAllExpressionsUpWithPruning(
        cond: TreePatternBits => Boolean,
        ruleId: RuleId = UnknownRuleId)(
        rule: PartialFunction[Expression, Expression]
      ): LogicalPlan = {
      plan.transformUpWithPruning(cond, ruleId) {
        case q: QueryPlan[_] =>
          q.transformExpressionsUpWithPruning(cond, ruleId)(rule)
      }
    }

    /**
     * Returns the result of running QueryPlan.transformExpressionsUp on this node
     * and all its children.
     */
    def transformAllExpressionsUp(
        rule: PartialFunction[Expression, Expression]): LogicalPlan = {
      transformAllExpressionsUpWithPruning(AlwaysProcess.fn, UnknownRuleId)(rule)
    }
  }
}
