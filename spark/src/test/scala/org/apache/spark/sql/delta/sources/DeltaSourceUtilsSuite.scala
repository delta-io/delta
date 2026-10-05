/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.sources

import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.V2ExpressionBuilder
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{BooleanType, IntegerType}

class DeltaSourceUtilsSuite extends SparkFunSuite {
  private val b = AttributeReference("b", BooleanType)()
  private val id = AttributeReference("id", IntegerType)()

  private val cases: Seq[(String, Seq[Filter], Seq[Filter])] = Seq(
    ("b <=> true", Seq(EqualNullSafe("b", true)), Seq.empty),
    ("true <=> b", Seq(EqualNullSafe("b", true)), Seq.empty),
    ("b <=> false", Seq(EqualNullSafe("b", false)), Seq.empty),
    ("NOT (b <=> true)", Seq(Not(EqualNullSafe("b", true))), Seq.empty),
    ("b IN (true, false)", Seq(In("b", Array[Any](true, false))), Seq.empty),
    ("b <=> true OR id = 4",
      Seq(Or(EqualNullSafe("b", true), EqualTo("id", 4))),
      Seq(EqualTo("id", 4))),
    ("b <=> true AND id = 4",
      Seq(And(EqualNullSafe("b", true), EqualTo("id", 4))),
      Seq.empty),
    ("true", Seq(AlwaysTrue()), Seq(AlwaysTrue())),
    ("false", Seq(AlwaysFalse()), Seq(AlwaysFalse())),
    ("id = 4", Seq(EqualTo("id", 4)), Seq(EqualTo("id", 4))))

  test("boolean literal preservation defaults to the test environment") {
    assert(
      DeltaSQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.defaultValue
        .contains(DeltaUtils.isTesting))
  }

  for {
    enabled <- Seq(false, true)
    (condition, fixed, legacy) <- cases
  } {
    test(s"translate V2 boolean predicates: $condition, enabled=$enabled") {
      val conf = new SQLConf
      conf.setConf(DeltaSQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED, enabled)
      SQLConf.withExistingConf(conf) {
        val expression = CatalystSqlParser.parseExpression(condition).transform {
          case attribute: UnresolvedAttribute if attribute.name == "b" => b
          case attribute: UnresolvedAttribute if attribute.name == "id" => id
        }
        val predicate = new V2ExpressionBuilder(expression, isPredicate = true)
          .buildPredicate().get
        val filters = DeltaSourceUtils.translateV2Predicates(Array(predicate))
        assert(filters.toSeq == (if (enabled) fixed else legacy))
      }
    }
  }
}
