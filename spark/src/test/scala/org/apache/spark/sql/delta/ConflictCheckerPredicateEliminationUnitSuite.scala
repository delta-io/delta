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

import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.util.DeltaSparkPlanUtils

import org.apache.spark.sql.{Column, QueryTest}
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Rand, ScalarSubquery}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * A set cheaper unit tests, that behave the same no matter if DVs, CDF, etc. are enabled
 * and do not need to be repeated in each conflict checker suite.
 */
class ConflictCheckerPredicateEliminationUnitSuite
  extends QueryTest
  with SharedSparkSession
  with ConflictCheckerPredicateElimination {

  val simpleExpressionA: Expression = (col("a") === 1).expr
  val simpleExpressionB: Expression = (col("b") === "test").expr

  val deterministicExpression: Expression = (col("c") > 5L).expr
  val nonDeterministicExpression: Expression = (col("c") > new Column(Rand(0))).expr
  lazy val deterministicSubquery: Expression = {
    val df = spark.sql("SELECT 5")
    df.collect()
    col("c").expr > ScalarSubquery(df.queryExecution.analyzed)
  }
  lazy val nonDeterministicSubquery: Expression = {
    val df = spark.sql("SELECT rand()")
    df.collect()
    col("c").expr > ScalarSubquery(df.queryExecution.analyzed)
  }

  private def defaultEliminationFunction(e: Seq[Expression]): PredicateElimination = {
    val options = DeltaSparkPlanUtils.CheckDeterministicOptions(allowDeterministicUdf = false)
    eliminateNonDeterministicPredicates(e, options)
  }

  private def checkEliminationResult(
      predicate: Expression,
      expected: PredicateElimination,
      eliminationFunction: Seq[Expression] => PredicateElimination = defaultEliminationFunction)
  : Unit = {
    require(expected.newPredicates.size === 1)
    val actual = eliminationFunction(Seq(predicate))
    assert(actual.newPredicates.size === 1)
    assert(actual.newPredicates.head.canonicalized == expected.newPredicates.head.canonicalized,
      s"actual=$actual\nexpected=$expected")
    assert(actual.eliminatedPredicates === expected.eliminatedPredicates)
  }

  for {
    deterministic <- BOOLEAN_DOMAIN
    subquery <- BOOLEAN_DOMAIN
  } {
    lazy val exprUnderTest = if (deterministic) {
      if (subquery) deterministicSubquery else deterministicExpression
    } else {
      if (subquery) nonDeterministicSubquery else nonDeterministicExpression
    }

    val testSuffix = s"deterministic $deterministic - subquery $subquery"

    def newPredicates(exprF: Expression => Expression): PredicateElimination = PredicateElimination(
      newPredicates = Seq(exprF(if (deterministic) exprUnderTest else Literal.TrueLiteral)),
      eliminatedPredicates = if (deterministic) Seq.empty else Seq("rand"))

    test(s"and expression - $testSuffix") {
      checkEliminationResult(
        predicate = simpleExpressionA && exprUnderTest,
        expected = newPredicates { eliminatedExprUnderTest =>
          if (deterministic) {
            simpleExpressionA && eliminatedExprUnderTest
          } else {
            simpleExpressionA
          }
        }
      )
    }

    test(s"or expression - $testSuffix") {
      checkEliminationResult(
        predicate = simpleExpressionA || exprUnderTest,
        expected = newPredicates { _ =>
          if (deterministic) {
            simpleExpressionA || exprUnderTest
          } else {
            Literal.TrueLiteral
          }
        }
      )
    }

    test(s"and or expression - $testSuffix") {
      checkEliminationResult(
        predicate = simpleExpressionA && (simpleExpressionB || exprUnderTest),
        expected = newPredicates { _ =>
          if (deterministic) {
            simpleExpressionA && (simpleExpressionB || exprUnderTest)
          } else {
            simpleExpressionA
          }
        }
      )
    }

    test(s"or and expression - $testSuffix") {
      checkEliminationResult(
        predicate = simpleExpressionA || (simpleExpressionB && exprUnderTest),
        expected = newPredicates { _ =>
          if (deterministic) {
            simpleExpressionA || (simpleExpressionB && exprUnderTest)
          } else {
            simpleExpressionA || simpleExpressionB
          }
        }
      )
    }

    test(s"or not and expression - $testSuffix") {
      checkEliminationResult(
        predicate = simpleExpressionA || !(simpleExpressionB && exprUnderTest),
        expected = newPredicates { _ =>
          if (deterministic) {
            simpleExpressionA || !(simpleExpressionB && exprUnderTest)
          } else {
            Literal.TrueLiteral
          }
        }
      )
    }

    test(s"and not or expression - $testSuffix") {
      checkEliminationResult(
        predicate = simpleExpressionA && !(simpleExpressionB || exprUnderTest),
        expected = newPredicates { _ =>
          if (deterministic) {
            simpleExpressionA && !(simpleExpressionB || exprUnderTest)
          } else {
            simpleExpressionA
          }
        })
    }
  }

  test("udf name is not exposed") {
    val random = udf(() => Math.random())
      .asNondeterministic()
      .withName("sensitive_udf_name")
    checkEliminationResult(
      predicate = simpleExpressionA && (col("c") > random()).expr,
      expected = PredicateElimination(
        newPredicates = Seq(simpleExpressionA),
        eliminatedPredicates = Seq("scalaudf")))
  }
}
