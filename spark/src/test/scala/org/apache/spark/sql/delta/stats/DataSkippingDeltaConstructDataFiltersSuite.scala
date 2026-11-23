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

package org.apache.spark.sql.delta.stats


import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.types.StringType

class DataSkippingDeltaConstructDataFiltersSuite
    extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {
  test("Verify constructDataFilters doesn't hang for expressions with Literal operands.") {
    val dataFilterBuilder = new DataFiltersBuilder(
      spark,
      DeltaDataSkippingType.dataSkippingOnlyV1,
      getStatsColumnOpt = _ => None)

    val literal = Literal.create("foo", StringType)
    Seq(
      EqualTo(literal, literal),
      Not(EqualTo(literal, literal)),
      EqualNullSafe(literal, literal),
      Not(EqualNullSafe(literal, literal)),
      LessThan(literal, literal),
      LessThanOrEqual(literal, literal),
      GreaterThan(literal, literal),
      GreaterThanOrEqual(literal, literal),
      Not(GreaterThanOrEqual(literal, literal)),
      In(literal, Seq(literal)),
      IsNull(literal),
      IsNotNull(literal),
      And(EqualTo(literal, literal), LessThan(literal, literal))
    ).foreach { expression =>
      assert(dataFilterBuilder.constructDataFilters(expression, isNullExpansionDepth = 0).isEmpty)
    }
  }

  test("Test when the query contains EqualTo(Literal, Literal) in the filter.") {
    setup {
      sql(
        """
          |explain
          |select
          | *
          |from
          |  view1 c
          |  join view2 cv on c.type=cv.type and c.key=cv.key
          |  join tbl3 b on cv.name=b.name
          |where
          |  (
          |       (b.name="name1" and c.type="foo")
          |       or
          |       (b.name="name2" and c.type="bar")
          |  )
          |""".stripMargin)
    }
  }

  test("Verify areAllLeavesLiteral can't be recursively called b/c it can cause stack overflow") {
    // Create a deeply nested Alias expression: Alias(Alias(Alias(...), "name3"), "name2"), "name1")
    val depth = 100000 // Deep enough to overflow the 256 KB stack of onSmallStack
    var nestedExpr: Expression = Literal.create("foo", StringType)
    for (i <- 1 to depth) {
      nestedExpr = Alias(nestedExpr, s"name$i")()
    }

    // Runs f on a thread with a fixed 256 KB stack, so the result does not depend on the
    // stack size of the test thread or on how the JIT compiles the recursion.
    def onSmallStack[T](f: => T): T = {
      var result: Option[T] = None
      var error: Throwable = null
      val thread = new Thread(
        null,
        () => try { result = Some(f) } catch { case t: Throwable => error = t },
        "areAllLeavesLiteral-small-stack",
        256 * 1024)
      thread.start()
      thread.join()
      if (error != null) throw error
      result.get
    }

    assert(onSmallStack(DataFiltersBuilder.areAllLeavesLiteral(nestedExpr)))

    // This should cause a StackOverflowError when areAllLeavesLiteral is called recursively
    def areAllLeavesLiteral(e: Expression): Boolean = e match {
      case _: Literal => true
      case _ if e.children.nonEmpty => e.children.forall(areAllLeavesLiteral)
      case _ => false
    }

    intercept[StackOverflowError] {
      onSmallStack(areAllLeavesLiteral(nestedExpr))
    }
  }

  private def setup(f: => Unit) {
    withTable("tbl1_foo", "tbl1_bar", "tbl2_foo", "tbl2_bar", "tbl3") {
      Seq("foo", "bar").foreach { tableType =>
        sql(s"CREATE TABLE tbl1_$tableType (key STRING) USING delta")
        sql(s"CREATE TABLE tbl2_$tableType (key STRING, name STRING) USING delta")
      }
      sql("CREATE TABLE tbl3 (name STRING) USING delta")

      withView("view1", "view2") {
        sql(
          s"""
             |CREATE VIEW view1 (type, key)
             |AS (
             |    select 'foo' as type, * from tbl1_foo
             |    union all
             |    select 'bar' as type, * from tbl1_bar
             |)
             |""".stripMargin
        )

        sql(
          s"""
             |CREATE VIEW view2 (type, key, name)
             |AS (
             |    select 'foo' as type, * from tbl2_foo
             |    union all
             |    select 'bar' as type, * from tbl2_bar
             |)
             |""".stripMargin
        )

        f
      }
    }
  }
}
