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

import org.apache.spark.sql.delta.DeltaLog

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.types.StringType

class DataSkippingDeltaConstructDataFiltersSuite
    extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {
  test("Verify constructDataFilters doesn't hang for expressions with Literal operands.") {
    val snapshot = DeltaLog.forTable(spark, "dummy_path").update()
    val dataFilterBuilder = new snapshot.DataFiltersBuilder(
      spark, DeltaDataSkippingType.dataSkippingOnlyV1)

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
      assert(dataFilterBuilder.constructDataFilters(expression).isEmpty)
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
