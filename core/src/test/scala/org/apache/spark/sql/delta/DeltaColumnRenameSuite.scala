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

import org.scalatest.GivenWhenThen

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}

class DeltaColumnRenameSuite extends QueryTest
  with DeltaArbitraryColumnNameSuiteBase
  with GivenWhenThen {

  testColumnMapping("rename in column mapping mode") { mode =>
    withTable("t1") {
      createTableWithSQLAPI("t1",
        simpleNestedData,
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode),
        partCols = Seq("a"))
      spark.sql(s"Alter table t1 RENAME COLUMN b to b1")
      // insert data after rename
      spark.sql("insert into t1 " +
        "values ('str3', struct('str1.3', 3), map('k3', 'v3'), array(3, 33))")

      // some queries
      checkAnswer(
        spark.table("t1"),
        Seq(
          Row("str1", Row("str1.1", 1), Map("k1" -> "v1"), Array(1, 11)),
          Row("str2", Row("str1.2", 2), Map("k2" -> "v2"), Array(2, 22)),
          Row("str3", Row("str1.3", 3), Map("k3" -> "v3"), Array(3, 33))))

      checkAnswer(
        spark.table("t1").select("b1"),
        Seq(Row(Row("str1.1", 1)), Row(Row("str1.2", 2)), Row(Row("str1.3", 3))))

      checkAnswer(
        spark.table("t1").select("a", "b1.c").where("b1.c = 'str1.2'"),
        Seq(Row("str2", "str1.2")))

      // b is no longer visible
      val e = intercept[AnalysisException] {
        spark.table("t1").select("b").collect()
      }
      assert(e.getErrorClass == "MISSING_COLUMN")

      // rename partition column
      spark.sql(s"Alter table t1 RENAME COLUMN a to a1")
      // rename nested column
      spark.sql(s"Alter table t1 RENAME COLUMN b1.c to c1")

      // a is no longer visible
      val e2 = intercept[AnalysisException] {
        spark.table("t1").select("a").collect()
      }
      assert(e2.getErrorClass == "MISSING_COLUMN")

      // b1.c is no longer visible
      val e3 = intercept[AnalysisException] {
        spark.table("t1").select("b1.c").collect()
      }
      assert(e3.getMessage.contains("No such struct field"))

      // insert data after rename
      spark.sql("insert into t1 " +
        "values ('str4', struct('str1.4', 4), map('k4', 'v4'), array(4, 44))")

      checkAnswer(
        spark.table("t1").select("a1", "b1.c1", "map")
          .where("b1.c1 = 'str1.4'"),
        Seq(Row("str4", "str1.4", Map("k4" -> "v4"))))
    }
  }

  test("rename workflow: error, upgrade to name mode and then rename") {
    // error when not in the correct protocol and mode
    withTable("t1") {
      createTableWithSQLAPI("t1",
        simpleNestedData,
        partCols = Seq("a"))
       val e = intercept[AnalysisException] {
        spark.sql(s"Alter table t1 RENAME COLUMN map to map1")
       }
      assert(e.getMessage.contains("upgrade your Delta table") &&
        e.getMessage.contains("change the column mapping mode"))

      alterTableWithProps("t1", Map(
        DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name",
        DeltaConfigs.MIN_READER_VERSION.key -> "2",
        DeltaConfigs.MIN_WRITER_VERSION.key -> "5"))

      // spice things up by changing name to arbitrary chars
      spark.sql(s"Alter table t1 RENAME COLUMN a to `${colName("a")}`")
      // rename partition column
      spark.sql(s"Alter table t1 RENAME COLUMN map to `${colName("map")}`")

      // insert data after rename
      spark.sql("insert into t1 " +
        "values ('str3', struct('str1.3', 3), map('k3', 'v3'), array(3, 33))")

      checkAnswer(
        spark.table("t1").select(colName("a"), "b.d", colName("map"))
          .where("b.c >= 'str1.2'"),
        Seq(Row("str2", 2, Map("k2" -> "v2")),
          Row("str3", 3, Map("k3" -> "v3"))))

      // add old column back?
      spark.sql(s"alter table t1 add columns (a string, map map<string, string>)")

      // insert data after rename
      spark.sql("insert into t1 " +
        "values ('str4', struct('str1.4', 4), map('k4', 'v4'), array(4, 44)," +
        " 'new_str4', map('new_k4', 'new_v4'))")

      checkAnswer(
        spark.table("t1").select(colName("a"), "a", colName("map"), "map")
          .where("b.c >= 'str1.2'"),
        Seq(
          Row("str2", null, Map("k2" -> "v2"), null),
          Row("str3", null, Map("k3" -> "v3"), null),
          Row("str4", "new_str4", Map("k4" -> "v4"), Map("new_k4" -> "new_v4"))))
    }
  }

  test("rename workflow: error, upgrade to name mode and then rename - " +
    "nested data with duplicated column name") {
    withTable("t1") {
      createTableWithSQLAPI("t1", simpleNestedDataWithDuplicatedNestedColumnName)
       val e = intercept[AnalysisException] {
        spark.sql(s"Alter table t1 RENAME COLUMN map to map1")
       }
      assert(e.getMessage.contains("upgrade your Delta table") &&
        e.getMessage.contains("change the column mapping mode"))

      // Upgrading this schema shouldn't cause any errors even if there are leaf column name
      // duplications such as a.c, b.c.
      alterTableWithProps("t1", Map(
        DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name",
        DeltaConfigs.MIN_READER_VERSION.key -> "2",
        DeltaConfigs.MIN_WRITER_VERSION.key -> "5"))

      // rename shouldn't cause duplicates in column names
      Seq(("a", "b"), ("arr", "map")).foreach { case (from, to) =>
        val e = intercept[AnalysisException] {
          spark.sql(s"Alter table t1 RENAME COLUMN $from to $to")
        }
        assert(e.getMessage.contains("Cannot rename column"))
      }

      // spice things up by changing name to arbitrary chars
      spark.sql(s"Alter table t1 RENAME COLUMN a to `${colName("a")}`")
      // rename partition column
      spark.sql(s"Alter table t1 RENAME COLUMN map to `${colName("map")}`")

      // insert data after rename
      spark.sql("insert into t1 " +
        "values (struct('str3', 3), struct('str1.3', 3), map('k3', 'v3'), array(3, 33))")

      checkAnswer(
        spark.table("t1").select(colName("a"), "b.d", colName("map"))
          .where("b.c >= 'str1.2'"),
        Seq(Row(Row("str2", 2), 2, Map("k2" -> "v2")),
          Row(Row("str3", 3), 3, Map("k3" -> "v3"))))

      // add old column back?
      spark.sql(s"alter table t1 add columns (a string, map map<string, string>)")

      // insert data after rename
      spark.sql("insert into t1 " +
        "values (struct('str4', 4), struct('str1.4', 4), map('k4', 'v4'), array(4, 44)," +
        " 'new_str4', map('new_k4', 'new_v4'))")

      checkAnswer(
        spark.table("t1").select(colName("a"), "a", colName("map"), "map")
          .where("b.c >= 'str1.2'"),
        Seq(
          Row(Row("str2", 2), null, Map("k2" -> "v2"), null),
          Row(Row("str3", 3), null, Map("k3" -> "v3"), null),
          Row(Row("str4", 4), "new_str4", Map("k4" -> "v4"), Map("new_k4" -> "new_v4"))))
    }
  }

}
