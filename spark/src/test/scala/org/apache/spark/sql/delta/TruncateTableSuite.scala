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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.test.SharedSparkSession

class TruncateTableSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaTestUtilsForTempViews {


  test("non-partitioned table") {
    withTable("t1") {
      sql("CREATE TABLE t1 (a int) USING delta")
      sql("INSERT into t1 VALUES (3)")
      sql("INSERT into t1 VALUES (4)")

      sql("TRUNCATE table t1")
      checkAnswer(sql("select * from t1"), Nil)

      val ae = intercept[AnalysisException] {
        sql("TRUNCATE table t1 partition (a = 3)")
      }
      assert(ae.message.contains("Operation not allowed"))
    }
  }

  test("partitioned table") {
    withTable("t1") {
      sql("CREATE TABLE t1 (a int, b int) USING delta partitioned by (b)")
      sql("INSERT into t1 VALUES (1, 1)")
      sql("INSERT into t1 VALUES (2, 2)")

      sql("TRUNCATE table t1")
      checkAnswer(sql("select * from t1"), Nil)

      val ae = intercept[AnalysisException] {
        sql("TRUNCATE table t1 partition (b = 1)")
      }
      assert(ae.message.contains("Operation not allowed"))
    }
  }

  test("path") {
    withTempDir { dir =>
      import testImplicits._
      val path = dir.getCanonicalPath
      Seq((1, 2)).toDF("key", "value")
        .write.format("delta").partitionBy("key").save(path)
      sql(s"TRUNCATE TABLE delta.`$path`")
      checkAnswer(sql(s"select * from delta.`$path`"), Nil)
      val ae = intercept[AnalysisException] {
        sql(s"TRUNCATE TABLE delta.`$path` partition (key = 1)")
      }
      assert(ae.message.contains("Operation not allowed"))
    }
  }

  test("truncate should refresh DF cache - table") {
    withTable("t1") {
      sql("CREATE TABLE t1 (a int, b int) USING delta partitioned by (b)")
      sql("INSERT into t1 VALUES (1, 1)")
      sql("INSERT into t1 VALUES (2, 2)")

      val df = spark.table("t1").cache()
      sql("TRUNCATE table t1")
      checkAnswer(df, Nil)
      checkAnswer(spark.table("t1"), Nil)
    }
  }

  test("truncate should refresh DF cache - path") {
    withTempDir { dir =>
      import testImplicits._
      val path = dir.getCanonicalPath
      Seq((1, 2)).toDF("key", "value")
        .write.format("delta").partitionBy("key").save(path)
      val df = sql(s"select * from delta.`$path`").cache()
      sql(s"TRUNCATE TABLE delta.`$path`")
      checkAnswer(df, Nil)
      checkAnswer(sql(s"select * from delta.`$path`"), Nil)
    }
  }

  testWithTempView("truncate on temp views") { isSQLTempView =>
    withTable("t1") {
      sql("CREATE TABLE t1 (a int) USING delta")
      sql("INSERT into t1 VALUES (3)")
      sql("INSERT into t1 VALUES (4)")

      createTempViewFromTable("t1", isSQLTempView)

      val ae = intercept[AnalysisException] {
        sql("TRUNCATE table v")
      }
      assert(ae.message.contains("EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE"))
    }
  }

}
