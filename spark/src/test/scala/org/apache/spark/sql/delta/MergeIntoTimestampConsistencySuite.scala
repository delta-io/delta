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

import java.sql.Timestamp

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.{CurrentTimestamp, Now}
import org.apache.spark.sql.functions.{current_timestamp, lit, timestamp_seconds}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class MergeIntoTimestampConsistencySuite extends MergeIntoTimestampConsistencySuiteBase {
}


abstract class MergeIntoTimestampConsistencySuiteBase extends QueryTest
    with SharedSparkSession with DeltaSQLCommandTest {
  private def withTestTables(block: => Unit): Unit = {
    def setupTablesAndRun(): Unit = {
      spark.range(0, 5)
        .toDF("id")
        .withColumn("updated", lit(false))
        .withColumn("timestampOne", timestamp_seconds(lit(1)))
        .withColumn("timestampTwo", timestamp_seconds(lit(1337)))
        .write
        .format("delta")
        .saveAsTable("target")
      spark.range(0, 10)
        .toDF("id")
        .withColumn("updated", lit(true))
        .withColumn("timestampOne", current_timestamp())
        .withColumn("timestampTwo", current_timestamp())
        .createOrReplaceTempView("source")

      block
    }

    Utils.tryWithSafeFinally(setupTablesAndRun) {
      sql("DROP VIEW IF EXISTS source")
      sql("DROP TABLE IF EXISTS target")
    }
  }

  test("Consistent timestamps between source and ON condition") {
    withTestTables {
      sql(s"""MERGE INTO target t
            | USING source s
            | ON s.id = t.id AND s.timestampOne = now()
            | WHEN MATCHED THEN UPDATE SET *""".stripMargin)

      assertAllRowsAreUpdated()
    }
  }

  test("Consistent timestamps between source and WHEN MATCHED condition") {
    withTestTables {
      sql(s"""MERGE INTO target t
            | USING source s
            | ON s.id = t.id
            | WHEN MATCHED AND s.timestampOne = now() AND s.timestampTwo = now()
            |   THEN UPDATE SET *""".stripMargin)

      assertAllRowsAreUpdated()
    }
  }

  test("Consistent timestamps between source and UPDATE SET") {
    withTestTables {
      sql(
        s"""MERGE INTO target t
          | USING source s
          | ON s.id = t.id
          | WHEN MATCHED THEN UPDATE
          |   SET updated = s.updated, t.timestampOne = s.timestampOne, t.timestampTwo = now()
          |""".stripMargin)

      assertUpdatedTimestampsInTargetAreAllEqual()
    }
  }

  test("Consistent timestamps between source and WHEN NOT MATCHED condition") {
    withTestTables {
      sql(s"""MERGE INTO target t
            | USING source s
            | ON s.id = t.id
            | WHEN NOT MATCHED AND s.timestampOne = now() AND s.timestampTwo = now()
            |   THEN INSERT *
            |""".stripMargin)

      assertNewSourceRowsInserted()
    }
  }

  test("Consistent timestamps between source and INSERT VALUES") {
    withTestTables {
      sql(
        s"""MERGE INTO target t
          | USING source s
          | ON s.id = t.id
          | WHEN NOT MATCHED THEN INSERT (id, updated, timestampOne, timestampTwo)
          |   VALUES (s.id, s.updated, s.timestampOne, now())
          |""".stripMargin)

      assertUpdatedTimestampsInTargetAreAllEqual()
    }
  }

  test("Consistent timestamps with subquery in source") {
    withTestTables {
      val sourceWithSubqueryTable = "source_with_subquery"
      withTempView(s"$sourceWithSubqueryTable") {
        sql(
          s"""CREATE OR REPLACE TEMPORARY VIEW $sourceWithSubqueryTable
            | AS SELECT * FROM source WHERE timestampOne IN (SELECT now())
            |""".stripMargin).collect()

        sql(s"""MERGE INTO target t
              | USING $sourceWithSubqueryTable s
              | ON s.id = t.id
              | WHEN MATCHED THEN UPDATE SET *""".stripMargin)

        assertAllRowsAreUpdated()
      }
    }
  }


  private def assertAllRowsAreUpdated(): Unit = {
    val nonUpdatedRowsCount = sql("SELECT * FROM target WHERE updated = FALSE").count()
    assert(0 === nonUpdatedRowsCount, "Un-updated rows in target table")
  }

  private def assertNewSourceRowsInserted(): Unit = {
    val numNotInsertedSourceRows =
      sql("SELECT * FROM source s LEFT ANTI JOIN target t ON s.id = t.id").count()
    assert(0 === numNotInsertedSourceRows, "Un-inserted rows in source table")
  }

  private def assertUpdatedTimestampsInTargetAreAllEqual(): Unit = {
    import testImplicits._

    val timestampCombinations =
      sql(s"""SELECT timestampOne, timestampTwo
            | FROM target WHERE updated = TRUE GROUP BY timestampOne, timestampTwo
            |""".stripMargin)
    val rows = timestampCombinations.as[(Timestamp, Timestamp)].collect()
    assert(1 === rows.length, "Multiple combinations of timestamp values in target table")
    assert(rows(0)._1 === rows(0)._2,
      "timestampOne and timestampTwo are not equal in target table")
  }
}
