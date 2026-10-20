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

package org.apache.spark.sql.delta.concurrency

import java.sql.Timestamp

import org.apache.spark.sql.delta.ChecksDeltaInternals
import org.apache.spark.sql.delta.commands.InsertReplaceOnOrUsingAPIOrigin
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

class DeltaInsertReplaceOnOrUsingMaterializationWithoutParallelConcurrencySuite
  extends DeltaInsertReplaceOnOrUsingConcurrencyTestUtils {

  import InsertReplaceOnOrUsingAPIOrigin._
  import testImplicits._

  for {
    replaceClause <- Seq("ON", "USING")
    pinStatementTime <- Seq(false, true)
    materializeSource <- Seq("auto", "all", "none")
  } {
    test(s"INSERT REPLACE $replaceClause retains its source clock during a concurrent append, " +
        s"pinStatementTime=$pinStatementTime, materializeSource=$materializeSource",
      ChecksDeltaInternals("uses DELETE/INSERT phase barriers")) {
      withSQLConf(
          DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_PIN_STATEMENT_TIME_ENABLED.key ->
            pinStatementTime.toString,
          DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_MATERIALIZE_SOURCE.key -> materializeSource) {
        withTable("source", "target") {
          // - Duplicate id=1 rows verify replacement preserves source multiplicity.
          // - Append id=3 after DELETE, once the source snapshot has been captured.
          // - The target keeps its id=3 row; only the source table sees the concurrent append.
          // - Pinning or forced materialization keeps the source clock before DELETE.
          //   Otherwise, INSERT folds it afterward.
          val targetTime = Timestamp.valueOf("2020-01-01 00:00:00")
          createTable("target", Seq("id int", "eventTime timestamp", "rowOrigin string"))
          insertValues("target", Seq(
            "(1, TIMESTAMP'2020-01-01 00:00:00', 'target-1')",
            "(1, TIMESTAMP'2020-01-01 00:00:00', 'target-duplicate')",
            "(2, TIMESTAMP'2020-01-01 00:00:00', 'target-2')",
            "(3, TIMESTAMP'2020-01-01 00:00:00', 'target-3')"))
          createTable("source", Seq("id int", "rowOrigin string"))
          insertValues("source", Seq(
            "(1, 'source-1')", "(1, 'source-duplicate')", "(2, 'source-2')"))
          val context = createObserverCtx("replace-statement-time-executor") {
            val sourceQuery =
              "SELECT id, current_timestamp() AS eventTime, rowOrigin FROM source"
            if (replaceClause == "ON") {
              executeReplaceOn(DFv1Save, "target", "t.id = s.id", sourceQuery)
            } else {
              executeReplaceUsing(DFv1Save, "target", Seq("id"), sourceQuery)
            }
          }
          runWithObserver(context) { controller =>
            controller.runDelete()
            insertValues("source", Seq("(3, 'concurrent-source')"))
            val deleteCompletedAt = sql("SELECT current_timestamp()").as[Timestamp].head()
            controller.runInsert()
            controller.runToCompletion()

            val sourceTimes = sql(
              "SELECT DISTINCT eventTime FROM target WHERE rowOrigin LIKE 'source%'")
              .as[Timestamp].collect()
            assert(sourceTimes.length === 1)
            val statementTime = sourceTimes.head
            val expectTimeBeforeDelete =
              pinStatementTime || materializeSource == "all"
            assert(
              if (expectTimeBeforeDelete) statementTime.before(deleteCompletedAt)
              else statementTime.after(deleteCompletedAt),
              s"pinStatementTime=$pinStatementTime, materializeSource=$materializeSource, " +
                s"statementTime=$statementTime, deleteCompletedAt=$deleteCompletedAt")
            checkAnswer(sql("SELECT * FROM target"), Seq(
              Row(1, statementTime, "source-1"),
              Row(1, statementTime, "source-duplicate"),
              Row(2, statementTime, "source-2"),
              Row(3, targetTime, "target-3")))
          }
          checkAnswer(sql("SELECT * FROM source"), Seq(
            Row(1, "source-1"), Row(1, "source-duplicate"), Row(2, "source-2"),
            Row(3, "concurrent-source")))
        }
      }
    }
  }

  // In non-parallel mode, delete is always before insert.
  for (replaceOnAPI <- InsertReplaceOnOrUsingAPIOrigin.values) {
    val testSuffix = s", replaceOnAPI = $replaceOnAPI"

    test(s"Materialization source is actually used in $replaceOnAPI REPLACE ON, " +
        "delete before insert" + testSuffix) {
      withTable("source", "target") {
        createTable(
          tableName = "target",
          tableCols = Seq("col1 int", "row_origin string"))

        insertValues("target", rows = Seq(
          "(1, 'target')",
          "(1, 'target')",
          "(2, 'target')"))

        sql(
          s"""
             |CREATE TABLE source (col1 int, row_origin string)
             |USING csv
             |""".stripMargin)
        insertValues("source", rows = Seq("(0, 'source')"))

        runWithObserver(createReplaceOnObserverCtx(
          api = replaceOnAPI,
          tableName = "target",
          matchingCond = "t.col1 = s.col1",
          sourceQuery = "SELECT * FROM source"
        )) { insertReplaceOn =>
          insertReplaceOn.runDelete()
          // We shouldn't be seeing this row that is inserted in the source,
          // after the INSERT REPLACE ON operation started.
          insertValues("source", rows = Seq("(1, 'source')"))
          insertReplaceOn.runToCompletion()
        }
        // Source materialization prevents new rows like (1, 'source') inserted after
        // the INSERT REPLACE ON operation starts, from incorrectly appearing in the
        // result.
        checkAnswer(
          sql("SELECT * FROM target"),
          Row(0, "source") ::
            Row(1, "target") ::
            Row(1, "target") ::
            Row(2, "target") ::
            Nil)
      }
    }

    test(s"Without materialization, $replaceOnAPI REPLACE ON can produce incorrect results " +
        "for non-deterministic source, delete before insert" + testSuffix) {
      withSQLConf(
        DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_MATERIALIZE_SOURCE.key ->
          DeltaSQLConf.MergeMaterializeSource.NONE) {
        withTable("source", "target") {
          createTable(
            tableName = "target",
            tableCols = Seq("col1 int", "row_origin string"))

          insertValues("target", rows = Seq(
            "(1, 'target')",
            "(1, 'target')",
            "(2, 'target')"))

          sql(
            s"""
               |CREATE TABLE source (col1 int, row_origin string)
               |USING csv
               |""".stripMargin)
          insertValues("source", rows = Seq("(0, 'source')"))

          runWithObserver(createReplaceOnObserverCtx(
            api = replaceOnAPI,
            tableName = "target",
            matchingCond = "t.col1 = s.col1",
            sourceQuery = "SELECT * FROM source"
          )) { insertReplaceOn =>
            insertReplaceOn.runDelete()
            // We shouldn't be seeing this row that is inserted in the source,
            // after the INSERT REPLACE ON operation started.
            insertValues("source", rows = Seq("(1, 'source')"))
            insertReplaceOn.runToCompletion()
          }
          // Without source materialization, the new row (1, 'source'),
          // inserted after the operation began, incorrectly appears in the result.
          checkAnswer(
            sql("SELECT * FROM target"),
            Row(0, "source") ::
              Row(1, "source") ::
              Row(1, "target") ::
              Row(1, "target") ::
              Row(2, "target") ::
              Nil)
        }
      }
    }

    test(s"Even with materialization disabled, $replaceOnAPI REPLACE ON should produce " +
        "correct results for deterministic source, delete before insert" + testSuffix) {
      withSQLConf(
        DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_MATERIALIZE_SOURCE.key ->
          DeltaSQLConf.MergeMaterializeSource.NONE) {
        withTable("source", "target") {
          createTable(
            tableName = "target",
            tableCols = Seq("col1 int", "row_origin string"))

          insertValues("target", rows = Seq(
            "(1, 'target')",
            "(1, 'target')",
            "(2, 'target')"))

          sql(
            s"""
               |CREATE TABLE source (col1 int, row_origin string)
               |USING delta
               |""".stripMargin)
          insertValues("source", rows = Seq("(0, 'source')"))

          runWithObserver(createReplaceOnObserverCtx(
            api = replaceOnAPI,
            tableName = "target",
            matchingCond = "t.col1 = s.col1",
            sourceQuery = "SELECT * FROM source"
          )) { insertReplaceOn =>
            insertReplaceOn.runDelete()
            // We shouldn't be seeing this row that is inserted in the source,
            // after the INSERT REPLACE ON operation started.
            insertValues("source", rows = Seq("(1, 'source')"))
            insertReplaceOn.runToCompletion()
          }
          // Materialization is disabled, but because the source query is deterministic,
          // it prevents new rows like (1, 'source') inserted after the INSERT REPLACE ON
          // operation starts, from incorrectly appearing in the result.
          checkAnswer(
            sql("SELECT * FROM target"),
            Row(0, "source") ::
              Row(1, "target") ::
              Row(1, "target") ::
              Row(2, "target") ::
              Nil)
        }
      }
    }
  }

  for (replaceUsingAPI <- InsertReplaceOnOrUsingAPIOrigin.values) {
    val testSuffix = s", replaceUsingAPI = $replaceUsingAPI"

    test(s"Materialization source is actually used in $replaceUsingAPI REPLACE USING, " +
        "delete before insert" + testSuffix) {
      withTable("source", "target") {
        createTable(
          tableName = "target",
          tableCols = Seq("col1 int", "row_origin string"))

        insertValues("target", rows = Seq(
          "(1, 'target')",
          "(1, 'target')",
          "(2, 'target')"))

        sql(
          s"""
             |CREATE TABLE source (col1 int, row_origin string)
             |USING csv
             |""".stripMargin)
        insertValues("source", rows = Seq("(0, 'source')"))

        runWithObserver(createReplaceUsingObserverCtx(
          api = replaceUsingAPI,
          tableName = "target",
          matchingCols = Seq("col1"),
          sourceQuery = "SELECT * FROM source"
        )) { insertReplaceUsing =>
          insertReplaceUsing.runDelete()
          // This row inserted after the operation started
          // should not be visible due to materialization.
          insertValues("source", rows = Seq("(1, 'source')"))
          insertReplaceUsing.runToCompletion()
        }
        // Source materialization prevents new rows like (1, 'source') inserted after
        // the INSERT REPLACE USING operation starts, from incorrectly appearing in the
        // result.
        checkAnswer(
          sql("SELECT * FROM target"),
          Row(0, "source") ::
            Row(1, "target") ::
            Row(1, "target") ::
            Row(2, "target") ::
            Nil)
      }
    }

    test(s"Without materialization, $replaceUsingAPI REPLACE USING can produce incorrect " +
        "results for non-deterministic source, delete before insert" + testSuffix) {
      withSQLConf(
        DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_MATERIALIZE_SOURCE.key ->
          DeltaSQLConf.MergeMaterializeSource.NONE) {
        withTable("source", "target") {
          createTable(
            tableName = "target",
            tableCols = Seq("col1 int", "row_origin string"))

          insertValues("target", rows = Seq(
            "(1, 'target')",
            "(1, 'target')",
            "(2, 'target')"))

          sql(
            s"""
               |CREATE TABLE source (col1 int, row_origin string)
               |USING csv
               |""".stripMargin)
          insertValues("source", rows = Seq("(0, 'source')"))

          runWithObserver(createReplaceUsingObserverCtx(
            api = replaceUsingAPI,
            tableName = "target",
            matchingCols = Seq("col1"),
            sourceQuery = "SELECT * FROM source"
          )) { insertReplaceUsing =>
            insertReplaceUsing.runDelete()
            // This row inserted after the operation started
            // should not be visible due to materialization.
            insertValues("source", rows = Seq("(1, 'source')"))
            insertReplaceUsing.runToCompletion()
          }
          // Without source materialization, the new row (1, 'source'),
          // inserted after the operation began, incorrectly appears in the result.
          checkAnswer(
            sql("SELECT * FROM target"),
            Row(0, "source") ::
              Row(1, "source") ::
              Row(1, "target") ::
              Row(1, "target") ::
              Row(2, "target") ::
              Nil)
        }
      }
    }

    test(s"Even with materialization disabled, $replaceUsingAPI REPLACE USING should " +
        "produce correct results for deterministic source, delete before insert" + testSuffix) {
      withSQLConf(
        DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_MATERIALIZE_SOURCE.key ->
          DeltaSQLConf.MergeMaterializeSource.NONE) {
        withTable("source", "target") {
          createTable(
            tableName = "target",
            tableCols = Seq("col1 int", "row_origin string"))

          insertValues("target", rows = Seq(
            "(1, 'target')",
            "(1, 'target')",
            "(2, 'target')"))

          sql(
            s"""
               |CREATE TABLE source (col1 int, row_origin string)
               |USING delta
               |""".stripMargin)
          insertValues("source", rows = Seq("(0, 'source')"))

          runWithObserver(createReplaceUsingObserverCtx(
            api = replaceUsingAPI,
            tableName = "target",
            matchingCols = Seq("col1"),
            sourceQuery = "SELECT * FROM source"
          )) { insertReplaceUsing =>
            insertReplaceUsing.runDelete()
            // This row inserted after the operation started
            // should not be visible due to materialization.
            insertValues("source", rows = Seq("(1, 'source')"))
            insertReplaceUsing.runToCompletion()
          }
          // Materialization is disabled, but because the source query is deterministic,
          // it prevents new rows from incorrectly appearing in the result.
          checkAnswer(
            sql("SELECT * FROM target"),
            Row(0, "source") ::
              Row(1, "target") ::
              Row(1, "target") ::
              Row(2, "target") ::
              Nil)
        }
      }
    }
  }
}
