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

import org.apache.spark.sql.delta.{ChecksDeltaInternals, DeltaLog}
import org.apache.spark.sql.delta.commands.InsertReplaceOnOrUsingAPIOrigin
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf

class DeltaInsertReplaceOnOrUsingMaterializationWithoutParallelConcurrencySuite
  extends DeltaInsertReplaceOnOrUsingConcurrencyTestUtils {

  import InsertReplaceOnOrUsingAPIOrigin._

  protected def statementTimeDeleteFirstOrders: Seq[Boolean] = Seq(true)

  for {
    replaceClause <- Seq("ON", "USING")
    pinStatementTime <- Seq(false, true)
    materialization <- Seq("auto", "all", "none")
    deleteFirst <- statementTimeDeleteFirstOrders
  } {
    test(s"INSERT REPLACE $replaceClause retains its source clock during a concurrent append, " +
        s"pin=$pinStatementTime, materialization=$materialization, deleteFirst=$deleteFirst",
      ChecksDeltaInternals("uses phase barriers and materialized source checkpoints")) {
      withSQLConf(
          DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_PIN_STATEMENT_TIME_ENABLED.key ->
            pinStatementTime.toString,
          DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_MATERIALIZE_SOURCE.key -> materialization,
          "spark.databricks.delta.insertAtomicReplace.cdc.reuseDataFiles" -> "true",
          "spark.databricks.delta.merge.disableSourceMaterializationNotAllowed" -> "false",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
          SQLConf.TIMESTAMP_TYPE.key -> "TIMESTAMP_LTZ",
          SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        withTable("source", "target") {
          createTable("target", Seq("k int", "row_origin string", "observed_time timestamp"))
          insertValues("target", Seq(
            "(1, 'target-1', TIMESTAMP'2000-01-01 00:00:00')",
            "(1, 'target-duplicate', TIMESTAMP'2000-01-01 00:00:00')",
            "(2, 'target-2', TIMESTAMP'2000-01-01 00:00:00')",
            "(3, 'target-3', TIMESTAMP'2000-01-01 00:00:00')"))
          createTable("source", Seq("k int", "row_origin string"))
          insertValues("source", Seq(
            "(1, 'source-1')", "(1, 'source-duplicate')", "(2, 'source-2')"))
          val keptTargetTime = sql("SELECT TIMESTAMP'2000-01-01 00:00:00'")
            .head().getTimestamp(0)
          val targetLog = DeltaLog.forTable(spark, TableIdentifier("target"))
          val targetVersion = targetLog.update().version
          val context = createObserverCtx("replace-statement-time-executor") {
            val sourceQuery =
              "SELECT k, row_origin, current_timestamp() AS observed_time FROM source"
            if (replaceClause == "ON") {
              executeReplaceOn(DFv1Save, "target", "t.k = s.k", sourceQuery)
            } else {
              executeReplaceUsing(DFv1Save, "target", Seq("k"), sourceQuery)
            }
          }
          runWithObserver(context) { controller =>
            if (deleteFirst) controller.runDelete() else controller.runInsert()
            val sourceWasMaterialized = spark.sparkContext.getPersistentRDDs.values
              .exists(_.name == "mergeMaterializedSource")
            if (materialization == "all") assert(sourceWasMaterialized)
            if (materialization == "none") assert(!sourceWasMaterialized)
            insertValues("source", Seq("(3, 'concurrent-source')"))
            val firstPhaseCompletedAt = sql("SELECT current_timestamp()").head().getTimestamp(0)
            if (deleteFirst) controller.runInsert() else controller.runDelete()
            controller.runToCompletion()

            val insertedTimes = sql(
              "SELECT DISTINCT observed_time FROM target WHERE row_origin LIKE 'source%'")
              .collect()
            assert(insertedTimes.length === 1)
            val insertedTime = insertedTimes.head.getTimestamp(0)
            // INSERT second must retain an earlier time when pinning or materialization is used.
            if (pinStatementTime || sourceWasMaterialized || !deleteFirst) {
              assert(!insertedTime.after(firstPhaseCompletedAt),
                s"INSERT time $insertedTime must precede phase completion $firstPhaseCompletedAt")
            } else {
              assert(insertedTime.after(firstPhaseCompletedAt),
                s"Unpinned INSERT time $insertedTime must follow $firstPhaseCompletedAt")
            }
            checkAnswer(sql("SELECT * FROM target"), Seq(
              Row(1, "source-1", insertedTime),
              Row(1, "source-duplicate", insertedTime),
              Row(2, "source-2", insertedTime),
              Row(3, "target-3", keptTargetTime)))
          }
          checkAnswer(sql("SELECT * FROM source"), Seq(
            Row(1, "source-1"), Row(1, "source-duplicate"), Row(2, "source-2"),
            Row(3, "concurrent-source")))
          assert(targetLog.update().version === targetVersion + 1)
          assert(spark.sparkContext.getPersistentRDDs.values.forall(
            _.name != "mergeMaterializedSource"))
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
