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

import org.apache.spark.sql.delta.commands.InsertReplaceOnOrUsingAPIOrigin
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

class DeltaInsertReplaceOnOrUsingMaterializationWithoutParallelConcurrencySuite
  extends DeltaInsertReplaceOnOrUsingConcurrencyTestUtils {

  import InsertReplaceOnOrUsingAPIOrigin._

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
