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

package io.delta.tables.shared

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

/**
 * Stored-plan temp view refresh tests for Delta tables, shared across classic and Connect.
 *
 * A temp view created from a Dataset (`spark.table(t).filter(...)`) captures the analyzed plan
 * rather than SQL text. These tests verify how such a view reacts when the underlying Delta table
 * changes, both via session SQL and via external writes (commits staged directly into the
 * `_delta_log`).
 *
 * Behavior depends on the V2 enable mode. Under AUTO the legacy V1 path picks up data and additive
 * schema changes on the next access and raises a Delta error on incompatible changes. Under STRICT
 * the Kernel V2 connector instead enforces the stored plan's schema and raises Spark's
 * [[INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION]] on any column change. The two modes
 * are gated below.
 */
trait DeltaTempViewStoredPlanRefreshTests
  extends DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  // Creates the stored-plan temp view `v` = `tableRef` filtered to `salary < 999`, runs `body`,
  // then drops the view. Does not assert the baseline; each test asserts the version and mode
  // specific initial contents itself.
  private def withStoredPlanView(tableRef: String)(body: => Unit): Unit =
    try {
      spark.table(tableRef).filter("salary < 999").createOrReplaceTempView("v")
      body
    } finally {
      spark.sql("DROP VIEW IF EXISTS v")
    }

  test("scenario 1 session: view reflects session write") {
    withInitialTable() { t =>
      withStoredPlanView(t) {
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        writerSql(s"INSERT INTO $t VALUES (2, 200)")
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.2+", "AUTO") | ("4.1", "STRICT") | ("4.1", "AUTO") |
              ("4.0", "AUTO") =>
            // STRICT 4.1+ and AUTO (all versions) refresh: the new row shows through the filter.
            checkAnswer(spark.table("v"), Seq(Row(1, 100), Row(2, 200)))
          case ("4.0", "STRICT") =>
            // 4.0 STRICT pins the snapshot captured when the view plan was built, so the session
            // write is invisible.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        }
      }
    }
  }

  test("scenario 1 external: view reflects external write") {
    withExternalTable() { path =>
      withStoredPlanView("t") {
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        externalDataWrite(path, Seq((2, 200)))
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.2+", "AUTO") | ("4.1", "STRICT") | ("4.1", "AUTO") |
              ("4.0", "AUTO") =>
            // STRICT 4.1+ and AUTO (all versions) refresh: the new row shows through the filter.
            checkAnswer(spark.table("v"), Seq(Row(1, 100), Row(2, 200)))
          case ("4.0", "STRICT") =>
            // 4.0 STRICT pins the view's snapshot, so the external write is invisible.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        }
      }
    }
  }

  test("scenario 2 session: view preserves schema after session ADD COLUMN") {
    withInitialTable() { t =>
      withStoredPlanView(t) {
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        writerSql(s"ALTER TABLE $t ADD COLUMN new_column INT")
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.1", "STRICT") | ("4.0", "STRICT") =>
            // TODO: schema evolution is not fully supported in V2 STRICT yet, so the wider INSERT
            // fails with an arity mismatch.
            checkError(
              exception = intercept[AnalysisException] {
                writerSql(s"INSERT INTO $t VALUES (2, 200, -1)")
              },
              condition = "INSERT_COLUMN_ARITY_MISMATCH")
          case ("4.2+", "AUTO") | ("4.1", "AUTO") | ("4.0", "AUTO") =>
            // AUTO: the view keeps its original two columns, so the wider INSERT succeeds and the
            // new row shows through projected to id and salary.
            writerSql(s"INSERT INTO $t VALUES (2, 200, -1)")
            checkAnswer(spark.table("v"), Seq(Row(1, 100), Row(2, 200)))
        }
      }
    }
  }

  test("scenario 2 external: view preserves schema after external ADD COLUMN") {
    withExternalTable() { path =>
      withStoredPlanView("t") {
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        externalAddColumnAndWrite(path, Seq((2, 200, -1)))
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.2+", "AUTO") | ("4.1", "AUTO") | ("4.0", "AUTO") =>
            // STRICT 4.2+ and AUTO (all versions) refresh preserving the original two columns; the
            // new row shows through projected to id and salary.
            checkAnswer(spark.table("v"), Seq(Row(1, 100), Row(2, 200)))
          case ("4.1", "STRICT") =>
            // 4.1 STRICT rejects the additive column change against the stored plan.
            checkError(
              exception = intercept[SparkThrowable] { spark.table("v").collect() },
              condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION")
          case ("4.0", "STRICT") =>
            // 4.0 STRICT pins the view's snapshot, so the external change is invisible.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        }
      }
    }
  }

  test("scenario 3 session: view detects session column removal") {
    withInitialTable(columnMapping = true) { t =>
      withStoredPlanView(t) {
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.1", "STRICT") | ("4.0", "STRICT") =>
            // STRICT: the view reads the filtered [1, 100]. TODO: the V2 STRICT ALTER COLUMN path
            // reports the column as missing rather than performing the DROP.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            checkError(
              exception = intercept[SparkThrowable] {
                writerSql(s"ALTER TABLE $t DROP COLUMN salary")
              },
              condition = "FIELD_NOT_FOUND")
          case ("4.2+", "AUTO") | ("4.1", "AUTO") | ("4.0", "AUTO") =>
            // AUTO: the view starts as the filtered [1, 100] and dropping the referenced column
            // raises the Delta schema-change error on the next read.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            writerSql(s"ALTER TABLE $t DROP COLUMN salary")
            checkError(
              exception = intercept[SparkThrowable] { spark.table("v").collect() },
              condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
        }
      }
    }
  }

  test("scenario 3 external: view detects external column removal") {
    withExternalTable(columnMapping = true) { path =>
      withStoredPlanView("t") {
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.1", "STRICT") =>
            // 4.2+/4.1 STRICT: the view starts as the filtered [1, 100] and the external
            // column removal is detected against the stored plan and rejected on read.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            externalDropColumn(path, "salary")
            checkError(
              exception = intercept[SparkThrowable] { spark.table("v").collect() },
              condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION")
          case ("4.2+", "AUTO") | ("4.1", "AUTO") | ("4.0", "AUTO") =>
            // AUTO: the view starts as the filtered [1, 100] and the external column removal raises
            // the Delta schema-change error on the next read.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            externalDropColumn(path, "salary")
            checkError(
              exception = intercept[SparkThrowable] { spark.table("v").collect() },
              condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
          case ("4.0", "STRICT") =>
            // 4.0 STRICT pins the view's snapshot, so the external removal is invisible.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            externalDropColumn(path, "salary")
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        }
      }
    }
  }

  test("scenario 4 session: view resolves to session-recreated table") {
    withInitialTable() { t =>
      withStoredPlanView(t) {
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        writerSql(s"DROP TABLE $t")
        writerSql(s"CREATE TABLE $t (id INT, salary INT) USING delta")
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.2+", "AUTO") | ("4.1", "STRICT") | ("4.1", "AUTO") |
              ("4.0", "AUTO") =>
            // STRICT 4.1+ and AUTO (all versions) re-resolve the table by name, reflecting the new
            // empty table.
            checkAnswer(spark.table("v"), Seq.empty)
          case ("4.0", "STRICT") =>
            // 4.0 STRICT pins the view to the dropped table's commit, whose files the session DROP
            // physically removed, so the read fails. Classic raises a raw Kernel exception (not a
            // SparkThrowable) and Connect wraps it; both messages contain "does not exist".
            val message =
              try {
                spark.table("v").collect()
                ""
              } catch {
                case t: Throwable => Option(t.getMessage).getOrElse("")
              }
            assert(message.contains("does not exist"),
              s"Expected a missing pinned-file error but got: '$message'")
        }
      }
    }
  }

  test("scenario 4 external: view resolves to externally recreated table") {
    withExternalTable() { path =>
      withStoredPlanView("t") {
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        externalDropAndRecreate(path)
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.2+", "AUTO") | ("4.1", "STRICT") | ("4.1", "AUTO") |
              ("4.0", "AUTO") =>
            // STRICT 4.1+ and AUTO (all versions) re-resolve the table by name, reflecting the new
            // empty table.
            checkAnswer(spark.table("v"), Seq.empty)
          case ("4.0", "STRICT") =>
            // 4.0 STRICT pins the view's snapshot (the external recreate keeps the old files), so
            // the view still returns its original rows.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        }
      }
    }
  }

  test("scenario 4 session column mapping: view resolves to session-recreated table") {
    withInitialTable(columnMapping = true) { t =>
      withStoredPlanView(t) {
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.1", "STRICT") =>
            // 4.2+/4.1 STRICT: the view starts as the filtered [1, 100] and after the drop and
            // recreate it re-resolves to the new empty table.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            writerSql(s"DROP TABLE $t")
            writerSql(
              s"CREATE TABLE $t (id INT, salary INT) USING delta " +
              "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
            checkAnswer(spark.table("v"), Seq.empty)
          case ("4.2+", "AUTO") | ("4.1", "AUTO") | ("4.0", "AUTO") =>
            // AUTO: recreating a column-mapping table assigns fresh column ids, breaking the stored
            // plan, so the view read raises the Delta schema-change error.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            writerSql(s"DROP TABLE $t")
            writerSql(
              s"CREATE TABLE $t (id INT, salary INT) USING delta " +
              "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
            checkError(
              exception = intercept[SparkThrowable] { spark.table("v").collect() },
              condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
          case ("4.0", "STRICT") =>
            // 4.0 STRICT pins the view to the dropped table's commit, whose files the session DROP
            // physically removed, so the read fails (raw Kernel exception whose message contains
            // "does not exist").
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            writerSql(s"DROP TABLE $t")
            writerSql(
              s"CREATE TABLE $t (id INT, salary INT) USING delta " +
              "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
            val message =
              try {
                spark.table("v").collect()
                ""
              } catch {
                case t: Throwable => Option(t.getMessage).getOrElse("")
              }
            assert(message.contains("does not exist"),
              s"Expected a missing pinned-file error but got: '$message'")
        }
      }
    }
  }

  test("scenario 4 external column mapping: view resolves to externally recreated table") {
    withExternalTable(columnMapping = true) { path =>
      withStoredPlanView("t") {
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.1", "STRICT") =>
            // 4.2+/4.1 STRICT: the view starts as the filtered [1, 100] and re-resolves to the new
            // empty table after the external drop and recreate.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            externalDropAndRecreate(path)
            checkAnswer(spark.table("v"), Seq.empty)
          case ("4.2+", "AUTO") | ("4.1", "AUTO") | ("4.0", "AUTO") =>
            // AUTO: the view re-resolves the table by name, reflecting the new empty table.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            externalDropAndRecreate(path)
            checkAnswer(spark.table("v"), Seq.empty)
          case ("4.0", "STRICT") =>
            // 4.0 STRICT pins the view's snapshot (the external recreate keeps the old files), so
            // the view still returns its original rows.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            externalDropAndRecreate(path)
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        }
      }
    }
  }

  test("scenario 5 session: view detects session drop and re-add column same type") {
    withInitialTable(columnMapping = true) { t =>
      withStoredPlanView(t) {
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.1", "STRICT") | ("4.0", "STRICT") =>
            // STRICT: the view reads the filtered [1, 100]. TODO: the V2 STRICT ALTER COLUMN path
            // reports the column as missing rather than performing the DROP.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            checkError(
              exception = intercept[SparkThrowable] {
                writerSql(s"ALTER TABLE $t DROP COLUMN salary")
              },
              condition = "FIELD_NOT_FOUND")
          case ("4.2+", "AUTO") | ("4.1", "AUTO") | ("4.0", "AUTO") =>
            // AUTO: the view starts as the filtered [1, 100]; re-adding the column with a fresh
            // column id breaks the stored plan, raising the Delta schema-change error.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            writerSql(s"ALTER TABLE $t DROP COLUMN salary")
            writerSql(s"ALTER TABLE $t ADD COLUMN salary INT")
            checkError(
              exception = intercept[SparkThrowable] { spark.table("v").collect() },
              condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
        }
      }
    }
  }

  test("scenario 5 external: view detects external drop and re-add column same type") {
    withExternalTable(columnMapping = true) { path =>
      withStoredPlanView("t") {
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.1", "STRICT") =>
            // 4.2+/4.1 STRICT: the view starts as the filtered [1, 100]; re-adding the column is
            // allowed but the fresh column id no longer matches the stored plan's filter, so the
            // view reads empty afterward.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            externalDropAndReAddColumn(path, "salary", IntegerType)
            checkAnswer(spark.table("v"), Seq.empty)
          case ("4.2+", "AUTO") | ("4.1", "AUTO") | ("4.0", "AUTO") =>
            // AUTO: the view starts as the filtered [1, 100]; re-adding the column with a fresh
            // column id breaks the stored plan, raising the Delta schema-change error.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            externalDropAndReAddColumn(path, "salary", IntegerType)
            checkError(
              exception = intercept[SparkThrowable] { spark.table("v").collect() },
              condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
          case ("4.0", "STRICT") =>
            // 4.0 STRICT pins the view's snapshot, so the external drop and re-add is invisible.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            externalDropAndReAddColumn(path, "salary", IntegerType)
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        }
      }
    }
  }

  test("scenario 6 session: view detects session column type change") {
    withInitialTable(columnMapping = true) { t =>
      withStoredPlanView(t) {
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.1", "STRICT") | ("4.0", "STRICT") =>
            // STRICT: the view reads the filtered [1, 100]. TODO: the V2 STRICT ALTER COLUMN path
            // reports the column as missing rather than performing the DROP.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            checkError(
              exception = intercept[SparkThrowable] {
                writerSql(s"ALTER TABLE $t DROP COLUMN salary")
              },
              condition = "FIELD_NOT_FOUND")
          case ("4.2+", "AUTO") | ("4.1", "AUTO") | ("4.0", "AUTO") =>
            // AUTO: the view starts as the filtered [1, 100]; re-adding the column with a different
            // type breaks the stored plan, raising the Delta schema-change error.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            writerSql(s"ALTER TABLE $t DROP COLUMN salary")
            writerSql(s"ALTER TABLE $t ADD COLUMN salary STRING")
            checkError(
              exception = intercept[SparkThrowable] { spark.table("v").collect() },
              condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
        }
      }
    }
  }

  test("scenario 6 external: view detects external column type change") {
    withExternalTable(columnMapping = true) { path =>
      withStoredPlanView("t") {
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.1", "STRICT") =>
            // 4.2+/4.1 STRICT: the view starts as the filtered [1, 100] and the external type
            // change is detected against the stored plan and rejected on read.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            externalDropAndReAddColumn(path, "salary", StringType)
            checkError(
              exception = intercept[SparkThrowable] { spark.table("v").collect() },
              condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION")
          case ("4.2+", "AUTO") | ("4.1", "AUTO") | ("4.0", "AUTO") =>
            // AUTO: the view starts as the filtered [1, 100]; re-adding the column with a different
            // type breaks the stored plan, raising the Delta schema-change error.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            externalDropAndReAddColumn(path, "salary", StringType)
            checkError(
              exception = intercept[SparkThrowable] { spark.table("v").collect() },
              condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
          case ("4.0", "STRICT") =>
            // 4.0 STRICT pins the view's snapshot, so the external type change is invisible.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
            externalDropAndReAddColumn(path, "salary", StringType)
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        }
      }
    }
  }

  test("scenario 7 session: view reflects session type widening") {
    withInitialTable(typeWidening = true) { t =>
      withStoredPlanView(t) {
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.1", "STRICT") | ("4.0", "STRICT") =>
            // TODO: the V2 STRICT ALTER COLUMN path reports the column as missing rather than
            // applying the type change.
            checkError(
              exception = intercept[SparkThrowable] {
                writerSql(s"ALTER TABLE $t ALTER COLUMN salary TYPE BIGINT")
              },
              condition = "FIELD_NOT_FOUND")
          case ("4.2+", "AUTO") | ("4.1", "AUTO") | ("4.0", "AUTO") =>
            // AUTO: type widening lets the ALTER succeed, but the widened column type breaks the
            // stored plan, so the view read raises the Delta schema-change error.
            writerSql(s"ALTER TABLE $t ALTER COLUMN salary TYPE BIGINT")
            checkError(
              exception = intercept[SparkThrowable] { spark.table("v").collect() },
              condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
        }
      }
    }
  }

  test("scenario 7 external: view reflects external type widening") {
    withExternalTable(typeWidening = true) { path =>
      withStoredPlanView("t") {
        checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        externalChangeColumnType(path, "salary", LongType)
        (sparkVersionBucket, v2EnableMode) match {
          case ("4.2+", "STRICT") | ("4.1", "STRICT") =>
            // STRICT 4.1+ rejects the incompatible column change against the stored plan.
            checkError(
              exception = intercept[SparkThrowable] { spark.table("v").collect() },
              condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION")
          case ("4.2+", "AUTO") | ("4.1", "AUTO") | ("4.0", "AUTO") =>
            // AUTO: the widened column type is incompatible with the stored plan, surfacing the
            // Delta schema-change error.
            checkError(
              exception = intercept[SparkThrowable] { spark.table("v").collect() },
              condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
          case ("4.0", "STRICT") =>
            // 4.0 STRICT pins the view's snapshot, so the external change is invisible.
            checkAnswer(spark.table("v"), Seq(Row(1, 100)))
        }
      }
    }
  }
}
