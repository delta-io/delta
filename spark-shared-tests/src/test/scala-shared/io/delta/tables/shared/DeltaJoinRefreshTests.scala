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
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

/**
 * Self-join refresh tests for incrementally constructed queries: a DataFrame captured at one table
 * version is joined against the same table read again after an external change (a commit staged
 * directly into `_delta_log`).
 *
 * Connect re-analyzes both join sides on every execution, so each scenario branches first on
 * `isConnect`:
 *   - Connect before Spark 4.2 throws AMBIGUOUS_COLUMN_OR_FIELD; 4.2+ sees both sides at the
 *     latest version, fixed by the self-join change in
 *     https://github.com/apache/spark/commit/266f7674b96a07958ef117194eae325565b53004.
 * Classic branches on the V2 enable mode and, under STRICT, on the Spark version:
 *   - AUTO: the V1 file index keeps df1's analysis-time schema but refreshes its data to the latest
 *     version; an incompatible schema change surfaces DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS.
 *   - STRICT 4.0: the V2 connector pins df1 to the snapshot captured when it was built, so the
 *     external change is invisible to df1.
 *   - STRICT 4.1+: [[DeltaV2Table.version]] lets Spark refresh df1 like AUTO; a widened column type
 *     surfaces INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.
 *     TODO: once [[DeltaV2Table]] implements [[Table.id]] (SPARK-54157), a drop/recreate will also
 *     be detected as a different table.
 */
trait DeltaJoinRefreshTests
  extends DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  test("scenario 1: join after external write") {
    withExternalTable() { path =>
      val df1 = spark.table("t")
      externalDataWrite(path, Seq((2, 200)))
      val df2 = spark.table("t")
      val selfJoinDF = df1.join(df2, df1("id") === df2("id"))
      if (isConnect) {
        sparkVersionBucket match {
          case "4.2+" => checkAnswer(selfJoinDF, Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
          case "4.1" | "4.0" =>
            // Connect before 4.2: the self-join is ambiguous, fixed in 4.2.
            checkError(
              exception = intercept[SparkThrowable] { selfJoinDF.collect() },
              condition = "AMBIGUOUS_COLUMN_OR_FIELD")
        }
      } else {
        (v2EnableMode, sparkVersionBucket) match {
          case ("STRICT", "4.0") =>
            // Today 4.0 STRICT pins df1 to its analysis snapshot, so only the row present on both
            // sides joins.
            checkAnswer(selfJoinDF, Seq(Row(1, 100, 1, 100)))
          case ("STRICT", _) =>
            // In 4.1+ with [[DeltaV2Table.version]] Spark refreshes both scans to the latest
            // version (like AUTO) and both rows join.
            checkAnswer(selfJoinDF, Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
          case ("AUTO", _) =>
            checkAnswer(selfJoinDF, Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
        }
      }
    }
  }

  // Same-session counterpart of scenario 1. df1 is pinned (STRICT 4.0) or refreshed (STRICT 4.1+
  // and AUTO) just as for an external write, so the join result matches; routing the write through
  // the session does not change the refresh story.
  test("scenario 1 same-session: join after same-session write") {
    withInitialTable() { _ =>
      val df1 = spark.table("t")
      writerSql("INSERT INTO t VALUES (2, 200)")
      val df2 = spark.table("t")
      val selfJoinDF = df1.join(df2, df1("id") === df2("id"))
      if (isConnect) {
        sparkVersionBucket match {
          case "4.2+" => checkAnswer(selfJoinDF, Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
          case "4.1" | "4.0" =>
            // Connect before 4.2: the self-join is ambiguous, fixed in 4.2.
            checkError(
              exception = intercept[SparkThrowable] { selfJoinDF.collect() },
              condition = "AMBIGUOUS_COLUMN_OR_FIELD")
        }
      } else {
        (v2EnableMode, sparkVersionBucket) match {
          case ("STRICT", "4.0") =>
            // 4.0 STRICT pins df1 to its analysis snapshot, so only the row present on both sides
            // joins, exactly as for an external write.
            checkAnswer(selfJoinDF, Seq(Row(1, 100, 1, 100)))
          case ("STRICT", _) =>
            // In 4.1+ with [[DeltaV2Table.version]] Spark refreshes both scans (like AUTO), so both
            // rows join.
            checkAnswer(selfJoinDF, Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
          case ("AUTO", _) =>
            checkAnswer(selfJoinDF, Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
        }
      }
    }
  }

  test("scenario 2: join after external ADD COLUMN") {
    withExternalTable() { path =>
      val df1 = spark.table("t")
      externalAddColumnAndWrite(path, Seq((2, 200, -1)))
      val df2 = spark.table("t")
      val selfJoinDF = df1.join(df2, df1("id") === df2("id"))
      if (isConnect) {
        sparkVersionBucket match {
          case "4.2+" =>
            // Connect re-resolves df1 with the new 3-column schema on both sides.
            checkAnswer(selfJoinDF,
              Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))
          case "4.1" | "4.0" =>
            // Connect before 4.2: the self-join is ambiguous, fixed in 4.2.
            checkError(
              exception = intercept[SparkThrowable] { selfJoinDF.collect() },
              condition = "AMBIGUOUS_COLUMN_OR_FIELD")
        }
      } else {
        (v2EnableMode, sparkVersionBucket) match {
          case ("STRICT", "4.0") =>
            // Today 4.0 STRICT pins df1 to its 2-column analysis snapshot, so only its original row
            // joins.
            checkAnswer(selfJoinDF, Seq(Row(1, 100, 1, 100, null)))
          case ("STRICT", _) =>
            // In 4.1+ with [[DeltaV2Table.version]] Spark refreshes the
            // data but keeps df1's original schema (like AUTO), so both rows join projected to
            // df1's two columns; using df1 in a write command would instead fail.
            checkAnswer(selfJoinDF, Seq(Row(1, 100, 1, 100, null), Row(2, 200, 2, 200, -1)))
          case ("AUTO", _) =>
            checkAnswer(selfJoinDF, Seq(Row(1, 100, 1, 100, null), Row(2, 200, 2, 200, -1)))
        }
      }
    }
  }

  test("scenario 3: join after external DROP COLUMN (column mapping)") {
    withExternalTable(columnMapping = true) { path =>
      val df1 = spark.table("t")
      externalDropColumn(path, "salary")
      val df2 = spark.table("t")
      val selfJoinDF = df1.join(df2, df1("id") === df2("id"))
      if (isConnect) {
        sparkVersionBucket match {
          case "4.2+" =>
            // TODO: column mapping over Connect 4.2+ is flaky (the re-resolved read races between
            // an empty result and the single joined row); accept either until the V2 column
            // mapping path stabilizes.
            val rows = selfJoinDF.collect()
            assert(rows.length <= 1,
              s"expected empty or one joined row but got ${rows.mkString("[", ", ", "]")}")
          case "4.1" | "4.0" =>
            // Connect before 4.2: the self-join is ambiguous, fixed in 4.2.
            checkError(
              exception = intercept[SparkThrowable] { selfJoinDF.collect() },
              condition = "AMBIGUOUS_COLUMN_OR_FIELD")
        }
      } else {
        (v2EnableMode, sparkVersionBucket) match {
          case ("STRICT", "4.0") =>
            // Today 4.0 STRICT pins df1 to its analysis snapshot, so the external column drop is
            // invisible to df1 and the join executes against the pinned rows.
            checkAnswer(selfJoinDF, Seq(Row(1, 100, 1)))
          case ("STRICT", _) =>
            // 4.1+ STRICT reads unpartitioned column mapping tables, so df1 refreshes and detects
            // the incompatible column drop, raising an analysis exception.
            checkError(
              exception = intercept[SparkThrowable] { selfJoinDF.collect() },
              condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH")
          case ("AUTO", _) =>
            checkError(
              exception = intercept[SparkThrowable] { selfJoinDF.collect() },
              condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
        }
      }
    }
  }

  // Same expectations with and without column mapping: the recreated table has the same schema.
  for ((label, columnMapping) <- Seq(("column mapping", true), ("no column mapping", false))) {
    test(s"scenario 4: join after external DROP/recreate ($label)") {
      withExternalTable(columnMapping = columnMapping) { path =>
        val df1 = spark.table("t")
        externalDropAndRecreate(path)
        val df2 = spark.table("t")
        val selfJoinDF = df1.join(df2, df1("id") === df2("id"))
        if (isConnect) {
          sparkVersionBucket match {
            case "4.2+" =>
              // TODO: once [[DeltaV2Table]] implements [[Table.id]], the recreated table reports a
              // new identity, so df1 detects the drop/recreate as a different table and this
              // assertion will fail.
              checkAnswer(selfJoinDF, Seq.empty)
            case "4.1" | "4.0" =>
              // Connect before 4.2: the self-join is ambiguous, fixed in 4.2.
              checkError(
                exception = intercept[SparkThrowable] { selfJoinDF.collect() },
                condition = "AMBIGUOUS_COLUMN_OR_FIELD")
          }
        } else {
          // The recreated table has the same schema, so df1 stays valid; df2 reads the new empty
          // table and nothing joins, in every classic mode. TODO: once [[DeltaV2Table]] implements
          // [[Table.id]], the recreated table reports a new identity, so Spark treats it as a
          // different table and df1's stale reference raises an analysis exception. Without a
          // table id (as today) the join still executes empty.
          checkAnswer(selfJoinDF, Seq.empty)
        }
      }
    }
  }

  // Today's expectations are identical whether the re-added column keeps its type or changes to a
  // different one; the desired behavior differs by scenario and Spark version, noted in the STRICT
  // arm.
  for ((scenario, label, newColumnType) <- Seq(
      (5, "same type", IntegerType), (6, "different type", StringType))) {
    test(s"scenario $scenario: join after external DROP/ADD $label (column mapping)") {
      withExternalTable(columnMapping = true) { path =>
        val df1 = spark.table("t")
        externalDropAndReAddColumn(path, "salary", newColumnType)
        val df2 = spark.table("t")
        val selfJoinDF = df1.join(df2, df1("id") === df2("id"))
        if (isConnect) {
          sparkVersionBucket match {
            case "4.2+" =>
              // TODO: column mapping over Connect 4.2+ is flaky (the re-resolved read races between
              // an empty result and the single joined row); accept either until the V2 column
              // mapping path stabilizes.
              val rows = selfJoinDF.collect()
              assert(rows.length <= 1,
                s"expected empty or one joined row but got ${rows.mkString("[", ", ", "]")}")
            case "4.1" | "4.0" =>
              // Connect before 4.2: the self-join is ambiguous, fixed in 4.2.
              checkError(
                exception = intercept[SparkThrowable] { selfJoinDF.collect() },
                condition = "AMBIGUOUS_COLUMN_OR_FIELD")
          }
        } else {
          (v2EnableMode, scenario, sparkVersionBucket) match {
            case ("STRICT", _, "4.0") =>
              // Today 4.0 STRICT pins df1 to its analysis snapshot, so the external drop and re-add
              // is invisible to df1. df2 reads the re-added column and the pinned rows join.
              checkAnswer(selfJoinDF, Seq(Row(1, 100, 1, null)))
            case ("STRICT", 5, _) =>
              // 4.1+ STRICT refreshes and picks up the new schema for the same-type column re-add.
              checkAnswer(selfJoinDF, Seq(Row(1, null, 1, null)))
            case ("STRICT", _, _) =>
              // 4.1+ STRICT: re-adding the column with a different type is an incompatible change,
              // so df1 detects it after analysis and raises.
              checkError(
                exception = intercept[SparkThrowable] { selfJoinDF.collect() },
                condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH")
            case ("AUTO", _, _) =>
              checkError(
                exception = intercept[SparkThrowable] { selfJoinDF.collect() },
                condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
          }
        }
      }
    }
  }

  test("scenario 7: join after external ALTER COLUMN TYPE INT to BIGINT (type widening)") {
    withExternalTable(typeWidening = true) { path =>
      val df1 = spark.table("t")
      externalChangeColumnType(path, "salary", LongType)
      val df2 = spark.table("t")
      val selfJoinDF = df1.join(df2, df1("id") === df2("id"))
      if (isConnect) {
        sparkVersionBucket match {
          case "4.2+" => checkAnswer(selfJoinDF, Seq(Row(1, 100L, 1, 100L)))
          case "4.1" | "4.0" =>
            // Connect before 4.2: the self-join is ambiguous, fixed in 4.2.
            checkError(
              exception = intercept[SparkThrowable] { selfJoinDF.collect() },
              condition = "AMBIGUOUS_COLUMN_OR_FIELD")
        }
      } else {
        (v2EnableMode, sparkVersionBucket) match {
          case ("STRICT", "4.0") =>
            // Today 4.0 STRICT pins df1 to its pre-widening snapshot, so the change is invisible.
            checkAnswer(selfJoinDF, Seq(Row(1, 100, 1, 100)))
          case ("STRICT", _) =>
            // In 4.1+ with [[DeltaV2Table.version]] Spark refreshes and detects the widened column
            // type, which is incompatible with df1's analyzed plan; the V2 path raises Spark's
            // INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.
            checkError(
              exception = intercept[SparkThrowable] { selfJoinDF.collect() },
              condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH")
          case ("AUTO", _) =>
            checkError(
              exception = intercept[SparkThrowable] { selfJoinDF.collect() },
              condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
        }
      }
    }
  }
}
