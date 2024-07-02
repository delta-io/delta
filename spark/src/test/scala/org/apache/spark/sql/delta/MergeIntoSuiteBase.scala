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

import java.io.File
import java.lang.{Integer => JInt}

import scala.language.implicitConversions

import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions, UsageRecord}
import org.apache.spark.sql.delta.commands.merge.MergeStats
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.test.ScanReportHelper
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.QueryContext
import org.apache.spark.sql.{functions, AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecution
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

abstract class MergeIntoSuiteBase
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLTestUtils
    with ScanReportHelper
    with DeltaTestUtilsForTempViews
    with MergeIntoTestUtils
    with MergeIntoSchemaEvolutionMixin
    with MergeIntoSchemaEvolutionAllTests
    with DeltaExcludedBySparkVersionTestMixinShims {
  import testImplicits._

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic case - merge to Delta table by path, isPartitioned: $isPartitioned") {
      withTable("source") {
        val partitions = if (isPartitioned) "key2" :: Nil else Nil
        append(Seq((2, 2), (1, 4)).toDF("key2", "value"), partitions)
        Seq((1, 1), (0, 3)).toDF("key1", "value").createOrReplaceTempView("source")

        executeMerge(
          target = s"delta.`$tempPath`",
          source = "source src",
          condition = "src.key1 = key2",
          update = "key2 = 20 + key1, value = 20 + src.value",
          insert = "(key2, value) VALUES (key1 - 10, src.value + 10)")

        checkAnswer(readDeltaTable(tempPath),
          Row(2, 2) :: // No change
            Row(21, 21) :: // Update
            Row(-10, 13) :: // Insert
            Nil)
      }
    }
  }

  Seq(true, false).foreach { skippingEnabled =>
    Seq(true, false).foreach { partitioned =>
      Seq(true, false).foreach { useSQLView =>
        test("basic case - merge to view on a Delta table by path, " +
          s"partitioned: $partitioned skippingEnabled: $skippingEnabled useSqlView: $useSQLView") {
          withTable("delta_target", "source") {
            withSQLConf(DeltaSQLConf.DELTA_STATS_SKIPPING.key -> skippingEnabled.toString) {
              Seq((1, 1), (0, 3), (1, 6)).toDF("key1", "value").createOrReplaceTempView("source")
              val partitions = if (partitioned) "key2" :: Nil else Nil
              append(Seq((2, 2), (1, 4)).toDF("key2", "value"), partitions)
              if (useSQLView) {
                sql(s"CREATE OR REPLACE TEMP VIEW delta_target AS " +
                  s"SELECT * FROM delta.`$tempPath` t")
              } else {
                readDeltaTable(tempPath).createOrReplaceTempView("delta_target")
              }

              executeMerge(
                target = "delta_target",
                source = "source src",
                condition = "src.key1 = key2 AND src.value < delta_target.value",
                update = "key2 = 20 + key1, value = 20 + src.value",
                insert = "(key2, value) VALUES (key1 - 10, src.value + 10)")

              checkAnswer(sql("SELECT key2, value FROM delta_target"),
                Row(2, 2) :: // No change
                  Row(21, 21) :: // Update
                  Row(-10, 13) :: // Insert
                  Row(-9, 16) :: // Insert
                  Nil)
            }
          }
        }
      }
    }
  }

  Seq(true, false).foreach { skippingEnabled =>
    Seq(true, false).foreach { isPartitioned =>
     test("basic case - merge to Delta table by name, " +
         s"isPartitioned: $isPartitioned skippingEnabled: $skippingEnabled") {
        withTable("delta_target", "source") {
          withSQLConf(DeltaSQLConf.DELTA_STATS_SKIPPING.key -> skippingEnabled.toString) {
            Seq((1, 1), (0, 3), (1, 6)).toDF("key1", "value").createOrReplaceTempView("source")
            val partitionByClause = if (isPartitioned) "PARTITIONED BY (key2)" else ""
            sql(
              s"""
                |CREATE TABLE delta_target(key2 INT, value INT)
                |USING delta
                |OPTIONS('path'='$tempPath')
                |$partitionByClause
               """.stripMargin)

            append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

            executeMerge(
              target = "delta_target",
              source = "source src",
              condition = "src.key1 = key2 AND src.value < delta_target.value",
              update = "key2 = 20 + key1, value = 20 + src.value",
              insert = "(key2, value) VALUES (key1 - 10, src.value + 10)")

            checkAnswer(sql("SELECT key2, value FROM delta_target"),
              Row(2, 2) :: // No change
                Row(21, 21) :: // Update
                Row(-10, 13) :: // Insert
                Row(-9, 16) :: // Insert
                Nil)
            }
        }
      }
    }
  }

  test("basic case - update value from both source and target table") {
    withTable("source") {
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))
      Seq((1, 1), (0, 3)).toDF("key1", "value").createOrReplaceTempView("source")

      executeMerge(
        target = s"delta.`$tempPath` as trgNew",
        source = "source src",
        condition = "src.key1 = key2",
        update = "key2 = 20 + key2, value = trgNew.value + src.value",
        insert = "(key2, value) VALUES (key1 - 10, src.value + 10)")

      checkAnswer(readDeltaTable(tempPath),
        Row(2, 2) :: // No change
          Row(21, 5) :: // Update
          Row(-10, 13) :: // Insert
          Nil)
    }
  }

  test("basic case - columns are specified in wrong order") {
    withTable("source") {
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))
      Seq((1, 1), (0, 3)).toDF("key1", "value").createOrReplaceTempView("source")

      executeMerge(
        target = s"delta.`$tempPath` as trgNew",
        source = "source src",
        condition = "src.key1 = key2",
        update = "value = trgNew.value + src.value, key2 = 20 + key2",
        insert = "(value, key2) VALUES (src.value + 10, key1 - 10)")

      checkAnswer(readDeltaTable(tempPath),
        Row(2, 2) :: // No change
          Row(21, 5) :: // Update
          Row(-10, 13) :: // Insert
          Nil)
    }
  }

  test("basic case - not all columns are specified in update") {
    withTable("source") {
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))
      Seq((1, 1), (0, 3)).toDF("key1", "value").createOrReplaceTempView("source")

      executeMerge(
        target = s"delta.`$tempPath` as trgNew",
        source = "source src",
        condition = "src.key1 = key2",
        update = "value = trgNew.value + 3",
        insert = "(key2, value) VALUES (key1 - 10, src.value + 10)")

      checkAnswer(readDeltaTable(tempPath),
        Row(2, 2) :: // No change
          Row(1, 7) :: // Update
          Row(-10, 13) :: // Insert
          Nil)
    }
  }

  test("basic case - multiple inserts") {
    withTable("source") {
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))
      Seq((1, 1), (0, 3), (3, 5)).toDF("key1", "value").createOrReplaceTempView("source")

      executeMerge(
        tgt = s"delta.`$tempPath` as trgNew",
        src = "source src",
        cond = "src.key1 = key2",
        insert(condition = "key1 = 0", values = "(key2, value) VALUES (src.key1, src.value + 3)"),
        insert(values = "(key2, value) VALUES (src.key1 - 10, src.value + 10)"))

      checkAnswer(readDeltaTable(tempPath),
        Row(2, 2) :: // No change
          Row(1, 4) :: // No change
          Row(0, 6) :: // Insert
          Row(-7, 15) :: // Insert
          Nil)
    }
  }

  test("basic case - upsert with only rows inserted") {
    withTable("source") {
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))
      Seq((1, 1), (0, 3)).toDF("key1", "value").createOrReplaceTempView("source")

      executeMerge(
        tgt = s"delta.`$tempPath` as trgNew",
        src = "source src",
        cond = "src.key1 = key2",
        update(condition = "key2 = 5", set = "value = src.value + 3"),
        insert(values = "(key2, value) VALUES (src.key1 - 10, src.value + 10)"))

      checkAnswer(readDeltaTable(tempPath),
        Row(2, 2) :: // No change
          Row(1, 4) :: // No change
          Row(-10, 13) :: // Insert
          Nil)
    }
  }

  protected def testNullCase(name: String)(
      target: Seq[(JInt, JInt)],
      source: Seq[(JInt, JInt)],
      condition: String,
      expectedResults: Seq[(JInt, JInt)]) = {
    Seq(true, false).foreach { isPartitioned =>
      test(s"basic case - null handling - $name, isPartitioned: $isPartitioned") {
        withView("sourceView") {
          val partitions = if (isPartitioned) "key" :: Nil else Nil
          append(target.toDF("key", "value"), partitions)
          source.toDF("key", "value").createOrReplaceTempView("sourceView")

          executeMerge(
            target = s"delta.`$tempPath` as t",
            source = "sourceView s",
            condition = condition,
            update = "t.value = s.value",
            insert = "(t.key, t.value) VALUES (s.key, s.value)")

          checkAnswer(
            readDeltaTable(tempPath),
            expectedResults.map { r => Row(r._1, r._2) }
          )

          Utils.deleteRecursively(new File(tempPath))
        }
      }
    }
  }

  testNullCase("null value in target")(
    target = Seq((null, null), (1, 1)),
    source = Seq((1, 10), (2, 20)),
    condition = "s.key = t.key",
    expectedResults = Seq(
      (null, null),   // No change
      (1, 10),        // Update
      (2, 20)         // Insert
    ))

  testNullCase("null value in source")(
    target = Seq((1, 1)),
    source = Seq((1, 10), (2, 20), (null, null)),
    condition = "s.key = t.key",
    expectedResults = Seq(
      (1, 10),        // Update
      (2, 20),        // Insert
      (null, null)    // Insert
    ))

  testNullCase("null value in both source and target")(
    target = Seq((1, 1), (null, null)),
    source = Seq((1, 10), (2, 20), (null, 0)),
    condition = "s.key = t.key",
    expectedResults = Seq(
      (null, null),   // No change as null in source does not match null in target
      (1, 10),        // Update
      (2, 20),        // Insert
      (null, 0)       // Insert
    ))

  testNullCase("null value in both source and target + IS NULL in condition")(
    target = Seq((1, 1), (null, null)),
    source = Seq((1, 10), (2, 20), (null, 0)),
    condition = "s.key = t.key AND s.key IS NULL",
    expectedResults = Seq(
      (null, null),   // No change as s.key != t.key
      (1, 1),         // No change as s.key is not null
      (null, 0),      // Insert
      (1, 10),        // Insert
      (2, 20)         // Insert
    ))

  testNullCase("null value in both source and target + IS NOT NULL in condition")(
    target = Seq((1, 1), (null, null)),
    source = Seq((1, null), (2, 20), (null, 0)),
    condition = "s.key = t.key AND t.value IS NOT NULL",
    expectedResults = Seq(
      (null, null),   // No change as t.value is null
      (1, null),      // Update as t.value is not null
      (null, 0),      // Insert
      (2, 20)         // Insert
    ))

  testNullCase("null value in both source and target + <=> in condition")(
    target = Seq((1, 1), (null, null)),
    source = Seq((1, 10), (2, 20), (null, 0)),
    condition = "s.key <=> t.key",
    expectedResults = Seq(
      (null, 0),      // Update
      (1, 10),        // Update
      (2, 20)         // Insert
    ))

  testNullCase("NULL in condition")(
    target = Seq((1, 1), (null, null)),
    source = Seq((1, 10), (2, 20), (null, 0)),
    condition = "s.key = t.key AND NULL",
    expectedResults = Seq(
      (null, null),   // No change as NULL condition did not match anything
      (1, 1),         // No change as NULL condition did not match anything
      (null, 0),      // Insert
      (1, 10),        // Insert
      (2, 20)         // Insert
    ))

  test("basic case - only insert") {
    withTable("source") {
      Seq((5, 5)).toDF("key1", "value").createOrReplaceTempView("source")
      append(Seq.empty[(Int, Int)].toDF("key2", "value"))

      executeMerge(
        target = s"delta.`$tempPath` as target",
        source = "source src",
        condition = "src.key1 = target.key2",
        update = "key2 = 20 + key1, value = 20 + src.value",
        insert = "(key2, value) VALUES (key1 - 10, src.value + 10)")

      checkAnswer(readDeltaTable(tempPath),
        Row(-5, 15) :: // Insert
          Nil)
    }
  }

  test("basic case - both source and target are empty") {
    withTable("source") {
      Seq.empty[(Int, Int)].toDF("key1", "value").createOrReplaceTempView("source")
      append(Seq.empty[(Int, Int)].toDF("key2", "value"))

      executeMerge(
        target = s"delta.`$tempPath` as target",
        source = "source src",
        condition = "src.key1 = target.key2",
        update = "key2 = 20 + key1, value = 20 + src.value",
        insert = "(key2, value) VALUES (key1 - 10, src.value + 10)")

      checkAnswer(readDeltaTable(tempPath), Nil)
    }
  }

  test("basic case - only update") {
    withTable("source") {
      Seq((1, 5), (2, 9)).toDF("key1", "value").createOrReplaceTempView("source")
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

      executeMerge(
        target = s"delta.`$tempPath` as target",
        source = "source src",
        condition = "src.key1 = target.key2",
        update = "key2 = 20 + key1, value = 20 + src.value",
        insert = "(key2, value) VALUES (key1 - 10, src.value + 10)")

      checkAnswer(readDeltaTable(tempPath),
        Row(21, 25) ::   // Update
          Row(22, 29) :: // Update
          Nil)
    }
  }

  test("same column names in source and target") {
    withTable("source") {
      Seq((1, 5), (2, 9)).toDF("key", "value").createOrReplaceTempView("source")
      append(Seq((2, 2), (1, 4)).toDF("key", "value"))

      executeMerge(
        target = s"delta.`$tempPath` as target",
        source = "source src",
        condition = "src.key = target.key",
        update = "target.key = 20 + src.key, target.value = 20 + src.value",
        insert = "(key, value) VALUES (src.key - 10, src.value + 10)")

      checkAnswer(
        readDeltaTable(tempPath),
        Row(21, 25) :: // Update
          Row(22, 29) :: // Update
          Nil)
    }
  }

  test("Source is a query") {
    withTable("source") {
      Seq((1, 6, "a"), (0, 3, "b")).toDF("key1", "value", "others")
        .createOrReplaceTempView("source")
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

      executeMerge(
        target = s"delta.`$tempPath` as trg",
        source = "(SELECT key1, value, others FROM source) src",
        condition = "src.key1 = trg.key2",
        update = "trg.key2 = 20 + key1, value = 20 + src.value",
        insert = "(trg.key2, value) VALUES (key1 - 10, src.value + 10)")

      checkAnswer(
        readDeltaTable(tempPath),
        Row(2, 2) :: // No change
          Row(21, 26) :: // Update
          Row(-10, 13) :: // Insert
          Nil)

      withCrossJoinEnabled {
        executeMerge(
        target = s"delta.`$tempPath` as trg",
        source = "(SELECT 5 as key1, 5 as value) src",
        condition = "src.key1 = trg.key2",
        update = "trg.key2 = 20 + key1, value = 20 + src.value",
        insert = "(trg.key2, value) VALUES (key1 - 10, src.value + 10)")
      }

      checkAnswer(readDeltaTable(tempPath),
        Row(2, 2) ::
          Row(21, 26) ::
          Row(-10, 13) ::
          Row(-5, 15) :: // new row
          Nil)
    }
  }

  test("self merge") {
    append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

    executeMerge(
      target = s"delta.`$tempPath` as target",
      source = s"delta.`$tempPath` as src",
      condition = "src.key2 = target.key2",
      update = "key2 = 20 + src.key2, value = 20 + src.value",
      insert = "(key2, value) VALUES (src.key2 - 10, src.value + 10)")

    checkAnswer(readDeltaTable(tempPath),
      Row(22, 22) :: // UPDATE
        Row(21, 24) :: // UPDATE
        Nil)
  }

  test("order by + limit in source query #1") {
    withTable("source") {
      Seq((1, 6, "a"), (0, 3, "b")).toDF("key1", "value", "others")
        .createOrReplaceTempView("source")
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

      executeMerge(
        target = s"delta.`$tempPath` as trg",
        source = "(SELECT key1, value, others FROM source order by key1 limit 1) src",
        condition = "src.key1 = trg.key2",
        update = "trg.key2 = 20 + key1, value = 20 + src.value",
        insert = "(trg.key2, value) VALUES (key1 - 10, src.value + 10)")

      checkAnswer(readDeltaTable(tempPath),
        Row(1, 4) :: // No change
          Row(2, 2) :: // No change
          Row(-10, 13) :: // Insert
          Nil)
    }
  }

  test("order by + limit in source query #2") {
    withTable("source") {
      Seq((1, 6, "a"), (0, 3, "b")).toDF("key1", "value", "others")
        .createOrReplaceTempView("source")
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

      executeMerge(
        target = s"delta.`$tempPath` as trg",
        source = "(SELECT key1, value, others FROM source order by value DESC limit 1) src",
        condition = "src.key1 = trg.key2",
        update = "trg.key2 = 20 + key1, value = 20 + src.value",
        insert = "(trg.key2, value) VALUES (key1 - 10, src.value + 10)")

      checkAnswer(readDeltaTable(tempPath),
        Row(2, 2) :: // No change
          Row(21, 26) :: // UPDATE
          Nil)
    }
  }

  testQuietly("Negative case - more than one source rows match the same target row") {
    withTable("source") {
      Seq((1, 1), (0, 3), (1, 5)).toDF("key1", "value").createOrReplaceTempView("source")
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

      val e = intercept[Exception] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key2 = 20 + key1, value = 20 + src.value",
          insert = "(key2, value) VALUES (key1 - 10, src.value + 10)")
      }.toString

      val expectedEx = DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException(spark)
      assert(e.contains(expectedEx.getMessage))
    }
  }

  test("More than one target rows match the same source row") {
    withTable("source") {
      Seq((1, 5), (2, 9)).toDF("key1", "value").createOrReplaceTempView("source")
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"), Seq("key2"))

      withCrossJoinEnabled {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "key1 = 1",
          update = "key2 = 20 + key1, value = 20 + src.value",
          insert = "(key2, value) VALUES (key1 - 10, src.value + 10)")
      }

      checkAnswer(readDeltaTable(tempPath),
        Row(-8, 19) :: // Insert
          Row(21, 25) :: // Update
          Row(21, 25) :: // Update
          Nil)
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"Merge table using different data types - implicit casting, parts: $isPartitioned") {
      withTable("source") {
        Seq((1, "5"), (3, "9"), (3, "a")).toDF("key1", "value").createOrReplaceTempView("source")
        val partitions = if (isPartitioned) "key2" :: Nil else Nil
        append(Seq((2, 2), (1, 4)).toDF("key2", "value"), partitions)

        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "key1 = key2",
          update = "key2 = 33 + cast(key2 as double), value = '20'",
          insert = "(key2, value) VALUES ('44', try_cast(src.value as double) + 10)")

        checkAnswer(readDeltaTable(tempPath),
          Row(44, 19) :: // Insert
          // NULL is generated when the type casting does not work for some values)
          Row(44, null) :: // Insert
          Row(34, 20) :: // Update
          Row(2, 2) :: // No change
          Nil)
      }
    }
  }

  test("Negative case - basic syntax analysis") {
    withTable("source") {
      Seq((1, 1), (0, 3)).toDF("key1", "value").createOrReplaceTempView("source")
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

      // insert expressions have target table reference
      var e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key2 = key1, value = src.value",
          insert = "(key2, value) VALUES (3, src.value + key2)")
      }.getMessage

      errorContains(e, "cannot resolve key2")

      // to-update columns have source table reference
      e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key1 = 1, value = 2",
          insert = "(key2, value) VALUES (3, 4)")
      }.getMessage

      errorContains(e, "Cannot resolve key1 in UPDATE clause")
      errorContains(e, "key2") // should show key2 as a valid name in target columns

      // to-insert columns have source table reference
      e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key2 = 1, value = 2",
          insert = "(key1, value) VALUES (3, 4)")
      }.getMessage

      errorContains(e, "Cannot resolve key1 in INSERT clause")
      errorContains(e, "key2") // should contain key2 as a valid name in target columns

      // ambiguous reference
      e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key2 = 1, value = value",
          insert = "(key2, value) VALUES (3, 4)")
      }.getMessage

      Seq("value", "is ambiguous", "could be").foreach(x => errorContains(e, x))

      // non-deterministic search condition
      e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2 and rand() > 0.5",
          update = "key2 = 1, value = 2",
          insert = "(key2, value) VALUES (3, 4)")
      }.getMessage

      errorContains(e, "Non-deterministic functions are not supported in the search condition")

      // aggregate function
      e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2 and max(target.key2) > 20",
          update = "key2 = 1, value = 2",
          insert = "(key2, value) VALUES (3, 4)")
      }.getMessage

      errorContains(e, "Aggregate functions are not supported in the search condition")
    }
  }

  test("Merge should use the same SparkSession consistently") {
    withTempDir { dir =>
      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "false") {
        val r = dir.getCanonicalPath
        val sourcePath = s"$r/source"
        val targetPath = s"$r/target"
        val numSourceRecords = 20
        spark.range(numSourceRecords)
          .withColumn("x", $"id")
          .withColumn("y", $"id")
          .write.mode("overwrite").format("delta").save(sourcePath)
        spark.range(1)
          .withColumn("x", $"id")
          .write.mode("overwrite").format("delta").save(targetPath)
        val spark2 = spark.newSession
        spark2.conf.set(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key, "true")
        val target = io.delta.tables.DeltaTable.forPath(spark2, targetPath)
        val source = spark.read.format("delta").load(sourcePath).alias("s")
        val merge = target.alias("t")
          .merge(source, "t.id = s.id")
          .whenMatched.updateExpr(Map("t.x" -> "t.x + 1"))
          .whenNotMatched.insertAll()
          .execute()
        // The target table should have the same number of rows as the source after the merge
        assert(spark.read.format("delta").load(targetPath).count() == numSourceRecords)
      }
    }
  }

  testSparkMasterOnly("Variant type") {
    withTable("source") {
      // Insert ("0", 0), ("1", 1)
      val dstDf = sql(
        """SELECT parse_json(cast(id as string)) v, id i
        FROM range(2)""")
      append(dstDf)
      // Insert ("1", 2), ("2", 3)
      // The first row will update, the second will insert.
      sql(
        s"""SELECT parse_json(cast(id as string)) v, id + 1 i
              FROM range(1, 3)""").createOrReplaceTempView("source")

      executeMerge(
        target = s"delta.`$tempPath` as trgNew",
        source = "source src",
        condition = "to_json(src.v) = to_json(trgNew.v)",
        update = "i = 10 + src.i + trgNew.i, v = trgNew.v",
        insert = """(i, v) VALUES (i + 100, parse_json('"inserted"'))""")

      checkAnswer(readDeltaTable(tempPath).selectExpr("i", "to_json(v)"),
        Row(0, "0") :: // No change
          Row(13, "1") :: // Update
          Row(103, "\"inserted\"") :: // Insert
          Nil)
    }
  }

  // Enable this test in OSS when Spark has the change to report better errors
  // when MERGE is not supported.
  ignore("Negative case - non-delta target") {
    withTable("source", "target") {
      Seq((1, 1), (0, 3), (1, 5)).toDF("key1", "value").createOrReplaceTempView("source")
      Seq((1, 1), (0, 3), (1, 5)).toDF("key2", "value").write.saveAsTable("target")

      val e = intercept[AnalysisException] {
        executeMerge(
          target = "target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key2 = 20 + key1, value = 20 + src.value",
          insert = "(key2, value) VALUES (key1 - 10, src.value + 10)")
      }.getMessage
      assert(e.contains("does not support MERGE") ||
        // The MERGE Scala API is for Delta only and reports error differently.
        e.contains("is not a Delta table") ||
        e.contains("MERGE destination only supports Delta sources"))
    }
  }

  test("Negative case - update assignments conflict because " +
    "same column with different references") {
    withTable("source") {
      Seq((1, 1), (0, 3), (1, 5)).toDF("key1", "value").createOrReplaceTempView("source")
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))
      val e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as t",
          source = "source s",
          condition = "s.key1 = t.key2",
          update = "key2 = key1, t.key2 = key1",
          insert = "(key2, value) VALUES (3, 4)")
      }.getMessage

      errorContains(e, "there is a conflict from these set columns")
    }
  }

  test("Negative case - more operations between merge and delta target") {
    withTempView("source", "target") {
      Seq((1, 1), (0, 3), (1, 5)).toDF("key1", "value").createOrReplaceTempView("source")
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))
      spark.read.format("delta").load(tempPath).filter("value <> 0").createTempView("target")

      val e = intercept[AnalysisException] {
        executeMerge(
          target = "target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key2 = 20 + key1, value = 20 + src.value",
          insert = "(key2, value) VALUES (key1 - 10, src.value + 10)")
      }.getMessage
      errorContains(e, "Expect a full scan of Delta sources, but found a partial scan")
    }
  }

  test("Negative case - MERGE to the child directory") {
    val df = Seq((1, 1), (0, 3), (1, 5)).toDF("key2", "value")
    val partitions = "key2" :: Nil
    append(df, partitions)

    val e = intercept[AnalysisException] {
      executeMerge(
        target = s"delta.`$tempPath/key2=1` target",
        source = "(SELECT 5 as key1, 5 as value) src",
        condition = "src.key1 = target.key2",
        update = "key2 = 20 + key1, value = 20 + src.value",
        insert = "(key2, value) VALUES (key1 - 10, src.value + 10)")
    }.getMessage
    errorContains(e, "Expect a full scan of Delta sources, but found a partial scan")
  }

  test(s"special character in path - matched delete") {
    val source = s"$tempDir/sou rce~"
    val target = s"$tempDir/tar get>"
    spark.range(0, 10, 2).write.format("delta").save(source)
    spark.range(10).write.format("delta").save(target)
    executeMerge(
      tgt = s"delta.`$target` t",
      src = s"delta.`$source` s",
      cond = "t.id = s.id",
      clauses = delete())
    checkAnswer(readDeltaTable(target), Seq(1, 3, 5, 7, 9).toDF("id"))
  }

  test(s"special character in path - matched update") {
    val source = s"$tempDir/sou rce("
    val target = s"$tempDir/tar get*"
    spark.range(0, 10, 2).write.format("delta").save(source)
    spark.range(10).write.format("delta").save(target)
    executeMerge(
      tgt = s"delta.`$target` t",
      src = s"delta.`$source` s",
      cond = "t.id = s.id",
      clauses = update(set = "id = t.id * 10"))
    checkAnswer(readDeltaTable(target), Seq(0, 1, 20, 3, 40, 5, 60, 7, 80, 9).toDF("id"))
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"single file, isPartitioned: $isPartitioned") {
      withTable("source") {
        val df = spark.range(5).selectExpr("id as key1", "id as key2", "id as col1").repartition(1)

        val partitions = if (isPartitioned) "key1" :: "key2" :: Nil else Nil
        append(df, partitions)

        df.createOrReplaceTempView("source")

        executeMerge(
          target = s"delta.`$tempPath` target",
          source = "(SELECT key1 as srcKey, key2, col1 FROM source where key1 < 3) AS source",
          condition = "srcKey = target.key1",
          update = "target.key1 = srcKey - 1000, target.key2 = source.key2 + 1000, " +
            "target.col1 = source.col1",
          insert = "(key1, key2, col1) VALUES (srcKey, source.key2, source.col1)")

        checkAnswer(readDeltaTable(tempPath),
          Row(-998, 1002, 2) :: // Update
            Row(-999, 1001, 1) :: // Update
            Row(-1000, 1000, 0) :: // Update
            Row(4, 4, 4) :: // No change
            Row(3, 3, 3) :: // No change
            Nil)
      }
    }
  }

  protected def testLocalPredicates(name: String)(
      target: Seq[(String, String, String)],
      source: Seq[(String, String)],
      condition: String,
      expectedResults: Seq[(String, String, String)],
      numFilesPerPartition: Int = 2) = {
    Seq(true, false).foreach { isPartitioned =>
      test(s"$name, isPartitioned: $isPartitioned") { withTable("source") {
        val partitions = if (isPartitioned) "key2" :: Nil else Nil
        append(target.toDF("key2", "value", "op").repartition(numFilesPerPartition), partitions)
        source.toDF("key1", "value").createOrReplaceTempView("source")

        // Local predicates are likely to be pushed down leading empty join conditions
        // and cross-join being used
        withCrossJoinEnabled { executeMerge(
          target = s"delta.`$tempPath` trg",
          source = "source src",
          condition = condition,
          update = "key2 = src.key1, value = src.value, op = 'update'",
          insert = "(key2, value, op) VALUES (src.key1, src.value, 'insert')")
        }

        checkAnswer(
          readDeltaTable(tempPath),
          expectedResults.map { r => Row(r._1, r._2, r._3) }
        )

        Utils.deleteRecursively(new File(tempPath))
      }
    }}
  }

  testLocalPredicates("basic case - local predicates - predicate has no matches, only inserts")(
    target = Seq(("2", "2", "noop"), ("1", "4", "noop"), ("3", "2", "noop"), ("4", "4", "noop")),
    source = Seq(("1", "8"), ("0", "3")),
    condition = "src.key1 = key2 and key2 != '1'",
    expectedResults =
      ("2", "2", "noop") ::
      ("1", "4", "noop") ::
      ("3", "2", "noop") ::
      ("4", "4", "noop") ::
      ("1", "8", "insert") ::
      ("0", "3", "insert") ::
      Nil)

  testLocalPredicates("basic case - local predicates - predicate has matches, updates and inserts")(
    target = Seq(("1", "2", "noop"), ("1", "4", "noop"), ("3", "2", "noop"), ("4", "4", "noop")),
    source = Seq(("1", "8"), ("0", "3")),
    condition = "src.key1 = key2 and key2 < '3'",
    expectedResults =
      ("3", "2", "noop") ::
      ("4", "4", "noop") ::
      ("1", "8", "update") ::
      ("1", "8", "update") ::
      ("0", "3", "insert") ::
      Nil)

  testLocalPredicates("basic case - local predicates - predicate has matches, only updates")(
    target = Seq(("1", "2", "noop"), ("1", "4", "noop"), ("3", "2", "noop"), ("4", "4", "noop")),
    source = Seq(("1", "8")),
    condition = "key2 < '3'",
    expectedResults =
      ("3", "2", "noop") ::
      ("4", "4", "noop") ::
      ("1", "8", "update") ::
      ("1", "8", "update") ::
      Nil)

  testLocalPredicates("basic case - local predicates - always false predicate, only inserts")(
      target = Seq(("1", "2", "noop"), ("1", "4", "noop"), ("3", "2", "noop"), ("4", "4", "noop")),
      source = Seq(("1", "8"), ("0", "3")),
      condition = "1 != 1",
      expectedResults =
        ("1", "2", "noop") ::
        ("1", "4", "noop") ::
        ("3", "2", "noop") ::
        ("4", "4", "noop") ::
        ("1", "8", "insert") ::
        ("0", "3", "insert") ::
        Nil)

  testLocalPredicates("basic case - local predicates - always true predicate, all updated")(
    target = Seq(("1", "2", "noop"), ("1", "4", "noop"), ("3", "2", "noop"), ("4", "4", "noop")),
    source = Seq(("1", "8")),
    condition = "1 = 1",
    expectedResults =
      ("1", "8", "update") ::
      ("1", "8", "update") ::
      ("1", "8", "update") ::
      ("1", "8", "update") ::
      Nil)

  testLocalPredicates("basic case - local predicates - single file, updates and inserts")(
    target = Seq(("1", "2", "noop"), ("1", "4", "noop"), ("3", "2", "noop"), ("4", "4", "noop")),
    source = Seq(("1", "8"), ("3", "10"), ("0", "3")),
    condition = "src.key1 = key2 and key2 < '3'",
    expectedResults =
      ("3", "2", "noop") ::
      ("4", "4", "noop") ::
      ("1", "8", "update") ::
      ("1", "8", "update") ::
      ("0", "3", "insert") ::
      ("3", "10", "insert") ::
      Nil,
    numFilesPerPartition = 1
  )

  Seq(true, false).foreach { isPartitioned =>
    test(s"basic case - column pruning, isPartitioned: $isPartitioned") {
      withTable("source") {
        val partitions = if (isPartitioned) "key2" :: Nil else Nil
        append(Seq((2, 2), (1, 4)).toDF("key2", "value"), partitions)
        Seq((1, 1, "a"), (0, 3, "b")).toDF("key1", "value", "col1")
          .createOrReplaceTempView("source")

        executeMerge(
          target = s"delta.`$tempPath`",
          source = "source src",
          condition = "src.key1 = key2",
          update = "key2 = 20 + key1, value = 20 + src.value",
          insert = "(key2, value) VALUES (key1 - 10, src.value + 10)")

        checkAnswer(readDeltaTable(tempPath),
          Row(2, 2) :: // No change
            Row(21, 21) :: // Update
            Row(-10, 13) :: // Insert
            Nil)
      }
    }
  }

  test("merge into cached table") {
    // Merge with a cached target only works in the join-based implementation right now
    withTable("source") {
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))
      Seq((1, 1), (0, 3), (3, 3)).toDF("key1", "value").createOrReplaceTempView("source")
      spark.table(s"delta.`$tempPath`").cache()
      spark.table(s"delta.`$tempPath`").collect()

      append(Seq((100, 100), (3, 5)).toDF("key2", "value"))
      // cache is in effect, as the above change is not reflected
      checkAnswer(spark.table(s"delta.`$tempPath`"),
        Row(2, 2) :: Row(1, 4) :: Row(100, 100) :: Row(3, 5) :: Nil)

      executeMerge(
        target = s"delta.`$tempPath` as trgNew",
        source = "source src",
        condition = "src.key1 = key2",
        update = "value = trgNew.value + 3",
        insert = "(key2, value) VALUES (key1, src.value + 10)")

      checkAnswer(spark.table(s"delta.`$tempPath`"),
        Row(100, 100) :: // No change (newly inserted record)
          Row(2, 2) :: // No change
          Row(1, 7) :: // Update
          Row(3, 8) :: // Update (on newly inserted record)
          Row(0, 13) :: // Insert
          Nil)
    }
  }

  // scalastyle:off argcount
  def testNestedDataSupport(name: String, namePrefix: String = "nested data support")(
      source: String,
      target: String,
      update: Seq[String],
      insert: String = null,
      targetSchema: StructType = null,
      sourceSchema: StructType = null,
      result: String = null,
      errorStrs: Seq[String] = null,
      confs: Seq[(String, String)] = Seq.empty): Unit = {
    // scalastyle:on argcount

    require(result == null ^ errorStrs == null, "either set the result or the error strings")

    val testName =
      if (result != null) s"$namePrefix - $name" else s"$namePrefix - analysis error - $name"

    test(testName) {
      withSQLConf(confs: _*) {
        withJsonData(source, target, targetSchema, sourceSchema) { case (sourceName, targetName) =>
          val fieldNames = spark.table(targetName).schema.fieldNames
          val fieldNamesStr = fieldNames.mkString("`", "`, `", "`")
          val keyName = s"`${fieldNames.head}`"

          def execMerge() = executeMerge(
            target = s"$targetName t",
            source = s"$sourceName s",
            condition = s"s.$keyName = t.$keyName",
            update = update.mkString(", "),
            insert = Option(insert).getOrElse(s"($fieldNamesStr) VALUES ($fieldNamesStr)"))

          if (result != null) {
            execMerge()
            val expectedDf = readFromJSON(strToJsonSeq(result), targetSchema)
            checkAnswer(spark.table(targetName), expectedDf)
          } else {
            val e = intercept[AnalysisException] {
              execMerge()
            }
            errorStrs.foreach { s => errorContains(e.getMessage, s) }
          }
        }
      }
    }
  }

  testNestedDataSupport("no update when not matched, only insert")(
    source = """
        { "key": { "x": "X3", "y": 3}, "value": { "a": 300, "b": "B300" } }""",
    target = """
        { "key": { "x": "X1", "y": 1}, "value": { "a": 1,   "b": "B1" } }
        { "key": { "x": "X2", "y": 2}, "value": { "a": 2,   "b": "B2" } }""",
    update = "value.b = 'UPDATED'" :: Nil,
    result = """
        { "key": { "x": "X1", "y": 1}, "value": { "a": 1,   "b": "B1" } }
        { "key": { "x": "X2", "y": 2}, "value": { "a": 2,   "b": "B2"      } }
        { "key": { "x": "X3", "y": 3}, "value": { "a": 300, "b": "B300"    } }""")

  testNestedDataSupport("update entire nested column")(
    source = """
        { "key": { "x": "X1", "y": 1}, "value": { "a": 100, "b": "B100" } }""",
    target = """
        { "key": { "x": "X1", "y": 1}, "value": { "a": 1,   "b": "B1" } }
        { "key": { "x": "X2", "y": 2}, "value": { "a": 2,   "b": "B2" } }""",
    update = "value = s.value" :: Nil,
    result = """
        { "key": { "x": "X1", "y": 1}, "value": { "a": 100, "b": "B100" } }
        { "key": { "x": "X2", "y": 2}, "value": { "a": 2, "b": "B2"   } }""")

  testNestedDataSupport("update one nested field")(
    source = """
        { "key": { "x": "X1", "y": 1}, "value": { "a": 100, "b": "B100" } }""",
    target = """
        { "key": { "x": "X1", "y": 1}, "value": { "a": 1,   "b": "B1" } }
        { "key": { "x": "X2", "y": 2}, "value": { "a": 2,   "b": "B2" } }""",
    update = "value.b = s.value.b" :: Nil,
    result = """
        { "key": { "x": "X1", "y": 1}, "value": { "a": 1, "b": "B100" } }
        { "key": { "x": "X2", "y": 2}, "value": { "a": 2, "b": "B2"   } }""")

  testNestedDataSupport("update multiple fields at different levels")(
    source = """
        { "key": { "x": "X1", "y": { "i": 1.0 } }, "value": { "a": 100, "b": "B100" } }""",
    target = """
        { "key": { "x": "X1", "y": { "i": 1.0 } }, "value": { "a": 1,   "b": "B1" } }
        { "key": { "x": "X2", "y": { "i": 2.0 } }, "value": { "a": 2,   "b": "B2" } }""",
    update =
      "key.x = 'XXX'" :: "key.y.i = 9000" ::
      "value = named_struct('a', 9000, 'b', s.value.b)" :: Nil,
    result = """
        { "key": { "x": "XXX", "y": { "i": 9000 } }, "value": { "a": 9000, "b": "B100" } }
        { "key": { "x": "X2" , "y": { "i": 2.0  } }, "value": { "a": 2, "b": "B2" } }""")

  testNestedDataSupport("update multiple fields at different levels to NULL")(
    source = """
        { "key": { "x": "X1", "y": { "i": 1.0 } }, "value": { "a": 100, "b": "B100" } }""",
    target = """
        { "key": { "x": "X1", "y": { "i": 1.0 } }, "value": { "a": 1,   "b": "B1" } }
        { "key": { "x": "X2", "y": { "i": 2.0 } }, "value": { "a": 2,   "b": "B2" } }""",
    update = "value = NULL" :: "key.x = NULL" :: "key.y.i = NULL" :: Nil,
    result = """
        { "key": { "x": null, "y": { "i" : null } }, "value": null }
        { "key": { "x": "X2" , "y": { "i" : 2.0  } }, "value": { "a": 2, "b": "B2" } }""")

  testNestedDataSupport("update multiple fields at different levels with implicit casting")(
    source = """
        { "key": { "x": "X1", "y": { "i": 1.0 } }, "value": { "a": 100, "b": "B100" } }""",
    target = """
        { "key": { "x": "X1", "y": { "i": 1.0 } }, "value": { "a": 1,   "b": "B1" } }
        { "key": { "x": "X2", "y": { "i": 2.0 } }, "value": { "a": 2,   "b": "B2" } }""",
    update =
      "key.x = 'XXX' " :: "key.y.i = '9000'" ::
      "value = named_struct('a', '9000', 'b', s.value.b)" :: Nil,
    result = """
        { "key": { "x": "XXX", "y": { "i": 9000 } }, "value": { "a": 9000, "b": "B100" } }
        { "key": { "x": "X2" , "y": { "i": 2.0  } }, "value": { "a": 2, "b": "B2" } }""")

  testNestedDataSupport("update array fields at different levels")(
    source = """
        { "key": { "x": "X1", "y": [ 1, 11 ] }, "value": [ -1, -10 , -100 ] }""",
    target = """
        { "key": { "x": "X1", "y": [ 1, 11 ] }, "value": [ 1, 10 , 100 ]} }
        { "key": { "x": "X2", "y": [ 2, 22 ] }, "value": [ 2, 20 , 200 ]} }""",
    update = "value = array(-9000)" :: "key.y = array(-1, -11)" :: Nil,
    result = """
        { "key": { "x": "X1", "y": [ -1, -11 ] }, "value": [ -9000 ]} }
        { "key": { "x": "X2", "y": [ 2, 22 ] }, "value": [ 2, 20 , 200 ]} }""")

  testNestedDataSupport("update using quoted names at different levels", "dotted name support")(
    source = """
        { "key": { "x": "X1", "y.i": 1.0 }, "value.a": "A" }""",
    target = """
        { "key": { "x": "X1", "y.i": 1.0 }, "value.a": "A1" }
        { "key": { "x": "X2", "y.i": 2.0 }, "value.a": "A2" }""",
    update = "`t`.key.`y.i` = 9000" ::  "t.`value.a` = 'UPDATED'" :: Nil,
    result = """
        { "key": { "x": "X1", "y.i": 9000 }, "value.a": "UPDATED" }
        { "key": { "x": "X2", "y.i" : 2.0 }, "value.a": "A2" }""")

  testNestedDataSupport("unknown nested field")(
    source = """{ "key": "A", "value": { "a": 0 } }""",
    target = """{ "key": "A", "value": { "a": 1 } }""",
    update = "value.c = 'UPDATED'" :: Nil,
    errorStrs = "No such struct field" :: Nil)

  testNestedDataSupport("assigning simple type to struct field")(
    source = """{ "key": "A", "value": { "a": { "x": 1 } } }""",
    target = """{ "key": "A", "value": { "a": { "x": 1 } } }""",
    update = "value.a = 'UPDATED'" :: Nil,
    errorStrs = "data type mismatch" :: Nil)

  testNestedDataSupport("conflicting assignments between two nested fields at different levels")(
    source = """{ "key": "A", "value": { "a": { "x": 0 } } }""",
    target = """{ "key": "A", "value": { "a": { "x": 1 } } }""",
    update = "value.a.x = 2" :: "value.a = named_struct('x', 3)" :: Nil,
    errorStrs = "There is a conflict from these SET columns" :: Nil)

  testNestedDataSupport("conflicting assignments between nested field and top-level column")(
    source = """{ "key": "A", "value": { "a": 0 } }""",
    target = """{ "key": "A", "value": { "a": 1 } }""",
    update = "value.a = 2" :: "value = named_struct('a', 3)" :: Nil,
    errorStrs = "There is a conflict from these SET columns" :: Nil)

  testNestedDataSupport("nested field not supported in INSERT")(
    source = """{ "key": "A", "value": { "a": 0 } }""",
    target = """{ "key": "B", "value": { "a": 1 } }""",
    update = "value.a = 2" :: Nil,
    insert = """(key, value.a) VALUES (s.key, s.value.a)""",
    errorStrs = "Nested field is not supported in the INSERT clause" :: Nil)

  testNestedDataSupport("updating map type")(
    source = """{ "key": "A", "value": { "a": 0 } }""",
    target = """{ "key": "A", "value": { "a": 1 } }""",
    update = "value.a = 2" :: Nil,
    targetSchema =
      new StructType().add("key", StringType).add("value", MapType(StringType, IntegerType)),
    errorStrs = "Updating nested fields is only supported for StructType" :: Nil)

  testNestedDataSupport("updating array type")(
    source = """{ "key": "A", "value": [ { "a": 0 } ] }""",
    target = """{ "key": "A", "value": [ { "a": 1 } ] }""",
    update = "value.a = 2" :: Nil,
    targetSchema =
      new StructType().add("key", StringType).add("value", MapType(StringType, IntegerType)),
    errorStrs = "Updating nested fields is only supported for StructType" :: Nil)

  testNestedDataSupport("resolution by name - update specific column")(
    source = """{ "key": "A", "value": { "b": 2, "a": { "y": 20, "x": 10} } }""",
    target = """{ "key": "A", "value": { "a": { "x": 1, "y": 2 }, "b": 1 }}""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", new StructType().add("x", IntegerType).add("y", IntegerType))
        .add("b", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("b", IntegerType)
        .add("a", new StructType().add("y", IntegerType).add("x", IntegerType))),
    update = "value.a = s.value.a",
    result = """{ "key": "A", "value": { "a": { "x": 10, "y": 20 }, "b": 1 } }""")

  // scalastyle:off line.size.limit
  testNestedDataSupport("resolution by name - update specific column - array of struct - longer source")(
    source = """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": 10 } }, { "b": "3", "a": { "y": 30, "x": 40 } } ] }""",
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType().add("x", IntegerType).add("y", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    update = "value = s.value",
    result = """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20 }, "b": 2 }, { "a": { "y": 30, "x": 40}, "b": 3 } ] }""")

  testNestedDataSupport("resolution by name - update specific column - array of struct - longer target")(
    source = """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": 10 } } ] }""",
    target = """{ "key": "A", "value": [{ "a": { "x": 1, "y": 2 }, "b": 1 }, { "a": { "x": 2, "y": 3 }, "b": 2 }] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType().add("x", IntegerType).add("y", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    update = "value = s.value",
    result = """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20 }, "b": 2 } ] }""")

  testNestedDataSupport("resolution by name - update specific column - nested array of struct - longer source")(
    source = """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": [ { "c": 10, "d": "30" }, { "c": 3, "d": "40" } ] } } ] }""",
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3} ] }, "b": 1 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType))))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
            ))))),
    update = "value = s.value",
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30 }, { "c": 3, "d": 40 } ] }, "b": 2 } ] }""")

  testNestedDataSupport("resolution by name - update specific column - nested array of struct - longer target")(
    source = """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": [ { "c": 10, "d": "30" } ] } } ] }""",
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3 }, { "c": 2, "d": 4 } ] }, "b": 1 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType))))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
            ))))),
    update = "value = s.value",
    result = """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30}]}, "b": 2 } ] }""")
  // scalastyle:on line.size.limit

  testNestedDataSupport("resolution by name - update *")(
    source = """{ "key": "A", "value": { "b": 2, "a": { "y": 20, "x": 10} } }""",
    target = """{ "key": "A", "value": { "a": { "x": 1, "y": 2 }, "b": 1 }}""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", new StructType().add("x", IntegerType).add("y", IntegerType))
        .add("b", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("b", IntegerType)
        .add("a", new StructType().add("y", IntegerType).add("x", IntegerType))),
    update = "*",
    result = """{ "key": "A", "value": { "a": { "x": 10, "y": 20 } , "b": 2} }""")

  // scalastyle:off line.size.limit
  testNestedDataSupport("resolution by name - update * - array of struct - longer source")(
    source = """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": 10 } }, { "b": "3", "a": { "y": 30, "x": 40 } } ] }""",
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType().add("x", IntegerType).add("y", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    update = "*",
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20 }, "b": 2 }, { "a": { "y": 30, "x": 40}, "b": 3 } ] }""".stripMargin)

  testNestedDataSupport("resolution by name - update * - array of struct - longer target")(
    source = """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": 10 } } ] }""",
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1 }, { "a": { "x": 2, "y": 3 }, "b": 4 }] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType().add("x", IntegerType).add("y", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    update = "*",
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20 }, "b": 2 } ] }""".stripMargin)

  testNestedDataSupport("resolution by name - update * - nested array of struct - longer source")(
    source = """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": [{ "c": 10, "d": "30"}, { "c": 3, "d": "40" } ] } } ] }""",
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3 } ] }, "b": 1 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType))))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
            ))))),
    update = "*",
    result = """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30}, { "c": 3, "d": 40 } ] }, "b": 2 } ] }""")

  testNestedDataSupport("resolution by name - update * - nested array of struct - longer target")(
    source = """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": [ { "c": 10, "d": "30" } ] } } ] }""",
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3}, { "c": 2, "d": 4} ] }, "b": 1 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType))))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
            ))))),
    update = "*",
    result = """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30 } ] }, "b": 2 } ] }""")
  // scalastyle:on line.size.limit

  testNestedDataSupport("resolution by name - insert specific column")(
    source = """{ "key": "B", "value": { "b": 2, "a": { "y": 20, "x": 10 } } }""",
    target = """{ "key": "A", "value": { "a": { "x": 1, "y": 2 }, "b": 1 } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", new StructType().add("x", IntegerType).add("y", IntegerType))
        .add("b", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("b", IntegerType)
        .add("a", new StructType().add("y", IntegerType).add("x", IntegerType))),
    update = "*",
    insert = "(key, value) VALUES (s.key, s.value)",
    result =
      """
        |{ "key": "A", "value": { "a": { "x": 1, "y": 2 }, "b": 1 } },
        |{ "key": "B", "value": { "a": { "x": 10, "y": 20 }, "b": 2 } }""".stripMargin)

  // scalastyle:off line.size.limit
  testNestedDataSupport("resolution by name - insert specific column - array of struct")(
    source = """{ "key": "B", "value": [ { "b": "2", "a": { "y": 20, "x": 10 } }, { "b": "3", "a": { "y": 30, "x": 40 } } ] }""",
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType().add("x", IntegerType).add("y", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    update = "*",
    insert = "(key, value) VALUES (s.key, s.value)",
    result =
      """ { "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1 } ] },
            { "key": "B", "value": [ { "a": { "x": 10, "y": 20 }, "b": 2 }, { "a": { "y": 30, "x": 40}, "b": 3 } ] }""")

  testNestedDataSupport("resolution by name - insert specific column - nested array of struct")(
    source = """{ "key": "B", "value": [ { "b": "2", "a": { "y": 20, "x": [ { "c": 10, "d": "30" }, { "c": 3, "d": "40" } ] } } ] }""",
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3 } ] }, "b": 1 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType))))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
            ))))),
    update = "*",
    insert = "(key, value) VALUES (s.key, s.value)",
    result =
      """
       { "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3 } ] }, "b": 1 } ] },
        { "key": "B", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30 }, { "c": 3, "d": 40 } ] }, "b": 2 } ] }""")
  // scalastyle:on line.size.limit

  testNestedDataSupport("resolution by name - insert *")(
    source = """{ "key": "B", "value": { "b": 2, "a": { "y": 20, "x": 10} } }""",
    target = """{ "key": "A", "value": { "a": { "x": 1, "y": 2 }, "b": 1 } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", new StructType().add("x", IntegerType).add("y", IntegerType))
        .add("b", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("b", IntegerType)
        .add("a", new StructType().add("y", IntegerType).add("x", IntegerType))),
    update = "*",
    insert = "*",
    result =
      """
        |{ "key": "A", "value": { "a": { "x": 1, "y": 2 }, "b": 1 } },
        |{ "key": "B", "value": { "a": { "x": 10, "y": 20 }, "b": 2 } }""".stripMargin)

  // scalastyle:off line.size.limit
  testNestedDataSupport("resolution by name - insert * - array of struct")(
    source = """{ "key": "B", "value": [ { "b": "2", "a": { "y": 20, "x": 10} }, { "b": "3", "a": { "y": 30, "x": 40 } } ] }""",
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
        .add("a", new StructType().add("x", IntegerType).add("y", IntegerType))
        .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
        .add("b", StringType)
        .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    update = "*",
    insert = "*",
    result =
      """
        |{ "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1 } ] },
        |{ "key": "B", "value": [ { "a": { "x": 10, "y": 20 }, "b": 2 }, { "a": { "y": 30, "x": 40}, "b": 3 } ] }""".stripMargin)

  testNestedDataSupport("resolution by name - insert * - nested array of struct")(
    source = """{ "key": "B", "value": [{ "b": "2", "a": { "y": 20, "x": [ { "c": 10, "d": "30"}, { "c": 3, "d": "40"} ] } } ] }""",
    target = """{ "key": "A", "value": [{ "a": { "y": 2, "x": [ { "c": 1, "d": 3} ] }, "b": 1 }] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
              .add("y", IntegerType)
              .add("x", ArrayType(
                new StructType()
                  .add("c", IntegerType)
                  .add("d", IntegerType))))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
            ))))),
    update = "*",
    insert = "*",
    result =
      """
        |{ "key": "A", "value": [{ "a": { "y": 2, "x": [{ "c": 1, "d": 3}]}, "b": 1 }] },
        |{ "key": "B", "value": [{ "a": { "y": 20, "x": [{ "c": 10, "d": 30}, { "c": 3, "d": 40}]}, "b": 2 }]}""".stripMargin)
  // scalastyle:on line.size.limit

  // Note that value.b has to be in the right position for this test to avoid throwing an error
  // trying to write its integer value into the value.a struct.
  testNestedDataSupport("update resolution by position with conf")(
    source = """{ "key": "A", "value": { "a": { "y": 20, "x": 10}, "b": 2 }}""",
    target = """{ "key": "A", "value": { "a": { "x": 1, "y": 2 }, "b": 1 } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", new StructType().add("x", IntegerType).add("y", IntegerType))
        .add("b", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
        .add("a", new StructType().add("y", IntegerType).add("x", IntegerType))
        .add("b", IntegerType)),
    update = "*",
    insert = "(key, value) VALUES (s.key, s.value)",
    result = """{ "key": "A", "value": { "a": { "x": 20, "y": 10 }, "b": 2 } }""",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)


  /**
   * Test runner to cover analysis exception in MERGE INTO.
   */
  protected def testMergeAnalysisException(
      name: String)(
      mergeOn: String,
      mergeClauses: MergeClause*)(
      expectedErrorClass: String,
      expectedMessageParameters: Map[String, String]): Unit = {
    test(s"analysis errors - $name") {
      withKeyValueData(
        source = Seq.empty,
        target = Seq.empty,
        sourceKeyValueNames = ("key", "srcValue"),
        targetKeyValueNames = ("key", "tgtValue")) { case (sourceName, targetName) =>
        val ex = intercept[AnalysisException] {
          executeMerge(s"$targetName t", s"$sourceName s", mergeOn, mergeClauses: _*)
        }

        // Spark 3.5 and below uses QueryContext, Spark 4.0 and above uses ExpectedContext.
        // Implicitly convert to ExpectedContext when needed for compatibility.
        implicit def toExpectedContext(ctxs: Array[QueryContext]): Array[ExpectedContext] =
          ctxs.map { ctx =>
            ExpectedContext(ctx.fragment(), ctx.startIndex(), ctx.stopIndex())
          }

        checkError(
          exception = ex,
          errorClass = expectedErrorClass,
          parameters = expectedMessageParameters,
          queryContext = ex.getQueryContext
        )
      }
    }
  }

  testMergeAnalysisException("update condition - ambiguous reference")(
    mergeOn = "s.key = t.key",
    update(condition = "key > 1", set = "tgtValue = srcValue"))(
    expectedErrorClass = "AMBIGUOUS_REFERENCE",
    expectedMessageParameters = Map(
      "name" -> "`key`",
      "referenceNames" -> "[`s`.`key`, `t`.`key`]"))

  testMergeAnalysisException("update condition - unknown reference")(
    mergeOn = "s.key = t.key",
    update(condition = "unknownAttrib > 1", set = "tgtValue = srcValue"))(
    // Should show unknownAttrib as invalid ref and (key, tgtValue, srcValue) as valid column names.
    expectedErrorClass = "DELTA_MERGE_UNRESOLVED_EXPRESSION",
    expectedMessageParameters = Map(
      "sqlExpr" -> "unknownAttrib",
      "clause" -> "UPDATE condition",
      "cols" -> "t.key, t.tgtValue, s.key, s.srcValue"))

  testMergeAnalysisException("update condition - aggregation function")(
    mergeOn = "s.key = t.key",
    update(condition = "max(0) > 0", set = "tgtValue = srcValue"))(
    expectedErrorClass = "DELTA_AGGREGATION_NOT_SUPPORTED",
    expectedMessageParameters = Map(
      "operation" -> "UPDATE condition of MERGE operation",
      "predicate" -> "(condition = (max(0) > 0))."))

  testMergeAnalysisException("update condition - subquery")(
    mergeOn = "s.key = t.key",
    update(condition = "s.srcValue in (select value from t)", set = "tgtValue = srcValue"))(
    expectedErrorClass = "TABLE_OR_VIEW_NOT_FOUND",
    expectedMessageParameters = Map("relationName" -> "`t`"))

  testMergeAnalysisException("delete condition - ambiguous reference")(
    mergeOn = "s.key = t.key",
    delete(condition = "key > 1"))(
    expectedErrorClass = "AMBIGUOUS_REFERENCE",
    expectedMessageParameters = Map(
      "name" -> "`key`",
      "referenceNames" -> "[`s`.`key`, `t`.`key`]"))

  testMergeAnalysisException("delete condition - unknown reference")(
    mergeOn = "s.key = t.key",
    delete(condition = "unknownAttrib > 1"))(
    // Should show unknownAttrib as invalid ref and (key, tgtValue, srcValue) as valid column names.
    expectedErrorClass = "DELTA_MERGE_UNRESOLVED_EXPRESSION",
    expectedMessageParameters = Map(
      "sqlExpr" -> "unknownAttrib",
      "clause" -> "DELETE condition",
      "cols" -> "t.key, t.tgtValue, s.key, s.srcValue"))

  testMergeAnalysisException("delete condition - aggregation function")(
    mergeOn = "s.key = t.key",
    delete(condition = "max(0) > 0"))(
    expectedErrorClass = "DELTA_AGGREGATION_NOT_SUPPORTED",
    expectedMessageParameters = Map(
      "operation" -> "DELETE condition of MERGE operation",
      "predicate" -> "(condition = (max(0) > 0))."))

  testMergeAnalysisException("delete condition - subquery")(
    mergeOn = "s.key = t.key",
    delete(condition = "s.srcValue in (select tgtValue from t)"))(
    expectedErrorClass = "TABLE_OR_VIEW_NOT_FOUND",
    expectedMessageParameters = Map("relationName" -> "`t`"))

  testMergeAnalysisException("insert condition - unknown reference")(
    mergeOn = "s.key = t.key",
    insert(condition = "unknownAttrib > 1", values = "(key, tgtValue) VALUES (s.key, s.srcValue)"))(
    // Should show unknownAttrib as invalid ref and (key, srcValue) as valid column names,
    // but not show tgtValue as a valid name as target columns cannot be present in insert clause.
    expectedErrorClass = "DELTA_MERGE_UNRESOLVED_EXPRESSION",
    expectedMessageParameters = Map(
      "sqlExpr" -> "unknownAttrib",
      "clause" -> "INSERT condition",
      "cols" -> "s.key, s.srcValue"))

  testMergeAnalysisException("insert condition - reference to target table column")(
    mergeOn = "s.key = t.key",
    insert(condition = "tgtValue > 1", values = "(key, tgtValue) VALUES (s.key, s.srcValue)"))(
    // Should show tgtValue as invalid ref and (key, srcValue) as valid column names
    expectedErrorClass = "DELTA_MERGE_UNRESOLVED_EXPRESSION",
    expectedMessageParameters = Map(
      "sqlExpr" -> "tgtValue",
      "clause" -> "INSERT condition",
      "cols" -> "s.key, s.srcValue"))

  testMergeAnalysisException("insert condition - aggregation function")(
    mergeOn = "s.key = t.key",
    insert(condition = "max(0) > 0", values = "(key, tgtValue) VALUES (s.key, s.srcValue)"))(
    expectedErrorClass = "DELTA_AGGREGATION_NOT_SUPPORTED",
    expectedMessageParameters = Map(
      "operation" -> "INSERT condition of MERGE operation",
      "predicate" -> "(condition = (max(0) > 0))."))

  testMergeAnalysisException("insert condition - subquery")(
    mergeOn = "s.key = t.key",
    insert(
      condition = "s.srcValue in (select srcValue from s)",
      values = "(key, tgtValue) VALUES (s.key, s.srcValue)"))(
    expectedErrorClass = "TABLE_OR_VIEW_NOT_FOUND",
    expectedMessageParameters = Map("relationName" -> "`s`"))


  protected def testExtendedMerge(
      name: String,
      namePrefix: String = "extended syntax")(
      source: Seq[(Int, Int)],
      target: Seq[(Int, Int)],
      mergeOn: String,
      mergeClauses: MergeClause*)(
      result: Seq[(Int, Int)]): Unit = {
    Seq(true, false).foreach { isPartitioned =>
      test(s"$namePrefix - $name - isPartitioned: $isPartitioned ") {
        withKeyValueData(source, target, isPartitioned) { case (sourceName, targetName) =>
          withSQLConf(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED.key -> "true") {
            executeMerge(s"$targetName t", s"$sourceName s", mergeOn, mergeClauses: _*)
          }
          val deltaPath = if (targetName.startsWith("delta.`")) {
            targetName.stripPrefix("delta.`").stripSuffix("`")
          } else targetName
          checkAnswer(
            readDeltaTable(deltaPath),
            result.map { case (k, v) => Row(k, v) })
        }
      }
    }
  }

  protected def testMergeErrorOnMultipleMatches(
      name: String,
      confs: Seq[(String, String)] = Seq())(
      source: Seq[(Int, Int)],
      target: Seq[(Int, Int)],
      mergeOn: String,
      mergeClauses: MergeClause*): Unit = {
    test(s"extended syntax - $name") {
      withSQLConf(confs: _*) {
        withKeyValueData(source, target) { case (sourceName, targetName) =>
          val docURL = "/delta-update.html#upsert-into-a-table-using-merge"

          checkError(
            exception = intercept[DeltaUnsupportedOperationException] {
              executeMerge(s"$targetName t", s"$sourceName s", mergeOn, mergeClauses: _*)
            },
            errorClass = "DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE",
            parameters = Map(
              "usageReference" -> DeltaErrors.generateDocsLink(
                spark.sparkContext.getConf, docURL, skipValidation = true))
          )
        }
      }
    }
  }

  testExtendedMerge("only update")(
    source = (0, 0) :: (1, 10) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2)  :: Nil,
    mergeOn = "s.key = t.key",
    update(set = "key = s.key, value = s.value"))(
    result = Seq(
      (1, 10),  // (1, 1) updated
      (2, 2)
    ))

  testMergeErrorOnMultipleMatches("only update with multiple matches")(
    source = (0, 0) :: (1, 10) :: (1, 11) :: (2, 20) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    update(set = "key = s.key, value = s.value"))

  testExtendedMerge("only conditional update")(
    source = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2) :: (3, 3) :: Nil,
    mergeOn = "s.key = t.key",
    update(condition = "s.value <> 20 AND t.value <> 3", set = "key = s.key, value = s.value"))(
    result = Seq(
      (1, 10),  // updated
      (2, 2),   // not updated due to source-only condition `s.value <> 20`
      (3, 3)    // not updated due to target-only condition `t.value <> 3`
    ))

  testMergeErrorOnMultipleMatches("only conditional update with multiple matches")(
    source = (0, 0) :: (1, 10) :: (1, 11) :: (2, 20) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    update(condition = "s.value = 10", set = "key = s.key, value = s.value"))

  testExtendedMerge("only delete")(
    source = (0, 0) :: (1, 10) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    delete())(
    result = Seq(
      (2, 2)    // (1, 1) deleted
    ))          // (3, 30) not inserted as not insert clause

  // This is not ambiguous even when there are multiple matches
  testExtendedMerge(s"only delete with multiple matches")(
    source = (0, 0) :: (1, 10) :: (1, 100) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    delete())(
    result = Seq(
      (2, 2)  // (1, 1) matches multiple source rows but unambiguously deleted
    )
  )

  testExtendedMerge("only conditional delete")(
    source = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2) :: (3, 3) :: Nil,
    mergeOn = "s.key = t.key",
    delete(condition = "s.value <> 20 AND t.value <> 3"))(
    result = Seq(
      (2, 2),   // not deleted due to source-only condition `s.value <> 20`
      (3, 3)    // not deleted due to target-only condition `t.value <> 3`
    ))          // (1, 1) deleted

  testMergeErrorOnMultipleMatches("only conditional delete with multiple matches")(
    source = (0, 0) :: (1, 10) :: (1, 100) :: (2, 20) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    delete(condition = "s.value = 10"))

  testExtendedMerge("conditional update + delete")(
    source = (0, 0) :: (1, 10) :: (2, 20) :: Nil,
    target = (1, 1) :: (2, 2) :: (3, 3) :: Nil,
    mergeOn = "s.key = t.key",
    update(condition = "s.key <> 1", set = "key = s.key, value = s.value"),
    delete())(
    result = Seq(
      (2, 20),  // (2, 2) updated, (1, 1) deleted as it did not match update condition
      (3, 3)
    ))

  testMergeErrorOnMultipleMatches("conditional update + delete with multiple matches")(
    source = (0, 0) :: (1, 10) :: (2, 20) :: (2, 200) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    update(condition = "s.value = 20", set = "key = s.key, value = s.value"),
    delete())

  testExtendedMerge("conditional update + conditional delete")(
    source = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2) :: (3, 3) :: (4, 4) :: Nil,
    mergeOn = "s.key = t.key",
    update(condition = "s.key <> 1", set = "key = s.key, value = s.value"),
    delete(condition = "s.key <> 2"))(
    result = Seq(
      (2, 20),  // (2, 2) updated as it matched update condition
      (3, 30),  // (3, 3) updated even though it matched update and delete conditions, as update 1st
      (4, 4)
    ))          // (1, 1) deleted as it matched delete condition

  testMergeErrorOnMultipleMatches(
    "conditional update + conditional delete with multiple matches")(
    source = (0, 0) :: (1, 10) :: (1, 100) :: (2, 20) :: (2, 200) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    update(condition = "s.value = 20", set = "key = s.key, value = s.value"),
    delete(condition = "s.value = 10"))

  testExtendedMerge("conditional delete + conditional update (order matters)")(
    source = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2) :: (3, 3) :: (4, 4) :: Nil,
    mergeOn = "s.key = t.key",
    delete(condition = "s.key <> 2"),
    update(condition = "s.key <> 1", set = "key = s.key, value = s.value"))(
    result = Seq(
      (2, 20),  // (2, 2) updated as it matched update condition
      (4, 4)    // (4, 4) unchanged
    ))          // (1, 1) and (3, 3) deleted as they matched delete condition (before update cond)

  testExtendedMerge("only insert")(
    source = (0, 0) :: (1, 10) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    insert(values = "(key, value) VALUES (s.key, s.value)"))(
    result = Seq(
      (0, 0),   // (0, 0) inserted
      (1, 1),   // (1, 1) not updated as no update clause
      (2, 2),   // (2, 2) not updated as no update clause
      (3, 30)   // (3, 30) inserted
    ))

  testExtendedMerge("only conditional insert")(
    source = (0, 0) :: (1, 10) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    insert(condition = "s.value <> 30", values = "(key, value) VALUES (s.key, s.value)"))(
    result = Seq(
      (0, 0),   // (0, 0) inserted by condition but not (3, 30)
      (1, 1),
      (2, 2)
    ))

  testExtendedMerge("update + conditional insert")(
    source = (0, 0) :: (1, 10) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    update("key = s.key, value = s.value"),
    insert(condition = "s.value <> 30", values = "(key, value) VALUES (s.key, s.value)"))(
    result = Seq(
      (0, 0),   // (0, 0) inserted by condition but not (3, 30)
      (1, 10),  // (1, 1) updated
      (2, 2)
    ))

  testExtendedMerge("conditional update + conditional insert")(
    source = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    update(condition = "s.key > 1", set = "key = s.key, value = s.value"),
    insert(condition = "s.key > 1", values = "(key, value) VALUES (s.key, s.value)"))(
    result = Seq(
      (1, 1),   // (1, 1) not updated by condition
      (2, 20),  // (2, 2) updated by condition
      (3, 30)   // (3, 30) inserted by condition but not (0, 0)
    ))

  // This is specifically to test the MergeIntoDeltaCommand.writeOnlyInserts code paths
  testExtendedMerge("update + conditional insert clause with data to only insert, no updates")(
    source = (0, 0) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    update("key = s.key, value = s.value"),
    insert(condition = "s.value <> 30", values = "(key, value) VALUES (s.key, s.value)"))(
    result = Seq(
      (0, 0),   // (0, 0) inserted by condition but not (3, 30)
      (1, 1),
      (2, 2)
    ))

  testExtendedMerge(s"delete + insert with multiple matches for both") (
    source = (1, 10) :: (1, 100) :: (3, 30) :: (3, 300) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    delete(),
    insert(values = "(key, value) VALUES (s.key, s.value)")) (
    result = Seq(
               // (1, 1) matches multiple source rows but unambiguously deleted
      (2, 2),  // existed previously
      (3, 30), // inserted
      (3, 300) // inserted
    )
  )

  testExtendedMerge("conditional update + conditional delete + conditional insert")(
    source = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: (4, 40) :: Nil,
    target = (1, 1) :: (2, 2) :: (3, 3) :: Nil,
    mergeOn = "s.key = t.key",
    update(condition = "s.key < 2", set = "key = s.key, value = s.value"),
    delete(condition = "s.key < 3"),
    insert(condition = "s.key > 1", values = "(key, value) VALUES (s.key, s.value)"))(
    result = Seq(
      (1, 10),  // (1, 1) updated by condition, but not (2, 2) or (3, 3)
      (3, 3),   // neither updated nor deleted as it matched neither condition
      (4, 40)   // (4, 40) inserted by condition, but not (0, 0)
    ))          // (2, 2) deleted by condition but not (1, 1) or (3, 3)

  testMergeErrorOnMultipleMatches(
    "conditional update + conditional delete + conditional insert with multiple matches")(
    source = (0, 0) :: (1, 10) :: (1, 100) :: (2, 20) :: (2, 200) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    update(condition = "s.value = 20", set = "key = s.key, value = s.value"),
    delete(condition = "s.value = 10"),
    insert(condition = "s.value = 0", values = "(key, value) VALUES (s.key, s.value)"))

  // complex merge condition = has target-only and source-only predicates
  testExtendedMerge(
    "conditional update + conditional delete + conditional insert + complex merge condition ")(
    source = (-1, -10) :: (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: (4, 40) :: (5, 50) :: Nil,
    target = (-1, -1) :: (1, 1) :: (2, 2) :: (3, 3) :: (5, 5) :: Nil,
    mergeOn = "s.key = t.key AND t.value > 0 AND s.key < 5",
    update(condition = "s.key < 2", set = "key = s.key, value = s.value"),
    delete(condition = "s.key < 3"),
    insert(condition = "s.key > 1", values = "(key, value) VALUES (s.key, s.value)"))(
    result = Seq(
      (-1, -1), // (-1, -1) not matched with (-1, -10) by target-only condition 't.value > 0', so
                // not updated, But (-1, -10) not inserted as insert condition is 's.key > 1'
                // (0, 0) not matched any target but not inserted as insert condition is 's.key > 1'
      (1, 10),  // (1, 1) matched with (1, 10) and updated as update condition is 's.key < 2'
                // (2, 2) matched with (2, 20) and deleted as delete condition is 's.key < 3'
      (3, 3),   // (3, 3) matched with (3, 30) but neither updated nor deleted as it did not
                // satisfy update or delete condition
      (4, 40),  // (4, 40) not matched any target, so inserted as insert condition is 's.key > 1'
      (5, 5),   // (5, 5) not matched with (5, 50) by source-only condition 's.key < 5', no update
      (5, 50)   // (5, 50) inserted as inserted as insert condition is 's.key > 1'
    ))

  test("extended syntax - different # cols in source than target") {
    val sourceData =
      (0, 0, 0) :: (1, 10, 100) :: (2, 20, 200) :: (3, 30, 300) :: (4, 40, 400) :: Nil
    val targetData = (1, 1) :: (2, 2) :: (3, 3) :: Nil

    withTempView("source") {
      append(targetData.toDF("key", "value"), Nil)
      sourceData.toDF("key", "value", "extra").createOrReplaceTempView("source")
      executeMerge(
        s"delta.`$tempPath` t",
        "source s",
        cond = "s.key = t.key",
        update(condition = "s.key < 2", set = "key = s.key, value = s.value + s.extra"),
        delete(condition = "s.key < 3"),
        insert(condition = "s.key > 1", values = "(key, value) VALUES (s.key, s.value + s.extra)"))

      checkAnswer(
        readDeltaTable(tempPath),
        Seq(
          Row(1, 110),  // (1, 1) updated by condition, but not (2, 2) or (3, 3)
          Row(3, 3),    // neither updated nor deleted as it matched neither condition
          Row(4, 440)   // (4, 40) inserted by condition, but not (0, 0)
        ))              // (2, 2) deleted by condition but not (1, 1) or (3, 3)
    }
  }

  protected def withJsonData(
      source: Seq[String],
      target: Seq[String],
      schema: StructType = null,
      sourceSchema: StructType = null)(
      thunk: (String, String) => Unit): Unit = {

    def toDF(strs: Seq[String]) = {
      if (sourceSchema != null && strs == source) {
        spark.read.schema(sourceSchema).json(strs.toDS)
      } else if (schema != null) {
        spark.read.schema(schema).json(strs.toDS)
      } else {
        spark.read.json(strs.toDS)
      }
    }
    append(toDF(target), Nil)
    withTempView("source") {
      toDF(source).createOrReplaceTempView("source")
      thunk("source", s"delta.`$tempPath`")
    }
  }

  test("extended syntax - nested data - conditions and actions") {
    withJsonData(
      source =
        """{ "key": { "x": "X1", "y": 1}, "value" : { "a": 100, "b": "B100" } }
          { "key": { "x": "X2", "y": 2}, "value" : { "a": 200, "b": "B200" } }
          { "key": { "x": "X3", "y": 3}, "value" : { "a": 300, "b": "B300" } }
          { "key": { "x": "X4", "y": 4}, "value" : { "a": 400, "b": "B400" } }""",
      target =
        """{ "key": { "x": "X1", "y": 1}, "value" : { "a": 1,   "b": "B1" } }
          { "key": { "x": "X2", "y": 2}, "value" : { "a": 2,   "b": "B2" } }"""
    ) { case (sourceName, targetName) =>
      executeMerge(
        s"$targetName t",
        s"$sourceName s",
        cond = "s.key = t.key",
        update(condition = "s.key.y < 2", set = "key = s.key, value = s.value"),
        insert(condition = "s.key.x < 'X4'", values = "(key, value) VALUES (s.key, s.value)"))

      checkAnswer(
        readDeltaTable(tempPath),
        spark.read.json(Seq(
          """{ "key": { "x": "X1", "y": 1}, "value" : { "a": 100, "b": "B100" } }""", // updated
          """{ "key": { "x": "X2", "y": 2}, "value" : { "a": 2,   "b": "B2"   } }""", // not updated
          """{ "key": { "x": "X3", "y": 3}, "value" : { "a": 300, "b": "B300" } }"""  // inserted
        ).toDS))
    }
  }

  protected implicit def strToJsonSeq(str: String): Seq[String] = {
    str.split("\n").filter(_.trim.length > 0)
  }

  def testStar(
      name: String)(
      source: Seq[String],
      target: Seq[String],
      mergeClauses: MergeClause*)(
      result: Seq[String] = null,
      errorStrs: Seq[String] = null) {

    require(result == null ^ errorStrs == null, "either set the result or the error strings")
    val testName =
      if (result != null) s"star syntax - $name" else s"star syntax - analysis error - $name"

    test(testName) {
      withJsonData(source, target) { case (sourceName, targetName) =>
        def execMerge() =
          executeMerge(s"$targetName t", s"$sourceName s", "s.key = t.key", mergeClauses: _*)
        if (result != null) {
          execMerge()
          val deltaPath = if (targetName.startsWith("delta.`")) {
            targetName.stripPrefix("delta.`").stripSuffix("`")
          } else targetName
          checkAnswer(
            readDeltaTable(deltaPath),
            readFromJSON(result))
        } else {
          val e = intercept[AnalysisException] { execMerge() }
          errorStrs.foreach { s => errorContains(e.getMessage, s) }
        }
      }
    }
  }

  testStar("basic star expansion")(
    source =
      """{ "key": "a", "value" : 10 }
         { "key": "c", "value" : 30 }""",
    target =
      """{ "key": "a", "value" : 1 }
         { "key": "b", "value" : 2 }""",
    update(set = "*"),
    insert(values = "*"))(
    result =
      """{ "key": "a", "value" : 10 }
         { "key": "b", "value" : 2   }
         { "key": "c", "value" : 30 }""")

  testStar("multiples columns and extra columns in source")(
    source =
      """{ "key": "a", "value" : 10, "value2" : 100, "value3" : 1000 }
         { "key": "c", "value" : 30, "value2" : 300, "value3" : 3000 }""",
    target =
      """{ "key": "a", "value" : 1, "value2" : 1 }
         { "key": "b", "value" : 2, "value2" : 2 }""",
    update(set = "*"),
    insert(values = "*"))(
    result =
      """{ "key": "a", "value" : 10, "value2" : 100 }
         { "key": "b", "value" : 2,  "value2" : 2  }
         { "key": "c", "value" : 30, "value2" : 300 }""")

  testExtendedMerge("insert only merge")(
    source = (0, 0) :: (1, 10) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2)  :: Nil,
    mergeOn = "s.key = t.key",
    insert(values = "*"))(
    result = Seq(
      (0, 0), // inserted
      (1, 1), // existed previously
      (2, 2), // existed previously
      (3, 30) // inserted
    ))

  testExtendedMerge("insert only merge with insert condition on source")(
    source = (0, 0) :: (1, 10) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2)  :: Nil,
    mergeOn = "s.key = t.key",
    insert(values = "*", condition = "s.key = s.value"))(
    result = Seq(
      (0, 0), // inserted
      (1, 1), // existed previously
      (2, 2)  // existed previously
    ))

  testExtendedMerge("insert only merge with predicate insert")(
    source = (0, 0) :: (1, 10) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2)  :: Nil,
    mergeOn = "s.key = t.key",
    insert(values = "(t.key, t.value) VALUES (s.key + 10, s.value + 10)"))(
    result = Seq(
      (10, 10), // inserted
      (1, 1), // existed previously
      (2, 2), // existed previously
      (13, 40) // inserted
    ))

  testExtendedMerge(s"insert only merge with multiple matches") (
    source = (0, 0) :: (1, 10) :: (1, 100) :: (3, 30) :: (3, 300) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    insert(values = "(key, value) VALUES (s.key, s.value)")) (
    result = Seq(
      (0, 0), // inserted
      (1, 1), // existed previously
      (2, 2), // existed previously
      (3, 30), // inserted
      (3, 300) // key exists but still inserted
    )
  )


  protected def testNullCaseInsertOnly(name: String)(
    target: Seq[(JInt, JInt)],
    source: Seq[(JInt, JInt)],
    condition: String,
    expectedResults: Seq[(JInt, JInt)],
    insertCondition: Option[String] = None) = {
    Seq(true, false).foreach { isPartitioned =>
      test(s"basic case - null handling - $name, isPartitioned: $isPartitioned") {
        withView("sourceView") {
          val partitions = if (isPartitioned) "key" :: Nil else Nil
          append(target.toDF("key", "value"), partitions)
          source.toDF("key", "value").createOrReplaceTempView("sourceView")
          withSQLConf(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED.key -> "true") {
            if (insertCondition.isDefined) {
              executeMerge(
                s"delta.`$tempPath` as t",
                "sourceView s",
                condition,
                insert("(t.key, t.value) VALUES (s.key, s.value)",
                  condition = insertCondition.get))
            } else {
              executeMerge(
                s"delta.`$tempPath` as t",
                "sourceView s",
                condition,
                insert("(t.key, t.value) VALUES (s.key, s.value)"))
            }
          }
          checkAnswer(
            readDeltaTable(tempPath),
            expectedResults.map { r => Row(r._1, r._2) }
          )

          Utils.deleteRecursively(new File(tempPath))
        }
      }
    }
  }

  testNullCaseInsertOnly("insert only merge - null in source") (
    target = Seq((1, 1)),
    source = Seq((1, 10), (2, 20), (null, null)),
    condition = "s.key = t.key",
    expectedResults = Seq(
      (1, 1),         // Existing value
      (2, 20),        // Insert
      (null, null)    // Insert
    ))

  testNullCaseInsertOnly("insert only merge - null value in both source and target")(
    target = Seq((1, 1), (null, null)),
    source = Seq((1, 10), (2, 20), (null, 0)),
    condition = "s.key = t.key",
    expectedResults = Seq(
      (null, null),   // No change as null in source does not match null in target
      (1, 1),         // Existing value
      (2, 20),        // Insert
      (null, 0)       // Insert
    ))

  testNullCaseInsertOnly("insert only merge - null in insert clause")(
    target = Seq((1, 1), (2, 20)),
    source = Seq((1, 10), (3, 30), (null, 0)),
    condition = "s.key = t.key",
    expectedResults = Seq(
      (1, 1),         // Existing value
      (2, 20),        // Existing value
      (null, 0)       // Insert
    ),
    insertCondition = Some("s.key IS NULL")
  )

  test("insert only merge - turn off feature flag") {
    withSQLConf(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED.key -> "false") {
      withKeyValueData(
        source = (1, 10) :: (3, 30) :: Nil,
        target = (1, 1) :: Nil
      ) { case (sourceName, targetName) =>
        insertOnlyMergeFeatureFlagOff(sourceName, targetName)
      }
    }
  }

  protected def insertOnlyMergeFeatureFlagOff(sourceName: String, targetName: String): Unit = {
    executeMerge(
      tgt = s"$targetName t",
      src = s"$sourceName s",
      cond = "s.key = t.key",
      insert(values = "(key, value) VALUES (s.key, s.value)"))

    checkAnswer(sql(s"SELECT key, value FROM $targetName"),
      Row(1, 1) :: Row(3, 30) :: Nil)

    val metrics = spark.sql(s"DESCRIBE HISTORY $targetName LIMIT 1")
      .select("operationMetrics")
      .collect().head.getMap(0).asInstanceOf[Map[String, String]]
    assert(metrics.contains("numTargetFilesRemoved"))
    // If insert-only code path is not used, then the general code path will rewrite existing
    // target files when DVs are not enabled.
    if (!spark.conf.get(DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS)) {
      assert(metrics("numTargetFilesRemoved").toInt > 0)
    }
  }

  testMergeErrorOnMultipleMatches(
    "unconditional insert only merge - multiple matches when feature flag off",
    confs = Seq(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED.key -> "false"))(
    source = (1, 10) :: (1, 100) :: (2, 20) :: Nil,
    target = (1, 1) :: Nil,
    mergeOn = "s.key = t.key",
    insert(values = "(key, value) VALUES (s.key, s.value)"))

  testMergeErrorOnMultipleMatches(
    "conditional insert only merge - multiple matches when feature flag off",
    confs = Seq(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED.key -> "false"))(
    source = (1, 10) :: (1, 100) :: (2, 20) :: (2, 200) :: Nil,
    target = (1, 1) :: Nil,
    mergeOn = "s.key = t.key",
    insert(condition = "s.value = 20", values = "(key, value) VALUES (s.key, s.value)"))

  def testMergeWithRepartition(
      name: String,
      partitionColumns: Seq[String],
      srcRange: Range,
      expectLessFilesWithRepartition: Boolean,
      clauses: MergeClause*): Unit = {
    test(s"merge with repartition - $name",
      DisableAdaptiveExecution("AQE coalese would partition number")) {
      withTempView("source") {
        withTempDir { basePath =>
          val tgt1 = basePath + "target"
          val tgt2 = basePath + "targetRepartitioned"

          val df = spark.range(100).withColumn("part1", 'id % 5).withColumn("part2", 'id % 3)
          df.write.format("delta").partitionBy(partitionColumns: _*).save(tgt1)
          df.write.format("delta").partitionBy(partitionColumns: _*).save(tgt2)
          val cond = "src.id = t.id"
          val src = srcRange.toDF("id")
            .withColumn("part1", 'id % 5)
            .withColumn("part2", 'id % 3)
            .createOrReplaceTempView("source")
          // execute merge without repartition
          withSQLConf(DeltaSQLConf.MERGE_REPARTITION_BEFORE_WRITE.key -> "false") {
            executeMerge(
              tgt = s"delta.`$tgt1` as t",
              src = "source src",
              cond = cond,
              clauses = clauses: _*)
          }
          // execute merge with repartition - default behavior
          executeMerge(
            tgt = s"delta.`$tgt2` as t",
            src = "source src",
            cond = cond,
            clauses = clauses: _*)
          checkAnswer(
            io.delta.tables.DeltaTable.forPath(tgt2).toDF,
            io.delta.tables.DeltaTable.forPath(tgt1).toDF
          )
          val filesAfterNoRepartition = DeltaLog.forTable(spark, tgt1).snapshot.numOfFiles
          val filesAfterRepartition = DeltaLog.forTable(spark, tgt2).snapshot.numOfFiles
          // check if there are fewer are number of files for merge with repartition
          if (expectLessFilesWithRepartition) {
            assert(filesAfterNoRepartition > filesAfterRepartition)
          } else {
            assert(filesAfterNoRepartition === filesAfterRepartition)
          }
        }
      }
    }
  }

  testMergeWithRepartition(
    name = "partition on multiple columns",
    partitionColumns = Seq("part1", "part2"),
    srcRange = Range(80, 110),
    expectLessFilesWithRepartition = true,
    update("t.part2 = 1"),
    insert("(id, part1, part2) VALUES (id, part1, part2)")
  )

  testMergeWithRepartition(
    name = "insert only merge",
    partitionColumns = Seq("part1"),
    srcRange = Range(110, 150),
    expectLessFilesWithRepartition = true,
    insert("(id, part1, part2) VALUES (id, part1, part2)")
  )

  testMergeWithRepartition(
    name = "non partitioned table",
    partitionColumns = Seq(),
    srcRange = Range(80, 180),
    expectLessFilesWithRepartition = false,
    update("t.part2 = 1"),
    insert("(id, part1, part2) VALUES (id, part1, part2)")
  )

  protected def testMatchedOnlyOptimization(
      name: String)(
      source: Seq[(Int, Int)],
      target: Seq[(Int, Int)],
      mergeOn: String,
      mergeClauses: MergeClause*) (
      result: Seq[(Int, Int)]): Unit = {
    Seq(true, false).foreach { matchedOnlyEnabled =>
      Seq(true, false).foreach { isPartitioned =>
        val s = if (matchedOnlyEnabled) "enabled" else "disabled"
        test(s"matched only merge - $s - $name - isPartitioned: $isPartitioned ") {
          withKeyValueData(source, target, isPartitioned) { case (sourceName, targetName) =>
            withSQLConf(DeltaSQLConf.MERGE_MATCHED_ONLY_ENABLED.key -> s"$matchedOnlyEnabled") {
              executeMerge(s"$targetName t", s"$sourceName s", mergeOn, mergeClauses: _*)
            }
            val deltaPath = if (targetName.startsWith("delta.`")) {
              targetName.stripPrefix("delta.`").stripSuffix("`")
            } else targetName
            checkAnswer(
              readDeltaTable(deltaPath),
              result.map { case (k, v) => Row(k, v) })
          }
        }
      }
    }
  }

  testMatchedOnlyOptimization("with update") (
    source = Seq((1, 100), (3, 300), (5, 500)),
    target = Seq((1, 10), (2, 20), (3, 30)),
    mergeOn = "s.key = t.key",
    update("t.key = s.key, t.value = s.value")) (
    result = Seq(
      (1, 100), // updated
      (2, 20), // existed previously
      (3, 300) // updated
    )
  )

  testMatchedOnlyOptimization("with delete") (
    source = Seq((1, 100), (3, 300), (5, 500)),
    target = Seq((1, 10), (2, 20), (3, 30)),
    mergeOn = "s.key = t.key",
    delete()) (
    result = Seq(
      (2, 20) // existed previously
    )
  )

  testMatchedOnlyOptimization("with update and delete")(
    source = Seq((1, 100), (3, 300), (5, 500)),
    target = Seq((1, 10), (3, 30), (5, 30)),
    mergeOn = "s.key = t.key",
    update("t.value = s.value", "t.key < 3"), delete("t.key > 3")) (
    result = Seq(
      (1, 100), // updated
      (3, 30)   // existed previously
    )
  )

  protected def testNullCaseMatchedOnly(name: String) (
      source: Seq[(JInt, JInt)],
      target: Seq[(JInt, JInt)],
      mergeOn: String,
      result: Seq[(JInt, JInt)]) = {
    Seq(true, false).foreach { isPartitioned =>
      withSQLConf(DeltaSQLConf.MERGE_MATCHED_ONLY_ENABLED.key -> "true") {
        test(s"matched only merge - null handling - $name, isPartitioned: $isPartitioned") {
          withView("sourceView") {
            val partitions = if (isPartitioned) "key" :: Nil else Nil
            append(target.toDF("key", "value"), partitions)
            source.toDF("key", "value").createOrReplaceTempView("sourceView")

            executeMerge(
              tgt = s"delta.`$tempPath` as t",
              src = "sourceView s",
              cond = mergeOn,
              update("t.value = s.value"))

            checkAnswer(
              readDeltaTable(tempPath),
              result.map { r => Row(r._1, r._2) }
            )

            Utils.deleteRecursively(new File(tempPath))
          }
        }
      }
    }
  }

  testNullCaseMatchedOnly("null in source") (
    source = Seq((1, 10), (2, 20), (null, null)),
    target = Seq((1, 1)),
    mergeOn = "s.key = t.key",
    result = Seq(
      (1, 10) // update
    )
  )

  testNullCaseMatchedOnly("null value in both source and target") (
    source = Seq((1, 10), (2, 20), (null, 0)),
    target = Seq((1, 1), (null, null)),
    mergeOn = "s.key = t.key",
    result = Seq(
      (null, null), // No change as null in source does not match null in target
      (1, 10) // update
    )
  )

  test("data skipping - target-only condition") {
    withKeyValueData(
      source = (1, 10) :: Nil,
      target = (1, 1) :: (2, 2) :: Nil,
      isKeyPartitioned = true) { case (sourceName, targetName) =>

      val report = getScanReport {
        executeMerge(
          target = s"$targetName t",
          source = s"$sourceName s",
          condition = "s.key = t.key AND t.key <= 1",
          update = "t.key = s.key, t.value = s.value",
          insert = "(key, value) VALUES (s.key, s.value)")
      }.head

      checkAnswer(sql(getDeltaFileStmt(tempPath)),
        Row(1, 10) ::  // Updated
        Row(2, 2) ::   // File should be skipped
        Nil)

      assert(report.size("scanned").bytesCompressed != report.size("total").bytesCompressed)
    }
  }

  test("insert only merge - target data skipping") {
    val tblName = "merge_target"
    withTable(tblName) {
      spark.range(10).withColumn("part", 'id % 5).withColumn("value", 'id + 'id)
        .write.format("delta").partitionBy("part").mode("append").saveAsTable(tblName)

      val source = "source"
      withTable(source) {
        spark.range(20).withColumn("part", functions.lit(1)).withColumn("value", 'id + 'id)
          .write.format("delta").saveAsTable(source)

        val scans = getScanReport {
          withSQLConf(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED.key -> "true") {
            executeMerge(
              s"$tblName t",
              s"$source s",
              "s.id = t.id AND t.part = 1",
              insert(condition = "s.id % 5 = s.part", values = "*"))
          }
        }
        checkAnswer(
          spark.table(tblName).where("part = 1"),
          Row(1, 1, 2) :: Row(6, 1, 12) :: Row(11, 1, 22) :: Row(16, 1, 32) :: Nil
        )

        assert(scans.length === 2, "We should scan the source and target " +
          "data once in an insert only optimization")

        // check if the source and target tables are scanned just once
        val sourceRoot = DeltaTableUtils.findDeltaTableRoot(
          spark, new Path(spark.table(source).inputFiles.head)).get.toString
        val targetRoot = DeltaTableUtils.findDeltaTableRoot(
          spark, new Path(spark.table(tblName).inputFiles.head)).get.toString
        assert(scans.map(_.path).toSet == Set(sourceRoot, targetRoot))

        // check scanned files
        val targetScans = scans.find(_.path == targetRoot)
        val deltaLog = DeltaLog.forTable(spark, targetScans.get.path)
        val numTargetFiles = deltaLog.snapshot.numOfFiles
        assert(targetScans.get.metrics("numFiles") < numTargetFiles)
        // check scanned sizes
        val scanSizes = targetScans.head.size
        assert(scanSizes("total").bytesCompressed.get > scanSizes("scanned").bytesCompressed.get,
          "Should have partition pruned target table")
      }
    }
  }

  /**
   * Test whether data skipping on matched predicates of a merge command is performed.
   * @param name The name of the test case.
   * @param source The source for merge.
   * @param target The target for merge.
   * @param dataSkippingOnTargetOnly The boolean variable indicates whether
   *                                 when matched clauses are on target fields only.
   *                                 Data Skipping should be performed before inner join if
   *                                 this variable is true.
   * @param isMatchedOnly The boolean variable indicates whether the merge command only
   *                      contains when matched clauses.
   * @param mergeClauses Merge Clauses.
   */
  protected def testMergeDataSkippingOnMatchPredicates(
      name: String)(
      source: Seq[(Int, Int)],
      target: Seq[(Int, Int)],
      dataSkippingOnTargetOnly: Boolean,
      isMatchedOnly: Boolean,
      mergeClauses: MergeClause*)(
      result: Seq[(Int, Int)]): Unit = {
    test(s"data skipping with matched predicates - $name") {
      withKeyValueData(source, target) { case (sourceName, targetName) =>
        val stats = performMergeAndCollectStatsForDataSkippingOnMatchPredicates(
          sourceName,
          targetName,
          result,
          mergeClauses)
        // Data skipping on match predicates should only be performed when it's a
        // matched only merge.
        if (isMatchedOnly) {
          // The number of files removed/added should be 0 because of the additional predicates.
          assert(stats.targetFilesRemoved == 0)
          assert(stats.targetFilesAdded == 0)
          // Verify that the additional predicates on data skipping
          // before inner join filters file out for match predicates only
          // on target.
          if (dataSkippingOnTargetOnly) {
            assert(stats.targetBeforeSkipping.files.get > stats.targetAfterSkipping.files.get)
          }
        } else {
          if (!spark.conf.get(DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS)) {
            assert(stats.targetFilesRemoved > 0)
          }
          // If there is no insert clause and the flag is enabled, data skipping should be
          // performed on targetOnly predicates.
          // However, with insert clauses, it's expected that no additional data skipping
          // is performed on matched clauses.
          assert(stats.targetBeforeSkipping.files.get == stats.targetAfterSkipping.files.get)
          assert(stats.targetRowsUpdated == 0)
        }
      }
    }
  }

  protected def performMergeAndCollectStatsForDataSkippingOnMatchPredicates(
      sourceName: String,
      targetName: String,
      result: Seq[(Int, Int)],
      mergeClauses: Seq[MergeClause]): MergeStats = {
    var events: Seq[UsageRecord] = Seq.empty
    // Perform merge on merge condition with matched clauses.
    events = Log4jUsageLogger.track {
      executeMerge(s"$targetName t", s"$sourceName s", "s.key = t.key", mergeClauses: _*)
    }
    val deltaPath = if (targetName.startsWith("delta.`")) {
      targetName.stripPrefix("delta.`").stripSuffix("`")
    } else targetName

    checkAnswer(
      readDeltaTable(deltaPath),
      result.map { case (k, v) => Row(k, v) })

    // Verify merge stats from usage events
    val mergeStats = events.filter { e =>
      e.metric == MetricDefinitions.EVENT_TAHOE.name &&
        e.tags.get("opType").contains("delta.dml.merge.stats")
    }

    assert(mergeStats.size == 1)

    JsonUtils.fromJson[MergeStats](mergeStats.head.blob)
  }

  testMergeDataSkippingOnMatchPredicates("match conditions on target fields only")(
    source = Seq((1, 100), (3, 300), (5, 500)),
    target = Seq((1, 10), (2, 20), (3, 30)),
    dataSkippingOnTargetOnly = true,
    isMatchedOnly = true,
    update(condition = "t.key == 10", set = "*"),
    update(condition = "t.value == 100", set = "*"))(
    result = Seq((1, 10), (2, 20), (3, 30))
  )

  testMergeDataSkippingOnMatchPredicates("match conditions on source fields only")(
    source = Seq((1, 100), (3, 300), (5, 500)),
    target = Seq((1, 10), (2, 20), (3, 30)),
    dataSkippingOnTargetOnly = false,
    isMatchedOnly = true,
    update(condition = "s.key == 10", set = "*"),
    update(condition = "s.value == 10", set = "*"))(
    result = Seq((1, 10), (2, 20), (3, 30))
  )

  testMergeDataSkippingOnMatchPredicates("match on source and target fields")(
    source = Seq((1, 100), (3, 300), (5, 500)),
    target = Seq((1, 10), (2, 20), (3, 30)),
    dataSkippingOnTargetOnly = false,
    isMatchedOnly = true,
    update(condition = "s.key == 10", set = "*"),
    update(condition = "s.value == 10", set = "*"),
    delete(condition = "t.key == 4"))(
    result = Seq((1, 10), (2, 20), (3, 30))
  )

  testMergeDataSkippingOnMatchPredicates("with insert clause")(
    source = Seq((1, 100), (3, 300), (5, 500)),
    target = Seq((1, 10), (2, 20), (3, 30)),
    dataSkippingOnTargetOnly = false,
    isMatchedOnly = false,
    update(condition = "t.key == 10", set = "*"),
    insert(condition = null, values = "(key, value) VALUES (s.key, s.value)"))(
    result = Seq((1, 10), (2, 20), (3, 30), (5, 500))
  )

  testMergeDataSkippingOnMatchPredicates("when matched and conjunction")(
    source = Seq((1, 100), (3, 300), (5, 500)),
    target = Seq((1, 10), (2, 20), (3, 30)),
    dataSkippingOnTargetOnly = true,
    isMatchedOnly = true,
    update(condition = "t.key == 1 AND t.value == 5", set = "*"))(
    result = Seq((1, 10), (2, 20), (3, 30)))

  /* unlimited number of merge clauses tests */

  protected def testUnlimitedClauses(
      name: String)(
      source: Seq[(Int, Int)],
      target: Seq[(Int, Int)],
      mergeOn: String,
      mergeClauses: MergeClause*)(
      result: Seq[(Int, Int)]): Unit =
    testExtendedMerge(name, "unlimited clauses")(source, target, mergeOn, mergeClauses : _*)(result)

  testUnlimitedClauses("two conditional update + two conditional delete + insert")(
    source = (0, 0) :: (1, 100) :: (3, 300) :: (4, 400) :: (5, 500) :: Nil,
    target = (1, 10) :: (2, 20) :: (3, 30) :: (4, 40) :: Nil,
    mergeOn = "s.key = t.key",
    delete(condition = "s.key < 2"),
    delete(condition = "s.key > 4"),
    update(condition = "s.key == 3", set = "key = s.key, value = s.value"),
    update(condition = "s.key == 4", set = "key = s.key, value = 2 * s.value"),
    insert(condition = null, values = "(key, value) VALUES (s.key, s.value)"))(
    result = Seq(
      (0, 0),    // insert (0, 0)
                 // delete (1, 10)
      (2, 20),   // neither updated nor deleted as it didn't match
      (3, 300),  // update (3, 30)
      (4, 800),  // update (4, 40)
      (5, 500)   // insert (5, 500)
    ))

  testUnlimitedClauses("two conditional delete + conditional update + update + insert")(
    source = (0, 0) :: (1, 100) :: (2, 200) :: (3, 300) :: (4, 400) :: Nil,
    target = (1, 10) :: (2, 20) :: (3, 30) :: (4, 40) :: Nil,
    mergeOn = "s.key = t.key",
    delete(condition = "s.key < 2"),
    delete(condition = "s.key > 3"),
    update(condition = "s.key == 2", set = "key = s.key, value = s.value"),
    update(condition = null, set = "key = s.key, value = 2 * s.value"),
    insert(condition = null, values = "(key, value) VALUES (s.key, s.value)"))(
    result = Seq(
      (0, 0),   // insert (0, 0)
                // delete (1, 10)
      (2, 200), // update (2, 20)
      (3, 600)  // update (3, 30)
                // delete (4, 40)
    ))

  testUnlimitedClauses("conditional delete + two conditional update + two conditional insert")(
    source = (1, 100) :: (2, 200) :: (3, 300) :: (4, 400) :: (6, 600) :: Nil,
    target = (1, 10) :: (2, 20) :: (3, 30) :: Nil,
    mergeOn = "s.key = t.key",
    delete(condition = "s.key < 2"),
    update(condition = "s.key == 2", set = "key = s.key, value = s.value"),
    update(condition = "s.key == 3", set = "key = s.key, value = 2 * s.value"),
    insert(condition = "s.key < 5", values = "(key, value) VALUES (s.key, s.value)"),
    insert(condition = "s.key > 5", values = "(key, value) VALUES (s.key, 1 + s.value)"))(
    result = Seq(
                // delete (1, 10)
      (2, 200), // update (2, 20)
      (3, 600), // update (3, 30)
      (4, 400), // insert (4, 400)
      (6, 601)  // insert (6, 600)
    ))

  testUnlimitedClauses("conditional update + update + conditional delete + conditional insert")(
    source = (1, 100) :: (2, 200) :: (3, 300) :: (4, 400) :: (5, 500) :: Nil,
    target = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: Nil,
    mergeOn = "s.key = t.key",
    update(condition = "s.key < 2", set = "key = s.key, value = s.value"),
    update(condition = "s.key < 3", set = "key = s.key, value = 2 * s.value"),
    delete(condition = "s.key < 4"),
    insert(condition = "s.key > 4", values = "(key, value) VALUES (s.key, s.value)"))(
    result = Seq(
      (0, 0),   // no change
      (1, 100), // (1, 10) updated by matched_0
      (2, 400), // (2, 20) updated by matched_1
                // (3, 30) deleted by matched_2
      (5, 500)  // (5, 500) inserted
    ))

  testUnlimitedClauses("conditional insert + insert")(
    source = (1, 100) :: (2, 200) :: (3, 300) :: (4, 400) :: (5, 500) :: Nil,
    target = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: Nil,
    mergeOn = "s.key = t.key",
    insert(condition = "s.key < 5", values = "(key, value) VALUES (s.key, s.value)"),
    insert(condition = null, values = "(key, value) VALUES (s.key, s.value + 1)"))(
    result = Seq(
      (0, 0),   // no change
      (1, 10),  // no change
      (2, 20),  // no change
      (3, 30),  // no change
      (4, 400), // (4, 400) inserted by notMatched_0
      (5, 501)  // (5, 501) inserted by notMatched_1
    ))

  testUnlimitedClauses("2 conditional inserts")(
    source = (1, 100) :: (2, 200) :: (3, 300) :: (4, 400) :: (5, 500) :: (6, 600) :: Nil,
    target = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: Nil,
    mergeOn = "s.key = t.key",
    insert(condition = "s.key < 5", values = "(key, value) VALUES (s.key, s.value)"),
    insert(condition = "s.key = 5", values = "(key, value) VALUES (s.key, s.value + 1)"))(
    result = Seq(
      (0, 0),   // no change
      (1, 10),  // no change
      (2, 20),  // no change
      (3, 30),  // no change
      (4, 400), // (4, 400) inserted by notMatched_0
      (5, 501)  // (5, 501) inserted by notMatched_1
                // (6, 600) not inserted as not insert condition matched
    ))

  testUnlimitedClauses("update/delete (no matches) + conditional insert + insert")(
    source = (4, 400) :: (5, 500) :: Nil,
    target = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: Nil,
    mergeOn = "s.key = t.key",
    update(condition = "t.key = 0", set = "key = s.key, value = s.value"),
    delete(condition = null),
    insert(condition = "s.key < 5", values = "(key, value) VALUES (s.key, s.value)"),
    insert(condition = null, values = "(key, value) VALUES (s.key, s.value + 1)"))(
    result = Seq(
      (0, 0),   // no change
      (1, 10),  // no change
      (2, 20),  // no change
      (3, 30),  // no change
      (4, 400), // (4, 400) inserted by notMatched_0
      (5, 501)  // (5, 501) inserted by notMatched_1
    ))

  testUnlimitedClauses("update/delete (no matches) + 2 conditional inserts")(
    source = (4, 400) :: (5, 500) :: (6, 600)  :: Nil,
    target = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: Nil,
    mergeOn = "s.key = t.key",
    update(condition = "t.key = 0", set = "key = s.key, value = s.value"),
    delete(condition = null),
    insert(condition = "s.key < 5", values = "(key, value) VALUES (s.key, s.value)"),
    insert(condition = "s.key = 5", values = "(key, value) VALUES (s.key, s.value + 1)"))(
    result = Seq(
      (0, 0),   // no change
      (1, 10),  // no change
      (2, 20),  // no change
      (3, 30),  // no change
      (4, 400), // (4, 400) inserted by notMatched_0
      (5, 501)  // (5, 501) inserted by notMatched_1
                // (6, 600) not inserted as not insert condition matched
    ))

  testUnlimitedClauses("2 update + 2 delete + 4 insert")(
    source = (1, 100) :: (2, 200) :: (3, 300) :: (4, 400) :: (5, 500) :: (6, 600) :: (7, 700) ::
      (8, 800) :: (9, 900) :: Nil,
    target = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: (4, 40) :: Nil,
    mergeOn = "s.key = t.key",
    update(condition = "s.key == 1", set = "key = s.key, value = s.value"),
    delete(condition = "s.key == 2"),
    update(condition = "s.key == 3", set = "key = s.key, value = 2 * s.value"),
    delete(condition = null),
    insert(condition = "s.key == 5", values = "(key, value) VALUES (s.key, s.value)"),
    insert(condition = "s.key == 6", values = "(key, value) VALUES (s.key, 1 + s.value)"),
    insert(condition = "s.key == 7", values = "(key, value) VALUES (s.key, 2 + s.value)"),
    insert(condition = null, values = "(key, value) VALUES (s.key, 3 + s.value)"))(
    result = Seq(
      (0, 0),    // no change
      (1, 100),  // (1, 10) updated by matched_0
                 // (2, 20) deleted by matched_1
      (3, 600),  // (3, 30) updated by matched_2
                 // (4, 40) deleted by matched_3
      (5, 500),  // (5, 500) inserted by notMatched_0
      (6, 601),  // (6, 600) inserted by notMatched_1
      (7, 702),  // (7, 700) inserted by notMatched_2
      (8, 803),  // (8, 800) inserted by notMatched_3
      (9, 903)   // (9, 900) inserted by notMatched_3
    ))

  testMergeAnalysisException("error on multiple insert clauses without condition")(
    mergeOn = "s.key = t.key",
    update(condition = "s.key == 3", set = "key = s.key, value = 2 * srcValue"),
    insert(condition = null, values = "(key, value) VALUES (s.key, srcValue)"),
    insert(condition = null, values = "(key, value) VALUES (s.key, 1 + srcValue)"))(
    expectedErrorClass = "NON_LAST_NOT_MATCHED_BY_TARGET_CLAUSE_OMIT_CONDITION",
    expectedMessageParameters = Map.empty)

  testMergeAnalysisException("error on multiple update clauses without condition")(
    mergeOn = "s.key = t.key",
    update(condition = "s.key == 3", set = "key = s.key, value = 2 * srcValue"),
    update(condition = null, set = "key = s.key, value = 3 * srcValue"),
    update(condition = null, set = "key = s.key, value = 4 * srcValue"),
    insert(condition = null, values = "(key, value) VALUES (s.key, srcValue)"))(
    expectedErrorClass = "NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION",
    expectedMessageParameters = Map.empty)

  testMergeAnalysisException("error on multiple update/delete clauses without condition")(
    mergeOn = "s.key = t.key",
    update(condition = "s.key == 3", set = "key = s.key, value = 2 * srcValue"),
    delete(condition = null),
    update(condition = null, set = "key = s.key, value = 4 * srcValue"),
    insert(condition = null, values = "(key, value) VALUES (s.key, srcValue)"))(
    expectedErrorClass = "NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION",
    expectedMessageParameters = Map.empty)

  testMergeAnalysisException(
    "error on non-empty condition following empty condition for update clauses")(
    mergeOn = "s.key = t.key",
    update(condition = null, set = "key = s.key, value = 2 * srcValue"),
    update(condition = "s.key < 3", set = "key = s.key, value = srcValue"),
    insert(condition = null, values = "(key, value) VALUES (s.key, srcValue)"))(
    expectedErrorClass = "NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION",
    expectedMessageParameters = Map.empty)

  testMergeAnalysisException(
    "error on non-empty condition following empty condition for insert clauses")(
    mergeOn = "s.key = t.key",
    update(condition = null, set = "key = s.key, value = srcValue"),
    insert(condition = null, values = "(key, value) VALUES (s.key, srcValue)"),
    insert(condition = "s.key < 3", values = "(key, value) VALUES (s.key, srcValue)"))(
    expectedErrorClass = "NON_LAST_NOT_MATCHED_BY_TARGET_CLAUSE_OMIT_CONDITION",
    expectedMessageParameters = Map.empty)

  /* end unlimited number of merge clauses tests */

  test("SC-70829 - prevent re-resolution with star and schema evolution") {
    val source = "source"
    val target = "target"
    withTable(source, target) {

      sql(s"""CREATE TABLE $source (id string, new string, old string, date DATE) USING delta""")
      sql(s"""CREATE TABLE $target (id string, old string, date DATE) USING delta""")

      withSQLConf("spark.databricks.delta.schema.autoMerge.enabled" -> "true") {
        executeMerge(
          tgt = s"$target t",
          src = s"$source s",
          // functions like date_sub requires additional work to resolve
          cond = "s.id = t.id AND t.date >= date_sub(current_date(), 3)",
          update(set = "*"),
          insert(values = "*"))
      }
    }
  }

  /**
   * @param function the unsupported function.
   * @param functionType The type of the unsupported expression to be tested.
   * @param sourceData the data in the source table.
   * @param targetData the data in the target table.
   * @param mergeCondition the merge condition containing the unsupported expression.
   * @param clauseCondition the clause condition containing the unsupported expression.
   * @param clauseAction the clause action containing the unsupported expression.
   * @param expectExceptionInAction whether expect exception thrown in action.
   * @param customConditionErrorRegex the customized error regex for condition.
   * @param customActionErrorRegex the customized error regex for action.
   */
  def testUnsupportedExpression(
      function: String,
      functionType: String,
      sourceData: => DataFrame,
      targetData: => DataFrame,
      mergeCondition: String,
      clauseCondition: String,
      clauseAction: String,
      expectExceptionInAction: Option[Boolean] = None,
      customConditionErrorRegex: Option[String] = None,
      customActionErrorRegex: Option[String] = None) {
    test(s"$functionType functions in merge" +
      s" - expect exception in action: ${expectExceptionInAction.getOrElse(true)}") {
      withTable("source", "target") {
        sourceData.write.format("delta").saveAsTable("source")
        targetData.write.format("delta").saveAsTable("target")

        val expectedErrorRegex = "(?s).*(?i)unsupported.*(?i).*Invalid expressions.*"

        def checkExpression(
            expectException: Boolean,
            condition: Option[String] = None,
            clause: Option[MergeClause] = None,
            expectedRegex: Option[String] = None) {
          if (expectException) {
            val dataBeforeException = spark.read.format("delta").table("target").collect()
            val e = intercept[Exception] {
              executeMerge(
                tgt = "target as t",
                src = "source as s",
                cond = condition.getOrElse("s.a = t.a"),
                clause.getOrElse(update(set = "b = s.b"))
              )
            }

            def extractErrorClass(e: Throwable): String =
              e match {
                case dt: DeltaThrowable => s"\\[${dt.getErrorClass}\\] "
                case _ => ""
              }

            val (message, errorClass) = if (e.getCause != null) {
              (e.getCause.getMessage, extractErrorClass(e.getCause))
            } else (e.getMessage, extractErrorClass(e))
            assert(message.matches(errorClass + expectedRegex.getOrElse(expectedErrorRegex)))
            checkAnswer(spark.read.format("delta").table("target"), dataBeforeException)
          } else {
            executeMerge(
              tgt = "target as t",
              src = "source as s",
              cond = condition.getOrElse("s.a = t.a"),
              clause.getOrElse(update(set = "b = s.b"))
            )
          }
        }

        // on merge condition
        checkExpression(
          expectException = true,
          condition = Option(mergeCondition),
          expectedRegex = customConditionErrorRegex
        )

        // on update condition
        checkExpression(
          expectException = true,
          clause = Option(update(condition = clauseCondition, set = "b = s.b")),
          expectedRegex = customConditionErrorRegex
        )

        // on update action
        checkExpression(
          expectException = expectExceptionInAction.getOrElse(true),
          clause = Option(update(set = s"b = $clauseAction")),
          expectedRegex = customActionErrorRegex
        )

        // on insert condition
        checkExpression(
          expectException = true,
          clause = Option(
            insert(values = "(a, b, c) VALUES (s.a, s.b, s.c)", condition = clauseCondition)),
          expectedRegex = customConditionErrorRegex
        )

        sql("update source set a = 2")
        // on insert action
        checkExpression(
          expectException = expectExceptionInAction.getOrElse(true),
          clause = Option(insert(values = s"(a, b, c) VALUES ($clauseAction, s.b, s.c)")),
          expectedRegex = customActionErrorRegex
        )
      }
    }
  }

  testUnsupportedExpression(
    function = "row_number",
    functionType = "Window",
    sourceData = Seq((1, 2, 3)).toDF("a", "b", "c"),
    targetData = Seq((1, 5, 6)).toDF("a", "b", "c"),
    mergeCondition = "(row_number() over (order by s.c)) = (row_number() over (order by t.c))",
    clauseCondition = "row_number() over (order by s.c) > 1",
    clauseAction = "row_number() over (order by s.c)"
  )

  testUnsupportedExpression(
    function = "max",
    functionType = "Aggregate",
    sourceData = Seq((1, 2, 3)).toDF("a", "b", "c"),
    targetData = Seq((1, 5, 6)).toDF("a", "b", "c"),
    mergeCondition = "t.a = max(s.a)",
    clauseCondition = "max(s.b) > 1",
    clauseAction = "max(s.c)",
    customConditionErrorRegex =
      Option("Aggregate functions are not supported in the .* condition of MERGE operation.*")
  )

  protected def testTempViews(name: String)(
      text: String,
      expectedResult: Seq[Row] = null,
      expectedErrorMsgForSQLTempView: String = null,
      expectedErrorMsgForDataSetTempView: String = null,
      expectedErrorClassForSQLTempView: String = null,
      expectedErrorClassForDataSetTempView: String = null): Unit = {
    testWithTempView(s"test merge on temp view - $name") { isSQLTempView =>
      withTable("tab") {
        withTempView("src") {
          Seq((0, 3), (1, 2)).toDF("key", "value").write.format("delta").saveAsTable("tab")
          createTempViewFromSelect(text, isSQLTempView)
          sql("CREATE TEMP VIEW src AS SELECT * FROM VALUES (1, 2), (3, 4) AS t(a, b)")
          val doesExpectError = if (isSQLTempView) {
            expectedErrorMsgForSQLTempView != null || expectedErrorClassForSQLTempView != null
          } else {
            expectedErrorMsgForDataSetTempView != null ||
              expectedErrorClassForDataSetTempView != null
          }
          if (doesExpectError) {
            val ex = intercept[AnalysisException] {
              executeMerge(
                target = "v",
                source = "src",
                condition = "src.a = v.key AND src.b = v.value",
                update = "v.value = src.b + 1",
                insert = "(v.key, v.value) VALUES (src.a, src.b)")
            }
            testErrorMessageAndClass(
              isSQLTempView,
              ex,
              expectedErrorMsgForSQLTempView,
              expectedErrorMsgForDataSetTempView,
              expectedErrorClassForSQLTempView,
              expectedErrorClassForDataSetTempView)
          } else {
            require(expectedResult != null)
            executeMerge(
              target = "v",
              source = "src",
              condition = "src.a = v.key AND src.b = v.value",
              update = "v.value = src.b + 1",
              insert = "(v.key, v.value) VALUES (src.a, src.b)")
            checkAnswer(spark.table("v"), expectedResult)
          }
        }
      }
    }
  }

  testTempViews("basic")(
    text = "SELECT * FROM tab",
    expectedResult = Seq(Row(0, 3), Row(1, 3), Row(3, 4))
  )

  testTempViews("subset cols")(
    text = "SELECT key FROM tab",
    expectedErrorMsgForSQLTempView = "cannot",
    expectedErrorMsgForDataSetTempView = "cannot"
  )

  testTempViews("superset cols")(
    text = "SELECT key, value, 1 FROM tab",
    // The analyzer can't tell whether the table originally had the extra column or not.
    expectedErrorMsgForSQLTempView =
      "The schema of your Delta table has changed in an incompatible way",
    expectedErrorMsgForDataSetTempView =
      "The schema of your Delta table has changed in an incompatible way"
  )

  testTempViews("nontrivial projection")(
    text = "SELECT value as key, key as value FROM tab",
    expectedResult = Seq(Row(2, 1), Row(2, 1), Row(3, 0), Row(4, 3))
  )

  testTempViews("view with too many internal aliases")(
    text = "SELECT * FROM (SELECT * FROM tab AS t1) AS t2",
    expectedResult = Seq(Row(0, 3), Row(1, 3), Row(3, 4))
  )

  test("merge correctly handle field metadata") {
    withTable("source", "target") {
      // Create a target table with user metadata (comments) and internal metadata (column mapping
      // information) on both a top-level column and a nested field.
      sql(
        """
          |CREATE TABLE target(
          |  key int not null COMMENT 'data column',
          |  value int not null,
          |  cstruct struct<foo int COMMENT 'foo field'>)
          |USING DELTA
          |TBLPROPERTIES (
          |  'delta.minReaderVersion' = '2',
          |  'delta.minWriterVersion' = '5',
          |  'delta.columnMapping.mode' = 'name')
        """.stripMargin
      )
      sql(s"INSERT INTO target VALUES (0, 0, null)")

      sql("CREATE TABLE source (key int not null, value int not null) USING DELTA")
      sql(s"INSERT INTO source VALUES (1, 1)")

      executeMerge(
        tgt = "target",
        src = "source",
        cond = "source.key = target.key",
        update(condition = "target.key = 1", set = "target.value = 42"),
        updateNotMatched(condition = "target.key = 100", set = "target.value = 22"))
    }
  }

  test("UDT Data Types - simple and nested") {
    withTable("source") {
      withTable("target") {
        // scalastyle:off line.size.limit
        val targetData = Seq(
          Row(SimpleTest(0), ComplexTest(10, Array(1, 2, 3))),
          Row(SimpleTest(1), ComplexTest(20, Array(4, 5))),
          Row(SimpleTest(2), ComplexTest(30, Array(6, 7, 8))))
        val sourceData = Seq(
          Row(SimpleTest(0), ComplexTest(40, Array(9, 10))),
          Row(SimpleTest(3), ComplexTest(50, Array(11))))
        val resultData = Seq(
          Row(SimpleTest(0), ComplexTest(40, Array(9, 10))),
          Row(SimpleTest(1), ComplexTest(20, Array(4, 5))),
          Row(SimpleTest(2), ComplexTest(30, Array(6, 7, 8))),
          Row(SimpleTest(3), ComplexTest(50, Array(11))))

        val schema = StructType(Array(
          StructField("id", new SimpleTestUDT),
          StructField("complex", new ComplexTestUDT)))

        val df = spark.createDataFrame(sparkContext.parallelize(targetData), schema)
        df.collect()

        spark.createDataFrame(sparkContext.parallelize(targetData), schema)
          .write.format("delta").saveAsTable("target")

        spark.createDataFrame(sparkContext.parallelize(sourceData), schema)
          .write.format("delta").saveAsTable("source")
        // scalastyle:on line.size.limit
        sql(
          s"""
             |MERGE INTO target as t
             |USING source as s
             |ON t.id = s.id
             |WHEN MATCHED THEN
             |  UPDATE SET *
             |WHEN NOT MATCHED THEN
             |  INSERT *
             """.stripMargin)

        checkAnswer(sql("select * from target"), resultData)
      }
    }
  }
  test("recorded operations - write all changes") {
    var events: Seq[UsageRecord] = Seq.empty
    withKeyValueData(
      source = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: (4, 40) :: Nil,
      target = (1, 1) :: (2, 2) :: (3, 3) :: (5, 5) :: (6, 6) :: Nil,
      isKeyPartitioned = true) { case (sourceName, targetName) =>

      events = Log4jUsageLogger.track {
        executeMerge(
          tgt = s"$targetName t",
          src = s"$sourceName s",
          cond = "s.key = t.key",
          update(condition = "s.key > 1", set = "key = s.key, value = s.value"),
          insert(condition = "s.key < 1", values = "(key, value) VALUES (s.key, s.value)"),
          deleteNotMatched(condition = "t.key > 5"))
      }

      checkAnswer(sql(getDeltaFileStmt(tempPath)), Seq(
        Row(0, 0),   // inserted
        Row(1, 1),   // existed previously
        Row(2, 20),  // updated
        Row(3, 30),  // updated
        Row(5, 5)    // existed previously
        // Row(6, 6)    deleted
      ))
    }

    // Get recorded operations from usage events
    val opTypes = events.filter { e =>
      e.metric == "sparkOperationDuration" && e.opType.get.typeName.contains("delta.dml.merge")
    }.map(_.opType.get.typeName).toSet

    assert(opTypes == expectedOpTypes)
  }

  protected lazy val expectedOpTypes: Set[String] = Set(
    "delta.dml.merge.findTouchedFiles", "delta.dml.merge.writeAllChanges", "delta.dml.merge")

  test("insert only merge - recorded operation") {
    var events: Seq[UsageRecord] = Seq.empty
    withKeyValueData(
      source = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: (4, 40) :: Nil,
      target = (1, 1) :: (2, 2) :: (3, 3) :: Nil,
      isKeyPartitioned = true) { case (sourceName, targetName) =>

      withSQLConf(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED.key -> "true") {
        events = Log4jUsageLogger.track {
          executeMerge(
            tgt = s"$targetName t",
            src = s"$sourceName s",
            cond = "s.key = t.key AND t.key > 1",
            insert(condition = "s.key = 4", values = "(key, value) VALUES (s.key, s.value)"))
        }
      }

      checkAnswer(sql(getDeltaFileStmt(tempPath)), Seq(
        Row(1, 1),  // existed previously
        Row(2, 2),  // existed previously
        Row(3, 3),  // existed previously
        Row(4, 40)  // inserted
      ))
    }

    // Get recorded operations from usage events
    val opTypes = events.filter { e =>
      e.metric == "sparkOperationDuration" && e.opType.get.typeName.contains("delta.dml.merge")
    }.map(_.opType.get.typeName).toSet

    assert(opTypes == Set(
      "delta.dml.merge", "delta.dml.merge.writeInsertsOnlyWhenNoMatchedClauses"))
  }

  test("recorded operations - write inserts only") {
    var events: Seq[UsageRecord] = Seq.empty
    withKeyValueData(
      source = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: (4, 40) :: Nil,
      target = (1, 1) :: (2, 2) :: (3, 3) :: Nil,
      isKeyPartitioned = true) { case (sourceName, targetName) =>

      events = Log4jUsageLogger.track {
        executeMerge(
          tgt = s"$targetName t",
          src = s"$sourceName s",
          cond = "s.key = t.key AND s.key > 5",
          update(condition = "s.key > 10", set = "key = s.key, value = s.value"),
          insert(condition = "s.key < 1", values = "(key, value) VALUES (s.key, s.value)"))
      }

      checkAnswer(sql(getDeltaFileStmt(tempPath)), Seq(
        Row(0, 0),   // inserted
        Row(1, 1),   // existed previously
        Row(2, 2),   // existed previously
        Row(3, 3)    // existed previously
      ))
    }

    // Get recorded operations from usage events
    val opTypes = events.filter { e =>
      e.metric == "sparkOperationDuration" && e.opType.get.typeName.contains("delta.dml.merge")
    }.map(_.opType.get.typeName).toSet

    assert(opTypes == expectedOpTypesInsertOnly)
  }

  protected lazy val expectedOpTypesInsertOnly: Set[String] = Set(
    "delta.dml.merge.findTouchedFiles",
    "delta.dml.merge.writeInsertsOnlyWhenNoMatches",
    "delta.dml.merge")
}


@SQLUserDefinedType(udt = classOf[SimpleTestUDT])
case class SimpleTest(value: Int)

class SimpleTestUDT extends UserDefinedType[SimpleTest] {
  override def sqlType: DataType = IntegerType

  override def serialize(input: SimpleTest): Any = input.value

  override def deserialize(datum: Any): SimpleTest = datum match {
    case a: Int => SimpleTest(a)
  }

  override def userClass: Class[SimpleTest] = classOf[SimpleTest]
}

@SQLUserDefinedType(udt = classOf[ComplexTestUDT])
case class ComplexTest(key: Int, values: Array[Int])

class ComplexTestUDT extends UserDefinedType[ComplexTest] {
  override def sqlType: DataType = StructType(Seq(
    StructField("key", IntegerType),
    StructField("values", ArrayType(IntegerType, containsNull = false))))

  override def serialize(input: ComplexTest): Any = {
    val row = new GenericInternalRow(2)
    row.setInt(0, input.key)
    row.update(1, UnsafeArrayData.fromPrimitiveArray(input.values))
    row
  }

  override def deserialize(datum: Any): ComplexTest = datum match {
    case row: InternalRow =>
      ComplexTest(row.getInt(0), row.getArray(1).toIntArray())
  }

  override def userClass: Class[ComplexTest] = classOf[ComplexTest]
}
