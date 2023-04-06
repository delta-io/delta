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
import java.util.Locale

import scala.language.implicitConversions

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecution
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

abstract class MergeIntoSuiteBase
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfterEach    with SQLTestUtils
    with DeltaTestUtilsForTempViews
    with MergeHelpers {

  import testImplicits._

  protected var tempDir: File = _

  protected def tempPath: String = tempDir.getCanonicalPath

  override def beforeEach() {
    super.beforeEach()
    // Using a space in path to provide coverage for special characters.
    tempDir = Utils.createTempDir(namePrefix = "spark test")
  }

  override def afterEach() {
    try {
      Utils.deleteRecursively(tempDir)
    } finally {
      super.afterEach()
    }
  }

  protected def executeMerge(
      target: String,
      source: String,
      condition: String,
      update: String,
      insert: String): Unit

  protected def executeMerge(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): Unit

  protected def append(df: DataFrame, partitions: Seq[String] = Nil): Unit = {
    val dfw = df.write.format("delta").mode("append")
    if (partitions.nonEmpty) {
      dfw.partitionBy(partitions: _*)
    }
    dfw.save(tempPath)
  }

  protected def readDeltaTable(path: String): DataFrame = {
    spark.read.format("delta").load(path)
  }

  protected def withCrossJoinEnabled(body: => Unit): Unit = {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") { body }
  }

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

  protected def errorContains(errMsg: String, str: String): Unit = {
    assert(errMsg.toLowerCase(Locale.ROOT).contains(str.toLowerCase(Locale.ROOT)))
  }

  def errorNotContains(errMsg: String, str: String): Unit = {
    assert(!errMsg.toLowerCase(Locale.ROOT).contains(str.toLowerCase(Locale.ROOT)))
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

  test("Negative case - non-delta target") {
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
      errorContains(e, "MERGE destination only supports Delta sources")
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

  protected def withKeyValueData(
      source: Seq[(Int, Int)],
      target: Seq[(Int, Int)],
      isKeyPartitioned: Boolean = false,
      sourceKeyValueNames: (String, String) = ("key", "value"),
      targetKeyValueNames: (String, String) = ("key", "value"))(
      thunk: (String, String) => Unit = null): Unit = {

    append(target.toDF(targetKeyValueNames._1, targetKeyValueNames._2).coalesce(2),
      if (isKeyPartitioned) Seq(targetKeyValueNames._1) else Nil)
    withTempView("source") {
      source.toDF(sourceKeyValueNames._1, sourceKeyValueNames._2).createOrReplaceTempView("source")
      thunk("source", s"delta.`$tempPath`")
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

  protected def testAnalysisErrorsInExtendedMerge(
      name: String,
      namePrefix: String = "extended syntax")(
      mergeOn: String,
      mergeClauses: MergeClause*)(
      errorStrs: Seq[String],
      notErrorStrs: Seq[String] = Nil): Unit = {
    test(s"$namePrefix - analysis errors - $name") {
      withKeyValueData(
        source = Seq.empty,
        target = Seq.empty,
        sourceKeyValueNames = ("key", "srcValue"),
        targetKeyValueNames = ("key", "tgtValue")
      ) { case (sourceName, targetName) =>
        val errMsg = intercept[AnalysisException] {
          executeMerge(s"$targetName t", s"$sourceName s", mergeOn, mergeClauses: _*)
        }.getMessage
        errorStrs.foreach { s => errorContains(errMsg, s) }
        notErrorStrs.foreach { s => errorNotContains(errMsg, s) }
      }
    }
  }

  testAnalysisErrorsInExtendedMerge("update condition - ambiguous reference")(
    mergeOn = "s.key = t.key",
    update(condition = "key > 1", set = "tgtValue = srcValue"))(
    errorStrs = "reference" :: "key" :: "is ambiguous" :: Nil)

  testAnalysisErrorsInExtendedMerge("update condition - unknown reference")(
    mergeOn = "s.key = t.key",
    update(condition = "unknownAttrib > 1", set = "tgtValue = srcValue"))(
    // Should show unknownAttrib as invalid ref and (key, tgtValue, srcValue) as valid column names.
    errorStrs = "UPDATE condition" :: "unknownAttrib" :: "key" :: "tgtValue" :: "srcValue" :: Nil)

  testAnalysisErrorsInExtendedMerge("update condition - aggregation function")(
    mergeOn = "s.key = t.key",
    update(condition = "max(0) > 0", set = "tgtValue = srcValue"))(
    errorStrs = "UPDATE condition" :: "aggregate functions are not supported" :: Nil)

  testAnalysisErrorsInExtendedMerge("update condition - subquery")(
    mergeOn = "s.key = t.key",
    update(condition = "s.value in (select value from t)", set = "tgtValue = srcValue"))(
    errorStrs = Nil) // subqueries fail for unresolved reference to `t`

  testAnalysisErrorsInExtendedMerge("delete condition - ambiguous reference")(
    mergeOn = "s.key = t.key",
    delete(condition = "key > 1"))(
    errorStrs = "reference" :: "key" :: "is ambiguous" :: Nil)

  testAnalysisErrorsInExtendedMerge("delete condition - unknown reference")(
    mergeOn = "s.key = t.key",
    delete(condition = "unknownAttrib > 1"))(
    // Should show unknownAttrib as invalid ref and (key, tgtValue, srcValue) as valid column names.
    errorStrs = "DELETE condition" :: "unknownAttrib" :: "key" :: "tgtValue" :: "srcValue" :: Nil)

  testAnalysisErrorsInExtendedMerge("delete condition - aggregation function")(
    mergeOn = "s.key = t.key",
    delete(condition = "max(0) > 0"))(
    errorStrs = "DELETE condition" :: "aggregate functions are not supported" :: Nil)

  testAnalysisErrorsInExtendedMerge("delete condition - subquery")(
    mergeOn = "s.key = t.key",
    delete(condition = "s.srcValue in (select tgtValue from t)"))(
    errorStrs = Nil)  // subqueries fail for unresolved reference to `t`

  testAnalysisErrorsInExtendedMerge("insert condition - unknown reference")(
    mergeOn = "s.key = t.key",
    insert(condition = "unknownAttrib > 1", values = "(key, tgtValue) VALUES (s.key, s.srcValue)"))(
    // Should show unknownAttrib as invalid ref and (key, srcValue) as valid column names,
    // but not show tgtValue as a valid name as target columns cannot be present in insert clause.
    errorStrs = "INSERT condition" :: "unknownAttrib" :: "key" :: "srcValue" :: Nil,
    notErrorStrs = "tgtValue")

  testAnalysisErrorsInExtendedMerge("insert condition - reference to target table column")(
    mergeOn = "s.key = t.key",
    insert(condition = "tgtValue > 1", values = "(key, tgtValue) VALUES (s.key, s.srcValue)"))(
    // Should show tgtValue as invalid ref and (key, srcValue) as valid column names
    errorStrs = "INSERT condition" :: "tgtValue" :: "key" :: "srcValue" :: Nil)

  testAnalysisErrorsInExtendedMerge("insert condition - aggregation function")(
    mergeOn = "s.key = t.key",
    insert(condition = "max(0) > 0", values = "(key, tgtValue) VALUES (s.key, s.srcValue)"))(
    errorStrs = "INSERT condition" :: "aggregate functions are not supported" :: Nil)

  testAnalysisErrorsInExtendedMerge("insert condition - subquery")(
    mergeOn = "s.key = t.key",
    insert(
      condition = "s.srcValue in (select srcValue from s)",
      values = "(key, tgtValue) VALUES (s.key, s.srcValue)"))(
    errorStrs = Nil)  // subqueries fail for unresolved reference to `s`


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

  protected def testExtendedMergeErrorOnMultipleMatches(
      name: String)(
      source: Seq[(Int, Int)],
      target: Seq[(Int, Int)],
      mergeOn: String,
      mergeClauses: MergeClause*): Unit = {
    test(s"extended syntax - $name") {
      withKeyValueData(source, target) { case (sourceName, targetName) =>
        val errMsg = intercept[UnsupportedOperationException] {
          executeMerge(s"$targetName t", s"$sourceName s", mergeOn, mergeClauses: _*)
        }.getMessage.toLowerCase(Locale.ROOT)
        assert(errMsg.contains("cannot perform merge as multiple source rows matched"))
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

  testExtendedMergeErrorOnMultipleMatches("only update with multiple matches")(
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

  testExtendedMergeErrorOnMultipleMatches("only conditional update with multiple matches")(
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

  testExtendedMergeErrorOnMultipleMatches("only conditional delete with multiple matches")(
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

  testExtendedMergeErrorOnMultipleMatches("conditional update + delete with multiple matches")(
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

  testExtendedMergeErrorOnMultipleMatches(
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

  testExtendedMergeErrorOnMultipleMatches(
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
    // target files.
    assert(metrics("numTargetFilesRemoved").toInt > 0)
  }

  test("insert only merge - multiple matches when feature flag off") {
    withSQLConf(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED.key -> "false") {
      // Verify that in case of multiple matches, it throws error rather than producing
      // incorrect results.
      withKeyValueData(
        source = (1, 10) :: (1, 100) :: (2, 20) :: Nil,
        target = (1, 1) :: Nil
      ) { case (sourceName, targetName) =>
        val errMsg = intercept[UnsupportedOperationException] {
          executeMerge(
            s"$targetName t",
            s"$sourceName s",
            "s.key = t.key",
            insert(values = "(key, value) VALUES (s.key, s.value)"))
        }.getMessage.toLowerCase(Locale.ROOT)
        assert(errMsg.contains("cannot perform merge as multiple source rows matched"))
      }

      // Verify that in case of multiple matches, it throws error rather than producing
      // incorrect results.
      withKeyValueData(
        source = (1, 10) :: (1, 100) :: (2, 20) :: (2, 200) :: Nil,
        target = (1, 1) :: Nil
      ) { case (sourceName, targetName) =>
        val errMsg = intercept[UnsupportedOperationException] {
          executeMerge(
            s"$targetName t",
            s"$sourceName s",
            "s.key = t.key",
            insert(condition = "s.value = 20", values = "(key, value) VALUES (s.key, s.value)"))
        }.getMessage.toLowerCase(Locale.ROOT)
        assert(errMsg.contains("cannot perform merge as multiple source rows matched"))
      }
    }
  }

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

  /**
   * Parse the input JSON data into a dataframe, one row per input element.
   * Throws an exception on malformed inputs or records that don't comply with the provided schema.
   */
  protected def readFromJSON(data: Seq[String], schema: StructType = null): DataFrame = {
    if (schema != null) {
      spark.read
        .schema(schema)
        .option("mode", FailFastMode.name)
        .json(data.toDS)
    } else {
      spark.read
        .option("mode", FailFastMode.name)
        .json(data.toDS)
    }
  }

  // scalastyle:off argcount
  protected def testNestedStructsEvolution(name: String)(
      target: Seq[String],
      source: Seq[String],
      targetSchema: StructType,
      sourceSchema: StructType,
      clauses: Seq[MergeClause] = Seq.empty,
      result: Seq[String] = null,
      resultSchema: StructType = null,
      resultWithoutEvolution: Seq[String] = null,
      expectErrorContains: String = null,
      expectErrorWithoutEvolutionContains: String = null,
      confs: Seq[(String, String)] = Seq()): Unit = {
    testEvolution(name) (
      targetData = readFromJSON(target, targetSchema),
      sourceData = readFromJSON(source, sourceSchema),
      clauses = clauses,
      expected =
        if (result != null ) {
          val schema = if (resultSchema != null) resultSchema else targetSchema
          readFromJSON(result, schema)
        } else {
          null
        },
      expectErrorContains = expectErrorContains,
      expectedWithoutEvolution =
        if (resultWithoutEvolution != null) {
          readFromJSON(resultWithoutEvolution, targetSchema)
        } else {
          null
        },
      expectErrorWithoutEvolutionContains = expectErrorWithoutEvolutionContains,
      confs = confs
    )
  }

  protected def testEvolution(name: String)(
      targetData: => DataFrame,
      sourceData: => DataFrame,
      clauses: Seq[MergeClause] = Seq.empty,
      expected: => DataFrame = null,
      expectedWithoutEvolution: => DataFrame = null,
      expectErrorContains: String = null,
      expectErrorWithoutEvolutionContains: String = null,
      confs: Seq[(String, String)] = Seq()) = {
    test(s"schema evolution - $name - with evolution disabled") {
      withSQLConf(confs: _*) {
        append(targetData)
        withTempView("source") {
          sourceData.createOrReplaceTempView("source")

          if (expectErrorWithoutEvolutionContains != null) {
            val ex = intercept[AnalysisException] {
              executeMerge(s"delta.`$tempPath` t", s"source s", "s.key = t.key",
                clauses.toSeq: _*)
            }
            errorContains(ex.getMessage, expectErrorWithoutEvolutionContains)
          } else {
            executeMerge(s"delta.`$tempPath` t", s"source s", "s.key = t.key",
              clauses.toSeq: _*)
            checkAnswer(
              spark.read.format("delta").load(tempPath),
              expectedWithoutEvolution.collect())
            assert(
              spark.read.format("delta").load(tempPath).schema.asNullable ===
                expectedWithoutEvolution.schema.asNullable)
          }
        }
      }
    }

    test(s"schema evolution - $name") {
      withSQLConf((confs :+ (DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key, "true")): _*) {
        append(targetData)
        withTempView("source") {
          sourceData.createOrReplaceTempView("source")

          if (expectErrorContains != null) {
            val ex = intercept[AnalysisException] {
              executeMerge(s"delta.`$tempPath` t", s"source s", "s.key = t.key",
                clauses.toSeq: _*)
            }
            assert(ex.getMessage.contains(expectErrorContains))
          } else {
            executeMerge(s"delta.`$tempPath` t", s"source s", "s.key = t.key",
              clauses.toSeq: _*)
            checkAnswer(
              spark.read.format("delta").load(tempPath),
              expected.collect())
            assert(spark.read.format("delta").load(tempPath).schema.asNullable ===
              expected.schema.asNullable)
          }
        }
      }
    }
  }

  testEvolution("new column with only insert *")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = insert("*") :: Nil,
    expected =
      ((0, 0, null) +: (3, 30, null) +: // unchanged
        (1, 10, null) +:  // not updated
        (2, 2, "extra2") +: Nil // newly inserted
      ).toDF("key", "value", "extra"),
    expectedWithoutEvolution =
      ((0, 0) +: (3, 30) +: (1, 10) +: (2, 2) +: Nil).toDF("key", "value")
  )

  testEvolution("new column with only update *")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = update("*") :: Nil,
    expected =
      ((0, 0, null) +: (3, 30, null) +:
        (1, 1, "extra1") +: // updated
        Nil // row 2 not inserted
      ).toDF("key", "value", "extra"),
    expectedWithoutEvolution = ((0, 0) +: (3, 30) +: (1, 1) +: Nil).toDF("key", "value")
  )

  // Schema evolution with UPDATE SET alone
  testEvolution("new column with update set")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = update(set = "key = s.key, value = s.value, extra = s.extra") :: Nil,
    expected = ((0, 0, null) +: (3, 30, null) +: (1, 1, "extra1") +: Nil)
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in UPDATE clause")

  testEvolution("new column updated with value from existing column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, -1), (2, 2, -2))
      .toDF("key", "value", "extra"),
    clauses = update(set = "extra = s.value") :: Nil,
    expected = ((0, 0, null) +: (1, 10, 1) +: (3, 30, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer, Integer)]]
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in UPDATE clause")

  // Schema evolution with INSERT alone
  testEvolution("new column with insert values")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = insert(values = "(key, value, extra) VALUES (s.key, s.value, s.extra)") :: Nil,
    expected = ((0, 0, null) +: (1, 10, null) +: (3, 30, null) +: (2, 2, "extra2") +: Nil)
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in INSERT clause")

   testEvolution("new column inserted with value from existing column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, -1), (2, 2, -2))
      .toDF("key", "value", "extra"),
    clauses = insert(values = "(key, extra) VALUES (s.key, s.value)") :: Nil,
    expected = ((0, 0, null) +: (1, 10, null) +: (3, 30, null) +: (2, null, 2) +: Nil)
      .asInstanceOf[List[(Integer, Integer, Integer)]]
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in INSERT clause")

  // Schema evolution (UPDATE) with two new columns in the source but only one added to the target.
  testEvolution("new column with update set and column not updated")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1", "unused1"), (2, 2, "extra2", "unused2"))
      .toDF("key", "value", "extra", "unused"),
    clauses = update(set = "extra = s.extra") :: Nil,
    expected = ((0, 0, null) +: (1, 10, "extra1") +: (3, 30, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer, String)]]
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in UPDATE clause")

  testEvolution("new column updated from other new column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1", "unused1"), (2, 2, "extra2", "unused2"))
      .toDF("key", "value", "extra", "unused"),
    clauses = update(set = "extra = s.unused") :: Nil,
    expected = ((0, 0, null) +: (1, 10, "unused1") +: (3, 30, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer, String)]]
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in UPDATE clause")

  // Schema evolution (INSERT) with two new columns in the source but only one added to the target.
  testEvolution("new column with insert values and column not inserted")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1", "unused1"), (2, 2, "extra2", "unused2"))
      .toDF("key", "value", "extra", "unused"),
    clauses = insert(values = "(key, extra) VALUES (s.key, s.extra)") :: Nil,
    expected = ((0, 0, null) +: (1, 10, null) +: (3, 30, null) +: (2, null, "extra2") +: Nil)
      .asInstanceOf[List[(Integer, Integer, String)]]
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in INSERT clause")

  testEvolution("new column inserted from other new column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1", "unused1"), (2, 2, "extra2", "unused2"))
      .toDF("key", "value", "extra", "unused"),
    clauses = insert(values = "(key, extra) VALUES (s.key, s.unused)") :: Nil,
    expected = ((0, 0, null) +: (1, 10, null) +: (3, 30, null) +: (2, null, "unused2") +: Nil)
      .asInstanceOf[List[(Integer, Integer, String)]]
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in INSERT clause")

  // Schema evolution with two new columns added by UPDATE and INSERT resp.
  testEvolution("new column added by insert and other new column added by update")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1", "other1"), (2, 2, "extra2", "other2"))
      .toDF("key", "value", "extra", "other"),
    clauses = update(set = "extra = s.extra") ::
      insert(values = "(key, other) VALUES (s.key, s.other)") :: Nil,
    expected =
      ((0, 0, null, null) +:
       (1, 10, "extra1", null) +:
       (3, 30, null, null) +:
       (2, null, null, "other2") +: Nil)
      .asInstanceOf[List[(Integer, Integer, String, String)]]
      .toDF("key", "value", "extra", "other"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in UPDATE clause")

  // Nested Schema evolution with UPDATE alone
  testNestedStructsEvolution("new nested source field added when updating top-level column")(
    target = """{ "key": "A", "value": { "a": 1 } }""",
    source = """{ "key": "A", "value": { "a": 2, "b": 3 } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
          .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)),
    clauses = update("value = s.value") :: Nil,
    result = """{ "key": "A", "value": { "a": 2, "b": 3 } }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("new nested source field not in update is ignored")(
    target = """{ "key": "A", "value": { "a": 1 } }""",
    source = """{ "key": "A", "value": { "a": 2, "b": 3 } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
          .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)),
    clauses = update("value.a = s.value.a") :: Nil,
    result = """{ "key": "A", "value": { "a": 2 } }""",
    resultWithoutEvolution = """{ "key": "A", "value": { "a": 2 } }""")

  testNestedStructsEvolution("two new nested source fields with update: one added, one ignored")(
    target = """{ "key": "A", "value": { "a": 1 } }""",
    source = """{ "key": "A", "value": { "a": 2, "b": 3, "c": 4 } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
          .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)
          .add("c", IntegerType)),
    clauses = update("value.b = s.value.b") :: Nil,
    result = """{ "key": "A", "value": { "a": 1, "b": 3 } }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)),
    expectErrorWithoutEvolutionContains = "No such struct field")


  // Nested Schema evolution with INSERT alone
  testNestedStructsEvolution("new nested source field added when inserting top-level column")(
    target = """{ "key": "A", "value": { "a": 1 } }""",
    source = """{ "key": "B", "value": { "a": 2, "b": 3 } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
          .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)),
    clauses = insert("(value) VALUES (s.value)") :: Nil,
    result =
    """{ "key": "A", "value": { "a": 1, "b": null } }
       { "key": null, "value": { "a": 2, "b": 3 } }""".stripMargin,
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("insert new nested source field not supported")(
    target = """{ "key": "A", "value": { "a": 1 } }""",
    source = """{ "key": "A", "value": { "a": 2, "b": 3, "c": 4 } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
          .add("a", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)
          .add("c", IntegerType)),
    clauses = insert("(value.b) VALUES (s.value.b)") :: Nil,
    expectErrorContains = "Nested field is not supported in the INSERT clause of MERGE operation",
    expectErrorWithoutEvolutionContains = "No such struct field")

  // No schema evolution
  testEvolution("old column updated from new column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, -1), (2, 2, -2))
      .toDF("key", "value", "extra"),
    clauses = update(set = "value = s.extra") :: Nil,
    expected = ((0, 0) +: (1, -1) +: (3, 30) +: Nil).toDF("key", "value"),
    expectedWithoutEvolution = ((0, 0) +: (1, -1) +: (3, 30) +: Nil).toDF("key", "value"))

  testEvolution("old column inserted from new column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, -1), (2, 2, -2))
      .toDF("key", "value", "extra"),
    clauses = insert(values = "(key) VALUES (s.extra)") :: Nil,
    expected = ((0, 0) +: (1, 10) +: (3, 30) +: (-2, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer)]]
      .toDF("key", "value"),
    expectedWithoutEvolution = ((0, 0) +: (1, 10) +: (3, 30) +: (-2, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer)]]
      .toDF("key", "value"))

  testEvolution("new column with insert existing column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = insert(values = "(key) VALUES (s.key)") :: Nil,
    expected = ((0, 0) +: (1, 10) +: (2, null) +: (3, 30) +: Nil)
      .asInstanceOf[List[(Integer, Integer)]]
      .toDF("key", "value"),
    expectedWithoutEvolution = ((0, 0) +: (1, 10) +: (2, null) +: (3, 30) +: Nil)
      .asInstanceOf[List[(Integer, Integer)]]
      .toDF("key", "value"))

  // Column doesn't exist with UPDATE/INSERT alone.
  testEvolution("update set nonexistent column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = update(set = "nonexistent = s.extra") :: Nil,
    expectErrorContains = "cannot resolve nonexistent in UPDATE clause",
    expectErrorWithoutEvolutionContains = "cannot resolve nonexistent in UPDATE clause")

  testEvolution("insert values nonexistent column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = insert(values = "(nonexistent) VALUES (s.extra)") :: Nil,
    expectErrorContains = "cannot resolve nonexistent in INSERT clause",
    expectErrorWithoutEvolutionContains = "cannot resolve nonexistent in INSERT clause")

  testEvolution("new column with update set and update *")(
    targetData = Seq((0, 0), (1, 10), (2, 20)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = update(condition = "s.key < 2", set = "value = s.value") :: update("*") :: Nil,
    expected =
      ((0, 0, null) +:
        (1, 1, null) +: // updated by first clause
        (2, 2, "extra2") +: // updated by second clause
        Nil
      ).toDF("key", "value", "extra"),
    expectedWithoutEvolution = ((0, 0) +: (1, 1) +: (2, 2) +: Nil).toDF("key", "value")
  )

  testEvolution("update * with column not in source")(
    targetData = Seq((0, 0, 0), (1, 10, 10), (3, 30, 30)).toDF("key", "value", "extra"),
    sourceData = Seq((1, 1), (2, 2)).toDF("key", "value"),
    clauses = update("*") :: Nil,
    // update went through even though `extra` wasn't there
    expected = ((0, 0, 0) +: (1, 1, 10) +: (3, 30, 30) +: Nil).toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in UPDATE clause"
  )

  testEvolution("insert * with column not in source")(
    targetData = Seq((0, 0, 0), (1, 10, 10), (3, 30, 30)).toDF("key", "value", "extra"),
    sourceData = Seq((1, 1), (2, 2)).toDF("key", "value"),
    clauses = insert("*") :: Nil,
    // insert went through even though `extra` wasn't there
    expected = ((0, 0, 0) +: (1, 10, 10) +: (2, 2, null) +: (3, 30, 30) +: Nil)
      .asInstanceOf[List[(Integer, Integer, Integer)]]
      .toDF("key", "value", "extra"),
    expectErrorWithoutEvolutionContains = "cannot resolve extra in INSERT clause"
  )

  testEvolution("explicitly insert subset of columns")(
    targetData = Seq((0, 0, 0), (1, 10, 10), (3, 30, 30)).toDF("key", "value", "extra"),
    sourceData = Seq((1, 1, 1), (2, 2, 2)).toDF("key", "value", "extra"),
    clauses = insert("(key, value) VALUES (s.key, s.value)") :: Nil,
    // 2 should have extra = null, since extra wasn't in the insert spec.
    expected = ((0, 0, 0) +: (1, 10, 10) +: (2, 2, null) +: (3, 30, 30) +: Nil)
      .asInstanceOf[List[(Integer, Integer, Integer)]]
      .toDF("key", "value", "extra"),
    expectedWithoutEvolution = ((0, 0, 0) +: (1, 10, 10) +: (2, 2, null) +: (3, 30, 30) +: Nil)
      .asInstanceOf[List[(Integer, Integer, Integer)]]
      .toDF("key", "value", "extra")
  )

  testEvolution("explicitly update one column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, 1), (2, 2, 2)).toDF("key", "value", "extra"),
    clauses = update("value = s.value") :: Nil,
    // Both results should be the same - we're checking that no evolution logic triggers
    // even though there's an extra source column.
    expected = ((0, 0) +: (1, 1) +: (3, 30) +: Nil).toDF("key", "value"),
    expectedWithoutEvolution = ((0, 0) +: (1, 1) +: (3, 30) +: Nil).toDF("key", "value")
  )

  testEvolution("new column with update non-* and insert *")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, 1), (2, 2, 2)).toDF("key", "value", "extra"),
    clauses = update("key = s.key, value = s.value") :: insert("*") :: Nil,
    expected = ((0, 0, null) +: (2, 2, 2) +: (3, 30, null) +:
      // null because `extra` isn't an update action, even though it's 1 in the source data
      (1, 1, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer, Integer)]].toDF("key", "value", "extra"),
    expectedWithoutEvolution = ((0, 0) +: (2, 2) +: (3, 30) +: (1, 1) +: Nil).toDF("key", "value")
  )

  // Note that the obvious dual of this test, "update * and insert non-*", doesn't exist
  // because nested columns can't be explicitly INSERTed to.
  testEvolution("new nested column with update non-* and insert *")(
    targetData = Seq((1, 1, 2, 3)).toDF("key", "a", "b", "c")
      .selectExpr("key", "named_struct('a', a, 'b', b, 'c', c) as x"),
    sourceData = Seq((1, 10, 30), (2, 20, 40)).toDF("key", "a", "c")
      .selectExpr("key", "named_struct('a', a, 'c', c) as x"),
    clauses = update("x.a = s.x.a") :: insert("*") :: Nil,
    expected = Seq((1, 10, 2, 3), (2, 20, null, 40))
      .asInstanceOf[List[(Integer, Integer, Integer, Integer)]]
      .toDF("key", "a", "b", "c")
      .selectExpr("key", "named_struct('a', a, 'b', b, 'c', c) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  testNestedStructsEvolution("add non-nullable column to target schema")(
    target = """{ "key": "A" }""",
    source = """{ "key": "B", "value": 4}""",
    targetSchema = new StructType()
      .add("key", StringType),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", IntegerType, nullable = false),
    clauses = update("*") :: Nil,
    result = """{ "key": "A", "value": null }""".stripMargin,
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", IntegerType, nullable = false),
    resultWithoutEvolution = """{ "key": "A" }""")

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("new nested column with update non-* and insert * - array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1, "c": 2 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": 10 } }, { "b": "3", "a": { "y": 30, "x": 20 } }, { "b": "4", "a": { "y": 30, "x": 20 } } ] }
           { "key": "B", "value": [ { "b": "3", "a": { "y": 30, "x": 40 } }, { "b": "4", "a": { "y": 30, "x": 40 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType))
          .add("b", IntegerType)
          .add("c", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20 }, "b": 2, "c": null}, { "a": { "x": 20, "y": 30}, "b": 3, "c": null }, { "a": { "x": 20, "y": 30}, "b": 4, "c": null } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "y": 30 }, "b": 3, "c": null }, { "a": { "x": 40, "y": 30}, "b": 4, "c": null } ] }""".stripMargin,
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("new nested column with update non-* and insert * - array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1, "c": 2 }, { "a": { "x": 3, "y": 2 }, "b": 2, "c": 2 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": 10 } } ] }
           { "key": "B", "value": [ { "b": "3", "a": { "y": 30, "x": 40 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType))
          .add("b", IntegerType)
          .add("c", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20}, "b": 2, "c": null } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "y": 30}, "b": 3, "c": null } ] }""".stripMargin,
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("new nested column with update non-* and insert * - nested array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3 } ] }, "b": 1, "c": 4 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": {"x": [ { "d": "30", "c": 10 }, { "d": "20", "c": 10 }, { "d": "20", "c": 10 } ], "y": 20 } } ] }
          { "key": "B", "value": [ { "b": "3", "a": {"x": [ { "d": "50", "c": 20 }, { "d": "20", "c": 10 } ], "y": 60 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
            )))
          .add("b", IntegerType)
          .add("c", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("x", ArrayType(
              new StructType()
                .add("d", StringType)
                .add("c", IntegerType)
            ))
            .add("y", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30 }, { "c": 10, "d": 20 }, { "c": 10, "d": 20 } ] }, "b": 2, "c": null}] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50 }, { "c": 10, "d": 20 } ] }, "b": 3, "c": null } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("new nested column with update non-* and insert * - nested array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3}, { "c": 2, "d": 3 } ] }, "b": 1, "c": 4 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": {"x": [ { "d": "30", "c": 10 } ], "y": 20 } } ] }
          { "key": "B", "value": [ { "b": "3", "a": {"x": [ { "d": "50", "c": 20 } ], "y": 60 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
            )))
          .add("b", IntegerType)
          .add("c", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("x", ArrayType(
              new StructType()
                .add("d", StringType)
                .add("c", IntegerType)
            ))
            .add("y", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30} ] }, "b": 2, "c": null}] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50 } ] }, "b": 3, "c": null } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")
  // scalastyle:on line.size.limit

  testEvolution("new nested-nested column with update non-* and insert *")(
    targetData = Seq((1, 1, 2, 3)).toDF("key", "a", "b", "c")
      .selectExpr("key", "named_struct('y', named_struct('a', a, 'b', b, 'c', c)) as x"),
    sourceData = Seq((1, 10, 30), (2, 20, 40)).toDF("key", "a", "c")
      .selectExpr("key", "named_struct('y', named_struct('a', a, 'c', c)) as x"),
    clauses = update("x.y.a = s.x.y.a") :: insert("*") :: Nil,
    expected = Seq((1, 10, 2, 3), (2, 20, null, 40))
      .asInstanceOf[List[(Integer, Integer, Integer, Integer)]]
      .toDF("key", "a", "b", "c")
      .selectExpr("key", "named_struct('y', named_struct('a', a, 'b', b, 'c', c)) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("new nested-nested column with update non-* and insert * - array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "z": 3 }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": 10 } }, { "b": "3", "a": { "y": 20, "x": 30 } }, { "b": "4", "a": { "y": 20, "x": 30 } } ] }
           { "key": "B", "value": [ { "b": "3", "a": { "y": 30, "x": 40 } }, { "b": "4", "a": { "y": 30, "x": 40 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": null }, "b": 2 }, { "a": { "x": 30, "y": 20, "z": null }, "b": 3}, { "a": { "x": 30, "y": 20, "z": null }, "b": 4 } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "y": 30, "z": null }, "b": 3 }, { "a": { "x": 40, "y": 30, "z": null }, "b": 4 } ] }""".stripMargin,
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("new nested-nested column with update non-* and insert * - array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "z": 3 }, "b": 1 }, { "a": { "x": 2, "y": 3, "z": 4 }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": { "y": 20, "x": 10 } } ] }
           { "key": "B", "value": [ { "b": "3", "a": { "y": 30, "x": 40 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType().add("y", IntegerType).add("x", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [{ "a": { "x": 10, "y": 20, "z": null }, "b": 2 }] }
           { "key": "B", "value": [{ "a": { "x": 40, "y": 30, "z": null }, "b": 3 }] }""".stripMargin,
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("new nested-nested column with update non-* and insert * - nested array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "e": 1 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": {"x": [ { "d": "30", "c": 10 }, { "d": "30", "c": 40 }, { "d": "30", "c": 50 } ], "y": 20 } } ] }
          { "key": "B", "value": [ { "b": "3", "a": {"x": [ { "d": "50", "c": 20 }, { "d": "50", "c": 30 } ], "y": 60 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("x", ArrayType(
              new StructType()
                .add("d", StringType)
                .add("c", IntegerType)
            ))
            .add("y", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30, "e": null }, { "c": 40, "d": 30, "e": null }, { "c": 50, "d": 30, "e": null } ] }, "b": 2 } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50, "e": null }, { "c": 30, "d": 50, "e": null } ] }, "b": 3 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("new nested-nested column with update non-* and insert * - nested array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "e": 1 }, { "c": 2, "d": 3, "e": 4 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": "2", "a": { "x": [ { "d": "30", "c": 10 } ], "y": 20 } } ] }
          { "key": "B", "value": [ { "b": "3", "a": { "x": [ { "d": "50", "c": 20 } ], "y": 60 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", StringType)
          .add("a", new StructType()
            .add("x", ArrayType(
              new StructType()
                .add("d", StringType)
                .add("c", IntegerType)
            ))
            .add("y", IntegerType)))),
    clauses = update("value = s.value") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30, "e": null } ] }, "b": 2 } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50, "e": null } ] }, "b": 3 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")
  // scalastyle:on line.size.limit

  testEvolution("new column with update * and insert non-*")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, 1), (2, 2, 2)).toDF("key", "value", "extra"),
    clauses = update("*") :: insert("(key, value) VALUES (s.key, s.value)") :: Nil,
    expected = ((0, 0, null) +: (1, 1, 1) +: (3, 30, null) +:
      // null because `extra` isn't an insert action, even though it's 2 in the source data
      (2, 2, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer, Integer)]].toDF("key", "value", "extra"),
    expectedWithoutEvolution = ((0, 0) +: (2, 2) +: (3, 30) +: (1, 1) +: Nil).toDF("key", "value")
  )

  testEvolution("evolve partitioned table")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF("key", "value", "extra"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = ((0, 0, null) +: (1, 1, "extra1") +: (2, 2, "extra2") +: (3, 30, null) +: Nil)
      .toDF("key", "value", "extra"),
    expectedWithoutEvolution = ((0, 0) +: (2, 2) +: (3, 30) +: (1, 1) +: Nil).toDF("key", "value")
  )

  testEvolution("star expansion with names including dots")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value.with.dotted.name"),
    sourceData = Seq((1, 1, "extra1"), (2, 2, "extra2")).toDF(
      "key", "value.with.dotted.name", "extra.dotted"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = ((0, 0, null) +: (1, 1, "extra1") +: (2, 2, "extra2") +: (3, 30, null) +: Nil)
      .toDF("key", "value.with.dotted.name", "extra.dotted"),
    expectedWithoutEvolution = ((0, 0) +: (2, 2) +: (3, 30) +: (1, 1) +: Nil)
      .toDF("key", "value.with.dotted.name")
  )

  // Note that incompatible types are those where a cast to the target type can't resolve - any
  // valid cast will be permitted.
  testEvolution("incompatible types in update *")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, Array[Byte](1)), (2, Array[Byte](2))).toDF("key", "value"),
    clauses = update("*") :: Nil,
    expectErrorContains =
      "Failed to merge incompatible data types IntegerType and BinaryType",
    expectErrorWithoutEvolutionContains = "cannot cast"
  )

  testEvolution("incompatible types in insert *")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, Array[Byte](1)), (2, Array[Byte](2))).toDF("key", "value"),
    clauses = insert("*") :: Nil,
    expectErrorContains = "Failed to merge incompatible data types IntegerType and BinaryType",
    expectErrorWithoutEvolutionContains = "cannot cast"
  )

  // All integral types other than long can be upcasted to integer.
  testEvolution("upcast numeric source types into integer target")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1.toByte, 1.toShort), (2.toByte, 2.toShort)).toDF("key", "value"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = Seq((0, 0), (1, 1), (2, 2), (3, 30)).toDF("key", "value"),
    expectedWithoutEvolution = Seq((0, 0), (1, 1), (2, 2), (3, 30)).toDF("key", "value")
  )

  // Delta's automatic schema evolution allows converting table columns with a numeric type narrower
  // than integer to integer, because in the underlying Parquet they're all stored as ints.
  testEvolution("upcast numeric target types from integer source")(
    targetData = Seq((0.toByte, 0.toShort), (1.toByte, 10.toShort)).toDF("key", "value"),
    sourceData = Seq((1, 1), (2, 2)).toDF("key", "value"),
    clauses = update("*") :: insert("*") :: Nil,
    expected =
      ((0.toByte, 0.toShort) +:
        (1.toByte, 1.toShort) +:
        (2.toByte, 2.toShort) +: Nil
        ).toDF("key", "value"),
    expectedWithoutEvolution =
      ((0.toByte, 0.toShort) +:
        (1.toByte, 1.toShort) +:
        (2.toByte, 2.toShort) +: Nil
        ).toDF("key", "value")
  )

  testEvolution("upcast int source type into long target")(
    targetData = Seq((0, 0L), (1, 10L), (3, 30L)).toDF("key", "value"),
    sourceData = Seq((1, 1), (2, 2)).toDF("key", "value"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = ((0, 0L) +: (1, 1L) +: (2, 2L) +: (3, 30L) +: Nil).toDF("key", "value"),
    expectedWithoutEvolution =
      ((0, 0L) +: (1, 1L) +: (2, 2L) +: (3, 30L) +: Nil).toDF("key", "value")
  )

  testEvolution("write string into int column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, "1"), (2, "2"), (5, "notANumber")).toDF("key", "value"),
    clauses = insert("*") :: Nil,
    expected = ((0, 0) +: (1, 10) +: (2, 2) +: (3, 30) +: (5, null) +: Nil)
      .asInstanceOf[List[(Integer, Integer)]].toDF("key", "value"),
    expectedWithoutEvolution =
      ((0, 0) +: (1, 10) +: (2, 2) +: (3, 30) +: (5, null) +: Nil)
        .asInstanceOf[List[(Integer, Integer)]].toDF("key", "value"),
    // Disable ANSI as this test needs to cast string "notANumber" to int
    confs = Seq(SQLConf.ANSI_ENABLED.key -> "false")
  )

  // This is kinda bug-for-bug compatibility. It doesn't really make sense that infinity is casted
  // to int as Int.MaxValue, but that's the behavior.
  testEvolution("write double into int column")(
    targetData = Seq((0, 0), (1, 10), (3, 30)).toDF("key", "value"),
    sourceData = Seq((1, 1.1), (2, 2.2), (5, Double.PositiveInfinity)).toDF("key", "value"),
    clauses = insert("*") :: Nil,
    expected =
      ((0, 0) +: (1, 10) +: (2, 2) +: (3, 30) +: (5, Int.MaxValue) +: Nil)
        .asInstanceOf[List[(Integer, Integer)]].toDF("key", "value"),
    expectedWithoutEvolution =
      ((0, 0) +: (1, 10) +: (2, 2) +: (3, 30) +: (5, Int.MaxValue) +: Nil)
        .asInstanceOf[List[(Integer, Integer)]].toDF("key", "value"),
    // Disable ANSI as this test needs to cast Double.PositiveInfinity to int
    confs = Seq(SQLConf.ANSI_ENABLED.key -> "false")
  )

  testEvolution("extra nested column in source - insert")(
    targetData = Seq((1, (1, 10))).toDF("key", "x"),
    sourceData = Seq((2, (2, 20, 30))).toDF("key", "x"),
    clauses = insert("*") :: Nil,
    expected = ((1, (1, 10, null)) +: (2, (2, 20, 30)) +: Nil)
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer))]].toDF("key", "x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  testEvolution("missing nested column in source - insert")(
    targetData = Seq((1, (1, 2, 3))).toDF("key", "x"),
    sourceData = Seq((2, (2, 3))).toDF("key", "x"),
    clauses = insert("*") :: Nil,
    expected = ((1, (1, 2, 3)) +: (2, (2, 3, null)) +: Nil)
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer))]].toDF("key", "x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  testEvolution("missing nested column resolved by name - insert")(
    targetData = Seq((1, 1, 2, 3)).toDF("key", "a", "b", "c")
      .selectExpr("key", "named_struct('a', a, 'b', b, 'c', c) as x"),
    sourceData = Seq((2, 2, 4)).toDF("key", "a", "c")
      .selectExpr("key", "named_struct('a', a, 'c', c) as x"),
    clauses = insert("*") :: Nil,
    expected = ((1, (1, 2, 3)) +: (2, (2, null, 4)) +: Nil)
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer))]].toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("missing nested column resolved by name - insert - array of struct")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "z": 1 }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "z": 20 }, "b": "2" } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "z": 30 }, "b": "3" }, { "a": { "x": 40, "z": 30 }, "b": "4" }, { "a": { "x": 40, "z": 30 }, "b": "5" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("z", IntegerType))
          .add("b", StringType))),
    clauses = insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "z": 1 }, "b": 1 } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "y": null, "z": 30 }, "b": 3 }, { "a": { "x": 40, "y": null, "z": 30 }, "b": 4 }, { "a": { "x": 40, "y": null, "z": 30 }, "b": 5 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("missing nested column resolved by name - insert - nested array of struct")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "e": 1 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "e": "30" } ] }, "b": "2" } ] }
          { "key": "B", "value": [ {"a": {"y": 60, "x": [ { "c": 20, "e": "50" }, { "c": 20, "e": "60" }, { "c": 20, "e": "80" } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("e", StringType)
            )))
          .add("b", StringType))),
    clauses = insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "e": 1 } ] }, "b": 1 } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": null, "e": 50 }, { "c": 20, "d": null, "e": 60 }, { "c": 20, "d": null, "e": 80 } ] }, "b": 3 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")
  // scalastyle:on line.size.limit

  testEvolution("additional nested column in source resolved by name - insert")(
    targetData = Seq((1, 10, 30)).toDF("key", "a", "c")
      .selectExpr("key", "named_struct('a', a, 'c', c) as x"),
    sourceData = Seq((2, 20, 30, 40)).toDF("key", "a", "b", "c")
      .selectExpr("key", "named_struct('a', a, 'b', b, 'c', c) as x"),
    clauses = insert("*") :: Nil,
    expected = ((1, (10, null, 30)) +: ((2, (20, 30, 40)) +: Nil))
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer))]].toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'c', x._3, 'b', x._2) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("additional nested column in source resolved by name - insert - array of struct")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "z": 2 }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": "2 "} ] }
           { "key": "B", "value": [ {"a": { "x": 40, "y": 30, "z": 3 }, "b": "3" }, {"a": { "x": 40, "y": 30, "z": 3 }, "b": "4" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("y", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 1, "z": 2, "y": null }, "b": 1 } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "z": 3, "y": 30 }, "b": 3 }, { "a": { "x": 40, "z": 3, "y": 30 }, "b": 4 } ] }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("z", IntegerType)
            .add("y", IntegerType))
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("additional nested column in source resolved by name - insert - nested array of struct")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "e": 3 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": "30", "e": 1 } ] }, "b": "2" } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": "50", "e": 2 }, { "c": 20, "d": "50", "e": 3 } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("e", IntegerType)
                .add("d", StringType)
            )))
          .add("b", IntegerType))),
    clauses = insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "e": 3, "d": null } ] }, "b": 1 } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "e": 2, "d": "50" }, { "c": 20, "e": 3, "d": "50" } ] }, "b": 3 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")
  // scalastyle:on line.size.limit

  testEvolution("extra nested column in source - update")(
    targetData = Seq((1, (1, 10)), (2, (2, 2000))).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'c', x._2) as x"),
    sourceData = Seq((1, (10, 100, 1000))).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    clauses = update("*") :: Nil,
    expected = ((1, (10, 100, 1000)) +: (2, (2, null, 2000)) +: Nil)
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer))]].toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'c', x._3, 'b', x._2) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("extra nested column in source - update - array of struct - longer source")(
    target =
      """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1 } ] }
         { "key": "B", "value": [ { "a": { "x": 40, "y": 30 }, "b": 3 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": "2" }, { "a": { "x": 10, "y": 20, "z": 2 }, "b": "3" }, { "a": { "x": 10, "y": 20, "z": 2 }, "b": "4" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("y", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": 2 }, { "a": { "x": 10, "y": 20, "z": 2 }, "b": 3 }, { "a": { "x": 10, "y": 20, "z": 2 }, "b": 4 } ] }
         { "key": "B", "value": [ { "a": { "x": 40, "y": 30, "z": null }, "b": 3 } ] }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("extra nested column in source - update - array of struct - longer target")(
    target =
      """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2 }, "b": 1 }, { "a": { "x": 1, "y": 2 }, "b": 2 } ] }
         { "key": "B", "value": [ { "a": { "x": 40, "y": 30 }, "b": 3 }, { "a": { "x": 40, "y": 30 }, "b": 4 }, { "a": { "x": 40, "y": 30 }, "b": 5 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": "2" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("y", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": 2 } ] }
         { "key": "B", "value": [ { "a": { "x": 40, "y": 30, "z": null }, "b": 3 }, { "a": { "x": 40, "y": 30, "z": null }, "b": 4 }, { "a": { "x": 40, "y": 30, "z": null }, "b": 5 } ] }""".stripMargin,
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("extra nested column in source - update - nested array of struct - longer source")(
    target =
      """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3 } ] }, "b": 1 } ] }
         { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50 } ] }, "b": 3 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": "30", "e": 1 }, { "c": 10, "d": "30", "e": 2 }, { "c": 10, "d": "30", "e": 3 } ] }, "b": 2 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30, "e": 1 }, { "c": 10, "d": 30, "e": 2 }, { "c": 10, "d": 30, "e": 3 } ] }, "b": 2 } ] }
         { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50, "e": null } ] }, "b": 3 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("extra nested column in source - update - nested array of struct - longer target")(
    target =
      """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3 }, { "c": 1, "d": 2 } ] }, "b": 1 } ] }
         { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50 }, { "c": 20, "d": 40 }, { "c": 20, "d": 60 } ] }, "b": 3 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": "30", "e": 1 } ] }, "b": "2" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30, "e": 1 } ] }, "b": 2 } ] }
         { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50, "e": null }, { "c": 20, "d": 40, "e": null }, { "c": 20, "d": 60, "e": null } ] }, "b": 3 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")
  // scalastyle:on line.size.limit

  testEvolution("missing nested column in source - update")(
    targetData = Seq((1, (1, 10, 100)), (2, (2, 20, 200))).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    sourceData = Seq((1, (0, 0))).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'c', x._2) as x"),
    clauses = update("*") :: Nil,
    expected = ((1, (0, 10, 0)) +: (2, (2, 20, 200)) +: Nil).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("missing nested column in source - update - array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "z": 3 }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "z": 2 }, "b": "2" }, { "a": { "x": 10, "z": 2 }, "b": "3" }, { "a": { "x": 10, "z": 2 }, "b": "4" } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "z": 3 }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": null, "z": 2 }, "b": 2 }, { "a": { "x": 10, "y": null, "z": 2 }, "b": 3 }, { "a": { "x": 10, "y": null, "z": 2 }, "b": 4 } ] }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("missing nested column in source - update - array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "z": 3 }, "b": 1 }, { "a": { "x": 1, "y": 2, "z": 3 }, "b": 2 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "z": 2 }, "b": "2" } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "z": 3 }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": null, "z": 2 }, "b": 2 } ] }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("missing nested column in source - update - nested array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "e": 4 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": {"y": 20, "x": [ { "c": 10, "e": 1 }, { "c": 10, "e": 2 }, { "c": 10, "e": 3 } ] }, "b": "2" } ] }
          { "key": "B", "value": [ { "a": {"y": 60, "x":  [{ "c": 20, "e": 2 } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": null, "e": 1 }, { "c": 10, "d": null, "e": 2 }, { "c": 10, "d": null, "e": 3} ] }, "b": 2 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("missing nested column in source - update - nested array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "e": 4 }, { "c": 1, "d": 3, "e": 5 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "e": 1 } ] }, "b": "2" } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "e": 2 } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    clauses = update("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": null, "e": 1 } ] }, "b": 2 } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")
  // scalastyle:on line.size.limit

  testEvolution("nested columns resolved by name with same column count but different names")(
    targetData = Seq((1, 1, 2, 3)).toDF("key", "a", "b", "c")
      .selectExpr("key", "struct(a, b, c) as x"),
    sourceData = Seq((1, 10, 20, 30), (2, 20, 30, 40)).toDF("key", "a", "b", "d")
      .selectExpr("key", "struct(a, b, d) as x"),
    clauses = update("*") :: insert("*") :: Nil,
    // We evolve to the schema (key, x.{a, b, c, d}).
    expected = ((1, (10, 20, 3, 30)) +: (2, (20, 30, null, 40)) +: Nil)
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer, Integer))]]
      .toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3, 'd', x._4) as x"),
    expectErrorWithoutEvolutionContains = "All nested columns must match."
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("nested columns resolved by name with same column count but different names - array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "o": 4 }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": "2" }, { "a": { "x": 10, "y": 20, "z": 2 }, "b": "3" }, { "a": { "x": 10, "y": 20, "z": 2 }, "b": "4" } ] }
           { "key": "B", "value": [ {"a": { "x": 40, "y": 30, "z": 3 }, "b": "3" }, {"a": { "x": 40, "y": 30, "z": 3 }, "b": "4" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("o", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("y", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "o": null, "z": 2 }, "b": 2 }, { "a": { "x": 10, "y": 20, "o": null, "z": 2 }, "b": 3 }, { "a": { "x": 10, "y": 20, "o": null, "z": 2 }, "b": 4 } ] }
           { "key": "B", "value": [ {"a": { "x": 40, "y": 30, "o": null, "z": 3 }, "b": 3 }, {"a": { "x": 40, "y": 30, "o": null, "z": 3 }, "b": 4 } ] }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("o", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("nested columns resolved by name with same column count but different names - array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "x": 1, "y": 2, "o": 4 }, "b": 1 }, { "a": { "x": 1, "y": 2, "o": 4 }, "b": 2 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": "2" } ] }
           { "key": "B", "value": [ {"a": { "x": 40, "y": 30, "z": 3 }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("o", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("y", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "o": null, "z": 2 }, "b": 2 } ] }
           { "key": "B", "value": [ {"a": { "x": 40, "y": 30, "o": null, "z": 3 }, "b": 3 } ] }""",
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("o", IntegerType)
            .add("z", IntegerType))
          .add("b", IntegerType))),
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("nested columns resolved by name with same column count but different names - nested array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "f": 4 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": "30", "e": 1 }, { "c": 10, "d": "30", "e": 2 }, { "c": 10, "d": "30", "e": 3 } ] }, "b": "2" } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": "50", "e": 2 }, { "c": 20, "d": "50", "e": 3 } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("f", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("f", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30, "f": null, "e": 1 }, { "c": 10, "d": 30, "f": null, "e": 2 }, { "c": 10, "d": 30, "f": null, "e": 3 } ] }, "b": 2 } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50, "f": null, "e": 2 }, { "c": 20, "d": 50, "f": null, "e": 3 } ] }, "b": 3} ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("nested columns resolved by name with same column count but different names - nested array of struct - longer target")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "f": 4 }, { "c": 1, "d": 3, "f": 4 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": "30", "e": 1 } ] }, "b": "2" } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": "50", "e": 2 } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("f", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("f", IntegerType)
                .add("e", IntegerType)
            )))
          .add("b", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": 30, "f": null, "e": 1 } ] }, "b": 2 } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": 50, "f": null, "e": 2 } ] }, "b": 3} ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")
  // scalastyle:on line.size.limit

  testEvolution("nested columns resolved by position with same column count but different names")(
    targetData = Seq((1, 1, 2, 3)).toDF("key", "a", "b", "c")
      .selectExpr("key", "struct(a, b, c) as x"),
    sourceData = Seq((1, 10, 20, 30), (2, 20, 30, 40)).toDF("key", "a", "b", "d")
      .selectExpr("key", "struct(a, b, d) as x"),
    clauses = update("*") :: insert("*") :: Nil,
    expectErrorContains = "cannot cast",
    expectedWithoutEvolution = ((1, (10, 20, 30)) +: (2, (20, 30, 40)) +: Nil)
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer))]]
      .toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("nested columns resolved by position with same column count but different names - array of struct - longer source")(
    target = """{ "key": "A", "value": [{ "a": { "x": 1, "y": 2, "o": 4 }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "z": 2 }, "b": "2" }, { "a": { "x": 10, "y": 20, "z": 3 }, "b": "2" }, { "a": { "x": 10, "y": 20, "z": 3 }, "b": "3" } ] }
           { "key": "B", "value": [ { "a": { "x": 40, "y": 30, "z": 3 }, "b": "3" }, { "a": { "x": 40, "y": 30, "z": 3 }, "b": "4" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("o", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("y", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: insert("*") :: Nil,
    resultWithoutEvolution =
      """{ "key": "A", "value": [ { "a": { "x": 10, "y": 20, "o": 2 }, "b": 2 }, { "a": { "x": 10, "y": 20, "o": 3 }, "b": 2 }, { "a": { "x": 10, "y": 20, "o": 3 }, "b": 3 } ] }
           { "key": "B", "value": [ {"a": { "x": 40, "y": 30, "o": 3 }, "b": 3 }, {"a": { "x": 40, "y": 30, "o": 3 }, "b": 4 } ] }""",
    expectErrorContains = "cannot cast",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)

  testNestedStructsEvolution("nested columns resolved by position with same column count but different names - array of struct - longer target")(
    target = """{ "key": "A", "value": [{ "a": { "x": 1, "y": 2, "o": 4 }, "b": 1}, { "a": { "x": 1, "y": 2, "o": 4 }, "b": 2}] }""",
    source =
      """{ "key": "A", "value": [{ "a": { "x": 10, "y": 20, "z": 2 }, "b": "2" } ] }
           { "key": "B", "value": [{"a": { "x": 40, "y": 30, "z": 3 }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType)
            .add("y", IntegerType)
            .add("o", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("x", IntegerType).add("y", IntegerType).add("z", IntegerType))
          .add("b", StringType))),
    clauses = update("*") :: insert("*") :: Nil,
    resultWithoutEvolution =
      """{ "key": "A", "value": [{ "a": { "x": 10, "y": 20, "o": 2}, "b": 2}] }
           { "key": "B", "value": [{"a": { "x": 40, "y": 30, "o": 3}, "b": 3}] }""",
    expectErrorContains = "cannot cast",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)

  testNestedStructsEvolution("nested columns resolved by position with same column count but different names - nested array of struct - longer source")(
    target = """{ "key": "A", "value": [ { "a": { "y": 2, "x": [ { "c": 1, "d": 3, "f": 4 } ] }, "b": 1 } ] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": "30", "e": 1 }, { "c": 10, "d": "30", "e": 2 }, { "c": 10, "d": "30", "e": 3} ] }, "b": "2" } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": "50", "e": 2 }, { "c": 20, "d": "50", "e": 3 } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("f", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    clauses = update("*") :: insert("*") :: Nil,
    resultWithoutEvolution =
      """{ "key": "A", "value": [ { "a": {"y": 20, "x": [ { "c": 10, "d": 30, "f": 1 }, { "c": 10, "d": 30, "f": 2 }, { "c": 10, "d": 30, "f": 3 } ] }, "b": 2 } ] }
          { "key": "B", "value": [ { "a": {"y": 60, "x": [ { "c": 20, "d": 50, "f": 2 }, { "c": 20, "d": 50, "f": 3 } ] }, "b": 3}]}""",
    expectErrorContains = "cannot cast",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)

  testNestedStructsEvolution("nested columns resolved by position with same column count but different names - nested array of struct - longer target")(
    target = """{ "key": "A", "value": [{ "a": { "y": 2, "x": [ { "c": 1, "d": 3, "f": 5 }, { "c": 1, "d": 3, "f": 6 } ] }, "b": 1}] }""",
    source =
      """{ "key": "A", "value": [ { "a": { "y": 20, "x": [ { "c": 10, "d": "30", "e": 1 } ] }, "b": "2" } ] }
          { "key": "B", "value": [ { "a": { "y": 60, "x": [ { "c": 20, "d": "50", "e": 2 } ] }, "b": "3" } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", IntegerType)
                .add("f", IntegerType)
            )))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType()
            .add("y", IntegerType)
            .add("x", ArrayType(
              new StructType()
                .add("c", IntegerType)
                .add("d", StringType)
                .add("e", IntegerType)
            )))
          .add("b", StringType))),
    clauses = update("*") :: insert("*") :: Nil,
    resultWithoutEvolution =
      """{ "key": "A", "value": [ { "a": {"y": 20, "x": [ { "c": 10, "d": 30, "f": 1 } ] }, "b": 2 } ] }
          { "key": "B", "value": [ { "a": {"y": 60, "x": [ { "c": 20, "d": 50, "f": 2 } ] }, "b": 3 } ] }""",
    expectErrorContains = "cannot cast",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)
  // scalastyle:on line.size.limit

  testEvolution("struct in different order")(
    targetData = Seq((1, (1, 10, 100)), (2, (2, 20, 200))).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    sourceData = Seq((1, (100, 10, 1)), (3, (300, 30, 3))).toDF("key", "x")
      .selectExpr("key", "named_struct('c', x._1, 'b', x._2, 'a', x._3) as x"),
    clauses = update("*") :: insert("*") :: Nil,
    expected = ((1, (1, 10, 100)) +: (2, (2, 20, 200)) +: (3, (3, 30, 300)) +: Nil).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    expectedWithoutEvolution =
      ((1, (1, 10, 100)) +: (2, (2, 20, 200)) +: (3, (3, 30, 300)) +: Nil).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x")
  )

  // scalastyle:off line.size.limit
  testNestedStructsEvolution("struct in different order - array of struct")(
    target = """{ "key": "A", "value": [{ "a": { "x": 1, "y": 2 }, "b": 1 }] }""",
    source = """{ "key": "A", "value": [{ "b": "2", "a": { "y": 20, "x": 10}}, { "b": "3", "a": { "y": 30, "x": 40}}] }""",
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
    clauses = update("*") :: insert("*") :: Nil,
    result = """{ "key": "A", "value": [{ "a": { "x": 10, "y": 20 }, "b": 2 }, { "a": { "y": 30, "x": 40}, "b": 3 }] }""",
    resultWithoutEvolution = """{ "key": "A", "value": [{ "a": { "x": 10, "y": 20 }, "b": 2 }, { "a": { "y": 30, "x": 40}, "b": 3 }] }""")

  testNestedStructsEvolution("struct in different order - nested array of struct")(
    target = """{ "key": "A", "value": [{ "a": { "y": 2, "x": [{ "c": 1, "d": 3}]}, "b": 1 }] }""",
    source = """{ "key": "A", "value": [{ "b": "2", "a": {"x": [{ "d": "30", "c": 10}, { "d": "40", "c": 3}], "y": 20}}]}""",
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
            .add("x", ArrayType(
              new StructType()
                .add("d", StringType)
                .add("c", IntegerType)
            ))
            .add("y", IntegerType)))),
    clauses = update("*") :: insert("*") :: Nil,
    result = """{ "key": "A", "value": [{ "a": { "y": 20, "x": [{ "c": 10, "d": 30}, { "c": 3, "d": 40}]}, "b": 2 }]}""",
    resultWithoutEvolution = """{ "key": "A", "value": [{ "a": { "y": 20, "x": [{ "c": 10, "d": 30}, { "c": 3, "d": 40}]}, "b": 2 }]}""")
  // scalastyle:on line.size.limit

  testNestedStructsEvolution("array of struct with same columns but in different order" +
    " which can be casted implicitly - by name")(
    target = """{ "key": "A", "value": [ { "a": 1, "b": 2 } ] }""",
    source =
      """{ "key": "A", "value": [ { "b": 4, "a": 3 } ] }
          { "key": "B", "value": [ { "b": 2, "a": 5 } ] }""".stripMargin,
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", IntegerType)
          .add("a", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    result = """{ "key": "A", "value": [ { "a": 3, "b": 4 } ] }
                { "key": "B", "value": [ { "a": 5, "b": 2 } ] }""".stripMargin,
    resultWithoutEvolution = """{ "key": "A", "value": [ { "a": 3, "b": 4 } ] }
                { "key": "B", "value": [ { "a": 5, "b": 2 } ] }""".stripMargin)

  testNestedStructsEvolution("array of struct with same columns but in different order" +
    " which can be casted implicitly - by position")(
    target = """{ "key": "A", "value": [ { "a": 1, "b": 2 } ] }""",
    source = """{ "key": "A", "value": [ { "b": 4, "a": 3 } ] }
                  { "key": "B", "value": [ { "b": 2, "a": 5 } ] }""".stripMargin,
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", IntegerType)
          .add("a", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    result =
      """{ "key": "A", "value": [ { "a": 4, "b": 3 } ] }
          { "key": "B", "value": [ { "a": 2, "b": 5 } ] }""".stripMargin,
    resultWithoutEvolution =
      """{ "key": "A", "value": [ { "a": 4, "b": 3 } ] }
          { "key": "B", "value": [ { "a": 2, "b": 5 } ] }""".stripMargin,
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)

  testNestedStructsEvolution("array of struct with same column count but all different names" +
    " - by name")(
    target = """{ "key": "A", "value": [ { "a": 1, "b": 2 } ] }""",
    source =
      """{ "key": "A", "value": [ { "c": 4, "d": 3 } ] }
          { "key": "B", "value": [ { "c": 2, "d": 5 } ] }""".stripMargin,
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("c", IntegerType)
          .add("d", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    resultSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)
          .add("c", IntegerType)
          .add("d", IntegerType))),
    result =
      """{ "key": "A", "value": [ { "a": null, "b": null, "c": 4, "d": 3 } ] }
          { "key": "B", "value": [ { "a": null, "b": null, "c": 2, "d": 5 } ] }""".stripMargin,
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("array of struct with same column count but all different names" +
    " - by position")(
    target = """{ "key": "A", "value": [ { "a": 1, "b": 2 } ] }""",
    source = """{ "key": "A", "value": [ { "c": 4, "d": 3 } ] }
                  { "key": "B", "value": [ { "c": 2, "d": 5 } ] }""".stripMargin,
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("c", IntegerType)
          .add("d", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    resultWithoutEvolution =
      """{ "key": "A", "value": [ { "a": 4, "b": 3 } ] }
          { "key": "B", "value": [ { "a": 2, "b": 5 } ] }""".stripMargin,
    expectErrorContains = " cannot cast",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)

  testNestedStructsEvolution("array of struct with same columns but in different order" +
    " which cannot be casted implicitly - by name")(
    target = """{ "key": "A", "value": [ { "a": {"c" : 1}, "b": 2 } ] }""",
    source = """{ "key": "A", "value": [ { "b": 4, "a": {"c" : 3 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType().add("c", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", IntegerType)
          .add("a", new StructType().add("c", IntegerType)))),
    clauses = update("*") :: insert("*") :: Nil,
    result = """{ "key": "A", "value": [ { "a": { "c" : 3 }, "b": 4 } ] }""",
    resultWithoutEvolution = """{ "key": "A", "value": [ { "a": { "c" : 3 }, "b": 4 } ] }""")

  testNestedStructsEvolution("array of struct with same columns but in different order" +
    " which cannot be casted implicitly - by position")(
    target = """{ "key": "A", "value": [ { "a": {"c" : 1}, "b": 2 } ] }""",
    source = """{ "key": "A", "value": [ { "b": 4, "a": {"c" : 3 } } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", new StructType().add("c", IntegerType))
          .add("b", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", IntegerType)
          .add("a", new StructType().add("c", IntegerType)))),
    clauses = update("*") :: insert("*") :: Nil,
    expectErrorContains = " cannot cast",
    expectErrorWithoutEvolutionContains = " cannot cast",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)

  testNestedStructsEvolution("array of struct with additional column in target - by name")(
    target = """{ "key": "A", "value": [ { "a": 1, "b": 2, "c": 3 } ] }""",
    source = """{ "key": "A", "value": [ { "b": 4, "a": 3 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)
          .add("c", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", IntegerType)
          .add("a", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    result = """{ "key": "A", "value": [ { "a": 3, "b": 4, "c": null } ] }""",
    expectErrorWithoutEvolutionContains = "Cannot cast")

  testNestedStructsEvolution("array of struct with additional column in target - by position")(
    target = """{ "key": "A", "value": [ { "a": 1, "b": 2, "c": 3 } ] }""",
    source = """{ "key": "A", "value": [ { "b": 4, "a": 3 } ] }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType)
          .add("c", IntegerType))),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(
        new StructType()
          .add("b", IntegerType)
          .add("a", IntegerType))),
    clauses = update("*") :: insert("*") :: Nil,
    expectErrorContains = " cannot cast",
    expectErrorWithoutEvolutionContains = "cannot cast",
    confs = (DeltaSQLConf.DELTA_RESOLVE_MERGE_UPDATE_STRUCTS_BY_NAME.key, "false") +: Nil)

  testEvolution("extra nested column in source - update - single target partition")(
    targetData = Seq((1, (1, 10)), (2, (2, 2000))).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'c', x._2) as x").repartition(1),
    sourceData = Seq((1, (10, 100, 1000))).toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'b', x._2, 'c', x._3) as x"),
    clauses = update("*") :: Nil,
    expected = ((1, (10, 100, 1000)) +: (2, (2, null, 2000)) +: Nil)
      .asInstanceOf[List[(Integer, (Integer, Integer, Integer))]].toDF("key", "x")
      .selectExpr("key", "named_struct('a', x._1, 'c', x._3, 'b', x._2) as x"),
    expectErrorWithoutEvolutionContains = "Cannot cast"
  )

  testEvolution("multiple clauses")(
    // 1 and 2 should be updated from the source, 3 and 4 should be deleted. Only 5 is unchanged
    targetData = Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")).toDF("key", "targetVal"),
    // 1 and 2 should be updated into the target, 6 and 7 should be inserted. 8 should be ignored
    sourceData = Seq((1, "t"), (2, "u"), (3, "v"), (4, "w"), (6, "x"), (7, "y"), (8, "z"))
      .toDF("key", "srcVal"),
    clauses =
      update("targetVal = srcVal", "s.key = 1") :: update("*", "s.key = 2") ::
      delete("s.key = 3") :: delete("s.key = 4") ::
      insert("(key) VALUES (s.key)", "s.key = 6") :: insert("*", "s.key = 7") :: Nil,
    expected =
      ((1, "t", null) :: (2, "b", "u") :: (5, "e", null) ::
        (6, null, null) :: (7, null, "y") :: Nil)
        .asInstanceOf[List[(Integer, String, String)]].toDF("key", "targetVal", "srcVal"),
    // The UPDATE * clause won't resolve without evolution because the source and target columns
    // don't match.
    expectErrorWithoutEvolutionContains = "cannot resolve targetVal"
  )

  testEvolution("multiple INSERT * clauses with UPDATE")(
    // 1 and 2 should be updated from the source, 3 and 4 should be deleted. Only 5 is unchanged
    targetData = Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")).toDF("key", "targetVal"),
    // 1 and 2 should be updated into the target, 6 and 7 should be inserted. 8 should be ignored
    sourceData = Seq((1, "t"), (2, "u"), (3, "v"), (4, "w"), (6, "x"), (7, "y"), (8, "z"))
      .toDF("key", "srcVal"),
    clauses =
      update("targetVal = srcVal", "s.key = 1") :: update("*", "s.key = 2") ::
      delete("s.key = 3") :: delete("s.key = 4") ::
      insert("*", "s.key = 6") :: insert("*", "s.key = 7") :: Nil,
    expected =
      ((1, "t", null) :: (2, "b", "u") :: (5, "e", null) ::
        (6, null, "x") :: (7, null, "y") :: Nil)
        .asInstanceOf[List[(Integer, String, String)]].toDF("key", "targetVal", "srcVal"),
    // The UPDATE * clause won't resolve without evolution because the source and target columns
    // don't match.
    expectErrorWithoutEvolutionContains = "cannot resolve targetVal"
  )

  /* unlimited number of merge clauses tests */

  protected def testUnlimitedClauses(
      name: String)(
      source: Seq[(Int, Int)],
      target: Seq[(Int, Int)],
      mergeOn: String,
      mergeClauses: MergeClause*)(
      result: Seq[(Int, Int)]): Unit =
    testExtendedMerge(name, "unlimited clauses")(source, target, mergeOn, mergeClauses : _*)(result)

  protected def testAnalysisErrorsInUnlimitedClauses(
      name: String)(
      mergeOn: String,
      mergeClauses: MergeClause*)(
      errorStrs: Seq[String],
      notErrorStrs: Seq[String] = Nil): Unit =
    testAnalysisErrorsInExtendedMerge(name, "unlimited clauses")(mergeOn, mergeClauses : _*)(
      errorStrs, notErrorStrs)

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

  testAnalysisErrorsInUnlimitedClauses("error on multiple insert clauses without condition")(
    mergeOn = "s.key = t.key",
    update(condition = "s.key == 3", set = "key = s.key, value = 2 * srcValue"),
    insert(condition = null, values = "(key, value) VALUES (s.key, srcValue)"),
    insert(condition = null, values = "(key, value) VALUES (s.key, 1 + srcValue)"))(
    errorStrs = "when there are more than one not matched clauses in a merge statement, " +
      "only the last not matched clause can omit the condition" :: Nil)

  testAnalysisErrorsInUnlimitedClauses("error on multiple update clauses without condition")(
    mergeOn = "s.key = t.key",
    update(condition = "s.key == 3", set = "key = s.key, value = 2 * srcValue"),
    update(condition = null, set = "key = s.key, value = 3 * srcValue"),
    update(condition = null, set = "key = s.key, value = 4 * srcValue"),
    insert(condition = null, values = "(key, value) VALUES (s.key, srcValue)"))(
    errorStrs = "when there are more than one matched clauses in a merge statement, " +
      "only the last matched clause can omit the condition" :: Nil)

  testAnalysisErrorsInUnlimitedClauses("error on multiple update/delete clauses without condition")(
    mergeOn = "s.key = t.key",
    update(condition = "s.key == 3", set = "key = s.key, value = 2 * srcValue"),
    delete(condition = null),
    update(condition = null, set = "key = s.key, value = 4 * srcValue"),
    insert(condition = null, values = "(key, value) VALUES (s.key, srcValue)"))(
    errorStrs = "when there are more than one matched clauses in a merge statement, " +
      "only the last matched clause can omit the condition" :: Nil)

  testAnalysisErrorsInUnlimitedClauses(
    "error on non-empty condition following empty condition for update clauses")(
    mergeOn = "s.key = t.key",
    update(condition = null, set = "key = s.key, value = 2 * srcValue"),
    update(condition = "s.key < 3", set = "key = s.key, value = srcValue"),
    insert(condition = null, values = "(key, value) VALUES (s.key, srcValue)"))(
    errorStrs = "when there are more than one matched clauses in a merge statement, " +
      "only the last matched clause can omit the condition" :: Nil)

  testAnalysisErrorsInUnlimitedClauses(
    "error on non-empty condition following empty condition for insert clauses")(
    mergeOn = "s.key = t.key",
    update(condition = null, set = "key = s.key, value = srcValue"),
    insert(condition = null, values = "(key, value) VALUES (s.key, srcValue)"),
    insert(condition = "s.key < 3", values = "(key, value) VALUES (s.key, 1 + srcValue)"))(
    errorStrs = "when there are more than one not matched clauses in a merge statement, " +
      "only the last not matched clause can omit the condition" :: Nil)

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

  testEvolution("array of struct should work with containsNull as false")(
    Seq(500000).toDF("key"),
    Seq(500000, 100000).toDF("key")
      .withColumn("generalDeduction",
        struct(current_date().as("date"), array(struct(lit(0d).as("data"))))),
    update("*") :: insert("*") :: Nil,
    Seq(500000, 100000).toDF("key")
      .withColumn("generalDeduction",
        struct(current_date().as("date"), array(struct(lit(0d).as("data"))))),
    Seq(500000, 100000).toDF("key")
  )

  testEvolution("test array_union with schema evolution")(
    Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2010 to 2019).map { i =>
            struct(
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i").as("location")
            )
          }: _*)),
    Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2020 to 8020).map { i =>
            struct(
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at")
            )
          }: _*)),
    update(set = "openings = array_union(s.openings, s.openings)") :: insert("*") :: Nil,
    Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2020 to 8020).map { i =>
            struct(
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
              lit(null).cast(StringType).as("opened_with"),
              lit(null).cast(StringType).as("location")
            )
          }: _*)),
    expectErrorWithoutEvolutionContains = "All nested columns must match"
  )

  testEvolution("test array_intersect with schema evolution")(
    Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2010 to 2019).map { i =>
            struct(
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i").as("location")
            )
          }: _*)),
    Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2020 to 8020).map { i =>
            struct(
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at")
            )
          }: _*)),
    update(set = "openings = array_intersect(s.openings, s.openings)") :: insert("*") :: Nil,
    Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2020 to 8020).map { i =>
            struct(
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
              lit(null).cast(StringType).as("opened_with"),
              lit(null).cast(StringType).as("location")
            )
          }: _*)),
    expectErrorWithoutEvolutionContains = "All nested columns must match"
  )

  testEvolution("test array_except with schema evolution")(
    Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2010 to 2020).map { i =>
            struct(
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i").as("location")
            )
          }: _*)),
    Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2020 to 8020).map { i =>
            struct(
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at")
            )
          }: _*)),
    update(set = "openings = array_except(s.openings, s.openings)") :: insert("*") :: Nil,
    Seq(1).toDF("key")
      .withColumn(
        "openings",
        array().cast(
          new ArrayType(
            new StructType()
              .add("opened_at", StringType)
              .add("opened_with", StringType)
              .add("location", StringType),
            true
          )
        )
      ),
    expectErrorWithoutEvolutionContains = "All nested columns must match"
  )

  testEvolution("test array_remove with schema evolution")(
    Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2010 to 2019).map { i =>
            struct(
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i").as("location")
            )
          }: _*)),
    Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2020 to 8020).map { i =>
            struct(
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at")
            )
          }: _*)),
    update(
      set = "openings = array_remove(s.openings," +
        "named_struct('opened_with', cast(null as string)," +
        "'opened_at', '2020-01-19T09:29:00.000+0000'))") :: insert("*") :: Nil,
    Seq(1).toDF("key")
      .withColumn(
        "openings",
        array((2021 to 8020).map { i =>
          struct(
            lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
            lit(null).cast(StringType).as("opened_with"),
            lit(null).cast(StringType).as("location")
          )
        }: _*)),
    expectErrorWithoutEvolutionContains = "All nested columns must match"
  )

  testEvolution("test array_distinct with schema evolution")(
    Seq(1).toDF("key")
      .withColumn("openings",
        array(
          (2010 to 2019).map { i =>
            struct(
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i").as("location")
            )
          }: _*
        )),
    Seq(1).toDF("key")
      .withColumn("openings",
        array(
          ((2020 to 8020) ++ (2020 to 8020)).map { i =>
            struct(
              lit(null).cast(StringType).as("opened_with"),
              lit(s"$i-01-19T09:29:00.000+0000").as("opened_at")
            )
          }: _*
        )),
    update(set = "openings = array_distinct(s.openings)") :: insert("*") :: Nil,
    Seq(1).toDF("key")
      .withColumn(
        "openings",
        array((2020 to 8020).map { i =>
          struct(
            lit(s"$i-01-19T09:29:00.000+0000").as("opened_at"),
            lit(null).cast(StringType).as("opened_with"),
            lit(null).cast(StringType).as("location")
          )
        }: _*)),
    expectErrorWithoutEvolutionContains = "All nested columns must match"
  )

  testEvolution("void columns are not allowed")(
    targetData = Seq((1, 1)).toDF("key", "value"),
    sourceData = Seq((1, 100, null), (2, 200, null)).toDF("key", "value", "extra"),
    clauses = update("*") :: insert("*") :: Nil,
    expectErrorContains = "Cannot add column 'extra' with type 'void'",
    expectedWithoutEvolution = Seq((1, 100), (2, 200)).toDF("key", "value")
  )

  testNestedStructsEvolution("nested void columns are not allowed")(
    target = """{ "key": "A", "value": { "a": { "x": 1 }, "b": 1 } }""",
    source = """{ "key": "A", "value": { "a": { "x": 2, "z": null } }""",
    targetSchema = new StructType()
      .add("key", StringType)
      .add("value",
        new StructType()
          .add("a", new StructType().add("x", IntegerType))
          .add("b", IntegerType)),
    sourceSchema = new StructType()
      .add("key", StringType)
      .add("value",
        new StructType()
          .add("a", new StructType().add("x", IntegerType).add("z", NullType))),
    clauses = update("*") :: Nil,
    expectErrorContains = "Cannot add column 'value.a.z' with type 'void'",
    expectErrorWithoutEvolutionContains = "All nested columns must match")


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

            val message = if (e.getCause != null) {
              e.getCause.getMessage
            } else e.getMessage
            assert(message.matches(expectedRegex.getOrElse(expectedErrorRegex)))
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

  testWithTempView("test merge on temp view - basic") { isSQLTempView =>
    withTable("tab") {
      withTempView("src") {
        Seq((0, 3), (1, 2)).toDF("key", "value").write.format("delta").saveAsTable("tab")
        createTempViewFromTable("tab", isSQLTempView)
        sql("CREATE TEMP VIEW src AS SELECT * FROM VALUES (1, 2), (3, 4) AS t(a, b)")
        executeMerge(
          target = "v",
          source = "src",
          condition = "src.a = v.key AND src.b = v.value",
          update = "v.value = src.b + 1",
          insert = "(v.key, v.value) VALUES (src.a, src.b)")
        checkAnswer(spark.table("v"), Seq(Row(0, 3), Row(1, 3), Row(3, 4)))
      }
    }
  }

  protected def testInvalidTempViews(name: String)(
      text: String,
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
            executeMerge(
              target = "v",
              source = "src",
              condition = "src.a = v.key AND src.b = v.value",
              update = "v.value = src.b + 1",
              insert = "(v.key, v.value) VALUES (src.a, src.b)")
            checkAnswer(spark.table("v"), Seq(Row(0, 3), Row(1, 3), Row(3, 4)))
          }
        }
      }
    }
  }

  testInvalidTempViews("subset cols")(
    text = "SELECT key FROM tab",
    expectedErrorMsgForSQLTempView = "cannot resolve",
    expectedErrorMsgForDataSetTempView = "cannot resolve"
  )

  testInvalidTempViews("superset cols")(
    text = "SELECT key, value, 1 FROM tab",
    // The analyzer can't tell whether the table originally had the extra column or not.
    expectedErrorMsgForSQLTempView =
      "The schema of your Delta table has changed in an incompatible way",
    expectedErrorMsgForDataSetTempView =
      "The schema of your Delta table has changed in an incompatible way"
  )


  private def testComplexTempViewOnMerge(name: String)(text: String, expectedResult: Seq[Row]) = {
    testOssOnlyWithTempView(s"test merge on temp view - $name") { isSQLTempView =>
      withTable("tab") {
        withTempView("src") {
          Seq((0, 3), (1, 2)).toDF("key", "value").write.format("delta").saveAsTable("tab")
          createTempViewFromSelect(text, isSQLTempView)
          sql("CREATE TEMP VIEW src AS SELECT * FROM VALUES (1, 2), (3, 4) AS t(a, b)")
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

  testComplexTempViewOnMerge("nontrivial projection")(
    "SELECT value as key, key as value FROM tab",
    Seq(Row(3, 0), Row(3, 1), Row(4, 3))
  )

  testComplexTempViewOnMerge("view with too many internal aliases")(
    "SELECT * FROM (SELECT * FROM tab AS t1) AS t2",
    Seq(Row(0, 3), Row(1, 3), Row(3, 4))
  )

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

trait MergeHelpers {
  /** A simple representative of a any WHEN clause in a MERGE statement */
  protected sealed trait MergeClause {
    def condition: String
    def action: String
    def clause: String
    def sql: String = {
      assert(action != null, "action not specified yet")
      val cond = if (condition != null) s"AND $condition" else ""
      s"WHEN $clause $cond THEN $action"
    }
  }

  protected case class MatchedClause(condition: String, action: String) extends MergeClause {
    override def clause: String = "MATCHED"
  }

  protected case class NotMatchedClause(condition: String, action: String) extends MergeClause {
    override def clause: String = "NOT MATCHED"
  }

  protected case class NotMatchedBySourceClause(condition: String, action: String)
    extends MergeClause {
    override def clause: String = "NOT MATCHED BY SOURCE"
  }

  protected def update(set: String = null, condition: String = null): MergeClause = {
    MatchedClause(condition, s"UPDATE SET $set")
  }

  protected def delete(condition: String = null): MergeClause = {
    MatchedClause(condition, s"DELETE")
  }

  protected def insert(values: String = null, condition: String = null): MergeClause = {
    NotMatchedClause(condition, s"INSERT $values")
  }

  protected def updateNotMatched(set: String = null, condition: String = null): MergeClause = {
    NotMatchedBySourceClause(condition, s"UPDATE SET $set")
  }

  protected def deleteNotMatched(condition: String = null): MergeClause = {
    NotMatchedBySourceClause(condition, s"DELETE")
  }
}
