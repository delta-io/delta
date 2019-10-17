/*
 * Copyright 2019 Databricks, Inc.
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

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, MapType, StringType, StructType}
import org.apache.spark.util.Utils

abstract class MergeIntoSuiteBase
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfterEach
    with SQLTestUtils {

  import testImplicits._

  protected var tempDir: File = _

  protected def tempPath: String = tempDir.getCanonicalPath

  override def beforeEach() {
    super.beforeEach()
    tempDir = Utils.createTempDir()
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
    test(s"basic case, isPartitioned: $isPartitioned") {
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
    Seq(true, false).foreach { isPartitioned =>
      test("basic case - merge to Delta table, " +
          s"isPartitioned: $isPartitioned skippingEnabled: $skippingEnabled") {
        withTable("delta_target", "source") {
          withSQLConf(DeltaSQLConf.DELTA_STATS_SKIPPING.key -> skippingEnabled.toString) {
            Seq((1, 1), (0, 3), (1, 6)).toDF("key1", "value").createOrReplaceTempView("source")
            val partitions = if (isPartitioned) "key2" :: Nil else Nil
            append(Seq((2, 2), (1, 4)).toDF("key2", "value"), partitions)
            readDeltaTable(tempPath).createOrReplaceTempView("delta_target")

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

  test("Self merge") {
    withTable("selfMergeTable") {
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))
      readDeltaTable(tempPath).createOrReplaceTempView("selfMergeTable")

      executeMerge(
        target = "selfMergeTable as target",
        source = "selfMergeTable as src",
        condition = "src.key2 = target.key2",
        update = "key2 = 20 + src.key2, value = 20 + src.value",
        insert = "(key2, value) VALUES (src.key2 - 10, src.value + 10)")

      checkAnswer(readDeltaTable(tempPath),
        Row(22, 22) :: // UPDATE
          Row(21, 24) :: // UPDATE
          Nil)
    }
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

      assert(e.contains(DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException.getMessage))
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
          update = "key2 = '33' + key2, value = '20'",
          insert = "(key2, value) VALUES ('44', src.value + '10')")

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

  def errorContains(errMsg: String, str: String): Unit = {
    assert(errMsg.toLowerCase(Locale.ROOT).contains(str.toLowerCase(Locale.ROOT)))
  }

  test("Negative case - basic syntax analysis") {
    withTable("source") {
      Seq((1, 1), (0, 3), (1, 5)).toDF("key1", "value").createOrReplaceTempView("source")
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

      // insert expressions are not complete
      var e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = key3",
          update = "key2 = key1, value = src.value",
          insert = "(key2, value) VALUES (key1, src.value)")
      }.getMessage

      errorContains(e, "Cannot resolve `key3` in search condition")

      // insert expressions are not complete
      e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key2 = key1, value = src.value",
          insert = "(value) VALUES (src.value + 10)")
      }.getMessage

      errorContains(e, "Unable to find the column 'key2'")
      errorContains(e, "INSERT clause must specify value for all the columns of the target table")

      // insert expressions have target table reference
      e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key2 = key1, value = src.value",
          insert = "(key2, value) VALUES (3, src.value + key2)")
      }.getMessage

      errorContains(e, "Cannot resolve `key2` in INSERT clause")

      // to-update columns have source table reference
      e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key1 = 1, value = 2",
          insert = "(key2, value) VALUES (3, 4)")
      }.getMessage

      errorContains(e, "Cannot resolve `key1` in UPDATE clause")

      // to-insert columns have source table reference
      e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key2 = 1, value = 2",
          insert = "(key1, value) VALUES (3, 4)")
      }.getMessage

      errorContains(e, "Cannot resolve `key1` in INSERT clause")

      // ambiguous reference
      e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key2 = 1, value = value",
          insert = "(key2, value) VALUES (3, 4)")
      }.getMessage

      errorContains(e, "Reference 'value' is ambiguous, could be: ")

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
      errorContains(e, "MERGE destination only supports Delta sources")
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
    errorContains(e, "Expect a full scan of Delta sources, but found the partial scan")
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

    append(target.toDF(targetKeyValueNames._1, targetKeyValueNames._2),
      if (isKeyPartitioned) Seq(targetKeyValueNames._1) else Nil)
    withTempView("source") {
      source.toDF(sourceKeyValueNames._1, sourceKeyValueNames._2).createOrReplaceTempView("source")
      thunk("source", s"delta.`$tempPath`")
    }
  }

  test("merge into cached table") {
    withTable("source") {
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))
      Seq((1, 1), (0, 3), (3, 3)).toDF("key1", "value").createOrReplaceTempView("source")
      readDeltaTable(tempPath).cache()
      readDeltaTable(tempPath).collect()

      append(Seq((100, 100), (3, 5)).toDF("key2", "value"))
      // cache is in effect, as the above change is not reflected
      checkAnswer(readDeltaTable(tempPath), Row(2, 2) :: Row(1, 4) :: Nil)

      executeMerge(
        target = s"delta.`$tempPath` as trgNew",
        source = "source src",
        condition = "src.key1 = key2",
        update = "value = trgNew.value + 3",
        insert = "(key2, value) VALUES (key1, src.value + 10)")

      checkAnswer(readDeltaTable(tempPath),
        Row(100, 100) :: // No change (newly inserted record)
          Row(2, 2) :: // No change
          Row(1, 7) :: // Update
          Row(3, 8) :: // Update (on newly inserted record)
          Row(0, 13) :: // Insert
          Nil)
    }
  }

  def testNestedDataSupport(name: String, namePrefix: String = "nested data support")(
      source: String,
      target: String,
      update: Seq[String],
      insert: String = null,
      schema: StructType = null,
      result: String = null,
      errorStrs: Seq[String] = null): Unit = {

    require(result == null ^ errorStrs == null, "either set the result or the error strings")

    val testName =
      if (result != null) s"$namePrefix - $name" else s"$namePrefix - analysis error - $name"

    test(testName) {
      withJsonData(source, target, schema) { case (sourceName, targetName) =>
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
          checkAnswer(spark.table(targetName), spark.read.json(strToJsonSeq(result).toDS))
        } else {
          val e = intercept[AnalysisException] { execMerge() }
          errorStrs.foreach { s => errorContains(e.getMessage, s) }
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
    schema = new StructType().add("key", StringType).add("value", MapType(StringType, IntegerType)),
    errorStrs = "Updating nested fields is only supported for StructType" :: Nil)

  testNestedDataSupport("updating array type")(
    source = """{ "key": "A", "value": [ { "a": 0 } ] }""",
    target = """{ "key": "A", "value": [ { "a": 1 } ] }""",
    update = "value.a = 2" :: Nil,
    schema = new StructType().add("key", StringType).add("value", MapType(StringType, IntegerType)),
    errorStrs = "Updating nested fields is only supported for StructType" :: Nil)

  /** A simple representative of a any WHEN clause in a MERGE statement */
  protected case class MergeClause(isMatched: Boolean, condition: String, action: String = null) {
    def sql: String = {
      assert(action != null, "action not specified yet")
      val matched = if (isMatched) "MATCHED" else "NOT MATCHED"
      val cond = if (condition != null) s"AND $condition" else ""
      s"WHEN $matched $cond THEN $action"
    }
  }

  protected def update(set: String = null, condition: String = null): MergeClause = {
    MergeClause(isMatched = true, condition, s"UPDATE SET $set")
  }

  protected def delete(condition: String = null): MergeClause = {
    MergeClause(isMatched = true, condition, s"DELETE")
  }

  protected def insert(values: String = null, condition: String = null): MergeClause = {
    MergeClause(isMatched = false, condition, s"INSERT $values")
  }

  protected def testAnalysisErrorsInExtendedMerge(
      name: String)(
      mergeOn: String,
      mergeClauses: MergeClause*)(
      errorStrs: Seq[String]): Unit = {
    test(s"extended syntax analysis errors - $name") {
      withKeyValueData(source = Seq.empty, target = Seq.empty) { case (sourceName, targetName) =>
        val e = intercept[AnalysisException] {
          executeMerge(s"$targetName t", s"$sourceName s", mergeOn, mergeClauses: _*)
        }
        errorStrs.foreach { s => errorContains(e.getMessage, s) }
      }
    }
  }

  testAnalysisErrorsInExtendedMerge("update condition - ambiguous reference")(
    mergeOn = "s.key = t.key",
    update(condition = "key > 1", set = "key = s.key, value = s.value"))(
    errorStrs = "reference 'key' is ambiguous" :: Nil)

  testAnalysisErrorsInExtendedMerge("update condition - unknown reference")(
    mergeOn = "s.key = t.key",
    update(condition = "unknownAttrib > 1", set = "key = s.key, value = s.value"))(
    errorStrs = "UPDATE condition" :: "unknownAttrib" :: Nil)

  testAnalysisErrorsInExtendedMerge("update condition - aggregation function")(
    mergeOn = "s.key = t.key",
    update(condition = "max(0) > 0", set = "key = s.key, value = s.value"))(
    errorStrs = "UPDATE condition" :: "aggregate functions are not supported" :: Nil)

  testAnalysisErrorsInExtendedMerge("update condition - subquery")(
    mergeOn = "s.key = t.key",
    update(condition = "s.value in (select value from t)", set = "key = s.key, value = s.value"))(
    errorStrs = Nil) // subqueries fail for unresolved reference to `t`

  testAnalysisErrorsInExtendedMerge("delete condition - unknown reference")(
    mergeOn = "s.key = t.key",
    delete(condition = "unknownAttrib > 1"))(
    errorStrs = "DELETE condition" :: "unknownAttrib" :: Nil)

  testAnalysisErrorsInExtendedMerge("delete condition - aggregation function")(
    mergeOn = "s.key = t.key",
    delete(condition = "max(0) > 0"))(
    errorStrs = "DELETE condition" :: "aggregate functions are not supported" :: Nil)

  testAnalysisErrorsInExtendedMerge("delete condition - subquery")(
    mergeOn = "s.key = t.key",
    delete(condition = "s.value in (select value from t)"))(
    errorStrs = Nil)  // subqueries fail for unresolved reference to `t`

  testAnalysisErrorsInExtendedMerge("insert condition - unknown reference")(
    mergeOn = "s.key = t.key",
    insert(condition = "unknownAttrib > 1", values = "(key, value) VALUES (s.key, s.value)"))(
    errorStrs = "INSERT condition" :: "unknownAttrib" :: Nil)

  testAnalysisErrorsInExtendedMerge("insert condition - aggregation function")(
    mergeOn = "s.key = t.key",
    insert(condition = "max(0) > 0", values = "(key, value) VALUES (s.key, s.value)"))(
    errorStrs = "INSERT condition" :: "aggregate functions are not supported" :: Nil)

  testAnalysisErrorsInExtendedMerge("insert condition - subquery")(
    mergeOn = "s.key = t.key",
    insert(
      condition = "s.value in (select value from s)",
      values = "(key, value) VALUES (s.key, s.value)"))(
    errorStrs = Nil)  // subqueries fail for unresolved reference to `t`


  private def testExtendedMerge(
      name: String)(
      source: Seq[(Int, Int)],
      target: Seq[(Int, Int)],
      mergeOn: String,
      mergeClauses: MergeClause*)(
      result: Seq[(Int, Int)]): Unit = {
    Seq(true, false).foreach { isPartitioned =>
      test(s"extended syntax - $name - isPartitioned: $isPartitioned ") {
        withKeyValueData(source, target, isPartitioned) { case (sourceName, targetName) =>
          executeMerge(s"$targetName t", s"$sourceName s", mergeOn, mergeClauses: _*)
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

  testExtendedMerge("only update")(
    source = (0, 0) :: (1, 10) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2)  :: Nil,
    mergeOn = "s.key = t.key",
    update(set = "key = s.key, value = s.value"))(
    result = Seq(
      (1, 10),  // (1, 1) updated
      (2, 2)
    ))


  test(s"extended syntax - only update with multiple matches") {
    withKeyValueData(
      source = (0, 0) :: (1, 10) :: (1, 11) :: (2, 20) :: Nil,
      target = (1, 1) :: (2, 2) :: Nil
    ) { case (sourceName, targetName) =>
      intercept[UnsupportedOperationException] {
        executeMerge(
          s"$targetName t",
          s"$sourceName s",
          "s.key = t.key",
          update(set = "key = s.key, value = s.value"))
      }
    }
  }

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

  testExtendedMerge("only delete")(
    source = (0, 0) :: (1, 10) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2) :: Nil,
    mergeOn = "s.key = t.key",
    delete())(
    result = Seq(
      (2, 2)    // (1, 1) deleted
    ))          // (3, 30) not inserted as not insert clause

  test(s"extended syntax - only delete with multiple matches") {
    withKeyValueData(
      source = (0, 0) :: (1, 10) :: (1, 100) :: (3, 30) :: Nil,
      target = (1, 1) :: (2, 2) :: Nil
    ) { case (sourceName, targetName) =>
      intercept[UnsupportedOperationException] {
        executeMerge(s"$targetName t", s"$sourceName s", "s.key = t.key", delete())
      }
    }
  }

  testExtendedMerge("only conditional delete")(
    source = (0, 0) :: (1, 10) :: (2, 20) :: (3, 30) :: Nil,
    target = (1, 1) :: (2, 2) :: (3, 3) :: Nil,
    mergeOn = "s.key = t.key",
    delete(condition = "s.value <> 20 AND t.value <> 3"))(
    result = Seq(
      (2, 2),   // not deleted due to source-only condition `s.value <> 20`
      (3, 3)    // not deleted due to target-only condition `t.value <> 3`
    ))          // (1, 1) deleted

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

  test(s"extended syntax - only insert with multiple matches") {
    withKeyValueData(
      source = (0, 0) :: (1, 10) :: (1, 100) :: (3, 30) :: (3, 300) :: Nil,
      target = (1, 1) :: (2, 2) :: Nil
    ) { case (sourceName, targetName) =>
      intercept[UnsupportedOperationException] {
        executeMerge(s"$targetName t", s"$sourceName s", "s.key = t.key",
          insert(values = "(key, value) VALUES (s.key, s.value)"))
      }
    }
  }

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
      schema: StructType = null)(
      thunk: (String, String) => Unit): Unit = {

    def toDF(strs: Seq[String]) = {
      if (schema != null) spark.read.schema(schema).json(strs.toDS) else spark.read.json(strs.toDS)
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
          checkAnswer(readDeltaTable(deltaPath), spark.read.json(result.toDS))
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

  testStar("update - target column not in source")(
    source = """{ "key": "a", "someValue" : 10 }""",
    target = """{ "key": "a", "value" : 1 }""",
    update(set = "*"),
    insert(values = "(key, value) VALUES (key, someValue)"))(
    errorStrs = "UPDATE clause" :: "value" :: Nil)

  testStar("insert - target column not in source")(
    source = """{ "key": "a", "someValue" : 10 }""",
    target = """{ "key": "a", "value" : 1 }""",
    update(set = "value = someValue"),
    insert(values = "*"))(
    errorStrs = "INSERT clause" :: "value" :: Nil)
}
