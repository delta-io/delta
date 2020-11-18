/*
 * Copyright (2020) The Delta Lake Project Authors.
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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.types.StructType

class MergeIntoSQLSuite extends MergeIntoSuiteBase  with DeltaSQLCommandTest {

  import testImplicits._

  private def basicMergeStmt(
      target: String,
      source: String,
      condition: String,
      update: String,
      insert: String): String = {
    s"""
       |MERGE INTO $target
       |USING $source
       |ON $condition
       |WHEN MATCHED THEN UPDATE SET $update
       |WHEN NOT MATCHED THEN INSERT $insert
      """.stripMargin
  }

  override protected def executeMerge(
      target: String,
      source: String,
      condition: String,
      update: String,
      insert: String): Unit = {
    sql(basicMergeStmt(target, source, condition, update, insert))
  }

  override protected def executeMerge(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): Unit = {

    val merge = s"MERGE INTO $tgt USING $src ON $cond\n" + clauses.map(_.sql).mkString("\n")
    sql(merge)
  }

  override protected def testUnlimitedClauses(
      name: String)(
      source: Seq[(Int, Int)],
      target: Seq[(Int, Int)],
      mergeOn: String,
      mergeClauses: MergeClause*)(
      result: Seq[(Int, Int)]): Unit = {
    Seq(true, false).foreach { isPartitioned =>
      ignore(s"unlimited clauses - $name - isPartitioned: $isPartitioned ") {
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

  override protected def testAnalysisErrorsInUnlimitedClauses(
      name: String)(
      mergeOn: String,
      mergeClauses: MergeClause*)(
      errorStrs: Seq[String],
      notErrorStrs: Seq[String] = Nil): Unit = {
    ignore(s"unlimited caluses - analysis errors - $name") {
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

  test("CTE as a source in MERGE") {
    withTable("source") {
      Seq((1, 1), (0, 3)).toDF("key1", "value").write.saveAsTable("source")
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

      val cte = "WITH cte1 AS (SELECT key1 + 2 AS key3, value FROM source) "
      val merge = basicMergeStmt(
        target = s"delta.`$tempPath` as target",
        source = "cte1 src",
        condition = "src.key3 = target.key2",
        update = "key2 = 20 + src.key3, value = 20 + src.value",
        insert = "(key2, value) VALUES (src.key3 - 10, src.value + 10)")

      sql(cte + merge)
      checkAnswer(readDeltaTable(tempPath),
        Row(1, 4) :: // No change
        Row(22, 23) :: // Update
        Row(-7, 11) :: // Insert
        Nil)
    }
  }

  test("inline tables with set operations in source query") {
    withTable("source") {
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

      executeMerge(
        target = s"delta.`$tempPath` as trg",
        source =
          """
            |( SELECT * FROM VALUES (1, 6, "a") as t1(key1, value, others)
            |  UNION
            |  SELECT * FROM VALUES (0, 3, "b") as t2(key1, value, others)
            |) src
          """.stripMargin,
        condition = "src.key1 = trg.key2",
        update = "trg.key2 = 20 + key1, value = 20 + src.value",
        insert = "(trg.key2, value) VALUES (key1 - 10, src.value + 10)")

      checkAnswer(readDeltaTable(tempPath),
        Row(2, 2) :: // No change
          Row(21, 26) :: // Update
          Row(-10, 13) :: // Insert
          Nil)
    }
  }

  testNestedDataSupport("conflicting assignments between two nested fields")(
    source = """{ "key": "A", "value": { "a": { "x": 0 } } }""",
    target = """{ "key": "A", "value": { "a": { "x": 1 } } }""",
    update = "value.a.x = 2" :: "value.a.x = 3" :: Nil,
    errorStrs = "There is a conflict from these SET columns" :: Nil)

  test("Negative case - basic syntax analysis SQL") {
    withTable("source") {
      Seq((1, 1), (0, 3), (1, 5)).toDF("key1", "value").createOrReplaceTempView("source")
      append(Seq((2, 2), (1, 4)).toDF("key2", "value"))

      // duplicate column names in update clause
      var e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key2 = 1, key2 = 2",
          insert = "(key2, value) VALUES (3, 4)")
      }.getMessage

      errorContains(e, "There is a conflict from these SET columns")

      // duplicate column names in insert clause
      e = intercept[AnalysisException] {
        executeMerge(
          target = s"delta.`$tempPath` as target",
          source = "source src",
          condition = "src.key1 = target.key2",
          update = "key2 = 1, value = 2",
          insert = "(key2, key2) VALUES (3, 4)")
      }.getMessage

      errorContains(e, "Duplicate column names in INSERT clause")
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"no column is used from source table - column pruning, isPartitioned: $isPartitioned") {
      withTable("source") {
        val partitions = if (isPartitioned) "key2" :: Nil else Nil
        append(Seq((2, 2), (1, 4)).toDF("key2", "value"), partitions)
        Seq((1, 1, "a"), (0, 3, "b")).toDF("key1", "value", "col1")
          .createOrReplaceTempView("source")

        // filter pushdown can cause empty join conditions and cross-join being used
        withCrossJoinEnabled {
          val merge = basicMergeStmt(
            target = s"delta.`$tempPath`",
            source = "source src",
            condition = "key2 < 0", // no row match
            update = "key2 = 20, value = 20",
            insert = "(key2, value) VALUES (10, 10)")

          val df = sql(merge)

          val readSchema: Seq[StructType] = df.queryExecution.executedPlan.collect {
            case f: FileSourceScanExec => f.requiredSchema
          }
          assert(readSchema.flatten.isEmpty, "column pruning does not work")
        }

        checkAnswer(readDeltaTable(tempPath),
          Row(2, 2) :: // No change
          Row(1, 4) :: // No change
          Row(10, 10) :: // Insert
          Row(10, 10) :: // Insert
          Nil)
      }
    }
  }

  test("negative case - omit multiple insert conditions") {
    withTable("source") {
      Seq((1, 1), (0, 3)).toDF("srcKey", "srcValue").write.saveAsTable("source")
      append(Seq((2, 2), (1, 4)).toDF("trgKey", "trgValue"))

      // TODO: In DBR we throw AnalysisException, but in OSS Delta we throw ParseException.
      //       The error message is also slightly different. Here we just catch general Exception.
      //       We should update this test when OSS delta upgrades to Spark 3.1.

      // only the last NOT MATCHED clause can omit the condition
      val e = intercept[Exception](
        sql(s"""
          |MERGE INTO delta.`$tempPath`
          |USING source
          |ON srcKey = trgKey
          |WHEN NOT MATCHED THEN
          |  INSERT (trgValue, trgKey) VALUES (srcValue, srcKey + 1)
          |WHEN NOT MATCHED THEN
          |  INSERT (trgValue, trgKey) VALUES (srcValue, srcKey)
        """.stripMargin))
      assert(e.getMessage.contains("only the last NOT MATCHED clause can omit the condition") ||
        e.getMessage.contains("There should be at most 1 'WHEN NOT MATCHED' clause"))
    }
  }

  // TODO: remove after Delta runs on Spark 3.1 which has unlimited clause support
  test("negative case - too many clauses") {
    withTable("source") {
      Seq((1, 1), (0, 3)).toDF("srcKey", "srcValue").write.saveAsTable("source")
      append(Seq((2, 2), (1, 4)).toDF("trgKey", "trgValue"))

      // More than 2 match clauses are not allowed
      var e = intercept[ParseException](
        sql(s"""
          |MERGE INTO delta.`$tempPath`
          |USING source
          |ON srcKey = trgKey
          |WHEN MATCHED AND trgKey = 1 THEN
          |  UPDATE SET trgKey = srcKey, trgValue = srcValue
          |WHEN MATCHED AND trgValue = 2 THEN
          |  UPDATE SET trgKey = srcKey, trgValue = srcValue + 1
          |WHEN MATCHED AND trgValue = 3 THEN
          |  DELETE
          |WHEN NOT MATCHED THEN
          |  INSERT (trgValue, trgKey) VALUES (srcValue, srcKey)
        """.stripMargin))
      assert(e.getMessage.contains("There must be at most two match clauses in a MERGE query") ||
        e.getMessage.contains("There should be at most 2 'WHEN MATCHED' clauses"))

      // Multiple update actions not allowed
      e = intercept[ParseException](
        sql(s"""
          |MERGE INTO delta.`$tempPath`
          |USING source
          |ON srcKey = trgKey
          |WHEN MATCHED AND trgKey = 1 THEN
          |  UPDATE SET trgKey = srcKey, trgValue = srcValue
          |WHEN MATCHED AND trgValue = 2 THEN
          |  UPDATE SET trgKey = srcKey, trgValue = srcValue + 1
          |WHEN NOT MATCHED THEN
          |  INSERT (trgValue, trgKey) VALUES (srcValue, srcKey)
        """.stripMargin))
      assert(e.getMessage.contains("INSERT, UPDATE and DELETE cannot appear twice") ||
        e.getMessage.contains("UPDATE and DELETE can appear at most once in MATCHED clauses"))

      // Multiple delete actions not allowed
      e = intercept[ParseException](
        sql(s"""
          |MERGE INTO delta.`$tempPath`
          |USING source
          |ON srcKey = trgKey
          |WHEN MATCHED AND trgKey = 1 THEN
          |  DELETE
          |WHEN MATCHED AND trgValue = 2 THEN
          |  DELETE
          |WHEN NOT MATCHED THEN
          |  INSERT (trgValue, trgKey) VALUES (srcValue, srcKey)
        """.stripMargin))
      assert(e.getMessage.contains("INSERT, UPDATE and DELETE cannot appear twice") ||
        e.getMessage.contains("UPDATE and DELETE can appear at most once in MATCHED clauses"))
    }
  }
}
