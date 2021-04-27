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

import java.util.Locale

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.tables._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

class MergeIntoScalaSuite extends MergeIntoSuiteBase  with DeltaSQLCommandTest {

  import testImplicits._

  test("basic scala API") {
    withTable("source") {
      append(Seq((1, 10), (2, 20)).toDF("key1", "value1"), Nil)  // target
      val source = Seq((1, 100), (3, 30)).toDF("key2", "value2")  // source

      io.delta.tables.DeltaTable.forPath(spark, tempPath)
        .merge(source, "key1 = key2")
        .whenMatched().updateExpr(Map("key1" -> "key2", "value1" -> "value2"))
        .whenNotMatched().insertExpr(Map("key1" -> "key2", "value1" -> "value2"))
        .execute()

      checkAnswer(
        readDeltaTable(tempPath),
        Row(1, 100) ::    // Update
          Row(2, 20) ::     // No change
          Row(3, 30) ::     // Insert
          Nil)
    }
  }

  test("extended scala API") {
    withTable("source") {
      append(Seq((1, 10), (2, 20), (4, 40)).toDF("key1", "value1"), Nil)  // target
      val source = Seq((1, 100), (3, 30), (4, 41)).toDF("key2", "value2")  // source

      io.delta.tables.DeltaTable.forPath(spark, tempPath)
        .merge(source, "key1 = key2")
        .whenMatched("key1 = 4").delete()
        .whenMatched("key2 = 1").updateExpr(Map("key1" -> "key2", "value1" -> "value2"))
        .whenNotMatched("key2 = 3").insertExpr(Map("key1" -> "key2", "value1" -> "value2"))
        .execute()

      checkAnswer(
        readDeltaTable(tempPath),
        Row(1, 100) ::    // Update
          Row(2, 20) ::     // No change
          Row(3, 30) ::     // Insert
          Nil)
    }
  }

  test("extended scala API with Column") {
    withTable("source") {
      append(Seq((1, 10), (2, 20), (4, 40)).toDF("key1", "value1"), Nil)  // target
      val source = Seq((1, 100), (3, 30), (4, 41)).toDF("key2", "value2")  // source

      io.delta.tables.DeltaTable.forPath(spark, tempPath)
        .merge(source, functions.expr("key1 = key2"))
        .whenMatched(functions.expr("key1 = 4")).delete()
        .whenMatched(functions.expr("key2 = 1"))
        .update(Map("key1" -> functions.col("key2"), "value1" -> functions.col("value2")))
        .whenNotMatched(functions.expr("key2 = 3"))
        .insert(Map("key1" -> functions.col("key2"), "value1" -> functions.col("value2")))
        .execute()

      checkAnswer(
        readDeltaTable(tempPath),
        Row(1, 100) ::    // Update
          Row(2, 20) ::     // No change
          Row(3, 30) ::     // Insert
          Nil)
    }
  }

  test("updateAll and insertAll") {
    withTable("source") {
      append(Seq((1, 10), (2, 20), (4, 40), (5, 50)).toDF("key", "value"), Nil)
      val source = Seq((1, 100), (3, 30), (4, 41), (5, 51), (6, 60))
        .toDF("key", "value").createOrReplaceTempView("source")

      executeMerge(
        target = s"delta.`$tempPath` as t",
        source = "source s",
        condition = "s.key = t.key",
        update = "*",
        insert = "*")

      checkAnswer(
        readDeltaTable(tempPath),
        Row(1, 100) ::    // Update
          Row(2, 20) ::     // No change
          Row(3, 30) ::     // Insert
          Row(4, 41) ::     // Update
          Row(5, 51) ::     // Update
          Row(6, 60) ::     // Insert
          Nil)
    }
  }

  test("updateAll and insertAll with columns containing dot") {
    withTable("source") {
      append(Seq((1, 10), (2, 20), (4, 40)).toDF("key", "the.value"), Nil) // target
      val source = Seq((1, 100), (3, 30), (4, 41)).toDF("key", "the.value") // source

      io.delta.tables.DeltaTable.forPath(spark, tempPath).as("t")
        .merge(source.as("s"), "t.key = s.key")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()

      checkAnswer(
        readDeltaTable(tempPath),
        Row(1, 100) :: // Update
          Row(2, 20) :: // No change
          Row(4, 41) :: // Update
          Row(3, 30) :: // Insert
          Nil)
    }
  }

  test("update with empty map should do nothing") {
    append(Seq((1, 10), (2, 20)).toDF("trgKey", "trgValue"), Nil) // target
    val source = Seq((1, 100), (3, 30)).toDF("srcKey", "srcValue") // source
    io.delta.tables.DeltaTable.forPath(spark, tempPath)
      .merge(source, "srcKey = trgKey")
      .whenMatched().updateExpr(Map[String, String]())
      .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
      .execute()

    checkAnswer(
      readDeltaTable(tempPath),
      Row(1, 10) ::       // Not updated since no update clause
      Row(2, 20) ::       // No change due to merge condition
      Row(3, 30) ::       // Not updated since no update clause
      Nil)

    // match condition should not be ignored when map is empty
    io.delta.tables.DeltaTable.forPath(spark, tempPath)
      .merge(source, "srcKey = trgKey")
      .whenMatched("trgKey = 1").updateExpr(Map[String, String]())
      .whenMatched().delete()
      .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
      .execute()

    checkAnswer(
      readDeltaTable(tempPath),
      Row(1, 10) ::     // Neither updated, nor deleted (condition is not ignored)
      Row(2, 20) ::     // No change due to merge condition
      Nil)              // Deleted (3, 30)
  }

  // Checks specific to the APIs that are automatically handled by parser for SQL
  test("check invalid merge API calls") {
    withTable("source") {
      append(Seq((1, 10), (2, 20)).toDF("trgKey", "trgValue"), Nil) // target
      val source = Seq((1, 100), (3, 30)).toDF("srcKey", "srcValue") // source

      // There must be at least one WHEN clause in a MERGE statement
      var e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .execute()
      }
      errorContains(e.getMessage, "There must be at least one WHEN clause in a MERGE statement")

      // When there are multiple MATCHED clauses in a MERGE statement,
      // the first MATCHED clause must have a condition
      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenMatched().delete()
          .whenMatched("trgKey = 1").updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .execute()
      }
      errorContains(e.getMessage, "When there are more than one MATCHED clauses in a MERGE " +
        "statement, only the last MATCHED clause can omit the condition.")

      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenMatched().updateExpr(Map("trgKey" -> "srcKey", "*" -> "*"))
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .execute()
      }
      errorContains(e.getMessage, "cannot resolve `*`")

      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenMatched().updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .whenNotMatched().insertExpr(Map("*" -> "*"))
          .execute()
      }
      errorContains(e.getMessage, "cannot resolve `*`")
    }
  }

  test("merge after schema change") {
    withSQLConf((DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key, "true")) {
      withTempPath { targetDir =>
        val targetPath = targetDir.getCanonicalPath
        spark.range(10).write.format("delta").save(targetPath)
        val t = io.delta.tables.DeltaTable.forPath(spark, targetPath).as("t")
        assert(t.toDF.schema == StructType.fromDDL("id LONG"))

        // Do one merge to change the schema.
        t.merge(Seq((11L, "newVal11")).toDF("id", "newCol1").as("s"), "t.id = s.id")
          .whenMatched().updateAll()
          .whenNotMatched().insertAll()
          .execute()
        // assert(t.toDF.schema == StructType.fromDDL("id LONG, newCol1 STRING"))

        // SC-35564 - ideally this shouldn't throw an error, but right now we can't fix it without
        // causing a regression.
        val ex = intercept[Exception] {
          t.merge(Seq((12L, "newVal12")).toDF("id", "newCol2").as("s"), "t.id = s.id")
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .execute()
        }
        ex.getMessage.contains("schema of your Delta table has changed in an incompatible way")
      }
    }
  }

  test("merge without table alias") {
    withTempDir { dir =>
      val location = dir.getAbsolutePath
      Seq((1, 1, 1), (2, 2, 2)).toDF("part", "id", "n").write
        .format("delta")
        .partitionBy("part")
        .save(location)
      val table = io.delta.tables.DeltaTable.forPath(spark, location)
      val data1 = Seq((2, 2, 4, 2), (9, 3, 6, 9), (3, 3, 9, 3)).toDF("part", "id", "n", "part2")
      table.alias("t").merge(
        data1,
        "t.part = part2")
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
    }
  }

  test("pre-resolved exprs: should work in all expressions in absence of duplicate refs") {
    withTempDir { dir =>
      val location = dir.getAbsolutePath
      Seq((1, 1), (2, 2)).toDF("key", "value").write
        .format("delta")
        .save(location)
      val table = io.delta.tables.DeltaTable.forPath(spark, location)
      val target = table.toDF
      val source = Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value")

      table.merge(source, target("key") === source("key"))
        .whenMatched(target("key") === lit(1) && source("value") === lit(10))
        .update(Map("value" -> (target("value") + source("value"))))
        .whenMatched(target("key") === lit(2) && source("value") === lit(20))
        .delete()
        .whenNotMatched(source("key") === lit(3) && source("value") === lit(30))
        .insert(Map("key" -> source("key"), "value" -> source("value")))
        .execute()

      checkAnswer(table.toDF, Seq((1, 11), (3, 30)).toDF("key", "value"))
    }
  }

  test("pre-resolved exprs: negative cases with refs resolved to wrong Dataframes") {
    withTempDir { dir =>
      val location = dir.getAbsolutePath
      Seq((1, 1), (2, 2)).toDF("key", "value").write
        .format("delta")
        .save(location)
      val table = io.delta.tables.DeltaTable.forPath(spark, location)
      val target = table.toDF
      val source = Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value")
      val dummyDF = Seq((0, 0)).toDF("key", "value")

      def checkError(f: => Unit): Unit = {
        val e = intercept[AnalysisException] { f }
        Seq("Resolved attribute", "missing from").foreach { m =>
          assert(e.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
        }
      }
      // Merge condition
      checkError {
        table.merge(source, target("key") === dummyDF("key"))
          .whenMatched().delete().execute()
      }

      // Matched clauses
      checkError {
        table.merge(source, target("key") === source("key"))
          .whenMatched(dummyDF("key") === lit(1)).updateAll().execute()
      }

      checkError {
        table.merge(source, target("key") === source("key"))
          .whenMatched().update(Map("key" -> dummyDF("key"))).execute()
      }

      // Not matched clauses
      checkError {
        table.merge(source, target("key") === source("key"))
          .whenNotMatched(dummyDF("key") === lit(1)).insertAll().execute()
      }
      checkError {
        table.merge(source, target("key") === source("key"))
          .whenNotMatched().insert(Map("key" -> dummyDF("key"))).execute()
      }
    }
  }

  /** Make sure the joins generated by merge do not have the duplicate AttributeReferences */
  private def verifyNoDuplicateRefsAcrossSourceAndTarget(f: => Unit): Unit = {
    val executedPlans = DeltaTestUtils.withLogicalPlansCaptured(spark, optimizedPlan = true) { f }
    val plansWithInnerJoin = executedPlans.filter { p =>
      p.collect { case b: Join if b.joinType == Inner => b }.nonEmpty
    }
    assert(plansWithInnerJoin.size == 1,
      "multiple plans found with inner join\n" + plansWithInnerJoin.mkString("\n"))
    val join = plansWithInnerJoin.head.collect { case j: Join => j }.head
    assert(join.left.outputSet.intersect(join.right.outputSet).isEmpty)
  }

  test("self-merge: duplicate attrib refs should be removed") {
    withTempDir { tempDir =>
      val df = spark.range(5).selectExpr("id as key", "id as value")
      df.write.format("delta").save(tempDir.toString)

      val deltaTable = io.delta.tables.DeltaTable.forPath(tempDir.toString)
      val target = deltaTable.toDF
      val source = target.filter("key = 4")

      val duplicateRefs =
        target.queryExecution.analyzed.outputSet.intersect(source.queryExecution.analyzed.outputSet)
      require(duplicateRefs.nonEmpty, "source and target were expected to have duplicate refs")

      verifyNoDuplicateRefsAcrossSourceAndTarget {
        deltaTable.as("t")
          .merge(source.as("s"), "t.key = s.key")
          .whenMatched()
          .delete()
          .execute()
      }
      checkAnswer(deltaTable.toDF, spark.range(4).selectExpr("id as key", "id as value"))
    }
  }

  test(
    "self-merge + pre-resolved exprs: merge condition fails with pre-resolved, duplicate refs") {
    withTempDir { tempDir =>
      val df = spark.range(5).selectExpr("id as key", "id as value")
      df.write.format("delta").save(tempDir.toString)

      val deltaTable = io.delta.tables.DeltaTable.forPath(tempDir.toString)
      val target = deltaTable.toDF
      val source = target.filter("key = 4")
      val e = intercept[AnalysisException] {
        deltaTable.merge(source, target("key") === source("key"))  // this is ambiguous
          .whenMatched()
          .delete()
          .execute()
      }
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains("ambiguous"))
    }
  }

  test(
    "self-merge + pre-resolved exprs: duplicate refs should resolve in not-matched clauses") {
    withTempDir { tempDir =>
      val df = spark.range(5).selectExpr("id as key", "id as value")
      df.write.format("delta").save(tempDir.toString)

      val deltaTable = io.delta.tables.DeltaTable.forPath(tempDir.toString)
      val target = deltaTable.toDF
      val source = target.filter("key = 4")

      // Insert clause can refer to only source attributes, so pre-resolved references,
      // even when written as`target("column")`, are actually unambiguous
      verifyNoDuplicateRefsAcrossSourceAndTarget {
        deltaTable.as("t")
          .merge(source.as("s"), "t.key = s.key")
          .whenNotMatched(source("value") > 0 && target("key") > 0)
          .insert(Map("key" -> source("key"), "value" -> target("value")))
          .whenMatched().update(Map("key" -> $"s.key")) // no-op
          .execute()
      }
      // nothing should be inserted as source matches completely with target
      checkAnswer(deltaTable.toDF, spark.range(5).selectExpr("id as key", "id as value"))
    }
  }

  test(
    "self-merge + pre-resolved exprs: non-duplicate but pre-resolved refs should still resolve") {
    withTempDir { tempDir =>
      val df = spark.range(5).selectExpr("id as key", "id as value")
      df.write.format("delta").save(tempDir.toString)

      val deltaTable = io.delta.tables.DeltaTable.forPath(tempDir.toString)
      val target = deltaTable.toDF
      val source = target.filter("key = 0").drop("value")
        .withColumn("value", col("key") + lit(0))
        .withColumn("other", lit(0))
      // source is just one row (key, value, other) = (4, 4, 0)

      // `value` should not be duplicate ref as its recreated in the source and have different
      // exprIds than the target value.
      val duplicateRefs =
        target.queryExecution.analyzed.outputSet.intersect(source.queryExecution.analyzed.outputSet)
      require(duplicateRefs.map(_.name).toSet == Set("key"),
        "unexpected duplicate refs, should be only 'key': " + duplicateRefs)

      // So both `source("value")` and `target("value")` are not ambiguous.
      // `source("other")` is obviously not ambiguous.
      verifyNoDuplicateRefsAcrossSourceAndTarget {
        deltaTable.as("t")
          .merge(
            source.as("s"),
            expr("t.key = s.key") && source("other") === 0 && target("value") === 4)
          .whenMatched(source("value") > 0 && target("value") > 0 && source("other") === 0)
          .update(Map(
            "key" -> expr("s.key"),
            "value" -> (target("value") + source("value") + source("other"))))
          .whenNotMatched(source("value") > 0 && source("other") === 0)
          .insert(Map(
            "key" -> expr("s.key"),
            "value" -> (source("value") + source("other"))))
          .execute()
      }
      // key = 4 should be updated to same values, and nothing should be inserted
      checkAnswer(deltaTable.toDF, spark.range(5).selectExpr("id as key", "id as value"))
    }
  }

  test("self-merge + pre-resolved exprs: negative cases in matched clauses with duplicate refs") {
    // Only matched clauses can have attribute references from both source and target, hence
    // pre-resolved expression can be ambiguous in presence of duplicate references from self-merge
    withTempDir { tempDir =>
      val df = spark.range(5).selectExpr("id as key", "id as value")
      df.write.format("delta").save(tempDir.toString)

      val deltaTable = io.delta.tables.DeltaTable.forPath(tempDir.toString)
      val target = deltaTable.toDF
      val source = target.filter("key = 4")

      def checkError(f: => Unit): Unit = {
        val e = intercept[AnalysisException] { f }
        assert(e.getMessage.toLowerCase(Locale.ROOT).contains("ambiguous"))
      }

      checkError {
        deltaTable
          .merge(source, target("key") === source("key"))  // this is ambiguous
          .whenMatched()
          .delete()
          .execute()
      }

      // Update
      checkError {
        deltaTable.as("t").merge(source.as("s"), "t.key = s.key")
          .whenMatched(target("key") === functions.lit(4))  // can map to either key column
          .updateAll()
          .execute()
      }

      checkError {
        deltaTable.as("t").merge(source.as("s"), "t.key = s.key")
          .whenMatched()
          .update(Map("value" -> target("value").plus(1)))  // can map to either value column
          .execute()
      }

      // Delete
      checkError {
        deltaTable.as("t").merge(source.as("s"), "t.key = s.key")
          .whenMatched(target("key") === functions.lit(4))  // can map to either key column
          .delete()
          .execute()
      }
    }
  }

  test("merge clause matched and not matched can interleave") {
    append(Seq((1, 10), (2, 20)).toDF("trgKey", "trgValue"), Nil) // target
    val source = Seq((1, 100), (2, 200), (3, 300), (4, 400)).toDF("srcKey", "srcValue") // source
    io.delta.tables.DeltaTable.forPath(spark, tempPath)
      .merge(source, "srcKey = trgKey")
      .whenMatched("trgKey = 1").updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
      .whenNotMatched("srcKey = 3").insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
      .whenMatched().delete()
      .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
      .execute()

    checkAnswer(
      readDeltaTable(tempPath),
      Row(1, 100) ::  // Update (1, 10)
                      // Delete (2, 20)
      Row(3, 300) ::  // Insert (3, 300)
      Row(4, 400) ::  // Insert (4, 400)
      Nil)
  }

  test("schema evolution with multiple update clauses") {
    withSQLConf(("spark.databricks.delta.schema.autoMerge.enabled", "true")) {
      withTable("target", "src") {
        Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "targetValue")
          .write.format("delta").saveAsTable("target")
        val source = Seq((1, "x"), (2, "y"), (4, "z")).toDF("id", "srcValue")

        io.delta.tables.DeltaTable.forName("target")
          .merge(source, col("target.id") === source.col("id"))
          .whenMatched("target.id = 1").updateExpr(Map("targetValue" -> "srcValue"))
          .whenMatched("target.id = 2").updateAll()
          .whenNotMatched().insertAll()
          .execute()
        checkAnswer(
          sql("select * from target"),
          Row(1, "x", null) +: Row(2, "b", "y") +: Row(3, "c", null) +: Row(4, null, "z") +: Nil)
      }
    }
  }

  override protected def executeMerge(
      target: String,
      source: String,
      condition: String,
      update: String,
      insert: String): Unit = {

    executeMerge(
      tgt = target,
      src = source,
      cond = condition,
      MergeClause(isMatched = true, condition = null, action = s"UPDATE SET $update"),
      MergeClause(isMatched = false, condition = null, action = s"INSERT $insert"))
  }

  override protected def executeMerge(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): Unit = {

    def parseTableAndAlias(tableNameWithAlias: String): (String, Option[String]) = {
      tableNameWithAlias.split(" ").toList match {
        case tableName :: Nil =>
          // 'MERGE INTO tableName' OR `MERGE INTO delta.`path`'
          tableName -> None
        case tableName :: alias :: Nil =>
          // 'MERGE INTO tableName alias' or 'MERGE INTO delta.`path` alias'
          tableName -> Some(alias)
        case list if list.size >= 3 && list(list.size - 2).toLowerCase(Locale.ROOT) == "as" =>
          // 'MERGE INTO ... AS alias'
          list.dropRight(2).mkString(" ").trim() -> Some(list.last)
        case list if list.size >= 2 =>
          // 'MERGE INTO ... alias'
          list.dropRight(1).mkString(" ").trim() -> Some(list.last)
        case _ =>
          fail(s"Could not build parse '$tableNameWithAlias' for table and optional alias")
      }
    }

    def buildClause(
      clause: MergeClause,
      mergeBuilder: DeltaMergeBuilder): DeltaMergeBuilder = {

      if (clause.isMatched) {
        val actionBuilder: DeltaMergeMatchedActionBuilder =
          if (clause.condition != null) mergeBuilder.whenMatched(clause.condition)
          else mergeBuilder.whenMatched()
        if (clause.action.startsWith("DELETE")) {   // DELETE clause
          actionBuilder.delete()
        } else {                                    // UPDATE clause
          val setColExprStr = clause.action.trim.stripPrefix("UPDATE SET")
          if (setColExprStr.trim == "*") {          // UPDATE SET *
            actionBuilder.updateAll()
          } else {                                  // UPDATE SET x = a, y = b, z = c
            val setColExprPairs = parseUpdate(setColExprStr.split(","))
            actionBuilder.updateExpr(setColExprPairs)
          }
        }
      } else {                                        // INSERT clause
        val actionBuilder: DeltaMergeNotMatchedActionBuilder =
          if (clause.condition != null) mergeBuilder.whenNotMatched(clause.condition)
          else mergeBuilder.whenNotMatched()
        val valueStr = clause.action.trim.stripPrefix("INSERT")
        if (valueStr.trim == "*") {                   // INSERT *
          actionBuilder.insertAll()
        } else {                                      // INSERT (x, y, z) VALUES (a, b, c)
          val valueColExprsPairs = parseInsert(valueStr, Some(clause))
          actionBuilder.insertExpr(valueColExprsPairs)
        }
      }
    }

    val deltaTable = {
      val (tableNameOrPath, optionalAlias) = parseTableAndAlias(tgt)
      var table = makeDeltaTable(tableNameOrPath)
      optionalAlias.foreach { alias => table = table.as(alias) }
      table
    }

    val sourceDataFrame: DataFrame = {
      val (tableOrQuery, optionalAlias) = parseTableAndAlias(src)
      var df =
        if (tableOrQuery.startsWith("(")) spark.sql(tableOrQuery) else spark.table(tableOrQuery)
      optionalAlias.foreach { alias => df = df.as(alias) }
      df
    }

    var mergeBuilder = deltaTable.merge(sourceDataFrame, cond)
    clauses.foreach { clause =>
      mergeBuilder = buildClause(clause, mergeBuilder)
    }
    mergeBuilder.execute()
    deltaTable.toDF
  }

  // scalastyle:off argcount
  override def testNestedDataSupport(name: String, namePrefix: String = "nested data support")(
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
          val pathOrName = parsePath(targetName)
          val fieldNames = readDeltaTable(pathOrName).schema.fieldNames
          val keyName = s"`${fieldNames.head}`"

          def execMerge() = {
            val t = makeDeltaTable(targetName)
            val m = t.as("t")
              .merge(
                spark.table(sourceName).as("s"),
                s"s.$keyName = t.$keyName")
            val withUpdate = if (update == Seq("*")) {
              m.whenMatched().updateAll()
            } else {
              val updateColExprMap = parseUpdate(update)
              m.whenMatched().updateExpr(updateColExprMap)
            }

            if (insert == "*") {
              withUpdate.whenNotMatched().insertAll().execute()
            } else {
              val insertExprMaps = if (insert != null) {
                parseInsert(insert, None)
              } else {
                fieldNames.map { f => s"t.`$f`" -> s"s.`$f`" }.toMap
              }

              withUpdate.whenNotMatched().insertExpr(insertExprMaps).execute()
            }
          }

          if (result != null) {
            execMerge()
            checkAnswer(readDeltaTable(pathOrName), spark.read.json(strToJsonSeq(result).toDS))
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

  private def makeDeltaTable(nameOrPath: String): DeltaTable = {
    val isPath: Boolean = nameOrPath.startsWith("delta.")
    if (isPath) {
      val path = nameOrPath.stripPrefix("delta.`").stripSuffix("`")
      io.delta.tables.DeltaTable.forPath(spark, path)
    } else {
      DeltaTableTestUtils.createTable(spark.table(nameOrPath), DeltaLog.forTable(spark, nameOrPath))
    }
  }

  private def parseUpdate(update: Seq[String]): Map[String, String] = {
    update.map { _.split("=").toList }.map {
      case setCol :: setExpr :: Nil => setCol.trim -> setExpr.trim
      case _ => fail("error parsing update actions " + update)
    }.toMap
  }

  private def parseInsert(valueStr: String, clause: Option[MergeClause]): Map[String, String] = {
    valueStr.split("VALUES").toList match {
      case colsStr :: exprsStr :: Nil =>
        def parse(str: String): Seq[String] = {
          str.trim.stripPrefix("(").stripSuffix(")").split(",").map(_.trim)
        }
        val cols = parse(colsStr)
        val exprs = parse(exprsStr)
        require(cols.size == exprs.size,
          s"Invalid insert action ${clause.get.action}: cols = $cols, exprs = $exprs")
        cols.zip(exprs).toMap

      case list =>
        fail(s"Invalid insert action ${clause.get.action} split into $list")
    }
  }

  private def parsePath(nameOrPath: String): String = {
    if (nameOrPath.startsWith("delta.`")) {
      nameOrPath.stripPrefix("delta.`").stripSuffix("`")
    } else nameOrPath
  }
}
