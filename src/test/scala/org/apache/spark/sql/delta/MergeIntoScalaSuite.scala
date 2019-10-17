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

import java.util.Locale

import io.delta.tables._

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

class MergeIntoScalaSuite extends MergeIntoSuiteBase {

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

  test("insert with empty map throws error") {
    append(Seq((1, 10), (2, 20)).toDF("trgKey", "trgValue"), Nil) // target
    val source = Seq((1, 100), (3, 30)).toDF("srcKey", "srcValue") // source
    val e = intercept[AnalysisException] {
      io.delta.tables.DeltaTable.forPath(spark, tempPath)
        .merge(source, "srcKey = trgKey")
        .whenMatched().updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
        .whenNotMatched().insertExpr(Map[String, String]())
        .execute()
    }
    errorContains(e.getMessage, "INSERT clause must specify value for all the columns")
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
      errorContains(e.getMessage, "There must be at least one WHEN clause in a MERGE query")

      // When there are 2 MATCHED clauses in a MERGE statement,
      // the first MATCHED clause must have a condition
      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenMatched().delete()
          .whenMatched("trgKey = 1").updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .execute()
      }
      errorContains(e.getMessage, "the first MATCHED clause must have a condition")

      // There must be at most two WHEN clauses in a MERGE statement
      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenMatched("trgKey = 1").updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .whenMatched("trgValue = 3").delete()
          .whenMatched("trgValue = 2")
          .updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue + 1"))
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .execute()
      }
      errorContains(e.getMessage, "There must be at most two match clauses in a MERGE query")

      // INSERT can appear at most once in NOT MATCHED clauses in a MERGE statement
      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey + 1", "trgValue" -> "srcValue"))
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .execute()
      }
      errorContains(e.getMessage,
        "INSERT, UPDATE and DELETE cannot appear twice in one MERGE query")

      // UPDATE can appear at most once in MATCHED clauses in a MERGE statement
      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenMatched("trgKey = 1").updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .whenMatched("trgValue = 2")
          .updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue + 1"))
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .execute()
      }
      errorContains(e.getMessage,
        "INSERT, UPDATE and DELETE cannot appear twice in one MERGE query")

      // DELETE can appear at most once in MATCHED clauses in a MERGE statement
      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenMatched("trgKey = 1").delete()
          .whenMatched("trgValue = 2").delete()
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .execute()
      }
      errorContains(e.getMessage,
        "INSERT, UPDATE and DELETE cannot appear twice in one MERGE query")

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

    def parse(tableNameWithAlias: String): (String, Option[String]) = {
      tableNameWithAlias.split(" ").toList match {
        case tableName :: Nil => tableName -> None
        case tableName :: alias :: Nil => tableName -> Some(alias)
        case list if list.size >= 3 && list(list.size - 2).toLowerCase(Locale.ROOT) == "as" =>
          list.dropRight(2).mkString(" ").trim() -> Some(list.last)
        case list if list.size >= 2 =>
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
      val (tableNameOrPath, optionalAlias) = parse(tgt)
      var table = makeDeltaTable(tableNameOrPath)
      optionalAlias.foreach { alias => table = table.as(alias) }
      table
    }

    val sourceDataFrame: DataFrame = {
      val (tableOrQuery, optionalAlias) = parse(src)
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

  override def testNestedDataSupport(name: String, namePrefix: String = "nested data support")(
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
        val pathOrName = parsePath(targetName)
        val fieldNames = readDeltaTable(pathOrName).schema.fieldNames
        val keyName = s"`${fieldNames.head}`"
        val updateColExprMap = parseUpdate(update)
        val insertExprMaps = if (insert != null) {
          parseInsert(insert, None)
        } else {
          fieldNames.map { f => s"t.`$f`" -> s"s.`$f`" }.toMap
        }

        def execMerge() = {
          val t = makeDeltaTable(targetName)
          t.as("t")
            .merge(
              spark.table(sourceName).as("s"),
              s"s.$keyName = t.$keyName")
            .whenMatched().updateExpr(updateColExprMap)
            .whenNotMatched().insertExpr(insertExprMaps)
            .execute()
        }

        if (result != null) {
          execMerge()
          checkAnswer(readDeltaTable(pathOrName), spark.read.json(strToJsonSeq(result).toDS))
        } else {
          val e = intercept[AnalysisException] { execMerge() }
          errorStrs.foreach { s => errorContains(e.getMessage, s) }
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
