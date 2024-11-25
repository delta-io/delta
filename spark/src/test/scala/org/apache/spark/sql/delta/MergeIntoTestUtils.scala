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

import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import io.delta.tables._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Base trait collecting helper methods to run MERGE tests. Merge test suite will want to mix in
 * either [[MergeIntoSQLTestUtils]] or [[MergeIntoScalaTestUtils]] to run merge tests using the SQL
 * or Scala API resp.
 */
trait MergeIntoTestUtils extends DeltaDMLTestUtils with MergeHelpers {
  self: SharedSparkSession =>

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

  protected def executeMergeWithSchemaEvolution(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): Unit

  protected def withCrossJoinEnabled(body: => Unit): Unit = {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") { body }
  }
}

trait MergeIntoSQLTestUtils extends DeltaSQLTestUtils with MergeIntoTestUtils {
  self: SharedSparkSession =>

  protected def basicMergeStmt(
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
      insert: String): Unit =
    sql(basicMergeStmt(target, source, condition, update, insert))

  override protected def executeMerge(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): Unit = {
    val clausesStr = clauses.map(_.sql).mkString("\n")
    sql(s"MERGE INTO $tgt USING $src ON $cond\n" + clausesStr)
  }

  override protected def executeMergeWithSchemaEvolution(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): Unit = {
    throw new UnsupportedOperationException(
      "The SQL syntax [WITH SCHEMA EVOLUTION] is not yet supported.")
  }
}

trait MergeIntoScalaTestUtils extends MergeIntoTestUtils {
  self: SharedSparkSession =>

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
      this.update(set = update),
      this.insert(values = insert))
  }

  override protected def executeMerge(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): Unit =
    getMergeBuilder(tgt, src, cond, clauses: _*).execute()

  override protected def executeMergeWithSchemaEvolution(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): Unit =
    getMergeBuilder(tgt, src, cond, clauses: _*).withSchemaEvolution().execute()

  private def getMergeBuilder(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): DeltaMergeBuilder = {
    def buildClause(clause: MergeClause, mergeBuilder: DeltaMergeBuilder)
      : DeltaMergeBuilder = clause match {
      case _: MatchedClause =>
        val actionBuilder: DeltaMergeMatchedActionBuilder =
          if (clause.condition != null) mergeBuilder.whenMatched(clause.condition)
          else mergeBuilder.whenMatched()
        if (clause.action.startsWith("DELETE")) {   // DELETE clause
          actionBuilder.delete()
        } else {                                    // UPDATE clause
          val setColExprStr = clause.action.trim.stripPrefix("UPDATE SET")
          if (setColExprStr.trim == "*") {          // UPDATE SET *
            actionBuilder.updateAll()
          } else if (setColExprStr.contains("array_")) { // UPDATE SET x = array_union(..)
            val setColExprPairs = parseUpdate(Seq(setColExprStr))
            actionBuilder.updateExpr(setColExprPairs)
          } else {                                 // UPDATE SET x = a, y = b, z = c
            val setColExprPairs = parseUpdate(setColExprStr.split(","))
            actionBuilder.updateExpr(setColExprPairs)
          }
        }
      case _: NotMatchedClause =>                     // INSERT clause
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
      case _: NotMatchedBySourceClause =>
        val actionBuilder: DeltaMergeNotMatchedBySourceActionBuilder =
          if (clause.condition != null) mergeBuilder.whenNotMatchedBySource(clause.condition)
          else mergeBuilder.whenNotMatchedBySource()
        if (clause.action.startsWith("DELETE")) { // DELETE clause
          actionBuilder.delete()
        } else { // UPDATE clause
          val setColExprStr = clause.action.trim.stripPrefix("UPDATE SET")
          if (setColExprStr.contains("array_")) { // UPDATE SET x = array_union(..)
            val setColExprPairs = parseUpdate(Seq(setColExprStr))
            actionBuilder.updateExpr(setColExprPairs)
          } else { // UPDATE SET x = a, y = b, z = c
            val setColExprPairs = parseUpdate(setColExprStr.split(","))
            actionBuilder.updateExpr(setColExprPairs)
          }
        }
      }

    val deltaTable = DeltaTestUtils.getDeltaTableForIdentifierOrPath(
      spark,
      DeltaTestUtils.getTableIdentifierOrPath(tgt))

    val sourceDataFrame: DataFrame = {
      val (tableOrQuery, optionalAlias) = DeltaTestUtils.parseTableAndAlias(src)
      var df =
        if (tableOrQuery.startsWith("(")) spark.sql(tableOrQuery) else spark.table(tableOrQuery)
      optionalAlias.foreach { alias => df = df.as(alias) }
      df
    }

    var mergeBuilder = deltaTable.merge(sourceDataFrame, cond)
    clauses.foreach { clause =>
      mergeBuilder = buildClause(clause, mergeBuilder)
    }
    mergeBuilder
  }

  protected def parseUpdate(update: Seq[String]): Map[String, String] = {
    update.map { _.split("=").toList }.map {
      case setCol :: setExpr :: Nil => setCol.trim -> setExpr.trim
      case _ => fail("error parsing update actions " + update)
    }.toMap
  }

  protected def parseInsert(valueStr: String, clause: Option[MergeClause]): Map[String, String] = {
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

  protected def parsePath(nameOrPath: String): String = {
    if (nameOrPath.startsWith("delta.`")) {
      nameOrPath.stripPrefix("delta.`").stripSuffix("`")
    } else nameOrPath
  }
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
