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

import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.delta.commands.InsertReplaceOnOrUsingStats
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

trait DeltaInsertReplaceOnOrUsingTestUtils
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  protected val currentTimeGridNumRows = 200000L

  // Separate the unpinned DELETE and INSERT clocks so the moving cutoff reproduces the bug.
  protected class DelayingDeleteObserver extends InsertAtomicReplaceExecutionObserver {
    override def delete[T](f: => T): T = {
      Thread.sleep(500)
      f
    }
    override def insert[T](f: => T): T = f
    override def commit[T](f: => T): T = f
  }

  /**
   * DELETE removes target ids selected at cutoff D; INSERT adds source ids selected at cutoff I.
   * Examples use six ids from the 200,000-row grid, starting at 12:00:00.000:
   *
   *   id:         0    1    2    3    4    5
   *   eventTime: .000 .001 .002 .003 .004 .005
   *
   * Both tables start with the same ids and eventTime values. Each tuple is (id, eventTime,
   * rowOrigin); only rowOrigin differs between the initial target and source rows:
   *
   *   target: (0, .000, 'target'), (1, .001, 'target'), ..., (5, .005, 'target')
   *   source: (0, .000, 'source'), (1, .001, 'source'), ..., (5, .005, 'source')
   *
   * Filtered source:
   *   INSERT INTO target REPLACE USING (id)
   *   SELECT * FROM source WHERE eventTime <= current_timestamp()
   *
   * This replaces matching rows in target; source stays unchanged. For these six ids:
   * - D = 12:00:00.004 deletes target ids 0..4.
   * - I = 12:00:00.002 inserts source ids 0..2.
   * Target now has source rows for ids 0, 1, 2 and its original row for id 5; ids 3 and 4 are
   * missing. Pinning D = I preserves every id once.
   */
  protected def checkCurrentTimeResult(pinStatementTime: Boolean): Unit = {
    val targetIds = spark.table("target").select("id")
    if (pinStatementTime) {
      checkAnswer(targetIds, spark.table("source").select("id"))
    } else {
      val counts = targetIds.selectExpr(
        "count(*) AS rowCount", "count(DISTINCT id) AS distinctIds").head()
      assert(counts.getAs[Long]("rowCount") != currentTimeGridNumRows ||
        counts.getAs[Long]("distinctIds") != currentTimeGridNumRows,
        s"expected missing or duplicate ids: $counts")
    }
  }

  protected def createTable(
     tableName: String,
     tableCols: Seq[String],
     partCols: Seq[String] = Seq.empty,
     clusterCols: Seq[String] = Seq.empty): Unit = {
    assert(partCols.isEmpty || clusterCols.isEmpty,
      "Cannot specify both partitioning and clustering for a table.")
    sql(
      s"""
         |CREATE TABLE $tableName ${tableCols.mkString("(", ",", ")")}
         |USING delta
         |${if (partCols.nonEmpty) s"PARTITIONED BY ${partCols.mkString("(", ",", ")")}" else ""}
         |${if (clusterCols.nonEmpty) s"CLUSTER BY ${clusterCols.mkString("(", ",", ")")}" else ""}
         |""".stripMargin)
  }

  protected def insertValues(tableName: String, rows: Seq[String]): Unit = {
    sql(
      s"""
         |INSERT INTO $tableName
         |VALUES ${rows.mkString(", ")}
         |""".stripMargin)
  }

  protected def executeInsertReplaceOn(
      tableName: String,
      matchingCond: String,
      sourceQuery: String,
      byName: Boolean = false): Unit = {
    val byNameClause = if (byName) "BY NAME" else ""
    sql(
      s"""
         |INSERT INTO $tableName $byNameClause
         |REPLACE ON $matchingCond
         |$sourceQuery
         |""".stripMargin)
  }

  protected def getLastCommitNumAddedAndRemovedBytes(deltaLog: DeltaLog): (Long, Long) = {
    val changes = deltaLog.getChanges(deltaLog.update().version).flatMap(_._2).toSeq
    val addedBytes = changes.collect { case a: AddFile => a.size }.sum
    val removedBytes = changes.collect { case r: RemoveFile => r.getFileSize }.sum

    (addedBytes, removedBytes)
  }

  object DataLayoutType extends Enumeration {
    type DataLayoutType = Value
    val PARTITIONED, CLUSTERED, NORMAL = Value
  }
}
