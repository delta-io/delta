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
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
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

  protected def checkCurrentTimestampMatchingKey(
      pinStatementTime: Boolean,
      materializeSource: String,
      loseSource: Boolean)(executeInsertReplace: String => Unit): Unit = {
    import testImplicits._

    withSQLConf(
        DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_PIN_STATEMENT_TIME_ENABLED.key ->
          pinStatementTime.toString,
        DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_MATERIALIZE_SOURCE.key -> materializeSource) {
      withTable("target") {
        createTable("target", Seq("id bigint", "eventTime timestamp", "rowOrigin string"))
        var firstSourceMicros: Option[Long] = None
        var injectedFailures = 0

        def materializedSource: Option[RDD[InternalRow]] = {
          spark.sparkContext.getPersistentRDDs.values
            .find(_.name == "mergeMaterializedSource")
            .map(_.asInstanceOf[RDD[InternalRow]])
        }

        val seedTargetBeforeSnapshot = new TransactionExecutionObserver {
          override def startingTransaction(f: => OptimisticTransaction): OptimisticTransaction = {
            if (firstSourceMicros.isEmpty) {
              materializedSource.foreach { sourceRDD =>
                val sourceTimes = sourceRDD.map(_.getLong(1)).distinct().collect()
                assert(sourceTimes.length === 1)
                val micros = sourceTimes.head
                firstSourceMicros = Some(micros)
                // Seed exact matching keys before the replacement reads its target snapshot.
                TransactionExecutionObserver.withObserver(NoOpTransactionExecutionObserver) {
                  insertValues("target", Seq(
                    s"(1, timestamp_micros($micros), 'target-match')",
                    s"(1, timestamp_micros($micros), 'target-match-duplicate')",
                    s"(2, timestamp_micros($micros), 'target-second-key')",
                    "(1, timestamp_micros(0), 'target-different-time')",
                    s"(4, timestamp_micros($micros), 'target-unmatched-id')"))
                }
              }
            }
            f
          }
          override def createChild(): TransactionExecutionObserver = this
          override def preparingCommit[T](f: => T): T = f
          override def beginDoCommit(): Unit = ()
          override def beginBackfill(): Unit = ()
          override def beginPostCommit(): Unit = ()
          override def transactionCommitted(): Unit = ()
          override def transactionAborted(): Unit = ()
          override def advanceToNextThreadObserver(): Unit = ()
        }
        val failFirstAttempt = new InsertAtomicReplaceExecutionObserver {
          private def beforePhase(): Unit = synchronized {
            if (loseSource && injectedFailures == 0) {
              assert(firstSourceMicros.nonEmpty, "target keys were not seeded")
              val checkpoint = materializedSource.getOrElse(
                fail("the range source should materialize"))
              injectedFailures += 1
              checkpoint.unpersist(blocking = true)
            }
          }
          override def delete[T](f: => T): T = {
            beforePhase()
            f
          }
          override def insert[T](f: => T): T = {
            beforePhase()
            f
          }
          override def commit[T](f: => T): T = f
        }
        val sourceQuery =
          """SELECT id % 3 + 1 AS id, current_timestamp() AS eventTime,
            |  concat('source-', id) AS rowOrigin
            |FROM range(0, 4)
            |""".stripMargin
        TransactionExecutionObserver.withObserver(seedTargetBeforeSnapshot) {
          InsertAtomicReplaceExecutionObserver.withObserver(failFirstAttempt) {
            executeInsertReplace(sourceQuery)
          }
        }
        assert(injectedFailures === (if (loseSource) 1 else 0))
        val originalMicros = firstSourceMicros.getOrElse(fail("target keys were not seeded"))
        val finalSourceTimes = sql(
          "SELECT DISTINCT unix_micros(eventTime) FROM target WHERE rowOrigin LIKE 'source-%'")
          .as[Long].collect()
        assert(finalSourceTimes.length === 1)
        val finalMicros = finalSourceTimes.head
        val expectMatchingKeys = pinStatementTime || !loseSource
        if (expectMatchingKeys) {
          assert(finalMicros === originalMicros)
        } else {
          assert(finalMicros > originalMicros)
        }
        val sourceRows = Seq(
          Row(1L, finalMicros, "source-0"),
          Row(2L, finalMicros, "source-1"),
          Row(3L, finalMicros, "source-2"),
          Row(1L, finalMicros, "source-3"))
        val untouchedRows = Seq(
          Row(1L, 0L, "target-different-time"),
          Row(4L, originalMicros, "target-unmatched-id"))
        val legacyRows = if (expectMatchingKeys) Nil else Seq(
          Row(1L, originalMicros, "target-match"),
          Row(1L, originalMicros, "target-match-duplicate"),
          Row(2L, originalMicros, "target-second-key"))
        checkAnswer(sql("SELECT id, unix_micros(eventTime), rowOrigin FROM target"),
          sourceRows ++ untouchedRows ++ legacyRows)
      }
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
