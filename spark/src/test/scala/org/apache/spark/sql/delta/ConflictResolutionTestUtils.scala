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

import java.util.concurrent.{ExecutionException, ThreadPoolExecutor}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration._

import org.apache.spark.sql.delta.concurrency.{PhaseLockingTestMixin, TransactionExecutionTestMixin}
import org.apache.spark.sql.delta.fuzzer.{PhaseLockingTransactionExecutionObserver => TransactionObserver}
import org.apache.spark.sql.delta.rowid.RowIdTestUtils
import io.delta.tables.{DeltaTable => IODeltaTable}

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.{ThreadUtils, Utils}

trait ConflictResolutionTestUtils
    extends QueryTest
    with SharedSparkSession
    with PhaseLockingTestMixin
    with TransactionExecutionTestMixin
    with DeletionVectorsTestUtils
    with RowIdTestUtils {

  import testImplicits._

  final val ID_COLUMN = "idCol"
  final val PARTITION_COLUMN = "partitionCol"

  override val timeout: FiniteDuration = 120.seconds

  def abbreviate(str: String, abbrevMarker: String, len: Int): String = {
    if (str == null || abbrevMarker == null) {
      null
    } else if (str.length() <= len || str.length() <= abbrevMarker.length()) {
      str
    } else {
      str.substring(0, len - abbrevMarker.length()) + abbrevMarker
    }
  }

  abstract class TestTransaction(sqlConf: Map[String, String] = Map.empty) {
    val name: String
    val sqlConfStr: String = sqlConf.map { case (k, v) => s"$k=$v" }.mkString(",")

    def toSQL(tableName: String): String

    def execute(ctx: TestContext): Unit = {
      ctx.trackTransaction(this) {
        withSQLConf(sqlConf.toSeq: _*) {
          executeImpl(ctx)
        }
      }
    }

    def executeImpl(ctx: TestContext): Unit = {
      val sqlStr = toSQL(s"delta.`${ctx.deltaLog.dataPath.toUri.getPath}`")
      spark.sql(sqlStr).collect()
    }

    /** Whether this transaction is committing data change actions. */
    def dataChange: Boolean

    /** Whether writing Deletion Vectors is enabled for this transaction. */
    def deletionVectorsEnabled(deltaLog: DeltaLog): Boolean = false

    /** The executor thread to run this transaction. */
    private lazy val executor: ThreadPoolExecutor =
      ThreadUtils.newDaemonSingleThreadExecutor(threadName = s"executor-$name")

    /** The transaction observer to step through the transaction phases. */
    var observer: Option[TransactionObserver] = None

    /** The asynchronous future for the result of the transaction. */
    private var future: Option[Future[Array[Row]]] = None

    /** Start transaction and unblock until precommit. */
    def start(ctx: TestContext): Unit = {
      withSQLConf(sqlConf.toSeq: _*) {
        val (observer_, future_) = runFunctionWithObserver(name, executor,
          fn = () => {
            executeImpl(ctx)
            // DV tests do not use the results. We just return an empty array to conform with
            // function's signature.
            Array.empty[Row]
          })
        unblockUntilPreCommit(observer_)
        busyWaitFor(observer_.phases.preparePhase.hasEntered, timeout)

        observer = Some(observer_)
        future = Some(future_)
      }
    }

    /** Commit the transaction. */
    def commit(ctx: TestContext): Unit = {
      assert(observer.isDefined, "transaction not started")
      assert(future.isDefined, "transaction not started")
      val preCommitVersion = ctx.deltaLog.update().version
      withSQLConf(sqlConf.toSeq: _*) {
        ctx.trackTransaction(this) {
          unblockCommit(observer.get)
          waitForCommit(observer.get)
          try {
            ThreadUtils.awaitResult(future.get, Duration.Inf)
          } catch {
            case e: SparkException if e.getCause.isInstanceOf[ExecutionException] =>
              throw e.getCause
            case e: SparkException =>
              throw e
          }
        }
      }

      // Ensure that the transaction actually commits something.
      val postCommitVersion = ctx.deltaLog.update().version
      assert(postCommitVersion > preCommitVersion, s"Transaction $this did not commit")
    }

    /** Run transaction and interleave fn() while transaction is stopped in precommit. */
    def interleave[T](ctx: TestContext)(fn: => Unit): Unit = {
      start(ctx)
      fn
      commit(ctx)
    }
  }

  /**
   * Helper class containing the Delta log and committed transactions of a test.
   */
  class TestContext(val deltaLog: DeltaLog) {
    /** The version of the Delta table after writing the initial data. */
    val initialVersion: Long = deltaLog.update().version

    private val committedTransactions: ArrayBuffer[TestTransaction] = ArrayBuffer.empty

    /** Returns the transactions that successfully committed. */
    def getCommittedTransactions: Seq[TestTransaction] = committedTransactions.toSeq

    /** Execute fn() and record the transaction if it successfully created a commit. */
    def trackTransaction(transaction: TestTransaction)(fn: => Unit): Unit = {
      val preCommitVersion = deltaLog.update().version
      fn
      if (deltaLog.update().version > preCommitVersion) {
        committedTransactions.append(transaction)
      }
    }

    def deltaTable: IODeltaTable = IODeltaTable.forPath(deltaLog.dataPath.toString)
  }

  case class Insert(
      rows: Seq[Long],
      partitionColumn: Long = 0L,
      sqlConf: Map[String, String] = Map.empty) extends TestTransaction(sqlConf) {
    override val name: String = {
      val rowsStr = abbreviate(rows.mkString(","), "...", 10)
      s"INSERT($rowsStr)($sqlConfStr)"
    }

    override def toSQL(tableName: String): String = {
      throw new UnsupportedOperationException("toSQL for Insert is not implemented yet")
    }

    override def executeImpl(ctx: TestContext): Unit = {
      rows.toDF(ID_COLUMN).withColumn(PARTITION_COLUMN, lit(partitionColumn))
        .write.format("delta").mode("append").save(ctx.deltaLog.dataPath.toString)
    }

    override def dataChange: Boolean = true
  }

  case class Delete(
      rows: Seq[Long],
      sqlConf: Map[String, String] = Map.empty) extends TestTransaction(sqlConf) {
    override val name: String = {
      val rowsStr = abbreviate(rows.mkString(","), "...", 10)
      s"DELETE($rowsStr)($sqlConfStr)"
    }

    override def toSQL(tableName: String): String = {
      val inRowsStr = rows.mkString("(", ", ", ")")
      s"DELETE FROM $tableName WHERE $ID_COLUMN IN $inRowsStr"
    }

    override def dataChange: Boolean = true

    override def deletionVectorsEnabled(deltaLog: DeltaLog): Boolean = {
      var result = false
      withSQLConf(sqlConf.toSeq: _*) {
        result = deletionVectorsEnabledInDelete(spark, deltaLog)
      }
      result
    }
  }

  case class Update(
      rows: Seq[Long],
      setValue: Long = 42,
      sqlConf: Map[String, String] = Map.empty) extends TestTransaction(sqlConf) {
    override val name: String = {
      val rowsStr = abbreviate(rows.mkString(","), "...", 10)
      s"UPDATE($rowsStr)($sqlConfStr)"
    }

    override def toSQL(tableName: String): String = {
      val inRowsStr = rows.mkString("(", ", ", ")")
      // Dummy update.
      s"UPDATE $tableName SET $ID_COLUMN=$setValue WHERE $ID_COLUMN IN $inRowsStr"
    }

    override def dataChange: Boolean = true

    override def deletionVectorsEnabled(deltaLog: DeltaLog): Boolean = {
      var result = false
      withSQLConf(sqlConf.toSeq: _*) {
        result = deletionVectorsEnabledInUpdate(spark, deltaLog)
      }
      result
    }
  }

  // Delete-only MERGE.
  case class Merge(
      deleteRows: Seq[Long],
      sqlConf: Map[String, String] = Map.empty) extends TestTransaction(sqlConf) {
    override val name: String = {
      val rowsStr = abbreviate(deleteRows.mkString(","), "...", 10)
      s"MERGE($rowsStr)($sqlConfStr)"
    }

    override def toSQL(tableName: String): String = {
      val inRowsStr = deleteRows.mkString("(", ", ", ")")
      s"""
         |MERGE INTO $tableName t
         |USING $tableName s
         |ON t.$ID_COLUMN = s.$ID_COLUMN AND t.$ID_COLUMN IN $inRowsStr
         |WHEN MATCHED THEN DELETE
         |""".stripMargin
    }

    override def dataChange: Boolean = true

    override def deletionVectorsEnabled(deltaLog: DeltaLog): Boolean = {
      var result = false
      withSQLConf(sqlConf.toSeq: _*) {
        result = deletionVectorsEnabledInMerge(spark, deltaLog)
      }
      result
    }
  }
}
