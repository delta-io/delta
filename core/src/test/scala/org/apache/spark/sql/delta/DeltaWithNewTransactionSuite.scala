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

import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.{ThreadUtils, Utils}

trait DeltaWithNewTransactionSuiteBase extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  /**
   * Test whether `withNewTransaction` captures all delta read made within it and correctly
   * detects conflicts in transaction table and provides snapshot isolation for other table reads.
   *
   * The order in which the given thunks are executed is as follows.
   * - Txn started using `withNewTransaction`. The following are executed while the txn is active.
   * - currentThreadReadOp - Read operations performed in current thread.
   * - concurrentUpdateOp - Update operations performed in different thread to
   *                        simulate concurrent modification. This is synchronously completed
   *                        before moving on.
   * - currentThreadCommitOperation - Attempt to commit changes in the txn.
   */
  protected def testWithNewTransaction(
      name: String,
      partitionedTableKeys: Seq[Int],
      preTxnSetup: DeltaLog => Unit = null,
      currentThreadReadOp: DataFrame => Unit,
      concurrentUpdateOp: String => Unit,
      currentThreadCommitOperation: OptimisticTransaction => Unit,
      shouldFail: Boolean,
      confs: Map[String, String] = Map.empty,
      partitionTablePath: String = Utils.createTempDir().getAbsolutePath): Unit = {

    val tableName = "NewTransactionTest"
    require(currentThreadCommitOperation != null)

    import testImplicits._

    test(s"withNewTransaction - $name") {
      withSQLConf(confs.toSeq: _*) { withTable(tableName) {
        sql(s"CREATE TABLE NewTransactionTest(key int, value int) " +
          s"USING delta partitioned by (key) LOCATION '$partitionTablePath'")
        partitionedTableKeys.toDS.select('value as "key", 'value)
          .write.mode("append").partitionBy("key").format("delta").saveAsTable(tableName)

        val log = DeltaLog.forTable(spark, partitionTablePath)
        assert(OptimisticTransaction.getActive().isEmpty, "active txn already set")

        if (preTxnSetup != null) preTxnSetup(log)

        log.withNewTransaction { txn =>
          assert(OptimisticTransaction.getActive().nonEmpty, "active txn not set")

          currentThreadReadOp(spark.table(tableName))

          ThreadUtils.runInNewThread(s"withNewTransaction test - $name") {
            concurrentUpdateOp(tableName)
          }

          if (shouldFail) {
            intercept[DeltaConcurrentModificationException] { currentThreadCommitOperation(txn) }
          } else {
            currentThreadCommitOperation(txn)
          }
        }
        assert(OptimisticTransaction.getActive().isEmpty, "active txn not cleared")
      }}
    }
  }

  testWithNewTransaction(
    name = "capture reads on txn table with no filters (i.e. full scan)",
    partitionedTableKeys = Seq(1, 2, 3),
    currentThreadReadOp = txnTable => {
      txnTable.count()
    },
    concurrentUpdateOp = txnTableName => {
      sql(s"DELETE FROM $txnTableName WHERE key = 1")
    },
    currentThreadCommitOperation = txn => {
      txn.commit(Seq.empty, DeltaOperations.ManualUpdate)
    },
    shouldFail = true)

  testWithNewTransaction(
    name = "capture reads on txn table with partition filter + conflicting concurrent updates",
    partitionedTableKeys = Seq(1, 2, 3),
    currentThreadReadOp = txnTable => {
      txnTable.filter("key == 1").count()
    },
    concurrentUpdateOp = txnTableName => {
      sql(s"DELETE FROM $txnTableName WHERE key = 1")
    },
    currentThreadCommitOperation = txn => {
      // Concurrent delete op touches the same partition as those read in the active txn.
      txn.commit(Seq.empty, DeltaOperations.ManualUpdate)
    },
    shouldFail = true)

  testWithNewTransaction(
    name = "snapshot isolation for query that can leverage metadata query optimization",
    partitionedTableKeys = Seq(1, 2, 3),
    currentThreadReadOp = txnTable => {
      txnTable.count()
    },
    concurrentUpdateOp = txnTableName => {
      sql(s"DELETE FROM $txnTableName WHERE key = 1")
    },
    currentThreadCommitOperation = txn => {
      txn.commit(Seq.empty, DeltaOperations.ManualUpdate)
    },
    shouldFail = true)

  testWithNewTransaction(
    name = "snapshot isolation for query that can leverage metadata query optimization " +
      "with partition filter + conflicting concurrent updates",
    partitionedTableKeys = Seq(1, 2, 3),
    currentThreadReadOp = txnTable => {
      txnTable.filter("key == 1").count()
    },
    concurrentUpdateOp = txnTableName => {
      sql(s"DELETE FROM $txnTableName WHERE key = 1")
    },
    currentThreadCommitOperation = txn => {
      // Concurrent delete op touches the same partition as those read in the active txn.
      txn.commit(Seq.empty, DeltaOperations.ManualUpdate)
    },
    shouldFail = true)

  testWithNewTransaction(
    name = "capture reads on txn table with data filter + conflicting concurrent updates",
    partitionedTableKeys = Seq(1, 2, 3),  // will generate (key, value) = (1, 1), (2, 2), (3, 3)
    currentThreadReadOp = txnTable => {
      txnTable.filter("value == 1").count()  // pure data filter that touches one file
    },
    concurrentUpdateOp = txnTableName => {
      sql(s"DELETE FROM $txnTableName WHERE key = 1")  // deletes the one file read above
    },
    currentThreadCommitOperation = txn => {
      txn.commit(Seq.empty, DeltaOperations.ManualUpdate)
    },
    shouldFail = true)

  testWithNewTransaction(
    name = "capture reads on txn table with partition filter + non-conflicting concurrent updates",
    partitionedTableKeys = Seq(1, 2, 3),
    currentThreadReadOp = txnTable => {
      txnTable.filter("key == 1").count()
    },
    concurrentUpdateOp = txnTableName => {
      sql(s"DELETE FROM $txnTableName WHERE key = 2")
      sql(s"INSERT INTO $txnTableName SELECT 4, 4")
    },
    currentThreadCommitOperation = txn => {
      // Concurrent delete op touches the different files as those read in the active txn.
      txn.commit(Seq.empty, DeltaOperations.ManualUpdate)
    },
    shouldFail = false)

  testWithNewTransaction(
    name = "snapshot isolation for metadata optimizable query with partition filter +" +
      " non-conflicting concurrent updates",
    partitionedTableKeys = Seq(1, 2, 3),
    currentThreadReadOp = txnTable => {
      txnTable.filter("key == 1").count()
    },
    concurrentUpdateOp = txnTableName => {
      sql(s"DELETE FROM $txnTableName WHERE key = 2")
      sql(s"INSERT INTO $txnTableName SELECT 4, 4")
    },
    currentThreadCommitOperation = txn => {
      // Concurrent delete op touches the different files as those read in the active txn.
      txn.commit(Seq.empty, DeltaOperations.ManualUpdate)
    },
    shouldFail = false)

  testWithNewTransaction(
    name = "capture reads on txn table with filter+limit and conflicting concurrent updates",
    partitionedTableKeys = Seq(1, 2, 3),
    currentThreadReadOp = txnTable => {
      txnTable.filter("key == 1").limit(1).collect()
    },
    concurrentUpdateOp = txnTableName => {
      sql(s"DELETE FROM $txnTableName WHERE key = 1")
    },
    currentThreadCommitOperation = txn => {
      // Concurrent delete op touches the same files as those read in the active txn.
      txn.commit(Seq.empty, DeltaOperations.ManualUpdate)
    },
    shouldFail = true)

  testWithNewTransaction(
    name = "capture reads on txn table with filter+limit and non-conflicting concurrent updates",
    partitionedTableKeys = Seq(1, 2, 3),
    currentThreadReadOp = txnTable => {
      txnTable.filter("key == 1").limit(1).collect()
    },
    concurrentUpdateOp = txnTableName => {
      sql(s"DELETE FROM $txnTableName WHERE key = 2")
      sql(s"INSERT INTO $txnTableName SELECT 4, 4")
    },
    currentThreadCommitOperation = txn => {
      // Concurrent delete op touches the different files as those read in the active txn.
      txn.commit(Seq.empty, DeltaOperations.ManualUpdate)
    },
    shouldFail = false)

  testWithNewTransaction(
    name = "capture reads on txn table with limit + conflicting concurrent updates",
    partitionedTableKeys = Seq(1, 2, 3),
    currentThreadReadOp = txnTable => {
      txnTable.limit(1).collect()
    },
    concurrentUpdateOp = txnTableName => {
      sql(s"DELETE FROM $txnTableName WHERE true")
    },
    currentThreadCommitOperation = txn => {
      txn.commit(Seq.empty, DeltaOperations.ManualUpdate)
    },
    shouldFail = true)

  testWithNewTransaction(
    name = "capture reads on txn table even when limit pushdown is disabled",
    confs = Map(DeltaSQLConf.DELTA_LIMIT_PUSHDOWN_ENABLED.key -> "false"),
    partitionedTableKeys = Seq(1, 2, 3),
    currentThreadReadOp = txnTable => {
      txnTable.limit(1).collect()
    },
    concurrentUpdateOp = txnTableName => {
      sql(s"UPDATE $txnTableName SET key = 2 WHERE key = 3")
    },
    currentThreadCommitOperation = txn => {
      // Any concurrent change (even if its seemingly non-conflicting) should fail the filter as
      // the whole table will be scanned by the filter when data skipping is disabled
      txn.commit(Seq.empty, DeltaOperations.ManualUpdate)
    },
    shouldFail = true)

  test("withNewTransaction - nesting withNewTransaction is not supported") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      log.withNewTransaction { txn =>

        require(OptimisticTransaction.getActive().nonEmpty)
        intercept[IllegalStateException] {
          OptimisticTransaction.setActive(txn)
        }

        intercept[IllegalStateException] {
          log.withNewTransaction { txn2 => }
        }
      }
    }
  }
}

class DeltaWithNewTransactionSuite extends DeltaWithNewTransactionSuiteBase
