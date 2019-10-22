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

import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, SetTransaction}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.test.SharedSparkSession

class OptimisticTransactionSuite extends QueryTest with SharedSparkSession {
  private val addA = AddFile("a", Map.empty, 1, 1, dataChange = true)
  private val addB = AddFile("b", Map.empty, 1, 1, dataChange = true)
  private val addC = AddFile("c", Map.empty, 1, 1, dataChange = true)

  import testImplicits._

  test("block append against metadata change") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      // Initialize the log. Truncate() is just a no-op placeholder.
      log.startTransaction().commit(Nil, Truncate())

      val txn = log.startTransaction()
      val winningTxn = log.startTransaction()
      winningTxn.commit(Metadata() :: Nil, Truncate())
      intercept[MetadataChangedException] {
        txn.commit(addA :: Nil, Truncate())
      }
    }
  }

  test("block read+append against append") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      // Initialize the log. Truncate() is just a no-op placeholder.
      log.startTransaction().commit(Metadata() :: Nil, Truncate())

      val txn = log.startTransaction()
      // reads the table
      txn.filterFiles()
      val winningTxn = log.startTransaction()
      winningTxn.commit(addA :: Nil, Truncate())
      // TODO: intercept a more specific exception
      intercept[DeltaConcurrentModificationException] {
        txn.commit(addB :: Nil, Truncate())
      }
    }
  }

  test("allow blind-append against any data change") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      // Initialize the log and add data. Truncate() is just a no-op placeholder.
      log.startTransaction().commit(addA :: Nil, Truncate())

      val txn = log.startTransaction()
      val winningTxn = log.startTransaction()
      winningTxn.commit(addA.remove :: addB :: Nil, Truncate())
      txn.commit(addC :: Nil, Truncate())
      checkAnswer(log.update().allFiles.select("path"), Row("b") :: Row("c") :: Nil)
    }
  }

  test("allow read+append+delete against no data change") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      // Initialize the log and add data. Truncate() is just a no-op placeholder.
      log.startTransaction().commit(addA :: Nil, Truncate())

      val txn = log.startTransaction()
      txn.filterFiles()
      val winningTxn = log.startTransaction()
      winningTxn.commit(Nil, Truncate())
      txn.commit(addA.remove :: addB :: Nil, Truncate())
      checkAnswer(log.update().allFiles.select("path"), Row("b") :: Nil)
    }
  }

  test("allow concurrent set-txns with different app ids") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      // Initialize the log and add data. Truncate() is just a no-op placeholder.
      log.startTransaction().commit(Nil, Truncate())

      val txn = log.startTransaction()
      txn.txnVersion("t1")
      val winningTxn = log.startTransaction()
      winningTxn.commit(SetTransaction("t2", 1, Some(1234L)) :: Nil, Truncate())
      txn.commit(Nil, Truncate())

      assert(log.update().transactions === Map("t2" -> 1))
    }
  }

  test("block concurrent set-txns with the same app id") {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      // Initialize the log and add data. Truncate() is just a no-op placeholder.
      log.startTransaction().commit(Nil, Truncate())

      val txn = log.startTransaction()
      txn.txnVersion("t1")
      val winningTxn = log.startTransaction()
      winningTxn.commit(SetTransaction("t1", 1, Some(1234L)) :: Nil, Truncate())

      intercept[ConcurrentTransactionException] {
        txn.commit(Nil, Truncate())
      }
    }
  }

  test("query with predicates should skip partitions") {
    withTempDir { tempDir =>
      val testPath = tempDir.getCanonicalPath
      spark.range(2)
        .map(_.toInt)
        .withColumn("part", $"value" % 2)
        .write
        .format("delta")
        .partitionBy("part")
        .mode("append")
        .save(testPath)

      val query = spark.read.format("delta").load(testPath).where("part = 1")
      val fileScans = query.queryExecution.executedPlan.collect {
        case f: FileSourceScanExec => f
      }
      assert(fileScans.size == 1)
      assert(fileScans.head.metadata.get("PartitionCount").contains("1"))
      checkAnswer(query, Seq(Row(1, 1)))
    }
  }
}
