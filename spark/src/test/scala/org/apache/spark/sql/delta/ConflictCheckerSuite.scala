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

import scala.concurrent.duration.Duration

import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.concurrency.{PhaseLockingTestMixin, TransactionExecutionTestMixin}

import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

class ConflictCheckerSuite
  extends QueryTest
  with SharedSparkSession
  with PhaseLockingTestMixin
  with TransactionExecutionTestMixin {

  import testImplicits._

  for (logicalConflict <- BOOLEAN_DOMAIN) {
    test(s"widen outer references in txn read predicates - logicalConflict=$logicalConflict") {
      withTable("target", "source") {
        // Create the target table.
        spark.range(0, 1000, 1, 20).toDF("dataCol")
          .withColumn("partCol", floor($"dataCol" / 100))
          .write
          .partitionBy("partCol")
          .format("delta")
          .saveAsTable("target")

        // Create the source table.
        val startIndex = if (logicalConflict) 0 else 100
        spark.range(startIndex, 1000, 1, 20).toDF("s_dataCol")
          .withColumn("s_partCol", floor($"s_dataCol" / 100))
          .withColumn(s"s_otherCol", lit("Lorem"))
          .write
          .format("delta")
          .saveAsTable("source")

        val winningTxn = "DELETE FROM target WHERE dataCol = 0"
        val currentTxn =
          s"""
             |DELETE FROM target t
             |WHERE
             |  t.dataCol = 0
             |  AND EXISTS(
             |    SELECT 1
             |    FROM source s
             |    WHERE
             |      t.partCol = s.s_partCol
             |      AND s.s_otherCol IN ('Lorem')
             |)
             |""".stripMargin

        val (usageRecords, currentFuture, winningFuture) = runTxnsWithOrder__A_Start__B__A_End(
          txnA = () => spark.sql(currentTxn).collect(),
          txnB = () => spark.sql(winningTxn).collect()
        )

        ThreadUtils.awaitResult(winningFuture, Duration.Inf)

        if (logicalConflict) {
          val e = intercept[SparkException] {
            ThreadUtils.awaitResult(currentFuture, Duration.Inf)
          }
          assert(e.getCause.isInstanceOf[ConcurrentAppendException])
        } else {
          ThreadUtils.awaitResult(currentFuture, Duration.Inf)
        }

        val opType = "delta.conflictDetection.partitionLevelConcurrency.widenOuterReferences"
        val event = usageRecords.find {
          record => record.metric == "tahoeEvent" && record.tags("opType") == opType
        }
        assert(event.nonEmpty)
      }
    }
  }
}
