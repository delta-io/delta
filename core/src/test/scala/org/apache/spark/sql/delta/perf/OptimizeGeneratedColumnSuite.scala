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

package org.apache.spark.sql.delta.perf

import org.apache.spark.sql.delta.ActiveOptimisticTransactionRule
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.util.ThreadUtils

import java.util.concurrent.{CountDownLatch, TimeUnit}


class OptimizeGeneratedColumnSuite extends OptimizeGeneratedColumnSuiteBase {
  import testImplicits._

  test("generated partition filters should avoid conflicts") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      withTableName("avoid_conflicts") { table =>
        createTable(
          table,
          "eventTime TIMESTAMP" :: Nil,
          "date DATE GENERATED ALWAYS AS ( CAST(eventTime AS DATE) )" :: Nil,
          Some(path))
        insertInto(
          path,
          Seq(Tuple1("2021-01-01 00:00:00"), Tuple1("2021-01-02 00:00:00"))
            .toDF("eventTime")
            .withColumn("eventTime", $"eventTime".cast(TimestampType))
        )

        val unblockQueries = new CountDownLatch(1)
        val waitForAllQueries = new CountDownLatch(2)

        ActiveOptimisticTransactionRule.withCallbackOnGetSnapshot(_ => {
          waitForAllQueries.countDown()
          assert(
            unblockQueries.await(30, TimeUnit.SECONDS),
            "the main thread didn't wake up queries")
        }) {
          val threadPool = ThreadUtils.newDaemonFixedThreadPool(2, "test")
          try {
            // Run two queries that should not conflict with each other if we generate the partition
            // filter correctly.
            val f1 = threadPool.submit(() => {
              spark.read.format("delta").load(path).where("eventTime = '2021-01-01 00:00:00'")
                .write.mode("append").format("delta").save(path)
              true
            })
            val f2 = threadPool.submit(() => {
              spark.read.format("delta").load(path).where("eventTime = '2021-01-02 00:00:00'")
                .write.mode("append").format("delta").save(path)
              true
            })
            assert(
              waitForAllQueries.await(30, TimeUnit.SECONDS),
              "queries didn't finish before timeout")
            unblockQueries.countDown()
            f1.get(30, TimeUnit.SECONDS)
            f2.get(30, TimeUnit.SECONDS)
          } finally {
            threadPool.shutdownNow()
          }
        }
      }
    }
  }
}
