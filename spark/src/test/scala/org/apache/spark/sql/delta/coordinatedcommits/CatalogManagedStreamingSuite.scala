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

package org.apache.spark.sql.delta.coordinatedcommits

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.test.shims.StreamingTestShims.MemoryStream

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamTest

/**
 * General note on streaming tests: by default, a streaming query uses the ProcessingTime trigger
 * with a 0-second interval, so the query will be executed as fast as possible, and the read path
 * will be triggered periodically. Because of this, things like asserting the number of getCommits
 * on the tracking client will not be deterministic.
 */
trait CatalogManagedStreamingSuiteBase
  extends StreamTest
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest
  with CatalogOwnedTestBaseSuite {


  import testImplicits._
  import org.apache.spark.sql.delta.test.DeltaTestImplicits._

  protected def assertNumCommitsCalled(expectedNumCommits: Int): Unit = {
    getTrackingClient.foreach { trackingClient =>
      assert(trackingClient.numCommitsCalled.get === expectedNumCommits)
    }
  }

  protected def assertNumGetCommitsCalled(expectedNumGetCommits: Int): Unit = {
    getTrackingClient.foreach { trackingClient =>
      assert(trackingClient.numGetCommitsCalled.get >= expectedNumGetCommits)
    }
  }

  protected def getTrackingClient: Option[TrackingCommitCoordinatorClient] =
    if (catalogOwnedDefaultCreationEnabledInTests) {
      Some(getCatalogOwnedCommitCoordinatorClient(
        CatalogOwnedTableUtils.DEFAULT_CATALOG_NAME_FOR_TESTING)
          .asInstanceOf[TrackingCommitCoordinatorClient])
    } else {
      None
    }

  protected def resetTrackingClient(): Unit = {
    getTrackingClient.foreach(_.reset())
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    resetTrackingClient()
  }

  test("stream from delta source") {
    withTempTable(createTable = false) { sourceTableName =>
      sql(s"CREATE TABLE $sourceTableName (value INT) USING delta")

      val df = spark.readStream
        .format("delta")
        .table(sourceTableName)

      resetTrackingClient()

      testStream(df)(
        Execute{ _ =>
          Seq(1, 2).toDF().write.format("delta").mode("append").saveAsTable(sourceTableName)
        },
        ProcessAllAvailable(),
        CheckAnswer(1, 2),
        Execute { _ => assertNumCommitsCalled(1) },
        // At least one read from the commit and one from checking the result
        Execute { _ => assertNumGetCommitsCalled(2) }
      )
    }
  }

  test("stream to delta sink") {
    // The dir is only used as the checkpoint location and doesn't imply a path-based access.
    withTempDir { tempDir =>
      withTempTable(createTable = false) { sinkTableName =>
        var expectedNumCommits = 0
        var expectedNumGetCommits = 0

        val inputData = MemoryStream[Int]
        val query = inputData
          .toDF()
          .writeStream
          .format("delta")
          .option("checkpointLocation", tempDir.getAbsolutePath)
          .toTable(sinkTableName)
        query.processAllAvailable()

        try {
          inputData.addData(1)
          query.processAllAvailable()
          expectedNumCommits += 1
          expectedNumGetCommits += 1

          // Loading after the first write to ensure the delta log is created.
          val outputDf = spark.read.format("delta").table(sinkTableName)
          checkDatasetUnorderly(outputDf.as[Int], 1)
          expectedNumGetCommits += 1
          assertNumCommitsCalled(expectedNumCommits)
          assertNumGetCommitsCalled(expectedNumGetCommits)

          inputData.addData(2)
          query.processAllAvailable()
          expectedNumCommits += 1
          expectedNumGetCommits += 1

          checkDatasetUnorderly(outputDf.as[Int], 1, 2)
          expectedNumGetCommits += 1
          assertNumCommitsCalled(expectedNumCommits)
          assertNumGetCommitsCalled(expectedNumGetCommits)
        } finally {
          query.stop()
        }
      }
    }
  }

  test("stream from delta source to delta sink with shared commit coordinator") {
    withTempDir { tempDir =>
      withTempTable(createTable = false) { sourceTableName =>
        withTempTable(createTable = false) { sinkTableName =>
          var expectedNumCommits = 0
          var expectedNumGetCommits = 0

          sql(s"CREATE TABLE $sourceTableName (value INT) USING delta")

          resetTrackingClient()

          val query = spark.readStream
            .format("delta")
            .table(sourceTableName)
            .toDF()
            .writeStream
            .format("delta")
            .option("checkpointLocation", tempDir.getAbsolutePath)
            .toTable(sinkTableName)
          query.processAllAvailable()
          expectedNumCommits += 1
          expectedNumGetCommits += 1
          assertNumCommitsCalled(expectedNumCommits)
          assertNumGetCommitsCalled(expectedNumGetCommits)

          try {
            Seq(1).toDF().write.format("delta").mode("append").saveAsTable(sourceTableName)
            query.processAllAvailable()
            // One commit to the source table and one commit to the sink table
            expectedNumCommits += 2
            expectedNumGetCommits += 2

            // Loading after the first write to ensure the delta log is created.
            val outputDf = spark.read.format("delta").table(sinkTableName)
            checkDatasetUnorderly(outputDf.as[Int], 1)
            expectedNumGetCommits += 1
            assertNumCommitsCalled(expectedNumCommits)
            assertNumGetCommitsCalled(expectedNumGetCommits)

            Seq(2).toDF().write.format("delta").mode("append").saveAsTable(sourceTableName)
            query.processAllAvailable()
            // One commit to the source table and one commit to the sink table
            expectedNumCommits += 2
            expectedNumGetCommits += 2

            checkDatasetUnorderly(outputDf.as[Int], 1, 2)
            expectedNumGetCommits += 1
            assertNumCommitsCalled(expectedNumCommits)
            assertNumGetCommitsCalled(expectedNumGetCommits)
          } finally {
            query.stop()
          }
        }
      }
    }
  }
}

class CatalogManagedStreamingSuite extends CatalogManagedStreamingSuiteBase {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(10)
}
