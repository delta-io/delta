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

import org.apache.spark.sql.delta.DeltaConfigs.{COORDINATED_COMMITS_COORDINATOR_CONF, COORDINATED_COMMITS_COORDINATOR_NAME}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamTest

/**
 * General note on streaming tests: by default, a streaming query uses the ProcessingTime trigger
 * with a 0-second interval, so the query will be executed as fast as possible, and the read path
 * will be triggered periodically. Because of this, things like asserting the number of getCommits
 * on the tracking client will not be deterministic.
 */
trait CoordinatedCommitsStreamingSuiteBase extends StreamTest
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest {

  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey,
        getCommitCoordinatorClientBuilder.getName)
      .set(COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey, JsonUtils.toJson(Map()))
  }

  protected def getTrackingClient: TrackingCommitCoordinatorClient = {
    CommitCoordinatorProvider
      .getCommitCoordinatorClient(getCommitCoordinatorClientBuilder.getName, Map.empty, spark)
      .asInstanceOf[TrackingCommitCoordinatorClient]
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    CommitCoordinatorProvider.clearNonDefaultBuilders()
    // Although we drop the tables at the end of each test, the tables are not cleaned up on
    // the commit coordinator side. Therefore, we create and register a new builder for each
    // test so that the tables created in one test do not interfere with the tables created
    // in other tests.
    CommitCoordinatorProvider.registerBuilder(getCommitCoordinatorClientBuilder)
    getTrackingClient.reset()
    DeltaLog.clearCache()
  }

  protected def getCommitCoordinatorClientBuilder: CommitCoordinatorBuilder

  test("stream from delta source") {
    val trackingClient = getTrackingClient
    val tableName = "source"

    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (value INT) USING delta")

      val df = spark.readStream
        .format("delta")
        .table(tableName)

      trackingClient.reset()

      testStream(df)(
        Execute{ _ =>
          Seq(1, 2).toDF().write.format("delta").mode("append").saveAsTable(tableName)
        },
        ProcessAllAvailable(),
        CheckAnswer(1, 2),
        Assert(trackingClient.numCommitsCalled.get === 1),
        // At least one read from the commit and one from checking the result
        Assert(trackingClient.numGetCommitsCalled.get >= 2)
      )
    }
  }

  test("stream to delta sink") {
    val trackingClient = getTrackingClient
    val tableName = "sink"

    withTempDir { tempDir =>
      withTable(tableName) {
        var expectedNumCommits = 0
        var expectedNumGetCommits = 0

        val inputData = MemoryStream[Int]
        val query = inputData
          .toDF()
          .writeStream
          .format("delta")
          .option("checkpointLocation", tempDir.getAbsolutePath)
          .toTable(tableName)
        query.processAllAvailable()

        try {
          inputData.addData(1)
          query.processAllAvailable()
          expectedNumCommits += 1
          expectedNumGetCommits += 1

          // Loading after the first write to ensure the delta log is created.
          val outputDf = spark.read.format("delta").table(tableName)
          checkDatasetUnorderly(outputDf.as[Int], 1)
          expectedNumGetCommits += 1
          assert(trackingClient.numCommitsCalled.get === expectedNumCommits)
          assert(trackingClient.numGetCommitsCalled.get >= expectedNumGetCommits)

          inputData.addData(2)
          query.processAllAvailable()
          expectedNumCommits += 1
          expectedNumGetCommits += 1

          checkDatasetUnorderly(outputDf.as[Int], 1, 2)
          expectedNumGetCommits += 1
          assert(trackingClient.numCommitsCalled.get === expectedNumCommits)
          assert(trackingClient.numGetCommitsCalled.get >= expectedNumGetCommits)
        } finally {
          query.stop()
        }
      }
    }
  }

  test("stream from delta source to delta sink with shared commit coordinator") {
    val trackingClient = getTrackingClient
    val sourceTableName = "source"
    val sinkTableName = "sink"

    withTempDir { tempDir =>
      withTable(sourceTableName, sinkTableName) {
        var expectedNumCommits = 0
        var expectedNumGetCommits = 0

        sql(s"CREATE TABLE $sourceTableName (value INT) USING delta")

        trackingClient.reset()

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
        assert(trackingClient.numCommitsCalled.get === expectedNumCommits)
        assert(trackingClient.numGetCommitsCalled.get >= expectedNumGetCommits)

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
          assert(trackingClient.numCommitsCalled.get === expectedNumCommits)
          assert(trackingClient.numGetCommitsCalled.get >= expectedNumGetCommits)

          Seq(2).toDF().write.format("delta").mode("append").saveAsTable(sourceTableName)
          query.processAllAvailable()
          // One commit to the source table and one commit to the sink table
          expectedNumCommits += 2
          expectedNumGetCommits += 2

          checkDatasetUnorderly(outputDf.as[Int], 1, 2)
          expectedNumGetCommits += 1
          assert(trackingClient.numCommitsCalled.get === expectedNumCommits)
          assert(trackingClient.numGetCommitsCalled.get >= expectedNumGetCommits)
        } finally {
          query.stop()
        }
      }
    }
  }
}


class CoordinatedCommitsStreamingSuite extends CoordinatedCommitsStreamingSuiteBase {

  import testImplicits._

  protected override def getCommitCoordinatorClientBuilder: CommitCoordinatorBuilder =
    TrackingInMemoryCommitCoordinatorBuilder(batchSize = 10)

  test("stream from delta source to delta sink with shared commit coordinator, path-based access") {
    val trackingClient = getTrackingClient

    withTempDir { sourceDir =>
      var expectedNumCommits = 0
      var expectedNumGetCommits = 0
      val sourceTablePath = sourceDir.getAbsolutePath

      sql(s"CREATE TABLE delta.`$sourceTablePath` (value INT) USING delta")

      trackingClient.reset()

      withTempDir { sinkDir =>
        val sinkTablePath = sinkDir.getAbsolutePath

        val query = spark.readStream
          .format("delta")
          .load(sourceTablePath)
          .toDF()
          .writeStream
          .format("delta")
          .option("checkpointLocation", sinkDir.getAbsolutePath)
          .start(sinkTablePath)
        query.processAllAvailable()
        assert(trackingClient.numCommitsCalled.get === expectedNumCommits)
        assert(trackingClient.numGetCommitsCalled.get >= expectedNumGetCommits)

        try {
          Seq(1).toDF().write.format("delta").mode("append").save(sourceTablePath)
          query.processAllAvailable()
          // One commit to the source table and one commit to the sink table
          expectedNumCommits += 2
          expectedNumGetCommits += 2

          // Loading after the first write to ensure the delta log is created.
          val outputDf = spark.read.format("delta").load(sinkTablePath)
          checkDatasetUnorderly(outputDf.as[Int], 1)
          expectedNumGetCommits += 1
          assert(trackingClient.numCommitsCalled.get === expectedNumCommits)
          assert(trackingClient.numGetCommitsCalled.get >= expectedNumGetCommits)

          Seq(2).toDF().write.format("delta").mode("append").save(sourceTablePath)
          query.processAllAvailable()
          // One commit to the source table and one commit to the sink table
          expectedNumCommits += 2
          expectedNumGetCommits += 2

          checkDatasetUnorderly(outputDf.as[Int], 1, 2)
          expectedNumGetCommits += 1
          assert(trackingClient.numCommitsCalled.get === expectedNumCommits)
          assert(trackingClient.numGetCommitsCalled.get >= expectedNumGetCommits)
        } finally {
          query.stop()
        }
      }
    }
  }
}

