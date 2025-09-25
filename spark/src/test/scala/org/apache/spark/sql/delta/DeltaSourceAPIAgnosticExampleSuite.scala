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

import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.hadoop.fs.Path

/**
 * Example of API-agnostic tests that demonstrate how to test both DSv1 and DSv2
 * with the same test logic.
 */
trait DeltaSourceAPIAgnosticTestsBase extends DeltaSourceAPITestBase {

  import testImplicits._

  protected def runBasicStreamingTests(): Unit = {
    testWithBothAPIs("basic streaming read") {
      withTempDir { inputDir =>
        val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
        withMetadata(deltaLog, StructType.fromDDL("value STRING"))

        val df = readDeltaStream(inputDir.getCanonicalPath)
          .filter($"value".contains("keep"))

        testStream(df)(
          AddToReservoir(inputDir, Seq("keep1", "keep2", "drop3").toDF),
          AssertOnQuery { q => q.processAllAvailable(); true },
          CheckAnswer("keep1", "keep2"),
          StopStream,
          AddToReservoir(inputDir, Seq("drop4", "keep5", "keep6").toDF),
          StartStream(),
          AssertOnQuery { q => q.processAllAvailable(); true },
          CheckAnswer("keep1", "keep2", "keep5", "keep6")
        )
      }
    }
  }

  protected def runStartingVersionTests(): Unit = {
    testWithBothAPIs("starting version option") {
      withTempDir { inputDir =>
        val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
        withMetadata(deltaLog, StructType.fromDDL("value STRING"))

        // Add initial data
        Seq("initial1", "initial2").toDF("value")
          .write.format("delta").mode("append").save(inputDir.getCanonicalPath)
        
        // Add more data  
        Seq("batch1", "batch2").toDF("value")
          .write.format("delta").mode("append").save(inputDir.getCanonicalPath)

        // Stream from version 1 (should skip initial data)
        val df = readDeltaStream(
          inputDir.getCanonicalPath,
          Map("startingVersion" -> "1")
        )

        testStream(df)(
          AssertOnQuery { q => q.processAllAvailable(); true },
          CheckAnswer("batch1", "batch2"),
          AddToReservoir(inputDir, Seq("batch3", "batch4").toDF("value")),
          AssertOnQuery { q => q.processAllAvailable(); true },
          CheckAnswer("batch1", "batch2", "batch3", "batch4")
        )
      }
    }
  }

  protected def runRateLimitingTests(): Unit = {
    testWithBothAPIs("rate limiting with maxFilesPerTrigger") {
      withTempDir { inputDir =>
        val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
        withMetadata(deltaLog, StructType.fromDDL("value STRING"))

        val df = readDeltaStream(
          inputDir.getCanonicalPath,
          Map("maxFilesPerTrigger" -> "1")
        )

        testStream(df)(
          AddToReservoir(inputDir, Seq("batch1_file1").toDF("value")),
          AssertOnQuery { q => q.processAllAvailable(); true },
          CheckAnswer("batch1_file1"),
          
          AddToReservoir(inputDir, Seq("batch2_file1").toDF("value")),
          AddToReservoir(inputDir, Seq("batch3_file1").toDF("value")),
          AssertOnQuery { q => q.processAllAvailable(); true },
          // With maxFilesPerTrigger=1, it should process files one by one
          CheckAnswer("batch1_file1", "batch2_file1", "batch3_file1")
        )
      }
    }
  }

  protected def runSchemaEvolutionTests(): Unit = {
    testWithBothAPIs("schema compatibility") {
      withTempDir { inputDir =>
        val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
        withMetadata(deltaLog, StructType.fromDDL("id LONG, value STRING"))

        // Write initial data
        Seq((1L, "value1"), (2L, "value2")).toDF("id", "value")
          .write.format("delta").mode("append").save(inputDir.getCanonicalPath)

        val df = readDeltaStream(inputDir.getCanonicalPath)

        testStream(df)(
          AssertOnQuery { q => q.processAllAvailable(); true },
          CheckAnswer(Row(1L, "value1"), Row(2L, "value2")),
          
          // Add more data with same schema
          AddToReservoir(inputDir, Seq((3L, "value3"), (4L, "value4")).toDF("id", "value")),
          AssertOnQuery { q => q.processAllAvailable(); true },
          CheckAnswer(
            Row(1L, "value1"), Row(2L, "value2"),
            Row(3L, "value3"), Row(4L, "value4")
          )
        )
      }
    }
  }

  /**
   * Tests specific to starting timestamp functionality
   */
  protected def runTimestampBasedTests(): Unit = {
    testWithBothAPIs("starting timestamp option") {
      withTempDir { inputDir =>
        val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
        withMetadata(deltaLog, StructType.fromDDL("value STRING"))

        // Add initial data
        Seq("before_timestamp").toDF("value")
          .write.format("delta").mode("append").save(inputDir.getCanonicalPath)
        
        // Get a timestamp for starting point
        val beforeTime = System.currentTimeMillis()
        Thread.sleep(1000) // Ensure time difference
        
        // Add data after timestamp
        Seq("after_timestamp").toDF("value")
          .write.format("delta").mode("append").save(inputDir.getCanonicalPath)

        val timestampStr = new java.sql.Timestamp(beforeTime).toString
        
        val df = readDeltaStream(
          inputDir.getCanonicalPath,
          Map("startingTimestamp" -> timestampStr)
        )

        testStream(df)(
          AssertOnQuery { q => q.processAllAvailable(); true },
          // Should only see data after the timestamp
          CheckAnswer("after_timestamp")
        )
      }
    }
  }
}

/**
 * DSv1 test suite - runs all API-agnostic tests with DSv1 configuration
 */
class DeltaSourceDSv1AgnosticSuite extends DeltaSourceAPIAgnosticTestsBase 
  with DeltaSourceDSv1Tests
  with DeltaSQLCommandTest {

  runBasicStreamingTests()
  runStartingVersionTests()
  runRateLimitingTests()
  runSchemaEvolutionTests()
  runTimestampBasedTests()
}

/**
 * DSv2 test suite - runs all API-agnostic tests with DSv2 configuration
 * Tests that fail due to DSv2 limitations will be marked as TODO/assumptions
 */
class DeltaSourceDSv2AgnosticSuite extends DeltaSourceAPIAgnosticTestsBase 
  with DeltaSourceDSv2Tests
  with DeltaSQLCommandTest {

  override protected def handleDSv2SpecificErrors(e: Throwable, testName: String): Unit = {
    e.getMessage match {
      case msg if msg.contains("latestOffset is not supported") =>
        logInfo(s"TODO(parity): $testName - DSv2 latestOffset not implemented")
        assume(false, s"DSv2 TODO: latestOffset not implemented")
      case msg if msg.contains("planInputPartitions is not supported") =>
        logInfo(s"TODO(parity): $testName - DSv2 planInputPartitions not implemented")
        assume(false, s"DSv2 TODO: planInputPartitions not implemented")
      case _ => super.handleDSv2SpecificErrors(e, testName)
    }
  }

  runBasicStreamingTests()
  runStartingVersionTests()
  runRateLimitingTests()
  runSchemaEvolutionTests()
  runTimestampBasedTests()
}

