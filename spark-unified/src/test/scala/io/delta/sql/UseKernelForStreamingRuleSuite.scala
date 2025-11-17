/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.sql

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.StreamingRelationV2
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.execution.streaming.{StreamTest, StreamingRelation}
import org.apache.spark.sql.test.SharedSparkSession
import io.delta.kernel.spark.table.SparkTable

class UseKernelForStreamingRuleSuite
  extends QueryTest
  with SharedSparkSession
  with StreamTest
  with DeltaSQLCommandTest {

  import testImplicits._

  // Helper: enable/disable config
  def withKernelStreaming[T](enabled: Boolean)(f: => T): T = {
    withSQLConf(DeltaSQLConf.DELTA_KERNEL_STREAMING_ENABLED.key -> enabled.toString) {
      f
    }
  }

  // Helper: check for V2 in logical plan
  def assertUsesV2(df: DataFrame): Unit = {
    val plan = df.queryExecution.analyzed
    val hasV2 = plan.collectFirst {
      case StreamingRelationV2(_, _, table, _, _, _, _, _)
        if table.table.isInstanceOf[SparkTable] =>
    }.isDefined
    assert(hasV2, s"Expected V2 with SparkTable:\n${plan.treeString}")
  }

  // Helper: check for V1 in logical plan
  def assertUsesV1(df: DataFrame): Unit = {
    val plan = df.queryExecution.analyzed
    val hasV1 = plan.collectFirst {
      case _: StreamingRelation =>
    }.isDefined
    assert(hasV1, s"Expected V1 StreamingRelation:\n${plan.treeString}")
  }

  test("catalog table logical plan uses V2 when enabled") {
    withTable("test_table") {
      sql("CREATE TABLE test_table (id INT, value STRING) USING delta")
      sql("INSERT INTO test_table VALUES (1, 'a'), (2, 'b')")

      withKernelStreaming(enabled = true) {
        val streamDF = spark.readStream.table("test_table")
        assertUsesV2(streamDF)
      }
    }
  }

  test("catalog table execution with V2 produces correct results") {
    withTable("test_table") {
      sql("CREATE TABLE test_table (id INT, value STRING) USING delta")
      sql("INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c')")

      withKernelStreaming(enabled = true) {
        val streamDF = spark.readStream.table("test_table")

        val query = streamDF
          .writeStream
          .format("memory")
          .queryName("output")
          .start()

        query.processAllAvailable()
        query.stop()

        checkAnswer(
          spark.table("output"),
          Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"))
        )
      }
    }
  }

  test("path-based table uses V1 even when config enabled") {
    withTempDir { dir =>
      spark.range(5).write.format("delta").save(dir.getCanonicalPath)

      withKernelStreaming(enabled = true) {
        val streamDF = spark.readStream.format("delta").load(dir.getCanonicalPath)
        assertUsesV1(streamDF)
      }
    }
  }

  test("path-based table execution produces correct results") {
    withTempDir { dir =>
      spark.range(5).toDF("id").write.format("delta").save(dir.getCanonicalPath)

      withKernelStreaming(enabled = true) {
        val streamDF = spark.readStream.format("delta").load(dir.getCanonicalPath)

        val query = streamDF
          .writeStream
          .format("memory")
          .queryName("path_output")
          .start()

        query.processAllAvailable()
        query.stop()

        checkAnswer(
          spark.table("path_output"),
          (0 until 5).map(i => Row(i.toLong))
        )
      }
    }
  }

  test("config disabled - catalog table uses V1") {
    withTable("test_table") {
      sql("CREATE TABLE test_table (id INT) USING delta")
      sql("INSERT INTO test_table VALUES (1)")

      withKernelStreaming(enabled = false) {
        val streamDF = spark.readStream.table("test_table")
        assertUsesV1(streamDF)
      }
    }
  }

  test("V1 fallback field is populated") {
    withTable("test_table") {
      sql("CREATE TABLE test_table (id INT) USING delta")
      sql("INSERT INTO test_table VALUES (1)")

      withKernelStreaming(enabled = true) {
        val streamDF = spark.readStream.table("test_table")
        val plan = streamDF.queryExecution.analyzed

        val v1Fallback = plan.collectFirst {
          case StreamingRelationV2(_, _, _, _, _, _, _, v1Relation) => v1Relation
        }

        assert(v1Fallback.isDefined, "StreamingRelationV2 should exist")
        assert(v1Fallback.get.isDefined, "v1Relation should be populated")
        assert(v1Fallback.get.get.isInstanceOf[StreamingRelation],
          "v1Relation should contain StreamingRelation")
      }
    }
  }

  test("multi-batch streaming with V2 processes correctly") {
    withTable("test_table") {
      sql("CREATE TABLE test_table (id INT, value STRING) USING delta")
      sql("INSERT INTO test_table VALUES (1, 'a')")

      withKernelStreaming(enabled = true) {
        val streamDF = spark.readStream.table("test_table")

        val query = streamDF
          .writeStream
          .format("memory")
          .queryName("multi_batch")
          .start()

        query.processAllAvailable()

        // Add more data while streaming
        sql("INSERT INTO test_table VALUES (2, 'b'), (3, 'c')")
        query.processAllAvailable()

        query.stop()

        checkAnswer(
          spark.table("multi_batch"),
          Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"))
        )
      }
    }
  }
}

