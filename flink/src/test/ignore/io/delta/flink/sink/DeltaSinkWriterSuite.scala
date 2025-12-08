/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.sink

import java.lang.management.ManagementFactory
import java.net.URI
import java.util.concurrent.Executors

import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsJava, SeqHasAsJava}

import io.delta.flink.TestHelper
import io.delta.flink.table.HadoopTable
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.types.{IntegerType, StringType, StructType}

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup
import org.apache.flink.table.data.{GenericRowData, StringData}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

class DeltaSinkWriterSuite extends AnyFunSuite with TestHelper {

  test("write to empty table with no partition") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List.empty[String].asJava)
      table.open()

      val sinkWriter = new DeltaSinkWriter.Builder()
        .withJobId("test-job")
        .withSubtaskId(0)
        .withAttemptNumber(1)
        .withDeltaTable(table)
        .withConf(new DeltaSinkConf(schema, Map.empty[String, String].asJava))
        .withMetricGroup(UnregisteredMetricsGroup.createSinkWriterMetricGroup())
        .build()

      for (i <- 0 until 20) {
        sinkWriter.write(
          GenericRowData.of(i, StringData.fromString("p" + (i % 3))),
          new TestSinkWriterContext(i * 100, i * 100))
      }
      val results = sinkWriter.prepareCommit()
      // One partition
      assert(1 == results.size())
      // Each partition has one action
      results.asScala.foreach { result =>
        assert(1 == result.getDeltaActions.size)
        assert(1900 == result.getContext.getHighWatermark)
        assert(0 == result.getContext.getLowWatermark)
      }
    }
  }

  test("write to empty table using multiple partitions") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List("part").asJava)
      table.open()

      val sinkWriter = new DeltaSinkWriter.Builder()
        .withJobId("test-job")
        .withSubtaskId(0)
        .withAttemptNumber(1)
        .withDeltaTable(table)
        .withConf(new DeltaSinkConf(schema, Map.empty[String, String].asJava))
        .withMetricGroup(UnregisteredMetricsGroup.createSinkWriterMetricGroup())
        .build()

      val rowCount = 20
      val numPartitions = 3
      for (i <- 0 until rowCount) {
        sinkWriter.write(
          GenericRowData.of(i, StringData.fromString("p" + (i % numPartitions))),
          new TestSinkWriterContext(i * 100, i * 100))
      }
      val results = sinkWriter.prepareCommit()
      val expectedWatermarks = (0 until numPartitions).map { i =>
        val overflow = if (i < rowCount % numPartitions) 0 else 1
        val hwm = (rowCount / numPartitions - overflow) * numPartitions + i
        (i * 100L, hwm * 100L)
      }.toMap

      assert(numPartitions == results.size())
      // Each partition has one action
      results.asScala.zipWithIndex.foreach { case (result, idx) =>
        assert(1 == result.getDeltaActions.size)
        assert(expectedWatermarks(result.getContext.getLowWatermark) ==
          result.getContext.getHighWatermark)
      }
    }
  }

  test("write to existing table using multiple partitions") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val engine = DefaultEngine.create(new Configuration())
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      // Create a non-empty table
      createNonEmptyTable(engine, tablePath, schema, Seq("part"))
      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List("part").asJava)
      table.open()

      val sinkWriter = new DeltaSinkWriter.Builder()
        .withDeltaTable(table)
        .withJobId("test-job")
        .withSubtaskId(0)
        .withAttemptNumber(1)
        .withConf(new DeltaSinkConf(schema, Map.empty[String, String].asJava))
        .withMetricGroup(UnregisteredMetricsGroup.createSinkWriterMetricGroup())
        .build()

      val rowCount = 20
      val numPartitions = 3
      for (i <- 0 until rowCount) {
        sinkWriter.write(
          GenericRowData.of(i, StringData.fromString("p" + (i % numPartitions))),
          new TestSinkWriterContext(i * 100, i * 100))
      }
      val results = sinkWriter.prepareCommit()
      val expectedWatermarks = (0 until numPartitions).map { i =>
        val overflow = if (i < rowCount % numPartitions) 0 else 1
        val hwm = (rowCount / numPartitions - overflow) * numPartitions + i
        (i * 100L, hwm * 100L)
      }.toMap
      assert(numPartitions == results.size())
      // Each partition has one action
      results.asScala.zipWithIndex.foreach { case (result, idx) =>
        assert(1 == result.getDeltaActions.size)
        assert(expectedWatermarks(result.getContext.getLowWatermark) ==
          result.getContext.getHighWatermark)
      }
    }
  }

  ignore("memory is stable on large amount of partitions") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", IntegerType.INTEGER)

      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List("part").asJava)
      table.open()

      val sinkWriter = new DeltaSinkWriter.Builder()
        .withJobId("test-job")
        .withSubtaskId(0)
        .withAttemptNumber(1)
        .withDeltaTable(table)
        .withConf(new DeltaSinkConf(schema, Map.empty[String, String].asJava))
        .withMetricGroup(UnregisteredMetricsGroup.createSinkWriterMetricGroup())
        .build()

      val logger = LoggerFactory.getLogger(classOf[DeltaSinkWriter])
      val threadPool = Executors.newFixedThreadPool(2)
      threadPool.submit(new Runnable {
        override def run(): Unit = {
          val mxBean = ManagementFactory.getMemoryMXBean
          var counter = 0
          while (!Thread.currentThread().isInterrupted) {
            Thread.sleep(5000)
            counter += 1
            if (counter % 100 == 0) {
              System.gc()
            }
            val usage = mxBean.getHeapMemoryUsage
            logger.info("{}, {}", usage.getUsed, usage.getMax)
          }
        }
      })

      val rowCount = 2000000
      val startTime = System.currentTimeMillis()
      var lastBegin = -1L
      for (i <- 0 until rowCount) {
        val begin = System.currentTimeMillis() - startTime
        if (lastBegin / 300000 != begin / 300000) {
          logger.info("Clearing up buffer")
          sinkWriter.prepareCommit()
        }
        lastBegin = begin

        sinkWriter.write(
          GenericRowData.of(i, random.nextInt(1000000)),
          new TestSinkWriterContext(i * 100, i * 100))
      }
    }
  }
}
