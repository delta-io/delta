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

import java.net.URI

import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsJava, SeqHasAsJava}

import io.delta.flink.TestHelper
import io.delta.flink.table.HadoopTable
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.types.{IntegerType, StringType, StructType}

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup
import org.apache.flink.table.data.{GenericRowData, StringData}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

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
      // Three partitions
      assert(3 == results.size())
      // Each partition has one action
      results.asScala.foreach { result =>
        assert(1 == result.getDeltaActions.size)
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

      val sinkWriter = new DeltaSinkWriter.Builder()
        .withDeltaTable(table)
        .withJobId("test-job")
        .withSubtaskId(0)
        .withAttemptNumber(1)
        .withConf(new DeltaSinkConf(schema, Map.empty[String, String].asJava))
        .withMetricGroup(UnregisteredMetricsGroup.createSinkWriterMetricGroup())
        .build()

      for (i <- 0 until 20) {
        sinkWriter.write(
          GenericRowData.of(i, StringData.fromString("p" + (i % 3))),
          new TestSinkWriterContext(i * 100, i * 100))
      }
      val results = sinkWriter.prepareCommit()
      // Three partitions
      assert(3 == results.size())
      // Each partition has one action
      results.asScala.foreach { result =>
        assert(1 == result.getDeltaActions.size)
      }
    }
  }
}
