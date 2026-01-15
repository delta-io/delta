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

import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

import io.delta.flink.TestHelper
import io.delta.flink.table.HadoopTable
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.types.{IntegerType, StringType, StructType}

import org.apache.flink.api.connector.sink2.Committer
import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest
import org.apache.flink.runtime.metrics.groups.{InternalSinkCommitterMetricGroup, UnregisteredMetricGroups}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class DeltaCommitterSuite extends AnyFunSuite with TestHelper {

  val metricGroup = InternalSinkCommitterMetricGroup.wrap(
    UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup)

  test("commit with single checkpoint to an empty table") {
    withTempDir { dir =>
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTable(
        dir.toURI,
        Map.empty[String, String].asJava,
        schema,
        List("part").asJava)
      table.open()

      val committer = new DeltaCommitter.Builder()
        .withDeltaTable(table)
        .withJobId("test-job")
        .withMetricGroup(metricGroup)
        .withConf(new DeltaSinkConf(schema, Map.empty[String, String].asJava))
        .build()

      // By the way we direct the stream traffic, we should receive only one committable.
      val commitMessages: List[Committer.CommitRequest[DeltaCommittable]] = Seq({
        val actions = (0 until 5).map { i =>
          dummyAddFileRow(schema, 10 + i, Map("part" -> Literal.ofString("p" + i)))
        }.toList.asJava
        val committable = new DeltaCommittable(
          "test-job",
          "test-opr",
          1000L,
          actions,
          new WriterResultContext())
        new MockCommitRequest(committable)
      }).toList

      committer.commit(commitMessages.asJava)

      // The target table should have one version
      verifyTableContent(
        dir.toString,
        (version, actions, _) => {
          assert(0L == version)
          // There should be 5 files to scan
          assert(5 == actions.size)
          assert(Set("p0", "p1", "p2", "p3", "p4") ==
            actions.map(_.getPartitionValues.getValues.getString(0)).toSet)
          assert(60 == actions.map(_.getNumRecords.get.longValue()).sum)
        })
    }
  }

  test("commit with multiple checkpoints") {
    withTempDir { dir =>
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTable(
        dir.toPath.toUri,
        Map.empty[String, String].asJava,
        schema,
        List("part").asJava)
      table.open()

      val committer = new DeltaCommitter.Builder()
        .withDeltaTable(table)
        .withJobId("test-job")
        .withMetricGroup(metricGroup)
        .withConf(new DeltaSinkConf(schema, Map.empty[String, String].asJava))
        .build()

      // Three checkpoints, each contains 5 add files
      val commitMessages: List[Committer.CommitRequest[DeltaCommittable]] =
        (0 until 3).map { i =>
          val actions = (0 until 5).map { j =>
            dummyAddFileRow(schema, j + 10, Map("part" -> Literal.ofString("p" + j)))
          }.toList.asJava
          val committable = new DeltaCommittable(
            "test-job",
            "test-opr",
            i,
            actions,
            new WriterResultContext())
          new MockCommitRequest(committable)
        }.toList

      committer.commit(commitMessages.asJava)

      verifyTableContent(
        dir.toString,
        (version, actions, _) => {
          assert(2L == version)
          // There should be 5 files to scan
          assert(15 == actions.size)
          assert(Set("p0", "p1", "p2", "p3", "p4") ==
            actions.map(_.getPartitionValues.getValues.getString(0)).toSet)
          assert(180 == actions.map(_.getNumRecords.get.longValue()).sum)
        })
    }
  }

  test("commit with no schema evolution policy") {
    withTempDir { dir =>
      val engine = DefaultEngine.create(new Configuration())
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)
      val anotherSchema = new StructType()
        .add("v1", StringType.STRING)
        .add("v2", StringType.STRING)

      val table = new HadoopTable(
        dir.toURI,
        Map.empty[String, String].asJava,
        schema,
        List("part").asJava)
      table.open()

      createNonEmptyTable(engine, dir.getAbsolutePath, anotherSchema, Seq("v1"))

      val committer = new DeltaCommitter.Builder()
        .withDeltaTable(table)
        .withJobId("test-job")
        .withMetricGroup(metricGroup)
        .withConf(new DeltaSinkConf(schema, Map.empty[String, String].asJava))
        .build()

      val commitMessages: List[Committer.CommitRequest[DeltaCommittable]] = Seq({
        val actions = (0 until 5).map { i =>
          dummyAddFileRow(schema, i + 10, Map("part" -> Literal.ofString("p" + i)))
        }.toList.asJava
        val committable = new DeltaCommittable(
          "test-job",
          "test-opr",
          1000L,
          actions,
          new WriterResultContext())
        new MockCommitRequest(committable)
      }).toList

      val e = intercept[IllegalStateException] {
        committer.commit(commitMessages.asJava)
      }
      assert(e.getMessage.contains("Invalid schema evolution observed, aborting committing"))
    }
  }

  test("commit with newcolumn schema evolution policy") {
    withTempDir { dir =>
      val engine = DefaultEngine.create(new Configuration())
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)
      val anotherSchema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)
        .add("another", StringType.STRING)

      val table = new HadoopTable(
        dir.toURI,
        Map.empty[String, String].asJava,
        anotherSchema,
        List().asJava)
      table.open()

      createNonEmptyTable(engine, dir.getAbsolutePath, anotherSchema, Seq())

      val committer = new DeltaCommitter.Builder()
        .withDeltaTable(table)
        .withJobId("test-job")
        .withMetricGroup(metricGroup)
        .withConf(new DeltaSinkConf(
          schema,
          Map("schema-evolution-mode" -> "newcolumn").asJava))
        .build()

      val commitMessages: List[Committer.CommitRequest[DeltaCommittable]] = Seq({
        val actions = (0 until 5).map { i =>
          dummyAddFileRow(schema, i + 10, Map("part" -> Literal.ofString("p" + i)))
        }.toList.asJava
        val committable = new DeltaCommittable(
          "test-job",
          "test-opr",
          1000L,
          actions,
          new WriterResultContext())
        new MockCommitRequest(committable)
      }).toList

      committer.commit(commitMessages.asJava)
    }
  }

  test("commit writes watermarks") {
    withTempDir { dir =>
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTable(
        dir.toURI,
        Map.empty[String, String].asJava,
        schema,
        List("part").asJava)
      table.open()

      val committer = new DeltaCommitter.Builder()
        .withDeltaTable(table)
        .withJobId("test-job")
        .withMetricGroup(metricGroup)
        .withConf(new DeltaSinkConf(schema, Map.empty[String, String].asJava))
        .build()

      // By the way we direct the stream traffic, we should receive only one committable.
      val commitMessages: List[Committer.CommitRequest[DeltaCommittable]] = List({
        val actions = (0 until 5).map { i =>
          dummyAddFileRow(schema, 10 + i, Map("part" -> Literal.ofString("p" + i)))
        }.toList.asJava
        val committable = new DeltaCommittable(
          "test-job",
          "test-opr",
          1000L,
          actions,
          new WriterResultContext(200, 100))
        new MockCommitRequest(committable)
      })

      committer.commit(commitMessages.asJava)

      // The target table should have one version
      verifyTableContent(
        dir.toString,
        (version, actions, props) => {
          assert(0L == version)
          // There should be 5 files to scan
          assert(5 == actions.size)
          assert(Set("p0", "p1", "p2", "p3", "p4") ==
            actions.map(_.getPartitionValues.getValues.getString(0)).toSet)
          assert(60 == actions.map(_.getNumRecords.get.longValue()).sum)
          assert(Map("flink.high-watermark" -> "200", "flink.low-watermark" -> "100")
            .asJava == props)
        })
    }
  }
}
