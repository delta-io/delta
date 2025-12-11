package io.delta.flink.sink

import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}
import io.delta.flink.TestHelper
import io.delta.flink.table.LocalKernelTable
import io.delta.kernel.CommitRangeBuilder.CommitBoundary
import io.delta.kernel.{Table, TableManager}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.ScanImpl
import io.delta.kernel.internal.actions.AddFile
import io.delta.kernel.types.{IntegerType, StringType, StructType}
import org.apache.flink.api.connector.sink2.Committer
import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup
import org.apache.flink.runtime.metrics.groups.{InternalSinkCommitterMetricGroup, UnregisteredMetricGroups}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

import java.net.URI
import java.nio.file.Paths

class DeltaCommitterSuite extends AnyFunSuite with TestHelper {

  val metricGroup = InternalSinkCommitterMetricGroup.wrap(
    UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup)

  test("commit with single checkpoint to an empty table") {
    withTempDir { dir =>
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new LocalKernelTable(dir.toURI, schema, List("part").asJava)

      val committer = new DeltaCommitter.Builder()
        .withDeltaTable(table)
        .withJobId("test-job")
        .withMetricGroup(metricGroup)
        .build()

      // By the way we direct the stream traffic, we should receive only one committable.
      val commitMessages: List[Committer.CommitRequest[DeltaCommittable]] = Seq({
        val actions = (0 until 5).map { i =>
          dummyAddFileRow(schema, 10 + i, Map("part" -> Literal.ofString("p" + i)))
        }.toList.asJava
        val committable = new DeltaCommittable("test-job", "test-opr", 1000L, actions)
        new MockCommitRequest(committable)
      }).toList

      committer.commit(commitMessages.asJava)

      // The target table should have one version
      val engine = DefaultEngine.create(new Configuration())
      val snapshot = TableManager.loadSnapshot(dir.toString).build(engine)
      assert(0L == snapshot.getVersion)
      val filesList = snapshot.getScanBuilder.build().asInstanceOf[ScanImpl]
        .getScanFiles(engine, true).toInMemoryList
      val actions = filesList.get(0).getRows
        .toInMemoryList.asScala.map(row => new AddFile(row.getStruct(0)))

      // There should be 5 files to scan
      assert(5 == actions.size)
      assert(Set("p0", "p1", "p2", "p3", "p4") ==
        actions.map(_.getPartitionValues.getValues.getString(0)).toSet)
      assert(60 == actions.map(_.getNumRecords.get.longValue()).sum)
    }
  }

  test("commit with multiple checkpoints") {
    withTempDir { dir =>
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new LocalKernelTable(dir.toPath.toUri, schema, List("part").asJava)

      val committer = new DeltaCommitter.Builder()
        .withDeltaTable(table)
        .withJobId("test-job")
        .withMetricGroup(metricGroup)
        .build()

      // Three checkpoints, each contains 5 add files
      val commitMessages: List[Committer.CommitRequest[DeltaCommittable]] =
        (0 until 3).map { i =>
          val actions = (0 until 5).map { j =>
            dummyAddFileRow(schema, j + 10, Map("part" -> Literal.ofString("p" + j)))
          }.toList.asJava
          val committable = new DeltaCommittable("test-job", "test-opr", i, actions)
          new MockCommitRequest(committable)
        }.toList

      committer.commit(commitMessages.asJava)

      // The target table should have 3 version
      val engine = DefaultEngine.create(new Configuration())
      val snapshot = TableManager.loadSnapshot(dir.toString).build(engine)
      assert(2L == snapshot.getVersion)

      for (version <- 0 to 2) {
        val filesList = TableManager.loadSnapshot(dir.toString).atVersion(version)
          .build(engine)
          .getScanBuilder
          .build()
          .asInstanceOf[ScanImpl]
          .getScanFiles(engine, true).toInMemoryList
        val actions = filesList.get(0).getRows
          .toInMemoryList.asScala.map(row => new AddFile(row.getStruct(0)))

        // There should be 5 files to scan
        assert(5 == actions.size)
        assert(Set("p0", "p1", "p2", "p3", "p4") ==
          actions.map(_.getPartitionValues.getValues.getString(0)).toSet)
        assert(60 == actions.map(_.getNumRecords.get.longValue()).sum)
      }
    }
  }

  test("commit to an existing table with different schema will fail") {
    withTempDir { dir =>
        val engine = DefaultEngine.create(new Configuration())
        val schema = new StructType()
          .add("id", IntegerType.INTEGER)
          .add("part", StringType.STRING)
        val anotherSchema = new StructType()
          .add("v1", StringType.STRING)
          .add("v2", StringType.STRING)

        val table = new LocalKernelTable(dir.toURI, schema, List("part").asJava)

        createNonEmptyTable(engine, dir.getAbsolutePath, anotherSchema, Seq("v1"))

        val committer = new DeltaCommitter.Builder()
          .withDeltaTable(table)
          .withJobId("test-job")
          .withMetricGroup(metricGroup)
          .build()

        val commitMessages: List[Committer.CommitRequest[DeltaCommittable]] = Seq({
          val actions = (0 until 5).map { i =>
            dummyAddFileRow(schema, i + 10, Map("part" -> Literal.ofString("p" + i)))
          }.toList.asJava
          val committable = new DeltaCommittable("test-job", "test-opr", 1000L, actions)
          new MockCommitRequest(committable)
        }).toList

        val e = intercept[IllegalArgumentException] {
          committer.commit(commitMessages.asJava)
        }
        assert(e.getMessage.contains("DeltaSink does not support schema evolution."))
      }
    }
}
