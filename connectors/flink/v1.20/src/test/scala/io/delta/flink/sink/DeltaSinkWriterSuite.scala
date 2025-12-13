package io.delta.flink.sink

import java.net.URI

import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

import io.delta.flink.TestHelper
import io.delta.flink.table.HadoopTable
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.types.{IntegerType, StringType, StructType}

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup
import org.apache.flink.table.data.{GenericRowData, StringData}
import org.apache.flink.table.types.logical.{IntType, RowType, VarCharType}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class DeltaSinkWriterSuite extends AnyFunSuite with TestHelper {

  test("write to empty table using multiple partitions") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTable(URI.create(tablePath), schema, List("part").asJava)

      val sinkWriter = new DeltaSinkWriter.Builder()
        .withJobId("test-job")
        .withSubtaskId(0)
        .withAttemptNumber(1)
        .withDeltaTable(table)
        .withMetricGroup(UnregisteredMetricsGroup.createSinkWriterMetricGroup())
        .build()

      for (i <- 0 until 20) {
        sinkWriter.write(GenericRowData.of(i, StringData.fromString("p" + (i % 3))), null)
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
      val table = new HadoopTable(URI.create(tablePath), schema, List("part").asJava)

      val sinkWriter = new DeltaSinkWriter.Builder()
        .withDeltaTable(table)
        .withJobId("test-job")
        .withSubtaskId(0)
        .withAttemptNumber(1)
        .withMetricGroup(UnregisteredMetricsGroup.createSinkWriterMetricGroup())
        .build()

      for (i <- 0 until 20) {
        sinkWriter.write(GenericRowData.of(i, StringData.fromString("p" + (i % 3))), null)
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
