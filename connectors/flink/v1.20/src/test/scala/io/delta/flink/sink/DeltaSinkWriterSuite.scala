package io.delta.flink.sink

import scala.jdk.CollectionConverters.CollectionHasAsScala

import io.delta.flink.TestHelper
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.types.{IntegerType, StringType, StructType}

import org.apache.flink.table.data.{GenericRowData, StringData}
import org.apache.flink.table.types.logical.{IntType, RowType, VarCharType}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class DeltaSinkWriterSuite extends AnyFunSuite with TestHelper {

  test("write to empty table using multiple partitions") {
    withTempDir { dir =>
      val engine = DefaultEngine.create(new Configuration())
      val flinkSchema = RowType.of(
        Array(new IntType(), new VarCharType(VarCharType.MAX_LENGTH)),
        Array[String]("id", "part"))
      val deltaSchema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)
      val wc = dummyWriterContext(engine, dir.getPath, deltaSchema, Seq("part"))

      val sinkWriter = new DeltaSinkWriter.Builder()
        .withEngine(engine)
        .withFlinkSchema(flinkSchema)
        .withJobId("test-job")
        .withSubtaskId(0)
        .withAttemptNumber(1)
        .withWriterContext(wc)
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
      val engine = DefaultEngine.create(new Configuration())
      val flinkSchema = RowType.of(
        Array(new IntType(), new VarCharType(VarCharType.MAX_LENGTH)),
        Array[String]("id", "part"))
      val deltaSchema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)
      val wc = dummyWriterContext(engine, dir.getPath, deltaSchema, Seq("part"))

      // Create a non-empty table
      createNonEmptyTable(engine, dir.getPath, deltaSchema, Seq("part"))

      val sinkWriter = new DeltaSinkWriter.Builder()
        .withEngine(engine)
        .withFlinkSchema(flinkSchema)
        .withJobId("test-job")
        .withSubtaskId(0)
        .withAttemptNumber(1)
        .withWriterContext(wc)
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
