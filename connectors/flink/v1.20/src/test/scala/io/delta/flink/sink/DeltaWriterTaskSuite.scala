package io.delta.flink.sink

import java.net.URI
import java.nio.file.Files

import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

import io.delta.flink.TestHelper
import io.delta.flink.table.HadoopTable
import io.delta.kernel.{Operation, Table}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.actions.{AddFile, SingleAction}
import io.delta.kernel.types.{IntegerType, StringType, StructType}

import org.apache.flink.table.data.{GenericRowData, StringData}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class DeltaWriterTaskSuite extends AnyFunSuite with TestHelper {

  test("write to empty table") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTable(URI.create(tablePath), schema, List("part").asJava)
      val partitionValues = Map("part" -> Literal.ofString("p0")).asJava

      val writerTask = new DeltaWriterTask(
        /* jobId= */ "test-job-id",
        /* subtaskId= */ 2,
        /* attemptNumber= */ 0,
        /* table = */ table,
        /* partitionValues= */ partitionValues)

      for (i <- 0 until 10) {
        writerTask.write(GenericRowData.of(i, StringData.fromString("p0")), null)
      }
      val results = writerTask.complete()

      assert(1 == results.size())
      results.forEach(result => {
        assert(1 == result.getDeltaActions.size())
        val action = result.getDeltaActions.get(0)
        val addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL))
        assert(addFile.getPath.contains("test-job-id-2-0"))
        // Stats are present
        assert(10 == addFile.getNumRecords.get())
        val fullPath = dir.toPath.resolve(addFile.getPath).toAbsolutePath
        assert(Files.exists(fullPath))

        val partitionMap = addFile.getPartitionValues
        assert(1 == partitionMap.getSize)
        assert("part" == partitionMap.getKeys.getString(0))
        assert("p0" == partitionMap.getValues.getString(0))

        // check the Parquet file content
        val rows = readParquet(fullPath, schema)
        assert(10 == rows.size)
        rows.zipWithIndex.iterator.foreach { case (row, idx) =>
          assert(idx == row.getInt(0))
        }
      })
    }
  }

  test("write to existing table") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTable(URI.create(tablePath), schema, List("part").asJava)
      val partitionValues = Map("part" -> Literal.ofString("p0")).asJava

      val writerTask = new DeltaWriterTask(
        /* jobId= */ "test-job-id",
        /* subtaskId= */ 2,
        /* attemptNumber= */ 0,
        /* table = */ table,
        /* partitionValues= */ partitionValues)

      for (i <- 0 until 10) {
        writerTask.write(GenericRowData.of(i, StringData.fromString("p0")), null)
      }
      val results = writerTask.complete()

      assert(1 == results.size())
      results.forEach(result => {
        assert(1 == result.getDeltaActions.size())
        val action = result.getDeltaActions.get(0)
        val addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL))
        assert(addFile.getPath.contains("test-job-id-2-0"))
        assert(10 == addFile.getNumRecords.get())
        val fullPath = dir.toPath.resolve(addFile.getPath).toAbsolutePath
        assert(Files.exists(fullPath))

        val partitionMap = addFile.getPartitionValues
        assert(1 == partitionMap.getSize)
        assert("part" == partitionMap.getKeys.getString(0))
        assert("p0" == partitionMap.getValues.getString(0))

        // check the Parquet file content
        val rows = readParquet(fullPath, schema)
        assert(10 == rows.size)
        rows.zipWithIndex.iterator.foreach { case (row, idx) =>
          assert(idx == row.getInt(0))
        }
      })
    }
  }
}
