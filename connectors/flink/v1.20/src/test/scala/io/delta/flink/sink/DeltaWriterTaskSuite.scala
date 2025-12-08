package io.delta.flink.sink

import java.nio.file.Files

import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}

import io.delta.flink.TestHelper
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
      val engine = DefaultEngine.create(new Configuration())
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = Table.forPath(engine, dir.toString)
      val txn = table.createTransactionBuilder(engine, "dummyEngine", Operation.CREATE_TABLE)
        .withSchema(engine, schema)
        .withPartitionColumns(engine, Seq("part").toList.asJava)
        .build(engine)
      val writerContext = txn.getTransactionState(engine)

      val partitionValues = Map("part" -> Literal.ofString("p0")).asJava

      val writerTask = new DeltaWriterTask(
        /* jobId= */ "test-job-id",
        /* subtaskId= */ 2,
        /* attemptNumber= */ 0,
        /* engine= */ engine,
        /* partitionValues= */ partitionValues,
        /* writeContext= */ writerContext)

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
      val engine = DefaultEngine.create(new Configuration())
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = createNonEmptyTable(engine, dir.toString, schema, Seq("part"))
      val txn = table.createTransactionBuilder(engine, "xxx", Operation.MANUAL_UPDATE).build(engine)
      val writerContext = txn.getTransactionState(engine)
      val partitionValues = Map("part" -> Literal.ofString("p0")).asJava

      val writerTask = new DeltaWriterTask(
        /* jobId= */ "test-job-id",
        /* subtaskId= */ 2,
        /* attemptNumber= */ 0,
        /* engine= */ engine,
        /* partitionValues= */ partitionValues,
        /* writeContext= */ writerContext)

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
