package io.delta.flink

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path}
import java.util.{Collections, Optional, UUID}

import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsJava, SeqHasAsJava}
import scala.util.Random

import io.delta.kernel.{Operation, Table}
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.internal.actions.{AddFile, SingleAction}
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.statistics.DataFileStatistics
import io.delta.kernel.types._
import io.delta.kernel.utils.{CloseableIterable, DataFileStatus, FileStatus}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.shaded.org.apache.commons.io.FileUtils

trait TestHelper {

  protected def withTempDir(f: File => Unit): Unit = {
    val tempDir = Files.createTempDirectory(UUID.randomUUID().toString).toFile
    try f(tempDir)
    finally {
      FileUtils.deleteDirectory(tempDir)
    }
  }

  protected def dummyRow(): Row = {
    val id = Random.nextInt(1048576)
    val map: Map[Integer, Object] = Map(Integer.valueOf(0) -> Integer.valueOf(id))
    new GenericRow(new StructType().add("id", IntegerType.INTEGER), map.asJava)
  }

  def dummyStatistics(numRecords: Long): DataFileStatistics =
    new DataFileStatistics(
      numRecords,
      Map.empty[Column, Literal].asJava,
      Map.empty[Column, Literal].asJava,
      Map.empty[Column, java.lang.Long].asJava,
      Optional.empty)

  def dummyAddFileRow(
      schema: StructType,
      numRows: Long,
      partitionValues: Map[String, Literal]): Row = {
    val addFileRow = AddFile.convertDataFileStatus(
      schema,
      URI.create("s3://abc/def"),
      new DataFileStatus(
        "s3://abc/def/" + UUID.randomUUID().toString,
        1000L,
        2000L,
        Optional.of(dummyStatistics(numRows))),
      partitionValues.asJava,
      /* dataChange= */ true,
      /* tags= */ Collections.emptyMap,
      /* baseRowId= */ Optional.empty,
      /* defaultRowCommitVersion= */ Optional.empty,
      /* deletionVectorDescriptor= */ Optional.empty)
    SingleAction.createAddFileSingleAction(addFileRow.toRow)
  }

  protected def dummyWriterContext(
      engine: Engine,
      tablePath: String,
      schema: StructType,
      partitionCols: Seq[String] = Seq.empty): Row = {
    val table = Table.forPath(engine, tablePath)
//    try {
//      table.getLatestSnapshot(engine);
//      val txn = table.createTransactionBuilder(engine, "dummyEngine", Operation.MANUAL_UPDATE)
//        .build(engine)
//      txn.getTransactionState(engine)
//    } catch {
//      case e: TableNotFoundException =>
    val txn = table.createTransactionBuilder(engine, "dummyEngine", Operation.CREATE_TABLE)
      .withSchema(engine, schema)
      .withPartitionColumns(engine, partitionCols.toList.asJava)
      .build(engine)
    txn.getTransactionState(engine)

  }

  protected def createNonEmptyTable(
      engine: Engine,
      tablePath: String,
      schema: StructType,
      partitionCols: Seq[String] = Seq.empty): Table = {
    val table = Table.forPath(engine, tablePath)
    val txn = table.createTransactionBuilder(engine, "dummyEngine", Operation.CREATE_TABLE)
      .withSchema(engine, schema)
      .withPartitionColumns(engine, partitionCols.toList.asJava)
      .build(engine)

    val partitionMap = partitionCols.map { colName =>
      (colName, dummyRandomLiteral(schema.get(colName).getDataType))
    }.toMap.asJava

    // Prepare some dummy AddFile
    val dummyAddFile = AddFile.convertDataFileStatus(
      schema,
      URI.create(table.getPath(engine)),
      new DataFileStatus("abcdef", 1000L, 2000L, Optional.empty),
      partitionMap,
      true,
      Map.empty[String, String].asJava,
      Optional.empty(),
      Optional.empty(),
      Optional.empty())
    txn.commit(
      engine,
      CloseableIterable
        .inMemoryIterable(Utils.singletonCloseableIterator(
          SingleAction.createAddFileSingleAction(dummyAddFile.toRow))))
    table
  }

  protected def readParquet(filePath: Path, schema: StructType): Seq[Row] = {
    val fileStatus = FileStatus.of(
      filePath.toString,
      Files.size(filePath),
      Files.getLastModifiedTime(filePath).toMillis)

    val results = DefaultEngine.create(new Configuration())
      .getParquetHandler.readParquetFiles(
        Utils.singletonCloseableIterator(fileStatus),
        schema,
        Optional.empty())
    assert(results.hasNext)
    val result = results.next()
    result.getData.getRows.toInMemoryList.asScala.toSeq
  }

  val random = new Random(System.currentTimeMillis())
  protected def dummyRandomLiteral(dataType: DataType): Literal = {
    dataType match {
      case IntegerType.INTEGER =>
        Literal.ofInt(random.nextInt())
      case StringType.STRING =>
        Literal.ofString("p" + random.nextInt())
      case LongType.LONG =>
        Literal.ofLong(random.nextLong())
      case _ => throw new UnsupportedOperationException
    }
  }
}
