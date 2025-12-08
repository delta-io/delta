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

package io.delta.flink

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
import io.delta.kernel.{Operation, Snapshot, Table}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.shaded.org.apache.commons.io.FileUtils

import java.io._
import java.net.URI
import java.nio.file.{Files, Path}
import java.util.{Collections, Optional, UUID}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsJava, SeqHasAsJava}
import scala.util.Random

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
      new DataFileStatus(UUID.randomUUID().toString, 1000L, 2000L, Optional.empty),
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

  // Make a random write to an existing table
  protected def writeTable(
      engine: Engine,
      tablePath: String,
      schema: StructType,
      partitionCols: Seq[String] = Seq.empty): Optional[Snapshot] = {
    val partitionMap = partitionCols.map { colName =>
      (colName, dummyRandomLiteral(schema.get(colName).getDataType))
    }.toMap.asJava
    // Prepare some dummy AddFile
    val dummyAddFile = AddFile.convertDataFileStatus(
      schema,
      URI.create(tablePath),
      new DataFileStatus(UUID.randomUUID().toString, 1000L, 2000L, Optional.empty),
      partitionMap,
      true,
      Map.empty[String, String].asJava,
      Optional.empty(),
      Optional.empty(),
      Optional.empty())
    val table = Table.forPath(engine, tablePath)
    val txn = table.createTransactionBuilder(engine, "dummyEngine", Operation.WRITE)
      .build(engine)
    txn.commit(
      engine,
      CloseableIterable
        .inMemoryIterable(Utils.singletonCloseableIterator(
          SingleAction.createAddFileSingleAction(dummyAddFile.toRow)))).getPostCommitSnapshot
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

  protected def checkSerializability(input: Object): Unit = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(input)
    oos.close()
    val bytes = baos.toByteArray
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val restored = ois.readObject()
    ois.close()

    assert(restored.getClass == input.getClass)
  }
}
