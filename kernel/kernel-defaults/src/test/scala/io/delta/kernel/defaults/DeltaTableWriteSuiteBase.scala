/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults

import com.fasterxml.jackson.databind.ObjectMapper
import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.Operation.{CREATE_TABLE, WRITE}
import io.delta.kernel._
import io.delta.kernel.data.{ColumnarBatch, ColumnVector, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.util.FileNames.checkpointFileSingular
import io.delta.kernel.{Table, TransactionCommitResult}
import io.delta.kernel.exceptions._
import io.delta.kernel.expressions.Literal
import io.delta.kernel.expressions.Literal._
import io.delta.kernel.internal.util.Clock
import io.delta.kernel.internal.util.SchemaUtils.casePreservingPartitionColNames
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.internal.TableImpl
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}
import io.delta.kernel.utils.{CloseableIterable, CloseableIterator}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.VersionNotFoundException
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.Optional
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

/**
 * Common utility methods for write test suites.
 */
trait DeltaTableWriteSuiteBase extends AnyFunSuite with TestUtils {
  val OBJ_MAPPER = new ObjectMapper()
  val testEngineInfo = "test-engine"

  /** Test table schemas and test */
  val testSchema = new StructType().add("id", INTEGER)
  val dataBatches1 = generateData(testSchema, Seq.empty, Map.empty, 200, 3)
  val dataBatches2 = generateData(testSchema, Seq.empty, Map.empty, 400, 5)

  val testPartitionColumns = Seq("part1", "part2")
  val testPartitionSchema = new StructType()
    .add("id", INTEGER)
    .add("part1", INTEGER) // partition column
    .add("part2", INTEGER) // partition column

  val dataPartitionBatches1 = generateData(
    testPartitionSchema,
    testPartitionColumns,
    Map("part1" -> ofInt(1), "part2" -> ofInt(2)),
    batchSize = 237,
    numBatches = 3)

  val dataPartitionBatches2 = generateData(
    testPartitionSchema,
    testPartitionColumns,
    Map("part1" -> ofInt(4), "part2" -> ofInt(5)),
    batchSize = 876,
    numBatches = 7)
  def withTempDirAndEngine(f: (String, Engine) => Unit): Unit = {
    val engine = DefaultEngine.create(new Configuration() {
      {
        // Set the batch sizes to small so that we get to test the multiple batch/file scenarios.
        set("delta.kernel.default.parquet.reader.batch-size", "20");
        set("delta.kernel.default.json.reader.batch-size", "20");
        set("delta.kernel.default.parquet.writer.targetMaxFileSize", "20");
      }
    })
    withTempDir { dir => f(dir.getAbsolutePath, engine) }
  }

  def verifyLastCheckpointMetadata(tablePath: String, checkpointAt: Long, expSize: Long): Unit = {
    val filePath = f"$tablePath/_delta_log/_last_checkpoint"

    val source = scala.io.Source.fromFile(filePath)
    val result = try source.getLines().mkString(",") finally source.close()

    assert(result === s"""{"version":$checkpointAt,"size":$expSize}""")
  }

  /** Helper method to remove the delta files before the given version, to make sure the read is
   * using a checkpoint as base for state reconstruction.
   */
  def deleteDeltaFilesBefore(tablePath: String, beforeVersion: Long): Unit = {
    Seq.range(0, beforeVersion).foreach { version =>
      val filePath = new Path(f"$tablePath/_delta_log/$version%020d.json")
      new Path(tablePath).getFileSystem(new Configuration()).delete(filePath, false /* recursive */)
    }

    // try to query a version < beforeVersion
    val ex = intercept[VersionNotFoundException] {
      spark.read.format("delta").option("versionAsOf", beforeVersion - 1).load(tablePath)
    }
    assert(ex.getMessage().contains(
      s"Cannot time travel Delta table to version ${beforeVersion - 1}"))
  }

  def setCheckpointInterval(tablePath: String, interval: Int): Unit = {
    spark.sql(s"ALTER TABLE delta.`$tablePath` " +
      s"SET TBLPROPERTIES ('delta.checkpointInterval' = '$interval')")
  }

  def setInCommitTimestampsEnabled(tablePath: String, enabled: Boolean): Unit = {
    spark.sql(s"ALTER TABLE delta.`$tablePath` " +
      s"SET TBLPROPERTIES ('delta.enableInCommitTimestamps-preview' = '$enabled')")
  }

  def createTableWithInCommitTimestampsEnabled(tablePath: String, enabled: Boolean): Unit = {
    spark.sql(s"CREATE TABLE test (a INT, b STRING) USING delta CLUSTER BY (a) " +
      s"LOCATION '$tablePath' " +
      s"TBLPROPERTIES ('delta.enableInCommitTimestamps-preview' = '$enabled')")
  }

  def dataFileCount(tablePath: String): Int = {
    Files.walk(Paths.get(tablePath)).iterator().asScala
      .count(path => path.toString.endsWith(".parquet") && !path.toString.contains("_delta_log"))
  }

  def checkpointFilePath(tablePath: String, checkpointVersion: Long): String = {
    f"$tablePath/_delta_log/$checkpointVersion%020d.checkpoint.parquet"
  }

  def assertCheckpointExists(tablePath: String, atVersion: Long): Unit = {
    val cpPath = checkpointFilePath(tablePath, checkpointVersion = atVersion)
    assert(new File(cpPath).exists())
  }

  def copyTable(goldenTableName: String, targetLocation: String): Unit = {
    val source = new File(goldenTablePath(goldenTableName))
    val target = new File(targetLocation)
    FileUtils.copyDirectory(source, target)
  }

  def checkpointIfReady(
    engine: Engine,
    tablePath: String,
    result: TransactionCommitResult,
    expSize: Long): Unit = {
    if (result.isReadyForCheckpoint) {
      Table.forPath(engine, tablePath).checkpoint(engine, result.getVersion)
      verifyLastCheckpointMetadata(tablePath, checkpointAt = result.getVersion, expSize)
    }
  }

  def createWriteTxnBuilder(table: Table): TransactionBuilder = {
    table.createTransactionBuilder(defaultEngine, testEngineInfo, Operation.WRITE)
  }

  def createTestTxn(
    engine: Engine, tablePath: String, schema: Option[StructType] = None): Transaction = {
    val table = Table.forPath(engine, tablePath)
    var txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)
    schema.foreach(s => txnBuilder = txnBuilder.withSchema(engine, s))
    txnBuilder.build(engine)
  }

  def generateData(
    schema: StructType,
    partitionCols: Seq[String],
    partitionValues: Map[String, Literal],
    batchSize: Int,
    numBatches: Int): Seq[FilteredColumnarBatch] = {
    val partitionValuesSchemaCase =
      casePreservingPartitionColNames(partitionCols.asJava, partitionValues.asJava)

    var batches = Seq.empty[ColumnarBatch]
    for (_ <- 0 until numBatches) {
      var vectors = Seq.empty[ColumnVector]
      schema.fields().forEach { field =>
        val colType = field.getDataType
        val partValue = partitionValuesSchemaCase.get(field.getName)
        if (partValue != null) {
          // handle the partition column by inserting a vector with single value
          val vector = testSingleValueVector(colType, batchSize, partValue.getValue)
          vectors = vectors :+ vector
        } else {
          // handle the regular columns
          val vector = testColumnVector(batchSize, colType)
          vectors = vectors :+ vector
        }
      }
      batches = batches :+ new DefaultColumnarBatch(batchSize, schema, vectors.toArray)
    }
    batches.map(batch => new FilteredColumnarBatch(batch, Optional.empty()))
  }

  def stageData(
    state: Row,
    partitionValues: Map[String, Literal],
    data: Seq[FilteredColumnarBatch])
  : CloseableIterator[Row] = {
    val physicalDataIter = Transaction.transformLogicalData(
      defaultEngine,
      state,
      toCloseableIterator(data.toIterator.asJava),
      partitionValues.asJava)

    val writeContext = Transaction.getWriteContext(defaultEngine, state, partitionValues.asJava)

    val writeResultIter = defaultEngine
      .getParquetHandler
      .writeParquetFiles(
        writeContext.getTargetDirectory,
        physicalDataIter,
        writeContext.getStatisticsColumns)

    Transaction.generateAppendActions(defaultEngine, state, writeResultIter, writeContext)
  }

  def generateTxn(
    engine: Engine = defaultEngine,
    tablePath: String,
    isNewTable: Boolean = false,
    schema: StructType = null,
    partCols: Seq[String] = null,
    clock: Clock = () => System.currentTimeMillis,
    tableProperties: Map[String, String] = null): Transaction = {

    var txnBuilder = createWriteTxnBuilder(
      TableImpl.forPath(engine, tablePath, clock))

    if (isNewTable) {
      txnBuilder = txnBuilder.withSchema(engine, schema)
        .withPartitionColumns(engine, partCols.asJava)
    }

    if (tableProperties != null) {
      txnBuilder = txnBuilder.withTableProperties(engine, tableProperties.asJava)
    }

    txnBuilder.build(engine)
  }

  def commitAppendData(
    engine: Engine = defaultEngine,
    txn: Transaction,
    data: Seq[(Map[String, Literal], Seq[FilteredColumnarBatch])]): TransactionCommitResult = {

    val txnState = txn.getTransactionState(engine)

    val actions = data.map { case (partValues, partData) =>
      stageData(txnState, partValues, partData)
    }

    val combineActions = inMemoryIterable(actions.reduceLeft(_ combine _))
    txn.commit(engine, combineActions)
  }

  def appendData(
    engine: Engine = defaultEngine,
    tablePath: String,
    isNewTable: Boolean = false,
    schema: StructType = null,
    partCols: Seq[String] = null,
    data: Seq[(Map[String, Literal], Seq[FilteredColumnarBatch])],
    clock: Clock = () => System.currentTimeMillis,
    tableProperties: Map[String, String] = null): TransactionCommitResult = {

    val txn = generateTxn(engine, tablePath, isNewTable, schema, partCols, clock, tableProperties)
    commitAppendData(engine, txn, data)
  }
}
