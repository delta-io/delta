/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.utils

import java.nio.{ByteBuffer, ByteOrder}
import java.util.{Collections, Optional}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.{Snapshot, Transaction}
import io.delta.kernel.data.{ColumnVector, Row}
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.{Column, Literal, StGeometryBoxesIntersect}
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.statistics.DataFileStatistics
import io.delta.kernel.types.{DataType, IntegerType, StructType}
import io.delta.kernel.utils.{CloseableIterable, DataFileStatus}
import io.delta.kernel.utils.CloseableIterable.inMemoryIterable

/** Mixin for geospatial test fixtures: WKB/WKT helpers, stats injection, box-prune queries. */
trait GeoTestUtils extends AbstractWriteUtils {

  /** 21-byte little-endian WKB for POINT(x y). */
  def pointWkb(x: Double, y: Double): Array[Byte] = {
    val buf = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN)
    buf.put(1.toByte)
    buf.putInt(1)
    buf.putDouble(x)
    buf.putDouble(y)
    buf.array()
  }

  def pointWktLiteral(x: Double, y: Double, geoType: DataType): Literal =
    Literal.ofGeospatialWKT(s"POINT ($x $y)", geoType)

  def geoColumnVector(
      geoType: DataType,
      values: Seq[Option[Array[Byte]]]): ColumnVector = new ColumnVector {
    override def getDataType: DataType = geoType
    override def getSize: Int = values.length
    override def close(): Unit = {}
    override def isNullAt(rowId: Int): Boolean = values(rowId).isEmpty
    override def getBinary(rowId: Int): Array[Byte] = values(rowId).orNull
  }

  def intColumnVector(values: Seq[Int]): ColumnVector = new ColumnVector {
    override def getDataType: DataType = IntegerType.INTEGER
    override def getSize: Int = values.length
    override def close(): Unit = {}
    override def isNullAt(rowId: Int): Boolean = false
    override def getInt(rowId: Int): Int = values(rowId)
  }

  def geoStats(
      geomCol: Column,
      minX: Double,
      minY: Double,
      maxX: Double,
      maxY: Double,
      geoType: DataType,
      numRecords: Long = 10L): DataFileStatistics = new DataFileStatistics(
    numRecords,
    Map(geomCol -> pointWktLiteral(minX, minY, geoType)).asJava,
    Map(geomCol -> pointWktLiteral(maxX, maxY, geoType)).asJava,
    Map(geomCol -> (0L: java.lang.Long)).asJava,
    Optional.empty())

  /** Stats with no min/max recorded - data skipping must always retain such files. */
  def emptyStats(numRecords: Long = 10L): DataFileStatistics = new DataFileStatistics(
    numRecords,
    Collections.emptyMap(),
    Collections.emptyMap(),
    Collections.emptyMap(),
    Optional.empty())

  def stats(
      minValues: Map[Column, Literal],
      maxValues: Map[Column, Literal],
      nullCounts: Map[Column, java.lang.Long] = Map.empty,
      numRecords: Long = 10L): DataFileStatistics = new DataFileStatistics(
    numRecords,
    minValues.asJava,
    maxValues.asJava,
    nullCounts.asJava,
    Optional.empty())

  /** Stages a synthetic add file (no real Parquet bytes) at part-{fileIdx}.parquet. */
  def appendActionsForGeoStatsFile(
      engine: Engine,
      txn: Transaction,
      fileIdx: Int,
      stats: DataFileStatistics,
      fileSize: Long = 1000L): CloseableIterable[Row] = {
    val txnState = txn.getTransactionState(engine)
    val writeContext = Transaction.getWriteContext(engine, txnState, Collections.emptyMap())
    val filePath = engine.getFileSystemClient.resolvePath(
      writeContext.getTargetDirectory + s"/part-$fileIdx.parquet")
    val fileStatus = new DataFileStatus(filePath, fileSize, 0L, Optional.of(stats))
    val actions = Transaction.generateAppendActions(
      engine,
      txnState,
      toCloseableIterator(Seq(fileStatus).iterator.asJava),
      writeContext)
    inMemoryIterable(actions)
  }

  /** One commit per stats entry; first creates the table, rest update. */
  def commitGeoStatsFiles(
      tablePath: String,
      engine: Engine,
      schema: StructType,
      stats: Seq[DataFileStatistics],
      tableProperties: Map[String, String] = Map.empty): Unit = stats.zipWithIndex.foreach {
    case (s, idx) =>
      val txn = if (idx == 0) {
        getCreateTxn(engine, tablePath, schema, tableProperties = tableProperties)
      } else {
        getUpdateTxn(engine, tablePath)
      }
      commitTransaction(txn, engine, appendActionsForGeoStatsFile(engine, txn, idx, s))
  }

  /** Count of scan files left after applying StGeometryBoxesIntersect on a query bbox. */
  def boxFilesHit(
      snapshot: Snapshot,
      geomCol: Column,
      geoType: DataType,
      qMinX: Double,
      qMinY: Double,
      qMaxX: Double,
      qMaxY: Double): Int = {
    val pred = new StGeometryBoxesIntersect(
      geomCol,
      pointWktLiteral(qMinX, qMinY, geoType),
      pointWktLiteral(qMaxX, qMaxY, geoType))
    collectScanFileRows(snapshot.getScanBuilder().withFilter(pred).build()).size
  }
}
