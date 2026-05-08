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

/**
 * Shared helpers for geospatial tests. Mix into any AnyFunSuite that already mixes in
 * AbstractWriteUtils. Provides:
 *
 *   - WKB and WKT literal construction for synthetic geometry/geography points
 *   - Column vectors carrying WKB binary or scalar int data
 *   - DataFileStatistics with min/max bounding-box literals
 *   - The boilerplate to stage a synthetic add file (no real Parquet write) and to
 *     commit one such file per stats entry against a given table
 *   - A box-intersect file-pruning shorthand that returns the count of scan files a
 *     query bounding box leaves after data skipping
 *
 * Suites that exercise the data-skipping path inject stats directly via
 * appendActionsForGeoStatsFile rather than going through the Parquet writer; that
 * keeps the test fixtures deterministic and decouples the predicate behavior from
 * the write pipeline.
 */
trait GeoTestUtils extends AbstractWriteUtils {

  /**
   * Builds a 21-byte little-endian WKB encoding for POINT(x y):
   * byteOrder(1) + type=1(4) + x(8) + y(8).
   */
  def pointWkb(x: Double, y: Double): Array[Byte] = {
    val buf = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN)
    buf.put(1.toByte)
    buf.putInt(1)
    buf.putDouble(x)
    buf.putDouble(y)
    buf.array()
  }

  /** WKT POINT literal of the given type, e.g. for stats min/max or query bounds. */
  def pointWktLiteral(x: Double, y: Double, geoType: DataType): Literal =
    Literal.ofGeospatialWKT(s"POINT ($x $y)", geoType)

  /**
   * ColumnVector backed by an in-memory Seq of WKB byte arrays. None entries become
   * null rows; Some entries return their bytes via getBinary.
   */
  def geoColumnVector(
      geoType: DataType,
      values: Seq[Option[Array[Byte]]]): ColumnVector = new ColumnVector {
    override def getDataType: DataType = geoType
    override def getSize: Int = values.length
    override def close(): Unit = {}
    override def isNullAt(rowId: Int): Boolean = values(rowId).isEmpty
    override def getBinary(rowId: Int): Array[Byte] = values(rowId).orNull
  }

  /** Trivial non-null IntegerType vector. */
  def intColumnVector(values: Seq[Int]): ColumnVector = new ColumnVector {
    override def getDataType: DataType = IntegerType.INTEGER
    override def getSize: Int = values.length
    override def close(): Unit = {}
    override def isNullAt(rowId: Int): Boolean = false
    override def getInt(rowId: Int): Int = values(rowId)
  }

  /**
   * DataFileStatistics with a single geo column's min/max set to POINT WKT literals
   * for the given bounding box, and null count = 0.
   */
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

  /**
   * DataFileStatistics with no min/max recorded - the data-skipping path treats
   * this file as "always include" because the predicate cannot prove non-intersection.
   */
  def emptyStats(numRecords: Long = 10L): DataFileStatistics = new DataFileStatistics(
    numRecords,
    Collections.emptyMap(),
    Collections.emptyMap(),
    Collections.emptyMap(),
    Optional.empty())

  /**
   * General-purpose DataFileStatistics builder for multi-column stats. Use when a
   * single file needs min/max recorded for several columns (e.g. a geo column AND
   * an int column to test combined predicates).
   */
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

  /**
   * Stages a synthetic add file at <txn-target-dir>/part-{fileIdx}.parquet with the
   * provided stats (no real Parquet bytes are written), returning the actions that
   * the caller should hand to commitTransaction. fileSize is non-zero so the add
   * action looks plausible to downstream consumers.
   */
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

  /**
   * Commits one synthetic add file per stats entry, creating the table on the first
   * commit and updating it on subsequent ones. Each entry becomes its own commit so
   * tests can reason about per-file pruning post-checkpoint.
   */
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

  /**
   * Returns the count of scan files a snapshot leaves after applying an
   * StGeometryBoxesIntersect predicate against the given column with the given query
   * bounding box. Files with missing geo stats fall through and count toward the result.
   */
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
