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
package io.delta.kernel.defaults

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel._
import io.delta.kernel.defaults.utils.WriteUtils
import io.delta.kernel.expressions.{Column, Literal, StGeometryBoxesIntersect}
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.statistics.DataFileStatistics
import io.delta.kernel.types.{GeometryType, StructType}
import io.delta.kernel.utils.CloseableIterable.inMemoryIterable
import io.delta.kernel.utils.DataFileStatus

import org.scalatest.funsuite.AnyFunSuite

/**
 * End-to-end tests for data skipping with geometry column types using
 * the StGeometryBoxesIntersect predicate.
 *
 * Tests bypass the Parquet writer by injecting DataFileStatistics with
 * geometry min/max literals directly into the Delta log.
 */
class GeometryDataSkippingSuite extends AnyFunSuite with WriteUtils {

  // 4-quadrant layout:
  //    y=10 +----------+----------+
  //         | NW file2 | NE file1 |
  //    y=7  +          +          +
  //         | x:[0,3]  | x:[7,10] |
  //    y=3  +----------+----------+
  //         | SW file0 | SE file3 |
  //    y=0  +----------+----------+
  //         x=0   x=3  x=7   x=10
  private val fileExtents: Seq[(Double, Double, Double, Double)] = Seq(
    (0.0, 0.0, 3.0, 3.0), // SW - file 0
    (7.0, 7.0, 10.0, 10.0), // NE - file 1
    (0.0, 7.0, 3.0, 10.0), // NW - file 2
    (7.0, 0.0, 10.0, 3.0) // SE - file 3
  )

  private val colType = new GeometryType("OGC:CRS84")

  test("StGeometryBoxesIntersect data skipping on GeometryType column") {
    withTempDirAndEngine { (tablePath, engine) =>
      val schema = new StructType().add("geom", colType)
      writeFilesWithGeometryStats(
        tablePath,
        engine,
        schema,
        fileExtents)

      val snapshot = latestSnapshot(tablePath)

      def filesHit(
          qMinX: Double,
          qMinY: Double,
          qMaxX: Double,
          qMaxY: Double): Int = {
        val pred = new StGeometryBoxesIntersect(
          new Column("geom"),
          Literal.ofString(s"POINT ($qMinX $qMinY)"),
          Literal.ofString(s"POINT ($qMaxX $qMaxY)"))
        collectScanFileRows(
          snapshot.getScanBuilder().withFilter(pred).build()).size
      }

      // SW query - intersects only SW file
      assert(filesHit(1.0, 1.0, 4.0, 4.0) == 1)
      // NE query - intersects only NE file
      assert(filesHit(8.0, 8.0, 11.0, 11.0) == 1)
      // Center gap - intersects nothing
      assert(filesHit(4.0, 4.0, 6.0, 6.0) == 0)
      // West strip [1,1]-[4,9] - intersects SW and NW
      assert(filesHit(1.0, 1.0, 4.0, 9.0) == 2)
      // Top strip [0,8]-[11,11] - intersects NE and NW
      assert(filesHit(0.0, 8.0, 11.0, 11.0) == 2)
      // Global - intersects all 4 files
      assert(filesHit(0.0, 0.0, 11.0, 11.0) == 4)
    }
  }

  test("StGeometryBoxesIntersect: file with null geometry stats is never skipped") {
    withTempDirAndEngine { (tablePath, engine) =>
      val schema = new StructType().add("geom", colType)
      val geomCol = new Column("geom")

      // File 0: SW quadrant with geometry stats
      // File 1: no geometry stats -> must always be included
      val extentsOpt: Seq[Option[(Double, Double, Double, Double)]] =
        Seq(Some((0.0, 0.0, 3.0, 3.0)), None)

      extentsOpt.zipWithIndex.foreach {
        case (extentOpt, idx) =>
          val txn =
            if (idx == 0)
              getCreateTxn(engine, tablePath, schema)
            else getUpdateTxn(engine, tablePath)

          val txnState = txn.getTransactionState(engine)
          val writeContext = Transaction.getWriteContext(
            engine,
            txnState,
            Collections.emptyMap())

          val stats = extentOpt match {
            case Some((minX, minY, maxX, maxY)) =>
              new DataFileStatistics(
                10,
                Map(geomCol -> Literal.ofString(
                  s"POINT ($minX $minY)")).asJava,
                Map(geomCol -> Literal.ofString(
                  s"POINT ($maxX $maxY)")).asJava,
                Map(geomCol -> (0L: java.lang.Long)).asJava,
                Optional.empty())
            case None =>
              new DataFileStatistics(
                10,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Optional.empty())
          }

          val filePath =
            engine.getFileSystemClient.resolvePath(
              writeContext.getTargetDirectory + s"/part-$idx.parquet")
          val fileStatus = new DataFileStatus(
            filePath,
            1000,
            0L,
            Optional.of(stats))

          val actions = Transaction.generateAppendActions(
            engine,
            txnState,
            toCloseableIterator(Seq(fileStatus).iterator.asJava),
            writeContext)
          commitTransaction(
            txn,
            engine,
            inMemoryIterable(actions))
      }

      val snapshot = latestSnapshot(tablePath)

      def filesHit(
          qMinX: Double,
          qMinY: Double,
          qMaxX: Double,
          qMaxY: Double): Int = {
        val pred = new StGeometryBoxesIntersect(
          geomCol,
          Literal.ofString(s"POINT ($qMinX $qMinY)"),
          Literal.ofString(s"POINT ($qMaxX $qMaxY)"))
        collectScanFileRows(
          snapshot.getScanBuilder().withFilter(pred).build()).size
      }

      // NE query: SW file is provably non-overlapping, but
      // null-stats file must be included
      assert(filesHit(8.0, 8.0, 11.0, 11.0) == 1)
      // SW query: SW file matches, null-stats file also included
      assert(filesHit(1.0, 1.0, 4.0, 4.0) == 2)
    }
  }

  test("StGeometryBoxesIntersect combined with AND predicate on second column") {
    withTempDirAndEngine { (tablePath, engine) =>
      val schema = new StructType()
        .add("id", io.delta.kernel.types.IntegerType.INTEGER)
        .add("geom", new GeometryType("OGC:CRS84"))

      val idCol = new Column("id")
      val geomCol = new Column("geom")

      // File 0 (SW): id in [1,5],   geom bbox [0,0]-[3,3]
      // File 1 (NE): id in [10,20], geom bbox [7,7]-[10,10]
      val files = Seq(
        (1, 5, 0.0, 0.0, 3.0, 3.0),
        (10, 20, 7.0, 7.0, 10.0, 10.0))

      files.zipWithIndex.foreach {
        case ((idMin, idMax, gMinX, gMinY, gMaxX, gMaxY), idx) =>
          val txn =
            if (idx == 0)
              getCreateTxn(engine, tablePath, schema)
            else getUpdateTxn(engine, tablePath)

          val txnState = txn.getTransactionState(engine)
          val writeContext = Transaction.getWriteContext(
            engine,
            txnState,
            Collections.emptyMap())

          val stats = new DataFileStatistics(
            10,
            Map(
              idCol -> Literal.ofInt(idMin),
              geomCol -> Literal.ofString(
                s"POINT ($gMinX $gMinY)")).asJava,
            Map(
              idCol -> Literal.ofInt(idMax),
              geomCol -> Literal.ofString(
                s"POINT ($gMaxX $gMaxY)")).asJava,
            Map(
              idCol -> (0L: java.lang.Long),
              geomCol -> (0L: java.lang.Long)).asJava,
            Optional.empty())

          val filePath =
            engine.getFileSystemClient.resolvePath(
              writeContext.getTargetDirectory + s"/part-$idx.parquet")
          val fileStatus = new DataFileStatus(
            filePath,
            1000,
            0L,
            Optional.of(stats))

          val actions = Transaction.generateAppendActions(
            engine,
            txnState,
            toCloseableIterator(Seq(fileStatus).iterator.asJava),
            writeContext)
          commitTransaction(
            txn,
            engine,
            inMemoryIterable(actions))
      }

      val snapshot = latestSnapshot(tablePath)

      // SW geometry + id <= 5: only File 0 survives both filters
      val geoAndId = new io.delta.kernel.expressions.And(
        new StGeometryBoxesIntersect(
          geomCol,
          Literal.ofString("POINT (1.0 1.0)"),
          Literal.ofString("POINT (4.0 4.0)")),
        new io.delta.kernel.expressions.Predicate(
          "<=",
          idCol,
          Literal.ofInt(5)))

      val files1 = collectScanFileRows(
        snapshot.getScanBuilder().withFilter(geoAndId).build())
      assert(files1.size == 1)

      // NE geometry + id >= 15: only File 1 survives both filters
      val geoAndId2 = new io.delta.kernel.expressions.And(
        new StGeometryBoxesIntersect(
          geomCol,
          Literal.ofString("POINT (8.0 8.0)"),
          Literal.ofString("POINT (11.0 11.0)")),
        new io.delta.kernel.expressions.Predicate(
          ">=",
          idCol,
          Literal.ofInt(15)))

      val files2 = collectScanFileRows(
        snapshot.getScanBuilder().withFilter(geoAndId2).build())
      assert(files2.size == 1)

      // Center geometry + any id: geometry miss skips both files
      val geoCenterAny = new StGeometryBoxesIntersect(
        geomCol,
        Literal.ofString("POINT (4.0 4.0)"),
        Literal.ofString("POINT (6.0 6.0)"))

      val files3 = collectScanFileRows(
        snapshot.getScanBuilder().withFilter(geoCenterAny).build())
      assert(files3.isEmpty)
    }
  }

  private def writeFilesWithGeometryStats(
      tablePath: String,
      engine: io.delta.kernel.engine.Engine,
      schema: StructType,
      extents: Seq[(Double, Double, Double, Double)]): Unit = {
    val geomCol = new Column("geom")
    extents.zipWithIndex.foreach {
      case ((minX, minY, maxX, maxY), idx) =>
        val txn =
          if (idx == 0)
            getCreateTxn(engine, tablePath, schema)
          else getUpdateTxn(engine, tablePath)

        val txnState = txn.getTransactionState(engine)
        val writeContext = Transaction.getWriteContext(
          engine,
          txnState,
          Collections.emptyMap())

        val stats = new DataFileStatistics(
          10,
          Map(geomCol -> Literal.ofString(
            s"POINT ($minX $minY)")).asJava,
          Map(geomCol -> Literal.ofString(
            s"POINT ($maxX $maxY)")).asJava,
          Map(geomCol -> (0L: java.lang.Long)).asJava,
          Optional.empty())

        val filePath =
          engine.getFileSystemClient.resolvePath(
            writeContext.getTargetDirectory + s"/part-$idx.parquet")
        val fileStatus = new DataFileStatus(
          filePath,
          1000,
          0L,
          Optional.of(stats))

        val actions = Transaction.generateAppendActions(
          engine,
          txnState,
          toCloseableIterator(Seq(fileStatus).iterator.asJava),
          writeContext)
        commitTransaction(
          txn,
          engine,
          inMemoryIterable(actions))
    }
  }
}
