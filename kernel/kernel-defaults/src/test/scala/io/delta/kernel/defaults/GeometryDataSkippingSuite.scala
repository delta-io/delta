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

  // 4-quadrant layout + 1 null-stats file (f4, never skipped):
  //    y=10 +------+        +------+
  //         |  f2  |        |  f1  |
  //    y=7  +------+        +------+
  //                (gap)
  //    y=3  +------+        +------+
  //         |  f0  |        |  f3  |
  //    y=0  +------+        +------+
  //         x=0    x=3      x=7    x=10
  private val fileExtents: Seq[Option[(Double, Double, Double, Double)]] =
    Seq(
      Some((0.0, 0.0, 3.0, 3.0)), // SW - file 0
      Some((7.0, 7.0, 10.0, 10.0)), // NE - file 1
      Some((0.0, 7.0, 3.0, 10.0)), // NW - file 2
      Some((7.0, 0.0, 10.0, 3.0)), // SE - file 3
      None // null stats - file 4 (never skipped)
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
          Literal.ofGeospatialWKT(s"POINT ($qMinX $qMinY)", colType),
          Literal.ofGeospatialWKT(s"POINT ($qMaxX $qMaxY)", colType))
        collectScanFileRows(
          snapshot.getScanBuilder().withFilter(pred).build()).size
      }

      // null-stats f4 is always included in every query
      // SW query - intersects f0 + f4(null)
      assert(filesHit(1.0, 1.0, 4.0, 4.0) == 2)
      // NE query - intersects f1 + f4(null)
      assert(filesHit(8.0, 8.0, 11.0, 11.0) == 2)
      // Center gap - no data file hit, only f4(null)
      assert(filesHit(4.0, 4.0, 6.0, 6.0) == 1)
      // West strip [1,1]-[4,9] - f0 + f2 + f4(null)
      assert(filesHit(1.0, 1.0, 4.0, 9.0) == 3)
      // Top strip [0,8]-[11,11] - f1 + f2 + f4(null)
      assert(filesHit(0.0, 8.0, 11.0, 11.0) == 3)
      // Global - all 4 data files + f4(null)
      assert(filesHit(0.0, 0.0, 11.0, 11.0) == 5)
    }
  }

  test("StGeometryBoxesIntersect combined with AND predicate on second column") {
    withTempDirAndEngine { (tablePath, engine) =>
      val schema = new StructType()
        .add("id", io.delta.kernel.types.IntegerType.INTEGER)
        .add("geom", new GeometryType("OGC:CRS84"))

      val idCol = new Column("id")
      val geomCol = new Column("geom")

      // File 0 (SW): id [1,5],   geom bbox [0,0]-[3,3]
      // File 1 (NE): id [10,20], geom bbox [7,7]-[10,10]
      // File 2: null stats (never skipped)
      val files: Seq[Option[(Int, Int, Double, Double, Double, Double)]] =
        Seq(
          Some((1, 5, 0.0, 0.0, 3.0, 3.0)),
          Some((10, 20, 7.0, 7.0, 10.0, 10.0)),
          None)

      files.zipWithIndex.foreach {
        case (fileOpt, idx) =>
          val txn = if (idx == 0) {
            getCreateTxn(engine, tablePath, schema)
          } else {
            getUpdateTxn(engine, tablePath)
          }

          val txnState = txn.getTransactionState(engine)
          val writeContext = Transaction.getWriteContext(
            engine,
            txnState,
            Collections.emptyMap())

          val stats = fileOpt match {
            case Some((idMin, idMax, gMinX, gMinY, gMaxX, gMaxY)) =>
              new DataFileStatistics(
                10,
                Map(
                  idCol -> Literal.ofInt(idMin),
                  geomCol -> Literal.ofGeospatialWKT(
                    s"POINT ($gMinX $gMinY)",
                    colType)).asJava,
                Map(
                  idCol -> Literal.ofInt(idMax),
                  geomCol -> Literal.ofGeospatialWKT(
                    s"POINT ($gMaxX $gMaxY)",
                    colType)).asJava,
                Map(
                  idCol -> (0L: java.lang.Long),
                  geomCol -> (0L: java.lang.Long)).asJava,
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

      // null-stats f2 is always included in every query

      // SW geo + id<=5: f0 matches both, f2(null) included
      val geoAndId = new io.delta.kernel.expressions.And(
        new StGeometryBoxesIntersect(
          geomCol,
          Literal.ofGeospatialWKT("POINT (1.0 1.0)", colType),
          Literal.ofGeospatialWKT("POINT (4.0 4.0)", colType)),
        new io.delta.kernel.expressions.Predicate(
          "<=",
          idCol,
          Literal.ofInt(5)))
      assert(collectScanFileRows(
        snapshot.getScanBuilder().withFilter(geoAndId).build()).size == 2)

      // NE geo + id>=15: f1 matches both, f2(null) included
      val geoAndId2 = new io.delta.kernel.expressions.And(
        new StGeometryBoxesIntersect(
          geomCol,
          Literal.ofGeospatialWKT("POINT (8.0 8.0)", colType),
          Literal.ofGeospatialWKT("POINT (11.0 11.0)", colType)),
        new io.delta.kernel.expressions.Predicate(
          ">=",
          idCol,
          Literal.ofInt(15)))
      assert(collectScanFileRows(
        snapshot.getScanBuilder().withFilter(geoAndId2).build()).size == 2)

      // Center geo: both data files skipped, only f2(null)
      val geoCenterAny = new StGeometryBoxesIntersect(
        geomCol,
        Literal.ofGeospatialWKT("POINT (4.0 4.0)", colType),
        Literal.ofGeospatialWKT("POINT (6.0 6.0)", colType))
      assert(collectScanFileRows(
        snapshot.getScanBuilder().withFilter(geoCenterAny).build()).size == 1)
    }
  }

  private def writeFilesWithGeometryStats(
      tablePath: String,
      engine: io.delta.kernel.engine.Engine,
      schema: StructType,
      extents: Seq[Option[(Double, Double, Double, Double)]]): Unit = {
    val geomCol = new Column("geom")
    extents.zipWithIndex.foreach {
      case (extentOpt, idx) =>
        val txn = if (idx == 0) {
          getCreateTxn(engine, tablePath, schema)
        } else {
          getUpdateTxn(engine, tablePath)
        }

        val txnState = txn.getTransactionState(engine)
        val writeContext = Transaction.getWriteContext(
          engine,
          txnState,
          Collections.emptyMap())

        val stats = extentOpt match {
          case Some((minX, minY, maxX, maxY)) =>
            new DataFileStatistics(
              10,
              Map(geomCol -> Literal.ofGeospatialWKT(
                s"POINT ($minX $minY)",
                colType)).asJava,
              Map(geomCol -> Literal.ofGeospatialWKT(
                s"POINT ($maxX $maxY)",
                colType)).asJava,
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
  }
}
