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

import scala.collection.immutable.Seq

import io.delta.kernel.defaults.utils.{GeoTestUtils, WriteUtils}
import io.delta.kernel.expressions.{And, Column, Literal, Predicate, StGeometryBoxesIntersect}
import io.delta.kernel.statistics.DataFileStatistics
import io.delta.kernel.types.{GeometryType, IntegerType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class GeometryDataSkippingSuite extends AnyFunSuite with WriteUtils with GeoTestUtils {

  // 4-quadrant layout + 1 null-stats file (f4, never skipped):
  //    y=10 +------+        +------+
  //         |  f2  |        |  f1  |
  //    y=7  +------+        +------+
  //                (gap)
  //    y=3  +------+        +------+
  //         |  f0  |        |  f3  |
  //    y=0  +------+        +------+
  //         x=0    x=3      x=7    x=10
  private val fileExtents: Seq[Option[(Double, Double, Double, Double)]] = Seq(
    Some((0.0, 0.0, 3.0, 3.0)),
    Some((7.0, 7.0, 10.0, 10.0)),
    Some((0.0, 7.0, 3.0, 10.0)),
    Some((7.0, 0.0, 10.0, 3.0)),
    None)

  private val colType = new GeometryType("OGC:CRS84")
  private val geomCol = new Column("geom")

  test("StGeometryBoxesIntersect data skipping on GeometryType column") {
    withTempDirAndEngine { (tablePath, engine) =>
      val schema = new StructType().add("geom", colType)
      val statsList = fileExtents.map {
        case Some((minX, minY, maxX, maxY)) =>
          geoStats(geomCol, minX, minY, maxX, maxY, colType)
        case None => emptyStats()
      }
      commitGeoStatsFiles(tablePath, engine, schema, statsList)

      val snapshot = latestSnapshot(tablePath)

      // null-stats f4 is always returned (+1 in every count below).
      assert(boxFilesHit(snapshot, geomCol, colType, 1.0, 1.0, 4.0, 4.0) == 2) // f0
      assert(boxFilesHit(snapshot, geomCol, colType, 8.0, 8.0, 11.0, 11.0) == 2) // f1
      assert(boxFilesHit(snapshot, geomCol, colType, 4.0, 4.0, 6.0, 6.0) == 1) // none
      assert(boxFilesHit(snapshot, geomCol, colType, 1.0, 1.0, 4.0, 9.0) == 3) // f0+f2
      assert(boxFilesHit(snapshot, geomCol, colType, 0.0, 8.0, 11.0, 11.0) == 3) // f1+f2
      assert(boxFilesHit(snapshot, geomCol, colType, 0.0, 0.0, 11.0, 11.0) == 5) // all
    }
  }

  test("file with completely missing stats falls through and is never pruned") {
    withTempDirAndEngine { (tablePath, engine) =>
      val schema = new StructType().add("geom", colType)
      commitGeoStatsFiles(
        tablePath,
        engine,
        schema,
        Seq(geoStats(geomCol, 0.0, 0.0, 3.0, 3.0, colType), emptyStats()))

      val snapshot = latestSnapshot(tablePath)
      assert(boxFilesHit(snapshot, geomCol, colType, 100.0, 100.0, 101.0, 101.0) == 1)
    }
  }

  test("StGeometryBoxesIntersect combined with AND predicate on second column") {
    withTempDirAndEngine { (tablePath, engine) =>
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("geom", colType)
      val idCol = new Column("id")

      // f0: id [1,5], geom [0,0]-[3,3]; f1: id [10,20], geom [7,7]-[10,10]; f2: null stats.
      val statsList = Seq[DataFileStatistics](
        stats(
          minValues = Map(
            idCol -> Literal.ofInt(1),
            geomCol -> pointWktLiteral(0.0, 0.0, colType)),
          maxValues = Map(
            idCol -> Literal.ofInt(5),
            geomCol -> pointWktLiteral(3.0, 3.0, colType)),
          nullCounts = Map(idCol -> 0L, geomCol -> 0L)),
        stats(
          minValues = Map(
            idCol -> Literal.ofInt(10),
            geomCol -> pointWktLiteral(7.0, 7.0, colType)),
          maxValues = Map(
            idCol -> Literal.ofInt(20),
            geomCol -> pointWktLiteral(10.0, 10.0, colType)),
          nullCounts = Map(idCol -> 0L, geomCol -> 0L)),
        emptyStats())
      commitGeoStatsFiles(tablePath, engine, schema, statsList)

      val snapshot = latestSnapshot(tablePath)

      val swGeoAndId = new And(
        new StGeometryBoxesIntersect(
          geomCol,
          pointWktLiteral(1.0, 1.0, colType),
          pointWktLiteral(4.0, 4.0, colType)),
        new Predicate("<=", idCol, Literal.ofInt(5)))
      assert(collectScanFileRows(
        snapshot.getScanBuilder().withFilter(swGeoAndId).build()).size == 2)

      val neGeoAndId = new And(
        new StGeometryBoxesIntersect(
          geomCol,
          pointWktLiteral(8.0, 8.0, colType),
          pointWktLiteral(11.0, 11.0, colType)),
        new Predicate(">=", idCol, Literal.ofInt(15)))
      assert(collectScanFileRows(
        snapshot.getScanBuilder().withFilter(neGeoAndId).build()).size == 2)

      assert(boxFilesHit(snapshot, geomCol, colType, 4.0, 4.0, 6.0, 6.0) == 1)
    }
  }
}
