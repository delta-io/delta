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

import io.delta.kernel.Table
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

/**
 * Reads the golden table `spark-geospatial`, which Delta Spark wrote with geometry and geography
 * columns (see its info.txt), to check that Kernel reads what Spark writes, not only what Kernel
 * writes.
 */
class SparkGeospatialTableSuite extends AnyFunSuite with TestUtils {

  private val point12 = "0101000000000000000000F03F0000000000000040"
  private val point1020 = "010100000000000000000024400000000000003440"
  private val pointNeg530 = "010100000000000000000014C00000000000003E40"
  private val point150 = "01010000000000000000002E400000000000000000"

  private def snapshot = Table.forPath(defaultEngine, getTestResourceFilePath("spark-geospatial"))
    .getLatestSnapshot(defaultEngine)

  private def hex(bytes: Array[Byte]): String = bytes.map("%02X".format(_)).mkString

  private def hexOrNull(row: Row, ordinal: Int): String =
    if (row.isNullAt(ordinal)) null else hex(row.getBinary(ordinal))

  test("the schema and protocol Spark wrote are read, with the CRS and algorithm") {
    val geometry = new GeometryType("OGC:CRS84")
    // Spark writes the algorithm in upper case, and Kernel keeps it as written.
    val geography = new GeographyType("OGC:CRS84", "SPHERICAL")
    assert(snapshot.getSchema == new StructType()
      .add("id", IntegerType.INTEGER)
      .add("geom", geometry)
      .add("geog", geography)
      .add("nested", new StructType().add("g", geometry)))

    val protocol = snapshot.asInstanceOf[SnapshotImpl].getProtocol
    assert(protocol.getReaderFeatures.contains("geospatial"))
    assert(protocol.getWriterFeatures.contains("geospatial"))
  }

  test("the values Spark wrote are read as WKB, including nulls and a nested geometry") {
    val rows = readSnapshot(snapshot).sortBy(_.getInt(0)).map { row =>
      val nested = if (row.isNullAt(3)) None else Some(hexOrNull(row.getStruct(3), 0))
      (row.getInt(0), hexOrNull(row, 1), hexOrNull(row, 2), nested)
    }
    assert(rows == Seq(
      (1, point12, point1020, Some(point12)),
      (2, null, pointNeg530, None),
      (3, point150, null, Some(null))))
  }
}
