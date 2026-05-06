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
package io.delta.kernel.defaults.internal.parquet

import java.nio.{ByteBuffer, ByteOrder}

import io.delta.kernel.defaults.utils.DefaultVectorTestUtils
import io.delta.kernel.expressions.Column
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for reading geometry and geography columns from Parquet.
 * Exercises the full data path: ParquetSchemaUtils, ParquetColumnWriters,
 * ParquetColumnReaders, DefaultBinaryVector, and bounding box stats.
 */
class GeometryParquetReaderSuite
    extends AnyFunSuite
    with ParquetSuiteBase
    with DefaultVectorTestUtils {

  // WKB for a 2D point (little-endian):
  // byteOrder(1) + type=1(4) + x(8) + y(8) = 21 bytes
  private def pointWkb(x: Double, y: Double): Array[Byte] = {
    val buf = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN)
    buf.put(1.toByte)
    buf.putInt(1)
    buf.putDouble(x)
    buf.putDouble(y)
    buf.array()
  }

  for (
    colType <- Seq[DataType](
      new GeometryType("OGC:CRS84"),
      new GeographyType("OGC:CRS84", "spherical"))
  ) {
    val typeName = colType.getClass.getSimpleName

    test(s"$typeName: write and read with null and non-null rows") {
      withTempDir { tempDir =>
        val targetDir = tempDir.getAbsolutePath

        val values: Seq[Option[Array[Byte]]] = Seq(
          Some(pointWkb(1.0, 2.0)),
          None,
          Some(pointWkb(3.0, 4.0)),
          None,
          Some(pointWkb(5.0, 6.0)))

        val geomVec = makeGeometryVector(colType, values)
        writeToParquetUsingKernel(
          Seq(columnarBatch(geomVec).toFiltered),
          targetDir)

        val readSchema = new StructType().add("col_0", colType)
        val batches = readParquetUsingKernelAsColumnarBatches(
          targetDir,
          readSchema)

        val results = batches.flatMap { batch =>
          val vec = batch.getColumnVector(0)
          (0 until batch.getSize).map { rowId =>
            if (vec.isNullAt(rowId)) None
            else Some(vec.getBinary(rowId).toSeq)
          }
        }

        assert(results.length == values.length)
        values.zip(results).foreach { case (expected, actual) =>
          assert(actual == expected.map(_.toSeq))
        }
      }
    }

  }

  test("GeometryType: bounding box stats are computed correctly") {
    withTempDir { tempDir =>
      val targetDir = tempDir.getAbsolutePath
      val colType = new GeometryType("OGC:CRS84")

      val values: Seq[Option[Array[Byte]]] = Seq(
        Some(pointWkb(10.0, -20.0)),
        None,
        Some(pointWkb(-5.0, 30.0)),
        Some(pointWkb(15.0, 0.0)))

      val geomVec = makeGeometryVector(colType, values)
      val statsCol = new Column("col_0")
      val fileStatuses = writeToParquetUsingKernel(
        Seq(columnarBatch(geomVec).toFiltered),
        targetDir,
        statsColumns = Seq(statsCol))

      assert(fileStatuses.size == 1)
      val stats = fileStatuses.head.getStatistics
      assert(stats.isPresent, "stats should be present")

      val fileStats = stats.get()
      assert(fileStats.getNumRecords == 4)

      val minLiteral = fileStats.getMinValues.get(statsCol)
      val maxLiteral = fileStats.getMaxValues.get(statsCol)
      assert(minLiteral != null, "min value should exist")
      assert(maxLiteral != null, "max value should exist")

      // Bounding box: x in [-5, 15], y in [-20, 30]
      assert(minLiteral.getValue == "POINT(-5.0 -20.0)")
      assert(maxLiteral.getValue == "POINT(15.0 30.0)")

      val nullCount = fileStats.getNullCount.get(statsCol)
      assert(nullCount == 1L)
    }
  }

  private def makeGeometryVector(
      colType: DataType,
      values: Seq[Option[Array[Byte]]]): io.delta.kernel.data.ColumnVector = {
    new io.delta.kernel.data.ColumnVector {
      override def getDataType: DataType = colType
      override def getSize: Int = values.length
      override def close(): Unit = {}
      override def isNullAt(rowId: Int): Boolean = values(rowId).isEmpty
      override def getBinary(rowId: Int): Array[Byte] =
        values(rowId).orNull
    }
  }
}
