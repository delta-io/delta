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
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for reading geometry and geography columns from Parquet via getBinary().
 *
 * Parquet files are written and read with GeometryType/GeographyType schema directly,
 * exercising the full data path: ParquetSchemaUtils, ParquetColumnWriters, ParquetColumnReaders,
 * and DefaultBinaryVector.
 */
class GeometryParquetReaderSuite extends AnyFunSuite with ParquetSuiteBase
    with DefaultVectorTestUtils {

  // WKB for a 2D point (little-endian): byteOrder(1) + type=1(4) + x(8) + y(8) = 21 bytes
  private def pointWkb(x: Double, y: Double): Array[Byte] = {
    val buf = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN)
    buf.put(1.toByte)
    buf.putInt(1)
    buf.putDouble(x)
    buf.putDouble(y)
    buf.array()
  }

  private val testPoints: Seq[Array[Byte]] = Seq(
    pointWkb(1.0, 2.0),
    pointWkb(-180.0, 90.0),
    pointWkb(0.0, 0.0),
    pointWkb(123.456, -78.9))

  for (colType <- Seq[DataType](new GeometryType(), new GeographyType())) {
    val typeName = colType.getClass.getSimpleName

    test(s"write and read $typeName column: getBinary returns WKB bytes") {
      withTempDir { tempDir =>
        val targetDir = tempDir.getAbsolutePath

        val geomVec = makeGeometryVector(colType, testPoints.map(Option(_)))
        writeToParquetUsingKernel(Seq(columnarBatch(geomVec).toFiltered), targetDir)

        val readSchema = new StructType().add("col_0", colType)
        val batches = readParquetUsingKernelAsColumnarBatches(targetDir, readSchema)

        val actual = batches.flatMap { batch =>
          val vec = batch.getColumnVector(0)
          (0 until batch.getSize).map(rowId => vec.getBinary(rowId).toSeq)
        }

        assert(actual.length == testPoints.length)
        testPoints.zip(actual).foreach { case (expected, actual) =>
          assert(actual == expected.toSeq, "WKB bytes should roundtrip unchanged")
        }
      }
    }

    test(s"write and read $typeName column with null rows") {
      withTempDir { tempDir =>
        val targetDir = tempDir.getAbsolutePath

        val values: Seq[Option[Array[Byte]]] = Seq(
          Some(pointWkb(1.0, 2.0)),
          None,
          Some(pointWkb(3.0, 4.0)),
          None,
          Some(pointWkb(5.0, 6.0)))

        val geomVec = makeGeometryVector(colType, values)
        writeToParquetUsingKernel(Seq(columnarBatch(geomVec).toFiltered), targetDir)

        val readSchema = new StructType().add("col_0", colType)
        val batches = readParquetUsingKernelAsColumnarBatches(targetDir, readSchema)

        val results = batches.flatMap { batch =>
          val vec = batch.getColumnVector(0)
          (0 until batch.getSize).map { rowId =>
            if (vec.isNullAt(rowId)) None else Some(vec.getBinary(rowId).toSeq)
          }
        }

        assert(results.length == values.length)
        values.zip(results).foreach { case (expected, actual) =>
          assert(actual == expected.map(_.toSeq))
        }
      }
    }

    test(s"getString throws UnsupportedOperationException on $typeName column vector") {
      withTempDir { tempDir =>
        val targetDir = tempDir.getAbsolutePath
        val geomVec = makeGeometryVector(colType, Seq(Some(pointWkb(1.0, 2.0))))
        writeToParquetUsingKernel(Seq(columnarBatch(geomVec).toFiltered), targetDir)

        val readSchema = new StructType().add("col_0", colType)
        val batches = readParquetUsingKernelAsColumnarBatches(targetDir, readSchema)
        val vec = batches.head.getColumnVector(0)

        intercept[UnsupportedOperationException] {
          vec.getString(0)
        }
      }
    }

    test(s"write and read $typeName column: all-null batch") {
      withTempDir { tempDir =>
        val targetDir = tempDir.getAbsolutePath
        val geomVec = makeGeometryVector(colType, Seq(None, None, None))
        writeToParquetUsingKernel(Seq(columnarBatch(geomVec).toFiltered), targetDir)

        val readSchema = new StructType().add("col_0", colType)
        val batches = readParquetUsingKernelAsColumnarBatches(targetDir, readSchema)

        val results = batches.flatMap { batch =>
          val vec = batch.getColumnVector(0)
          (0 until batch.getSize).map(vec.isNullAt)
        }
        assert(results.forall(_ == true), "All rows should be null")
      }
    }
  }

  // Creates a geometry/geography ColumnVector backed by WKB byte arrays.
  // getBinary() returns the raw WKB for non-null rows.
  private def makeGeometryVector(
      colType: DataType,
      values: Seq[Option[Array[Byte]]]): io.delta.kernel.data.ColumnVector = {
    new io.delta.kernel.data.ColumnVector {
      override def getDataType: DataType = colType
      override def getSize: Int = values.length
      override def close(): Unit = {}
      override def isNullAt(rowId: Int): Boolean = values(rowId).isEmpty
      override def getBinary(rowId: Int): Array[Byte] = values(rowId).orNull
    }
  }
}
