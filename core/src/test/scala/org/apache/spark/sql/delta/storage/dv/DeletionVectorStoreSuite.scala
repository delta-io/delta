/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.storage.dv

import java.io.{DataInputStream, DataOutputStream, File}

import org.apache.spark.sql.delta.{DeltaChecksumException, DeltaConfigs, DeltaLog}
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore.{CHECKSUM_LEN, DATA_SIZE_LEN}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.PathWithFileSystem
import com.google.common.primitives.Ints
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

trait DeletionVectorStoreSuiteBase
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  lazy val dvStore: DeletionVectorStore =
    DeletionVectorStore.createInstance(newHadoopConf)

  protected def newHadoopConf: Configuration = {
    // scalastyle:off deltahadoopconfiguration
    spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
  }

  // Test bitmaps
  protected lazy val simpleBitmap = {
    val data = Seq(1L, 5L, 6L, 7L, 1000L, 8000000L, 8000001L)
    RoaringBitmapArray(data: _*)
  }

  protected lazy val simpleBitmap2 = {
    val data = Seq(78L, 256L, 998L, 1000002L, 22623423L)
    RoaringBitmapArray(data: _*)
  }


  def withTempHadoopFileSystemPath[T](f: Path => T): T = {
    val dir: File = Utils.createTempDir()
    dir.delete()
    val tempPath = DeletionVectorStore.stringToPath(dir.toString)
    try f(tempPath) finally Utils.deleteRecursively(dir)
  }

  testWithAllSerializationFormats("Write simple DV directly to disk") { serializationFormat =>
    val readDV =
      withTempHadoopFileSystemPath { tableDir =>
        val tableWithFS = PathWithFileSystem.withConf(tableDir, newHadoopConf)
        val dvPath = dvStore.generateUniqueNameInTable(tableWithFS)
        val serializedBitmap = simpleBitmap.serializeAsByteArray(serializationFormat)
        val dvRange = Utils.tryWithResource(dvStore.createWriter(dvPath)) { writer =>
          writer.write(serializedBitmap)
        }
        assert(dvRange.offset === 1) // there's a version id at byte 0
        assert(dvRange.length === serializedBitmap.length)
        dvStore.read(dvPath.path, dvRange.offset, dvRange.length)
      }
    assert(simpleBitmap === readDV)
  }


  testWithAllSerializationFormats("Detect corrupted DV checksum ") { serializationFormat =>
    withTempHadoopFileSystemPath { tableDir =>
      val tableWithFS = PathWithFileSystem.withConf(tableDir, newHadoopConf)
      val dvPath = dvStore.generateUniqueNameInTable(tableWithFS)
      val dvBytes = simpleBitmap.serializeAsByteArray(serializationFormat)
      val dvRange = Utils.tryWithResource(dvStore.createWriter(dvPath)) {
        writer => writer.write(dvBytes)
      }
      assert(dvRange.offset === 1) // there's a version id at byte 0
      assert(dvRange.length === dvBytes.length)
      // corrupt 1 byte in the middle of the stored DV (after the checksum)
      corruptByte(dvPath, byteToCorrupt = DeletionVectorStore.CHECKSUM_LEN + dvRange.length / 2)
      val e = intercept[DeltaChecksumException] {
        dvStore.read(dvPath.path, dvRange.offset, dvRange.length)
      }
      // make sure this is our exception not ChecksumFileSystem's
      assert(e.getErrorClass == "DELTA_DELETION_VECTOR_CHECKSUM_MISMATCH")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage ==
        "Could not verify deletion vector integrity, CRC checksum verification failed.")
    }
  }

  testWithAllSerializationFormats("Detect corrupted DV size") { serializationFormat =>
    withTempHadoopFileSystemPath { tableDir =>
      val tableWithFS = PathWithFileSystem.withConf(tableDir, newHadoopConf)
      val dvPath = dvStore.generateUniqueNameInTable(tableWithFS)
      val dvBytes = simpleBitmap.serializeAsByteArray(serializationFormat)
      val dvRange = Utils.tryWithResource(dvStore.createWriter(dvPath)) {
        writer => writer.write(dvBytes)
      }
      assert(dvRange.offset === 1) // there's a version id at byte 0
      assert(dvRange.length === dvBytes.length)

      // Corrupt 1 byte in the part where the serialized DV size is stored.
      // Format:<Version - 1byte> <SerializedDV Size> <SerializedDV Bytes> <DV Checksum>
      corruptByte(dvPath, byteToCorrupt = 2)
      val e = intercept[DeltaChecksumException] {
        dvStore.read(dvPath.path, dvRange.offset, dvRange.length)
      }
      assert(e.getErrorClass == "DELTA_DELETION_VECTOR_SIZE_MISMATCH")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "Deletion vector integrity check failed. Encountered a size mismatch.")
    }
  }

  testWithAllSerializationFormats("Multiple DVs in one file") { serializationFormat =>
    withTempHadoopFileSystemPath { tableDir =>
      val tableWithFS = PathWithFileSystem.withConf(tableDir, newHadoopConf)
      val dvPath = dvStore.generateUniqueNameInTable(tableWithFS)
      val dvBytes1 = simpleBitmap.serializeAsByteArray(serializationFormat)
      val dvBytes2 = simpleBitmap2.serializeAsByteArray(serializationFormat)
      val (dvRange1, dvRange2) = Utils.tryWithResource(dvStore.createWriter(dvPath)) {
        writer =>
          (writer.write(dvBytes1), writer.write(dvBytes2))
      }
      assert(dvRange1.offset === 1) // there's a version id at byte 0
      assert(dvRange1.length === dvBytes1.length)

      // DV2 should be written immediately after the DV1
      // DV Format:<SerializedDV Size> <SerializedDV Bytes> <DV Checksum>
      val totalDV1Size = DATA_SIZE_LEN + dvBytes1.length + CHECKSUM_LEN
      assert(dvRange2.offset === 1 + totalDV1Size) // 1byte for file format version
      assert(dvRange2.length === dvBytes2.length)

      // Read back DVs from the file and verify
      assert(dvStore.read(dvPath.path, dvRange1.offset, dvRange1.length) === simpleBitmap)
      assert(dvStore.read(dvPath.path, dvRange2.offset, dvRange2.length) === simpleBitmap2)
    }
  }

  test("Exception is thrown for DVDescriptors with invalid maxRowIndex") {
    withSQLConf(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true",
        DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> true.toString) {
      withTempDir { dir =>
        val path = dir.toString
        spark.range(0, 50, 1, 1).write.format("delta").save(path)
        val targetTable = io.delta.tables.DeltaTable.forPath(path)
        val deltaLog = DeltaLog.forTable(spark, path)
        val tableName = s"delta.`$path`"
        spark.sql(s"DELETE FROM $tableName WHERE id = 3")
        val file = deltaLog.update().allFiles.first()
        val dvDescriptorWithInvalidRowIndex = file.deletionVector.copy(maxRowIndex = Some(50))

        val e = intercept[DeltaChecksumException] {
          file.removeRows(
            dvDescriptorWithInvalidRowIndex
          )
        }
        assert(e.getErrorClass == "DELTA_DELETION_VECTOR_INVALID_ROW_INDEX")
        assert(e.getSqlState == "XXKDS")
        assert(e.getMessage ==
          "Deletion vector integrity check failed. Encountered an invalid row index.")
      }
    }
  }

  /** Helper method to run the test using all DV serialization formats */
  protected def testWithAllSerializationFormats(name: String)
      (func: RoaringBitmapArrayFormat.Value => Unit): Unit = {
    for (serializationFormat <- RoaringBitmapArrayFormat.values) {
      test(s"$name - $serializationFormat") {
        func(serializationFormat)
      }
    }
  }

  /** Helper to method to simulate data corruption in on-disk DV */
  private def corruptByte(pathWithFS: PathWithFileSystem, byteToCorrupt: Int): Unit = {
    val fs = pathWithFS.fs
    val path = pathWithFS.path
    val status = fs.getFileStatus(path)
    val len = Ints.checkedCast(status.getLen)

    val bytes = Utils.tryWithResource(fs.open(path)) { stream =>
      val reader = new DataInputStream(stream)
      // readAllBytes is not available in 1.8, yet
      val buffer = new Array[Byte](len)
      reader.readFully(buffer)
      buffer
    }
    bytes(byteToCorrupt) = (bytes(byteToCorrupt) + 1).toByte
    val overwrite = true
    Utils.tryWithResource(fs.create(path, overwrite)) { stream =>
      val writer = new DataOutputStream(stream)
      writer.write(bytes)
      writer.flush()
    }
  }
}

class DeletionVectorStoreSuite
  extends DeletionVectorStoreSuiteBase
