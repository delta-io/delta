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
package io.delta.kernel.defaults

import java.io.File
import java.nio.file.Files

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.shaded.org.apache.commons.io.FileUtils
import org.scalatest.funsuite.AnyFunSuite

class DeletionVectorSuite extends AnyFunSuite with TestUtils {

  test("end-to-end usage: reading a table with dv") {
    checkTable(
      path = getTestResourceFilePath("basic-dv-no-checkpoint"),
      expectedAnswer = (2L until 10L).map(TestRow(_)))
  }

  test("end-to-end usage: reading a table with dv with space in the root path") {
    withTempDir { tempDir =>
      val target = tempDir.getCanonicalPath + "spark test"
      spark.sql(s"""CREATE TABLE tbl (
          id int
        ) USING delta LOCATION '$target'
        TBLPROPERTIES ('delta.enableDeletionVectors' = true) """)
      spark.sql("INSERT INTO tbl VALUES (1),(2),(3),(4),(5)")
      spark.sql("DELETE FROM tbl WHERE id = 1")
      checkTable(
        path = target,
        expectedAnswer = Seq(TestRow(2), TestRow(3), TestRow(4), TestRow(5)))
    }
  }

  test("end-to-end usage: reading a table with dv with checkpoint") {
    checkTable(
      path = getTestResourceFilePath("basic-dv-with-checkpoint"),
      expectedAnswer = (0L until 500L).filter(_ % 11 != 0).map(TestRow(_)))
  }

  test("end-to-end usage: reading partitioned dv table with checkpoint") {
    val conf = new Configuration()
    // Set the batch size small enough so there will be multiple batches
    conf.setInt("delta.kernel.default.parquet.reader.batch-size", 2)

    val expectedResult = (0 until 50).map(x => (x % 10, x, s"foo${x % 5}"))
      .filter { case (_, col1, _) =>
        !(col1 % 2 == 0 && col1 < 30)
      }

    checkTable(
      path = goldenTablePath("dv-partitioned-with-checkpoint"),
      expectedAnswer = expectedResult.map(TestRow.fromTuple(_)),
      engine = defaultEngine)
  }

  test(
    "end-to-end usage: reading partitioned dv table with checkpoint with columnMappingMode=name") {
    val expectedResult = (0 until 50).map(x => (x % 10, x, s"foo${x % 5}"))
      .filter { case (_, col1, _) =>
        !(col1 % 2 == 0 && col1 < 30)
      }
    checkTable(
      path = goldenTablePath("dv-with-columnmapping"),
      expectedAnswer = expectedResult.map(TestRow.fromTuple(_)))
  }

  test("corrupted DV checksum is detected") {
    withTempDir { tempDir =>
      // Copy the test table so we can corrupt it without affecting other tests
      FileUtils.copyDirectory(
        new File(getTestResourceFilePath("basic-dv-no-checkpoint")),
        tempDir)

      // The DV binary layout at file offset `dvDescriptor.offset` (=1 here):
      //   [0..3]  : 4-byte big-endian int  = stored size
      //   [4..N-1]: N bytes                = serialized bitmap
      //   [N..N+3]: 4-byte big-endian int  = CRC32 checksum
      // Corrupt the checksum by flipping the last byte of the file.
      val dvFile = tempDir.listFiles().find(_.getName.startsWith("deletion_vector_")).get
      val bytes = Files.readAllBytes(dvFile.toPath)
      bytes(bytes.length - 1) = (bytes(bytes.length - 1) ^ 0xFF).toByte
      Files.write(dvFile.toPath, bytes)

      val ex = intercept[RuntimeException] {
        checkTable(path = tempDir.getCanonicalPath, expectedAnswer = Seq.empty)
      }
      assert(
        exOrCauseMessageContains(ex, "DV checksum mismatch"),
        s"Expected 'DV checksum mismatch' in exception chain, got: ${ex.getMessage}")
    }
  }

  test("corrupted DV size is detected") {
    withTempDir { tempDir =>
      FileUtils.copyDirectory(
        new File(getTestResourceFilePath("basic-dv-no-checkpoint")),
        tempDir)

      // The stored size int is at bytes [offset..offset+3] in the file.
      // For this table the DV descriptor has offset=1, so the size int lives at file bytes [1..4].
      // Write an incorrect size (999) so it mismatches the descriptor's sizeInBytes (36).
      val dvFile = tempDir.listFiles().find(_.getName.startsWith("deletion_vector_")).get
      val bytes = Files.readAllBytes(dvFile.toPath)
      val wrongSize = 999
      bytes(1) = ((wrongSize >> 24) & 0xFF).toByte
      bytes(2) = ((wrongSize >> 16) & 0xFF).toByte
      bytes(3) = ((wrongSize >> 8) & 0xFF).toByte
      bytes(4) = (wrongSize & 0xFF).toByte
      Files.write(dvFile.toPath, bytes)

      val ex = intercept[RuntimeException] {
        checkTable(path = tempDir.getCanonicalPath, expectedAnswer = Seq.empty)
      }
      assert(
        exOrCauseMessageContains(ex, "DV size mismatch"),
        s"Expected 'DV size mismatch' in exception chain, got: ${ex.getMessage}")
    }
  }

  // TODO multiple dvs in one file

  // Walks the cause chain and returns true if any message contains `substr`.
  private def exOrCauseMessageContains(ex: Throwable, substr: String): Boolean = {
    var t = ex
    while (t != null) {
      if (t.getMessage != null && t.getMessage.contains(substr)) return true
      t = t.getCause
    }
    false
  }
}

object DeletionVectorsSuite {
  // TODO: test using this once we support reading by version
  val table1Path = "src/test/resources/delta/table-with-dv-large"
  // Table at version 0: contains [0, 2000)
  val expectedTable1DataV0 = Seq.range(0, 2000)
  // Table at version 1: removes rows with id = 0, 180, 300, 700, 1800
  val v1Removed = Set(0, 180, 300, 700, 1800)
  val expectedTable1DataV1 = expectedTable1DataV0.filterNot(e => v1Removed.contains(e))
  // Table at version 2: inserts rows with id = 300, 700
  val v2Added = Set(300, 700)
  val expectedTable1DataV2 = expectedTable1DataV1 ++ v2Added
  // Table at version 3: removes rows with id = 300, 250, 350, 900, 1353, 1567, 1800
  val v3Removed = Set(300, 250, 350, 900, 1353, 1567, 1800)
  val expectedTable1DataV3 = expectedTable1DataV2.filterNot(e => v3Removed.contains(e))
  // Table at version 4: inserts rows with id = 900, 1567
  val v4Added = Set(900, 1567)
  val expectedTable1DataV4 = expectedTable1DataV3 ++ v4Added
}
