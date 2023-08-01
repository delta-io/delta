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
package io.delta.kernel

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.client.DefaultTableClient
import io.delta.kernel.utils.DefaultKernelTestUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class DeletionVectorSuite extends AnyFunSuite with TestUtils {

  test("end-to-end usage: reading a table with dv") {
    val path = DefaultKernelTestUtils.getTestResourceFilePath("basic-dv-no-checkpoint")
    val expectedResult = Seq.range(start = 2, end = 10).toSet

    val snapshot = latestSnapshot(path)
    val result = readSnapshot(snapshot).map { row =>
      row.getLong(0)
    }

    assert(result.toSet === expectedResult)
  }

  test("end-to-end usage: reading a table with dv with checkpoint") {
    val path = DefaultKernelTestUtils.getTestResourceFilePath("basic-dv-with-checkpoint")
    val expectedResult = Seq.range(start = 0, end = 500).filter(_ % 11 != 0).toSet

    val snapshot = latestSnapshot(path)
    val result = readSnapshot(snapshot).map  { row =>
      row.getLong(0)
    }

    assert(result.toSet === expectedResult)
  }

  test("end-to-end usage: reading partitioned dv table with checkpoint") {
    // kernel expects a fully qualified path
    val path = "file:" + goldenTablePath("dv-partitioned-with-checkpoint")
    val expectedResult = (0 until 50).map(x => (x%10, x, s"foo${x % 5}"))
      .filter{ case (_, col1, _) =>
        !(col1 % 2 == 0 && col1 < 30)
      }.toSet

    val conf = new Configuration()
    // Set the batch size small enough so there will be multiple batches
    conf.setInt("delta.kernel.default.parquet.reader.batch-size", 2)
    val tableClient = DefaultTableClient.create(conf)

    val snapshot = latestSnapshot(path, tableClient = tableClient)
    val result = readSnapshot(snapshot, tableClient = tableClient).map { row =>
      (row.getInt(0), row.getInt(1), row.getString(2))
    }

    assert (result.toSet == expectedResult)
  }

  // TODO: update to use goldenTables once bug is fixed in delta-spark see issue #1886
  test(
    "end-to-end usage: reading partitioned dv table with checkpoint with columnMappingMode=name") {
    val path = DefaultKernelTestUtils.getTestResourceFilePath("dv-with-columnmapping")
    val expectedResult = (0 until 50).map(x => (x%10, x, s"foo${x % 5}"))
      .filter{ case (_, col1, _) =>
        !(col1 % 2 == 0 && col1 < 30)
      }.toSet

    val snapshot = latestSnapshot(path)
    val result = readSnapshot(snapshot).map { row =>
      (row.getInt(0), row.getInt(1), row.getString(2))
    }

    assert (result.toSet == expectedResult)
  }


  // TODO detect corrupted DV checksum
  // TODO detect corrupted dv size
  // TODO multiple dvs in one file
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
