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

import java.util.Optional

import io.delta.golden.GoldenTableUtils
import io.delta.kernel.data.{ColumnarBatch, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.client.DefaultTableClient
import io.delta.kernel.types.{LongType, StructType}
import io.delta.kernel.utils.CloseableIterator
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

// scalastyle:off println
class LogReplaySuite extends AnyFunSuite {
  // TODO: refactor to use TestUtils

  private val tableClient = DefaultTableClient.create(new Configuration() {{
      // Set the batch sizes to small so that we get to test the multiple batch scenarios.
      set("delta.kernel.default.parquet.reader.batch-size", "2");
      set("delta.kernel.default.json.reader.batch-size", "2");
    }})

  private def readSnapshot(readSchema: StructType, snapshot: Snapshot): Seq[ColumnarBatch] = {
    val scan = snapshot.getScanBuilder(tableClient).withReadSchema(tableClient, readSchema).build()
    val scanState = scan.getScanState(tableClient)
    val scanFileIter = scan.getScanFiles(tableClient)
    readScanFiles(scanState, scanFileIter)
  }

  private def readScanFiles(
      scanState: Row,
      scanFilesBatchIter: CloseableIterator[FilteredColumnarBatch]): Seq[ColumnarBatch] = {
    val dataBatches = scala.collection.mutable.ArrayBuffer[ColumnarBatch]()
    try {
      while (scanFilesBatchIter.hasNext()) {
        // Read data
        try {
          val data = Scan.readData(
            tableClient, scanState, scanFilesBatchIter.next().getRows, Optional.empty())
          try {
            while (data.hasNext()) {
              val dataReadResult = data.next()
              assert(dataReadResult.getSelectionVector.isPresent == false)
              dataBatches += dataReadResult.getData()
            }
          } finally if (data != null) data.close()
        }
      }
    } finally scanFilesBatchIter.close()
    dataBatches
  }

  test("simple end to end with inserts and deletes and checkpoint") {
    val unresolvedPath = GoldenTableUtils.goldenTablePath("basic-with-inserts-deletes-checkpoint")
    val table = io.delta.kernel.Table.forPath(tableClient, unresolvedPath)
    val conf = new Configuration()
    val client = DefaultTableClient.create(conf)
    val snapshot = table.getLatestSnapshot(client)
    val schema = snapshot.getSchema(client)
    val version = snapshot.getVersion(client)

    assert(schema === new StructType().add("id", LongType.INSTANCE))
    assert(version === 13)

    var output = new scala.collection.mutable.ArrayBuffer[Long]()
    readSnapshot(schema, snapshot).foreach { data =>
      val rowsIter = data.getRows
      while (rowsIter.hasNext) {
        val row = rowsIter.next()
        output += row.getLong(0)
      }
      rowsIter.close()
    }

    output = output.sorted
    println(output)
    assert(output.length == 41)
  }

}
