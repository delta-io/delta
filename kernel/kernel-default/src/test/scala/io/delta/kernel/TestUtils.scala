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
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration

import io.delta.kernel.client.DefaultTableClient
import io.delta.kernel.data.Row
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterator

trait TestUtils {
  implicit class CloseableIteratorOps[T: ClassTag](private val iter: CloseableIterator[T]) {

    def forEach(f: T => Unit): Unit = {
      try {
        while (iter.hasNext) {
          f(iter.next())
        }
      } finally {
        iter.close()
      }
    }
  }

  def readTable[T](path: String, conf: Configuration, schema: StructType = null)
                  (getValue: Row => T): Seq[T] = {
    val result = ArrayBuffer[T]()

    val tableClient = DefaultTableClient.create(conf)
    val table = Table.forPath(path)
    val snapshot = table.getLatestSnapshot(tableClient)

    val readSchema = if (schema == null) {
      snapshot.getSchema(tableClient)
    } else {
      schema
    }

    val scan = snapshot.getScanBuilder(tableClient)
      .withReadSchema(tableClient, readSchema)
      .build()

    val scanState = scan.getScanState(tableClient);
    val fileIter = scan.getScanFiles(tableClient)
    // TODO serialize scan state and scan rows

    fileIter.forEach { fileColumnarBatch =>
      // TODO deserialize scan state and scan rows
      val dataBatches = Scan.readData(
        tableClient,
        scanState,
        fileColumnarBatch.getRows(),
        Optional.empty()
      )

      dataBatches.forEach { batch =>
        val selectionVector = batch.getSelectionVector()
        val data = batch.getData()

        var i = 0
        val rowIter = data.getRows()
        try {
          while (rowIter.hasNext) {
            val row = rowIter.next()
            if (!selectionVector.isPresent || selectionVector.get.getBoolean(i)) { // row is valid
              result.append(getValue(row))
            }
            i += 1
          }
        } finally {
          rowIter.close()
        }
      }
    }
    result
  }
}
