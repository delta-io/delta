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

import java.util.{Optional, TimeZone}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.{Scan, Snapshot}
import io.delta.kernel.client.TableClient
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.client.DefaultTableClient
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterator
import org.apache.hadoop.conf.Configuration

trait TestUtils {
  lazy val defaultTableClient = DefaultTableClient.create(new Configuration())

  implicit class CloseableIteratorOps[T](private val iter: CloseableIterator[T]) {

    def forEach(f: T => Unit): Unit = {
      try {
        while (iter.hasNext) {
          f(iter.next())
        }
      } finally {
        iter.close()
      }
    }

    def toSeq: Seq[T] = {
      try {
        val result = new ArrayBuffer[T]
        while (iter.hasNext) {
          result.append(iter.next())
        }
        result
      } finally {
        iter.close()
      }
    }
  }

  implicit class StructTypeOps(schema: StructType) {

    def withoutField(name: String): StructType = {
      val newFields = schema.fields().asScala
        .filter(_.getName != name).asJava
      new StructType(newFields)
    }
  }

  def readSnapshot(
    snapshot: Snapshot,
    readSchema: StructType = null,
    tableClient: TableClient = defaultTableClient): Seq[Row] = {

    val result = ArrayBuffer[Row]()

    var scanBuilder = snapshot.getScanBuilder(tableClient)

    if (readSchema != null) {
      scanBuilder = scanBuilder.withReadSchema(tableClient, readSchema)
    }

    val scan = scanBuilder.build()

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
              result.append(row)
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

  def withTimeZone(zoneId: String)(f: => Unit): Unit = {
    val currentDefault = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(zoneId))
      f
    } finally {
      TimeZone.setDefault(currentDefault)
    }
  }

  /** All simple data type used in parameterized tests where type is one of the test dimensions. */
  val SIMPLE_TYPES = Seq(
    BooleanType.INSTANCE,
    ByteType.INSTANCE,
    ShortType.INSTANCE,
    IntegerType.INSTANCE,
    LongType.INSTANCE,
    FloatType.INSTANCE,
    DoubleType.INSTANCE,
    DateType.INSTANCE,
    TimestampType.INSTANCE,
    StringType.INSTANCE,
    BinaryType.INSTANCE,
    new DecimalType(10, 5)
  )

  /** All types. Used in parameterized tests where type is one of the test dimensions. */
  val ALL_TYPES = SIMPLE_TYPES ++ Seq(
    new ArrayType(BooleanType.INSTANCE, true),
    new MapType(IntegerType.INSTANCE, LongType.INSTANCE, true),
    new StructType().add("s1", BooleanType.INSTANCE).add("s2", IntegerType.INSTANCE)
  )
}
