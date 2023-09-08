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
package io.delta.kernel.defaults.utils

import java.util.{Optional, TimeZone}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.{Scan, Snapshot, Table}
import io.delta.kernel.client.TableClient
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.client.DefaultTableClient
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterator
import org.apache.hadoop.conf.Configuration
import org.scalatest.exceptions.TestFailedException
import org.scalatest.Assertions

trait TestUtils extends Assertions {

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

  def checkTable(
    path: String,
    expectedAnswer: Seq[TestRow],
    readCols: Seq[String] = null,
    tableClient: TableClient = defaultTableClient,
    expectedSchema: StructType = null
    // filter
    // version
  ): Unit = {

    val snapshot = Table.forPath(path).getLatestSnapshot(tableClient)

    val readSchema = if (readCols == null) {
      null
    } else {
      val schema = snapshot.getSchema(tableClient)
      new StructType(readCols.map(schema.get(_)).asJava)
    }

    if (expectedSchema != null) {
      assert(
        expectedSchema == snapshot.getSchema(tableClient),
        s"""
           |Expected schema does not match actual schema:
           |Expected schema: $expectedSchema
           |Actual schema: ${snapshot.getSchema(tableClient)}
           |""".stripMargin
      )
    }

    val result = readSnapshot(snapshot, readSchema = readSchema, tableClient = tableClient)
    checkAnswer(result, expectedAnswer)
  }

  def checkAnswer(result: => Seq[Row], expectedAnswer: Seq[TestRow]): Unit = {
    checkAnswer(result.map(TestRow(_)), expectedAnswer)
  }

  def checkAnswer(result: Seq[TestRow], expectedAnswer: Seq[TestRow]): Unit = {
    if (!compare(prepareAnswer(result), prepareAnswer(expectedAnswer))) {
      fail(genErrorMessage(expectedAnswer, result))
    }
  }

  def prepareAnswer(answer: Seq[TestRow]): Seq[TestRow] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted = answer.map(prepareRow)
    converted.sortBy(_.toString())
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  def prepareRow(row: TestRow): TestRow = {
    TestRow.fromSeq(row.toSeq.map {
      case null => null
      case bd: java.math.BigDecimal => BigDecimal(bd)
      // Equality of WrappedArray differs for AnyVal and AnyRef in Scala 2.12.2+
      case seq: Seq[_] => seq.map {
        case b: java.lang.Byte => b.byteValue
        case s: java.lang.Short => s.shortValue
        case i: java.lang.Integer => i.intValue
        case l: java.lang.Long => l.longValue
        case f: java.lang.Float => f.floatValue
        case d: java.lang.Double => d.doubleValue
        case x => x
      }
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: TestRow => prepareRow(r)
      case o => o
    })
  }

  def compare(obj1: Any, obj2: Any): Boolean = (obj1, obj2) match {
      case (null, null) => true
      case (null, _) => false
      case (_, null) => false
      case (a: Array[_], b: Array[_]) =>
        a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r) }
      case (a: Map[_, _], b: Map[_, _]) =>
        a.size == b.size && a.keys.forall { aKey =>
          b.keys.find(bKey => compare(aKey, bKey)).exists(bKey => compare(a(aKey), b(bKey)))
        }
      case (a: Iterable[_], b: Iterable[_]) =>
        a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r) }
      case (a: Product, b: Product) =>
        compare(a.productIterator.toSeq, b.productIterator.toSeq)
      case (a: TestRow, b: TestRow) =>
        compare(a.toSeq, b.toSeq)
      // 0.0 == -0.0, turn float/double to bits before comparison, to distinguish 0.0 and -0.0.
      case (a: Double, b: Double) =>
        java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
      case (a: Float, b: Float) =>
        java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)
      case (a, b) => a.equals(b) // In scala == does not call equals for boxed numeric classes?
    }

  def genErrorMessage(expectedAnswer: Seq[TestRow], result: Seq[TestRow]): String = {
    // TODO: improve to include schema or Java type information to help debugging
    s"""
       |== Results ==
       |
       |== Expected Answer - ${expectedAnswer.size} ==
       |${prepareAnswer(expectedAnswer).map(_.toString()).mkString("(", ",", ")")}
       |
       |== Result - ${result.size} ==
       |${prepareAnswer(result).map(_.toString()).mkString("(", ",", ")")}
       |
       |""".stripMargin
  }
}
