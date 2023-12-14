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

import java.io.File
import java.nio.file.Files
import java.util.{Optional, TimeZone, UUID}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import io.delta.golden.GoldenTableUtils
import io.delta.kernel.{Scan, Snapshot, Table}
import io.delta.kernel.client.TableClient
import io.delta.kernel.data.{ColumnVector, MapValue, Row}
import io.delta.kernel.defaults.client.DefaultTableClient
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.data.ScanStateRow
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.shaded.org.apache.commons.io.FileUtils
import org.scalatest.Assertions

trait TestUtils extends Assertions {

  lazy val configuration = new Configuration()
  lazy val defaultTableClient = DefaultTableClient.create(configuration)

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

  def withGoldenTable(tableName: String)(testFunc: String => Unit): Unit = {
    val tablePath = GoldenTableUtils.goldenTablePath(tableName)
    testFunc(tablePath)
  }

  def latestSnapshot(path: String): Snapshot = {
    Table.forPath(defaultTableClient, path)
      .getLatestSnapshot(defaultTableClient)
  }

  def collectScanFileRows(scan: Scan, tableClient: TableClient = defaultTableClient): Seq[Row] = {
    scan.getScanFiles(tableClient).toSeq
      .flatMap(_.getRows.toSeq)
  }

  def readSnapshot(
    snapshot: Snapshot,
    readSchema: StructType = null,
    filter: Predicate = null,
    expectedRemainingFilter: Predicate = null,
    tableClient: TableClient = defaultTableClient): Seq[Row] = {

    val result = ArrayBuffer[Row]()

    var scanBuilder = snapshot.getScanBuilder(tableClient)

    if (readSchema != null) {
      scanBuilder = scanBuilder.withReadSchema(tableClient, readSchema)
    }

    if (filter != null) {
      scanBuilder = scanBuilder.withFilter(tableClient, filter)
    }

    val scan = scanBuilder.build()

    if (filter != null) {
      val actRemainingPredicate = scan.getRemainingFilter()
      assert(
        actRemainingPredicate.toString === Optional.ofNullable(expectedRemainingFilter).toString)
    }

    val scanState = scan.getScanState(tableClient);
    val fileIter = scan.getScanFiles(tableClient)

    val physicalDataReadSchema = ScanStateRow.getPhysicalDataReadSchema(tableClient, scanState)
    fileIter.forEach { fileColumnarBatch =>
      fileColumnarBatch.getRows().forEach { scanFileRow =>
        val physicalDataIter = tableClient.getParquetHandler().readParquetFiles(
          singletonCloseableIterator(scanFileRow),
          physicalDataReadSchema,
          Optional.empty())
        val dataBatches = Scan.transformPhysicalData(
          tableClient,
          scanState,
          scanFileRow,
          physicalDataIter
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
    }
    result
  }

  /**
   * Execute {@code f} with {@code TimeZone.getDefault()} set to the time zone provided.
   *
   * @param zoneId the ID for a TimeZone, either an abbreviation such as "PST", a full name such as
   *               "America/Los_Angeles", or a custom ID such as "GMT-8:00".
   */
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
    BooleanType.BOOLEAN,
    ByteType.BYTE,
    ShortType.SHORT,
    IntegerType.INTEGER,
    LongType.LONG,
    FloatType.FLOAT,
    DoubleType.DOUBLE,
    DateType.DATE,
    TimestampType.TIMESTAMP,
    StringType.STRING,
    BinaryType.BINARY,
    new DecimalType(10, 5)
  )

  /** All types. Used in parameterized tests where type is one of the test dimensions. */
  val ALL_TYPES = SIMPLE_TYPES ++ Seq(
    new ArrayType(BooleanType.BOOLEAN, true),
    new MapType(IntegerType.INTEGER, LongType.LONG, true),
    new StructType().add("s1", BooleanType.BOOLEAN).add("s2", IntegerType.INTEGER)
  )

  /**
   * Compares the rows in the tables latest snapshot with the expected answer and fails if they
   * do not match. The comparison is order independent. If expectedSchema is provided, checks
   * that the latest snapshot's schema is equivalent.
   *
   * @param path fully qualified path of the table to check
   * @param expectedAnswer expected rows
   * @param readCols subset of columns to read; if null then all columns will be read
   * @param tableClient table client to use to read the table
   * @param expectedSchema expected schema to check for; if null then no check is performed
   * @param filter Filter to select a subset of rows form the table
   * @param expectedRemainingFilter Remaining predicate out of the `filter` that is not enforced
   *                                by Kernel.
   * @param expectedVersion expected version of the latest snapshot for the table
   */
  def checkTable(
    path: String,
    expectedAnswer: Seq[TestRow],
    readCols: Seq[String] = null,
    tableClient: TableClient = defaultTableClient,
    expectedSchema: StructType = null,
    filter: Predicate = null,
    expectedRemainingFilter: Predicate = null,
    expectedVersion: Option[Long] = None
  ): Unit = {

    val snapshot = latestSnapshot(path)

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

    expectedVersion.foreach { version =>
      assert(version == snapshot.getVersion(defaultTableClient),
        s"Expected version $version does not match actual version" +
          s" ${snapshot.getVersion(defaultTableClient)}")
    }

    val result = readSnapshot(snapshot, readSchema, filter, expectedRemainingFilter, tableClient)
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

  private def prepareAnswer(answer: Seq[TestRow]): Seq[TestRow] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted = answer.map(prepareRow)
    converted.sortBy(_.toString())
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  private def prepareRow(row: TestRow): TestRow = {
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

  private def compare(obj1: Any, obj2: Any): Boolean = (obj1, obj2) match {
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

  private def genErrorMessage(expectedAnswer: Seq[TestRow], result: Seq[TestRow]): String = {
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

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected def withTempDir(f: File => Unit): Unit = {
    val tempDir = Files.createTempDirectory(UUID.randomUUID().toString).toFile
    try f(tempDir) finally {
      FileUtils.deleteDirectory(tempDir)
    }
  }

  /**
   * Builds a MapType ColumnVector from a sequence of maps.
   */
  def buildMapVector(mapValues: Seq[Map[AnyRef, AnyRef]], dataType: MapType): ColumnVector = {
    val keyType = dataType.getKeyType
    val valueType = dataType.getValueType

    def getMapValue(map: Map[AnyRef, AnyRef]): MapValue = {
      if (map == null) {
        null
      } else {
        val (keys, values) = map.unzip
        new MapValue() {
          override def getSize: Int = map.size

          override def getKeys = DefaultGenericVector.fromArray(keyType, keys.toArray)

          override def getValues = DefaultGenericVector.fromArray(valueType, values.toArray)
        }
      }
    }

    DefaultGenericVector.fromArray(dataType, mapValues.map(getMapValue).toArray)
  }
}
