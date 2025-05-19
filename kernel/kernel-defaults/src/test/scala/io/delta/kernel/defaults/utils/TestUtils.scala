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

import java.io.{File, FileNotFoundException}
import java.math.{BigDecimal => BigDecimalJ}
import java.nio.file.Files
import java.util.{Optional, TimeZone, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.golden.GoldenTableUtils
import io.delta.kernel.{Scan, Snapshot, Table, TransactionCommitResult}
import io.delta.kernel.data.{ColumnarBatch, ColumnVector, FilteredColumnarBatch, MapValue, Row}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.defaults.internal.data.vector.{DefaultGenericVector, DefaultStructVector}
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.{Column, Predicate}
import io.delta.kernel.hook.PostCommitHook.PostCommitHookType
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl, TableImpl}
import io.delta.kernel.internal.checksum.{ChecksumReader, ChecksumWriter, CRCInfo}
import io.delta.kernel.internal.data.ScanStateRow
import io.delta.kernel.internal.fs.{Path => KernelPath}
import io.delta.kernel.internal.stats.FileSizeHistogram
import io.delta.kernel.internal.util.FileNames.checksumFile
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.types._
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.shaded.org.apache.commons.io.FileUtils
import org.apache.spark.sql.{types => sparktypes}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.scalatest.Assertions

trait TestUtils extends Assertions with SQLHelper {

  lazy val configuration = new Configuration()
  lazy val defaultEngine = DefaultEngine.create(configuration)

  lazy val spark = SparkSession
    .builder()
    .appName("Spark Test Writer for Delta Kernel")
    .config("spark.master", "local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    // Set this conf to empty string so that the golden tables generated
    // using with the test-prefix (i.e. there is no DELTA_TESTING set) can still work
    .config(DeltaSQLConf.TEST_DV_NAME_PREFIX.key, "")
    .getOrCreate()

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
        result.toSeq
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

    def toSpark: sparktypes.StructType = {
      toSparkSchema(schema)
    }
  }

  implicit class ColumnarBatchOps(batch: ColumnarBatch) {
    def toFiltered: FilteredColumnarBatch = {
      new FilteredColumnarBatch(batch, Optional.empty())
    }

    def toFiltered(predicate: Option[Predicate]): FilteredColumnarBatch = {
      if (predicate.isEmpty) {
        new FilteredColumnarBatch(batch, Optional.empty())
      } else {
        val predicateEvaluator = defaultEngine.getExpressionHandler
          .getPredicateEvaluator(batch.getSchema, predicate.get)
        val selVector = predicateEvaluator.eval(batch, Optional.empty())
        new FilteredColumnarBatch(batch, Optional.of(selVector))
      }
    }
  }

  implicit class FilteredColumnarBatchOps(batch: FilteredColumnarBatch) {
    def toTestRows: Seq[TestRow] = {
      batch.getRows.toSeq.map(TestRow(_))
    }
  }

  implicit class ColumnOps(column: Column) {
    def toPath: String = column.getNames.mkString(".")
  }

  implicit class JavaOptionalOps[T](optional: Optional[T]) {
    def toScala: Option[T] = if (optional.isPresent) Some(optional.get()) else None
  }

  implicit object ResourceLoader {
    lazy val classLoader: ClassLoader = ResourceLoader.getClass.getClassLoader
  }

  def withTempDirAndEngine(
      f: (String, Engine) => Unit,
      hadoopConf: Map[String, String] = Map.empty): Unit = {
    val engine = DefaultEngine.create(new Configuration() {
      {
        for ((key, value) <- hadoopConf) {
          set(key, value)
        }
        // Set the batch sizes to small so that we get to test the multiple batch/file scenarios.
        set("delta.kernel.default.parquet.reader.batch-size", "20");
        set("delta.kernel.default.json.reader.batch-size", "20");
        set("delta.kernel.default.parquet.writer.targetMaxFileSize", "20");
      }
    })
    withTempDir { dir => f(dir.getAbsolutePath, engine) }
  }

  def withGoldenTable(tableName: String)(testFunc: String => Unit): Unit = {
    val tablePath = GoldenTableUtils.goldenTablePath(tableName)
    testFunc(tablePath)
  }

  def latestSnapshot(path: String, engine: Engine = defaultEngine): Snapshot = {
    Table.forPath(engine, path)
      .getLatestSnapshot(engine)
  }

  def tableSchema(path: String): StructType = {
    Table.forPath(defaultEngine, path)
      .getLatestSnapshot(defaultEngine)
      .getSchema()
  }

  def hasTableProperty(tablePath: String, propertyKey: String, expValue: String): Boolean = {
    val table = Table.forPath(defaultEngine, tablePath)
    val schema = table.getLatestSnapshot(defaultEngine).getSchema()
    schema.fields().asScala.exists { field =>
      field.getMetadata.getString(propertyKey) == expValue
    }
  }

  /** Get the list of all leaf-level primitive column references in the given `structType` */
  def leafLevelPrimitiveColumns(basePath: Seq[String], structType: StructType): Seq[Column] = {
    structType.fields.asScala.flatMap {
      case field if field.getDataType.isInstanceOf[StructType] =>
        leafLevelPrimitiveColumns(
          basePath :+ field.getName,
          field.getDataType.asInstanceOf[StructType])
      case field
          if !field.getDataType.isInstanceOf[ArrayType] &&
            !field.getDataType.isInstanceOf[MapType] =>
        // for all primitive types
        Seq(new Column((basePath :+ field.getName).asJava.toArray(new Array[String](0))));
      case _ => Seq.empty
    }.toSeq
  }

  def collectScanFileRows(scan: Scan, engine: Engine = defaultEngine): Seq[Row] = {
    scan.getScanFiles(engine).toSeq
      .flatMap(_.getRows.toSeq)
  }

  def readSnapshot(
      snapshot: Snapshot,
      readSchema: StructType = null,
      filter: Predicate = null,
      expectedRemainingFilter: Predicate = null,
      engine: Engine = defaultEngine): Seq[Row] = {

    val result = ArrayBuffer[Row]()

    var scanBuilder = snapshot.getScanBuilder()

    if (readSchema != null) {
      scanBuilder = scanBuilder.withReadSchema(readSchema)
    }

    if (filter != null) {
      scanBuilder = scanBuilder.withFilter(filter)
    }

    val scan = scanBuilder.build()

    if (filter != null) {
      val actRemainingPredicate = scan.getRemainingFilter()
      assert(
        actRemainingPredicate.toString === Optional.ofNullable(expectedRemainingFilter).toString)
    }

    val scanState = scan.getScanState(engine);
    val fileIter = scan.getScanFiles(engine)

    val physicalDataReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState)
    fileIter.forEach { fileColumnarBatch =>
      fileColumnarBatch.getRows().forEach { scanFileRow =>
        val fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow)
        val physicalDataIter = engine.getParquetHandler().readParquetFiles(
          singletonCloseableIterator(fileStatus),
          physicalDataReadSchema,
          Optional.empty())
        var dataBatches: CloseableIterator[FilteredColumnarBatch] = null
        try {
          dataBatches = Scan.transformPhysicalData(
            engine,
            scanState,
            scanFileRow,
            physicalDataIter)

          dataBatches.forEach { batch =>
            val selectionVector = batch.getSelectionVector()
            val data = batch.getData()

            var i = 0
            val rowIter = data.getRows()
            try {
              while (rowIter.hasNext) {
                val row = rowIter.next()
                if (!selectionVector.isPresent || selectionVector.get.getBoolean(i)) {
                  // row is valid
                  result.append(row)
                }
                i += 1
              }
            } finally {
              rowIter.close()
            }
          }
        } finally {
          dataBatches.close()
        }
      }
    }
    result.toSeq
  }

  def readTableUsingKernel(
      engine: Engine,
      tablePath: String,
      readSchema: StructType): Seq[FilteredColumnarBatch] = {
    val scan = Table.forPath(engine, tablePath)
      .getLatestSnapshot(engine)
      .getScanBuilder()
      .withReadSchema(readSchema)
      .build()
    val scanState = scan.getScanState(engine)

    val physicalDataReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState)
    var result: Seq[FilteredColumnarBatch] = Nil
    scan.getScanFiles(engine).forEach { fileColumnarBatch =>
      fileColumnarBatch.getRows.forEach { scanFile =>
        val fileStatus = InternalScanFileUtils.getAddFileStatus(scanFile)
        val physicalDataIter = engine.getParquetHandler.readParquetFiles(
          singletonCloseableIterator(fileStatus),
          physicalDataReadSchema,
          Optional.empty())
        var dataBatches: CloseableIterator[FilteredColumnarBatch] = null
        try {
          dataBatches = Scan.transformPhysicalData(engine, scanState, scanFile, physicalDataIter)
          dataBatches.forEach { dataBatch => result = result :+ dataBatch }
        } finally {
          Utils.closeCloseables(dataBatches)
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
    TimestampNTZType.TIMESTAMP_NTZ,
    StringType.STRING,
    BinaryType.BINARY,
    new DecimalType(10, 5))

  /** All types. Used in parameterized tests where type is one of the test dimensions. */
  val ALL_TYPES = SIMPLE_TYPES ++ Seq(
    new ArrayType(BooleanType.BOOLEAN, true),
    new MapType(IntegerType.INTEGER, LongType.LONG, true),
    new StructType().add("s1", BooleanType.BOOLEAN).add("s2", IntegerType.INTEGER))

  /**
   * Compares the rows in the tables latest snapshot with the expected answer and fails if they
   * do not match. The comparison is order independent. If expectedSchema is provided, checks
   * that the latest snapshot's schema is equivalent.
   *
   * @param path fully qualified path of the table to check
   * @param expectedAnswer expected rows
   * @param readCols subset of columns to read; if null then all columns will be read
   * @param engine engine to use to read the table
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
      engine: Engine = defaultEngine,
      expectedSchema: StructType = null,
      filter: Predicate = null,
      version: Option[Long] = None,
      timestamp: Option[Long] = None,
      expectedRemainingFilter: Predicate = null,
      expectedVersion: Option[Long] = None): Unit = {
    assert(version.isEmpty || timestamp.isEmpty, "Cannot provide both a version and timestamp")

    val snapshot = if (version.isDefined) {
      Table.forPath(engine, path)
        .getSnapshotAsOfVersion(engine, version.get)
    } else if (timestamp.isDefined) {
      Table.forPath(engine, path)
        .getSnapshotAsOfTimestamp(engine, timestamp.get)
    } else {
      latestSnapshot(path, engine)
    }

    val readSchema = if (readCols == null) {
      null
    } else {
      val schema = snapshot.getSchema()
      new StructType(readCols.map(schema.get(_)).asJava)
    }

    if (expectedSchema != null) {
      assert(
        expectedSchema == snapshot.getSchema(),
        s"""
           |Expected schema does not match actual schema:
           |Expected schema: $expectedSchema
           |Actual schema: ${snapshot.getSchema()}
           |""".stripMargin)
    }

    expectedVersion.foreach { version =>
      assert(
        version == snapshot.getVersion(),
        s"Expected version $version does not match actual version ${snapshot.getVersion()}")
    }

    val result = readSnapshot(snapshot, readSchema, filter, expectedRemainingFilter, engine)
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
    case (a, b) =>
      if (!a.equals(b)) {
        val sds = 200;
      }
      a.equals(b)
    // In scala == does not call equals for boxed numeric classes?
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
    try f(tempDir)
    finally {
      FileUtils.deleteDirectory(tempDir)
    }
  }

  /**
   * Drops table `tableName` after calling `f`.
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f
    finally {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  def withSparkTimeZone(timeZone: String)(fn: => Unit): Unit = {
    val prevTimeZone = spark.conf.get("spark.sql.session.timeZone")
    try {
      spark.conf.set("spark.sql.session.timeZone", timeZone)
      fn
    } finally {
      spark.conf.set("spark.sql.session.timeZone", prevTimeZone)
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

  /**
   * Utility method to generate a [[dataType]] column vector of given size.
   * The nullability of rows is determined by the [[testIsNullValue(dataType, rowId)]].
   * The row values are determined by [[testColumnValue(dataType, rowId)]].
   */
  def testColumnVector(size: Int, dataType: DataType): ColumnVector = {
    dataType match {
      // Build a DefaultStructVector and recursively
      // build child vectors for each field.
      case structType: StructType =>
        val memberVectors: Array[ColumnVector] =
          structType.fields().asScala.map { field =>
            testColumnVector(size, field.getDataType)
          }.toArray

        new DefaultStructVector(
          size,
          structType,
          Optional.empty(),
          memberVectors)

      case _ =>
        new ColumnVector {
          override def getDataType: DataType = dataType

          override def getSize: Int = size

          override def close(): Unit = {}

          override def isNullAt(rowId: Int): Boolean = testIsNullValue(dataType, rowId)

          override def getBoolean(rowId: Int): Boolean =
            testColumnValue(dataType, rowId).asInstanceOf[Boolean]

          override def getByte(rowId: Int): Byte =
            testColumnValue(dataType, rowId).asInstanceOf[Byte]

          override def getShort(rowId: Int): Short =
            testColumnValue(dataType, rowId).asInstanceOf[Short]

          override def getInt(rowId: Int): Int = testColumnValue(dataType, rowId).asInstanceOf[Int]

          override def getLong(rowId: Int): Long =
            testColumnValue(dataType, rowId).asInstanceOf[Long]

          override def getFloat(rowId: Int): Float =
            testColumnValue(dataType, rowId).asInstanceOf[Float]

          override def getDouble(rowId: Int): Double =
            testColumnValue(dataType, rowId).asInstanceOf[Double]

          override def getBinary(rowId: Int): Array[Byte] =
            testColumnValue(dataType, rowId).asInstanceOf[Array[Byte]]

          override def getString(rowId: Int): String =
            testColumnValue(dataType, rowId).asInstanceOf[String]

          override def getDecimal(rowId: Int): BigDecimalJ =
            testColumnValue(dataType, rowId).asInstanceOf[BigDecimalJ]
        }
    }
  }

  /** Utility method to generate a consistent `isNull` value for given column type and row id */
  def testIsNullValue(dataType: DataType, rowId: Int): Boolean = {
    dataType match {
      case BooleanType.BOOLEAN => rowId % 4 == 0
      case ByteType.BYTE => rowId % 8 == 0
      case ShortType.SHORT => rowId % 12 == 0
      case IntegerType.INTEGER => rowId % 20 == 0
      case LongType.LONG => rowId % 25 == 0
      case FloatType.FLOAT => rowId % 5 == 0
      case DoubleType.DOUBLE => rowId % 10 == 0
      case StringType.STRING => rowId % 2 == 0
      case BinaryType.BINARY => rowId % 3 == 0
      case DateType.DATE => rowId % 5 == 0
      case TimestampType.TIMESTAMP => rowId % 3 == 0
      case TimestampNTZType.TIMESTAMP_NTZ => rowId % 2 == 0
      case _ =>
        if (dataType.isInstanceOf[DecimalType]) rowId % 6 == 0
        else throw new UnsupportedOperationException(s"$dataType is not supported")
    }
  }

  /** Utility method to generate a consistent column value for given column type and row id */
  def testColumnValue(dataType: DataType, rowId: Int): Any = {
    dataType match {
      case BooleanType.BOOLEAN => rowId % 7 == 0
      case ByteType.BYTE => (rowId * 7 / 17).toByte
      case ShortType.SHORT => (rowId * 9 / 87).toShort
      case IntegerType.INTEGER => rowId * 2876 / 176
      case LongType.LONG => rowId * 287623L / 91
      case FloatType.FLOAT => rowId * 7651.2323f / 91
      case DoubleType.DOUBLE => rowId * 23423.23d / 17
      case StringType.STRING => (rowId % 19).toString
      case BinaryType.BINARY => Array[Byte]((rowId % 21).toByte, (rowId % 7 - 1).toByte)
      case DateType.DATE => (rowId * 28234) % 2876
      case TimestampType.TIMESTAMP => (rowId * 2342342L) % 23
      case TimestampNTZType.TIMESTAMP_NTZ => (rowId * 523423L) % 29
      case _ =>
        if (dataType.isInstanceOf[DecimalType]) new BigDecimalJ(rowId * 22342.23)
        else throw new UnsupportedOperationException(s"$dataType is not supported")
    }
  }

  def testSingleValueVector(dataType: DataType, size: Int, value: Any): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = dataType

      override def getSize: Int = size

      override def close(): Unit = {}

      override def isNullAt(rowId: Int): Boolean = value == null

      override def getBoolean(rowId: Int): Boolean =
        value.asInstanceOf[Boolean]

      override def getByte(rowId: Int): Byte = value.asInstanceOf[Byte]

      override def getShort(rowId: Int): Short =
        value.asInstanceOf[Short]

      override def getInt(rowId: Int): Int = value.asInstanceOf[Int]

      override def getLong(rowId: Int): Long = value.asInstanceOf[Long]

      override def getFloat(rowId: Int): Float =
        value.asInstanceOf[Float]

      override def getDouble(rowId: Int): Double =
        value.asInstanceOf[Double]

      override def getBinary(rowId: Int): Array[Byte] =
        value.asInstanceOf[Array[Byte]]

      override def getString(rowId: Int): String =
        value.asInstanceOf[String]

      override def getDecimal(rowId: Int): BigDecimalJ =
        value.asInstanceOf[BigDecimalJ]
    }
  }

  /**
   * Converts a Delta Schema to a Spark Schema.
   */
  private def toSparkSchema(deltaSchema: StructType): sparktypes.StructType = {
    toSparkType(deltaSchema).asInstanceOf[sparktypes.StructType]
  }

  /**
   * Converts a Delta DataType to a Spark DataType.
   */
  private def toSparkType(deltaType: DataType): sparktypes.DataType = {
    deltaType match {
      case BooleanType.BOOLEAN => sparktypes.DataTypes.BooleanType
      case ByteType.BYTE => sparktypes.DataTypes.ByteType
      case ShortType.SHORT => sparktypes.DataTypes.ShortType
      case IntegerType.INTEGER => sparktypes.DataTypes.IntegerType
      case LongType.LONG => sparktypes.DataTypes.LongType
      case FloatType.FLOAT => sparktypes.DataTypes.FloatType
      case DoubleType.DOUBLE => sparktypes.DataTypes.DoubleType
      case StringType.STRING => sparktypes.DataTypes.StringType
      case BinaryType.BINARY => sparktypes.DataTypes.BinaryType
      case DateType.DATE => sparktypes.DataTypes.DateType
      case TimestampType.TIMESTAMP => sparktypes.DataTypes.TimestampType
      case TimestampNTZType.TIMESTAMP_NTZ => sparktypes.DataTypes.TimestampNTZType
      case dt: DecimalType =>
        sparktypes.DecimalType(dt.getPrecision, dt.getScale)
      case at: ArrayType =>
        sparktypes.ArrayType(toSparkType(at.getElementType), at.containsNull())
      case mt: MapType =>
        sparktypes.MapType(
          toSparkType(mt.getKeyType),
          toSparkType(mt.getValueType),
          mt.isValueContainsNull)
      case st: StructType =>
        sparktypes.StructType(st.fields().asScala.map { field =>
          sparktypes.StructField(
            field.getName,
            toSparkType(field.getDataType),
            field.isNullable)
        }.toSeq)
    }
  }

  /**
   * Returns a URI encoded path of the resource.
   */
  def getTestResourceFilePath(resourcePath: String): String = {
    val resource = ResourceLoader.classLoader.getResource(resourcePath)
    if (resource == null) {
      throw new FileNotFoundException("resource not found")
    }
    resource.getFile
  }

  def checkpointFileExistsForTable(tablePath: String, versions: Int): Boolean =
    Files.exists(
      new File(FileNames.checkpointFileSingular(
        new Path(s"$tablePath/_delta_log"),
        versions).toString).toPath)

  def deleteChecksumFileForTable(tablePath: String, versions: Seq[Int]): Unit =
    versions.foreach(v =>
      Files.deleteIfExists(
        new File(FileNames.checksumFile(new Path(s"$tablePath/_delta_log"), v).toString).toPath))

  def rewriteChecksumFileToExcludeDomainMetadata(
      engine: Engine,
      tablePath: String,
      version: Long): Unit = {
    val logPath = new KernelPath(s"$tablePath/_delta_log");
    val crcInfo = ChecksumReader.getCRCInfo(
      engine,
      FileStatus.of(checksumFile(
        logPath,
        version).toString)).get()
    // Delete it in hdfs.
    engine.getFileSystemClient.delete(FileNames.checksumFile(
      new Path(s"$tablePath/_delta_log"),
      version).toString)
    val crcWriter = new ChecksumWriter(logPath)
    crcWriter.writeCheckSum(
      engine,
      new CRCInfo(
        crcInfo.getVersion,
        crcInfo.getMetadata,
        crcInfo.getProtocol,
        crcInfo.getTableSizeBytes,
        crcInfo.getNumFiles,
        crcInfo.getTxnId,
        /* domainMetadata */ Optional.empty(),
        crcInfo.getFileSizeHistogram))
  }

  def executeCrcSimple(result: TransactionCommitResult, engine: Engine): TransactionCommitResult = {
    result.getPostCommitHooks
      .stream()
      .filter(hook => hook.getType == PostCommitHookType.CHECKSUM_SIMPLE)
      .forEach(hook => hook.threadSafeInvoke(engine))
    result
  }

  /**
   * Verify checksum data matches the expected values in the snapshot.
   * @param snapshot Snapshot to verify the checksum against
   */
  protected def verifyChecksumForSnapshot(
      snapshot: Snapshot,
      expectEmptyTable: Boolean = false): Unit = {
    val logPath = snapshot.asInstanceOf[SnapshotImpl].getLogPath
    val crcInfoOpt = ChecksumReader.getCRCInfo(
      defaultEngine,
      FileStatus.of(checksumFile(
        logPath,
        snapshot.getVersion).toString))
    assert(
      crcInfoOpt.isPresent,
      s"CRC information should be present for version ${snapshot.getVersion}")
    crcInfoOpt.toScala.foreach { crcInfo =>
      // TODO: check metadata, protocol and file size.
      assert(
        crcInfo.getNumFiles === collectScanFileRows(snapshot.getScanBuilder.build()).size,
        "Number of files in checksum should match snapshot")
      if (expectEmptyTable) {
        assert(crcInfo.getTableSizeBytes == 0)
        crcInfo.getFileSizeHistogram.toScala.foreach { fileSizeHistogram =>
          assert(fileSizeHistogram == FileSizeHistogram.createDefaultHistogram)
        }
      }
      assert(
        crcInfo.getDomainMetadata === Optional.of(
          snapshot.asInstanceOf[SnapshotImpl].getActiveDomainMetadataMap.values().asScala
            .toSet
            .asJava),
        "Domain metadata in checksum should match snapshot")
    }
  }

  /**
   * Ensure checksum is readable by CRC reader, matches snapshot data, and can be regenerated.
   * This test verifies:
   * 1. The initial checksum exists and is correct
   * 2. After deleting the checksum file, it can be regenerated with the same content
   */
  def verifyChecksum(tablePath: String, expectEmptyTable: Boolean = false): Unit = {
    val currentSnapshot = latestSnapshot(tablePath, defaultEngine)
    val checksumVersion = currentSnapshot.getVersion

    // Step 1: Verify initial checksum
    verifyChecksumForSnapshot(currentSnapshot)

    // Step 2: Delete and regenerate the checksum
    defaultEngine.getFileSystemClient.delete(buildCrcPath(tablePath, checksumVersion).toString)
    Table.forPath(defaultEngine, tablePath).checksum(defaultEngine, checksumVersion)

    // Step 3: Verify regenerated checksum
    verifyChecksumForSnapshot(currentSnapshot)
  }

  protected def buildCrcPath(basePath: String, version: Long): java.nio.file.Path = {
    new File(FileNames.checksumFile(new Path(f"$basePath/_delta_log"), version).toString).toPath
  }
}
