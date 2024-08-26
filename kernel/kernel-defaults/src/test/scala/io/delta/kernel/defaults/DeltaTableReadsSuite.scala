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

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.exceptions.{InvalidTableException, KernelException, TableNotFoundException}
import io.delta.kernel.expressions.{Literal, ScalarExpression}
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.InternalUtils.daysSinceEpoch
import io.delta.kernel.internal.util.{DateTimeConstants, FileNames}
import io.delta.kernel.types.{BooleanType, LongType, StringType, StructType}
import io.delta.kernel.Table
import io.delta.kernel.data.ColumnVector
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.DefaultStringVector
import io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator
import org.apache.hadoop.shaded.org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.math.BigDecimal
import java.sql.Date
import java.util.Optional
import scala.collection.JavaConverters._

class DeltaTableReadsSuite extends AnyFunSuite with TestUtils {

  //////////////////////////////////////////////////////////////////////////////////
  // Timestamp type tests
  //////////////////////////////////////////////////////////////////////////////////

  // Below table is written in either UTC or PDT for the golden tables
  // Kernel always interprets partition timestamp columns in UTC
  /*
  id: int  | Part (TZ agnostic): timestamp     | time : timestamp
  ------------------------------------------------------------------------
  0        | 2020-01-01 08:09:10.001           | 2020-02-01 08:09:10
  1        | 2021-10-01 08:09:20               | 1999-01-01 09:00:00
  2        | 2021-10-01 08:09:20               | 2000-01-01 09:00:00
  3        | 1969-01-01 00:00:00               | 1969-01-01 00:00:00
  4        | null                              | null
  */

  def row0: TestRow = TestRow(
    0,
    1577866150001000L, // 2020-01-01 08:09:10.001 UTC to micros since the epoch
    1580544550000000L // 2020-02-01 08:09:10 UTC to micros since the epoch
  )

  def row1: TestRow = TestRow(
    1,
    1633075760000000L, // 2021-10-01 08:09:20 UTC to micros since the epoch
    915181200000000L // 1999-01-01 09:00:00 UTC to micros since the epoch
  )

  def row2: TestRow = TestRow(
    2,
    1633075760000000L, // 2021-10-01 08:09:20 UTC to micros since the epoch
    946717200000000L // 2000-01-01 09:00:00 UTC to micros since the epoch
  )

  def row3: TestRow = TestRow(
    3,
    -31536000000000L, // 1969-01-01 00:00:00  UTC to micros since the epoch
    -31536000000000L // 1969-01-01 00:00:00 UTC to micros since the epoch
  )

  def row4: TestRow = TestRow(
    4,
    null,
    null
  )

  def utcTableExpectedResult: Seq[TestRow] = Seq(row0, row1, row2, row3, row4)

  def testTimestampTable(
    goldenTableName: String,
    timeZone: String,
    expectedResult: Seq[TestRow]): Unit = {
    withTimeZone(timeZone) {
      checkTable(
        path = goldenTablePath(goldenTableName),
        expectedAnswer = expectedResult
      )
    }
  }

  for (timestampType <- Seq("INT96", "TIMESTAMP_MICROS", "TIMESTAMP_MILLIS")) {
    for (timeZone <- Seq("UTC", "Iceland", "PST", "America/Los_Angeles")) {
      test(
        s"end-to-end usage: timestamp table parquet timestamp format $timestampType tz $timeZone") {
        testTimestampTable("kernel-timestamp-" + timestampType, timeZone, utcTableExpectedResult)
      }
    }
  }

  // PST table - all the "time" col timestamps are + 8 hours
  def pstTableExpectedResult: Seq[TestRow] = utcTableExpectedResult.map { testRow =>
    val values = testRow.toSeq
    TestRow(
      values(0),
      // Partition columns are written as the local date time without timezone information and then
      // interpreted by Kernel in UTC --> so the written partition value (& the read value) is the
      // same as the UTC table
      values(1),
      if (values(2) == null) {
        null
      } else {
        values(2).asInstanceOf[Long] + DateTimeConstants.MICROS_PER_HOUR * 8
      }
    )
  }

  for (timeZone <- Seq("UTC", "Iceland", "PST", "America/Los_Angeles")) {
    test(s"end-to-end usage: timestamp in written in PST read in $timeZone") {
      testTimestampTable("kernel-timestamp-PST", timeZone, pstTableExpectedResult)
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Timestamp_NTZ tests
  //////////////////////////////////////////////////////////////////////////////////

  // Below is the golden table used in test
  // (INTEGER id, TIMESTAMP_NTZ tsNtz, TIMESTAMP_NTZ tsNtzPartition)
  // (0, '2021-11-18 02:30:00.123456','2021-11-18 02:30:00.123456'),
  // (1, '2013-07-05 17:01:00.123456','2021-11-18 02:30:00.123456'),
  // (2, NULL,                         '2021-11-18 02:30:00.123456'),
  // (3, '2021-11-18 02:30:00.123456','2013-07-05 17:01:00.123456'),
  // (4, '2013-07-05 17:01:00.123456','2013-07-05 17:01:00.123456'),
  // (5, NULL,                        '2013-07-05 17:01:00.123456'),
  // (6, '2021-11-18 02:30:00.123456', NULL),
  // (7, '2013-07-05 17:01:00.123456', NULL),
  // (8, NULL,                         NULL)
  val expectedTimestampNtzTestRows = Seq(
    TestRow(0, 1637202600123456L, 1637202600123456L),
    TestRow(1, 1373043660123456L, 1637202600123456L),
    TestRow(2, null, 1637202600123456L),
    TestRow(3, 1637202600123456L, 1373043660123456L),
    TestRow(4, 1373043660123456L, 1373043660123456L),
    TestRow(5, null, 1373043660123456L),
    TestRow(6, 1637202600123456L, null),
    TestRow(7, 1373043660123456L, null),
    TestRow(8, null, null)
  )

  Seq("", "-name-mode", "-id-mode").foreach { cmMode =>
    test(s"end-to-end: read table with timestamp_ntz columns (including partition): $cmMode") {
      checkTable(
        path = goldenTablePath(s"data-reader-timestamp_ntz$cmMode"),
        expectedAnswer = expectedTimestampNtzTestRows)
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Decimal type tests
  //////////////////////////////////////////////////////////////////////////////////

  for (tablePath <- Seq("basic-decimal-table", "basic-decimal-table-legacy")) {
    test(s"end to end: reading $tablePath") {
      val expectedResult = Seq(
        ("234.00000", "1.00", "2.00000", "3.0000000000"),
        ("2342222.23454", "111.11", "22222.22222", "3333333333.3333333333"),
        ("0.00004", "0.00", "0.00000", "0E-10"),
        ("-2342342.23423", "-999.99", "-99999.99999", "-9999999999.9999999999")
      ).map { tup =>
        (new BigDecimal(tup._1), new BigDecimal(tup._2), new BigDecimal(tup._3),
          new BigDecimal(tup._4))
      }

      checkTable(
        path = goldenTablePath(tablePath),
        expectedAnswer = expectedResult.map(TestRow.fromTuple(_))
      )
    }
  }

  test(s"end to end: reading decimal-various-scale-precision") {
    val tablePath = goldenTablePath("decimal-various-scale-precision")
    val expResults = spark.sql(s"SELECT * FROM delta.`$tablePath`")
      .collect()
      .map(TestRow(_))

    checkTable(
      path = goldenTablePath("decimal-various-scale-precision"),
      expectedAnswer = expResults
    )
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Table/Snapshot tests
  //////////////////////////////////////////////////////////////////////////////////

  test("invalid path") {
    val invalidPath = "/path/to/non-existent-directory"
    val table = Table.forPath(defaultEngine, invalidPath)

    def expectTableNotFoundException(fn: () => Unit): Unit = {
      val ex = intercept[TableNotFoundException] {
        fn()
      }
      assert(ex.getMessage().contains(s"Delta table at path `file:$invalidPath` is not found"))
    }

    expectTableNotFoundException(() => table.getLatestSnapshot(defaultEngine))
    expectTableNotFoundException(() =>
      table.getSnapshotAsOfTimestamp(defaultEngine, 1))
    expectTableNotFoundException(() =>
      table.getSnapshotAsOfVersion(defaultEngine, 1))
  }

  test("table deleted after the `Table` creation") {
    withTempDir { temp =>
      val source = new File(goldenTablePath("data-reader-primitives"))
      val target = new File(temp.getCanonicalPath)
      FileUtils.copyDirectory(source, target)

      val table = Table.forPath(defaultEngine, target.getCanonicalPath)
      // delete the table and try to get the snapshot. Expect a failure.
      FileUtils.deleteDirectory(target)
      val ex = intercept[TableNotFoundException] {
        table.getLatestSnapshot(defaultEngine)
      }
      assert(ex.getMessage.contains(
        s"Delta table at path `file:${target.getCanonicalPath}` is not found"))
    }
  }

  // TODO for the below, when should we throw an exception? #2253
  //   - on Table creation?
  //   - on Snapshot creation?

  test("empty _delta_log folder") {
    withTempDir { dir =>
      new File(dir, "_delta_log").mkdirs()
      intercept[TableNotFoundException] {
        latestSnapshot(dir.getAbsolutePath)
      }
    }
  }

  test("empty folder with no _delta_log dir") {
    withTempDir { dir =>
      intercept[TableNotFoundException] {
        latestSnapshot(dir.getAbsolutePath)
      }
    }
  }

  test("non-empty folder not a delta table") {
    intercept[TableNotFoundException] {
      latestSnapshot(goldenTablePath("no-delta-log-folder"))
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Misc tests
  //////////////////////////////////////////////////////////////////////////////////

  test("end to end: multi-part checkpoint") {
    checkTable(
      path = goldenTablePath("multi-part-checkpoint"),
      expectedAnswer = (Seq(0L) ++ (0L until 30L)).map(TestRow(_))
    )
  }

  test("read partitioned table") {
    val path = "file:" + goldenTablePath("data-reader-partition-values")

    // for now we don't support timestamp type partition columns so remove from read columns
    val readCols = Table.forPath(defaultEngine, path).getLatestSnapshot(defaultEngine)
      .getSchema(defaultEngine)
      .withoutField("as_timestamp")
      .fields()
      .asScala
      .map(_.getName)

    val expectedAnswer = Seq(0, 1).map { i =>
      TestRow(
        i,
        i.toLong,
        i.toByte,
        i.toShort,
        i % 2 == 0,
        i.toFloat,
        i.toDouble,
        i.toString,
        "null",
        daysSinceEpoch(Date.valueOf("2021-09-08")),
        new BigDecimal(i),
        Seq(TestRow(i), TestRow(i), TestRow(i)),
        TestRow(i.toString, i.toString, TestRow(i, i.toLong)),
        i.toString
      )
    } ++ (TestRow(
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      Seq(TestRow(2), TestRow(2), TestRow(2)),
      TestRow("2", "2", TestRow(2, 2L)),
      "2"
    ) :: Nil)

    checkTable(
      path = path,
      expectedAnswer = expectedAnswer,
      readCols = readCols
    )
  }

  test("table with complex array types") {
    val path = "file:" + goldenTablePath("data-reader-array-complex-objects")

    val expectedAnswer = (0 until 10).map { i =>
      TestRow(
        i,
        Seq(Seq(Seq(i, i, i), Seq(i, i, i)), Seq(Seq(i, i, i), Seq(i, i, i))),
        Seq(
          Seq(Seq(Seq(i, i, i), Seq(i, i, i)), Seq(Seq(i, i, i), Seq(i, i, i))),
          Seq(Seq(Seq(i, i, i), Seq(i, i, i)), Seq(Seq(i, i, i), Seq(i, i, i)))
        ),
        Seq(
          Map[String, Long](i.toString -> i.toLong),
          Map[String, Long](i.toString -> i.toLong)
        ),
        Seq(TestRow(i), TestRow(i), TestRow(i))
      )
    }

    checkTable(
      path = path,
      expectedAnswer = expectedAnswer
    )
  }

  Seq("name", "id").foreach { columnMappingMode =>
    test(s"table with `$columnMappingMode` column mapping mode") {
      val path = goldenTablePath(s"table-with-columnmapping-mode-$columnMappingMode")

      val expectedAnswer = (0 until 5).map { i =>
        TestRow(
          i.byteValue(),
          i.shortValue(),
          i,
          i.longValue(),
          i.floatValue(),
          i.doubleValue(),
          new java.math.BigDecimal(i),
          i % 2 == 0,
          i.toString,
          i.toString.getBytes,
          daysSinceEpoch(Date.valueOf("2021-11-18")), // date in days
          (i * 1000).longValue(), // timestamp in micros
          TestRow(i.toString, TestRow(i)), // nested_struct
          Seq(i, i + 1), // array_of_prims
          Seq(Seq(i, i + 1), Seq(i + 2, i + 3)), // array_of_arrays
          Seq(Map(i -> Seq(2, 3), i + 1 -> Seq(4, 5))), // array_of_map_of_arrays
          Seq(TestRow(i), TestRow(i)), // array_of_structs
          TestRow( // struct_of_arrays_maps_of_structs
            Seq(i, i + 1),
            Map(Seq(i, i + 1) -> TestRow(i + 2))
          ),
          Map(
            i -> (i + 1).longValue(),
            (i + 2) -> (i + 3).longValue()
          ), // map_of_prims
          Map(i + 1 -> TestRow((i * 20).longValue())), // map_of_rows
          {
            val val1 = Seq(i, null, i + 1)
            val val2 = Seq[Integer]()
            Map(
              i.longValue() -> val1,
              (i + 1).longValue() -> val2
            ) // map_of_arrays
          },
          Map( // map_of_maps
            i.toLong -> Map(i -> i),
            (i + 1).toLong -> Map(i + 2 -> i)
          )
        )
      } ++ Seq(TestRow(Seq.fill(22)(null): _*)) // all nulls row, 22 columns

      checkTable(
        path = path,
        expectedAnswer = expectedAnswer
      )
    }
  }

  Seq("name", "id").foreach { columnMappingMode =>
    test(s"table with `$columnMappingMode` column mapping mode - read subset of columns") {
      val path = goldenTablePath(s"table-with-columnmapping-mode-$columnMappingMode")

      val expectedAnswer = (0 until 5).map { i =>
        TestRow(
          i.byteValue(),
          new java.math.BigDecimal(i),
          TestRow(i.toString, TestRow(i)), // nested_struct
          Seq(i, i + 1), // array_of_prims
          Map(
            i -> (i + 1).longValue(),
            (i + 2) -> (i + 3).longValue()
          ) // map_of_prims
        )
      } ++ (TestRow(
        null,
        null,
        null,
        null,
        null
      ) :: Nil)

      checkTable(
        path = path,
        expectedAnswer = expectedAnswer,
        readCols = Seq("ByteType", "decimal", "nested_struct", "array_of_prims", "map_of_prims")
      )
    }
  }

  test("table with complex map types") {
    val path = "file:" + goldenTablePath("data-reader-map")

    val expectedAnswer = (0 until 10).map { i =>
      TestRow(
        i,
        Map(i -> i),
        Map(i.toLong -> i.toByte),
        Map(i.toShort -> (i % 2 == 0)),
        Map(i.toFloat -> i.toDouble),
        Map(i.toString -> new BigDecimal(i)),
        Map(i -> Seq(TestRow(i), TestRow(i), TestRow(i)))
      )
    }

    checkTable(
      path = path,
      expectedAnswer = expectedAnswer
    )
  }

  test("table with array of primitives") {
    val expectedAnswer = (0 until 10).map { i =>
      TestRow(
        Seq(i), Seq(i.toLong), Seq(i.toByte), Seq(i.toShort),
        Seq(i % 2 == 0), Seq(i.toFloat), Seq(i.toDouble), Seq(i.toString),
        Seq(Array(i.toByte, i.toByte)), Seq(new BigDecimal(i))
      )
    }
    checkTable(
      path = goldenTablePath("data-reader-array-primitives"),
      expectedAnswer = expectedAnswer
    )
  }

  test("table primitives") {
    val expectedAnswer = (0 to 10).map {
      case 10 => TestRow(null, null, null, null, null, null, null, null, null, null)
      case i => TestRow(
        i,
        i.toLong,
        i.toByte,
        i.toShort,
        i % 2 == 0,
        i.toFloat,
        i.toDouble,
        i.toString,
        Array[Byte](i.toByte, i.toByte),
        new BigDecimal(i)
      )
    }

    checkTable(
      path = goldenTablePath("data-reader-primitives"),
      expectedAnswer = expectedAnswer
    )
  }

  test("table with checkpoint") {
    checkTable(
      path = getTestResourceFilePath("basic-with-checkpoint"),
      expectedAnswer = (0 until 150).map(i => TestRow(i.toLong))
    )
  }

  test(s"table with spaces in the table path") {
    withTempDir { tempDir =>
      val target = tempDir.getCanonicalPath + s"/table- -path"
      spark.sql(s"CREATE TABLE delta.`$target` USING DELTA " +
        s"SELECT * FROM delta.`${getTestResourceFilePath("basic-with-checkpoint")}`")
      checkTable(
        path = target,
        expectedAnswer = (0 until 150).map(i => TestRow(i.toLong))
      )
    }
  }

  test("table with name column mapping mode") {
    val expectedAnswer = (0 to 10).map {
      case 10 => TestRow(null, null, null, null, null, null, null, null, null, null)
      case i => TestRow(
        i,
        i.toLong,
        i.toByte,
        i.toShort,
        i % 2 == 0,
        i.toFloat,
        i.toDouble,
        i.toString,
        Array[Byte](i.toByte, i.toByte),
        new BigDecimal(i)
      )
    }

    checkTable(
      path = getTestResourceFilePath("data-reader-primitives-column-mapping-name"),
      expectedAnswer = expectedAnswer
    )
  }

  test("partitioned table with column mapping") {
    val expectedAnswer = (0 to 2).map {
      case 2 => TestRow(null, null, "2")
      case i => TestRow(i, i.toDouble, i.toString)
    }
    val readCols = Seq(
      // partition fields
      "as_int",
      "as_double",
      // data fields
      "value"
    )

    checkTable(
      path = getTestResourceFilePath("data-reader-partition-values-column-mapping-name"),
      readCols = readCols,
      expectedAnswer = expectedAnswer
    )
  }

  test("simple end to end with vacuum protocol check feature") {
    val expectedValues = (0 until 100).map(x => (x, s"val=$x"))
    checkTable(
      path = goldenTablePath("basic-with-vacuum-protocol-check-feature"),
      expectedAnswer = expectedValues.map(TestRow.fromTuple))
  }

  test("table with nested struct") {
    val expectedAnswer = (0 until 10).map { i =>
      TestRow(TestRow(i.toString, i.toString, TestRow(i, i.toLong)), i)
    }
    checkTable(
      path = goldenTablePath("data-reader-nested-struct"),
      expectedAnswer = expectedAnswer
    )
  }

  test("table with empty parquet files") {
    checkTable(
      path = goldenTablePath("125-iterator-bug"),
      expectedAnswer = (1 to 5).map(TestRow(_))
    )
  }

  test("handle corrupted '_last_checkpoint' file") {
    checkTable(
      path = goldenTablePath("corrupted-last-checkpoint-kernel"),
      expectedAnswer = (0L until 100L).map(TestRow(_))
    )
  }

  test("error - version not contiguous") {
    val e = intercept[InvalidTableException] {
      latestSnapshot(goldenTablePath("versions-not-contiguous"))
    }
    assert(e.getMessage.contains("versions are not continuous: ([0, 2])"))
  }

  test("table protocol version greater than reader protocol version") {
    val e = intercept[Exception] {
      latestSnapshot(goldenTablePath("deltalog-invalid-protocol-version"))
        .getScanBuilder(defaultEngine)
        .build()
    }
    assert(e.getMessage.contains("Unsupported Delta protocol reader version"))
  }

  test("table with void type - throws KernelException") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      spark.sql(s"CREATE TABLE delta.`${tempDir.getAbsolutePath}`(x INTEGER, y VOID) USING DELTA")
      val e = intercept[KernelException] {
        latestSnapshot(path)
      }
      assert(e.getMessage.contains(
        "Failed to parse the schema. Encountered unsupported Delta data type: VOID"))
    }
  }

  test("read a shallow cloned table") {
    withTempDir { tempDir =>
      val target = tempDir.getCanonicalPath
      val source = goldenTablePath("data-reader-partition-values")
      spark.sql(s"CREATE TABLE delta.`$target` SHALLOW CLONE delta.`$source`")

      withSparkTimeZone("UTC") {
        val expAnswer = spark.read.format("delta").load(source).collect().map(TestRow(_)).toSeq
        assert(expAnswer.size == 3)
        checkTable(target, expAnswer)
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // getSnapshotAtVersion end-to-end tests (log segment tests in SnapshotManagerSuite)
  //////////////////////////////////////////////////////////////////////////////////

  test("getSnapshotAtVersion: basic end-to-end read") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      (0 to 10).foreach { i =>
        spark.range(i*10, i*10 + 10).write
          .format("delta")
          .mode("append")
          .save(path)
      }
      // Read a checkpoint version
      checkTable(
        path = path,
        expectedAnswer = (0L to 99L).map(TestRow(_)),
        version = Some(9),
        expectedVersion = Some(9)
      )
      // Read a JSON version
      checkTable(
        path = path,
        expectedAnswer = (0L to 89L).map(TestRow(_)),
        version = Some(8),
        expectedVersion = Some(8)
      )
      // Read the current version
      checkTable(
        path = path,
        expectedAnswer = (0L to 109L).map(TestRow(_)),
        version = Some(10),
        expectedVersion = Some(10)
      )
      // Cannot read a version that does not exist
      val e = intercept[RuntimeException] {
        Table.forPath(defaultEngine, path)
          .getSnapshotAsOfVersion(defaultEngine, 11)
      }
      assert(e.getMessage.contains(
        "Cannot load table version 11 as it does not exist. The latest available version is 10"))
    }
  }

  test("getSnapshotAtVersion: end-to-end test with truncated delta log") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath
      // Write versions [0, 10] (inclusive) including a checkpoint
      (0 to 10).foreach { i =>
        spark.range(i*10, i*10 + 10).write
          .format("delta")
          .mode("append")
          .save(tablePath)
      }
      val log = org.apache.spark.sql.delta.DeltaLog.forTable(
        spark, new org.apache.hadoop.fs.Path(tablePath))
      val deltaCommitFileProvider = org.apache.spark.sql.delta.util.DeltaCommitFileProvider(
        log.unsafeVolatileSnapshot)
      // Delete the log files for versions 0-9, truncating the table history to version 10
      (0 to 9).foreach { i =>
        val jsonFile = deltaCommitFileProvider.deltaFile(i)
        new File(new org.apache.hadoop.fs.Path(log.logPath, jsonFile).toUri).delete()
      }
      // Create version 11 that overwrites the whole table
      spark.range(50).write
        .format("delta")
        .mode("overwrite")
        .save(tablePath)

      // Cannot read a version that has been truncated
      val e = intercept[RuntimeException] {
        Table.forPath(defaultEngine, tablePath)
          .getSnapshotAsOfVersion(defaultEngine, 9)
      }
      assert(e.getMessage.contains("Cannot load table version 9"))
      // Can read version 10
      checkTable(
        path = tablePath,
        expectedAnswer = (0L to 109L).map(TestRow(_)),
        version = Some(10),
        expectedVersion = Some(10)
      )
      // Can read version 11
      checkTable(
        path = tablePath,
        expectedAnswer = (0L until 50L).map(TestRow(_)),
        version = Some(11),
        expectedVersion = Some(11)
      )
    }
  }

  test("time travel with schema change") {
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.getCanonicalPath)
      spark.range(10, 20).withColumn("part", col("id"))
        .write.format("delta").mode("append").option("mergeSchema", true)
        .save(tempDir.getCanonicalPath)
      checkTable(
        path = tempDir.getCanonicalPath,
        expectedAnswer = (0L until 10L).map(TestRow(_)),
        expectedSchema = new StructType().add("id", LongType.LONG),
        version = Some(0),
        expectedVersion = Some(0)
      )
    }
  }

  test("time travel with partition change") {
    withTempDir { tempDir =>
      spark.range(10).withColumn("part5", col("id") % 5)
        .write.format("delta").partitionBy("part5").mode("append")
        .save(tempDir.getCanonicalPath)
      spark.range(10, 20).withColumn("part2", col("id") % 2)
        .write
        .format("delta")
        .partitionBy("part2")
        .mode("overwrite")
        .option("overwriteSchema", true)
        .save(tempDir.getCanonicalPath)
      checkTable(
        path = tempDir.getCanonicalPath,
        expectedAnswer = (0L until 10L).map(v => TestRow(v, v % 5)),
        expectedSchema = new StructType()
          .add("id", LongType.LONG)
          .add("part5", LongType.LONG),
        version = Some(0),
        expectedVersion = Some(0)
      )
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // getSnapshotAtTimestamp end-to-end tests (more tests in DeltaHistoryManagerSuite)
  //////////////////////////////////////////////////////////////////////////////////

  private def generateCommits(path: String, commits: Long*): Unit = {
    commits.zipWithIndex.foreach { case (ts, i) =>
      spark.range(i*10, i*10 + 10).write.format("delta").mode("append").save(path)
      val file = new File(FileNames.deltaFile(new Path(path, "_delta_log"), i))
      file.setLastModified(ts)
    }
  }

  test("getSnapshotAtTimestamp: basic end-to-end read") {
    withTempDir { tempDir =>
      val start = 1540415658000L
      val minuteInMilliseconds = 60000L
      generateCommits(tempDir.getCanonicalPath, start, start + 20 * minuteInMilliseconds,
        start + 40 * minuteInMilliseconds)
      // Exact timestamp for version 0
      checkTable(
        path = tempDir.getCanonicalPath,
        expectedAnswer = (0L until 10L).map(TestRow(_)),
        timestamp = Some(start),
        expectedVersion = Some(0)
      )
      // Timestamp between version 0 and 1 should load version 0
      checkTable(
        path = tempDir.getCanonicalPath,
        expectedAnswer = (0L until 10L).map(TestRow(_)),
        timestamp = Some(start + 10 * minuteInMilliseconds),
        expectedVersion = Some(0)
      )
      // Exact timestamp for version 1
      checkTable(
        path = tempDir.getCanonicalPath,
        expectedAnswer = (0L until 20L).map(TestRow(_)),
        timestamp = Some(start + 20 * minuteInMilliseconds),
        expectedVersion = Some(1)
      )
      // Exact timestamp for the last version
      checkTable(
        path = tempDir.getCanonicalPath,
        expectedAnswer = (0L until 30L).map(TestRow(_)),
        timestamp = Some(start + 40 * minuteInMilliseconds),
        expectedVersion = Some(2)
      )
      // Timestamp after last commit fails
      val e1 = intercept[RuntimeException] {
        checkTable(
          path = tempDir.getCanonicalPath,
          expectedAnswer = Seq(),
          timestamp = Some(start + 50 * minuteInMilliseconds)
        )
      }
      assert(e1.getMessage.contains(
        s"The provided timestamp ${start + 50 * minuteInMilliseconds} ms " +
          s"(2018-10-24T22:04:18Z) is after the latest available version"))
      // Timestamp before the first commit fails
      val e2 = intercept[RuntimeException] {
        checkTable(
          path = tempDir.getCanonicalPath,
          expectedAnswer = Seq(),
          timestamp = Some(start - 1L)
        )
      }
      assert(e2.getMessage.contains(
        s"The provided timestamp ${start - 1L} ms (2018-10-24T21:14:17.999Z) is before " +
          s"the earliest available version"))
    }
  }

  test("getSnapshotAtTimestamp: empty _delta_log folder") {
    withTempDir { dir =>
      new File(dir, "_delta_log").mkdirs()
      intercept[TableNotFoundException] {
        Table.forPath(defaultEngine, dir.getCanonicalPath)
          .getSnapshotAsOfTimestamp(defaultEngine, 0L)
      }
    }
  }

  test("getSnapshotAtTimestamp: empty folder no _delta_log dir") {
    withTempDir { dir =>
      intercept[TableNotFoundException] {
        Table.forPath(defaultEngine, dir.getCanonicalPath)
          .getSnapshotAsOfTimestamp(defaultEngine, 0L)
      }
    }
  }

  test("getSnapshotAtTimestamp: non-empty folder not a delta table") {
    withTempDir { dir =>
      spark.range(20).write.format("parquet").mode("overwrite").save(dir.getCanonicalPath)
      intercept[TableNotFoundException] {
        Table.forPath(defaultEngine, dir.getCanonicalPath)
          .getSnapshotAsOfTimestamp(defaultEngine, 0L)
      }
    }
  }
}
