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
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestRow, TestUtils}
import io.delta.kernel.expressions.Literal._
import io.delta.kernel.expressions.{Column, Literal, Predicate}
import io.delta.kernel.types.TimestampNTZType.TIMESTAMP_NTZ
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite

import java.math.{BigDecimal => BigDecimalJ}

class PartitionPruningSuite extends AnyFunSuite with TestUtils with ExpressionTestUtils {

  // scalastyle:off sparkimplicits
  // scalastyle:on sparkimplicits

  // Test golden table containing partition columns of all simple types
  val allTypesPartitionTable = goldenTablePath("data-reader-partition-values")

  // Test case to verify pruning on each partition column type works.
  // format: partition column reference -> (nonNullPartitionValues, nullPartitionValue)
  val testCasesAllTypes = Map(
    col("as_boolean") -> (ofBoolean(false), ofNull(BooleanType.BOOLEAN)),
    col("as_byte") -> (ofByte(1), ofNull(ByteType.BYTE)),
    col("as_short") -> (ofShort(1), ofNull(ShortType.SHORT)),
    col("as_int") -> (ofInt(1), ofNull(IntegerType.INTEGER)),
    col("as_long") -> (ofLong(1), ofNull(LongType.LONG)),
    col("as_float") -> (ofFloat(1), ofNull(FloatType.FLOAT)),
    col("as_double") -> (ofDouble(1), ofNull(DoubleType.DOUBLE)),
    // 2021-09-08 in days since epoch 18878
    col("as_date") -> (ofDate(18878 /* daysSinceEpochUTC */), ofNull(DateType.DATE)),
    col("as_string") -> (ofString("1", "UTF8_BINARY"), ofNull(StringType.STRING)),
    // 2021-09-08 11:11:11 in micros since epoch UTC
    col("as_timestamp") -> (ofTimestamp(1631099471000000L), ofNull(TimestampType.TIMESTAMP)),
    col("as_big_decimal") -> (
      ofDecimal(new BigDecimalJ(1), 1, 0),
      ofNull(new DecimalType(1, 0))))

  // Test for each partition column data type with partition value equal to non-null and null each
  // Try with or without selecting the partition column that has the predicate
  testCasesAllTypes.foreach {
    case (partitionCol, (nonNullLiteral, nullLiteral)) =>
      Seq(nonNullLiteral, nullLiteral).foreach { literal =>
        Seq(true, false).foreach { selectPredicatePartitionCol =>
          test(s"partition pruning: simple filter `$partitionCol = $literal`, " +
            s"select partition predicate column = $selectPredicatePartitionCol") {

            val isPartitionColDateOrTimestampType = literal.getDataType.isInstanceOf[DateType] ||
              literal.getDataType.isInstanceOf[TimestampType]

            val filter = predicate("=", partitionCol, literal)
            val expectedResult = if (literal.getValue == null) {
              Seq.empty // part1 == null should always return false - that means no results
            } else {
              if (selectPredicatePartitionCol) {
                if (isPartitionColDateOrTimestampType) {
                  // Date and timestamp type has two partitions with the same value in golden table
                  Seq((literal.getValue, 0L, "0"), (literal.getValue, 1L, "1"))
                } else {
                  Seq((literal.getValue, 1L, "1"))
                }
              } else {
                if (isPartitionColDateOrTimestampType) {
                  // Date and timestamp type has two partitions with the same value in golden table
                  Seq((0L, "0"), (1L, "1"))
                } else {
                  Seq((1L, "1"))
                }
              }
            }

            // "value" is a non-partition column
            val selectedColumns = if (selectPredicatePartitionCol) {
              Seq(partColName(partitionCol), "as_long", "value")
            } else {
              Seq("as_long", "value")
            }

            checkTable(
              path = allTypesPartitionTable,
              expectedAnswer = expectedResult.map(TestRow.fromTuple(_)),
              readCols = selectedColumns,
              filter = filter,
              expectedRemainingFilter = null)
          }
        }
      }
  }

  // Various combinations of predicate mix on partition and/or data columns mixes with AND or OR
  // test case format: (test_name, predicate) -> (remainingPredicate, expectedResults)
  // expected results is for query selecting `as_date` (partition column) and `value` (data column)
  val combinationTestCases = Map(
    ("partition pruning: with predicate on two different partition col combined with AND",
      and(
        predicate(">=", col("as_float"), ofFloat(-200)),
        predicate("=", col("as_date"), ofDate(18878 /* daysSinceEpochUTC */))
      )
    ) -> (null, Seq((18878, "0"), (18878, "1"))),

    (
      "partition pruning: with predicate on two different partition col combined with OR",
      or(
        predicate("=", col("as_float"), ofFloat(0)),
        predicate("=", col("as_int"), ofInt(1)))
    ) -> (null, Seq((18878, "0"), (18878, "1"))),

    (
      "partition pruning: with predicate on data and partition column mix with AND",
      and(
        predicate("=", col("as_value"), ofString("1", "UTF8_BINARY")), // data col filter
        predicate("=", col("as_float"), ofFloat(0)) // partition col filter
      )
    ) -> (
      predicate("=", col("as_value"), ofString("1", "UTF8_BINARY")),
      Seq((18878, "0"))
    ),

    (
      "partition pruning: with predicate on data and partition column mix with OR",
      or(
        predicate("=", col("as_value"), ofString("1", "UTF8_BINARY")), // data col filter
        predicate("=", col("as_float"), ofFloat(0)) // partition col filter
      )
    ) -> (
      or(
        predicate("=", col("as_value"), ofString("1", "UTF8_BINARY")), // data col filter
        predicate("=", col("as_float"), ofFloat(0)) // partition col filter
      ),
      Seq((18878, "0"), (18878, "1"), (null, "2"))
    ),

    (
      "partition pruning: partition predicate prunes everything",
      and(
        predicate("=", col("as_value"), ofString("200", "UTF8_BINARY")), // data col filter
        predicate("=", col("as_float"), ofFloat(234)) // partition col filter
      )
    ) -> (
      predicate("=", col("as_value"), ofString("200", "UTF8_BINARY")),
      Seq()
    )
  )

  combinationTestCases.foreach {
    case ((testTag, predicate), (expRemainingFilter, expResults)) =>
      test(testTag) {
        checkTable(
          path = allTypesPartitionTable,
          expectedAnswer = expResults.map(TestRow.fromTuple(_)),
          readCols = Seq("as_date", "value"),
          filter = predicate,
          expectedRemainingFilter = expRemainingFilter)
      }
  }

  Seq("name", "id").foreach { mode =>
    test(s"partition pruning on a column mapping enabled table: mode = $mode") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getCanonicalPath
        spark.sql(
          s"""CREATE TABLE delta.`$tablePath`(c1 long, c2 STRING, p1 STRING, p2 LONG)
             | USING delta PARTITIONED BY (p1, p2)
             | TBLPROPERTIES(
             |'delta.columnMapping.mode' = '$mode',
             |'delta.minReaderVersion' = '2',
             |'delta.minWriterVersion' = '5')
             |""".stripMargin)
        Seq.range(0, 5).foreach { i =>
          spark.sql(s"insert into delta.`$tablePath` values ($i, '$i', '$i', $i)")
        }

        checkTable(
          tablePath,
          expectedAnswer = Seq((3L, "3"), (4L, "4")).map(TestRow.fromTuple(_)),
          readCols = Seq("p2", "c2"),
          filter = predicate(">=", col("p2"), ofLong(3)),
          expectedRemainingFilter = null)
      }
    }
  }

  Seq("", "-name-mode", "-id-mode").foreach { cmMode =>
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

    // test case (kernel predicate object, equivalent spark predicate as string,
    //              expected row count, expected remaining filter)
    Seq(
      (
        // 1637202600123456L in epoch micros for '2021-11-18 02:30:00.123456'
        predicate("=", col("tsNtzPartition"), ofTimestampNtz(1637202600123456L)),
        "tsNtzPartition = '2021-11-18 02:30:00.123456'",
        3, // expected row count
        null.asInstanceOf[Predicate] // expected remaining filter
      ),
      (
        predicate("=", col("tsNtzPartition"), Literal ofNull (TIMESTAMP_NTZ)),
        "tsNtzPartition = null",
        0, // expected row count
        null.asInstanceOf[Predicate] // expected remaining filter
      ),
      (
        // 1373043660123456L in epoch micros for '2013-07-05 17:01:00.123456'
        predicate(">=", col("tsNtzPartition"), ofTimestampNtz(1373043660123456L)),
        "tsNtzPartition >= '2013-07-05 17:01:00'",
        6, // expected row count
        null.asInstanceOf[Predicate] // expected remaining filter
      ),
      (
        predicate("IS_NULL", col("tsNtzPartition")),
        "tsNtzPartition IS NULL",
        3, // expected row count
        null.asInstanceOf[Predicate] // expected remaining filter
      ),
      (
        // Filter on just the data column
        // 1637202600123456L in epoch micros for '2021-11-18 02:30:00.123456'
        predicate("OR",
          predicate("=", col("tsNtz"), ofTimestampNtz(1637202600123456L)),
          predicate("=", col("tsNtz"), ofTimestampNtz(1373043660123456L))),
        "",
        9, // expected row count
        // expected remaining filter
        predicate("OR",
          predicate("=", col("tsNtz"), ofTimestampNtz(1637202600123456L)),
          predicate("=", col("tsNtz"), ofTimestampNtz(1373043660123456L)))
      )
    ).foreach {
      case (kernelPredicate, sparkPredicate, expectedRowCount, expRemainingFilter) =>
        test(s"partition pruning on timestamp_ntz columns: $cmMode ($kernelPredicate)") {
          val tablePath = goldenTablePath(s"data-reader-timestamp_ntz$cmMode")
          val expectedResult = readUsingSpark(tablePath, sparkPredicate)
          assert(expectedResult.size === expectedRowCount)
          checkTable(
            expectedAnswer = expectedResult,
            path = tablePath,
            expectedRemainingFilter = expRemainingFilter,
            filter = kernelPredicate)
        }
    }
  }

  test("partition pruning from checkpoint") {
    withTempDir { path =>
      val tbl = "tbl"
      withTable(tbl) {
        // Create partitioned table and insert some data, ensuring that a checkpoint is created
        // after the last insertion.
        spark.sql(s"CREATE TABLE $tbl (a INT, b STRING) USING delta " +
          s"PARTITIONED BY (a) LOCATION '$path' " +
          s"TBLPROPERTIES ('delta.checkpointInterval' = '2')")
        spark.sql(s"INSERT INTO $tbl VALUES (1, 'a'), (2, 'b')")
        spark.sql(s"INSERT INTO $tbl VALUES (3, 'c'), (4, 'd')")
        spark.sql(s"INSERT INTO $tbl VALUES (5, 'e'), (6, 'f')")

        // Read from the source table with a partition predicate and validate the results.
        val result = readSnapshot(
          latestSnapshot(path.toString),
          filter = greaterThan(col("a"), Literal.ofInt(3)))
        checkAnswer(result, Seq(TestRow(4, "d"), TestRow(5, "e"), TestRow(6, "f")))
      }
    }
  }

  private def readUsingSpark(tablePath: String, predicate: String): Seq[TestRow] = {
    val where = if (predicate.isEmpty) "" else s"WHERE $predicate"
    spark.sql(s"SELECT * FROM delta.`$tablePath` $where")
      .collect()
      .map(TestRow(_))
  }

  private def partColName(column: Column): String = {
    assert(column.getNames.length == 1)
    column.getNames()(0)
  }
}
