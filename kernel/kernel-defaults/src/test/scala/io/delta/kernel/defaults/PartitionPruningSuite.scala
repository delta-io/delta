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

import java.math.{BigDecimal => BigDecimalJ}
import scala.collection.JavaConverters._
import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.expressions.{Column, Expression, Predicate}
import io.delta.kernel.expressions.Literal._
import io.delta.kernel.types._
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.scalatest.funsuite.AnyFunSuite

class PartitionPruningSuite extends AnyFunSuite with TestUtils {

  // scalastyle:off sparkimplicits
  import spark.implicits._
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
    col("as_string") -> (ofString("1"), ofNull(StringType.STRING)),
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
        predicate("=", col("as_value"), ofString("1")), // data col filter
        predicate("=", col("as_float"), ofFloat(0)) // partition col filter
      )
    ) -> (
      predicate("=", col("as_value"), ofString("1")),
      Seq((18878, "0"))
    ),

    (
      "partition pruning: with predicate on data and partition column mix with OR",
      or(
        predicate("=", col("as_value"), ofString("1")), // data col filter
        predicate("=", col("as_float"), ofFloat(0)) // partition col filter
      )
    ) -> (
      or(
        predicate("=", col("as_value"), ofString("1")), // data col filter
        predicate("=", col("as_float"), ofFloat(0)) // partition col filter
      ),
      Seq((18878, "0"), (18878, "1"), (null, "2"))
    ),

    (
      "partition pruning: partition predicate prunes everything",
      and(
        predicate("=", col("as_value"), ofString("200")), // data col filter
        predicate("=", col("as_float"), ofFloat(234)) // partition col filter
      )
    ) -> (
      predicate("=", col("as_value"), ofString("200")),
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

  private def col(names: String*): Column = new Column(names.toArray)

  private def partColName(column: Column): String = {
    assert(column.getNames.length == 1)
    column.getNames()(0)
  }

  private def predicate(name: String, children: Expression*): Predicate =
    new Predicate(name, children.asJava)

  private def and(left: Predicate, right: Predicate) = predicate("AND", left, right)

  private def or(left: Predicate, right: Predicate) = predicate("OR", left, right)
}
