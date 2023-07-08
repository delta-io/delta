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

package org.apache.spark.sql.delta.perf

import java.sql.Timestamp
import java.util.Locale
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.util.matching.Regex

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta._

import org.apache.spark.sql.delta.sources.DeltaSQLConf.{DELTA_COLLECT_STATS, GENERATED_COLUMN_PARTITION_FILTER_OPTIMIZATION_ENABLED}
import org.apache.spark.sql.delta.stats.PrepareDeltaScanBase
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.Utils

class OptimizeGeneratedColumnSuite extends GeneratedColumnTest {
  import testImplicits._

  private val regex = new Regex(s"(\\S+)\\s(\\S+)\\sGENERATED\\sALWAYS\\sAS\\s\\((.*)\\s?\\)",
  "col_name", "data_type", "generated_as")

  private def getPushedPartitionFilters(queryExecution: QueryExecution): Seq[Expression] = {
    queryExecution.executedPlan.collectFirst {
      case scan: FileSourceScanExec => scan.partitionFilters
    }.getOrElse(Nil)
  }

  protected def insertInto(path: String, df: DataFrame) = {
    df.write.format("delta").mode("append").save(path)
  }

  /**
   * Verify we can recognize an `OptimizablePartitionExpression` and generate corresponding
   * partition filters correctly.
   *
   * @param dataSchema DDL of the data columns
   * @param partitionSchema DDL of the partition columns
   * @param generatedColumns a map of generated partition columns defined using the above data
   *                         columns
   * @param expectedPartitionExpr the expected `OptimizablePartitionExpression` to be recognized
   * @param auxiliaryTestName string to append to the generated test name
   * @param expressionKey key to check for the optmizable expression if not the default first
   *                      word in the data schema
   * @param skipNested Whether to skip the nested variant of the test
   * @param filterTestCases test cases for partition filters. The key is the data filter, and the
   *                        value is the partition filters we should generate.
   */
  private def testOptimizablePartitionExpression(
      dataSchema: String,
      partitionSchema: String,
      generatedColumns: Map[String, String],
      expectedPartitionExpr: OptimizablePartitionExpression,
      auxiliaryTestName: Option[String] = None,
      expressionKey: Option[String] = None,
      skipNested: Boolean = false,
      filterTestCases: Seq[(String, Seq[String])]): Unit = {
    test(expectedPartitionExpr.toString + auxiliaryTestName.getOrElse("")) {
      val normalCol = dataSchema.split(" ")(0)

      withTableName("optimizable_partition_expression") { table =>
        createTable(
          table,
          None,
          s"$dataSchema, $partitionSchema",
          generatedColumns,
          generatedColumns.keys.toSeq
        )

        val metadata = DeltaLog.forTable(spark, TableIdentifier(table)).snapshot.metadata
        assert(metadata.optimizablePartitionExpressions(expressionKey.getOrElse(
          normalCol).toLowerCase(Locale.ROOT)) == expectedPartitionExpr :: Nil)
        filterTestCases.foreach { filterTestCase =>
          val partitionFilters = getPushedPartitionFilters(
            sql(s"SELECT * from $table where ${filterTestCase._1}").queryExecution)
          assert(partitionFilters.map(_.sql) == filterTestCase._2)
        }
      }
    }

    if (!skipNested) {
      test(expectedPartitionExpr.toString + auxiliaryTestName.getOrElse("") + " nested") {
        val normalCol = dataSchema.split(" ")(0)
        val nestedSchema = s"nested struct<${dataSchema.replace(" ", ": ")}>"
        val updatedGeneratedColumns =
          generatedColumns.mapValues(v => v.replaceAll(s"(?i)($normalCol)", "nested.$1")).toMap

        withTableName("optimizable_partition_expression") { table =>
          createTable(
            table,
            None,
            s"$nestedSchema, $partitionSchema",
            updatedGeneratedColumns,
            updatedGeneratedColumns.keys.toSeq
          )

          val metadata = DeltaLog.forTable(spark, TableIdentifier(table)).snapshot.metadata
          val nestedColPath =
            s"nested.${expressionKey.getOrElse(normalCol).toLowerCase(Locale.ROOT)}"
          assert(metadata.optimizablePartitionExpressions(nestedColPath)
            == expectedPartitionExpr :: Nil)
          filterTestCases.foreach { filterTestCase =>
            val updatedFilter = filterTestCase._1.replaceAll(s"(?i)($normalCol)", "nested.$1")
            val partitionFilters = getPushedPartitionFilters(
              sql(s"SELECT * from $table where $updatedFilter").queryExecution)
            assert(partitionFilters.map(_.sql) == filterTestCase._2)
          }
        }
      }
    }
  }

  /** Format a human readable SQL filter into Spark's compact SQL format */
  private def compactFilter(filter: String): String = {
    filter.replaceAllLiterally("\n", "")
      .replaceAll("(?<=\\)) +(?=\\))", "")
      .replaceAll("(?<=\\() +(?=\\()", "")
      .replaceAll("\\) +OR +\\(", ") OR (")
      .replaceAll("\\) +AND +\\(", ") AND (")
  }

  testOptimizablePartitionExpression(
    "eventTime TIMESTAMP",
    "date DATE",
    Map("date" -> "CAST(eventTime AS DATE)"),
    expectedPartitionExpr = DatePartitionExpr("date"),
    auxiliaryTestName = Option(" from cast(timestamp)"),
    filterTestCases = Seq(
      "eventTime < '2021-01-01 18:00:00'" ->
        Seq("((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "eventTime <= '2021-01-01 18:00:00'" ->
        Seq("((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "eventTime = '2021-01-01 18:00:00'" ->
        Seq("((date = CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date = CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "eventTime > '2021-01-01 18:00:00'" ->
        Seq("((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "eventTime >= '2021-01-01 18:00:00'" ->
        Seq("((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "eventTime is null" -> Seq("(date IS NULL)"),
      // Verify we can reverse the order
      "'2021-01-01 18:00:00' > eventTime" ->
        Seq("((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "'2021-01-01 18:00:00' >= eventTime" ->
        Seq("((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "'2021-01-01 18:00:00' = eventTime" ->
        Seq("((date = CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date = CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "'2021-01-01 18:00:00' < eventTime" ->
        Seq("((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      "'2021-01-01 18:00:00' <= eventTime" ->
        Seq("((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
          "OR ((date >= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"),
      // Verify date type literal. In theory, the best filter should be date < DATE '2021-01-01'.
      // But Spark's analyzer converts eventTime < '2021-01-01' to
      // `eventTime` < TIMESTAMP '2021-01-01 00:00:00'. So it's the same as
      // eventTime < '2021-01-01 18:00:00' for `OptimizeGeneratedColumn`.
      "eventTime < '2021-01-01'" ->
        Seq("((date <= CAST(TIMESTAMP '2021-01-01 00:00:00' AS DATE)) " +
          "OR ((date <= CAST(TIMESTAMP '2021-01-01 00:00:00' AS DATE)) IS NULL))")
    )
  )

  testOptimizablePartitionExpression(
    "eventDate DATE",
    "date DATE",
    Map("date" -> "CAST(eventDate AS DATE)"),
    expectedPartitionExpr = DatePartitionExpr("date"),
    auxiliaryTestName = Option(" from cast(date)"),
    filterTestCases = Seq(
      "eventDate < '2021-01-01 18:00:00'" ->
        Seq("((date <= DATE '2021-01-01') " +
          "OR ((date <= DATE '2021-01-01') IS NULL))"),
      "eventDate <= '2021-01-01 18:00:00'" ->
        Seq("((date <= DATE '2021-01-01') " +
          "OR ((date <= DATE '2021-01-01') IS NULL))"),
      "eventDate = '2021-01-01 18:00:00'" ->
        Seq("((date = DATE '2021-01-01') " +
          "OR ((date = DATE '2021-01-01') IS NULL))"),
      "eventDate > '2021-01-01 18:00:00'" ->
        Seq("((date >= DATE '2021-01-01') " +
          "OR ((date >= DATE '2021-01-01') IS NULL))"),
      "eventDate >= '2021-01-01 18:00:00'" ->
        Seq("((date >= DATE '2021-01-01') " +
          "OR ((date >= DATE '2021-01-01') IS NULL))"),
      "eventDate is null" -> Seq("(date IS NULL)"),
      // Verify we can reverse the order
      "'2021-01-01 18:00:00' > eventDate" ->
        Seq("((date <= DATE '2021-01-01') " +
          "OR ((date <= DATE '2021-01-01') IS NULL))"),
      "'2021-01-01 18:00:00' >= eventDate" ->
        Seq("((date <= DATE '2021-01-01') " +
          "OR ((date <= DATE '2021-01-01') IS NULL))"),
      "'2021-01-01 18:00:00' = eventDate" ->
        Seq("((date = DATE '2021-01-01') " +
          "OR ((date = DATE '2021-01-01') IS NULL))"),
      "'2021-01-01 18:00:00' < eventDate" ->
        Seq("((date >= DATE '2021-01-01') " +
          "OR ((date >= DATE '2021-01-01') IS NULL))"),
      "'2021-01-01 18:00:00' <= eventDate" ->
        Seq("((date >= DATE '2021-01-01') " +
          "OR ((date >= DATE '2021-01-01') IS NULL))"),
      // Verify date type literal. In theory, the best filter should be date < DATE '2021-01-01'.
      // But Spark's analyzer converts eventTime < '2021-01-01' to
      // `eventTime` < TIMESTAMP '2021-01-01 00:00:00'. So it's the same as
      // eventTime < '2021-01-01 18:00:00' for `OptimizeGeneratedColumn`.
      "eventDate < '2021-01-01'" ->
        Seq("((date <= DATE '2021-01-01') " +
          "OR ((date <= DATE '2021-01-01') IS NULL))")
    )
  )

  testOptimizablePartitionExpression(
    "eventTime TIMESTAMP",
    "year INT, month INT, day INT, hour INT",
    Map(
      "year" -> "YEAR(eventTime)",
      "month" -> "MONTH(eventTime)",
      "day" -> "DAY(eventTime)",
      "hour" -> "HOUR(eventTime)"
    ),
    expectedPartitionExpr = YearMonthDayHourPartitionExpr("year", "month", "day", "hour"),
    filterTestCases = Seq(
      "eventTime < '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (
            |    (
            |      (year < year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      OR
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month < month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |    )
            |    OR
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day < dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day = dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (hour <= hour(TIMESTAMP '2021-01-01 18:00:00'))
            |  )
            |)
            |""".stripMargin)),
      "eventTime <= '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (
            |    (
            |      (year < year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      OR
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month < month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |    )
            |    OR
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day < dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day = dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (hour <= hour(TIMESTAMP '2021-01-01 18:00:00'))
            |  )
            |)
            |""".stripMargin)),
      "eventTime = '2021-01-01 18:00:00'" -> Seq(
        "(year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))",
        "(month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))",
        "(day = dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))",
        "(hour = hour(TIMESTAMP '2021-01-01 18:00:00'))"
      ),
      "eventTime > '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (
            |    (
            |      (year > year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      OR
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month > month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |    )
            |    OR
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day > dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day = dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (hour >= hour(TIMESTAMP '2021-01-01 18:00:00'))
            |  )
            |)
            |""".stripMargin)),
      "eventTime >= '2021-01-01 18:00:00'" ->Seq(
        compactFilter(
          """(
            |  (
            |    (
            |      (year > year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      OR
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month > month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |    )
            |    OR
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day > dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (
            |        (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |        AND
            |        (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      )
            |      AND
            |      (day = dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (hour >= hour(TIMESTAMP '2021-01-01 18:00:00'))
            |  )
            |)
            |""".stripMargin)),
      "eventTime is null" -> Seq(
        "(year IS NULL)",
        "(month IS NULL)",
        "(day IS NULL)",
        "(hour IS NULL)"
      )
    )
  )

  testOptimizablePartitionExpression(
    "eventTime TIMESTAMP",
    "year INT, month INT, day INT",
    Map(
      "year" -> "YEAR(eventTime)",
      "month" -> "MONTH(eventTime)",
      "day" -> "DAY(eventTime)"
    ),
    expectedPartitionExpr = YearMonthDayPartitionExpr("year", "month", "day"),
    filterTestCases = Seq(
      "eventTime < '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (
            |    (year < year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    OR
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month < month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (day <= dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime <= '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (
            |    (year < year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    OR
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month < month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (day <= dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime = '2021-01-01 18:00:00'" -> Seq(
        "(year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))",
        "(month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))",
        "(day = dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))"
      ),
      "eventTime > '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (
            |    (year > year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    OR
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month > month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (day >= dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime >= '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (
            |    (year > year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    OR
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month > month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |  )
            |  OR
            |  (
            |    (
            |      (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |      AND
            |      (month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    )
            |    AND
            |    (day >= dayofmonth(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime is null" -> Seq(
        "(year IS NULL)",
        "(month IS NULL)",
        "(day IS NULL)"
      )
    )
  )

  testOptimizablePartitionExpression(
    "eventTime TIMESTAMP",
    "year INT, month INT",
    // Use different cases to verify we can recognize the same column using different cases in
    // generation expressions.
    Map(
      "year" -> "YEAR(EVENTTIME)",
      "month" -> "MONTH(eventTime)"
    ),
    expectedPartitionExpr = YearMonthPartitionExpr("year", "month"),
    filterTestCases = Seq(
      "eventTime < '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (year < year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  OR
            |  (
            |    (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    AND
            |    (month <= month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime <= '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (year < year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  OR
            |  (
            |    (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    AND
            |    (month <= month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime = '2021-01-01 18:00:00'" -> Seq(
        "(year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))",
        "(month = month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))"
      ),
      "eventTime > '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (year > year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  OR
            |  (
            |    (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    AND
            |    (month >= month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime >= '2021-01-01 18:00:00'" -> Seq(
        compactFilter(
          """(
            |  (year > year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  OR
            |  (
            |    (year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |    AND
            |    (month >= month(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)))
            |  )
            |)
            |""".stripMargin)),
      "eventTime is null" -> Seq("(year IS NULL)", "(month IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "eventTime TIMESTAMP",
    "year INT",
    Map("year" -> "YEAR(eventTime)"),
    expectedPartitionExpr = YearPartitionExpr("year"),
    filterTestCases = Seq(
      "eventTime < '2021-01-01 18:00:00'" ->
        Seq("((year <= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) " +
          "OR ((year <= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) IS NULL))"),
      "eventTime <= '2021-01-01 18:00:00'" ->
        Seq("((year <= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) " +
          "OR ((year <= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) IS NULL))"),
      "eventTime = '2021-01-01 18:00:00'" ->
        Seq("((year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) " +
          "OR ((year = year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) IS NULL))"),
      "eventTime > '2021-01-01 18:00:00'" ->
        Seq("((year >= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) " +
          "OR ((year >= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) IS NULL))"),
      "eventTime >= '2021-01-01 18:00:00'" ->
        Seq("((year >= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) " +
          "OR ((year >= year(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE))) IS NULL))"),
      "eventTime is null" -> Seq("(year IS NULL)")
    )
  )

  Seq(("YEAR(eventDate)", " from year(date)"),
    ("YEAR(CAST(eventDate AS DATE))", " from year(cast(date))"))
    .foreach { case (partCol, auxTestName) =>
      testOptimizablePartitionExpression(
        "eventDate DATE",
        "year INT",
        Map("year" -> partCol),
        expectedPartitionExpr = YearPartitionExpr("year"),
        auxiliaryTestName = Option(auxTestName),
        filterTestCases = Seq(
          "eventDate < '2021-01-01'" ->
            Seq("((year <= year(DATE '2021-01-01')) " +
              "OR ((year <= year(DATE '2021-01-01')) IS NULL))"),
          "eventDate <= '2021-01-01'" ->
            Seq("((year <= year(DATE '2021-01-01')) " +
              "OR ((year <= year(DATE '2021-01-01')) IS NULL))"),
          "eventDate = '2021-01-01'" ->
            Seq("((year = year(DATE '2021-01-01')) " +
              "OR ((year = year(DATE '2021-01-01')) IS NULL))"),
          "eventDate > '2021-01-01'" ->
            Seq("((year >= year(DATE '2021-01-01')) " +
              "OR ((year >= year(DATE '2021-01-01')) IS NULL))"),
          "eventDate >= '2021-01-01'" ->
            Seq("((year >= year(DATE '2021-01-01')) " +
              "OR ((year >= year(DATE '2021-01-01')) IS NULL))"),
          "eventDate is null" -> Seq("(year IS NULL)")
        )
      )
    }

  testOptimizablePartitionExpression(
    "value STRING",
    "substr STRING",
    Map("substr" -> "SUBSTRING(value, 2, 3)"),
    expectedPartitionExpr = SubstringPartitionExpr("substr", 2, 3),
    filterTestCases = Seq(
      "value < 'foo'" -> Nil,
      "value <= 'foo'" -> Nil,
      "value = 'foo'" -> Seq("((substr IS NULL) OR (substr = substring('foo', 2, 3)))"),
      "value > 'foo'" -> Nil,
      "value >= 'foo'" -> Nil,
      "value is null" -> Seq("(substr IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "value STRING",
    "substr STRING",
    Map("substr" -> "SUBSTRING(value, 0, 3)"),
    expectedPartitionExpr = SubstringPartitionExpr("substr", 0, 3),
    filterTestCases = Seq(
      "value < 'foo'" -> Seq("((substr IS NULL) OR (substr <= substring('foo', 0, 3)))"),
      "value <= 'foo'" -> Seq("((substr IS NULL) OR (substr <= substring('foo', 0, 3)))"),
      "value = 'foo'" -> Seq("((substr IS NULL) OR (substr = substring('foo', 0, 3)))"),
      "value > 'foo'" -> Seq("((substr IS NULL) OR (substr >= substring('foo', 0, 3)))"),
      "value >= 'foo'" -> Seq("((substr IS NULL) OR (substr >= substring('foo', 0, 3)))"),
      "value is null" -> Seq("(substr IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "value DOUBLE",
    "part BIGINT",
    Map("part" -> "CEIL(value)"),
    expectedPartitionExpr = CeilPartitionExpr("part"),
    filterTestCases = Seq(
      "value < 2.1" -> Seq("((part <= CEIL(2.1D)) OR ((part <= CEIL(2.1D)) IS NULL))"),
      "value <= 2.1" -> Seq("((part <= CEIL(2.1D)) OR ((part <= CEIL(2.1D)) IS NULL))"),
      "value = 2.1" -> Seq("((part = CEIL(2.1D)) OR ((part = CEIL(2.1D)) IS NULL))"),
      "value >= 2.1" -> Seq("((part >= CEIL(2.1D)) OR ((part >= CEIL(2.1D)) IS NULL))"),
      "value > 2.1" -> Seq("((part >= CEIL(2.1D)) OR ((part >= CEIL(2.1D)) IS NULL))"),
      "value is null" -> Seq("(part IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "value DECIMAL",
    "part1 DECIMAL",
    Map("part1" -> "CEIL(value)"),
    expectedPartitionExpr = CeilPartitionExpr("part1"),
    filterTestCases = Seq(
      "value < 2.12" -> Seq("((part1 <= CEIL(2BD)) OR ((part1 <= CEIL(2BD)) IS NULL))"),
      "value <= 2.12" -> Seq("((part1 <= CEIL(2BD)) OR ((part1 <= CEIL(2BD)) IS NULL))"),
      "value = CAST(2.12 AS DECIMAL(10, 0))" ->
        Seq("((part1 = CEIL(2BD)) OR ((part1 = CEIL(2BD)) IS NULL))"),
      "value >= 2.12" -> Seq("((part1 >= CEIL(2BD)) OR ((part1 >= CEIL(2BD)) IS NULL))"),
      "value > 2.12" -> Seq("((part1 >= CEIL(2BD)) OR ((part1 >= CEIL(2BD)) IS NULL))"),
      "value is null" -> Seq("(part1 IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "value BIGINT",
    "part3 BIGINT",
    Map("part3" -> "CEIL(value)"),
    expectedPartitionExpr = CeilPartitionExpr("part3"),
    filterTestCases = Seq(
      "value < 3" -> Seq("((part3 <= CEIL(3L)) OR ((part3 <= CEIL(3L)) IS NULL))"),
      "value <= 3" -> Seq("((part3 <= CEIL(3L)) OR ((part3 <= CEIL(3L)) IS NULL))"),
      "value = 3" -> Seq("((part3 = CEIL(3L)) OR ((part3 = CEIL(3L)) IS NULL))"),
      "value >= 3" -> Seq("((part3 >= CEIL(3L)) OR ((part3 >= CEIL(3L)) IS NULL))"),
      "value > 3" -> Seq("((part3 >= CEIL(3L)) OR ((part3 >= CEIL(3L)) IS NULL))"),
      "value is null" -> Seq("(part3 IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "value STRING",
    "substr STRING",
    Map("substr" -> "SUBSTRING(value, 1, 3)"),
    expectedPartitionExpr = SubstringPartitionExpr("substr", 1, 3),
    filterTestCases = Seq(
      "value < 'foo'" -> Seq("((substr IS NULL) OR (substr <= substring('foo', 1, 3)))"),
      "value <= 'foo'" -> Seq("((substr IS NULL) OR (substr <= substring('foo', 1, 3)))"),
      "value = 'foo'" -> Seq("((substr IS NULL) OR (substr = substring('foo', 1, 3)))"),
      "value > 'foo'" -> Seq("((substr IS NULL) OR (substr >= substring('foo', 1, 3)))"),
      "value >= 'foo'" -> Seq("((substr IS NULL) OR (substr >= substring('foo', 1, 3)))"),
      "value is null" -> Seq("(substr IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "value STRING",
    "`my.substr` STRING",
    Map("my.substr" -> "SUBSTRING(value, 1, 3)"),
    expectedPartitionExpr = SubstringPartitionExpr("my.substr", 1, 3),
    filterTestCases = Seq(
      "value < 'foo'" -> Seq("((`my.substr` IS NULL) OR (`my.substr` <= substring('foo', 1, 3)))"),
      "value <= 'foo'" -> Seq("((`my.substr` IS NULL) OR (`my.substr` <= substring('foo', 1, 3)))"),
      "value = 'foo'" -> Seq("((`my.substr` IS NULL) OR (`my.substr` = substring('foo', 1, 3)))"),
      "value > 'foo'" -> Seq("((`my.substr` IS NULL) OR (`my.substr` >= substring('foo', 1, 3)))"),
      "value >= 'foo'" -> Seq("((`my.substr` IS NULL) OR (`my.substr` >= substring('foo', 1, 3)))"),
      "value is null" -> Seq("(`my.substr` IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "outer struct<inner struct<nested: struct<value:STRING>>>",
    "substr STRING",
    Map("substr" -> "SUBSTRING(outer.inner.nested.value, 1, 3)"),
    expectedPartitionExpr = SubstringPartitionExpr("substr", 1, 3),
    auxiliaryTestName = Some(" deeply nested"),
    expressionKey = Some("outer.inner.nested.value"),
    skipNested = true,
    filterTestCases = Seq(
      "outer.inner.nested.value < 'foo'" ->
        Seq("((substr IS NULL) OR (substr <= substring('foo', 1, 3)))"),
      "outer.inner.nested.value <= 'foo'" ->
        Seq("((substr IS NULL) OR (substr <= substring('foo', 1, 3)))"),
      "outer.inner.nested.value = 'foo'" ->
        Seq("((substr IS NULL) OR (substr = substring('foo', 1, 3)))"),
      "outer.inner.nested.value > 'foo'" ->
        Seq("((substr IS NULL) OR (substr >= substring('foo', 1, 3)))"),
      "outer.inner.nested.value >= 'foo'" ->
        Seq("((substr IS NULL) OR (substr >= substring('foo', 1, 3)))"),
      "outer.inner.nested.value is null" -> Seq("(substr IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "eventTime TIMESTAMP",
    "eventTimeTrunc TIMESTAMP",
    Map("eventTimeTrunc" -> "date_trunc('YEAR', eventTime)"),
    expectedPartitionExpr = TimestampTruncPartitionExpr("YEAR", "eventTimeTrunc"),
    auxiliaryTestName = Option(" from date_trunc(timestamp)"),
    filterTestCases = Seq(
      "eventTime < '2021-01-01 18:00:00'" ->
        Seq("((eventTimeTrunc <= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) " +
          "OR ((eventTimeTrunc <= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) IS NULL))"),
      "eventTime <= '2021-01-01 18:00:00'" ->
        Seq("((eventTimeTrunc <= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) " +
          "OR ((eventTimeTrunc <= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) IS NULL))"),
      "eventTime = '2021-01-01 18:00:00'" ->
        Seq("((eventTimeTrunc = date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) " +
          "OR ((eventTimeTrunc = date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) IS NULL))"),
      "eventTime > '2021-01-01 18:00:00'" ->
        Seq("((eventTimeTrunc >= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) " +
          "OR ((eventTimeTrunc >= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) IS NULL))"),
      "eventTime >= '2021-01-01 18:00:00'" ->
        Seq("((eventTimeTrunc >= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) " +
          "OR ((eventTimeTrunc >= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) IS NULL))"),
      "eventTime is null" -> Seq("(eventTimeTrunc IS NULL)"),
      // Verify we can reverse the order
      "'2021-01-01 18:00:00' > eventTime" ->
        Seq("((eventTimeTrunc <= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) " +
          "OR ((eventTimeTrunc <= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) IS NULL))"),
      "'2021-01-01 18:00:00' >= eventTime" ->
        Seq("((eventTimeTrunc <= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) " +
          "OR ((eventTimeTrunc <= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) IS NULL))"),
      "'2021-01-01 18:00:00' = eventTime" ->
        Seq("((eventTimeTrunc = date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) " +
          "OR ((eventTimeTrunc = date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) IS NULL))"),
      "'2021-01-01 18:00:00' < eventTime" ->
        Seq("((eventTimeTrunc >= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) " +
          "OR ((eventTimeTrunc >= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) IS NULL))"),
      "'2021-01-01 18:00:00' <= eventTime" ->
        Seq("((eventTimeTrunc >= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) " +
          "OR ((eventTimeTrunc >= date_trunc('YEAR', TIMESTAMP '2021-01-01 18:00:00')) IS NULL))")
    )
  )

  testOptimizablePartitionExpression(
    "eventDate DATE",
    "eventTimeTrunc TIMESTAMP",
    Map("eventTimeTrunc" -> "date_trunc('DD', eventDate)"),
    expectedPartitionExpr = TimestampTruncPartitionExpr("DD", "eventTimeTrunc"),
    auxiliaryTestName = Option(" from date_trunc(cast(date))"),
    filterTestCases = Seq(
      "eventDate < '2021-01-01'" ->
        Seq("((eventTimeTrunc <= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) " +
      "OR ((eventTimeTrunc <= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) IS NULL))"),
      "eventDate <= '2021-01-01'" ->
        Seq("((eventTimeTrunc <= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) " +
      "OR ((eventTimeTrunc <= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) IS NULL))"),
      "eventDate = '2021-01-01'" ->
        Seq("((eventTimeTrunc = date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) " +
      "OR ((eventTimeTrunc = date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) IS NULL))"),
      "eventDate > '2021-01-01'" ->
        Seq("((eventTimeTrunc >= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) " +
      "OR ((eventTimeTrunc >= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) IS NULL))"),
      "eventDate >= '2021-01-01'" ->
        Seq("((eventTimeTrunc >= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) " +
      "OR ((eventTimeTrunc >= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) IS NULL))"),
      "eventDate is null" -> Seq("(eventTimeTrunc IS NULL)"),
      // Verify we can reverse the order
      "'2021-01-01' > eventDate" ->
        Seq("((eventTimeTrunc <= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) " +
      "OR ((eventTimeTrunc <= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) IS NULL))"),
      "'2021-01-01' >= eventDate" ->
        Seq("((eventTimeTrunc <= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) " +
      "OR ((eventTimeTrunc <= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) IS NULL))"),
      "'2021-01-01' = eventDate" ->
        Seq("((eventTimeTrunc = date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) " +
      "OR ((eventTimeTrunc = date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) IS NULL))"),
      "'2021-01-01' < eventDate" ->
        Seq("((eventTimeTrunc >= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) " +
      "OR ((eventTimeTrunc >= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) IS NULL))"),
      "'2021-01-01' <= eventDate" ->
        Seq("((eventTimeTrunc >= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) " +
      "OR ((eventTimeTrunc >= date_trunc('DD', CAST(DATE '2021-01-01' AS TIMESTAMP))) IS NULL))")
    )
  )

  testOptimizablePartitionExpression(
    "value STRING",
    "part STRING",
    Map("part" -> "value"),
    expectedPartitionExpr = IdentityPartitionExpr("part"),
    expressionKey = Some("value"),
    filterTestCases = Seq(
      "value < 'foo'" -> Seq("((part IS NULL) OR (part < 'foo'))"),
      "value <= 'foo'" -> Seq("((part IS NULL) OR (part <= 'foo'))"),
      "value = 'foo'" -> Seq("((part IS NULL) OR (part = 'foo'))"),
      "value > 'foo'" -> Seq("((part IS NULL) OR (part > 'foo'))"),
      "value >= 'foo'" -> Seq("((part IS NULL) OR (part >= 'foo'))"),
      "value is null" -> Seq("(part IS NULL)")
    )
  )

  /**
   * In order to distinguish between field names with periods and nested field names,
   * fields with periods must be escaped. Otherwise in the example below, there's
   * no way to tell whether a filter on nested.value should be applied to part1 or part2.
   */
  testOptimizablePartitionExpression(
    "`nested.value` STRING, nested struct<value: STRING>",
    "part1 STRING, part2 STRING",
    Map("part1" -> "`nested.value`", "part2" -> "nested.value"),
    auxiliaryTestName = Some(" escaped field names"),
    expectedPartitionExpr = IdentityPartitionExpr("part1"),
    expressionKey = Some("`nested.value`"),
    skipNested = true,
    filterTestCases = Seq(
      "`nested.value` < 'foo'" -> Seq("((part1 IS NULL) OR (part1 < 'foo'))"),
      "`nested.value` <= 'foo'" -> Seq("((part1 IS NULL) OR (part1 <= 'foo'))"),
      "`nested.value` = 'foo'" -> Seq("((part1 IS NULL) OR (part1 = 'foo'))"),
      "`nested.value` > 'foo'" -> Seq("((part1 IS NULL) OR (part1 > 'foo'))"),
      "`nested.value` >= 'foo'" -> Seq("((part1 IS NULL) OR (part1 >= 'foo'))"),
      "`nested.value` is null" -> Seq("(part1 IS NULL)")
    )
  )

  test("end-to-end optimizable partition expression") {
    withTempDir { tempDir =>
      withTableName("optimizable_partition_expression") { table =>

        createTable(
          table,
          Some(tempDir.getCanonicalPath),
          "c1 INT, c2 TIMESTAMP, c3 DATE",
          Map("c3" -> "CAST(c2 AS DATE)"),
          Seq("c3")
        )

          Seq(
            Tuple2(1, "2020-12-31 11:00:00"),
            Tuple2(2, "2021-01-01 12:00:00"),
            Tuple2(3, "2021-01-02 13:00:00")
          ).foreach { values =>
            insertInto(
              tempDir.getCanonicalPath,
              Seq(values).toDF("c1", "c2")
                .withColumn("c2", $"c2".cast(TimestampType))
            )
          }
        assert(tempDir.listFiles().map(_.getName).toSet ==
          Set("c3=2021-01-01", "c3=2021-01-02", "c3=2020-12-31", "_delta_log"))
        // Delete folders which should not be read if we generate the partition filters correctly
        tempDir.listFiles().foreach { f =>
          if (f.getName != "c3=2021-01-01" && f.getName != "_delta_log") {
            Utils.deleteRecursively(f)
          }
        }
        assert(tempDir.listFiles().map(_.getName).toSet == Set("c3=2021-01-01", "_delta_log"))
        checkAnswer(
          sql(s"select * from $table where " +
            s"c2 >= '2021-01-01 12:00:00' AND c2 <= '2021-01-01 18:00:00'"),
          Row(2, sqlTimestamp("2021-01-01 12:00:00"), sqlDate("2021-01-01")))
        // Verify `OptimizeGeneratedColumn` doesn't mess up Projects.
        checkAnswer(
          sql(s"select c1 from $table where " +
            s"c2 >= '2021-01-01 12:00:00' AND c2 <= '2021-01-01 18:00:00'"),
          Row(2))

        // Check both projection orders to make sure projection orders are handled correctly
        checkAnswer(
          sql(s"select c1, c2 from $table where " +
            s"c2 >= '2021-01-01 12:00:00' AND c2 <= '2021-01-01 18:00:00'"),
          Row(2, Timestamp.valueOf("2021-01-01 12:00:00")))
        checkAnswer(
          sql(s"select c2, c1 from $table where " +
            s"c2 >= '2021-01-01 12:00:00' AND c2 <= '2021-01-01 18:00:00'"),
          Row(Timestamp.valueOf("2021-01-01 12:00:00"), 2))

        // Verify the optimization works for limit.
        val limitQuery = sql(
          s"""select * from $table
             |where c2 >= '2021-01-01 12:00:00' AND c2 <= '2021-01-01 18:00:00'
             |limit 10""".stripMargin)
        val expectedPartitionFilters = Seq(
          "((c3 >= CAST(TIMESTAMP '2021-01-01 12:00:00' AS DATE)) " +
            "OR ((c3 >= CAST(TIMESTAMP '2021-01-01 12:00:00' AS DATE)) IS NULL))",
          "((c3 <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) " +
            "OR ((c3 <= CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE)) IS NULL))"
        )
        assert(expectedPartitionFilters ==
          getPushedPartitionFilters(limitQuery.queryExecution).map(_.sql))
        checkAnswer(limitQuery, Row(2, sqlTimestamp("2021-01-01 12:00:00"), sqlDate("2021-01-01")))
      }
    }
  }

  test("empty string and null ambiguity in a partition column") {
    withTempDir { tempDir =>
      withTableName("optimizable_partition_expression") { table =>
        createTable(
          table,
          Some(tempDir.getCanonicalPath),
          "c1 STRING, c2 STRING",
          Map("c2" -> "SUBSTRING(c1, 1, 4)"),
          Seq("c2")
        )
        insertInto(
          tempDir.getCanonicalPath,
          Seq(Tuple1("")).toDF("c1")
        )
        checkAnswer(
          sql(s"select * from $table where c1 = ''"),
          Row("", null))
        // The following check shows the weird behavior of SPARK-24438 and confirms the generated
        // partition filter doesn't impact the answer.
        withSQLConf(GENERATED_COLUMN_PARTITION_FILTER_OPTIMIZATION_ENABLED.key -> "false") {
          checkAnswer(
            sql(s"select * from $table where c1 = ''"),
            Row("", null))
        }
      }
    }
  }

  test("substring on multibyte characters") {
    withTempDir { tempDir =>
      withTableName("multibyte_characters") { table =>
        createTable(
          table,
          Some(tempDir.getCanonicalPath),
          "c1 STRING, c2 STRING",
          Map("c2" -> "SUBSTRING(c1, 1, 2)"),
          Seq("c2")
        )
        // scalastyle:off nonascii
        insertInto(
          tempDir.getCanonicalPath,
          Seq(Tuple1("一二三四")).toDF("c1")
        )
        val testQuery = s"select * from $table where c1 > 'abcd'"
        assert("((c2 IS NULL) OR (c2 >= substring('abcd', 1, 2)))" :: Nil ==
          getPushedPartitionFilters(sql(testQuery).queryExecution).map(_.sql))
        checkAnswer(
          sql(testQuery),
          Row("一二三四", "一二"))
        // scalastyle:on nonascii
      }
    }
  }

  testOptimizablePartitionExpression(
    "eventTime TIMESTAMP",
    "month STRING",
    Map("month" -> "DATE_FORMAT(eventTime, 'yyyy-MM')"),
    expectedPartitionExpr = DateFormatPartitionExpr("month", "yyyy-MM"),
    auxiliaryTestName = Option(" from timestamp"),
    filterTestCases = Seq(
      "eventTime < '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') <= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') <= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), " +
          "'yyyy-MM')) IS NULL))"),
      "eventTime <= '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') <= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') <= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), " +
          "'yyyy-MM')) IS NULL))"),
      "eventTime = '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') = " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') = " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), " +
          "'yyyy-MM')) IS NULL))"),
      "eventTime > '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') >= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') >= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), " +
          "'yyyy-MM')) IS NULL))"),
      "eventTime >= '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') >= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') >= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM'), " +
          "'yyyy-MM')) IS NULL))"),
      "eventTime is null" -> Seq("(month IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "eventDate DATE",
    "month STRING",
    Map("month" -> "DATE_FORMAT(eventDate, 'yyyy-MM')"),
    expectedPartitionExpr = DateFormatPartitionExpr("month", "yyyy-MM"),
    auxiliaryTestName = Option(" from cast(date)"),
    filterTestCases = Seq(
      "eventDate < '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') <= unix_timestamp(date_format(CAST(" +
          "DATE '2021-06-28' AS TIMESTAMP), 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') <= " +
          "unix_timestamp(date_format(CAST(DATE '2021-06-28' AS TIMESTAMP), " +
          "'yyyy-MM'), 'yyyy-MM')) IS NULL))"),
      "eventDate <= '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') <= unix_timestamp(date_format(CAST(" +
          "DATE '2021-06-28' AS TIMESTAMP), 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') <= " +
          "unix_timestamp(date_format(CAST(DATE '2021-06-28' AS TIMESTAMP), " +
          "'yyyy-MM'), 'yyyy-MM')) IS NULL))"),
      "eventDate = '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') = unix_timestamp(date_format(CAST(" +
          "DATE '2021-06-28' AS TIMESTAMP), 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') = " +
          "unix_timestamp(date_format(CAST(DATE '2021-06-28' AS TIMESTAMP), " +
          "'yyyy-MM'), 'yyyy-MM')) IS NULL))"),
      "eventDate > '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') >= unix_timestamp(date_format(CAST(" +
          "DATE '2021-06-28' AS TIMESTAMP), 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') >= " +
          "unix_timestamp(date_format(CAST(DATE '2021-06-28' AS TIMESTAMP), " +
          "'yyyy-MM'), 'yyyy-MM')) IS NULL))"),
      "eventDate >= '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(month, 'yyyy-MM') >= unix_timestamp(date_format(CAST(" +
          "DATE '2021-06-28' AS TIMESTAMP), 'yyyy-MM'), 'yyyy-MM')) " +
          "OR ((unix_timestamp(month, 'yyyy-MM') >= " +
          "unix_timestamp(date_format(CAST(DATE '2021-06-28' AS TIMESTAMP), " +
          "'yyyy-MM'), 'yyyy-MM')) IS NULL))"),
      "eventDate is null" -> Seq("(month IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "eventTime TIMESTAMP",
    "day STRING",
    Map("day" -> "DATE_FORMAT(eventTime, 'yyyy-MM-dd')"),
    expectedPartitionExpr = DateFormatPartitionExpr("day", "yyyy-MM-dd"),
    auxiliaryTestName = Option(" from timestamp"),
    filterTestCases = Seq(
      "eventTime < '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(day, 'yyyy-MM-dd') <= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd'), 'yyyy-MM-dd')) " +
          "OR ((unix_timestamp(day, 'yyyy-MM-dd') <= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd'), 'yyyy-MM-dd')) IS NULL))"),
      "eventTime <= '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(day, 'yyyy-MM-dd') <= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd'), 'yyyy-MM-dd')) " +
          "OR ((unix_timestamp(day, 'yyyy-MM-dd') <= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd'), 'yyyy-MM-dd')) IS NULL))"),
      "eventTime = '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(day, 'yyyy-MM-dd') = unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd'), 'yyyy-MM-dd')) " +
          "OR ((unix_timestamp(day, 'yyyy-MM-dd') = unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd'), 'yyyy-MM-dd')) IS NULL))"),
      "eventTime > '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(day, 'yyyy-MM-dd') >= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd'), 'yyyy-MM-dd')) " +
          "OR ((unix_timestamp(day, 'yyyy-MM-dd') >= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd'), 'yyyy-MM-dd')) IS NULL))"),
      "eventTime >= '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(day, 'yyyy-MM-dd') >= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd'), 'yyyy-MM-dd')) " +
          "OR ((unix_timestamp(day, 'yyyy-MM-dd') >= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd'), 'yyyy-MM-dd')) IS NULL))"),
      "eventTime is null" -> Seq("(day IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "eventTime TIMESTAMP",
    "hour STRING",
    Map("hour" -> "(DATE_FORMAT(eventTime, 'yyyy-MM-dd-HH'))"),
    expectedPartitionExpr = DateFormatPartitionExpr("hour", "yyyy-MM-dd-HH"),
    filterTestCases = Seq(
      "eventTime < '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(hour, 'yyyy-MM-dd-HH') <= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) " +
          "OR ((unix_timestamp(hour, 'yyyy-MM-dd-HH') <= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', " +
          "'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) IS NULL))"),
      "eventTime <= '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(hour, 'yyyy-MM-dd-HH') <= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) " +
          "OR ((unix_timestamp(hour, 'yyyy-MM-dd-HH') <= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', " +
          "'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) IS NULL))"),
      "eventTime = '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(hour, 'yyyy-MM-dd-HH') = unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) " +
          "OR ((unix_timestamp(hour, 'yyyy-MM-dd-HH') = " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', " +
          "'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) IS NULL))"),
      "eventTime > '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(hour, 'yyyy-MM-dd-HH') >= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) " +
          "OR ((unix_timestamp(hour, 'yyyy-MM-dd-HH') >= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', " +
          "'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) IS NULL))"),
      "eventTime >= '2021-06-28 18:00:00'" ->
        Seq("((unix_timestamp(hour, 'yyyy-MM-dd-HH') >= unix_timestamp(" +
          "date_format(TIMESTAMP '2021-06-28 18:00:00', 'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) " +
          "OR ((unix_timestamp(hour, 'yyyy-MM-dd-HH') >= " +
          "unix_timestamp(date_format(TIMESTAMP '2021-06-28 18:00:00', " +
          "'yyyy-MM-dd-HH'), 'yyyy-MM-dd-HH')) IS NULL))"),
      "eventTime is null" -> Seq("(hour IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "eventTime TIMESTAMP",
    "date DATE",
    Map("date" -> "(trunc(eventTime, 'year'))"),
    expectedPartitionExpr = TruncDatePartitionExpr("date", "year"),
    filterTestCases = Seq(
      "eventTime < '2021-01-01 18:00:00'" ->
        Seq("((date <= trunc(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE), 'year')) " +
        "OR ((date <= trunc(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE), 'year')) IS NULL))"),
      "eventTime <= '2021-01-01 18:00:00'" ->
        Seq("((date <= trunc(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE), 'year')) " +
        "OR ((date <= trunc(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE), 'year')) IS NULL))"),
      "eventTime = '2021-01-01 18:00:00'" ->
        Seq("((date = trunc(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE), 'year')) " +
          "OR ((date = trunc(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE), 'year')) IS NULL))"),
      "eventTime > '2021-01-01 18:00:00'" ->
        Seq("((date >= trunc(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE), 'year')) " +
          "OR ((date >= trunc(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE), 'year')) IS NULL))"),
      "eventTime >= '2021-01-01 18:00:00'" ->
        Seq("((date >= trunc(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE), 'year')) " +
          "OR ((date >= trunc(CAST(TIMESTAMP '2021-01-01 18:00:00' AS DATE), 'year')) IS NULL))"),
      "eventTime is null" ->
        Seq("(date IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "eventDate DATE",
    "date DATE",
    Map("date" -> "(trunc(eventDate, 'month'))"),
    expectedPartitionExpr = TruncDatePartitionExpr("date", "month"),
    filterTestCases = Seq(
      "eventDate < '2021-12-01'" ->
        Seq("((date <= trunc(DATE '2021-12-01', 'month')) " +
          "OR ((date <= trunc(DATE '2021-12-01', 'month')) IS NULL))"),
      "eventDate <= '2021-12-01'" ->
        Seq("((date <= trunc(DATE '2021-12-01', 'month')) " +
          "OR ((date <= trunc(DATE '2021-12-01', 'month')) IS NULL))"),
      "eventDate = '2021-12-01'" ->
        Seq("((date = trunc(DATE '2021-12-01', 'month')) " +
          "OR ((date = trunc(DATE '2021-12-01', 'month')) IS NULL))"),
      "eventDate > '2021-12-01'" ->
        Seq("((date >= trunc(DATE '2021-12-01', 'month')) " +
          "OR ((date >= trunc(DATE '2021-12-01', 'month')) IS NULL))"),
      "eventDate >= '2021-12-01'" ->
        Seq("((date >= trunc(DATE '2021-12-01', 'month')) " +
          "OR ((date >= trunc(DATE '2021-12-01', 'month')) IS NULL))"),
      "eventDate is null" ->
        Seq("(date IS NULL)")
    )
  )

  testOptimizablePartitionExpression(
    "eventDateStr STRING",
    "date DATE",
    Map("date" -> "(trunc(eventDateStr, 'quarter'))"),
    expectedPartitionExpr = TruncDatePartitionExpr("date", "quarter"),
    filterTestCases = Seq(
      "eventDateStr < '2022-04-01'" ->
        Seq("((date <= trunc(CAST('2022-04-01' AS DATE), 'quarter')) " +
          "OR ((date <= trunc(CAST('2022-04-01' AS DATE), 'quarter')) IS NULL))"),
      "eventDateStr <= '2022-04-01'" ->
        Seq("((date <= trunc(CAST('2022-04-01' AS DATE), 'quarter')) " +
          "OR ((date <= trunc(CAST('2022-04-01' AS DATE), 'quarter')) IS NULL))"),
      "eventDateStr = '2022-04-01'" ->
        Seq("((date = trunc(CAST('2022-04-01' AS DATE), 'quarter')) " +
          "OR ((date = trunc(CAST('2022-04-01' AS DATE), 'quarter')) IS NULL))"),
      "eventDateStr > '2022-04-01'" ->
        Seq("((date >= trunc(CAST('2022-04-01' AS DATE), 'quarter')) " +
          "OR ((date >= trunc(CAST('2022-04-01' AS DATE), 'quarter')) IS NULL))"),
      "eventDateStr >= '2022-04-01'" ->
        Seq("((date >= trunc(CAST('2022-04-01' AS DATE), 'quarter')) " +
          "OR ((date >= trunc(CAST('2022-04-01' AS DATE), 'quarter')) IS NULL))"),
      "eventDateStr is null" ->
        Seq("(date IS NULL)")
    )
  )

  test("five digits year in a year month day partition column") {
    withTempDir { tempDir =>
      withTableName("optimizable_partition_expression") { table =>
        createTable(
          table,
          Some(tempDir.getCanonicalPath),
          "c1 TIMESTAMP, c2 INT, c3 INT, c4 INT",
          Map(
            "c2" -> "YEAR(c1)",
            "c3" -> "MONTH(c1)",
            "c4" -> "DAY(c1)"
          ),
          Seq("c2", "c3", "c4")
        )
        insertInto(
          tempDir.getCanonicalPath,
          Seq(Tuple1("12345-07-15 18:00:00"))
            .toDF("c1")
            .withColumn("c1", $"c1".cast(TimestampType))
        )

        checkAnswer(
          sql(s"select * from $table where c1 = CAST('12345-07-15 18:00:00' as timestamp)"),
          Row(new Timestamp(327420320400000L), 12345, 7, 15))
        withSQLConf(GENERATED_COLUMN_PARTITION_FILTER_OPTIMIZATION_ENABLED.key -> "false") {
          checkAnswer(
            sql(s"select * from $table where c1 = CAST('12345-07-15 18:00:00' as timestamp)"),
            Row(new Timestamp(327420320400000L), 12345, 7, 15))
        }
      }
    }
  }

  test("five digits year in a date_format yyyy-MM partition column") {
    withTempDir { tempDir =>
      withTableName("optimizable_partition_expression") { table =>
        createTable(
          table,
          Some(tempDir.getCanonicalPath),
          "c1 TIMESTAMP, c2 STRING",
          Map("c2" -> "DATE_FORMAT(c1, 'yyyy-MM')"),
          Seq("c2")
        )
        insertInto(
          tempDir.getCanonicalPath,
          Seq(Tuple1("12345-07-15 18:00:00"))
            .toDF("c1")
            .withColumn("c1", $"c1".cast(TimestampType))
        )

        checkAnswer(
          sql(s"select * from $table where c1 = CAST('12345-07-15 18:00:00' as timestamp)"),
          Row(new Timestamp(327420320400000L), "+12345-07"))
        withSQLConf(GENERATED_COLUMN_PARTITION_FILTER_OPTIMIZATION_ENABLED.key -> "false") {
          checkAnswer(
            sql(s"select * from $table where c1 = CAST('12345-07-15 18:00:00' as timestamp)"),
            Row(new Timestamp(327420320400000L), "+12345-07"))
        }
      }
    }
  }

  test("five digits year in a date_format yyyy-MM-dd-HH partition column") {
    withTempDir { tempDir =>
      withTableName("optimizable_partition_expression") { table =>
        createTable(
          table,
          Some(tempDir.getCanonicalPath),
          "c1 TIMESTAMP, c2 STRING",
          Map("c2" -> "DATE_FORMAT(c1, 'yyyy-MM-dd-HH')"),
          Seq("c2")
        )
        insertInto(
          tempDir.getCanonicalPath,
          Seq(Tuple1("12345-07-15 18:00:00"))
            .toDF("c1")
            .withColumn("c1", $"c1".cast(TimestampType))
        )

        checkAnswer(
          sql(s"select * from $table where c1 = CAST('12345-07-15 18:00:00' as timestamp)"),
          Row(new Timestamp(327420320400000L), "+12345-07-15-18"))
        withSQLConf(GENERATED_COLUMN_PARTITION_FILTER_OPTIMIZATION_ENABLED.key -> "false") {
          checkAnswer(
            sql(s"select * from $table where c1 = CAST('12345-07-15 18:00:00' as timestamp)"),
            Row(new Timestamp(327420320400000L), "+12345-07-15-18"))
        }
      }
    }
  }

  test("end-to-end test of behaviors of write/read null on partition column") {
    //              unix_timestamp('12345-12', 'yyyy-MM') | unix_timestamp('+12345-12', 'yyyy-MM')
    //  EXCEPTION               fail                     |           327432240000
    //  CORRECTED               null                     |           327432240000
    //  LEGACY               327432240000                |               null
    withTempDir { tempDir =>
      withTableName("optimizable_partition_expression") { table =>
        createTable(
          table,
          Some(tempDir.getCanonicalPath),
          "c1 TIMESTAMP, c2 STRING",
          Map("c2" -> "DATE_FORMAT(c1, 'yyyy-MM')"),
          Seq("c2")
        )

        // write in LEGACY
        withSQLConf(
        "spark.sql.legacy.timeParserPolicy" -> "CORRECTED"
        ) {
          insertInto(
            tempDir.getCanonicalPath,
            Seq(Tuple1("12345-07-01 00:00:00"))
              .toDF("c1")
              .withColumn("c1", $"c1".cast(TimestampType))
          )
          insertInto(
            tempDir.getCanonicalPath,
            Seq(Tuple1("+23456-07-20 18:30:00"))
              .toDF("c1")
              .withColumn("c1", $"c1".cast(TimestampType))
          )
        }

        // write in LEGACY
        withSQLConf(
          "spark.sql.legacy.timeParserPolicy" -> "LEGACY"
        ) {
          insertInto(
            tempDir.getCanonicalPath,
            Seq(Tuple1("+12349-07-01 00:00:00"))
              .toDF("c1")
              .withColumn("c1", $"c1".cast(TimestampType))
          )
          insertInto(
            tempDir.getCanonicalPath,
            Seq(Tuple1("+30000-12-30 20:00:00"))
              .toDF("c1")
              .withColumn("c1", $"c1".cast(TimestampType))
          )
        }

        // we have partitions based on CORRECTED + LEGACY parser (with +)
        assert(tempDir.listFiles().map(_.getName).toSet ==
          Set("c2=+23456-07", "c2=12349-07", "c2=30000-12", "c2=+12345-07", "_delta_log"))

        // read behaviors in CORRECTED, we still can query correctly
        withSQLConf("spark.sql.legacy.timeParserPolicy" -> "CORRECTED") {
          checkAnswer(
            sql(s"select (unix_timestamp('+20000-01', 'yyyy-MM')) as value"),
            Row(568971849600L)
          )
          withSQLConf("spark.sql.ansi.enabled" -> "false") {
            checkAnswer(
              sql(s"select (unix_timestamp('20000-01', 'yyyy-MM')) as value"),
              Row(null)
            )
            checkAnswer(
              sql(s"select * from $table where " +
                s"c1 >= '20000-01-01 12:00:00'"),
              // 23456-07-20 18:30:00
              Row(new Timestamp(678050098200000L), "+23456-07") ::
                // 30000-12-30 20:00:00
                Row(new Timestamp(884572891200000L), "30000-12") :: Nil
            )
          }
        }

        // read behaviors in LEGACY, we still can query correctly
        withSQLConf("spark.sql.legacy.timeParserPolicy" -> "LEGACY") {
          checkAnswer(
            sql(s"select (unix_timestamp('20000-01', 'yyyy-MM')) as value"),
            Row(568971849600L)
          )
          withSQLConf("spark.sql.ansi.enabled" -> "false") {
            checkAnswer(
              sql(s"select (unix_timestamp('+20000-01', 'yyyy-MM')) as value"),
              Row(null)
            )
            checkAnswer(
              sql(s"select * from $table where " +
                s"c1 >= '20000-01-01 12:00:00'"),
              // 23456-07-20 18:30:00
              Row(new Timestamp(678050098200000L), "+23456-07") ::
                // 30000-12-30 20:00:00
                Row(new Timestamp(884572891200000L), "30000-12") :: Nil
            )
          }
        }
      }
    }
  }

  test("generated partition filters should avoid conflicts") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      withTableName("avoid_conflicts") { table =>
        createTable(
          table,
          Some(path),
          "eventTime TIMESTAMP, date DATE",
          Map("date" -> "CAST(eventTime AS DATE)"),
          Seq("date")
        )
        insertInto(
          path,
          Seq(Tuple1("2021-01-01 00:00:00"), Tuple1("2021-01-02 00:00:00"))
            .toDF("eventTime")
            .withColumn("eventTime", $"eventTime".cast(TimestampType))
        )

        val unblockQueries = new CountDownLatch(1)
        val waitForAllQueries = new CountDownLatch(2)

        PrepareDeltaScanBase.withCallbackOnGetDeltaScanGenerator(_ => {
          waitForAllQueries.countDown()
          assert(
            unblockQueries.await(30, TimeUnit.SECONDS),
            "the main thread didn't wake up queries")
        }) {
          val threadPool = ThreadUtils.newDaemonFixedThreadPool(2, "test")
          try {
            // Run two queries that should not conflict with each other if we generate the partition
            // filter correctly.
            val f1 = threadPool.submit(() => {
              spark.read.format("delta").load(path).where("eventTime = '2021-01-01 00:00:00'")
                .write.mode("append").format("delta").save(path)
              true
            })
            val f2 = threadPool.submit(() => {
              spark.read.format("delta").load(path).where("eventTime = '2021-01-02 00:00:00'")
                .write.mode("append").format("delta").save(path)
              true
            })
            assert(
              waitForAllQueries.await(30, TimeUnit.SECONDS),
              "queries didn't finish before timeout")
            unblockQueries.countDown()
            f1.get(30, TimeUnit.SECONDS)
            f2.get(30, TimeUnit.SECONDS)
          } finally {
            threadPool.shutdownNow()
          }
        }
      }
    }
  }
}
