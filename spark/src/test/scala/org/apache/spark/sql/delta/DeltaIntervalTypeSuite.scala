/*
 * Copyright (2026) The Delta Lake Project Authors.
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


package org.apache.spark.sql.delta

import java.io.File
import java.time.{Duration, Period}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.actions.Protocol
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType


/**
 * Tests the INTERVAL type supports in Delta tables.
 * <p>
 * @see [[org.apache.spark.sql.delta.stats.DataSkippingDeltaTests]]
 *      for INTERVAL test with dataskipping.
 */
class DeltaIntervalTypeSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSourceSuiteBase {

  private def assertPartitionByTwoColumns(
      tableName: String,
      totalPartitionKeys: Int,
      firsColumnKeys: Int,
      lastColumnKeys: Int): Unit = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(tableName))
    val partitionColValues = snapshot.allFiles.collect().map(_.partitionValues.values.toList)
    assert(partitionColValues.distinct.length === totalPartitionKeys)
    assert(partitionColValues.map(_.head).distinct.length === firsColumnKeys)
    assert(partitionColValues.map(_.last).distinct.length === lastColumnKeys)
  }

  private def createTableAndInsertData(dir: File, testType: String, testData: String): String = {
    val path = dir.getCanonicalPath
    sql(
      s"create table delta.`$path`(c1 interval $testType, c2 interval $testType) using delta"
    )
    sql(s"insert into delta.`$path` values(interval $testData, interval $testData)")
    path
  }

  private def assertResultAndNumOfFilesRead(
      expectedRows: Seq[Row],
      numFilesRead: Int,
      sqlText: String): Unit = {
        val query = sql(sqlText)
        checkAnswer(query, expectedRows)

        val filesRead = getNumberOfFilesRead(query)
        assert(filesRead.get === numFilesRead)
  }

  private def getNumberOfFilesRead(result: DataFrame): Option[Long] = {
    result.queryExecution.executedPlan.collectFirst {
      case f: FileSourceScanExec =>
        f.selectedPartitions.totalNumberOfFiles
    }
  }


  private sealed trait ExpectedBehavior
  private case class ExpectEqualTo(expectedResult: Any) extends ExpectedBehavior
  private case class ExpectNotEqualTo(falseResult: Any) extends ExpectedBehavior
  private case class ExpectException(validateException: Throwable => Unit) extends ExpectedBehavior

  private case class InitTestData(testType: String, testData: String)
  private case class IntervalTestCase[T <: ExpectedBehavior](
      initData: InitTestData,
      expectedBehaviour: T
  ) {
    def this(testType: String, testData: String, expectedBehaviour: T) =
      this(InitTestData(testType, testData), expectedBehaviour)

    def testData: String = initData.testData
    def testType: String = initData.testType

    def performTest(initTest: InitTestData => Unit, checkResult: Any => Unit): Unit = {
      try {
        initTest(initData)
        expectedBehaviour match {
          case ExpectEqualTo(expectedResult) =>
            checkResult(expectedResult)
          case ExpectNotEqualTo(unexpectedResult) =>
            // Inverted logic path, we expect a failure.
            assertThrows[TestFailedException] {
              checkResult(unexpectedResult)
            }
          case ExpectException(_) =>
            fail("test should throw an exception with message but didn't")
        }
      } catch {
        // ArithmeticException should be fixed in SPARK-50072
        case e @ (_: SparkIllegalArgumentException | _: ArithmeticException) =>
          expectedBehaviour match {
            case ExpectException(validateException) =>
              validateException(e)
            case _ =>
              fail(s"test failed with $e but exception was not expected")
          }
      }
    }
  }

  // Test cases for INTERVAL type
  // Used in several parametrized tests
  private val intervalCases: Seq[IntervalTestCase[ExpectedBehavior]] = Seq(
    new IntervalTestCase(
      "year to month",
      "10 years 5 months",
      ExpectEqualTo(Period.of(10, 5, 0)),
    ),
    new IntervalTestCase(
      "day to second",
      "2 day 3 hour 4 minute 5 second",
      ExpectEqualTo(Duration.ofSeconds(183845))
    ),
    new IntervalTestCase(
      "year to month",
      "178956970 year 7 months",
      ExpectEqualTo(Period.of(178956970, 7, 0))
    ),
    new IntervalTestCase(
      "year",
      "11 years",
      ExpectEqualTo(Period.of(11, 0, 0))
    ),
    new IntervalTestCase(
      "year",
      "11 year",
      ExpectEqualTo(Period.of(11, 0, 0))
    ),
    new IntervalTestCase(
      "month",
      "6 months",
      ExpectEqualTo(Period.of(0, 6, 0))
    ),
    new IntervalTestCase(
      "month",
      "16 months",
      ExpectEqualTo(Period.of(1, 4, 0))
    ),
    new IntervalTestCase(
      "month",
      "-599 months",
      ExpectEqualTo(Period.of(-49, -11, 0))
    ),
    new IntervalTestCase(
      "month",
      "-600 months",
      ExpectEqualTo(Period.of(-50, 0, 0))
    ),
    new IntervalTestCase(
      "month",
      "-3600 months",
      ExpectEqualTo(Period.of(-300, 0, 0))
    ),
    new IntervalTestCase(
      "year to month",
      "-1 year -3 months",
      ExpectEqualTo(Period.of(-1, -3, 0))
    ),
    new IntervalTestCase(
      "year to month",
      "-1 year 3 months",
      ExpectEqualTo(Period.of(0, -9, 0))
    ),
    new IntervalTestCase(
      "year to month",
      "1 year -3 months",
      ExpectEqualTo(Period.of(0, 9, 0))
    ),
    // 6 months != 8 months
    new IntervalTestCase(
      "month",
      "6 month",
      ExpectNotEqualTo(Period.of(0, 8, 0))
    ),
    new IntervalTestCase(
      "month",
      "0 month",
      ExpectEqualTo(Period.ofWeeks(0))
    ),
    // Period 7 days != Duration 168 hours
    new IntervalTestCase(
      "day to second",
      "7 days 0 hours 0 minutes 0 seconds",
      ExpectNotEqualTo(Period.ofDays(7))
    ),
    new IntervalTestCase(
      "day to second",
      "106751991 day 4 hour 0 minute 54.775 second",
      ExpectEqualTo(Duration.ofSeconds(9223372036854L, 775000000L))
    ),
    new IntervalTestCase(
      "day to second",
      "106751991 day 4 hour 0 minute 54.775807 second",
      ExpectEqualTo(Duration.ofSeconds(9223372036854L, 775807000L))
    ),
    new IntervalTestCase(
      "day to second",
      "-106751991 day -4 hour -0 minute -54.775808 second",
      ExpectEqualTo(Duration.ofSeconds(-9223372036854L, -775808000L))
    ),
    new IntervalTestCase(
      "day to second",
      "'1 day 4 hour 0 minute 54.775807 second'",
      ExpectEqualTo(Duration.ofSeconds(100854, 775807000L))
    ),
    new IntervalTestCase(
      "day to second",
      "-1 day -4 hour -0 minute -54.775807 second",
      ExpectEqualTo(Duration.ofSeconds(-100854, -775807000L))
    ),
    new IntervalTestCase(
      "day to second",
      "-1 day +1 hour -1 minute +1.000001 second",
      ExpectEqualTo(Duration.ofSeconds(-82858, -999999000L))
    ),
    new IntervalTestCase(
      "day to second",
      "+1 day -1 hour +1 minute -1.000001 second",
      ExpectEqualTo(Duration.ofSeconds(82858, 999999000L))
    ),
    // Overflow
    new IntervalTestCase(
      "day to second",
      "106751991 day 4 hour 0 minute 54.776 second",
      ExpectException {
        case e: ArithmeticException =>
          assert(e.getMessage.contains("INTERVAL_ARITHMETIC_OVERFLOW"))
      }
    ),
    new IntervalTestCase(
      "year to month",
      "178956970 years 8 months",
      ExpectException {
        case e: SparkIllegalArgumentException =>
          assert(e.getMessageParameters.asScala === Map("input" -> " 178956970 years 8 months"))
      }
    ),
    new IntervalTestCase(
      "year to month",
      "-178956970 years -9 months",
      ExpectException {
        case e: SparkIllegalArgumentException =>
          assert(e.getMessageParameters.asScala === Map("input" -> " -178956970 years -9 months"))
      }
    ),
    new IntervalTestCase(
      "day to second",
      "7 day 0 hour 0 minute 0 second",
      ExpectEqualTo(Duration.ofDays(7))
    ),
    new IntervalTestCase(
      "day to hour",
      "1 day 0 hours",
      ExpectEqualTo(Duration.ofSeconds(86400))
    ),
    new IntervalTestCase(
      "day to second",
      "1 day 0 second",
      ExpectEqualTo(Duration.ofSeconds(86400))
    )
  )

  private val positiveIntervalCases: Seq[IntervalTestCase[ExpectEqualTo]] =
    intervalCases.collect {
      case tc @ IntervalTestCase(_, _: ExpectEqualTo) =>
        tc.asInstanceOf[IntervalTestCase[ExpectEqualTo]]
    }

  for {
    testCase <- intervalCases
  } test(s"create table with interval then insert then select - $testCase") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(
        s"""create table delta.`$path`(
           |  c1 interval ${testCase.testType},
           |  c2 interval ${testCase.testType})
           | using delta""".stripMargin
      )
      try {
        sql(
          s"""insert into delta.`$path` values(
             |interval ${testCase.testData},
             |interval ${testCase.testData})""".stripMargin
        )
        testCase.expectedBehaviour match {
          case ExpectEqualTo(expectedResult) =>
            checkAnswer(
              sql(s"select * from delta.`$path`"),
              Seq(Row(expectedResult, expectedResult))
            )
          case ExpectNotEqualTo(unexpectedResult) =>
            // Inverted logic path, we expect a failure.
            assertThrows[TestFailedException] {
              checkAnswer(
                sql(s"select * from delta.`$path`"),
                Seq(Row(unexpectedResult, unexpectedResult))
              )
            }
          case ExpectException(_) =>
            fail("test should throw an exception with message but didn't")
        }
      } catch {
        case e @ (_: SparkIllegalArgumentException | _: ArithmeticException) =>
          testCase.expectedBehaviour match {
            case ExpectException(validateException) =>
              validateException(e)
            case _ =>
              fail(s"test failed with $e but exception was not expected")
          }
      }
    }
  }

  // This test highlights lack of support so we dont need more then one case
  test(s"v1 Write API does not support interval") {
    val testCase = positiveIntervalCases.head
    withTempDir { dir =>
      val path = createTableAndInsertData(dir, testCase.testType, testCase.testData)
      checkError(
        exception = intercept[AnalysisException] {
          sql(
            s"select interval ${testCase.testData} as c1, interval ${testCase.testData} as c2"
          ).write
            .mode("append")
            .format("delta")
            .save(path)
        },
        condition = "UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE",
        parameters = Map(
          "columnName" -> "`c1`",
          "columnType" -> "\"INTERVAL .*\"",
          "format" -> ".*DeltaDataSource.*"
        ),
        matchPVals = true
      )
    }
  }

  for {
    // Negative cases would cover the same code path as other tests so they would be redundant here.
    testCase <- positiveIntervalCases
  } test(s"v2 Write API supports interval - $testCase") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(
        s"""create table delta.`$path`(
           | c1 interval ${testCase.testType},
           | c2 interval ${testCase.testType})
           |  using delta""".stripMargin
      )
      sql(s"select interval ${testCase.testData} as c1, interval ${testCase.testData} as c2")
        .writeTo(s"delta.`$path`")
        .append()
      val expectedResult = testCase.expectedBehaviour.expectedResult
      checkAnswer(
        sql(s"select * from delta.`$path`"),
        Seq(Row(expectedResult, expectedResult))
      )
    }
  }


  for {
    testCase <- intervalCases
  } test(s"Streaming Write with interval - $testCase") {
    withTempDirs { (sourceDir, checkpointDir, destDir) =>
      val destPath = destDir.getCanonicalPath
      testCase.performTest(
        initTest = { initData =>
          sql(
            s"""create table delta.`$destPath`(
               |  c1 interval ${initData.testType},
               |  c2 interval ${initData.testType})
               | using delta""".stripMargin
          )
          val sourcePath = createTableAndInsertData(sourceDir, initData.testType, initData.testData)
          val q = spark.readStream
            .format("delta")
            .option("inferSchema", "true")
            .load(sourcePath)
            .writeStream
            .format("delta")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .trigger(Trigger.AvailableNow())
            .start(destPath)
          q.awaitTermination(10000)
        },
        checkResult = { expectedResult =>
          checkAnswer(
            sql(s"select * from delta.`$destPath`"),
            Seq(Row(expectedResult, expectedResult)))
        }
      )
    }
  }

  /**
   *  Looks strange that unsupported type is considered as supported from the minimal version.
   *  This behavior is acceptable for now but may change in the future.
   *  @see DeltaTimestampNTZSuite - as an example of the desired behaviour
   */
  test("creating a table with interval uses the usual minimum protocol") {
    withTable("tbl") {
      sql(
        """create table tbl(c1 string, c2 interval year to month, c3 interval day to second)
          | using delta""".stripMargin
      )
      assert(getProtocolForTable("tbl") === Protocol(1, 2))
    }
  }

  test("alter table wth add column with interval type and add default value") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(
        s"""create table delta.`$path`(c1 interval year to month, c2 interval day to second)
           | using delta""".stripMargin
      )
      sql(
        s"""insert into delta.`$path` values(interval '10 years 5 months',
           | interval '2 days 3 hours 4 minutes 5 seconds')""".stripMargin
      )
      // add column
      sql(s"alter table delta.`$path` add column c3 interval year")
      sql(
        s"""insert into delta.`$path` values(interval '10 years 5 months',
           | interval '2 days 3 hours 4 minutes 5 seconds', interval '2 years')""".stripMargin
      )
      // add default value
      sql(
        s"""alter table delta.`$path`
           | set tblproperties('delta.feature.allowColumnDefaults' = 'supported')""".stripMargin
      )
      sql(s"alter table delta.`$path` alter column c2 set default interval '42 seconds'")
      sql(s"alter table delta.`$path` alter column c3 set default interval '1 years'")
      sql(s"insert into delta.`$path`(c1) values(interval '10 years 5 months')")

      checkAnswer(
        sql(s"select * from delta.`$path`"),
        Seq(
          Row(
            Period.of(10, 5, 0),
            Duration.ofSeconds(((2 * 24 + 3) * 60 + 4) * 60 + 5),
            null
          ),
          Row(
            Period.of(10, 5, 0),
            Duration.ofSeconds(((2 * 24 + 3) * 60 + 4) * 60 + 5),
            Period.of(2, 0, 0)
          ),
          Row(
            Period.of(10, 5, 0),
            Duration.ofSeconds(42),
            Period.of(1, 0, 0)
          )
        )
      )
    }
  }

  test("generated column with interval type") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(
        s"""create table delta.`$path`(c1 string, c2 string,
           | c3 interval year to month generated always as (cast(c1 as interval year to month)),
           | c4 interval day to second generated always as (cast(c2 as interval day to second)))
           | using delta""".stripMargin
      )
      sql(s"insert into delta.`$path`(c1, c2) values('10-5', '0 0:1:5')")
      checkAnswer(
        sql(s"select * from delta.`$path`"),
        Seq(Row("10-5", "0 0:1:5", Period.of(10, 5, 0), Duration.ofSeconds(65)))
      )
    }
  }

  test("partitioned by generated column with interval type") {
    withTable("delta_test") {
      sql(
        """create table delta_test(c1 string, c2 string,
           |c3 interval year to month generated always as (cast(c1 as interval year to month)),
           |c4 interval day to second generated always as (cast(c2 as interval day to second)))
           |using delta
           |partitioned by (c3, c4)""".stripMargin
      )
      sql(
        """insert into delta_test(c1, c2) values
           |('2-7', '1 2:3:4'),
           |('-5-3', '1 2:3:4'),
           |('2-7', '-0 0:0:30'),
           |('-5-3', '-0 0:0:30'),
           |('1-0', '1 0:1:0'),
           |('1-0', '1 0:1:0.0')""".stripMargin
      )
      assertPartitionByTwoColumns(
        "delta_test",
        totalPartitionKeys = 5,
        firsColumnKeys = 3,
        lastColumnKeys = 3
      )
      checkAnswer(
        sql("select * from delta_test"),
        Seq(
          Row(
            "2-7",
            "1 2:3:4",
            Period.of(2, 7, 0),
            Duration.ofSeconds(((1 * 24 + 2) * 60 + 3) * 60 + 4)
          ),
          Row(
            "-5-3",
            "1 2:3:4",
            Period.of(-5, -3, 0),
            Duration.ofSeconds(((1 * 24 + 2) * 60 + 3) * 60 + 4)
          ),
          Row("2-7", "-0 0:0:30", Period.of(2, 7, 0), Duration.ofSeconds(-30)),
          Row("-5-3", "-0 0:0:30", Period.of(-5, -3, 0), Duration.ofSeconds(-30)),
          Row("1-0", "1 0:1:0", Period.of(1, 0, 0), Duration.ofSeconds(24 * 60 * 60 + 60)),
          Row("1-0", "1 0:1:0.0", Period.of(1, 0, 0), Duration.ofSeconds(24 * 60 * 60 + 60))
        )
      )
    }
  }



  /**
   * Doesn't work for intervals for now.
   * When it will be fixed, the dataskipping test should be updated in
   * [[org.apache.spark.sql.delta.stats.DataSkippingDeltaTests]].
   */
  ignore("min/max stats collection should apply on interval") {
    withTable("delta_test") {
      val schemaString = "c1 string, c2 interval year to month, c3 interval day to second"
      sql(s"create table delta_test($schemaString) using delta")
      val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("delta_test"))
      val statsSchema = snapshot.statsSchema
      assert(statsSchema("minValues").dataType === StructType.fromDDL(schemaString))
      assert(statsSchema("maxValues").dataType === StructType.fromDDL(schemaString))
    }
  }

  test("should be able to partition by interval") {
    withTable("delta_test") {
      val schemaString = "c1 string, c2 interval year to month, c3 interval day to second"
      sql(s"create table delta_test($schemaString) using delta partitioned by (c2, c3)")
      // 4 distinct partition values + 1 repeat
      sql("""insert into delta_test values
             | ('foo', interval '10 years 5 months', interval '2 days 3 hours 4 minutes 5 seconds'),
             | ('foo', interval '10 years 0 months', interval '2 days 3 hours 4 minutes 5 seconds'),
             | ('foo', interval '10 years 5 months', interval '5.55 seconds'),
             | ('foo', interval '10 years 0 months', interval '5.55 seconds'),
             | ('bar', interval '10 years', interval '0 minutes 5.55 seconds')""".stripMargin)

      val expectedRows = Seq(
        Row("foo", Period.of(10, 5, 0), Duration.ofSeconds(((2 * 24 + 3) * 60 + 4) * 60 + 5)),
        Row("foo", Period.of(10, 0, 0), Duration.ofSeconds(((2 * 24 + 3) * 60 + 4) * 60 + 5)),
        Row("foo", Period.of(10, 5, 0), Duration.ofSeconds(5, 550000000)),
        Row("foo", Period.of(10, 0, 0), Duration.ofSeconds(5, 550000000)),
        Row("bar", Period.of(10, 0, 0), Duration.ofSeconds(5, 550000000))
      )
      // check delta's metadata
      assertPartitionByTwoColumns(
        "delta_test",
        totalPartitionKeys = 4,
        firsColumnKeys = 2,
        lastColumnKeys = 2
      )
      // check execution on partitioned data
      assertResultAndNumOfFilesRead(expectedRows, numFilesRead = 4, "select * from delta_test")
      assertResultAndNumOfFilesRead(
        Seq(expectedRows(3), expectedRows(4)),
        numFilesRead = 1,
        """select *
          | from delta_test
          | where c2 = interval '10 years 0 months' and c3 = interval '5.55 seconds'""".stripMargin
      )
      assertResultAndNumOfFilesRead(
        Seq(expectedRows(1), expectedRows(3), expectedRows(4)),
        numFilesRead = 2,
        "select * from delta_test where c2 = interval '10 years 0 months'"
      )
      assertResultAndNumOfFilesRead(
        Seq(expectedRows(0), expectedRows(2)),
        numFilesRead = 2,
        "select * from delta_test where c2 = interval '10 years 5 months'"
      )
      assertResultAndNumOfFilesRead(
        Seq(),
        numFilesRead = 0,
        "select * from delta_test where c2 = interval '23 years 5 months'"
      )
    }
  }
}
