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

package org.apache.spark.sql.delta

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.ImplicitDMLCastingSuiteShims._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaExceptionTestUtils, DeltaSQLCommandTest}

import org.apache.spark.{SparkConf, SparkException, SparkThrowable}
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.internal.SQLConf

/**
 * Tests for casts that are implicitly added in DML commands modifying Delta tables.
 * These casts are added to convert values to the schema of a table.
 * INSERT operations are excluded as they are covered by InsertSuite and InsertSuiteEdge.
 */
abstract class ImplicitDMLCastingSuite extends QueryTest
  with DeltaExceptionTestUtils
  with DeltaSQLCommandTest {

  /** Implement the actual test for a specific DML command in subclasses. */
  protected def commandTest(sqlConfig: SqlConfiguration, testConfig: TestConfiguration): Unit

  protected case class TestConfiguration(
      sourceType: String,
      sourceTypeInErrorMessage: String,
      targetType: String,
      targetTypeInErrorMessage: String,
      validValue: String,
      overflowValue: String,
      // String because SparkArithmeticException is private and cannot be used for matching.
      exceptionAnsiCast: String
  ) {
    override def toString: String = s"sourceType: $sourceType, targetType: $targetType"
  }

  protected case class SqlConfiguration(
      followAnsiEnabled: Boolean,
      ansiEnabled: Boolean,
      storeAssignmentPolicy: SQLConf.StoreAssignmentPolicy.Value) {

    def withSqlSettings(f: => Unit): Unit =
      withSQLConf(
        DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key
          -> followAnsiEnabled.toString,
        SQLConf.STORE_ASSIGNMENT_POLICY.key -> storeAssignmentPolicy.toString,
        SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString)(f)

    override def toString: String =
      s"followAnsiEnabled: $followAnsiEnabled, ansiEnabled: $ansiEnabled," +
        s" storeAssignmentPolicy: $storeAssignmentPolicy"
  }

  protected def expectLegacyCastingBehaviour(sqlConfig: SqlConfiguration): Boolean = {
    (sqlConfig.followAnsiEnabled && !sqlConfig.ansiEnabled) ||
      (!sqlConfig.followAnsiEnabled &&
        sqlConfig.storeAssignmentPolicy == SQLConf.StoreAssignmentPolicy.LEGACY)
  }

  // Note that DATE to TIMESTAMP casts are not in this list as they always throw an error on
  // overflow no matter if ANSI is enabled or not.
  private val testConfigurations = Seq(
    TestConfiguration(sourceType = "INT", sourceTypeInErrorMessage = "INT",
      targetType = "TINYINT", targetTypeInErrorMessage = "TINYINT",
      validValue = "1", overflowValue = Int.MaxValue.toString,
      exceptionAnsiCast = "SparkArithmeticException"),
    TestConfiguration(sourceType = "INT", sourceTypeInErrorMessage = "INT",
      targetType = "SMALLINT", targetTypeInErrorMessage = "SMALLINT",
      validValue = "1", overflowValue = Int.MaxValue.toString,
      exceptionAnsiCast = "SparkArithmeticException"),
    TestConfiguration(sourceType = "BIGINT", sourceTypeInErrorMessage = "BIGINT",
      targetType = "INT", targetTypeInErrorMessage = "INT",
      validValue = "1", overflowValue = Long.MaxValue.toString,
      exceptionAnsiCast = "SparkArithmeticException"),
    TestConfiguration(sourceType = "DOUBLE", sourceTypeInErrorMessage = "DOUBLE",
      targetType = "BIGINT", targetTypeInErrorMessage = "BIGINT",
      validValue = "1", overflowValue = "12345678901234567890D",
      exceptionAnsiCast = "SparkArithmeticException"),
    TestConfiguration(sourceType = "BIGINT", sourceTypeInErrorMessage = "BIGINT",
      targetType = "DECIMAL(7,2)", targetTypeInErrorMessage = "DECIMAL(7,2)",
      validValue = "1", overflowValue = Long.MaxValue.toString,
      exceptionAnsiCast = "SparkArithmeticException"),
    TestConfiguration(sourceType = "Struct<value:BIGINT>", sourceTypeInErrorMessage = "BIGINT",
      targetType = "Struct<value:INT>", targetTypeInErrorMessage = "INT",
      validValue = "named_struct('value', 1)",
      overflowValue = s"named_struct('value', ${Long.MaxValue.toString})",
      exceptionAnsiCast = "SparkArithmeticException"),
    TestConfiguration(sourceType = "ARRAY<BIGINT>", sourceTypeInErrorMessage = "ARRAY<BIGINT>",
      targetType = "ARRAY<INT>", targetTypeInErrorMessage = "ARRAY<INT>",
      validValue = "ARRAY(1)", overflowValue = s"ARRAY(${Long.MaxValue.toString})",
      exceptionAnsiCast = "SparkArithmeticException"),
    TestConfiguration(sourceType = "STRING", sourceTypeInErrorMessage = "STRING",
      targetType = "INT", targetTypeInErrorMessage = "INT",
      validValue = "'1'", overflowValue = s"'${Long.MaxValue.toString}'",
      exceptionAnsiCast = "SparkNumberFormatException"),
    TestConfiguration(sourceType = "MAP<STRING, BIGINT>",
      sourceTypeInErrorMessage = "MAP<STRING, BIGINT>", targetType = "MAP<STRING, INT>",
      targetTypeInErrorMessage = "MAP<STRING, INT>", validValue = "map('abc', 1)",
      overflowValue = s"map('abc', ${Long.MaxValue.toString})",
      exceptionAnsiCast = "SparkArithmeticException"),
    TestConfiguration(sourceType = "DECIMAL(3,1)",
      sourceTypeInErrorMessage = "DECIMAL(3,1)", targetType = "DECIMAL(3,2)",
      targetTypeInErrorMessage = "DECIMAL(3,2)", validValue = "CAST(1 AS DECIMAL(3,1))",
      overflowValue = s"CAST(12.3 AS DECIMAL(3,1))",
      exceptionAnsiCast = "SparkArithmeticException")
  )

  /** Returns cast failure exception if present in the cause chain. None otherwise. */
  @tailrec
  private def castFailureCause(exception: Throwable): Option[Throwable] = {
    exception match {
      case arithmeticException: ArithmeticException => Some(arithmeticException)
      case numberFormatException: NumberFormatException => Some(numberFormatException)
      case _ if exception.getCause != null => castFailureCause(exception.getCause)
      case _ => None
    }
  }

  /**
   * Validate that a custom error is throws in case ansi.enabled is false, or a different
   * overflow error is case ansi.enabled is true.
   */
  protected def validateException(
      exception: Throwable, sqlConfig: SqlConfiguration, testConfig: TestConfiguration): Unit = {
    // Validate that the type of error matches the expected error type.
    castFailureCause(exception) match {
      case Some(failureCause) if sqlConfig.followAnsiEnabled =>
        assert(sqlConfig.ansiEnabled)
        assert(failureCause.toString.contains(testConfig.exceptionAnsiCast))

        val sparkThrowable = failureCause.asInstanceOf[SparkThrowable]
        assert(Seq("CAST_OVERFLOW", NUMERIC_VALUE_OUT_OF_RANGE_ERROR_MSG, "CAST_INVALID_INPUT")
          .contains(sparkThrowable.getErrorClass))
      case Some(failureCause) if !sqlConfig.followAnsiEnabled =>
        assert(sqlConfig.storeAssignmentPolicy === SQLConf.StoreAssignmentPolicy.ANSI)

        val sparkThrowable = failureCause.asInstanceOf[SparkThrowable]
        // Only arithmetic exceptions get a custom error message.
        if (testConfig.exceptionAnsiCast == "SparkArithmeticException") {
          assert(sparkThrowable.getErrorClass == "DELTA_CAST_OVERFLOW_IN_TABLE_WRITE")
          assert(sparkThrowable.getMessageParameters ==
            Map("sourceType" -> ("\"" + testConfig.sourceTypeInErrorMessage + "\""),
                "targetType" -> ("\"" + testConfig.targetTypeInErrorMessage + "\""),
                "columnName" -> "`value`",
                "storeAssignmentPolicyFlag" -> SQLConf.STORE_ASSIGNMENT_POLICY.key,
                "updateAndMergeCastingFollowsAnsiEnabledFlag" ->
                  DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key,
                "ansiEnabledFlag" -> SQLConf.ANSI_ENABLED.key).asJava)
        } else {
          assert(sparkThrowable.getErrorClass == "CAST_INVALID_INPUT")
          assert(sparkThrowable.getMessageParameters.get("sourceType") == "\"STRING\"")
        }
      case None => assert(false, s"No arithmetic exception thrown: $exception")
    }
  }
  for {
    followAnsiEnabled <- BOOLEAN_DOMAIN
    ansiEnabled <- BOOLEAN_DOMAIN
    storeAssignmentPolicy <-
      Seq(SQLConf.StoreAssignmentPolicy.LEGACY, SQLConf.StoreAssignmentPolicy.ANSI)
    sqlConfiguration <-
      Some(SqlConfiguration(followAnsiEnabled, ansiEnabled, storeAssignmentPolicy))
    testConfiguration <- testConfigurations
  } commandTest(sqlConfiguration, testConfiguration)


  test("Details are part of the error message") {
    val sourceTableName = "source_table_name"
    val sourceValueType = "INT"
    val targetTableName = "target_table_name"
    val targetValueType = "LONG"
    val valueColumnName = "value"

    withTable(sourceTableName, targetTableName) {
      sql(s"CREATE OR REPLACE TABLE $targetTableName(id LONG, $valueColumnName $sourceValueType) " +
        "USING DELTA")
      sql(s"CREATE OR REPLACE TABLE $sourceTableName(id LONG, $valueColumnName $targetValueType) " +
        "USING DELTA")
      sql(s"INSERT INTO $sourceTableName VALUES(0, 9223372036854775807)")

      val userFacingError = interceptWithUnwrapping[DeltaArithmeticException] {
        sql(s"""MERGE INTO $targetTableName t
               |USING $sourceTableName s
               |ON s.id = t.id
               |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      }
      val expectedDetails =
        Seq("DELTA_CAST_OVERFLOW_IN_TABLE_WRITE", sourceValueType, valueColumnName)
      for (detail <- expectedDetails) {
        assert(userFacingError.toString.contains(detail))
      }
    }
  }
}

class ImplicitUpdateCastingSuite extends ImplicitDMLCastingSuite {

  /** Test an UPDATE that requires to cast the update value that is part of the SET clause. */
  override protected def commandTest(
      sqlConfig: SqlConfiguration,
      testConfig: TestConfiguration): Unit = {
    val testName = s"UPDATE overflow $testConfig $sqlConfig"
    test(testName) {
      sqlConfig.withSqlSettings {
        val tableName = "overflowTable"
        withTable(tableName) {
          sql(s"""CREATE TABLE $tableName USING DELTA
                 |AS SELECT cast(${testConfig.validValue} AS ${testConfig.targetType}) AS value
                 |""".stripMargin)
          val updateCommand = s"UPDATE $tableName SET value = ${testConfig.overflowValue}"

          if (expectLegacyCastingBehaviour(sqlConfig)) {
            sql(updateCommand)
          } else {
            val exception = intercept[Throwable] {
              sql(updateCommand)
            }

            validateException(exception, sqlConfig, testConfig)
          }
        }
      }
    }
  }
}

class ImplicitMergeCastingSuite extends ImplicitDMLCastingSuite {

  /** Tests for MERGE with overflows cause by the different conditions. */
  override protected def commandTest(
      sqlConfig: SqlConfiguration,
      testConfig: TestConfiguration): Unit = {
    mergeTest(matchedCondition = s"WHEN MATCHED THEN UPDATE SET t.value = s.value",
      sqlConfig, testConfig)

    mergeTest(matchedCondition = s"WHEN NOT MATCHED THEN INSERT *", sqlConfig, testConfig)

    mergeTest(matchedCondition =
      s"WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.value = ${testConfig.overflowValue}",
      sqlConfig, testConfig)
  }

  private def mergeTest(
      matchedCondition: String,
      sqlConfig: SqlConfiguration,
      testConfig: TestConfiguration): Unit = {
    val testName = s"MERGE overflow in $matchedCondition $testConfig $sqlConfig"
    test(testName) {
      sqlConfig.withSqlSettings {
        val targetTableName = "target_table"
        val sourceViewName = "source_view"
        withTable(targetTableName) {
          withTempView(sourceViewName) {
            val numRows = 10
            sql(s"""CREATE TABLE $targetTableName USING DELTA
                   |AS SELECT col as key,
                   |  cast(${testConfig.validValue} AS ${testConfig.targetType}) AS value
                   |FROM explode(sequence(0, $numRows))""".stripMargin)
            // The view maps the key space such that we get matched, not matched by source, and
            // not match by target rows.
            sql(s"""CREATE TEMPORARY VIEW $sourceViewName
                   |AS SELECT key + ($numRows / 2) AS key,
                   |  cast(${testConfig.overflowValue} AS ${testConfig.sourceType}) AS value
                   |FROM $targetTableName""".stripMargin)
            val mergeCommand = s"""MERGE INTO $targetTableName t
                                  |USING $sourceViewName s
                                  |ON s.key = t.key
                                  |$matchedCondition
                                  |""".stripMargin

            if (expectLegacyCastingBehaviour(sqlConfig)) {
              sql(mergeCommand)
            } else {
              val exception = intercept[Throwable] {
                sql(mergeCommand)
              }

              validateException(exception, sqlConfig, testConfig)
            }
          }
        }
      }
    }
  }
}

class ImplicitStreamingMergeCastingSuite extends ImplicitDMLCastingSuite {

  /** A merge that is executed for each batch of a stream and has to cast values before insert. */
  override protected def commandTest(
      sqlConfig: SqlConfiguration,
      testConfig: TestConfiguration): Unit = {
    val testName = s"Streaming MERGE overflow $testConfig $sqlConfig"
    test(testName) {
      sqlConfig.withSqlSettings {
        val targetTableName = "target_table"
        val sourceTableName = "source_table"
        withTable(sourceTableName, targetTableName) {
          sql(s"CREATE TABLE $targetTableName (key INT, value ${testConfig.targetType})" +
            " USING DELTA")
          sql(s"CREATE TABLE $sourceTableName (key INT, value ${testConfig.sourceType})" +
            " USING DELTA")

          def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long): Unit = {
            microBatchOutputDF.createOrReplaceTempView("micro_batch_output")

            microBatchOutputDF.sparkSession.sql(s"""MERGE INTO $targetTableName t
                                                   |USING micro_batch_output s
                                                   |ON s.key = t.key
                                                   |WHEN NOT MATCHED THEN INSERT *
                                                   |""".stripMargin)
          }

          val sourceStream = spark.readStream.table(sourceTableName)
          val streamWriter =
            sourceStream
              .writeStream
              .format("delta")
              .foreachBatch(upsertToDelta _)
              .outputMode("update")
              .start()

          sql(s"INSERT INTO $sourceTableName(key, value) VALUES(0, ${testConfig.overflowValue})")

          if (expectLegacyCastingBehaviour(sqlConfig)) {
            streamWriter.processAllAvailable()
          } else {
            val exception = intercept[Throwable] {
              streamWriter.processAllAvailable()
            }

            validateException(exception, sqlConfig, testConfig)
          }
        }
      }
    }
  }
}

