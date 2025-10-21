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

package org.apache.spark.sql.delta.typewidening

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap


/**
 * Suite covering widening columns and fields type as part of automatic schema evolution in INSERT
 * when the type widening table feature is supported.
 */
class TypeWideningInsertSchemaEvolutionBasicSuite
    extends QueryTest
    with DeltaDMLTestUtils
    with TypeWideningTestMixin
    with TypeWideningInsertSchemaEvolutionBasicTests {

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key, "true")
  }
}

/**
 * Tests covering type widening during schema evolution in INSERT.
 */
trait TypeWideningInsertSchemaEvolutionBasicTests
  extends DeltaInsertIntoTest
  with TypeWideningTestCases
  with DeltaExcludedBySparkVersionTestMixinShims {
  self: QueryTest with TypeWideningTestMixin with DeltaDMLTestUtils =>

  import testImplicits._
  import scala.collection.JavaConverters._

  for {
    testCase <- restrictedAutomaticWideningTestCases ++ supportedTestCases
  } {
    test(s"INSERT - always automatic type widening " +
      s"${testCase.fromType.sql} -> ${testCase.toType.sql}") {
      append(testCase.initialValuesDF)

      withSQLConf(DeltaSQLConf.DELTA_ALLOW_AUTOMATIC_WIDENING.key -> "always") {
        testCase.additionalValuesDF
          .write
          .format("delta")
          .mode("append")
          .option("mergeSchema", "true")
          .insertInto(s"delta.`$tempPath`")
      }

      assert(readDeltaTable(tempPath).schema("value").dataType === testCase.toType)
      checkAnswerWithTolerance(
        actualDf = readDeltaTable(tempPath).select("value"),
        expectedDf = testCase.expectedResult.select($"value".cast(testCase.toType)),
        toType = testCase.toType
      )
    }
  }

  for {
    testCase <- restrictedAutomaticWideningTestCases ++ supportedTestCases
  } {
    test(s"INSERT - never automatic type widening " +
      s"${testCase.fromType.sql} -> ${testCase.toType.sql}") {
      append(testCase.initialValuesDF)

      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.LEGACY.toString,
        DeltaSQLConf.DELTA_ALLOW_AUTOMATIC_WIDENING.key -> "never") {
        testCase.additionalValuesDF
          .write
          .format("delta")
          .mode("append")
          .option("mergeSchema", "true")
          .insertInto(s"delta.`$tempPath`")
      }

      assert(readDeltaTable(tempPath).schema("value").dataType === testCase.fromType)
    }
  }

  testSparkMasterOnly(s"INSERT - logs for missed opportunity for conversion") {
    val testCase = restrictedAutomaticWideningTestCases.head

    append(testCase.initialValuesDF)

    val events = Log4jUsageLogger.track {
      withSQLConf(
          SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.LEGACY.toString,
          DeltaSQLConf.DELTA_ALLOW_AUTOMATIC_WIDENING.key -> "same_family_type") {
        testCase.additionalValuesDF
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .insertInto(s"delta.`$tempPath`")
      }
    }

    assert(readDeltaTable(tempPath).schema("value").dataType === testCase.fromType)
    assert(events.exists(event => event.metric == "tahoeEvent" &&
      event.tags.get("opType") == Option("delta.typeWidening.missedAutomaticWidening")))
  }

  test(s"INSERT - no logs for lack of missed opportunity for conversion") {
    val testCase = supportedTestCases.head
    append(testCase.initialValuesDF)

    val events = Log4jUsageLogger.track {
      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.LEGACY.toString) {
        testCase.additionalValuesDF
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .insertInto(s"delta.`$tempPath`")
      }
    }

    assert(readDeltaTable(tempPath).schema("value").dataType === testCase.toType)
    assert(!events.exists(event => event.metric == "tahoeEvent" &&
      event.tags.get("opType") == Option("delta.typeWidening.missedAutomaticWidening")))
  }

  for {
    testCase <- supportedTestCases ++ restrictedAutomaticWideningTestCases
  } {
    test(s"INSERT - automatic type widening ${testCase.fromType.sql} -> ${testCase.toType.sql}") {
      append(testCase.initialValuesDF)
      testCase.additionalValuesDF
        .write
        .mode("append")
        .insertInto(s"delta.`$tempPath`")

      assert(readDeltaTable(tempPath).schema("value").dataType === testCase.toType)
      checkAnswerWithTolerance(
        actualDf = readDeltaTable(tempPath).select("value"),
        expectedDf = testCase.expectedResult.select($"value".cast(testCase.toType)),
        toType = testCase.toType
      )
    }
  }

  for {
    testCase <- unsupportedTestCases
  } {
    test(s"INSERT - unsupported automatic type widening " +
      s"${testCase.fromType.sql} -> ${testCase.toType.sql}") {
      append(testCase.initialValuesDF)
      // Test cases for some of the unsupported type changes may overflow while others only have
      // values that can be implicitly cast to the narrower type - e.g. double ->float.
      // We set storeAssignmentPolicy to LEGACY to ignore overflows, this test only ensures
      // that the table schema didn't evolve.
      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.LEGACY.toString) {
        testCase.additionalValuesDF.write.mode("append")
          .insertInto(s"delta.`$tempPath`")
        assert(readDeltaTable(tempPath).schema("value").dataType === testCase.fromType)
      }
    }
  }

  test("INSERT - type widening isn't applied when schema evolution is disabled") {
    sql(s"CREATE TABLE delta.`$tempPath` (a short) USING DELTA")
    withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "false") {
      // Insert integer values. This should succeed and downcast the values to short.
      sql(s"INSERT INTO delta.`$tempPath` VALUES (1), (2)")
      assert(readDeltaTable(tempPath).schema("a").dataType === ShortType)
      checkAnswer(readDeltaTable(tempPath),
        Seq(1, 2).toDF("a").select($"a".cast(ShortType)))
    }

    // Check that we would actually widen if schema evolution was enabled.
    withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
      sql(s"INSERT INTO delta.`$tempPath` VALUES (3), (4)")
      assert(readDeltaTable(tempPath).schema("a").dataType === IntegerType)
      checkAnswer(readDeltaTable(tempPath), Seq(1, 2, 3, 4).toDF("a"))
    }
  }

  test("INSERT - type widening isn't applied when it's disabled") {
    sql(s"CREATE TABLE delta.`$tempPath` (a short) USING DELTA")
    enableTypeWidening(tempPath, enabled = false)
    sql(s"INSERT INTO delta.`$tempPath` VALUES (1), (2)")
    assert(readDeltaTable(tempPath).schema("a").dataType === ShortType)
    checkAnswer(readDeltaTable(tempPath),
      Seq(1, 2).toDF("a").select($"a".cast(ShortType)))
  }

  test("INSERT - type widening is triggered when schema evolution is enabled via option") {
    val tableName = "type_widening_insert_into_table"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (a short) USING DELTA")
      Seq(1, 2).toDF("a")
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .insertInto(tableName)

      val result = spark.read.format("delta").table(tableName)
      assert(result.schema("a").dataType === IntegerType)
      checkAnswer(result, Seq(1, 2).toDF("a"))
    }
  }

  /**
   * Short-hand to create a logical plan to insert into the table. This captures the state of the
   * table at the time the method is called, e.p. the type widening property value that will be used
   * during analysis.
   */
  private def createInsertPlan(df: DataFrame): LogicalPlan = {
    val relation = DataSourceV2Relation.create(
      table = DeltaTableV2(spark, new Path(tempPath)),
      catalog = None,
      identifier = None,
      options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
    )
    AppendData.byPosition(relation, df.queryExecution.logical)
  }

  test(s"INSERT - fail if type widening gets enabled by a concurrent transaction") {
    sql(s"CREATE TABLE delta.`$tempPath` (a short) USING DELTA")
    enableTypeWidening(tempPath, enabled = false)
    val insert = createInsertPlan(Seq(1).toDF("a"))
    // Enabling type widening after analysis doesn't impact the insert operation: the data is
    // already cast to conform to the current schema.
    enableTypeWidening(tempPath, enabled = true)
    DataFrameUtils.ofRows(spark, insert).collect()
    assert(readDeltaTable(tempPath).schema == new StructType().add("a", ShortType))
    checkAnswer(readDeltaTable(tempPath), Row(1))
  }

  test(s"INSERT - fail if type widening gets disabled by a concurrent transaction") {
    sql(s"CREATE TABLE delta.`$tempPath` (a short) USING DELTA")
    val insert = createInsertPlan(Seq(1).toDF("a"))
    // Disabling type widening after analysis results in inserting data with a wider type into the
    // table while type widening is actually disabled during execution. We do actually widen the
    // table schema in that case because `short` and `int` are both stored as INT32 in parquet.
    enableTypeWidening(tempPath, enabled = false)
    DataFrameUtils.ofRows(spark, insert).collect()
    assert(readDeltaTable(tempPath).schema == new StructType().add("a", IntegerType))
    checkAnswer(readDeltaTable(tempPath), Row(1))
  }
}
