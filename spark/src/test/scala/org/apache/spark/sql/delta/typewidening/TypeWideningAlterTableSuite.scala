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
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._

/**
 * Suite providing core coverage for type widening using ALTER TABLE CHANGE COLUMN TYPE.
 */
class TypeWideningAlterTableSuite
  extends QueryTest
    with ParquetTest
    with TypeWideningTestMixin
    with TypeWideningAlterTableTests

trait TypeWideningAlterTableTests
  extends DeltaExcludedBySparkVersionTestMixinShims
    with QueryErrorsBase
    with TypeWideningTestCases {
  self: QueryTest with ParquetTest with TypeWideningTestMixin =>

  import testImplicits._

  for {
    testCase <- supportedTestCases
    partitioned <- BOOLEAN_DOMAIN
  } {
    test(s"type widening ${testCase.fromType.sql} -> ${testCase.toType.sql}, " +
      s"partitioned=$partitioned") {
      def writeData(df: DataFrame): Unit = if (partitioned) {
        // The table needs to have at least 1 non-partition column, use a dummy one.
        append(df.withColumn("dummy", lit(1)), partitionBy = Seq("value"))
      } else {
        append(df)
      }

      writeData(testCase.initialValuesDF)
      sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN value TYPE ${testCase.toType.sql}")
      withAllParquetReaders {
        assert(readDeltaTable(tempPath).schema("value").dataType === testCase.toType)
        checkAnswerWithTolerance(
          actualDf = readDeltaTable(tempPath).select("value"),
          expectedDf = testCase.initialValuesDF.select($"value".cast(testCase.toType)),
          toType = testCase.toType
        )
      }
      writeData(testCase.additionalValuesDF)
      withAllParquetReaders {
        checkAnswerWithTolerance(
          actualDf = readDeltaTable(tempPath).select("value"),
          expectedDf = testCase.expectedResult.select($"value".cast(testCase.toType)),
          toType = testCase.toType
        )
      }
    }
  }

  for {
    testCase <- unsupportedTestCases ++ previewOnlySupportedTestCases
    partitioned <- BOOLEAN_DOMAIN
  } {
    test(s"unsupported type changes ${testCase.fromType.sql} -> ${testCase.toType.sql}, " +
      s"partitioned=$partitioned") {
      if (partitioned) {
        // The table needs to have at least 1 non-partition column, use a dummy one.
        append(testCase.initialValuesDF.withColumn("dummy", lit(1)), partitionBy = Seq("value"))
      } else {
        append(testCase.initialValuesDF)
      }

      val alterTableSql =
        s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN value TYPE ${testCase.toType.sql}"

      // Type changes that aren't upcast are rejected early during analysis by Spark, while upcasts
      // are rejected in Delta when the ALTER TABLE command is executed.
      if (Cast.canUpCast(testCase.fromType, testCase.toType)) {
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql(alterTableSql)
          },
          errorClass = "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
          sqlState = None,
          parameters = Map(
            "fieldPath" -> "value",
            "oldField" -> testCase.fromType.sql,
            "newField" -> testCase.toType.sql)
        )
      } else {
        checkError(
          exception = intercept[AnalysisException] {
            sql(alterTableSql)
          },
          errorClass = "NOT_SUPPORTED_CHANGE_COLUMN",
          sqlState = None,
          parameters = Map(
            "table" -> s"`spark_catalog`.`delta`.`$tempPath`",
            "originName" -> toSQLId("value"),
            "originType" -> toSQLType(testCase.fromType),
            "newName" -> toSQLId("value"),
            "newType" -> toSQLType(testCase.toType)),
          context = ExpectedContext(
            fragment = alterTableSql,
            start = 0,
            stop = alterTableSql.length - 1)
        )
      }
    }
  }

  test("type widening using ALTER TABLE REPLACE COLUMNS") {
    append(Seq(1, 2).toDF("value").select($"value".cast(ShortType)))
    assert(readDeltaTable(tempPath).schema === new StructType().add("value", ShortType))
    sql(s"ALTER TABLE delta.`$tempPath` REPLACE COLUMNS (value INT)")
    assert(readDeltaTable(tempPath).schema ===
      new StructType()
        .add("value", IntegerType, nullable = true, metadata = new MetadataBuilder()
          .putMetadataArray("delta.typeChanges", Array(
            new MetadataBuilder()
              .putString("toType", "integer")
              .putString("fromType", "short")
              .putLong("tableVersion", 1)
              .build()
          )).build()))
    checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2)))
    append(Seq(3, 4).toDF("value"))
    checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2), Row(3), Row(4)))
  }

  def withTimestampNTZDisabled(f: => Unit): Unit = {
    val timestampNTZKey = TableFeatureProtocolUtils.defaultPropertyKey(TimestampNTZTableFeature)
    conf.unsetConf(timestampNTZKey)
    if (!conf.contains(timestampNTZKey)) return f

    val timestampNTZSupported = conf.getConfString(timestampNTZKey)
    conf.unsetConf(timestampNTZKey)
    try {
      f
    } finally {
      conf.setConfString(timestampNTZKey, timestampNTZSupported)
    }
  }

  testSparkMasterOnly(
    "widening Date -> TimestampNTZ rejected when TimestampNTZ feature isn't supported") {
    withTimestampNTZDisabled {
      sql(s"CREATE TABLE delta.`$tempPath` (a date) USING DELTA")
      val currentProtocol = deltaLog.unsafeVolatileSnapshot.protocol
      val currentFeatures = currentProtocol.implicitlyAndExplicitlySupportedFeatures
        .map(_.name)
        .toSeq
        .sorted
        .mkString(", ")

      checkError(
        exception = intercept[DeltaTableFeatureException] {
          sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE TIMESTAMP_NTZ")
        },
        errorClass = "DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT",
        parameters = Map(
          "unsupportedFeatures" -> "timestampNtz",
          "supportedFeatures" -> currentFeatures
        )
      )
    }
  }

  test("type widening type change metrics") {
    sql(s"CREATE TABLE delta.`$tempDir` (a byte) USING DELTA")
    val usageLogs = Log4jUsageLogger.track {
      sql(s"ALTER TABLE delta.`$tempDir` CHANGE COLUMN a TYPE int")
    }

    val metrics = filterUsageRecords(usageLogs, "delta.typeWidening.typeChanges")
      .map(r => JsonUtils.fromJson[Map[String, Seq[Map[String, String]]]](r.blob))
      .head

    assert(metrics("changes") === Seq(
      Map(
        "fromType" -> "TINYINT",
        "toType" -> "INT"
      ))
    )
  }
}
