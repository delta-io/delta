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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Suite covering the type widening table feature.
 */
class DeltaTypeWideningSuite
  extends QueryTest
    with ParquetTest
    with DeltaDMLTestUtils
    with DeltaSQLCommandTest
    with DeltaTypeWideningTestMixin
    with DeltaTypeWideningAlterTableTests
    with DeltaTypeWideningTableFeatureTests

/**
 * Test mixin that enables type widening by default for all tests in the suite.
 */
trait DeltaTypeWideningTestMixin extends SharedSparkSession {
  protected override def sparkConf: SparkConf = {
    super.sparkConf.set(DeltaConfigs.ENABLE_TYPE_WIDENING.defaultTablePropertyKey, "true")
  }

  /** Enable (or disable) type widening for the table under the given path. */
  protected def enableTypeWidening(tablePath: String, enabled: Boolean = true): Unit =
    sql(s"ALTER TABLE delta.`$tablePath` " +
          s"SET TBLPROPERTIES('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = '${enabled.toString}')")
}

/**
 * Trait collecting a subset of tests providing core coverage for type widening using ALTER TABLE
 * CHANGE COLUMN TYPE.
 */
trait DeltaTypeWideningAlterTableTests extends QueryErrorsBase {
  self: QueryTest with ParquetTest with DeltaTypeWideningTestMixin with DeltaDMLTestUtils =>

  import testImplicits._

  /**
   * Represents the input of a type change test.
   * @param fromType         The original type of the column 'value' in the test table.
   * @param toType           The type to use when changing the type of column 'value'
   * @param initialValues    The initial values to insert in column 'value after table creation,
   *                         using type `fromType`
   * @param additionalValues Additional values to insert after changing the type of the column
   *                         'value' to `toType`.
   */
  case class TypeEvolutionTestCase(
      fromType: DataType,
      toType: DataType,
      initialValues: Seq[String],
      additionalValues: Seq[String] = Seq.empty) {
    def initialValuesDF: DataFrame =
      initialValues.toDF("value").select($"value".cast(fromType))

    def additionalValuesDF: DataFrame =
      additionalValues.toDF("value").select($"value".cast(toType))

    def expectedResult: DataFrame =
      initialValuesDF.union(additionalValuesDF).select($"value".cast(toType))
  }

  // Type changes that are supported by all Parquet readers. Byte, Short, Int are all stored as
  // INT32 in parquet so these changes are guaranteed to be supported.
  private val supportedTestCases: Seq[TypeEvolutionTestCase] = Seq(
    TypeEvolutionTestCase(ByteType, ShortType,
      Seq("1", "2", Byte.MinValue.toString),
      Seq("4", "5", Int.MaxValue.toString)),
    TypeEvolutionTestCase(ByteType, IntegerType,
      Seq("1", "2", Byte.MinValue.toString),
      Seq("4", "5", Int.MaxValue.toString)),
    TypeEvolutionTestCase(ShortType, IntegerType,
      Seq("1", "2", Byte.MinValue.toString),
      Seq("4", "5", Int.MaxValue.toString))
  )

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
        checkAnswer(readDeltaTable(tempPath).select("value"),
          testCase.initialValuesDF.select($"value".cast(testCase.toType)))
      }
      writeData(testCase.additionalValuesDF)
      withAllParquetReaders {
        checkAnswer(readDeltaTable(tempPath).select("value"), testCase.expectedResult)
      }
    }
  }

  // Test type changes that aren't supported.
  private val unsupportedNonTestCases: Seq[TypeEvolutionTestCase] = Seq(
    TypeEvolutionTestCase(IntegerType, ByteType,
      Seq("1", "2", Int.MinValue.toString)),
    TypeEvolutionTestCase(LongType, IntegerType,
      Seq("4", "5", Long.MaxValue.toString)),
    TypeEvolutionTestCase(DoubleType, FloatType,
      Seq("987654321.987654321", Double.NaN.toString, Double.NegativeInfinity.toString,
        Double.PositiveInfinity.toString, Double.MinPositiveValue.toString,
        Double.MinValue.toString, Double.MaxValue.toString)),
    TypeEvolutionTestCase(IntegerType, DecimalType(6, 0),
      Seq("1", "2", Int.MinValue.toString)),
    TypeEvolutionTestCase(TimestampNTZType, DateType,
      Seq("2020-03-17 15:23:15", "2023-12-31 23:59:59", "0001-01-01 00:00:00")),
    // Reduce scale
    TypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS, 3),
      Seq("-67.89", "9" * (Decimal.MAX_INT_DIGITS - 2) + ".99")),
    // Reduce precision
    TypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS - 1, 2),
      Seq("-67.89", "9" * (Decimal.MAX_LONG_DIGITS - 2) + ".99")),
    // Reduce precision & scale
    TypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS - 1, 1),
      Seq("-67.89", "9" * (Decimal.MAX_LONG_DIGITS - 2) + ".99")),
    // Increase scale more than precision
    TypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS + 1, 4),
      Seq("-67.89", "9" * (Decimal.MAX_LONG_DIGITS - 2) + ".99"))
  )

  for {
    testCase <- unsupportedNonTestCases
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
      sql(s"ALTER TABLE delta.`$tempPath` " +
        s"SET TBLPROPERTIES('delta.feature.timestampNtz' = 'supported')")

      val alterTableSql =
        s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN value TYPE ${testCase.toType.sql}"
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

  test("type widening in nested fields") {
    sql(s"CREATE TABLE delta.`$tempPath` " +
      "(s struct<a: byte>, m map<byte, short>, a array<short>) USING DELTA")
    append(Seq((1, 2, 3, 4))
      .toDF("a", "b", "c", "d")
      .selectExpr(
        "named_struct('a', cast(a as byte)) as s",
        "map(cast(b as byte), cast(c as short)) as m",
        "array(cast(d as short)) as a"))

    assert(readDeltaTable(tempPath).schema === new StructType()
      .add("s", new StructType().add("a", ByteType))
      .add("m", MapType(ByteType, ShortType))
      .add("a", ArrayType(ShortType)))

    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN s.a TYPE short")
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN m.key TYPE int")
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN m.value TYPE int")
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a.element TYPE int")

    assert(readDeltaTable(tempPath).schema === new StructType()
      .add("s", new StructType().add("a", ShortType))
      .add("m", MapType(IntegerType, IntegerType))
      .add("a", ArrayType(IntegerType)))

    append(Seq((5, 6, 7, 8))
      .toDF("a", "b", "c", "d")
        .selectExpr("named_struct('a', cast(a as short)) as s", "map(b, c) as m", "array(d) as a"))

    checkAnswer(
      readDeltaTable(tempPath),
      Seq((1, 2, 3, 4), (5, 6, 7, 8))
        .toDF("a", "b", "c", "d")
        .selectExpr("named_struct('a', cast(a as short)) as s", "map(b, c) as m", "array(d) as a"))
  }

  test("type widening using ALTER TABLE REPLACE COLUMNS") {
    append(Seq(1, 2).toDF("value").select($"value".cast(ShortType)))
    assert(readDeltaTable(tempPath).schema === new StructType().add("value", ShortType))
    sql(s"ALTER TABLE delta.`$tempPath` REPLACE COLUMNS (value INT)")
    assert(readDeltaTable(tempPath).schema === new StructType().add("value", IntegerType))
    checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2)))
    append(Seq(3, 4).toDF("value"))
    checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2), Row(3), Row(4)))
  }

  test("row group skipping Short -> Int") {
    withSQLConf(
      SQLConf.FILES_MAX_PARTITION_BYTES.key -> 1024.toString) {
      append((0 until 1024).toDF("value").select($"value".cast(ShortType)))
      sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN value TYPE INT")
      append((Short.MinValue + 1 until Short.MaxValue + 2048).toDF("value"))
      withAllParquetReaders {
        checkAnswer(
          sql(s"SELECT * FROM delta.`$tempPath` " +
            s"WHERE value >= CAST(${Short.MaxValue} AS INT) + 1000"),
          (Short.MaxValue + 1000 until Short.MaxValue + 2048).map(Row(_)))
      }
    }
  }

}

trait DeltaTypeWideningTableFeatureTests {
    self: QueryTest with ParquetTest with DeltaDMLTestUtils with DeltaTypeWideningTestMixin
      with SharedSparkSession =>

  def isTypeWideningSupported: Boolean = {
    val snapshot = DeltaLog.forTable(spark, tempPath).unsafeVolatileSnapshot
    TypeWidening.isSupported(snapshot.protocol)
  }

  def isTypeWideningEnabled: Boolean = {
    val snapshot = DeltaLog.forTable(spark, tempPath).unsafeVolatileSnapshot
    TypeWidening.isEnabled(snapshot.protocol, snapshot.metadata)
  }

  test("enable type widening at table creation then disable it") {
    sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'true')")
    assert(isTypeWideningSupported)
    assert(isTypeWideningEnabled)
    enableTypeWidening(tempPath, enabled = false)
    assert(isTypeWideningSupported)
    assert(!isTypeWideningEnabled)
  }

  test("enable type widening after table creation then disable it") {
    sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")
    assert(!isTypeWideningSupported)
    assert(!isTypeWideningEnabled)
    // Setting the property to false shouldn't add the table feature if it's not present.
    enableTypeWidening(tempPath, enabled = false)
    assert(!isTypeWideningSupported)
    assert(!isTypeWideningEnabled)

    enableTypeWidening(tempPath)
    assert(isTypeWideningSupported)
    assert(isTypeWideningEnabled)
    enableTypeWidening(tempPath, enabled = false)
    assert(isTypeWideningSupported)
    assert(!isTypeWideningEnabled)
  }

  test("set table property to incorrect value") {
    val ex = intercept[IllegalArgumentException] {
      sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
        s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'bla')")
    }
    assert(ex.getMessage.contains("For input string: \"bla\""))
    sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
       s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")
    checkError(
      exception = intercept[SparkException] {
        sql(s"ALTER TABLE delta.`$tempPath` " +
          s"SET TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'bla')")
      },
      errorClass = "_LEGACY_ERROR_TEMP_2045",
      parameters = Map(
        "message" -> "For input string: \"bla\""
      )
    )
    assert(!isTypeWideningSupported)
    assert(!isTypeWideningEnabled)
  }

  test("change column type without table feature") {
    sql(s"CREATE TABLE delta.`$tempPath` (a TINYINT) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE SMALLINT")
      },
      errorClass = "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
      parameters = Map(
        "fieldPath" -> "a",
        "oldField" -> "TINYINT",
        "newField" -> "SMALLINT"
      )
    )
  }

  test("change column type with type widening table feature supported but table property set to " +
    "false") {
    sql(s"CREATE TABLE delta.`$tempPath` (a SMALLINT) USING DELTA")
    sql(s"ALTER TABLE delta.`$tempPath` " +
      s"SET TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")

    checkError(
      exception = intercept[AnalysisException] {
        sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
      },
      errorClass = "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
      parameters = Map(
        "fieldPath" -> "a",
        "oldField" -> "SMALLINT",
        "newField" -> "INT"
      )
    )
  }

  test("no-op type changes are always allowed") {
    sql(s"CREATE TABLE delta.`$tempPath` (a int) USING DELTA " +
      s"TBLPROPERTIES ('${DeltaConfigs.ENABLE_TYPE_WIDENING.key}' = 'false')")
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
    enableTypeWidening(tempPath, enabled = true)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
    enableTypeWidening(tempPath, enabled = false)
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE INT")
  }
}
