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
import org.apache.spark.sql.{AnalysisException, DataFrame, Encoder, QueryTest, Row}
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
    with DeltaTypeWideningNestedFieldsTests
    with DeltaTypeWideningTableFeatureTests

/**
 * Test mixin that enables type widening by default for all tests in the suite.
 */
trait DeltaTypeWideningTestMixin extends SharedSparkSession {
  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaConfigs.ENABLE_TYPE_WIDENING.defaultTablePropertyKey, "true")
      // Ensure we don't silently cast test inputs to null on overflow.
      .set(SQLConf.ANSI_ENABLED.key, "true")
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
   * @param toType           The type to use when changing the type of column 'value'.
   */
  abstract class TypeEvolutionTestCase(
      val fromType: DataType,
      val toType: DataType) {
    /** The initial values to insert with type `fromType` in column 'value' after table creation. */
    def initialValuesDF: DataFrame
    /** Additional values to insert after changing the type of the column 'value' to `toType`. */
    def additionalValuesDF: DataFrame
    /** Expected content of the table after inserting the additional values. */
    def expectedResult: DataFrame
  }

  /**
   * Represents the input of a supported type change test. Handles converting the test values from
   * scala types to a dataframe.
   */
  case class SupportedTypeEvolutionTestCase[
      FromType  <: DataType, ToType <: DataType,
      FromVal: Encoder, ToVal: Encoder
    ](
      override val fromType: FromType,
      override val toType: ToType,
      initialValues: Seq[FromVal],
      additionalValues: Seq[ToVal]
  ) extends TypeEvolutionTestCase(fromType, toType) {
    override def initialValuesDF: DataFrame =
      initialValues.toDF("value").select($"value".cast(fromType))

    override def additionalValuesDF: DataFrame =
      additionalValues.toDF("value").select($"value".cast(toType))

    override def expectedResult: DataFrame =
      initialValuesDF.union(additionalValuesDF).select($"value".cast(toType))
  }

  // Type changes that are supported by all Parquet readers. Byte, Short, Int are all stored as
  // INT32 in parquet so these changes are guaranteed to be supported.
  private val supportedTestCases: Seq[TypeEvolutionTestCase] = Seq(
    SupportedTypeEvolutionTestCase(ByteType, ShortType,
      Seq(1, -1, Byte.MinValue, Byte.MaxValue, null.asInstanceOf[Byte]),
      Seq(4, -4, Short.MinValue, Short.MaxValue, null.asInstanceOf[Short])),
    SupportedTypeEvolutionTestCase(ByteType, IntegerType,
      Seq(1, -1, Byte.MinValue, Byte.MaxValue, null.asInstanceOf[Byte]),
      Seq(4, -4, Int.MinValue, Int.MaxValue, null.asInstanceOf[Int])),
    SupportedTypeEvolutionTestCase(ShortType, IntegerType,
      Seq(1, -1, Short.MinValue, Short.MaxValue, null.asInstanceOf[Short]),
      Seq(4, -4, Int.MinValue, Int.MaxValue, null.asInstanceOf[Int]))
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
        checkAnswer(readDeltaTable(tempPath).select("value").sort("value"),
          testCase.initialValuesDF.select($"value".cast(testCase.toType)).sort("value"))
      }
      writeData(testCase.additionalValuesDF)
      withAllParquetReaders {
        checkAnswer(
          readDeltaTable(tempPath).select("value").sort("value"),
          testCase.expectedResult.sort("value"))
      }
    }
  }

  /**
   * Represents the input of an unsupported type change test. Handles converting the test values
   * from scala types to a dataframe. Additional values to insert are always empty since the type
   * change is expected to fail.
   */
  case class UnsupportedTypeEvolutionTestCase[
    FromType  <: DataType, ToType <: DataType, FromVal : Encoder](
      override val fromType: FromType,
      override val toType: ToType,
      initialValues: Seq[FromVal]) extends TypeEvolutionTestCase(fromType, toType) {
    override def initialValuesDF: DataFrame =
      initialValues.toDF("value").select($"value".cast(fromType))

    override def additionalValuesDF: DataFrame =
      spark.createDataFrame(
        sparkContext.emptyRDD[Row],
        new StructType().add(StructField("value", toType)))

    override def expectedResult: DataFrame =
      initialValuesDF.select($"value".cast(toType))
  }

  // Test type changes that aren't supported.
  private val unsupportedTestCases: Seq[TypeEvolutionTestCase] = Seq(
    UnsupportedTypeEvolutionTestCase(IntegerType, ByteType,
      Seq(1, 2, Int.MinValue)),
    UnsupportedTypeEvolutionTestCase(LongType, IntegerType,
      Seq(4, 5, Long.MaxValue)),
    UnsupportedTypeEvolutionTestCase(DoubleType, FloatType,
      Seq(987654321.987654321d, Double.NaN, Double.NegativeInfinity,
        Double.PositiveInfinity, Double.MinPositiveValue,
        Double.MinValue, Double.MaxValue)),
    UnsupportedTypeEvolutionTestCase(IntegerType, DecimalType(9, 0),
      Seq(1, -1, Int.MinValue)),
    UnsupportedTypeEvolutionTestCase(LongType, DecimalType(19, 0),
      Seq(1, -1, Long.MinValue)),
    UnsupportedTypeEvolutionTestCase(TimestampNTZType, DateType,
      Seq("2020-03-17 15:23:15", "2023-12-31 23:59:59", "0001-01-01 00:00:00")),
    // Reduce scale
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS, 3),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_INT_DIGITS - 2) + ".99"))),
    // Reduce precision
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS - 1, 2),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_INT_DIGITS - 2) + ".99"))),
    // Reduce precision & scale
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_LONG_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS - 1, 1),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_LONG_DIGITS - 2) + ".99"))),
    // Increase scale more than precision
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS + 1, 4),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_INT_DIGITS - 2) + ".99"))),
    // Smaller scale and larger precision.
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_LONG_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS + 3, 1),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_LONG_DIGITS - 2) + ".99")))
  )

  for {
    testCase <- unsupportedTestCases
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

  test("type widening using ALTER TABLE REPLACE COLUMNS") {
    append(Seq(1, 2).toDF("value").select($"value".cast(ShortType)))
    assert(readDeltaTable(tempPath).schema === new StructType().add("value", ShortType))
    sql(s"ALTER TABLE delta.`$tempPath` REPLACE COLUMNS (value INT)")
    assert(readDeltaTable(tempPath).schema === new StructType().add("value", IntegerType))
    checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2)))
    append(Seq(3, 4).toDF("value"))
    checkAnswer(readDeltaTable(tempPath), Seq(Row(1), Row(2), Row(3), Row(4)))
  }

}

/**
 * Tests covering type changes on nested fields in structs, maps and arrays.
 */
trait DeltaTypeWideningNestedFieldsTests {
  self: QueryTest with ParquetTest with DeltaDMLTestUtils with DeltaTypeWideningTestMixin
    with SharedSparkSession =>

  import testImplicits._

  /** Create a table with a struct, map and array for each test. */
  protected def createNestedTable(): Unit = {
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
  }

  test("unsupported ALTER TABLE CHANGE COLUMN on non-leaf fields") {
    createNestedTable()
    // Running ALTER TABLE CHANGE COLUMN on non-leaf fields is invalid.
    var alterTableSql = s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN s TYPE struct<a: short>"
    checkError(
      exception = intercept[AnalysisException] { sql(alterTableSql) },
      errorClass = "CANNOT_UPDATE_FIELD.STRUCT_TYPE",
      parameters = Map(
        "table" -> s"`spark_catalog`.`delta`.`$tempPath`",
        "fieldName" -> "`s`"
      ),
      context = ExpectedContext(
        fragment = alterTableSql,
        start = 0,
        stop = alterTableSql.length - 1)
    )

    alterTableSql = s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN m TYPE map<int, int>"
    checkError(
      exception = intercept[AnalysisException] { sql(alterTableSql) },
      errorClass = "CANNOT_UPDATE_FIELD.MAP_TYPE",
      parameters = Map(
        "table" -> s"`spark_catalog`.`delta`.`$tempPath`",
        "fieldName" -> "`m`"
      ),
      context = ExpectedContext(
        fragment = alterTableSql,
        start = 0,
        stop = alterTableSql.length - 1)
    )

    alterTableSql = s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE array<int>"
    checkError(
      exception = intercept[AnalysisException] { sql(alterTableSql) },
      errorClass = "CANNOT_UPDATE_FIELD.ARRAY_TYPE",
      parameters = Map(
        "table" -> s"`spark_catalog`.`delta`.`$tempPath`",
        "fieldName" -> "`a`"
      ),
      context = ExpectedContext(
        fragment = alterTableSql,
        start = 0,
        stop = alterTableSql.length - 1)
    )
  }

  test("type widening with ALTER TABLE on nested fields") {
    createNestedTable()
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

  test("type widening using ALTER TABLE REPLACE COLUMNS on nested fields") {
    createNestedTable()
    sql(s"ALTER TABLE delta.`$tempPath` REPLACE COLUMNS " +
      "(s struct<a: short>, m map<int, int>, a array<int>)")
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
