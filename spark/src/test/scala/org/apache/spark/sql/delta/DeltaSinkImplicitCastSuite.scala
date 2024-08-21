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

import java.io.File
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.delta.sources.{DeltaSink, DeltaSQLConf}

import org.apache.spark.{SparkArithmeticException, SparkThrowable}
import org.apache.spark.sql.{DataFrame, Encoder, Row}
import org.apache.spark.sql.errors.QueryExecutionErrors.toSQLType
import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamExecution}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException}
import org.apache.spark.sql.types._

/**
 * Covers handling implicit casting to handle type mismatches when writing data to a Delta sink.
 */
abstract class DeltaSinkImplicitCastTest extends DeltaSinkTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.DELTA_STREAMING_SINK_ALLOW_IMPLICIT_CASTS.key, "true")
    spark.conf.set(SQLConf.ANSI_ENABLED.key, "true")
  }

  /**
   * Helper to write to and read from a Delta sink. Creates and runs a streaming query for each call
   * to `write`.
   */
  class TestDeltaStream[T: Encoder](
      outputDir: File,
      checkpointDir: File) {
    private val source = MemoryStream[T]

    def write(data: T*)(selectExpr: String*): Unit =
      write(outputMode = OutputMode.Append, extraOptions = Map.empty)(data: _*)(selectExpr: _*)

    def write(
        outputMode: OutputMode,
        extraOptions: Map[String, String])(
        data: T*)(
        selectExpr: String*): Unit = {
      val query =
        source.toDF()
          .selectExpr(selectExpr: _*)
          .writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(outputMode)
          .options(extraOptions)
          .format("delta")
          .start(outputDir.getCanonicalPath)
      try {
        source.addData(data)
        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }
      } finally {
        query.stop()
      }
    }

    def currentSchema: StructType =
      spark.read.format("delta").load(outputDir.getCanonicalPath).schema

    def read(): DataFrame =
      spark.read.format("delta").load(outputDir.getCanonicalPath)

    def deltaLog: DeltaLog =
      DeltaLog.forTable(spark, outputDir.getCanonicalPath)
  }

  /** Sets up a new [[TestDeltaStream]] to write to and read from a test Delta sink. */
  def withDeltaStream[T: Encoder](f: TestDeltaStream[T] => Unit): Unit =
    withTempDirs { (outputDir, checkpointDir) =>
      f(new TestDeltaStream[T](outputDir, checkpointDir))
    }

  /**
   * Validates that the table history for the test Delta sink matches the given list of operations.
   */
  def checkOperationHistory[T](stream: TestDeltaStream[T], expectedOperations: Seq[String])
  : Unit = {
    val history = sql(s"DESCRIBE HISTORY delta.`${stream.deltaLog.dataPath}`")
      .sort("version")
      .select("operation")
    checkAnswer(history, expectedOperations.map(Row(_)))
  }
}

class DeltaSinkImplicitCastSuite extends DeltaSinkImplicitCastTest {
  import testImplicits._

  test(s"write wider type - long -> int") {
    withDeltaStream[Long] { stream =>
      stream.write(17)("CAST(value AS INT)")
      assert(stream.currentSchema("value").dataType === IntegerType)
      checkAnswer(stream.read(), Row(17))

      stream.write(23)("CAST(value AS LONG)")
      assert(stream.currentSchema("value").dataType === IntegerType)
      checkAnswer(stream.read(), Row(17) :: Row(23) :: Nil)
      checkOperationHistory(stream, expectedOperations = Seq(
        "STREAMING UPDATE", // First write
        "STREAMING UPDATE"  // Second write
      ))
    }
  }

  test("write wider type - long -> int - overflow with " +
    s"storeAssignmentPolicy=${StoreAssignmentPolicy.STRICT}") {
    withDeltaStream[Long] { stream =>
      stream.write(17)("CAST(value AS INT)")
      assert(stream.currentSchema("value").dataType === IntegerType)
      checkAnswer(stream.read(), Row(17))
      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.STRICT.toString) {
        val ex = intercept[StreamingQueryException] {
          stream.write(Long.MaxValue)("CAST(value AS LONG)")
        }
        checkError(
          exception = ex.getCause.asInstanceOf[SparkThrowable],
          errorClass = "CANNOT_UP_CAST_DATATYPE",
          parameters = Map(
            "expression" -> "value",
            "sourceType" -> toSQLType("BIGINT"),
            "targetType" -> toSQLType("INT"),
            "details" -> ("The type path of the target object is:\n\nYou can either add an " +
              "explicit cast to the input data or choose a higher precision type of the field in " +
              "the target object")
          )
        )
      }
    }
  }

  test("write wider type - long -> int - overflow with " +
    s"storeAssignmentPolicy=${StoreAssignmentPolicy.ANSI}") {
    withDeltaStream[Long] { stream =>
      stream.write(17)("CAST(value AS INT)")
      assert(stream.currentSchema("value").dataType === IntegerType)
      checkAnswer(stream.read(), Row(17))
      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.ANSI.toString) {
        val ex = intercept[StreamingQueryException] {
          stream.write(Long.MaxValue)("CAST(value AS LONG)")
        }

        def getSparkArithmeticException(ex: Throwable): SparkArithmeticException = ex match {
          case e: SparkArithmeticException => e
          case e: Throwable if e.getCause != null => getSparkArithmeticException(e.getCause)
          case e => fail(s"Unexpected exception: $e")
        }
        checkError(
          exception = getSparkArithmeticException(ex),
          errorClass = "CAST_OVERFLOW_IN_TABLE_INSERT",
          parameters = Map(
          "sourceType" -> "\"BIGINT\"",
          "targetType" -> "\"INT\"",
          "columnName" -> "`value`")
        )
      }
    }
  }

  test("write wider type - long -> int - overflow with " +
    s"storeAssignmentPolicy=${StoreAssignmentPolicy.LEGACY}") {
    withDeltaStream[Long] { stream =>
      stream.write(17)("CAST(value AS INT)")
      assert(stream.currentSchema("value").dataType === IntegerType)
      checkAnswer(stream.read(), Row(17))
      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.LEGACY.toString) {
        stream.write(Long.MaxValue)("CAST(value AS LONG)")
        // LEGACY allows the value to silently overflow.
        checkAnswer(stream.read(), Row(17) :: Row(-1) :: Nil)
      }
    }
  }

  test("write wider type - Decimal(10, 4) -> Decimal(6, 2)") {
    withDeltaStream[BigDecimal] { stream =>
      stream.write(BigDecimal(123456L, scale = 2))("CAST(value AS DECIMAL(6, 2))")
      assert(stream.currentSchema("value").dataType === DecimalType(6, 2))
      checkAnswer(stream.read(), Row(BigDecimal(123456L, scale = 2)))

      stream.write(BigDecimal(987654L, scale = 4))("CAST(value AS DECIMAL(10, 4))")
      assert(stream.currentSchema("value").dataType === DecimalType(6, 2))
      checkAnswer(stream.read(),
        Row(BigDecimal(123456L, scale = 2)) :: Row(BigDecimal(9877L, scale = 2)) :: Nil
      )
    }
  }

  test("write narrower type - int -> long") {
    withDeltaStream[Long] { stream =>
      stream.write(Long.MinValue)("CAST(value AS LONG)")
      assert(stream.currentSchema("value").dataType === LongType)
      checkAnswer(stream.read(), Row(Long.MinValue))

      stream.write(23)("CAST(value AS INT)")
      assert(stream.currentSchema("value").dataType === LongType)
      checkAnswer(stream.read(), Row(Long.MinValue) :: Row(23) :: Nil)
    }
  }

  test("write different type - date -> string") {
    withDeltaStream[String] { stream =>
      stream.write("abc")("CAST(value AS STRING)")
      assert(stream.currentSchema("value").dataType === StringType)
      checkAnswer(stream.read(), Row("abc"))

      stream.write("2024-07-25")("CAST(value AS DATE)")
      assert(stream.currentSchema("value").dataType === StringType)
      checkAnswer(stream.read(), Row("abc") :: Row("2024-07-25") :: Nil)
    }
  }

  test("implicit cast in nested struct/array/map") {
    withDeltaStream[Int] { stream =>
      stream.write(17)("named_struct('a', value) AS s")
      assert(stream.currentSchema("s").dataType === new StructType().add("a", IntegerType))
      checkAnswer(stream.read(), Row(Row(17)))

      stream.write(-12)("named_struct('a', CAST(value AS LONG)) AS s")
      assert(stream.currentSchema("s").dataType === new StructType().add("a", IntegerType))
      checkAnswer(stream.read(), Row(Row(17)) :: Row(Row(-12)) :: Nil)
    }

    withDeltaStream[(Int, Int)] { stream =>
      stream.write((17, 57))("map(_1, _2) AS m")
      assert(stream.currentSchema("m").dataType === MapType(IntegerType, IntegerType))
      checkAnswer(stream.read(), Row(Map(17 -> 57)))
      stream.write((-12, 3))("map(CAST(_1 AS LONG), CAST(_2 AS STRING)) AS m")
      assert(stream.currentSchema("m").dataType === MapType(IntegerType, IntegerType))
      checkAnswer(stream.read(), Row(Map(17 -> 57)) :: Row(Map(-12 -> 3)) :: Nil)
    }

    withDeltaStream[(Int, Int)] { stream =>
      stream.write((17, 57))("array(_1, _2) AS a")
      assert(stream.currentSchema("a").dataType === ArrayType(IntegerType))
      checkAnswer(stream.read(), Row(Seq(17, 57)) :: Nil)
      stream.write((-12, 3))("array(_1, _2) AS a")
      assert(stream.currentSchema("a").dataType === ArrayType(IntegerType))
      checkAnswer(stream.read(), Row(Seq(17, 57)) :: Row(Seq(-12, 3)) :: Nil)
    }
  }

  test("write invalid nested type - array -> struct") {
    withDeltaStream[Int] { stream =>
      stream.write(17)("named_struct('a', value) AS s")
      assert(stream.currentSchema("s").dataType === new StructType().add("a", IntegerType))
      checkAnswer(stream.read(), Row(Row(17)))

      val ex = intercept[StreamingQueryException] {
        stream.write(-12)("array(value) AS s")
      }
      checkError(
        exception = ex.getCause.asInstanceOf[SparkThrowable],
        errorClass = "DELTA_FAILED_TO_MERGE_FIELDS",
        parameters = Map(
        "currentField" -> "s",
        "updateField" -> "s")
      )
    }
  }

  test("implicit cast on partition value") {
    withDeltaStream[(String, Int)] { stream =>
      sql(
        s"""
          |CREATE TABLE delta.`${stream.deltaLog.dataPath}` (day date, value int)
          |USING DELTA
          |PARTITIONED BY (day)
        """.stripMargin)

      stream.write(("2024-07-26", 1))("CAST(_1 AS DATE) AS day", "_2 AS value")
      assert(stream.currentSchema === new StructType()
        .add("day", DateType)
        .add("value", IntegerType))
      checkAnswer(stream.read(), Row(Date.valueOf("2024-07-26"), 1))

      stream.write(("2024-07-27", 2))(
        "CAST(_1 AS TIMESTAMP) AS day", "CAST(_2 AS DECIMAL(4, 1)) AS value")
      assert(stream.currentSchema === new StructType()
        .add("day", DateType)
        .add("value", IntegerType))
      checkAnswer(stream.read(),
        Row(Date.valueOf("2024-07-26"), 1) :: Row(Date.valueOf("2024-07-27"), 2) :: Nil)
    }
  }

  test("implicit cast with schema evolution") {
    withDeltaStream[(Long, String)] { stream =>
      stream.write((123, "unused"))("CAST(_1 AS DECIMAL(6, 3)) AS a")
      assert(stream.currentSchema === new StructType()
        .add("a", DecimalType(6, 3)))
      checkAnswer(stream.read(), Row(BigDecimal(123000, scale = 3)))

      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
        stream.write((678, "abc"))("CAST(_1 AS LONG) AS a", "_2 AS b")
        assert(stream.currentSchema === new StructType()
          .add("a", DecimalType(6, 3))
          .add("b", StringType))
        checkAnswer(stream.read(),
          Row(BigDecimal(123000, scale = 3), null) ::
          Row(BigDecimal(678000, scale = 3), "abc") :: Nil)
      }
    }
  }

  test("implicit cast with schema overwrite") {
    withTempDirs { (outputDir, checkpointDir) =>
      val source = MemoryStream[Long]

      def write(streamingDF: DataFrame, data: Long*): Unit = {
        val query = streamingDF.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Complete)
          .option(DeltaOptions.OVERWRITE_SCHEMA_OPTION, "true")
          .format("delta")
          .start(outputDir.getCanonicalPath)
        try {
          source.addData(data: _*)
          failAfter(streamingTimeout) {
            query.processAllAvailable()
          }
        } finally {
          query.stop()
        }
      }

      // Initial write to the sink with columns a, count, b, c.
      val initialDF = source.toDF()
        .selectExpr("CAST(value AS DECIMAL(6, 3)) AS a")
        .groupBy("a")
        .count()
        .withColumn("b", col("count").cast("INT"))
        .withColumn("c", lit(11).cast("STRING"))
      write(initialDF, 10)
      val initialResult = spark.read.format("delta").load(outputDir.getCanonicalPath)
      assert(initialResult.schema === new StructType()
        .add("a", DecimalType(6, 3))
        .add("count", LongType)
        .add("b", IntegerType)
        .add("c", StringType))
      checkAnswer(initialResult, Row(BigDecimal(10000, scale = 3), 1, 1, "11"))

      // Second write with overwrite schema: change type of column b and replace c with d.
      val overwriteDF = source.toDF()
        .selectExpr("CAST(value AS DECIMAL(6, 3)) AS a")
        .groupBy("a")
        .count()
        .withColumn("b", col("count").cast("LONG"))
        .withColumn("d", lit(21).cast("STRING"))
      write(overwriteDF, 20)
      val overwriteResult = spark.read.format("delta").load(outputDir.getCanonicalPath)
      assert(overwriteResult.schema === new StructType()
        .add("a", DecimalType(6, 3))
        .add("count", LongType)
        .add("b", LongType)
        .add("d", StringType))
      checkAnswer(overwriteResult,
        Row(BigDecimal(10000, scale = 3), 1, 1, "21") ::
        Row(BigDecimal(20000, scale = 3), 1, 1, "21") :: Nil
      )
    }
  }

  // Writing to a delta sink is always case insensitive and ignores the value of
  // 'spark.sql.caseSensitive'.
  for (caseSensitive <- Seq(true, false))
  test(s"implicit cast with case sensitivity, caseSensitive=$caseSensitive") {
    withDeltaStream[Long] { stream =>
      stream.write(17)("CAST(value AS LONG) AS value")
      assert(stream.currentSchema === new StructType().add("value", LongType))
      checkAnswer(stream.read(), Row(17))

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        stream.write(23)("CAST(value AS INT) AS VALUE")
        assert(stream.currentSchema === new StructType().add("value", LongType))
        checkAnswer(stream.read(), Row(17) :: Row(23) :: Nil)
      }
    }
  }

  test("implicit cast and missing column") {
    withDeltaStream[(String, String)] { stream =>
      stream.write(("2024-07-28 12:00:00", "abc"))("CAST(_1 AS TIMESTAMP) AS a", "_2 AS b")
      assert(stream.currentSchema === new StructType()
        .add("a", TimestampType)
        .add("b", StringType))
      checkAnswer(stream.read(), Row(Timestamp.valueOf("2024-07-28 12:00:00"), "abc"))

      stream.write(("2024-07-29", "unused"))("CAST(_1 AS DATE) AS a")
      assert(stream.currentSchema === new StructType()
        .add("a", TimestampType)
        .add("b", StringType))
      checkAnswer(stream.read(),
        Row(Timestamp.valueOf("2024-07-28 12:00:00"), "abc") ::
        Row(Timestamp.valueOf("2024-07-29 00:00:00"), null) :: Nil)
      checkOperationHistory(stream, expectedOperations = Seq(
        "STREAMING UPDATE", // First write
        "STREAMING UPDATE"  // Second write
      ))
    }
  }

  test("implicit cast after renaming/dropping columns with column mapping") {
    withDeltaStream[(Int, Int)] { stream =>
      stream.write((1, 100))("_1 AS a", "CAST(_2 AS LONG) AS b")
      assert(stream.currentSchema === new StructType()
        .add("a", IntegerType)
        .add("b", LongType))
      checkAnswer(stream.read(), Row(1, 100))
      sql(
        s"""
           |ALTER TABLE delta.`${stream.deltaLog.dataPath}` SET TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.minReaderVersion' = '2',
           |  'delta.minWriterVersion' = '5'
           |)
         """.stripMargin)

      sql(s"ALTER TABLE delta.`${stream.deltaLog.dataPath}` DROP COLUMN a")
      sql(s"ALTER TABLE delta.`${stream.deltaLog.dataPath}` RENAME COLUMN b to a")
      assert(stream.currentSchema === new StructType()
        .add("a", LongType))

      stream.write((17, -1))("CAST(_1 AS STRING) AS a")
      assert(stream.currentSchema === new StructType()
        .add("a", LongType))
      checkAnswer(stream.read(), Row(100) :: Row(17) :: Nil)

      checkOperationHistory(stream, expectedOperations = Seq(
        "STREAMING UPDATE",  // First write
        "SET TBLPROPERTIES", // Enable column mapping
        "DROP COLUMNS",      // Drop column
        "RENAME COLUMN",     // Rename Column
        "STREAMING UPDATE"   // Second write
      ))
    }
  }

  test("disallow implicit cast with spark.databricks.delta.streaming.sink.allowImplicitCasts") {
    withSQLConf(DeltaSQLConf.DELTA_STREAMING_SINK_ALLOW_IMPLICIT_CASTS.key -> "false") {
      withDeltaStream[Long] { stream =>
        stream.write(17)("CAST(value AS INT)")
        assert(stream.currentSchema("value").dataType === IntegerType)
        checkAnswer(stream.read(), Row(17))

        val ex = intercept[StreamingQueryException] {
          stream.write(23)("CAST(value AS LONG)")
        }
        checkError(
          exception = ex.getCause.asInstanceOf[SparkThrowable],
          errorClass = "DELTA_FAILED_TO_MERGE_FIELDS",
          parameters = Map(
          "currentField" -> "value",
          "updateField" -> "value")
        )
      }
    }
  }

  for (allowImplicitCasts <- Seq(true, false))
  test(s"schema evolution with case sensitivity and without type mismatch, " +
    s"allowImplicitCasts=$allowImplicitCasts") {
    withSQLConf(
      DeltaSQLConf.DELTA_STREAMING_SINK_ALLOW_IMPLICIT_CASTS.key -> allowImplicitCasts.toString,
      SQLConf.CASE_SENSITIVE.key -> "true",
      DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true"
    ) {
      withDeltaStream[(Long, Long)] { stream =>
        stream.write((17, -1))("CAST(_1 AS INT) AS a")
        assert(stream.currentSchema == new StructType().add("a", IntegerType))
        checkAnswer(stream.read(), Row(17))

        stream.write((21, 22))("CAST(_1 AS INT) AS A", "_2 AS b")
        assert(stream.currentSchema == new StructType()
          .add("a", IntegerType)
          .add("b", LongType))
        checkAnswer(stream.read(), Row(17, null) :: Row(21, 22) :: Nil)
      }
    }
  }

  test("handling type mismatch in addBatch") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getAbsolutePath
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      sqlContext.sparkContext.setLocalProperty(StreamExecution.QUERY_ID_KEY, "streaming_query")
      val sink = DeltaSink(
        sqlContext,
        path = deltaLog.dataPath,
        partitionColumns = Seq.empty,
        outputMode = OutputMode.Append(),
        options = new DeltaOptions(options = Map.empty, conf = spark.sessionState.conf)
      )

      val schema = new StructType().add("value", IntegerType)

      {
        val data = Seq(0, 1).toDF("value").selectExpr("CAST(value AS INT)")
        sink.addBatch(0, data)
        val df = spark.read.format("delta").load(tablePath)
        assert(df.schema === schema)
        checkAnswer(df, Row(0) :: Row(1) :: Nil)
      }
      {
        val data = Seq(2, 3).toDF("value").selectExpr("CAST(value AS LONG)")
        sink.addBatch(1, data)
        val df = spark.read.format("delta").load(tablePath)
        assert(df.schema === schema)
        checkAnswer(df, Row(0) :: Row(1) :: Row(2) :: Row(3) :: Nil)
      }
    }
  }
}
