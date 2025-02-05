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

import java.io.File

import org.apache.spark.sql.delta._

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.SparkArithmeticException
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

/**
 * Tests covering streaming reads from a Delta table that had a column type widened.
 */
class TypeWideningStreamingSourceSuite extends TypeWideningStreamingSourceTests

trait TypeWideningStreamingSourceTests
  extends StreamTest
  with SQLTestUtils
  with TypeWideningTestMixin {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.udf.register("scala_udf", (x: Int) => x + 1)
  }

  override def afterAll(): Unit = {
    // The scala UDF is a temporary function, no need to drop it.
    super.afterAll()
  }

  /** Short-hand to read a data stream from the Delta table at the given location. */
  private def readStream(
      path: File,
      checkpointDir: File,
      options: Map[String, String] = Map.empty): DataFrame =
    spark.readStream.format("delta")
      // Type widening requires tracking schema changes.
      .option(DeltaOptions.SCHEMA_TRACKING_LOCATION, checkpointDir.toString)
      .options(options)
      .load(path.getCanonicalPath)

  /** Test action checking that the stream fails due to a metadata change - typ. a schema change. */
  object ExpectMetadataEvolutionException {
    def apply(): StreamAction =
      ExpectFailure[DeltaRuntimeException] { ex =>
        assert(ex.asInstanceOf[DeltaRuntimeException].getErrorClass ===
          "DELTA_STREAMING_METADATA_EVOLUTION")
      }
  }

  /** Test action checking that the stream fails due to a type change being blocked. */
  object ExpectTypeChangeBlockedException {
    def apply(): StreamAction =
      ExpectFailure[DeltaRuntimeException] { ex =>
        assert(ex.asInstanceOf[DeltaRuntimeException].getErrorClass ===
          "DELTA_STREAMING_CANNOT_CONTINUE_PROCESSING_TYPE_WIDENING")
      }
  }

  /** Test action checking that the stream fails due to an unsupported type change. */
  object ExpectIncompatibleSchemaChangeException {
    def apply(): StreamAction =
      ExpectFailure[DeltaIllegalStateException] { ex =>
        assert(ex.asInstanceOf[DeltaIllegalStateException].getErrorClass ===
          "DELTA_SCHEMA_CHANGED_WITH_VERSION")
      }
  }

  /**
   * Test a streaming query with a type widening operation. Creates a Delta source with two columns
   * `widened` and `other` of type `byte` and widens the `widened` column to `int`. The query under
   * test is used to read from the table and checked against the expected result.
   * @param name           Test name.
   * @param query          Streaming query to apply on the source.
   * @param expectedResult In case of success, checks the last batch of data written by the stream
   *                       matches the expected result. In case of failure, the caller provides a
   *                       check to perform on the exception.
   * @param outputMode     Output mode of the streaming query. `Append` by default but can be
   *                       overriden to e.g. `Complete` for aggregations.
   */
  private def testStreamTypeWidening(
      name: String,
      query: DataFrame => DataFrame,
      partitionBy: Option[String] = None,
      expectedResult: ExpectedResult[Seq[Row]],
      outputMode: OutputMode = OutputMode.Append()): Unit = {
    test(s"type change - $name") {
      withTempDir { dir =>
        val partitionByStr = partitionBy.map(p => s"PARTITIONED BY ($p)").getOrElse("")
        sql(s"CREATE TABLE delta.`$dir` (widened byte, other byte) USING DELTA $partitionByStr")
        val checkpointDir = new File(dir, "sink_checkpoint")

        testStream(query(readStream(dir, checkpointDir)), outputMode)(
          StartStream(checkpointLocation = checkpointDir.toString),
          Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (1, 1)") },
          Execute { _ => sql(s"ALTER TABLE delta.`$dir`ALTER COLUMN widened TYPE int") },
          ExpectMetadataEvolutionException()
        )

        val streamActions = expectedResult match {
          case ExpectedResult.Success(rows: Seq[Row @unchecked]) =>
            Seq(
              Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (123456789, 2)") },
              ProcessAllAvailable(),
              CheckLastBatch(rows: _*)
            )
          case ExpectedResult.Failure(checkError) =>
            Seq(AssertOnQuery { q =>
              val ex = intercept[StreamingQueryException] {
                q.processAllAvailable()
              }
              val cause = if (ex.getCause.getMessage.contains(
                "Provided schema doesn't match to the schema for existing state!")) {
                // State store schema mismatches were non-spark exception until Spark 3.5. We wrap
                // them into a spark exception to be able to check them consistently across spark
                // versions.
                new SparkException(
                  message = ex.getCause.getMessage,
                  cause = ex,
                  errorClass = Some("STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE"),
                  messageParameters = Map.empty
                )
              } else {
                assert(ex.getCause.isInstanceOf[SparkThrowable])
                ex.getCause.asInstanceOf[SparkThrowable]
              }
              checkError(cause)
              true
            })
        }

        // We need to unblock the type change to let the stream make progress.
        withSQLConf("spark.databricks.delta.streaming.allowSourceColumnTypeChange" -> "always") {
          testStream(query(readStream(dir, checkpointDir)), outputMode)(
            StartStream(checkpointLocation = checkpointDir.toString) +:
              streamActions: _*
          )
        }
      }
    }
  }

  testStreamTypeWidening("filter",
    query = _.where(col("widened") > 10),
    expectedResult = ExpectedResult.Success(Seq(Row(123456789, 2)))
  )

  testStreamTypeWidening("projection",
    query = _.withColumn("add", col("widened") + col("other")),
    expectedResult = ExpectedResult.Success(Seq(Row(123456789, 2, 123456791)))
  )

  testStreamTypeWidening("projection partition column",
    query = _.withColumn("add", col("widened") + col("other")),
    partitionBy = Some("widened"),
    expectedResult = ExpectedResult.Success(Seq(Row(123456789, 2, 123456791)))
  )

  testStreamTypeWidening("widen unused scala udf field",
    query = _.selectExpr("scala_udf(other)"),
    expectedResult = ExpectedResult.Success(Seq(Row(3)))
  )

  testStreamTypeWidening("widen scala udf argument",
    query = _.selectExpr("scala_udf(widened)"),
    expectedResult = ExpectedResult.Success(Seq(Row(123456790)))
  )

  testStreamTypeWidening("widen aggregation grouping key",
    query = _.groupBy("widened").agg(count(col("other"))),
    expectedResult = ExpectedResult.Failure { ex =>
      assert(ex.getErrorClass === "STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE")
    },
    outputMode = OutputMode.Complete()
  )

  testStreamTypeWidening("widen aggregation expression",
    query = _.groupBy("other").agg(count(col("widened"))),
    expectedResult = ExpectedResult.Success(Seq(Row(1, 1), Row(2, 1))),
    outputMode = OutputMode.Complete()
  )

  testStreamTypeWidening("widen aggregation expression partition column",
    query = _.groupBy("other").agg(count(col("widened"))),
    partitionBy = Some("widened"),
    expectedResult = ExpectedResult.Success(Seq(Row(1, 1), Row(2, 1))),
    outputMode = OutputMode.Complete()
  )

  testStreamTypeWidening("widen aggregation expression after projection",
    query = _.groupBy(col("widened") + lit(1).cast(ByteType)).agg(count(col("other"))),
    expectedResult = ExpectedResult.Failure { ex =>
      assert(ex.getErrorClass === "STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE")
    },
    outputMode = OutputMode.Complete()
  )

  testStreamTypeWidening("widen limit",
    query = _.select("widened").limit(1),
    expectedResult = ExpectedResult.Success(Seq.empty)
  )

  testStreamTypeWidening("widen distinct",
    query = _.select("widened").distinct(),
    expectedResult = ExpectedResult.Failure { ex =>
      assert(ex.getErrorClass === "STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE")
    }
  )

  testStreamTypeWidening("widen drop duplicates",
    query = _.select("widened").dropDuplicates(),
    expectedResult = ExpectedResult.Failure { ex =>
      assert(ex.getErrorClass === "STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE")
    },
    outputMode = OutputMode.Update()
  )

  testStreamTypeWidening("widen drop duplicates with watermark",
    query = _.select("widened")
      .withColumn("watermark", lit("2025-02-04").cast("timestamp"))
      .withWatermark("watermark", "0 seconds")
      .dropDuplicatesWithinWatermark(),
    expectedResult = ExpectedResult.Failure { ex =>
      assert(ex.getErrorClass === "STATE_STORE_KEY_SCHEMA_NOT_COMPATIBLE")
    },
    outputMode = OutputMode.Update()
  )

  testStreamTypeWidening("widen flatMap groups with state",
    query = _.select("widened").as[Int]
      .groupByKey(x => x)
        .flatMapGroupsWithState(
          outputMode = OutputMode.Update,
          timeoutConf = GroupStateTimeout.NoTimeout
        )((key: Int, values: Iterator[Int], state: GroupState[Int]) => {
          Iterator(values.max)
        })
      .toDF(),
    expectedResult = ExpectedResult.Success(Seq(Row(123456789))),
    outputMode = OutputMode.Update()
  )

  test("widening type change then restore back") {
    withTempDir { dir =>
      sql(s"CREATE TABLE delta.`$dir` (a byte) USING DELTA")
      val checkpointDir = new File(dir, "sink_checkpoint")

      testStream(readStream(dir, checkpointDir))(
        StartStream(checkpointLocation = checkpointDir.toString),
        Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (1)") },
        Execute { _ => sql(s"ALTER TABLE delta.`$dir`ALTER COLUMN a TYPE int") },
        // Widening a column type requires restarting the stream so that the new, wider schema is
        // used to process the batch.
        ExpectMetadataEvolutionException()
      )

      testStream(readStream(dir, checkpointDir))(
        StartStream(checkpointLocation = checkpointDir.toString),
        Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (123456789)") },
        // The type change is blocked until the user reviews it and unblocks the stream.
        ExpectTypeChangeBlockedException()
      )

      withSQLConf("spark.databricks.delta.streaming.allowSourceColumnTypeChange" -> "always") {
        testStream(readStream(dir, checkpointDir, options = Map("ignoreDeletes" -> "true")))(
          StartStream(checkpointLocation = checkpointDir.toString),
          ProcessAllAvailable(),
          CheckLastBatch(123456789),
          // Restore will narrow the type back, the schema change fails the query.
          Execute { _ => sql(s"RESTORE delta.`$dir` VERSION AS OF 1") },
          ExpectMetadataEvolutionException()
        )
      }

      // Retrying doesn't allow the narrowing type change to go through.
      withSQLConf("spark.databricks.delta.streaming.allowSourceColumnTypeChange" -> "always") {
        testStream(readStream(dir, checkpointDir, options = Map("ignoreDeletes" -> "true")))(
          StartStream(checkpointLocation = checkpointDir.toString),
          ExpectIncompatibleSchemaChangeException()
        )
      }
    }
  }

  for { (name: String, toType: DataType) <- Seq(
    ("narrowing", ByteType),
    ("arbitrary", StringType))
  } {
    test(s"$name type changes are not supported") {
      withTempDir { dir =>
        sql(s"CREATE TABLE delta.`$dir` (a int) USING DELTA")
        val checkpointDir = new File(dir, "sink_checkpoint")

        testStream(readStream(dir, checkpointDir, options = Map("ignoreDeletes" -> "true")))(
          StartStream(checkpointLocation = checkpointDir.toString),
          Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (1)") },
          ProcessAllAvailable(),
          Execute { _ =>
            // Overwrite the table schema to apply an arbitrary type change.
            spark
              .createDataFrame(
                sparkContext.emptyRDD[Row],
                StructType.fromDDL(s"a ${toType.sql}"))
              .write
              .format("delta")
              .mode(SaveMode.Overwrite)
              .option("overwriteSchema", "true")
              .save(dir.getCanonicalPath)
          },
          ExpectMetadataEvolutionException()
        )

        // Try to restart the stream even though the error is not retryable and it will fail again.
        testStream(readStream(dir, checkpointDir, options = Map("ignoreDeletes" -> "true")))(
          StartStream(checkpointLocation = checkpointDir.toString),
          Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (2)") },
          ExpectIncompatibleSchemaChangeException()
        )
      }
    }
  }

  test(
    "type change first without schemaTrackingLocation and unblock using schemaTrackingLocation") {
    withTempDir { dir =>
      sql(s"CREATE TABLE delta.`$dir` (widened byte) USING DELTA")
      val checkpointDir = new File(dir, "sink_checkpoint")

      def readWithoutSchemaTrackingLog(): DataFrame =
        spark.readStream.format("delta").load(dir.getCanonicalPath)

      testStream(readWithoutSchemaTrackingLog())(
        StartStream(checkpointLocation = checkpointDir.toString),
        Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (1)") },
        ProcessAllAvailable(),
        CheckAnswer(1)
      )

      testStream(readWithoutSchemaTrackingLog())(
        StartStream(checkpointLocation = checkpointDir.toString),
        Execute { _ => sql(s"ALTER TABLE delta.`$dir`ALTER COLUMN widened TYPE int") },
        Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (123456789)") },
        ExpectFailure[DeltaStreamingNonAdditiveSchemaIncompatibleException]()
      )

      // First retry with schema log initializes it.
      testStream(readStream(dir, checkpointDir))(
        StartStream(checkpointLocation = checkpointDir.toString),
        ExpectMetadataEvolutionException()
      )
      // Second retry updates the schema log after the type change, then fails.
      testStream(readStream(dir, checkpointDir))(
        StartStream(checkpointLocation = checkpointDir.toString),
        ExpectMetadataEvolutionException()
      )
      // Third retry requests user action to unblock the stream.
      testStream(readStream(dir, checkpointDir))(
        StartStream(checkpointLocation = checkpointDir.toString),
        ExpectTypeChangeBlockedException()
      )
      // Unblocking the stream goes through.
      withSQLConf("spark.databricks.delta.streaming.allowSourceColumnTypeChange" -> "always") {
        testStream(readStream(dir, checkpointDir))(
          StartStream(checkpointLocation = checkpointDir.toString),
          ProcessAllAvailable(),
          CheckAnswer(123456789)
        )
      }
    }
  }

  for ((name, getSqlConf: (Int => String), value) <- Seq(
    ("unblock all", (_: Int) => "allowSourceColumnTypeChange", "always"),
    ("unblock stream", (hash: Int) => s"allowSourceColumnTypeChange.ckpt_$hash", "always"),
    ("unblock version", (hash: Int) => s"allowSourceColumnTypeChange.ckpt_$hash", "2")
  )) {
    test(s"unblocking stream after type change - $name") {
      withTempDir { dir =>
        sql(s"CREATE TABLE delta.`$dir` (widened byte, other byte) USING DELTA")
        // Getting the checkpoint dir through the delta log to ensure the format is consistent with
        // the path used internally to compute the hash of the checkpoint location to unblock the
        // stream.
        val deltaLog = DeltaLog.forTable(spark, dir.toString)
        val checkpointDir = new File(deltaLog.dataPath.toString, "sink_checkpoint")

        def readWithAgg(): DataFrame =
          readStream(dir, checkpointDir)
            .groupBy("other")
            .agg(count(col("widened")))

        testStream(readWithAgg(), outputMode = OutputMode.Complete())(
          StartStream(checkpointLocation = checkpointDir.toString),
          Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (1, 1)") },
          Execute { _ => sql(s"ALTER TABLE delta.`$dir`ALTER COLUMN widened TYPE int") },
          ExpectMetadataEvolutionException()
        )

        testStream(readWithAgg(), outputMode = OutputMode.Complete())(
          StartStream(checkpointLocation = checkpointDir.toString),
          ExpectTypeChangeBlockedException()
        )

        val checkpointHash = s"$checkpointDir/sources/0".hashCode

        withSQLConf(s"spark.databricks.delta.streaming.${getSqlConf(checkpointHash)}" -> value) {
          testStream(readWithAgg(), outputMode = OutputMode.Complete())(
            StartStream(checkpointLocation = checkpointDir.toString),
            Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (123456789, 1)") },
            ProcessAllAvailable(),
            CheckLastBatch(Row(1, 2))
          )
        }
      }
    }
  }

  test(s"overwrite schema with type change and dropped column") {
    withTempDir { dir =>
      sql(s"CREATE TABLE delta.`$dir` (a byte, b int) USING DELTA")
      val checkpointDir = new File(dir, "sink_checkpoint")

      testStream(readStream(dir, checkpointDir, options = Map("ignoreDeletes" -> "true")))(
        StartStream(checkpointLocation = checkpointDir.toString),
        Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (1, 1)") },
        ProcessAllAvailable(),
        Execute { _ =>
          // Overwrite the table schema.
          spark
            .createDataFrame(
              sparkContext.emptyRDD[Row],
              StructType.fromDDL(s"a INT"))
            .write
            .format("delta")
            .mode(SaveMode.Overwrite)
            .option("overwriteSchema", "true")
            .save(dir.getCanonicalPath)
        },
        ExpectMetadataEvolutionException()
      )

      testStream(readStream(dir, checkpointDir, options = Map("ignoreDeletes" -> "true")))(
        StartStream(checkpointLocation = checkpointDir.toString),
        ExpectFailure[DeltaRuntimeException] { ex =>
          checkErrorMatchPVals(
            exception = ex.asInstanceOf[DeltaRuntimeException],
            "DELTA_STREAMING_CANNOT_CONTINUE_PROCESSING_POST_SCHEMA_EVOLUTION",
            parameters = Map(
              "opType" -> "DROP AND TYPE WIDENING",
              "previousSchemaChangeVersion" -> "0",
              "currentSchemaChangeVersion" -> "2",
              "unblockChangeConfs" ->
                ".*allowSourceColumnDrop(.|\\n)*allowSourceColumnTypeChange.*",
              "unblockStreamConfs" ->
                ".*allowSourceColumnDrop(.|\\n)*allowSourceColumnTypeChange.*",
              "unblockAllConfs" ->
                ".*allowSourceColumnDrop(.|\\n)*allowSourceColumnTypeChange.*"
            ))
        }
      )
      // Allowing both source column drop and type widening allows the stream to proceed
      withSQLConf(
          "spark.databricks.delta.streaming.allowSourceColumnDrop" -> "always",
          "spark.databricks.delta.streaming.allowSourceColumnTypeChange" -> "always") {
        testStream(readStream(dir, checkpointDir, options = Map("ignoreDeletes" -> "true")))(
          StartStream(checkpointLocation = checkpointDir.toString),
          Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (2)") },
          ProcessAllAvailable()
        )
      }
    }
  }

  test("type change in delta source writing to a delta sink") {
    // End-to-end test with a delta source and a delta sink.
    withTempDir { sourceDir =>
      withTempDir { sinkDir =>
        // The test mixin implicitly enables type widening on all tables, disable type widening on
        // the sink initially for this test.
        sql(s"CREATE TABLE delta.`$sourceDir` (a byte) USING DELTA")
        sql(
          s"""
             |CREATE TABLE delta.`$sinkDir` (a byte) USING DELTA
             |TBLPROPERTIES('delta.enableTypeWidening' = 'false')
           """.stripMargin)
        val checkpointDir = new File(sinkDir, "checkpoint_dir")

        def runStream(mergeSchema: Boolean): Unit = try {
          withSQLConf(
            "spark.databricks.delta.streaming.allowSourceColumnTypeChange" -> "always") {
            val q = spark
              .readStream
              .format("delta")
              .option(DeltaOptions.SCHEMA_TRACKING_LOCATION, checkpointDir.toString)
              .load(sourceDir.toString)
              .writeStream
              .format("delta")
              .option("checkpointLocation", checkpointDir.toString)
              .option("mergeSchema", mergeSchema.toString)
              .start(sinkDir.getCanonicalPath)
            q.processAllAvailable()
            q.stop()
          }
        } catch {
          case e: StreamingQueryException =>
            // Unwrap the exception for convenience
            throw e.getCause
        }

        // Start with no type change.
        sql(s"INSERT INTO delta.`$sourceDir` VALUES (1)")
        runStream(mergeSchema = false)
        checkAnswer(readDeltaTable(sinkDir.toString), Seq(Row(1)))

        // Change type of column 'a' and introduce a new column 'b'. Schema evolution is enabled
        // so the new column 'b' is added to the sink, but type widening is disabled on the sink so
        // the type of column 'a' remains INT: values are downcasted from INT to BYTE on write.
        sql(s"ALTER TABLE delta.`$sourceDir`ALTER COLUMN a TYPE int")
        sql(s"ALTER TABLE delta.`$sourceDir`ADD COLUMN b int")
        sql(s"INSERT INTO delta.`$sourceDir` VALUES (2, 2)")
        val evolutionException = intercept[DeltaRuntimeException] {
          runStream(mergeSchema = true)
        }
        assert(evolutionException.getErrorClass === "DELTA_STREAMING_METADATA_EVOLUTION")
        runStream(mergeSchema = true)
        assert(readDeltaTable(sinkDir.toString).schema("a").dataType === ByteType)
        assert(readDeltaTable(sinkDir.toString).schema("b").dataType === IntegerType)
        checkAnswer(readDeltaTable(sinkDir.toString), Seq(Row(1, null), Row(2, 2)))

        // Enable type widening on the sink and insert a value in 'a' that won't fit, first with
        // schema evolution disabled: the type of column 'a' in the sink isn't automatically changed
        // to INT and values are downcast: the value overflows and fails.
        sql(s"ALTER TABLE delta.`$sinkDir` SET TBLPROPERTIES('delta.enableTypeWidening' = 'true')")
        sql(s"INSERT INTO delta.`$sourceDir` VALUES (${Int.MaxValue}, ${Int.MaxValue})")

        def getSparkArithmeticException(ex: Throwable): SparkArithmeticException = ex match {
          case e: SparkArithmeticException => e
          case e: Throwable if e.getCause != null => getSparkArithmeticException(e.getCause)
          case e => fail(s"Unexpected exception: $e")
        }

        val ex = intercept[Throwable] {
          runStream(mergeSchema = false)
        }
        assert(getSparkArithmeticException(ex).getErrorClass === "CAST_OVERFLOW_IN_TABLE_INSERT")

        // Retry with schema evolution enabled. Type widening is also enabled on the sink, the type
        // of column 'a' is widened to INT and the write succeeds.
        runStream(mergeSchema = true)
        assert(readDeltaTable(sinkDir.toString).schema("a").dataType === IntegerType)
        assert(readDeltaTable(sinkDir.toString).schema("b").dataType === IntegerType)
        checkAnswer(
          readDeltaTable(sinkDir.toString),
          Seq(Row(1, null), Row(2, 2), Row(Int.MaxValue, Int.MaxValue))
        )
      }
    }
  }
}
