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
import java.util.Locale

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.sources.{DeltaSink, DeltaSQLConf}
import org.apache.spark.sql.delta.test.{DeltaColumnMappingSelectedTestMixin, DeltaSQLCommandTest}
import org.apache.commons.io.FileUtils
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.execution.DataSourceScanExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming.{MemoryStream, MicroBatchExecution, StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSourceV1
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

abstract class DeltaSinkTest
  extends StreamTest
  with DeltaSQLCommandTest {

  override val streamingTimeout = 60.seconds
  import testImplicits._

  // Before we start running the tests in this suite, we should let Spark perform all necessary set
  // up that needs to be done for streaming. Without this, the first test in the suite may be flaky
  // as its running time can exceed the timeout for the test due to Spark setup. See: ES-235735
  override def beforeAll(): Unit = {
    super.beforeAll()
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Int].toDF()
      val query = inputData.writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .format("delta")
        .start(outputDir.getCanonicalPath)

      query.stop()
    }
  }

  protected def withTempDirs(f: (File, File) => Unit): Unit = {
    withTempDir { file1 =>
      withTempDir { file2 =>
        f(file1, file2)
      }
    }
  }
}

class DeltaSinkSuite
  extends DeltaSinkTest
  with DeltaColumnMappingTestUtils {

  import testImplicits._

  test("append mode") {
    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>
        val inputData = MemoryStream[Int]
        val df = inputData.toDF()
        val query = df.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("delta")
          .start(outputDir.getCanonicalPath)
        val log = DeltaLog.forTable(spark, outputDir.getCanonicalPath)
        try {
          inputData.addData(1)
          query.processAllAvailable()

          val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
          checkDatasetUnorderly(outputDf.as[Int], 1)
          assert(log.update().transactions.head == (query.id.toString -> 0L))

          inputData.addData(2)
          query.processAllAvailable()

          checkDatasetUnorderly(outputDf.as[Int], 1, 2)
          assert(log.update().transactions.head == (query.id.toString -> 1L))

          inputData.addData(3)
          query.processAllAvailable()

          checkDatasetUnorderly(outputDf.as[Int], 1, 2, 3)
          assert(log.update().transactions.head == (query.id.toString -> 2L))
        } finally {
          query.stop()
        }
      }
    }
  }

  test("complete mode") {
    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>
        val inputData = MemoryStream[Int]
        val df = inputData.toDF()
        val query =
          df.groupBy().count()
            .writeStream
            .outputMode("complete")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("delta")
            .start(outputDir.getCanonicalPath)
        val log = DeltaLog.forTable(spark, outputDir.getCanonicalPath)
        try {
          inputData.addData(1)
          query.processAllAvailable()

          val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
          checkDatasetUnorderly(outputDf.as[Long], 1L)
          assert(log.update().transactions.head == (query.id.toString -> 0L))

          inputData.addData(2)
          query.processAllAvailable()

          checkDatasetUnorderly(outputDf.as[Long], 2L)
          assert(log.update().transactions.head == (query.id.toString -> 1L))

          inputData.addData(3)
          query.processAllAvailable()

          checkDatasetUnorderly(outputDf.as[Long], 3L)
          assert(log.update().transactions.head == (query.id.toString -> 2L))
        } finally {
          query.stop()
        }
      }
    }
  }

  test("update mode: not supported") {
    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>
        val inputData = MemoryStream[Int]
        val df = inputData.toDF()
        val e = intercept[AnalysisException] {
          df.writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .outputMode("update")
            .format("delta")
            .start(outputDir.getCanonicalPath)
        }
        Seq("update", "not support").foreach { msg =>
          assert(e.getMessage.toLowerCase(Locale.ROOT).contains(msg))
        }
      }
    }
  }

  test("path not specified") {
    failAfter(streamingTimeout) {
      withTempDir { checkpointDir =>
        val inputData = MemoryStream[Int]
        val df = inputData.toDF()
        val e = intercept[IllegalArgumentException] {
          df.writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("delta")
            .start()
        }
        Seq("path", " not specified").foreach { msg =>
          assert(e.getMessage.toLowerCase(Locale.ROOT).contains(msg))
        }
      }
    }
  }

  test("SPARK-21167: encode and decode path correctly") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[String]
      val query = inputData.toDS()
        .map(s => (s, s.length))
        .toDF("value", "len")
        .writeStream
        .partitionBy("value")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .format("delta")
        .start(outputDir.getCanonicalPath)

      try {
        // The output is partitioned by "value", so the value will appear in the file path.
        // This is to test if we handle spaces in the path correctly.
        inputData.addData("hello world")
        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }
        val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
        checkDatasetUnorderly(outputDf.as[(String, Int)], ("hello world", "hello world".length))
      } finally {
        query.stop()
      }
    }
  }

  test("partitioned writing and batch reading") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Int]
      val ds = inputData.toDS()
      val query =
        ds.map(i => (i, i * 1000))
          .toDF("id", "value")
          .writeStream
          .partitionBy("id")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("delta")
          .start(outputDir.getCanonicalPath)
      try {

        inputData.addData(1, 2, 3)
        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }

        val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
        val expectedSchema = new StructType()
          .add(StructField("id", IntegerType))
          .add(StructField("value", IntegerType))
        assert(outputDf.schema === expectedSchema)

        // Verify the correct partitioning schema has been inferred
        val hadoopFsRelations = outputDf.queryExecution.analyzed.collect {
          case LogicalRelation(baseRelation, _, _, _) if
              baseRelation.isInstanceOf[HadoopFsRelation] =>
            baseRelation.asInstanceOf[HadoopFsRelation]
        }
        assert(hadoopFsRelations.size === 1)
        assert(hadoopFsRelations.head.partitionSchema.exists(_.name == "id"))
        assert(hadoopFsRelations.head.dataSchema.exists(_.name == "value"))

        // Verify the data is correctly read
        checkDatasetUnorderly(
          outputDf.as[(Int, Int)],
          (1, 1000), (2, 2000), (3, 3000))

        /** Check some condition on the partitions of the FileScanRDD generated by a DF */
        def checkFileScanPartitions(df: DataFrame)(func: Seq[FilePartition] => Unit): Unit = {
          val filePartitions = df.queryExecution.executedPlan.collect {
            case scan: DataSourceScanExec if scan.inputRDDs().head.isInstanceOf[FileScanRDD] =>
              scan.inputRDDs().head.asInstanceOf[FileScanRDD].filePartitions
          }.flatten
          if (filePartitions.isEmpty) {
            fail(s"No FileScan in query\n${df.queryExecution}")
          }
          func(filePartitions)
        }

        // Read without pruning
        checkFileScanPartitions(outputDf) { partitions =>
          // There should be as many distinct partition values as there are distinct ids
          assert(partitions.flatMap(_.files.map(_.partitionValues)).distinct.size === 3)
        }

        // Read with pruning, should read only files in partition dir id=1
        checkFileScanPartitions(outputDf.filter("id = 1")) { partitions =>
          // use physical name
          val filesToBeRead = partitions.flatMap(_.files)
          assert(filesToBeRead.forall(_.partitionValues.getInt(0) == 1))
          assert(filesToBeRead.map(_.partitionValues).distinct.size === 1)
        }

        // Read with pruning, should read only files in partition dir id=1 and id=2
        checkFileScanPartitions(outputDf.filter("id in (1,2)")) { partitions =>
          val filesToBeRead = partitions.flatMap(_.files)
          assert(filesToBeRead.forall(_.partitionValues.getInt(0) != 3))
          assert(filesToBeRead.map(_.partitionValues).distinct.size === 2)
        }
      } finally {
        if (query != null) {
          query.stop()
        }
      }
    }
  }

  test("work with aggregation + watermark") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Long]
      val inputDF = inputData.toDF.toDF("time")
      val outputDf = inputDF
        .selectExpr("CAST(time AS timestamp) AS timestamp")
        .withWatermark("timestamp", "10 seconds")
        .groupBy(window($"timestamp", "5 seconds"))
        .count()
        .select("window.start", "window.end", "count")

      val query =
        outputDf.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("delta")
          .start(outputDir.getCanonicalPath)
      try {
        def addTimestamp(timestampInSecs: Int*): Unit = {
          inputData.addData(timestampInSecs.map(_ * 1L): _*)
          failAfter(streamingTimeout) {
            query.processAllAvailable()
          }
        }

        def check(expectedResult: ((Long, Long), Long)*): Unit = {
          val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
            .selectExpr(
              "CAST(start as BIGINT) AS start",
              "CAST(end as BIGINT) AS end",
              "count")
          checkDatasetUnorderly(
            outputDf.as[(Long, Long, Long)],
            expectedResult.map(x => (x._1._1, x._1._2, x._2)): _*)
        }

        addTimestamp(100) // watermark = None before this, watermark = 100 - 10 = 90 after this
        addTimestamp(104, 123) // watermark = 90 before this, watermark = 123 - 10 = 113 after this

        addTimestamp(140) // wm = 113 before this, emit results on 100-105, wm = 130 after this
        check((100L, 105L) -> 2L, (120L, 125L) -> 1L) // no-data-batch emits results on 120-125

      } finally {
        if (query != null) {
          query.stop()
        }
      }
    }
  }

  test("throw exception when users are trying to write in batch with different partitioning") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Int]
      val ds = inputData.toDS()
      val query =
        ds.map(i => (i, i * 1000))
          .toDF("id", "value")
          .writeStream
          .partitionBy("id")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("delta")
          .start(outputDir.getCanonicalPath)
      try {

        inputData.addData(1, 2, 3)
        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }

        val e = intercept[AnalysisException] {
          spark.range(100)
            .select('id.cast("integer"), 'id % 4 as "by4", 'id.cast("integer") * 1000 as "value")
            .write
            .format("delta")
            .partitionBy("id", "by4")
            .mode("append")
            .save(outputDir.getCanonicalPath)
        }
        assert(e.getMessage.contains("Partition columns do not match"))

      } finally {
        query.stop()
      }
    }
  }

  testQuietly("incompatible schema merging throws errors - first streaming then batch") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Int]
      val ds = inputData.toDS()
      val query =
        ds.map(i => (i, i * 1000))
          .toDF("id", "value")
          .writeStream
          .partitionBy("id")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("delta")
          .start(outputDir.getCanonicalPath)
      try {

        inputData.addData(1, 2, 3)
        failAfter(streamingTimeout) {
          query.processAllAvailable()
        }

        val e = intercept[AnalysisException] {
          spark.range(100).select('id, ('id * 3).cast("string") as "value")
            .write
            .partitionBy("id")
            .format("delta")
            .mode("append")
            .save(outputDir.getCanonicalPath)
        }
        checkError(
          exception = e,
          errorClass = "DELTA_FAILED_TO_MERGE_FIELDS",
          parameters = Map("currentField" -> "id", "updateField" -> "id"))
      } finally {
        query.stop()
      }
    }
  }

  test("incompatible schema merging throws errors - first batch then streaming") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Int]
      val ds = inputData.toDS()
      val dsWriter =
        ds.map(i => (i, i * 1000))
          .toDF("id", "value")
          .writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("delta")
      spark.range(100).select('id, ('id * 3).cast("string") as "value")
        .write
        .format("delta")
        .mode("append")
        .save(outputDir.getCanonicalPath)

      // More tests covering type changes can be found in [[DeltaSinkImplicitCastSuite]]. This only
      // covers type changes disabled.
      withSQLConf(DeltaSQLConf.DELTA_STREAMING_SINK_ALLOW_IMPLICIT_CASTS.key -> "false") {
        val wrapperException = intercept[StreamingQueryException] {
          val q = dsWriter.start(outputDir.getCanonicalPath)
          inputData.addData(1, 2, 3)
          q.processAllAvailable()
        }
        assert(wrapperException.cause.isInstanceOf[AnalysisException])
        checkError(
          exception = wrapperException.cause.asInstanceOf[AnalysisException],
          errorClass = "DELTA_FAILED_TO_MERGE_FIELDS",
          parameters = Map("currentField" -> "id", "updateField" -> "id"))
      }
    }
  }

  private def verifyDeltaSinkCatalog(f: DataStreamWriter[_] => StreamingQuery): Unit = {
    // Create a Delta sink whose target table is defined by our caller.
    val input = MemoryStream[Int]
    val streamWriter = input.toDF
      .writeStream
      .format("delta")
      .option(
        "checkpointLocation",
        Utils.createTempDir(namePrefix = "tahoe-test").getCanonicalPath)
    val q = f(streamWriter).asInstanceOf[StreamingQueryWrapper]

    // WARNING: Only the query execution thread is allowed to initialize the logical plan (enforced
    // by an assertion in MicroBatchExecution.scala). To avoid flaky failures, run the stream to
    // completion, to guarantee the query execution thread ran before we try to access the plan.
    try {
      input.addData(1, 2, 3)
      q.processAllAvailable()
    } finally {
      q.stop()
    }

    val plan = q.streamingQuery.logicalPlan
    val WriteToMicroBatchDataSourceV1(catalogTable, sink: DeltaSink, _, _, _, _, _) = plan
    assert(catalogTable === sink.catalogTable)
  }

  test("DeltaSink.catalogTable is correctly populated - catalog-based table") {
    withTable("tab") {
      verifyDeltaSinkCatalog(_.toTable("tab"))
    }
  }

  test("DeltaSink.catalogTable is correctly populated - path-based table") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }
      verifyDeltaSinkCatalog(_.start(tempDir.getCanonicalPath))
    }
  }

  test("can't write out with all columns being partition columns") {
    withTempDirs { (outputDir, checkpointDir) =>
      val inputData = MemoryStream[Int]
      val ds = inputData.toDS()
      val query =
        ds.map(i => (i, i * 1000))
          .toDF("id", "value")
          .writeStream
          .partitionBy("id", "value")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("delta")
          .start(outputDir.getCanonicalPath)
      val e = intercept[StreamingQueryException] {
        inputData.addData(1)
        query.awaitTermination(30000)
      }
      assert(e.cause.isInstanceOf[AnalysisException])
    }
  }

  test("streaming write correctly sets isBlindAppend in CommitInfo") {
    withTempDirs { (outputDir, checkpointDir) =>

      val input = MemoryStream[Int]
      val inputDataStream = input.toDF().toDF("value")

      def tableData: DataFrame = spark.read.format("delta").load(outputDir.toString)

      def appendToTable(df: DataFrame): Unit = failAfter(streamingTimeout) {
        var q: StreamingQuery = null
        try {
          input.addData(0)
          q = df.writeStream
            .format("delta")
            .option("checkpointLocation", checkpointDir.toString)
            .start(outputDir.toString)
          q.processAllAvailable()
        } finally {
          if (q != null) q.stop()
        }
      }

      var lastCheckedVersion = -1L
      def isLastCommitBlindAppend: Boolean = {
        val log = DeltaLog.forTable(spark, outputDir.toString)
        val lastVersion = log.update().version
        assert(lastVersion > lastCheckedVersion, "no new commit was made")
        lastCheckedVersion = lastVersion
        val lastCommitChanges = log.getChanges(lastVersion).toSeq.head._2
        lastCommitChanges.collectFirst { case c: CommitInfo => c }.flatMap(_.isBlindAppend).get
      }

      // Simple streaming write should have isBlindAppend = true
      appendToTable(inputDataStream)
      assert(
        isLastCommitBlindAppend,
        "simple write to target table should have isBlindAppend = true")

      // Join with the table should have isBlindAppend = false
      appendToTable(inputDataStream.join(tableData, "value"))
      assert(
        !isLastCommitBlindAppend,
        "joining with target table in the query should have isBlindAppend = false")
    }
  }

  test("do not trust user nullability, so that parquet files aren't corrupted") {
    val jsonRec = """{"s": "ss", "b": {"s": "ss"}}"""
    val schema = new StructType()
      .add("s", StringType)
      .add("b", new StructType()
        .add("s", StringType)
        .add("i", IntegerType, nullable = false))
      .add("c", IntegerType, nullable = false)

    withTempDir { base =>
      val sourceDir = new File(base, "source").getCanonicalPath
      val tableDir = new File(base, "output").getCanonicalPath
      val chkDir = new File(base, "checkpoint").getCanonicalPath

      FileUtils.write(new File(sourceDir, "a.json"), jsonRec)

      val q = spark.readStream
        .format("json")
        .schema(schema)
        .load(sourceDir)
        .withColumn("file", input_file_name()) // Not sure why needs this to reproduce
        .writeStream
        .format("delta")
        .trigger(org.apache.spark.sql.streaming.Trigger.Once)
        .option("checkpointLocation", chkDir)
        .start(tableDir)

      q.awaitTermination()

      checkAnswer(
        spark.read.format("delta").load(tableDir).drop("file"),
        Seq(Row("ss", Row("ss", null), null)))
    }
  }

  test("history includes user-defined metadata for DataFrame.writeStream API") {
    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>
        val inputData = MemoryStream[Int]
        val df = inputData.toDF()
        val query = df.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .option("userMetadata", "testMeta!")
          .format("delta")
          .start(outputDir.getCanonicalPath)
        val log = DeltaLog.forTable(spark, outputDir.getCanonicalPath)

        inputData.addData(1)
        query.processAllAvailable()

        val lastCommitInfo = io.delta.tables.DeltaTable.forPath(spark, outputDir.getCanonicalPath)
            .history(1).as[DeltaHistory].head

        assert(lastCommitInfo.userMetadata === Some("testMeta!"))
        query.stop()
      }
    }
  }

}

abstract class DeltaSinkColumnMappingSuiteBase extends DeltaSinkSuite
  with DeltaColumnMappingSelectedTestMixin {
  import testImplicits._

  override protected def runOnlyTests = Seq(
    "append mode",
    "complete mode",
    "partitioned writing and batch reading",
    "work with aggregation + watermark"
  )


  test("allow schema evolution after renaming column") {
    Seq(true, false).foreach { schemaMergeEnabled =>
      withClue(s"Schema merge enabled: $schemaMergeEnabled") {
        withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaMergeEnabled.toString) {
          failAfter(streamingTimeout) {
            withTempDirs { (outputDir, checkpointDir) =>
              val sourceDir = Utils.createTempDir()
              def addData(df: DataFrame): Unit =
                df.coalesce(1).write.mode("append").save(sourceDir.getCanonicalPath)

              // save data to target dir
              Seq(100).toDF("value").write.format("delta").save(outputDir.getCanonicalPath)
              // use parquet stream as MemoryStream doesn't support recovering failed batches
              val df = spark.readStream
                .schema(new StructType().add("value", IntegerType, true))
                .parquet(sourceDir.getCanonicalPath)
              // start writing into Delta sink
              def queryGen(df: DataFrame): StreamingQuery = df.writeStream
                .option("checkpointLocation", checkpointDir.getCanonicalPath)
                .format("delta")
                .start(outputDir.getCanonicalPath)

              val query = queryGen(df)
              val log = DeltaLog.forTable(spark, outputDir.getCanonicalPath)

              // delta sink contains [100, 1]
              addData(Seq(1).toDF("value"))
              query.processAllAvailable()

              def outputDf: DataFrame =
                spark.read.format("delta").load(outputDir.getCanonicalPath)
              checkDatasetUnorderly(outputDf.as[Int], 100, 1)
              require(log.update().transactions.head == (query.id.toString -> 0L))

              sql(s"ALTER TABLE delta.`${outputDir.getAbsolutePath}` " +
                s"RENAME COLUMN value TO new_value")

              if (!schemaMergeEnabled) {
                // schema has changed, we can't automatically migrate the schema
                val e = intercept[StreamingQueryException] {
                  addData(Seq(2).toDF("value"))
                  query.processAllAvailable()
                }
                assert(e.cause.isInstanceOf[AnalysisException])
                assert(e.cause.getMessage.contains("A schema mismatch detected when writing"))

                // restart using the same query would still fail
                val query2 = queryGen(df)
                val e2 = intercept[StreamingQueryException] {
                  addData(Seq(2).toDF("value"))
                  query2.processAllAvailable()
                }
                assert(e2.cause.isInstanceOf[AnalysisException])
                assert(e2.cause.getMessage.contains("A schema mismatch detected when writing"))

                // but reingest using new schema should work
                val df2 = spark.readStream
                  .schema(new StructType().add("value", IntegerType, true))
                  .parquet(sourceDir.getCanonicalPath)
                  .withColumnRenamed("value", "new_value")
                val query3 = queryGen(df2)
                // delta sink contains [100, 1, 2] + [2, 2] due to recovering the failed batched
                addData(Seq(2).toDF("value"))
                query3.processAllAvailable()
                checkAnswer(outputDf,
                  Row(100) :: Row(1) :: Row(2) :: Row(2) :: Row(2) :: Nil)
                assert(outputDf.schema == new StructType().add("new_value", IntegerType, true))
                query3.stop()
              } else {
                // we allow auto schema migration, delta sink contains [100, 1, 2]
                addData(Seq(2).toDF("value"))
                query.processAllAvailable()
                // Since the incoming `value` column is now merged as a new column (even though it
                // has the same value as the original name) in which only the 3rd record has data.
                checkAnswer(outputDf, Row(100, null) :: Row(1, null) :: Row(null, 2) :: Nil)
                assert(outputDf.schema ==
                  new StructType().add("new_value", IntegerType, true)
                    .add("value", IntegerType, true))
                query.stop()
              }
            }
          }
        }
      }
    }
  }

  test("allow schema evolution after dropping column") {
    Seq(true, false).foreach { schemaMergeEnabled =>
      withClue(s"Schema merge enabled: $schemaMergeEnabled") {
        withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> schemaMergeEnabled.toString) {
          failAfter(streamingTimeout) {
            withTempDirs { (outputDir, checkpointDir) =>
              val sourceDir = Utils.createTempDir()
              def addData(df: DataFrame): Unit =
                df.coalesce(1).write.mode("append").save(sourceDir.getCanonicalPath)

              // save data to target dir
              Seq((1, 100)).toDF("id", "value").write.format("delta")
                .save(outputDir.getCanonicalPath)

              // use parquet stream as MemoryStream doesn't support recovering failed batches
              val df = spark.readStream
                .schema(new StructType().add("id", IntegerType, true)
                  .add("value", IntegerType, true))
                .parquet(sourceDir.getCanonicalPath)

              // start writing into Delta sink
              def queryGen(df: DataFrame): StreamingQuery = df.writeStream
                .option("checkpointLocation", checkpointDir.getCanonicalPath)
                .format("delta")
                .start(outputDir.getCanonicalPath)

              val query = queryGen(df)
              val log = DeltaLog.forTable(spark, outputDir.getCanonicalPath)
              // delta sink contains [(1, 100), (2, 200)]
              addData(Seq((2, 200)).toDF("id", "value"))
              query.processAllAvailable()

              def outputDf: DataFrame =
                spark.read.format("delta").load(outputDir.getCanonicalPath)

              checkDatasetUnorderly(outputDf.as[(Int, Int)], (1, 100), (2, 200))
              assert(log.update().transactions.head == (query.id.toString -> 0L))

              withSQLConf(DeltaSQLConf.DELTA_ALTER_TABLE_DROP_COLUMN_ENABLED.key -> "true") {
                sql(s"ALTER TABLE delta.`${outputDir.getAbsolutePath}` DROP COLUMN value")
              }

              if (!schemaMergeEnabled) {
                // schema changed, we can't automatically migrate the schema
                val e = intercept[StreamingQueryException] {
                  addData(Seq((3, 300)).toDF("id", "value"))
                  query.processAllAvailable()
                }
                assert(e.cause.isInstanceOf[AnalysisException])
                assert(e.cause.getMessage.contains("A schema mismatch detected when writing"))

                // restart using the same query would still fail
                val query2 = queryGen(df)
                val e2 = intercept[StreamingQueryException] {
                  addData(Seq((3, 300)).toDF("id", "value"))
                  query2.processAllAvailable()
                }
                assert(e2.cause.isInstanceOf[AnalysisException])
                assert(e2.cause.getMessage.contains("A schema mismatch detected when writing"))

                // but reingest using new schema should work
                val df2 = spark.readStream
                  .schema(new StructType().add("id", IntegerType, true))
                  .parquet(sourceDir.getCanonicalPath)
                val query3 = queryGen(df2)
                // delta sink contains [1, 2, 3] + [3, 3] due to
                // recovering failed batches
                addData(Seq((3, 300)).toDF("id", "value"))
                query3.processAllAvailable()
                checkAnswer(outputDf,
                  Row(1) :: Row(2) :: Row(3) :: Row(3) :: Row(3) :: Nil)
                assert(outputDf.schema == new StructType().add("id", IntegerType, true))
                query3.stop()
              } else {
                addData(Seq((3, 300)).toDF("id", "value"))
                query.processAllAvailable()
                // None/null value appears because even though the added column has the same
                // logical name (`value`) as the dropped column, the physical name has been
                // changed so the old data could not be loaded.
                checkAnswer(outputDf, Row(1, null) :: Row(2, null) :: Row(3, 300) :: Nil)
                assert(outputDf.schema ==
                  new StructType().add("id", IntegerType, true).add("value", IntegerType, true))
                query.stop()
              }
            }
          }
        }
      }
    }
  }

}

class DeltaSinkIdColumnMappingSuite extends DeltaSinkColumnMappingSuiteBase
  with DeltaColumnMappingEnableIdMode
  with DeltaColumnMappingTestUtils

class DeltaSinkNameColumnMappingSuite extends DeltaSinkColumnMappingSuiteBase
  with DeltaColumnMappingEnableNameMode
