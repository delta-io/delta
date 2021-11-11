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

import java.io.{File, FileNotFoundException}
import java.util.concurrent.atomic.AtomicInteger

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.actions.{CommitInfo, Protocol}
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.util.FileNames.deltaFile
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkException
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.InSet
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

class DeltaSuite extends QueryTest
  with SharedSparkSession
  with DeltaColumnMappingTestUtils  with SQLTestUtils
  with DeltaSQLCommandTest {

  import testImplicits._

  private def tryDeleteNonRecursive(fs: FileSystem, path: Path): Boolean = {
    try fs.delete(path, false) catch {
      case _: FileNotFoundException => true
    }
  }

  test("handle partition filters and data filters") {
    withTempDir { inputDir =>
      val testPath = inputDir.getCanonicalPath
      spark.range(10)
        .map(_.toInt)
        .withColumn("part", $"value" % 2)
        .write
        .format("delta")
        .partitionBy("part")
        .mode("append")
        .save(testPath)

      val ds = spark.read.format("delta").load(testPath).as[(Int, Int)]
      // partition filter
      checkDatasetUnorderly(
        ds.where("part = 1"),
        1 -> 1, 3 -> 1, 5 -> 1, 7 -> 1, 9 -> 1)
      checkDatasetUnorderly(
        ds.where("part = 0"),
        0 -> 0, 2 -> 0, 4 -> 0, 6 -> 0, 8 -> 0)
      // data filter
      checkDatasetUnorderly(
        ds.where("value >= 5"),
        5 -> 1, 6 -> 0, 7 -> 1, 8 -> 0, 9 -> 1)
      checkDatasetUnorderly(
        ds.where("value < 5"),
        0 -> 0, 1 -> 1, 2 -> 0, 3 -> 1, 4 -> 0)
      // partition filter + data filter
      checkDatasetUnorderly(
        ds.where("part = 1 and value >= 5"),
        5 -> 1, 7 -> 1, 9 -> 1)
      checkDatasetUnorderly(
        ds.where("part = 1 and value < 5"),
        1 -> 1, 3 -> 1)
    }
  }

  test("query with predicates should skip partitions") {
    withTempDir { tempDir =>
      val testPath = tempDir.getCanonicalPath

      // Generate two files in two partitions
      spark.range(2)
        .withColumn("part", $"id" % 2)
        .write
        .format("delta")
        .partitionBy("part")
        .mode("append")
        .save(testPath)

      // Read only one partition
      val query = spark.read.format("delta").load(testPath).where("part = 1")
      val fileScans = query.queryExecution.executedPlan.collect {
        case f: FileSourceScanExec => f
      }

      // Force the query to read files and generate metrics
      query.queryExecution.executedPlan.execute().count()

      // Verify only one file was read
      assert(fileScans.size == 1)
      val numFilesAferPartitionSkipping = fileScans.head.metrics.get("numFiles")
      assert(numFilesAferPartitionSkipping.nonEmpty)
      assert(numFilesAferPartitionSkipping.get.value == 1)
      checkAnswer(query, Seq(Row(1, 1)))
    }
  }

  test("partition column location should not impact table schema") {
    val tableColumns = Seq("c1", "c2")
    for (partitionColumn <- tableColumns) {
      withTempDir { inputDir =>
        val testPath = inputDir.getCanonicalPath
        Seq(1 -> "a", 2 -> "b").toDF(tableColumns: _*)
          .write
          .format("delta")
          .partitionBy(partitionColumn)
          .save(testPath)
        val ds = spark.read.format("delta").load(testPath).as[(Int, String)]
        checkDatasetUnorderly(ds, 1 -> "a", 2 -> "b")
      }
    }
  }

  test("SC-8078: read deleted directory") {
    val tempDir = Utils.createTempDir()
    val path = new Path(tempDir.getCanonicalPath)
    Seq(1).toDF().write.format("delta").save(tempDir.toString)

    val df = spark.read.format("delta").load(tempDir.toString)
    // scalastyle:off deltahadoopconfiguration
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    fs.delete(path, true)

    val e = intercept[AnalysisException] {
      withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0s") {
        checkAnswer(df, Row(1) :: Nil)
      }
    }.getMessage
    assert(e.contains("The schema of your Delta table has changed"))
    val e2 = intercept[AnalysisException] {
      withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0s") {
        // Define new DataFrame
        spark.read.format("delta").load(tempDir.toString).collect()
      }
    }.getMessage
    assert(e2.contains("is not a Delta table"))
  }

  test("SC-70676: directory deleted before first DataFrame is defined") {
    val tempDir = Utils.createTempDir()
    val path = new Path(tempDir.getCanonicalPath)
    Seq(1).toDF().write.format("delta").save(tempDir.toString)

    // scalastyle:off deltahadoopconfiguration
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    fs.delete(path, true)

    val e = intercept[AnalysisException] {
      spark.read.format("delta").load(tempDir.toString).collect()
    }.getMessage
    assert(e.contains("is not a Delta table"))
  }

  test("append then read") {
    val tempDir = Utils.createTempDir()
    Seq(1).toDF().write.format("delta").save(tempDir.toString)
    Seq(2, 3).toDF().write.format("delta").mode("append").save(tempDir.toString)

    def data: DataFrame = spark.read.format("delta").load(tempDir.toString)
    checkAnswer(data, Row(1) :: Row(2) :: Row(3) :: Nil)

    // append more
    Seq(4, 5, 6).toDF().write.format("delta").mode("append").save(tempDir.toString)
    checkAnswer(data.toDF(), Row(1) :: Row(2) :: Row(3) :: Row(4) :: Row(5) :: Row(6) :: Nil)
  }

  test("partitioned append - nulls") {
    val tempDir = Utils.createTempDir()
    Seq(Some(1), None).toDF()
      .withColumn("is_odd", $"value" % 2 === 1)
      .write
      .format("delta")
      .partitionBy("is_odd")
      .save(tempDir.toString)

    val df = spark.read.format("delta").load(tempDir.toString)

    // Verify the correct partitioning schema is picked up
    val hadoopFsRelations = df.queryExecution.analyzed.collect {
      case LogicalRelation(baseRelation, _, _, _) if
      baseRelation.isInstanceOf[HadoopFsRelation] =>
        baseRelation.asInstanceOf[HadoopFsRelation]
    }
    assert(hadoopFsRelations.size === 1)
    assert(hadoopFsRelations.head.partitionSchema.exists(_.name == "is_odd"))
    assert(hadoopFsRelations.head.dataSchema.exists(_.name == "value"))

    checkAnswer(df.where("is_odd = true"), Row(1, true) :: Nil)
    checkAnswer(df.where("is_odd IS NULL"), Row(null, null) :: Nil)
  }

  test("input files should be absolute paths") {
    withTempDir { dir =>
      val basePath = dir.getAbsolutePath
      spark.range(10).withColumn("part", 'id % 3)
        .write.format("delta").partitionBy("part").save(basePath)

      val df1 = spark.read.format("delta").load(basePath)
      val df2 = spark.read.format("delta").load(basePath).where("part = 1")
      val df3 = spark.read.format("delta").load(basePath).where("part = 1").limit(3)

      assert(df1.inputFiles.forall(_.contains(basePath)))
      assert(df2.inputFiles.forall(_.contains(basePath)))
      assert(df3.inputFiles.forall(_.contains(basePath)))
    }
  }

  test("invalid replaceWhere") {
    Seq(true, false).foreach { enabled =>
      withSQLConf(DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> enabled.toString) {
        val tempDir = Utils.createTempDir()
        Seq(1, 2, 3, 4).toDF()
          .withColumn("is_odd", $"value" % 2 =!= 0)
          .write
          .format("delta")
          .partitionBy("is_odd")
          .save(tempDir.toString)
        val e1 = intercept[AnalysisException] {
          Seq(6).toDF()
            .withColumn("is_odd", $"value" % 2 =!= 0)
            .write
            .format("delta")
            .mode("overwrite")
            .option(DeltaOptions.REPLACE_WHERE_OPTION, "is_odd = true")
            .save(tempDir.toString)
        }.getMessage
        assert(e1.contains("Data written out does not match replaceWhere"))

        val e2 = intercept[AnalysisException] {
          Seq(true).toDF("is_odd")
            .write
            .format("delta")
            .mode("overwrite")
            .option(DeltaOptions.REPLACE_WHERE_OPTION, "is_odd = true")
            .save(tempDir.toString)
        }.getMessage
        assert(e2.contains(
          "Data written into Delta needs to contain at least one non-partitioned"))

        val e3 = intercept[AnalysisException] {
          Seq(6).toDF()
            .withColumn("is_odd", $"value" % 2 =!= 0)
            .write
            .format("delta")
            .mode("overwrite")
            .option(DeltaOptions.REPLACE_WHERE_OPTION, "not_a_column = true")
            .save(tempDir.toString)
        }.getMessage
        if (enabled) {
          assert(e3.contains(
            "Data written out does not match replaceWhere 'not_a_column = true'"))
        } else {
          assert(e3.contains(
            "Predicate references non-partition column 'not_a_column'. Only the " +
              "partition columns may be referenced: [is_odd]"))
        }

        val e4 = intercept[AnalysisException] {
          Seq(6).toDF()
            .withColumn("is_odd", $"value" % 2 =!= 0)
            .write
            .format("delta")
            .mode("overwrite")
            .option(DeltaOptions.REPLACE_WHERE_OPTION, "value = 1")
            .save(tempDir.toString)
        }.getMessage
        if (enabled) {
          assert(e4.contains("Data written out does not match replaceWhere 'value = 1'"))
        } else {
          assert(e4.contains("Predicate references non-partition column 'value'. Only the " +
            "partition columns may be referenced: [is_odd]"))
        }

        val e5 = intercept[AnalysisException] {
          Seq(6).toDF()
            .withColumn("is_odd", $"value" % 2 =!= 0)
            .write
            .format("delta")
            .mode("overwrite")
            .option(DeltaOptions.REPLACE_WHERE_OPTION, "")
            .save(tempDir.toString)
        }.getMessage
        assert(e5.contains("Cannot recognize the predicate ''"))
      }
    }
  }

  test("replaceWhere with rearrangeOnly") {
    withTempDir { dir =>
      Seq(1, 2, 3, 4).toDF()
        .withColumn("is_odd", $"value" % 2 =!= 0)
        .write
        .format("delta")
        .partitionBy("is_odd")
        .save(dir.toString)

      // dataFilter non empty
      val e = intercept[AnalysisException] {
        Seq(9).toDF()
          .withColumn("is_odd", $"value" % 2 =!= 0)
          .write
          .format("delta")
          .mode("overwrite")
          .option(DeltaOptions.REPLACE_WHERE_OPTION, "is_odd = true and value < 2")
          .option(DeltaOptions.DATA_CHANGE_OPTION, "false")
          .save(dir.toString)
      }.getMessage
      assert(e.contains(
        "'replaceWhere' cannot be used with data filters when 'dataChange' is set to false"))

      Seq(9).toDF()
        .withColumn("is_odd", $"value" % 2 =!= 0)
        .write
        .format("delta")
        .mode("overwrite")
        .option(DeltaOptions.REPLACE_WHERE_OPTION, "is_odd = true")
        .option(DeltaOptions.DATA_CHANGE_OPTION, "false")
        .save(dir.toString)
      checkAnswer(
        spark.read.format("delta").load(dir.toString),
        Seq(2, 4, 9).toDF().withColumn("is_odd", $"value" % 2 =!= 0))
    }
  }

  test("valid replaceWhere") {
    Seq(true, false).foreach { enabled =>
      withSQLConf(DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> enabled.toString) {
        Seq(true, false).foreach { partitioned =>
          // Skip when it's not enabled and not partitioned.
          if (enabled || partitioned) {
            withTempDir { dir =>
              val writer = Seq(1, 2, 3, 4).toDF()
                .withColumn("is_odd", $"value" % 2 =!= 0)
                .withColumn("is_even", $"value" % 2 === 0)
                .write
                .format("delta")

              if (partitioned) {
                writer.partitionBy("is_odd").save(dir.toString)
              } else {
                writer.save(dir.toString)
              }

              def data: DataFrame = spark.read.format("delta").load(dir.toString)

              Seq(5, 7).toDF()
                .withColumn("is_odd", $"value" % 2 =!= 0)
                .withColumn("is_even", $"value" % 2 === 0)
                .write
                .format("delta")
                .mode("overwrite")
                .option(DeltaOptions.REPLACE_WHERE_OPTION, "is_odd = true")
                .save(dir.toString)
              checkAnswer(
                data,
                Seq(2, 4, 5, 7).toDF()
                  .withColumn("is_odd", $"value" % 2 =!= 0)
                  .withColumn("is_even", $"value" % 2 === 0))

              // replaceWhere on non-partitioning columns if enabled.
              if (enabled) {
                Seq(6, 8).toDF()
                  .withColumn("is_odd", $"value" % 2 =!= 0)
                  .withColumn("is_even", $"value" % 2 === 0)
                  .write
                  .format("delta")
                  .mode("overwrite")
                  .option(DeltaOptions.REPLACE_WHERE_OPTION, "is_even = true")
                  .save(dir.toString)
                checkAnswer(
                  data,
                  Seq(5, 6, 7, 8).toDF()
                    .withColumn("is_odd", $"value" % 2 =!= 0)
                    .withColumn("is_even", $"value" % 2 === 0))

                // nothing to be replaced because the condition is false.
                Seq(10, 12).toDF()
                  .withColumn("is_odd", $"value" % 2 =!= 0)
                  .withColumn("is_even", $"value" % 2 === 0)
                  .write
                  .format("delta")
                  .mode("overwrite")
                  .option(DeltaOptions.REPLACE_WHERE_OPTION, "1 = 2")
                  .save(dir.toString)
                checkAnswer(
                  data,
                  Seq(5, 6, 7, 8, 10, 12).toDF()
                    .withColumn("is_odd", $"value" % 2 =!= 0)
                    .withColumn("is_even", $"value" % 2 === 0)
                )

                // replace the whole thing because the condition is true.
                Seq(10, 12).toDF()
                  .withColumn("is_odd", $"value" % 2 =!= 0)
                  .withColumn("is_even", $"value" % 2 === 0)
                  .write
                  .format("delta")
                  .mode("overwrite")
                  .option(DeltaOptions.REPLACE_WHERE_OPTION, "1 = 1")
                  .save(dir.toString)
                checkAnswer(
                  data,
                  Seq(10, 12).toDF()
                    .withColumn("is_odd", $"value" % 2 =!= 0)
                    .withColumn("is_even", $"value" % 2 === 0)
                )
              }
            }
          }
        }
      }
    }
  }

  test("replace arbitrary with multiple references") {
    withTempDir { dir =>
      def data: DataFrame = spark.read.format("delta").load(dir.toString)

      Seq((1, 3, 8), (1, 5, 9)).toDF("a", "b", "c")
        .write
        .format("delta")
        .mode("overwrite")
        .save(dir.toString)

      Seq((2, 4, 6)).toDF("a", "b", "c")
        .write
        .format("delta")
        .mode("overwrite")
        .option(DeltaOptions.REPLACE_WHERE_OPTION, "a + c < 10")
        .save(dir.toString)

      checkAnswer(
        data,
        Seq((1, 5, 9), (2, 4, 6)).toDF("a", "b", "c"))
    }
  }

  test("replaceWhere with constraint check disabled") {
    withSQLConf(DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED.key -> "false") {
      withTempDir { dir =>
        Seq(1, 2, 3, 4).toDF()
          .withColumn("is_odd", $"value" % 2 =!= 0)
          .write
          .format("delta")
          .partitionBy("is_odd")
          .save(dir.toString)

        def data: DataFrame = spark.read.format("delta").load(dir.toString)

        Seq(6).toDF()
          .withColumn("is_odd", $"value" % 2 =!= 0)
          .write
          .format("delta")
          .mode("overwrite")
          .option(DeltaOptions.REPLACE_WHERE_OPTION, "is_odd = true")
          .save(dir.toString)

        checkAnswer(data, Seq(2, 4, 6).toDF().withColumn("is_odd", $"value" % 2 =!= 0))
      }
    }
  }

  test("move delta table") {
    val tempDir = Utils.createTempDir()
    Seq(1, 2, 3).toDS().write.format("delta").mode("append").save(tempDir.toString)

    def data: DataFrame = spark.read.format("delta").load(tempDir.toString)
    checkAnswer(data.toDF(), Row(1) :: Row(2) :: Row(3) :: Nil)

    // Append files in log path should use relative paths and should work with file renaming.
    val targetDir = new File(Utils.createTempDir(), "target")
    assert(tempDir.renameTo(targetDir))

    def data2: DataFrame = spark.read.format("delta").load(targetDir.toString)
    checkDatasetUnorderly(data2.toDF().as[Int], 1, 2, 3)
  }

  test("append table to itself") {
    val tempDir = Utils.createTempDir()
    Seq(1, 2, 3).toDS().write.format("delta").mode("append").save(tempDir.toString)

    def data: DataFrame = spark.read.format("delta").load(tempDir.toString)
    checkDatasetUnorderly(data.toDF.as[Int], 1, 2, 3)
    data.write.format("delta").mode("append").save(tempDir.toString)

    checkDatasetUnorderly(data.toDF.as[Int], 1, 1, 2, 2, 3, 3)
  }

  test("missing partition columns") {
    val tempDir = Utils.createTempDir()
    Seq(1, 2, 3).toDF()
      .withColumn("part", $"value" % 2)
      .write
      .format("delta")
      .partitionBy("part")
      .save(tempDir.toString)

    val e = intercept[Exception] {
      Seq(1, 2, 3).toDF()
        .write
        .format("delta")
        .mode("append")
        .save(tempDir.toString)
    }
    assert(e.getMessage contains "Partition column")
    assert(e.getMessage contains "part")
    assert(e.getMessage contains "not found")
  }

  test("batch write: append, overwrite") {
    withTempDir { tempDir =>
      def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

      Seq(1, 2, 3).toDF
        .write
        .format("delta")
        .mode("append")
        .save(tempDir.getCanonicalPath)
      checkDatasetUnorderly(data.toDF.as[Int], 1, 2, 3)

      Seq(4, 5, 6).toDF
        .write
        .format("delta")
        .mode("overwrite")
        .save(tempDir.getCanonicalPath)
      checkDatasetUnorderly(data.toDF.as[Int], 4, 5, 6)
    }
  }

  test("batch write: overwrite an empty directory with replaceWhere") {
    withTempDir { tempDir =>
      def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

      Seq (1, 3, 5).toDF
        .withColumn("part", $"value" % 2)
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("part")
        .option(DeltaOptions.REPLACE_WHERE_OPTION, "part = 1")
        .save(tempDir.getCanonicalPath)
      checkDatasetUnorderly(data.toDF.as[(Int, Int)], 1 -> 1, 3 -> 1, 5 -> 1)
    }
  }

  test("batch write: append, overwrite where") {
    withTempDir { tempDir =>
      def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

      Seq (1, 2, 3).toDF
        .withColumn("part", $"value" % 2)
        .write
        .format("delta")
        .partitionBy("part")
        .mode("append")
        .save(tempDir.getCanonicalPath)

      Seq(1, 5).toDF
        .withColumn("part", $"value" % 2)
        .write
        .format("delta")
        .partitionBy("part")
        .mode("overwrite")
        .option(DeltaOptions.REPLACE_WHERE_OPTION, "part=1")
        .save(tempDir.getCanonicalPath)
      checkDatasetUnorderly(data.toDF.select($"value".as[Int]), 1, 2, 5)
    }
  }

  test("batch write: ignore") {
    withTempDir { tempDir =>
      def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

      Seq(1, 2, 3).toDF
        .write
        .format("delta")
        .mode("ignore")
        .save(tempDir.getCanonicalPath)
      checkDatasetUnorderly(data.toDF.as[Int], 1, 2, 3)

      // The following data will be ignored
      Seq(4, 5, 6).toDF
        .write
        .format("delta")
        .mode("ignore")
        .save(tempDir.getCanonicalPath)
      checkDatasetUnorderly(data.toDF.as[Int], 1, 2, 3)
    }
  }

  test("batch write: error") {
    withTempDir { tempDir =>
      def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

      Seq(1, 2, 3).toDF
        .write
        .format("delta")
        .mode("error")
        .save(tempDir.getCanonicalPath)
      checkDatasetUnorderly(data.toDF.as[Int], 1, 2, 3)

      val e = intercept[AnalysisException] {
        Seq(4, 5, 6).toDF
          .write
          .format("delta")
          .mode("error")
          .save(tempDir.getCanonicalPath)
      }
      assert(e.getMessage.contains("already exists"))
    }
  }

  testQuietly("creating log should not create the log directory") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }
      val log = DeltaLog.forTable(spark, tempDir)

      // Creating an empty log should not create the directory
      assert(!tempDir.exists())

      // Writing to table should create the directory
      Seq(1, 2, 3).toDF
        .write
        .format("delta")
        .save(tempDir.getCanonicalPath)

      def data: DataFrame = spark.read.format("delta").load(tempDir.toString)
      checkDatasetUnorderly(data.toDF.as[Int], 1, 2, 3)
    }
  }

  test("read via data source API when the directory doesn't exist") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }

      // a batch query should fail at once
      var e = intercept[AnalysisException] {
        spark.read
          .format("delta")
          .load(tempDir.getCanonicalPath)
          .show()
      }

      assert(e.getMessage.contains("is not a Delta table"))
      assert(e.getMessage.contains(tempDir.getCanonicalPath))

      assert(!tempDir.exists())

      // a streaming query will also fail but it's because there is no schema
      e = intercept[AnalysisException] {
        spark.readStream
          .format("delta")
          .load(tempDir.getCanonicalPath)
      }
      assert(e.getMessage.contains("Table schema is not set"))
      assert(e.getMessage.contains("CREATE TABLE"))
    }
  }

  test("write via data source API when the directory doesn't exist") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }

      // a batch query should create the output directory automatically
      Seq(1, 2, 3).toDF
        .write
        .format("delta").save(tempDir.getCanonicalPath)
      checkDatasetUnorderly(
        spark.read.format("delta").load(tempDir.getCanonicalPath).as[Int],
        1, 2, 3)

      Utils.deleteRecursively(tempDir)
      assert(!tempDir.exists())

      // a streaming query should create the output directory automatically
      val input = MemoryStream[Int]
      val q = input.toDF
        .writeStream
        .format("delta")
        .option(
          "checkpointLocation",
          Utils.createTempDir(namePrefix = "tahoe-test").getCanonicalPath)
        .start(tempDir.getCanonicalPath)
      try {
        input.addData(1, 2, 3)
        q.processAllAvailable()
        checkDatasetUnorderly(
          spark.read.format("delta").load(tempDir.getCanonicalPath).as[Int],
          1, 2, 3)
      } finally {
        q.stop()
      }
    }
  }

  test("support partitioning with batch data source API - append") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }

      spark.range(100).select('id, 'id % 4 as 'by4, 'id % 8 as 'by8)
        .write
        .format("delta")
        .partitionBy("by4", "by8")
        .save(tempDir.toString)

      val files = spark.read.format("delta").load(tempDir.toString).inputFiles

      val deltaLog = loadDeltaLog(tempDir.getAbsolutePath)
      assertPartitionExists("by4", deltaLog, files)
      assertPartitionExists("by8", deltaLog, files)
    }
  }

  test("support removing partitioning") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }

      spark.range(100).select('id, 'id % 4 as 'by4)
        .write
        .format("delta")
        .partitionBy("by4")
        .save(tempDir.toString)

      val deltaLog = DeltaLog.forTable(spark, tempDir)
      assert(deltaLog.snapshot.metadata.partitionColumns === Seq("by4"))

      spark.read.format("delta").load(tempDir.toString).write
        .option(DeltaOptions.OVERWRITE_SCHEMA_OPTION, "true")
        .format("delta")
        .mode(SaveMode.Overwrite)
        .save(tempDir.toString)

      assert(deltaLog.snapshot.metadata.partitionColumns === Nil)
    }
  }


  test("throw exception when users are trying to write in batch with different partitioning") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }

      spark.range(100).select('id, 'id % 4 as 'by4, 'id % 8 as 'by8)
        .write
        .format("delta")
        .partitionBy("by4", "by8")
        .save(tempDir.toString)

      val e = intercept[AnalysisException] {
        spark.range(100).select('id, 'id % 4 as 'by4)
          .write
          .format("delta")
          .partitionBy("by4")
          .mode("append")
          .save(tempDir.toString)
      }
      assert(e.getMessage.contains("Partition columns do not match"))
    }
  }

  test("incompatible schema merging throws errors") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }

      spark.range(100).select('id, ('id * 3).cast("string") as 'value)
        .write
        .format("delta")
        .save(tempDir.toString)

      val e = intercept[AnalysisException] {
        spark.range(100).select('id, 'id * 3 as 'value)
          .write
          .format("delta")
          .mode("append")
          .save(tempDir.toString)
      }
      assert(e.getMessage.contains("incompatible"))
    }
  }

  test("support partitioning with batch data source API - overwrite") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }

      spark.range(100).select('id, 'id % 4 as 'by4)
        .write
        .format("delta")
        .partitionBy("by4")
        .save(tempDir.toString)

      val files = spark.read.format("delta").load(tempDir.toString).inputFiles

      assert(files.forall(path => path.contains("by4=")),
        s"${files.toSeq.mkString("\n")}\ndidn't contain partition columns by4")

      spark.range(101, 200).select('id, 'id % 4 as 'by4, 'id % 8 as 'by8)
        .write
        .format("delta")
        .option(DeltaOptions.MERGE_SCHEMA_OPTION, "true")
        .mode("overwrite")
        .save(tempDir.toString)

      checkAnswer(
        spark.read.format("delta").load(tempDir.toString),
        spark.range(101, 200).select('id, 'id % 4 as 'by4, 'id % 8 as 'by8))
    }
  }

  test("overwrite and replaceWhere should check partitioning compatibility") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }

      spark.range(100).select('id, 'id % 4 as 'by4)
        .write
        .format("delta")
        .partitionBy("by4")
        .save(tempDir.toString)

      val files = spark.read.format("delta").load(tempDir.toString).inputFiles

      val deltaLog = loadDeltaLog(tempDir.getAbsolutePath)
      assertPartitionExists("by4", deltaLog, files)

      val e = intercept[AnalysisException] {
        spark.range(101, 200).select('id, 'id % 4 as 'by4, 'id % 8 as 'by8)
          .write
          .format("delta")
          .partitionBy("by4", "by8")
          .option(DeltaOptions.REPLACE_WHERE_OPTION, "by4 > 0")
          .mode("overwrite")
          .save(tempDir.toString)
      }
      assert(e.getMessage.contains("Partition columns do not match"))
    }
  }

  test("can't write out with all columns being partition columns") {
    withTempDir { tempDir =>
      SaveMode.values().foreach { mode =>
        if (tempDir.exists()) {
          assert(tempDir.delete())
        }

        val e = intercept[AnalysisException] {
          spark.range(100).select('id, 'id % 4 as 'by4)
            .write
            .format("delta")
            .partitionBy("by4", "id")
            .mode(mode)
            .save(tempDir.toString)
        }
        assert(e.getMessage.contains("Cannot use all columns for partition columns"))
      }
    }
  }

  test("SC-8727 - default snapshot num partitions") {
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.toString)
      val deltaLog = DeltaLog.forTable(spark, tempDir)
      val numParts = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_SNAPSHOT_PARTITIONS).get
      assert(deltaLog.snapshot.stateDS.rdd.getNumPartitions == numParts)
    }
  }

  test("SC-8727 - can't set negative num partitions") {
    withTempDir { tempDir =>
      val caught = intercept[IllegalArgumentException] {
        withSQLConf(("spark.databricks.delta.snapshotPartitions", "-1")) {}
      }

      assert(caught.getMessage.contains("Delta snapshot partition number must be positive."))
    }
  }

  test("SC-8727 - reconfigure num partitions") {
    withTempDir { tempDir =>
      withSQLConf(("spark.databricks.delta.snapshotPartitions", "410")) {
        spark.range(10).write.format("delta").save(tempDir.toString)
        val deltaLog = DeltaLog.forTable(spark, tempDir)
        assert(deltaLog.snapshot.stateDS.rdd.getNumPartitions == 410)
      }
    }
  }

  test("SC-8727 - can't set zero num partitions") {
    withTempDir { tempDir =>
      val caught = intercept[IllegalArgumentException] {
        withSQLConf(("spark.databricks.delta.snapshotPartitions", "0")) {}
      }

      assert(caught.getMessage.contains("Delta snapshot partition number must be positive."))
    }
  }

  testQuietly("SC-8810: skip deleted file") {
    withSQLConf(("spark.sql.files.ignoreMissingFiles", "true")) {
      withTempDir { tempDir =>
        val tempDirPath = new Path(tempDir.getCanonicalPath)
        Seq(1).toDF().write.format("delta").mode("append").save(tempDir.toString)
        Seq(2, 2).toDF().write.format("delta").mode("append").save(tempDir.toString)
        Seq(4).toDF().write.format("delta").mode("append").save(tempDir.toString)
        Seq(5).toDF().write.format("delta").mode("append").save(tempDir.toString)

        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)
        val deltaLog = DeltaLog.forTable(spark, tempDir)

        // The file names are opaque. To identify which one we're deleting, we ensure that only one
        // append has 2 partitions, and give them the same value so we know what was deleted.
        val inputFiles = TahoeLogFileIndex(spark, deltaLog).inputFiles.toSeq
        assert(inputFiles.size == 5)

        val filesToDelete = inputFiles.filter(_.split("/").last.startsWith("part-00001"))
        assert(filesToDelete.size == 1)
        filesToDelete.foreach { f =>
          val deleted = tryDeleteNonRecursive(
            tempDirPath.getFileSystem(deltaLog.newDeltaHadoopConf()),
            new Path(tempDirPath, f))
          assert(deleted)
        }

        // The single 2 that we deleted should be missing, with the rest of the data still present.
        checkAnswer(data.toDF(), Row(1) :: Row(2) :: Row(4) :: Row(5) :: Nil)
      }
    }
  }

  testQuietly("SC-8810: skipping deleted file still throws on corrupted file") {
    withSQLConf(("spark.sql.files.ignoreMissingFiles", "true")) {
      withTempDir { tempDir =>
        val tempDirPath = new Path(tempDir.getCanonicalPath)
        Seq(1).toDF().write.format("delta").mode("append").save(tempDir.toString)
        Seq(2, 2).toDF().write.format("delta").mode("append").save(tempDir.toString)
        Seq(4).toDF().write.format("delta").mode("append").save(tempDir.toString)
        Seq(5).toDF().write.format("delta").mode("append").save(tempDir.toString)

        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)
        val deltaLog = DeltaLog.forTable(spark, tempDir)

        // The file names are opaque. To identify which one we're deleting, we ensure that only one
        // append has 2 partitions, and give them the same value so we know what was deleted.
        val inputFiles = TahoeLogFileIndex(spark, deltaLog).inputFiles.toSeq
        assert(inputFiles.size == 5)

        val filesToCorrupt = inputFiles.filter(_.split("/").last.startsWith("part-00001"))
        assert(filesToCorrupt.size == 1)
        val fs = tempDirPath.getFileSystem(deltaLog.newDeltaHadoopConf())
        filesToCorrupt.foreach { f =>
          val filePath = new Path(tempDirPath, f)
          fs.create(filePath, true).close()
        }

        val thrown = intercept[SparkException] {
          data.toDF().count()
        }
        assert(thrown.getMessage.contains("is not a Parquet file"))
      }
    }
  }

  testQuietly("SC-8810: skip multiple deleted files") {
    withSQLConf(("spark.sql.files.ignoreMissingFiles", "true")) {
      withTempDir { tempDir =>
        val tempDirPath = new Path(tempDir.getCanonicalPath)
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)
        val deltaLog = DeltaLog.forTable(spark, tempDir)

        Range(0, 10).foreach(n =>
          Seq(n).toDF().write.format("delta").mode("append").save(tempDir.toString))

        val inputFiles = TahoeLogFileIndex(spark, deltaLog).inputFiles.toSeq

        val filesToDelete = inputFiles.take(4)
        filesToDelete.foreach { f =>
          val deleted = tryDeleteNonRecursive(
            tempDirPath.getFileSystem(deltaLog.newDeltaHadoopConf()),
            new Path(tempDirPath, f))
          assert(deleted)
        }

        // We don't have a good way to tell which specific values got deleted, so just check that
        // the right number remain. (Note that this works because there's 1 value per append, which
        // means 1 value per file.)
        assert(data.toDF().count() == 6)
      }
    }
  }

  test("deleted files cause failure by default") {
    withTempDir { tempDir =>
      val tempDirPath = new Path(tempDir.getCanonicalPath)
      def data: DataFrame = spark.read.format("delta").load(tempDir.toString)
      val deltaLog = DeltaLog.forTable(spark, tempDir)

      Range(0, 10).foreach(n =>
        Seq(n).toDF().write.format("delta").mode("append").save(tempDir.toString))

      val inputFiles = TahoeLogFileIndex(spark, deltaLog).inputFiles.toSeq

      val filesToDelete = inputFiles.take(4)
      filesToDelete.foreach { f =>
        val deleted = tryDeleteNonRecursive(
          tempDirPath.getFileSystem(deltaLog.newDeltaHadoopConf()),
          new Path(tempDirPath, f))
        assert(deleted)
      }

      val thrown = intercept[SparkException] {
        data.toDF().count()
      }
      assert(thrown.getMessage.contains("FileNotFound"))
    }
  }

  test("ES-4716: Delta shouldn't be broken when users turn on case sensitivity") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir { tempDir =>
        // We use a column with the weird name just to make sure that customer configurations still
        // work. The original bug was within the `Snapshot` code, where we referred to `metaData`
        // as `metadata`.
        Seq(1, 2, 3).toDF("aBc").write.format("delta").mode("append").save(tempDir.toString)

        def testDf(columnName: Symbol): Unit = {
          DeltaLog.clearCache()
          val df = spark.read.format("delta").load(tempDir.getCanonicalPath).select(columnName)
          checkDatasetUnorderly(df.as[Int], 1, 2, 3)
        }

        withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
          testDf('aBc)

          intercept[AnalysisException] {
            testDf('abc)
          }
        }
        testDf('aBc)
        testDf('abc)
      }
    }
  }

  test("special chars in base path") {
    withTempDir { dir =>
      val basePath = new File(new File(dir, "some space"), "and#spec*al+ch@rs")
      spark.range(10).write.format("delta").save(basePath.getCanonicalPath)
      checkAnswer(
        spark.read.format("delta").load(basePath.getCanonicalPath),
        spark.range(10).toDF()
      )
    }
  }

  test("get touched files for update, delete and merge") {
    withTempDir { dir =>
      val directory = new File(dir, "test with space")
      val df = Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value")
      val writer = df.write.format("delta").mode("append")
      writer.save(directory.getCanonicalPath)
      spark.sql(s"UPDATE delta.`${directory.getCanonicalPath}` SET value = value + 10")
      spark.sql(s"DELETE FROM delta.`${directory.getCanonicalPath}` WHERE key = 4")
      Seq((3, 30)).toDF("key", "value").createOrReplaceTempView("inbound")
      spark.sql(s"""|MERGE INTO delta.`${directory.getCanonicalPath}` AS base
                       |USING inbound
                       |ON base.key = inbound.key
                       |WHEN MATCHED THEN UPDATE SET base.value =
                       |base.value+inbound.value""".stripMargin)
      spark.sql(s"UPDATE delta.`${directory.getCanonicalPath}` SET value = 40 WHERE key = 1")
      spark.sql(s"DELETE FROM delta.`${directory.getCanonicalPath}` WHERE key = 2")
      checkAnswer(
        spark.read.format("delta").load(directory.getCanonicalPath),
        Seq((1, 40), (3, 70)).toDF("key", "value")
      )
    }
  }

  test("can't create zero-column table with a write") {
    withTempDir { dir =>
      intercept[AnalysisException] {
        Seq(1).toDF("a").drop("a").write.format("delta").save(dir.getAbsolutePath)
      }
    }
  }

  test("SC-10573: InSet operator prunes partitions properly") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      Seq((1, 1L, "1")).toDS()
        .write
        .format("delta")
        .partitionBy("_2", "_3")
        .save(path)
      val df = spark.read.format("delta").load(path)
        .where("_2 IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)").select("_1")
      val condition = df.queryExecution.optimizedPlan.collectFirst {
        case f: Filter => f.condition
      }
      assert(condition.exists(_.isInstanceOf[InSet]))
      checkAnswer(df, Row(1))
    }
  }

  test("SC-24886: partition columns have correct datatype in metadata scans") {
    withTempDir { inputDir =>
      Seq(("foo", 2019)).toDF("name", "y")
        .write.format("delta").partitionBy("y").mode("overwrite")
        .save(inputDir.getAbsolutePath)

      // Before the fix, this query would fail because it tried to read strings from the metadata
      // partition values as the LONG type that the actual partition columns are. This works now
      // because we added a cast.
      val df = spark.read.format("delta")
        .load(inputDir.getAbsolutePath)
        .where(
          """cast(format_string("%04d-01-01 12:00:00", y) as timestamp) is not null""".stripMargin)
      assert(df.collect().length == 1)
    }
  }

  test("SC-11332: session isolation for cached delta logs") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      val oldSession = spark
      val deltaLog = DeltaLog.forTable(spark, path)
      val maxSLL = deltaLog.maxSnapshotLineageLength

      val activeSession = oldSession.newSession()
      SparkSession.setActiveSession(activeSession)
      activeSession.sessionState.conf.setConf(
        DeltaSQLConf.DELTA_MAX_SNAPSHOT_LINEAGE_LENGTH, maxSLL + 1)

      // deltaLog fetches conf from active session
      assert(deltaLog.maxSnapshotLineageLength == maxSLL + 1)

      // new session confs don't propagate to old session
      assert(maxSLL ==
        oldSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_MAX_SNAPSHOT_LINEAGE_LENGTH))
    }
  }

  test("SC-11198: global configs - save to path") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      withSQLConf("spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols" -> "1") {
        spark.range(5).write.format("delta").save(path)

        val tableConfigs = DeltaLog.forTable(spark, path).update().metadata.configuration
        assert(tableConfigs.get("delta.dataSkippingNumIndexedCols") == Some("1"))
      }
    }
  }

  test("SC-24982 - initial snapshot has zero partitions") {
    withTempDir { tempDir =>
      val deltaLog = DeltaLog.forTable(spark, tempDir)
      assert(deltaLog.snapshot.stateDS.rdd.getNumPartitions == 0)
    }
  }

  test("SC-24982 - initial snapshot does not trigger jobs") {
    val jobCount = new AtomicInteger(0)
    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        // Spark will always log a job start/end event even when the job does not launch any task.
        if (jobStart.stageInfos.exists(_.numTasks > 0)) {
          jobCount.incrementAndGet()
        }
      }
    }
    sparkContext.listenerBus.waitUntilEmpty(15000)
    sparkContext.addSparkListener(listener)
    try {
      withTempDir { tempDir =>
        val files = DeltaLog.forTable(spark, tempDir).snapshot.stateDS.collect()
        assert(files.isEmpty)
      }
      sparkContext.listenerBus.waitUntilEmpty(15000)
      assert(jobCount.get() == 0)
    } finally {
      sparkContext.removeSparkListener(listener)
    }
  }

  def lastDeltaHistory(dir: String): DeltaHistory =
    io.delta.tables.DeltaTable.forPath(spark, dir).history(1).as[DeltaHistory].head

  test("history includes user-defined metadata for DataFrame.Write API") {
    val tempDir = Utils.createTempDir().toString
    val df = Seq(2).toDF().write.format("delta").mode("overwrite")

    df.option("userMetadata", "meta1")
      .save(tempDir)

    assert(lastDeltaHistory(tempDir).userMetadata === Some("meta1"))

    df.option("userMetadata", "meta2")
      .save(tempDir)

    assert(lastDeltaHistory(tempDir).userMetadata === Some("meta2"))
  }

  test("history includes user-defined metadata for SQL API") {
    val tempDir = Utils.createTempDir().toString
    val tblName = "tblName"

    withTable(tblName) {
      withSQLConf(DeltaSQLConf.DELTA_USER_METADATA.key -> "meta1") {
        spark.sql(s"CREATE TABLE $tblName (data STRING) USING delta LOCATION '$tempDir';")
      }
      assert(lastDeltaHistory(tempDir).userMetadata === Some("meta1"))

      withSQLConf(DeltaSQLConf.DELTA_USER_METADATA.key -> "meta2") {
        spark.sql(s"INSERT INTO $tblName VALUES ('test');")
      }
      assert(lastDeltaHistory(tempDir).userMetadata === Some("meta2"))

      withSQLConf(DeltaSQLConf.DELTA_USER_METADATA.key -> "meta3") {
        spark.sql(s"INSERT INTO $tblName VALUES ('test2');")
      }
      assert(lastDeltaHistory(tempDir).userMetadata === Some("meta3"))
    }
  }

  test("history includes user-defined metadata for DF.Write API and config setting") {
    val tempDir = Utils.createTempDir().toString
    val df = Seq(2).toDF().write.format("delta").mode("overwrite")

    withSQLConf(DeltaSQLConf.DELTA_USER_METADATA.key -> "meta1") {
      df.save(tempDir)
    }
    assert(lastDeltaHistory(tempDir).userMetadata === Some("meta1"))

    withSQLConf(DeltaSQLConf.DELTA_USER_METADATA.key -> "meta2") {
      df.option("userMetadata", "optionMeta2")
        .save(tempDir)
    }
    assert(lastDeltaHistory(tempDir).userMetadata === Some("optionMeta2"))
  }

  test("history includes user-defined metadata for SQL + DF.Write API") {
    val tempDir = Utils.createTempDir().toString
    val df = Seq(2).toDF().write.format("delta").mode("overwrite")

    // metadata given in `option` should beat config
    withSQLConf(DeltaSQLConf.DELTA_USER_METADATA.key -> "meta1") {
      df.option("userMetadata", "optionMeta1")
        .save(tempDir)
    }
    assert(lastDeltaHistory(tempDir).userMetadata === Some("optionMeta1"))

    withSQLConf(DeltaSQLConf.DELTA_USER_METADATA.key -> "meta2") {
      df.option("userMetadata", "optionMeta2")
        .save(tempDir)
    }
    assert(lastDeltaHistory(tempDir).userMetadata === Some("optionMeta2"))
  }

  test("SC-77958 - history includes user-defined metadata for createOrReplace") {
    withTable("tbl") {
      spark.range(10).writeTo("tbl").using("delta").option("userMetadata", "meta").createOrReplace()

      val history = sql("DESCRIBE HISTORY tbl LIMIT 1").as[DeltaHistory].head()
      assert(history.userMetadata === Some("meta"))
    }
  }

  test("SC-77958 - history includes user-defined metadata for saveAsTable") {
    withTable("tbl") {
      spark.range(10).write.format("delta").option("userMetadata", "meta1")
        .mode("overwrite").saveAsTable("tbl")

      val history = sql("DESCRIBE HISTORY tbl LIMIT 1").as[DeltaHistory].head()
      assert(history.userMetadata === Some("meta1"))
    }
  }

  test("lastCommitVersionInSession - init") {
    spark.sessionState.conf.unsetConf(DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION)
    withTempDir { tempDir =>

      assert(spark.conf.get(DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION) === None)

      Seq(1).toDF
        .write
        .format("delta")
        .save(tempDir.getCanonicalPath)

      assert(spark.conf.get(DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION) === Some(0))
    }
  }

  test("lastCommitVersionInSession - SQL") {
    spark.sessionState.conf.unsetConf(DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION)
    withTempDir { tempDir =>

      val k = DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION.key
      assert(sql(s"SET $k").head().get(1) === "<undefined>")

      Seq(1).toDF
        .write
        .format("delta")
        .save(tempDir.getCanonicalPath)

      assert(sql(s"SET $k").head().get(1) === "0")
    }
  }

  test("lastCommitVersionInSession - SQL only") {
    spark.sessionState.conf.unsetConf(DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION)
    withTable("test_table") {
      val k = DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION.key
      assert(sql(s"SET $k").head().get(1) === "<undefined>")

      sql("CREATE TABLE test_table USING delta AS SELECT * FROM range(10)")
      assert(sql(s"SET $k").head().get(1) === "0")
    }
  }

  test("lastCommitVersionInSession - CONVERT TO DELTA") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath + "/table"
      spark.range(10).write.format("parquet").save(path)
      sql(s"CONVERT TO DELTA parquet.`$path`")

      assert(spark.conf.get(DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION) === Some(0))
    }
  }

  test("lastCommitVersionInSession - many writes") {
    withTempDir { tempDir =>

      for (i <- 0 until 10) {
        Seq(i).toDF
          .write
          .mode("overwrite")
          .format("delta")
          .save(tempDir.getCanonicalPath)
      }

      Seq(10).toDF
        .write
        .format("delta")
        .mode("append")
        .save(tempDir.getCanonicalPath)

      assert(spark.conf.get(DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION) === Some(10))
    }
  }

  test("lastCommitVersionInSession - new thread writes") {
    withTempDir { tempDir =>

      Seq(1).toDF
        .write
        .format("delta")
        .mode("overwrite")
        .save(tempDir.getCanonicalPath)

      val t = new Thread {
        override def run(): Unit = {
          Seq(2).toDF
            .write
            .format("delta")
            .mode("overwrite")
            .save(tempDir.getCanonicalPath)
        }
      }

      t.start
      t.join
      assert(spark.conf.get(DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION) === Some(1))
    }
  }

  test("change data capture not implemented") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(id INT) USING DELTA")
      val ex = intercept[AnalysisException] {
        sql(s"ALTER TABLE tbl SET TBLPROPERTIES (${DeltaConfigs.CHANGE_DATA_FEED.key} = true)")
      }

      assert(ex.getMessage.contains("Configuration delta.enableChangeDataFeed cannot be set"))
    }
  }

  test("change data capture write not implemented") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.range(10).write.format("delta").save(path)

      // Side channel since the config can't normally be set.
      val log = DeltaLog.forTable(spark, path)
      log.store.write(
        deltaFile(log.logPath, 1),
        Iterator(log.snapshot.metadata.copy(
          configuration = Map(DeltaConfigs.CHANGE_DATA_FEED.key -> "true")).json),
        overwrite = false,
        log.newDeltaHadoopConf())
      log.update()

      val ex = intercept[AnalysisException] {
        spark.range(10).write.mode("append").format("delta").save(path)
      }

      assert(ex.getMessage.contains("Cannot write to table with delta.enableChangeDataFeed set"))
    }
  }

  test("An external write should be reflected during analysis of a path based query") {
    val tempDir = Utils.createTempDir().toString
    spark.range(10).coalesce(1).write.format("delta").mode("append").save(tempDir)
    spark.range(10, 20).coalesce(1).write.format("delta").mode("append").save(tempDir)

    val deltaLog = DeltaLog.forTable(spark, tempDir)
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    val snapshot = deltaLog.snapshot
    val files = snapshot.allFiles.collect()

    // assign physical name to new schema
    val newMetadata = if (columnMappingEnabled) {
      DeltaColumnMapping.assignColumnIdAndPhysicalName(
        snapshot.metadata.copy(schemaString = new StructType().add("data", "bigint").json),
        snapshot.metadata,
        isChangingModeOnExistingTable = false)
    } else {
      snapshot.metadata.copy(schemaString = new StructType().add("data", "bigint").json)
    }

    // Now make a commit that comes from an "external" writer that deletes existing data and
    // changes the schema
    val actions = Seq(Protocol(), newMetadata) ++ files.map(_.remove)
    deltaLog.store.write(
      FileNames.deltaFile(deltaLog.logPath, snapshot.version + 1),
      actions.map(_.json).iterator,
      overwrite = false,
      hadoopConf)

    deltaLog.store.write(
      FileNames.deltaFile(deltaLog.logPath, snapshot.version + 2),
      files.take(1).map(_.json).iterator,
      overwrite = false,
      hadoopConf)

    // Since the column `data` doesn't exist in our old files, we read it as null.
    checkAnswer(
      spark.read.format("delta").load(tempDir),
      Seq.fill(10)(Row(null))
    )
  }

  test("isBlindAppend with save and saveAsTable") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      withTable("blind_append") {
        sql(s"CREATE TABLE blind_append(value INT) USING delta LOCATION '$path'") // version = 0
        sql("INSERT INTO blind_append VALUES(1)") // version = 1
        spark.read.format("delta").load(path)
          .where("value = 1")
          .write.mode("append").format("delta").save(path) // version = 2
        checkAnswer(spark.table("blind_append"), Row(1) :: Row(1) :: Nil)
        assert(sql("desc history blind_append")
          .select("version", "isBlindAppend").head == Row(2, false))
        spark.table("blind_append").where("value = 1").write.mode("append").format("delta")
          .saveAsTable("blind_append") // version = 3
        checkAnswer(spark.table("blind_append"), Row(1) :: Row(1) :: Row(1) :: Row(1) :: Nil)
        assert(sql("desc history blind_append")
          .select("version", "isBlindAppend").head == Row(3, false))
      }
    }
  }

  test("isBlindAppend with DataFrameWriterV2") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      withTable("blind_append") {
        sql(s"CREATE TABLE blind_append(value INT) USING delta LOCATION '$path'") // version = 0
        sql("INSERT INTO blind_append VALUES(1)") // version = 1
        spark.read.format("delta").load(path)
          .where("value = 1")
          .writeTo("blind_append").append() // version = 2
        checkAnswer(spark.table("blind_append"), Row(1) :: Row(1) :: Nil)
        assert(sql("desc history blind_append")
          .select("version", "isBlindAppend").head == Row(2, false))
      }
    }
  }

  test("isBlindAppend with RTAS") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      withTable("blind_append") {
        sql(s"CREATE TABLE blind_append(value INT) USING delta LOCATION '$path'") // version = 0
        sql("INSERT INTO blind_append VALUES(1)") // version = 1
        sql("REPLACE TABLE blind_append USING delta AS SELECT * FROM blind_append") // version = 2
        checkAnswer(spark.table("blind_append"), Row(1) :: Nil)
        assert(sql("desc history blind_append")
          .select("version", "isBlindAppend").head == Row(2, false))
      }
    }
  }

  test("replaceWhere should support backtick when flag is disabled") {
    val table = "replace_where_backtick"
    withSQLConf(DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> "false") {
      withTable(table) {
        // The STRUCT column is added to prevent us from introducing any ambiguity in future
        sql(s"CREATE TABLE $table(`a.b` STRING, `c.d` STRING, a STRUCT<b:STRING>)" +
          s"USING delta PARTITIONED BY (`a.b`)")
        Seq(("a", "b", "c"))
          .toDF("a.b", "c.d", "ab")
          .withColumn("a", struct($"ab".alias("b")))
          .drop("ab")
          .write
          .format("delta")
          // "replaceWhere" should support backtick and remove it correctly. Technically,
          // "a.b" is not correct, but some users may already use it,
          // so we keep supporting both. This is not ambiguous since "replaceWhere" only
          // supports partition columns and it doesn't support struct type or map type.
          .option("replaceWhere", "`a.b` = 'a' AND a.b = 'a'")
          .mode("overwrite")
          .saveAsTable(table)
        checkAnswer(sql(s"SELECT `a.b`, `c.d`, a.b from $table"), Row("a", "b", "c") :: Nil)
      }
    }
  }

  test("replaceArbitrary should enforce proper usage of backtick") {
    val table = "replace_where_backtick"
    withTable(table) {
      sql(s"CREATE TABLE $table(`a.b` STRING, `c.d` STRING, a STRUCT<b:STRING>)" +
        s"USING delta PARTITIONED BY (`a.b`)")

      // User has to use backtick properly. If they want to use a.b to match on `a.b`,
      // error will be thrown if `a.b` doesn't have the value.
      val e = intercept[AnalysisException] {
        Seq(("a", "b", "c"))
          .toDF("a.b", "c.d", "ab")
          .withColumn("a", struct($"ab".alias("b")))
          .drop("ab")
          .write
          .format("delta")
          .option("replaceWhere", "a.b = 'a' AND `a.b` = 'a'")
          .mode("overwrite")
          .saveAsTable(table)
      }
      assert(e.getMessage.startsWith("Data written out does not match replaceWhere"))

      Seq(("a", "b", "c"), ("d", "e", "f"))
        .toDF("a.b", "c.d", "ab")
        .withColumn("a", struct($"ab".alias("b")))
        .drop("ab")
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table)

      // Use backtick properly for `a.b`
      Seq(("a", "h", "c"))
        .toDF("a.b", "c.d", "ab")
        .withColumn("a", struct($"ab".alias("b")))
        .drop("ab")
        .write
        .format("delta")
        .option("replaceWhere", "`a.b` = 'a'")
        .mode("overwrite")
        .saveAsTable(table)

      checkAnswer(sql(s"SELECT `a.b`, `c.d`, a.b from $table"),
        Row("a", "h", "c") :: Row("d", "e", "f") :: Nil)

      // struct field can only be referred by "a.b".
      Seq(("a", "b", "c"))
        .toDF("a.b", "c.d", "ab")
        .withColumn("a", struct($"ab".alias("b")))
        .drop("ab")
        .write
        .format("delta")
        .option("replaceWhere", "a.b = 'c'")
        .mode("overwrite")
        .saveAsTable(table)
      checkAnswer(sql(s"SELECT `a.b`, `c.d`, a.b from $table"),
        Row("a", "b", "c") :: Row("d", "e", "f") :: Nil)
    }
  }

  test("need to update DeltaLog on DataFrameReader.load() code path") {
    // Due to possible race conditions (like in mounting/unmounting paths) there might be an initial
    // snapshot that gets cached for a table that should have a valid (non-initial) snapshot. In
    // such a case we need to call deltaLog.update() in the DataFrame read paths to update the
    // initial snapshot to a valid one.
    //
    // We simulate a cached InitialSnapshot + valid delta table by creating an empty DeltaLog
    // (which creates an InitialSnapshot cached for that path) then move an actual Delta table's
    // transaction log into the path for the empty log.
    val dir1 = Utils.createTempDir()
    val dir2 = Utils.createTempDir()
    val log = DeltaLog.forTable(spark, dir1)
    assert(!log.tableExists)
    spark.range(10).write.format("delta").save(dir2.getCanonicalPath)
    // rename dir2 to dir1 then read
    dir2.renameTo(dir1)
    checkAnswer(spark.read.format("delta").load(dir1.getCanonicalPath), spark.range(10).toDF)
  }
}

