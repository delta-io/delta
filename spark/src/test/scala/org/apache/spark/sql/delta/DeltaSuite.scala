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
import org.apache.spark.sql.delta.actions.{Action, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{DeltaFileOperations, FileNames}
import org.apache.spark.sql.delta.util.FileNames.deltaFile
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkException
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.InSet
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{asc, col, expr, lit, map_values, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.Utils

class DeltaSuite extends QueryTest
  with SharedSparkSession  with DeltaColumnMappingTestUtils  with SQLTestUtils
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
    assert(e2.contains("Path does not exist"))
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
    assert(e.contains("Path does not exist"))
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
            "A column or function parameter with name `not_a_column` cannot be resolved") ||
            e3.contains("Column 'not_a_column' does not exist. Did you mean one of " +
              "the following? [value, is_odd]"))
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

    Seq(false, true).foreach { replaceWhereInDataColumn =>
      test(s"valid replaceWhere with cdf enabled, " +
        s"replaceWhereInDataColumn = $replaceWhereInDataColumn") {
        testReplaceWhereWithCdf(
          replaceWhereInDataColumn)
      }
    }

  def testReplaceWhereWithCdf(
    replaceWhereInDataColumn: Boolean): Unit = {
      withSQLConf(
        DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> replaceWhereInDataColumn.toString,
        DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
        withTempDir { dir =>
          Seq(1, 2, 3, 4).map(i => (i, i + 2)).toDF("key", "value.1")
            .withColumn("is_odd", $"`value.1`" % 2 =!= 0)
            .withColumn("is_even", $"`value.1`" % 2 === 0)
            .coalesce(1)
            .write
            .format("delta")
            .partitionBy("is_odd").save(dir.toString)

          checkAnswer(
            CDCReader.changesToBatchDF(DeltaLog.forTable(spark, dir), 0, 0, spark)
              .drop(CDCReader.CDC_COMMIT_TIMESTAMP),
            Row(1, 3, true, false, "insert", 0) :: Row(3, 5, true, false, "insert", 0) ::
              Row(2, 4, false, true, "insert", 0) :: Row(4, 6, false, true, "insert", 0) :: Nil)

          def data: DataFrame = spark.read.format("delta").load(dir.toString)

          Seq(5, 7).map(i => (i, i + 2)).toDF("key", "value.1")
            .withColumn("is_odd", $"`value.1`" % 2 =!= 0)
            .withColumn("is_even", $"`value.1`" % 2 === 0)
            .coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .option(DeltaOptions.REPLACE_WHERE_OPTION, "is_odd = true")
            .save(dir.toString)
          checkAnswer(
            data,
            Seq(2, 4, 5, 7).map(i => (i, i + 2)).toDF("key", "value.1")
              .withColumn("is_odd", $"`value.1`" % 2 =!= 0)
              .withColumn("is_even", $"`value.1`" % 2 === 0))

          checkAnswer(
            CDCReader.changesToBatchDF(DeltaLog.forTable(spark, dir), 1, 1, spark)
              .drop(CDCReader.CDC_COMMIT_TIMESTAMP),
            Row(1, 3, true, false, "delete", 1) :: Row(3, 5, true, false, "delete", 1) ::
              Row(5, 7, true, false, "insert", 1) :: Row(7, 9, true, false, "insert", 1) :: Nil)

          if (replaceWhereInDataColumn) {
            // replaceWhere on non-partitioning columns if enabled.
            Seq((4, 8)).toDF("key", "value.1")
              .withColumn("is_odd", $"`value.1`" % 2 =!= 0)
              .withColumn("is_even", $"`value.1`" % 2 === 0)
              .write
              .format("delta")
              .mode("overwrite")
              .option(DeltaOptions.REPLACE_WHERE_OPTION, "key = 4")
              .save(dir.toString)
            checkAnswer(
              data,
              Seq((2, 4), (4, 8), (5, 7), (7, 9)).toDF("key", "value.1")
                .withColumn("is_odd", $"`value.1`" % 2 =!= 0)
                .withColumn("is_even", $"`value.1`" % 2 === 0))

            checkAnswer(
              CDCReader.changesToBatchDF(DeltaLog.forTable(spark, dir), 2, 2, spark)
                .drop(CDCReader.CDC_COMMIT_TIMESTAMP),
              Row(4, 6, false, true, "delete", 2) :: Row(4, 8, false, true, "insert", 2) :: Nil)
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

  Seq(true, false).foreach { p =>
    test(s"replaceWhere user defined _change_type column doesn't get dropped - partitioned=$p") {
      withTable("tab") {
        sql(
          s"""CREATE TABLE tab USING DELTA
             |${if (p) "PARTITIONED BY (part) " else ""}
             |TBLPROPERTIES (delta.enableChangeDataFeed = false)
             |AS SELECT id, floor(id / 10) AS part, 'foo' as _change_type
             |FROM RANGE(1000)
             |""".stripMargin)
        Seq(33L).map(id => id * 42).toDF("id")
          .withColumn("part", expr("floor(id / 10)"))
          .withColumn("_change_type", lit("bar"))
          .write
          .format("delta")
          .mode("overwrite")
          .option(DeltaOptions.REPLACE_WHERE_OPTION, "id % 7 = 0")
          .saveAsTable("tab")

        sql("SELECT id, _change_type FROM tab").collect().foreach { row =>
          val _change_type = row.getString(1)
          assert(_change_type === "foo" || _change_type === "bar",
            s"Invalid _change_type for id=${row.get(0)}")
        }
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

  test("DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED = false: defaults to static overwrites") {
    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "false") {
      // This checks that when dynamic partition overwrite mode is disabled, we return to our
      // previous behavior: setting `partitionOverwriteMode` to `dynamic` is a no-op, and we
      // statically overwrite data

      // DataFrame write, dynamic partition overwrite enabled in DataFrameWriter option
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq(1, 2, 3).toDF
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
          .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("value").as[Int], 1, 5)
      }

      // DataFrame write, dynamic partition overwrite enabled in sparkConf
      withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> "dynamic") {
        withTempDir { tempDir =>
          def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

          Seq(1, 2, 3).toDF
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
            .save(tempDir.getCanonicalPath)
          checkDatasetUnorderly(data.select("value").as[Int], 1, 5)
        }
      }

      // SQL write
      withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key ->  "dynamic") {
        val table_name = "test_table"
        withTable(table_name) {
          spark.sql(
            s"CREATE TABLE $table_name (value int, part int) USING DELTA PARTITIONED BY (part)")
          spark.sql(s"INSERT INTO $table_name VALUES (1, 1), (2, 0), (3, 1)")
          spark.sql(s"INSERT OVERWRITE $table_name VALUES (1, 1), (5, 1)")
          checkDatasetUnorderly(spark.sql(s"SELECT value FROM $table_name").as[Int], 1, 5)
        }
      }
    }
  }

  test("batch write: append, dynamic partition overwrite integer partition column") {
    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq(1, 2, 3).toDF
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
          .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("value").as[Int], 1, 2, 5)
      }
    }
  }

  test("batch write: append, dynamic partition overwrite string partition column") {
    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq(("a", "x"), ("b", "y"), ("c", "x")).toDF("value", "part")
          .write
          .format("delta")
          .partitionBy("part")
          .mode("append")
          .save(tempDir.getCanonicalPath)

        Seq(("a", "x"), ("d", "x")).toDF("value", "part")
          .write
          .format("delta")
          .partitionBy("part")
          .mode("overwrite")
          .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("value").as[String], "a", "b", "d")
      }
    }
  }

  test("batch write: append, dynamic partition overwrite string and integer partition column") {
    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq((1, "x"), (2, "y"), (3, "z")).toDF("value", "part2")
          .withColumn("part1", $"value" % 2)
          .write
          .format("delta")
          .partitionBy("part1", "part2")
          .mode("append")
          .save(tempDir.getCanonicalPath)

        Seq((5, "x"), (7, "y")).toDF("value", "part2")
          .withColumn("part1", $"value" % 2)
          .write
          .format("delta")
          .partitionBy("part1", "part2")
          .mode("overwrite")
          .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("value").as[Int], 2, 3, 5, 7)
      }
    }
  }

  test("batch write: append, dynamic partition overwrite overwrites nothing") {
    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq(("a", "x"), ("b", "y"), ("c", "x")).toDF("value", "part")
          .write
          .format("delta")
          .partitionBy("part")
          .mode("append")
          .save(tempDir.getCanonicalPath)

        Seq(("d", "z")).toDF("value", "part")
          .write
          .format("delta")
          .partitionBy("part")
          .mode("overwrite")
          .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("value", "part").as[(String, String)],
          ("a", "x"), ("b", "y"), ("c", "x"), ("d", "z"))
      }
    }
  }

  test("batch write: append, dynamic partition overwrite multiple partition columns") {
    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq(("a", "x", 1), ("b", "y", 2), ("c", "x", 3)).toDF("part1", "part2", "value")
          .write
          .format("delta")
          .partitionBy("part1", "part2")
          .mode("append")
          .save(tempDir.getCanonicalPath)

        Seq(("a", "x", 4), ("d", "x", 5)).toDF("part1", "part2", "value")
          .write
          .format("delta")
          .partitionBy("part1", "part2")
          .mode("overwrite")
          .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("part1", "part2", "value").as[(String, String, Int)],
          ("a", "x", 4), ("b", "y", 2), ("c", "x", 3), ("d", "x", 5))
      }
    }
  }

  test("batch write: append, dynamic partition overwrite without partitionBy") {
    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq(1, 2, 3).toDF
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
          .mode("overwrite")
          .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("value").as[Int], 1, 2, 5)
      }
    }
  }

  test("batch write: append, dynamic partition overwrite conf, replaceWhere takes precedence") {
    // when dynamic partition overwrite mode is enabled in the spark configuration, and a
    // replaceWhere expression is provided, we delete data according to the replaceWhere expression
    withSQLConf(
      DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true",
      SQLConf.PARTITION_OVERWRITE_MODE.key -> "dynamic") {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq((1, "x"), (2, "y"), (3, "z")).toDF("value", "part2")
          .withColumn("part1", $"value" % 2)
          .write
          .format("delta")
          .partitionBy("part1", "part2")
          .mode("append")
          .save(tempDir.getCanonicalPath)

        Seq((5, "x")).toDF("value", "part2")
          .withColumn("part1", $"value" % 2)
          .write
          .format("delta")
          .partitionBy("part1", "part2")
          .mode("overwrite")
          .option(DeltaOptions.REPLACE_WHERE_OPTION, "part1 = 1")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select($"value").as[Int], 2, 5)
      }
    }
  }

  test("batch write: append, replaceWhere + dynamic partition overwrite enabled in options") {
    // when dynamic partition overwrite mode is enabled in the DataFrameWriter options, and
    // a replaceWhere expression is provided, we throw an error
    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq((1, "x"), (2, "y"), (3, "z")).toDF("value", "part2")
          .withColumn("part1", $"value" % 2)
          .write
          .format("delta")
          .partitionBy("part1", "part2")
          .mode("append")
          .save(tempDir.getCanonicalPath)

        val e = intercept[IllegalArgumentException] {
          Seq((3, "x"), (5, "x")).toDF("value", "part2")
            .withColumn("part1", $"value" % 2)
            .write
            .format("delta")
            .partitionBy("part1", "part2")
            .mode("overwrite")
            .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
            .option(DeltaOptions.REPLACE_WHERE_OPTION, "part1 = 1")
            .save(tempDir.getCanonicalPath)
        }
        assert(e.getMessage === "A 'replaceWhere' expression and " +
          "'partitionOverwriteMode'='dynamic' cannot both be set in the DataFrameWriter options.")
      }
    }
  }

  test("batch write: append, dynamic partition overwrite set via conf") {
    withSQLConf(
      DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true",
      SQLConf.PARTITION_OVERWRITE_MODE.key -> "dynamic") {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq(1, 2, 3).toDF
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
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("value").as[Int], 1, 2, 5)
      }
    }
  }

  test("batch write: append, dynamic partition overwrite set via conf and overridden via option") {
    withSQLConf(
      DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true",
      SQLConf.PARTITION_OVERWRITE_MODE.key -> "dynamic") {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq(1, 2, 3).toDF
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
          .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "static")
          .mode("overwrite")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("value").as[Int], 1, 5)
      }
    }
  }

  test("batch write: append, overwrite without partitions should ignore partition overwrite mode") {
    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq(1, 2, 3).toDF
          .withColumn("part", $"value" % 2)
          .write
          .format("delta")
          .mode("append")
          .save(tempDir.getCanonicalPath)

        Seq(1, 5).toDF
          .withColumn("part", $"value" % 2)
          .write
          .format("delta")
          .mode("overwrite")
          .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("value").as[Int], 1, 5)
      }
    }
  }

  test("batch write: append, overwrite non-partitioned table with replaceWhere ignores partition " +
    "overwrite mode option") {
    // we check here that setting both replaceWhere and dynamic partition overwrite in the
    // DataFrameWriter options is allowed for a non-partitioned table
    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq(1, 2, 3).toDF
          .withColumn("part", $"value" % 2)
          .write
          .format("delta")
          .mode("append")
          .save(tempDir.getCanonicalPath)

        Seq(1, 5).toDF
          .withColumn("part", $"value" % 2)
          .write
          .format("delta")
          .mode("overwrite")
          .option(DeltaOptions.REPLACE_WHERE_OPTION, "part = 1")
          .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("value").as[Int], 1, 2, 5)
      }
    }
  }

  test("batch write: append, dynamic partition with 'partitionValues' column") {
    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq(1, 2, 3).toDF
          .withColumn("partitionValues", $"value" % 2)
          .write
          .format("delta")
          .partitionBy("partitionValues")
          .mode("append")
          .save(tempDir.getCanonicalPath)

        Seq(1, 5).toDF
          .withColumn("partitionValues", $"value" % 2)
          .write
          .format("delta")
          .partitionBy("partitionValues")
          .mode("overwrite")
          .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("value").as[Int], 1, 2, 5)
      }
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
      assert(e.getMessage.contains("Cannot write to already existent path"))
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

      assert(e.getMessage.contains("Path does not exist"))
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

  test("columns with commas as partition columns") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }

      val dfw = spark.range(100).select('id, 'id % 4 as "by,4")
        .write
        .format("delta")
        .partitionBy("by,4")

      // if in column mapping mode, we should not expect invalid character errors
      if (!columnMappingEnabled) {
        val e = intercept[AnalysisException] {
          dfw.save(tempDir.toString)
        }
        assert(e.getMessage.contains("invalid character(s)"))
      }

      withSQLConf(DeltaSQLConf.DELTA_PARTITION_COLUMN_CHECK_ENABLED.key -> "false") {
        dfw.save(tempDir.toString)
      }

      // Note: although we are able to write, we cannot read the table with Spark 3.2+ with
      // OSS Delta 1.1.0+ because SPARK-36271 adds a column name check in the read path.
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

      val deltaLog = loadDeltaLog(tempDir.getAbsolutePath)
      assertPartitionExists("by4", deltaLog, files)

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
    withSQLConf(
      ("spark.sql.files.ignoreMissingFiles", "true")) {
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
          data.toDF().collect()
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
        assert(data.toDF().collect().size == 6)
      }
    }
  }

  testQuietly("deleted files cause failure by default") {
    withTempDir { tempDir =>
      val tempDirPath = new Path(tempDir.getCanonicalPath)
      def data: DataFrame = spark.read.format("delta").load(tempDir.toString)
      val deltaLog = DeltaLog.forTable(spark, tempDir)

      Range(0, 10).foreach(n =>
        Seq(n).toDF().write.format("delta").mode("append").save(tempDir.toString))

      val inputFiles = TahoeLogFileIndex(spark, deltaLog).inputFiles.toSeq
      val fileToDelete = inputFiles.head
      val pathToDelete = new Path(tempDirPath, fileToDelete)
      val deleted = tryDeleteNonRecursive(
        tempDirPath.getFileSystem(deltaLog.newDeltaHadoopConf()), pathToDelete)
      assert(deleted)

      val thrown = intercept[SparkException] {
        data.toDF().collect()
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


  test("all operations with special characters in path") {
    withTempDir { dir =>
      val directory = new File(dir, "test with space")
      val df = Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value")
      val writer = df.write.format("delta").mode("append")
      writer.save(directory.getCanonicalPath)

      // UPDATE and DELETE
      spark.sql(s"UPDATE delta.`${directory.getCanonicalPath}` SET value = 99")
      spark.sql(s"DELETE FROM delta.`${directory.getCanonicalPath}` WHERE key = 4")
      spark.sql(s"DELETE FROM delta.`${directory.getCanonicalPath}` WHERE key = 3")
      checkAnswer(
        spark.read.format("delta").load(directory.getCanonicalPath),
        Seq((1, 99), (2, 99)).toDF("key", "value")
      )

      // INSERT
      spark.sql(s"INSERT INTO delta.`${directory.getCanonicalPath}` VALUES (5, 50)")
      spark.sql(s"INSERT INTO delta.`${directory.getCanonicalPath}` VALUES (5, 50)")
      checkAnswer(
        spark.read.format("delta").load(directory.getCanonicalPath),
        Seq((1, 99), (2, 99), (5, 50), (5, 50)).toDF("key", "value")
      )

      // MERGE
      Seq((1, 1), (3, 88), (5, 88)).toDF("key", "value").createOrReplaceTempView("inbound")
      spark.sql(s"""|MERGE INTO delta.`${directory.getCanonicalPath}` AS base
                    |USING inbound
                    |ON base.key = inbound.key
                    |WHEN MATCHED THEN DELETE
                    |WHEN NOT MATCHED THEN INSERT *
                    |""".stripMargin)
      checkAnswer(
        spark.read.format("delta").load(directory.getCanonicalPath),
        Seq((2, 99), (3, 88)).toDF("key", "value")
      )

      // DELETE and INSERT again
      spark.sql(s"DELETE FROM delta.`${directory.getCanonicalPath}` WHERE key = 3")
      spark.sql(s"INSERT INTO delta.`${directory.getCanonicalPath}` VALUES (5, 99)")
      checkAnswer(
        spark.read.format("delta").load(directory.getCanonicalPath),
        Seq((2, 99), (5, 99)).toDF("key", "value")
      )

      // VACUUM
      withSQLConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
        spark.sql(s"VACUUM delta.`${directory.getCanonicalPath}` RETAIN 0 HOURS")
      }
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`${directory.getCanonicalPath}@v8`"),
        Seq((2, 99), (5, 99)).toDF("key", "value")
      )
      // Version 0 should be lost, as version 1 rewrites the whole file
      val ex = intercept[Exception] {
        checkAnswer(
          spark.sql(s"SELECT * FROM delta.`${directory.getCanonicalPath}@v0`"),
          spark.emptyDataFrame
        )
      }
      var cause = ex.getCause
      while (cause.getCause != null) {
        cause = cause.getCause
      }
      assert(cause.getMessage.contains(".parquet does not exist"))
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
      convertToDelta(s"parquet.`$path`")

      // In column mapping (name mode), we perform convertToDelta with a CONVERT and an ALTER,
      // so the version has been updated
      val commitVersion = if (columnMappingEnabled) 1 else 0
      assert(spark.conf.get(DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION) ===
        Some(commitVersion))
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
    val actions = Seq(Action.supportedProtocolVersion(), newMetadata) ++ files.map(_.remove)
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

  test("set metadata upon write") {
    withTempDir { inputDir =>
      val testPath = inputDir.getCanonicalPath
      spark.range(10)
        .map(_.toInt)
        .withColumn("part", $"value" % 2)
        .write
        .format("delta")
        .option("delta.logRetentionDuration", "123 days")
        .option("mergeSchema", "true")
        .partitionBy("part")
        .mode("append")
        .save(testPath)

      val deltaLog = DeltaLog.forTable(spark, testPath)
      // We need to drop default properties set by subclasses to make this test pass in them
      assert(deltaLog.snapshot.metadata.configuration
        .filterKeys(!_.startsWith("delta.columnMapping.")).toMap ===
        Map("delta.logRetentionDuration" -> "123 days"))
    }
  }

  test("idempotent write: idempotent DataFrame insert") {
    withTempDir { tableDir =>
      spark.conf.set("spark.databricks.delta.write.txnAppId", "insertTest")

      io.delta.tables.DeltaTable.createOrReplace(spark)
        .addColumn("col1", "INT")
        .addColumn("col2", "INT")
        .location(tableDir.getCanonicalPath)
        .execute()
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tableDir.getCanonicalPath)

      def runInsert(data: (Int, Int)): Unit = {
        Seq(data).toDF("col1", "col2")
          .write
          .format("delta")
          .mode("append")
          .save(tableDir.getCanonicalPath)
      }

      def assertTable(numRows: Int): Unit = {
        val count = deltaTable.toDF.count()
        assert(count == numRows)
      }

      // run insert (1,1), table should have 1 row (1,1)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
      runInsert((1, 1))
      assertTable(1)
      // run insert (2,2), table should have 2 rows (1,1),(2,2)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "2")
      runInsert((2, 2))
      assertTable(2)
      // retry update 2, table should have 2 rows (1,1),(2,2)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "2")
      runInsert((2, 2))
      assertTable(2)
      // run insert (3,3), table should have 3 rows (1,1),(2,2),(3,3)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "3")
      runInsert((3, 3))
      assertTable(3)

      // clean up
      spark.conf.unset("spark.databricks.delta.write.txnAppId")
      spark.conf.unset("spark.databricks.delta.write.txnVersion")
    }
  }

  test("idempotent write: idempotent SQL insert") {
    withTempDir { tableDir =>
      val tableName = "myInsertTable"
      spark.conf.set("spark.databricks.delta.write.txnAppId", "insertTestSQL")

      spark.sql(s"CREATE TABLE $tableName (col1 INT, col2 INT) USING DELTA LOCATION '" +
        tableDir.getCanonicalPath + "'")

      def runInsert(data: (Int, Int)): Unit = {
        spark.sql(s"INSERT INTO $tableName (col1, col2) VALUES (${data._1}, ${data._2})")
      }

      def assertTable(numRows: Int): Unit = {
        val count = spark.sql(s"SELECT * FROM $tableName").count()
        assert(count == numRows)
      }

      // run insert (1,1), table should have 1 row (1,1)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
      runInsert((1, 1))
      assertTable(1)
      // run insert (2,2), table should have 2 rows (1,1),(2,2)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "2")
      runInsert((2, 2))
      assertTable(2)
      // retry update 2, table should have 2 rows (1,1),(2,2)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "2")
      runInsert((2, 2))
      assertTable(2)
      // run insert (3,3), table should have 3 rows (1,1),(2,2),(3,3)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "3")
      runInsert((3, 3))
      assertTable(3)

      // clean up
      spark.conf.unset("spark.databricks.delta.write.txnAppId")
      spark.conf.unset("spark.databricks.delta.write.txnVersion")
    }
  }

  test("idempotent write: idempotent DeltaTable merge") {
    withTempDir { tableDir =>
      spark.conf.set("spark.databricks.delta.write.txnAppId", "mergeTest")

      io.delta.tables.DeltaTable.createOrReplace(spark)
        .addColumn("col1", "INT")
        .addColumn("col2", "INT")
        .location(tableDir.getCanonicalPath)
        .execute()
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tableDir.getCanonicalPath)

      def runMerge(data: (Int, Int)): Unit = {
        val df = Seq(data).toDF("col1", "col2")
        deltaTable.as("t")
          .merge(
            df.as("s"),
            "t.col1 = s.col1")
          .whenMatched.updateExpr(Map("t.col2" -> "t.col2 + s.col2"))
          .whenNotMatched().insertAll()
          .execute()
      }

      def assertTable(col2Val: Int, numRows: Int): Unit = {
        val res1 = deltaTable.toDF.select("col2").where("col1 = 1").collect()
        assert(res1.length == numRows)
        assert(res1(0).getInt(0) == col2Val)
      }

      // merge (1,0) into empty table, table should have 1 row (1,0)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
      runMerge((1, 0))
      assertTable(0, 1)
      // merge (1,2) into table, table should have 1 row (1,2)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "2")
      runMerge((1, 2))
      assertTable(2, 1)
      // retry merge 2, table should have 1 row (1,2)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "2")
      runMerge((1, 2))
      assertTable(2, 1)
      // merge (1,3) into table, table should have 1 row (1,5)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "3")
      runMerge((1, 3))
      assertTable(5, 1)

      // clean up
      spark.conf.unset("spark.databricks.delta.write.txnAppId")
      spark.conf.unset("spark.databricks.delta.write.txnVersion")
    }
  }

  test("idempotent write: idempotent SQL merge") {
    def withTempDirs(f: (File, File) => Unit): Unit = {
      withTempDir { file1 =>
        withTempDir { file2 =>
          f(file1, file2)
        }
      }
    }

    withTempDirs { (tableDir, updateTableDir) =>
      val targetTableName = "myMergeTable"
      val sourceTableName = "updates"
      spark.conf.set("spark.databricks.delta.write.txnAppId", "mergeTestSQL")

      spark.sql(s"CREATE TABLE $targetTableName (col1 INT, col2 INT) USING DELTA LOCATION '" +
        tableDir.getCanonicalPath + "'")
      spark.sql(s"CREATE TABLE $sourceTableName (col1 INT, col2 INT) USING DELTA LOCATION '" +
        updateTableDir.getCanonicalPath + "'")

      def runMerge(data: (Int, Int), txnVersion: Int): Unit = {
        val df = Seq(data).toDF("col1", "col2")
        spark.conf.set("spark.databricks.delta.write.txnVersion", s"$txnVersion")
        df.write.format("delta").mode("overwrite").save(updateTableDir.getCanonicalPath)
        spark.conf.set("spark.databricks.delta.write.txnVersion", s"$txnVersion")
        spark.sql(s"""
                     |MERGE INTO $targetTableName AS t USING $sourceTableName AS s
                     | ON t.col1 = s.col1
                     | WHEN MATCHED THEN UPDATE SET t.col2 = t.col2 + s.col2
                     | WHEN NOT MATCHED THEN INSERT (col1, col2) VALUES (col1, col2)
                     |""".stripMargin)
      }

      def assertTable(col2Val: Int, numRows: Int): Unit = {
        val res1 = spark.sql(s"SELECT col2 FROM $targetTableName WHERE col1 = 1").collect()
        assert(res1.length == numRows)
        assert(res1(0).getInt(0) == col2Val)
      }

      // merge (1,0) into empty table, table should have 1 row (1,0)
      runMerge((1, 0), 1)
      assertTable(0, 1)
      // merge (1,2) into table, table should have 1 row (1,2)
      runMerge((1, 2), 2)
      assertTable(2, 1)
      // retry merge 2, table should have 1 row (1,2)
      runMerge((1, 2), 2)
      assertTable( 2, 1)
      // merge (1,3) into table, table should have 1 row (1,5)
      runMerge((1, 3), 3)
      assertTable(5, 1)

      // clean up
      spark.conf.unset("spark.databricks.delta.write.txnAppId")
      spark.conf.unset("spark.databricks.delta.write.txnVersion")
    }
  }

  test("idempotent write: idempotent DeltaTable update") {
    withTempDir { tableDir =>
      spark.conf.set("spark.databricks.delta.write.txnAppId", "updateTest")

      io.delta.tables.DeltaTable.createOrReplace(spark)
        .addColumn("col1", "INT")
        .addColumn("col2", "INT")
        .location(tableDir.getCanonicalPath)
        .execute()
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tableDir.getCanonicalPath)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "0")
      Seq((1, 0)).toDF("col1", "col2")
        .write.format("delta").mode("append").save(tableDir.getCanonicalPath)

      def runUpdate(data: (Int, Int)): Unit = {
        deltaTable.update(
          condition = expr(s"col1 == ${data._1}"),
          set = Map("col2" -> expr(s"col2 + ${data._2}"))
        )
      }

      def assertTable(col2Val: Int, numRows: Int): Unit = {
        val res1 = deltaTable.toDF.select("col2").where("col1 = 1").collect()
        assert(res1.length == numRows)
        assert(res1(0).getInt(0) == col2Val)
      }

      // run update (1,1), table should have 1 row (1,1)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
      runUpdate((1, 1))
      assertTable(1, 1)
      // run update (1,2), table should have 1 row (1,3)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "2")
      runUpdate((1, 2))
      assertTable(3, 1)
      // retry update 2, table should have 1 row (1,3)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "2")
      runUpdate((1, 2))
      assertTable(3, 1)
      // retry update 1, table should have 1 row (1,3)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
      runUpdate((1, 1))
      assertTable(3, 1)
      // run update (1,3) into table, table should have 1 row (1,6)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "3")
      runUpdate((1, 3))
      assertTable(6, 1)

      // clean up
      spark.conf.unset("spark.databricks.delta.write.txnAppId")
      spark.conf.unset("spark.databricks.delta.write.txnVersion")
    }
  }

  test("idempotent write: idempotent SQL update") {
    withTempDir { tableDir =>
      val tableName = "myUpdateTable"
      spark.conf.set("spark.databricks.delta.write.txnAppId", "updateTestSQL")

      spark.sql(s"CREATE TABLE $tableName (col1 INT, col2 INT) USING DELTA LOCATION '" +
        tableDir.getCanonicalPath + "'")
      spark.conf.set("spark.databricks.delta.write.txnVersion", "0")
      spark.sql(s"INSERT INTO $tableName (col1, col2) VALUES (1, 0)")

      def runUpdate(data: (Int, Int)): Unit = {
        spark.sql(s"""
                     |UPDATE $tableName SET
                     | col2 = col2 + ${data._2} WHERE col1 = ${data._1}
              """.stripMargin)
      }

      def assertTable(col2Val: Int, numRows: Int): Unit = {
        val res1 = spark.sql(s"SELECT col2 FROM $tableName WHERE col1 = 1").collect()
        assert(res1.length == numRows)
        assert(res1(0).getInt(0) == col2Val)
      }

      // run update (1,1), table should have 1 row (1,1)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
      runUpdate((1, 1))
      assertTable(1, 1)
      // run update (1,2), table should have 1 row (1,3)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "2")
      runUpdate((1, 2))
      assertTable(3, 1)
      // retry update 2, table should have 1 row (1,3)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "2")
      runUpdate((1, 2))
      assertTable(3, 1)
      // retry update 1, table should have 1 row (1,3)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
      runUpdate((1, 1))
      assertTable(3, 1)
      // run update (1,3) into table, table should have 1 row (1,6)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "3")
      runUpdate((1, 3))
      assertTable(6, 1)

      // clean up
      spark.conf.unset("spark.databricks.delta.write.txnAppId")
      spark.conf.unset("spark.databricks.delta.write.txnVersion")
    }
  }

  test("idempotent write: idempotent DeltaTable delete") {
    withTempDir { tableDir =>
      spark.conf.set("spark.databricks.delta.write.txnAppId", "deleteTest")

      io.delta.tables.DeltaTable.createOrReplace(spark)
        .addColumn("col1", "INT")
        .addColumn("col2", "INT")
        .location(tableDir.getCanonicalPath)
        .execute()
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tableDir.getCanonicalPath)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "0")
      Seq((1, 0), (2, 0), (3, 0), (4, 0)).toDF("col1", "col2")
        .write.format("delta").mode("append").save(tableDir.getCanonicalPath)

      def runDelete(toDelete: Int): Unit = {
        deltaTable.delete(s"col1 = $toDelete")
      }

      def assertTable(numRows: Int): Unit = {
        val rows = deltaTable.toDF.count()
        assert(rows == numRows)
      }

      // run delete (1), table should have 3 rows (2,0),(3,0),(4,0)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
      runDelete(1)
      assertTable(3)
      // add (1,0) back to table
      spark.conf.set("spark.databricks.delta.write.txnVersion", "2")
      Seq((1, 0)).toDF("col1", "col2")
        .write.format("delta").mode("append").save(tableDir.getCanonicalPath)
      assertTable(4)
      // retry delete 1, table should have 4 rows (2,0),(3,0),(4,0)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
      runDelete(1)
      assertTable(4)
      // run delete (1), table should have 3 rows (2,0),(3,0),(4,0)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "3")
      runDelete(1)
      assertTable(3)

      // clean up
      spark.conf.unset("spark.databricks.delta.write.txnAppId")
      spark.conf.unset("spark.databricks.delta.write.txnVersion")
    }
  }

  test("idempotent write: idempotent SQL delete") {
    withTempDir { tableDir =>
      val tableName = "myDeleteTable"
      spark.conf.set("spark.databricks.delta.write.txnAppId", "deleteTestSQL")

      spark.sql(s"CREATE TABLE $tableName (col1 INT, col2 INT) USING DELTA LOCATION '" +
        tableDir.getCanonicalPath + "'")
      spark.conf.set("spark.databricks.delta.write.txnVersion", "0")
      spark.sql(s"INSERT INTO $tableName (col1, col2) VALUES (1, 0), (2, 0), (3, 0), (4, 0)")

      def runDelete(toDelete: Int): Unit = {
        spark.sql(s"DELETE FROM $tableName WHERE col1 = $toDelete")
      }

      def assertTable(numRows: Long): Unit = {
        val res1 = spark.sql(s"SELECT COUNT(*) FROM $tableName").collect()
        assert(res1.length == 1)
        assert(res1(0).getLong(0) == numRows)
      }

      // run delete (1), table should have 3 rows (2,0),(3,0),(4,0)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
      runDelete(1)
      assertTable(3)
      // add (1,0) back to table
      spark.conf.set("spark.databricks.delta.write.txnVersion", "2")
      spark.sql(s"INSERT INTO $tableName (col1, col2) VALUES (1, 0)")
      assertTable(4)
      // retry delete (1), table should have 4 rows (2,0),(3,0),(4,0)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
      runDelete(1)
      assertTable(4)
      // run delete (1), table should have 3 rows (2,0),(3,0),(4,0)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "3")
      runDelete(1)
      assertTable(3)

      // clean up
      spark.conf.unset("spark.databricks.delta.write.txnAppId")
      spark.conf.unset("spark.databricks.delta.write.txnVersion")
    }
  }

  test("idempotent write: valid txnVersion") {
    spark.conf.set("spark.databricks.delta.write.txnAppId", "deleteTestSQL")
    val e = intercept[IllegalArgumentException] {
      spark.sessionState.conf.setConfString(
        "spark.databricks.delta.write.txnVersion", "someVersion")
    }
    assert(e.getMessage == "spark.databricks.delta.write.txnVersion should be " +
      "long, but was someVersion")

    // clean up
    spark.conf.unset("spark.databricks.delta.write.txnAppId")
    spark.conf.unset("spark.databricks.delta.write.txnVersion")
  }

  Seq("REPLACE", "CREATE OR REPLACE").foreach { command =>
    test(s"Idempotent $command command") {
      withTempDir { tableDir =>
        val tableName = "myIdempotentReplaceTable"
        withTable(tableName) {
          spark.conf.set("spark.databricks.delta.write.txnAppId", "replaceTestSQL")
          spark.sql(s"CREATE TABLE $tableName(c1 INT, c2 INT, c3 INT)" +
            s"USING DELTA LOCATION '" + tableDir.getCanonicalPath + "'")

          def runReplace(data: (Int, Int, Int)): Unit = {
            spark.sql(s"$command table $tableName USING DELTA " +
              s"as SELECT ${data._1} as c1, ${data._2} as c2, ${data._3} as c3")
          }

          def assertTable(numRows: Int, commitVersion: Int, data: (Int, Int, Int)): Unit = {
            val count = spark.sql(s"SELECT * FROM $tableName").count()
            assert(count == numRows)
            val snapshot = DeltaLog.forTable(spark, tableDir.getCanonicalPath).update()
            assert(snapshot.version == commitVersion)
            val tableContent = spark.sql(s"SELECT * FROM $tableName").collect().head
            assert(tableContent.getInt(0) == data._1)
            assert(tableContent.getInt(1) == data._2)
            assert(tableContent.getInt(2) == data._3)
          }

          // run replace (1,1,1) with version 1, table should have 1 row (1,1,1).
          spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
          runReplace((1, 1, 1))
          assertTable(1, 1, (1, 1, 1))
          // run replace (2,2,2) with version 2, table should have 1 row (2,2,2)
          spark.conf.set("spark.databricks.delta.write.txnVersion", "2")
          runReplace((2, 2, 2))
          assertTable(1, 2, (2, 2, 2))
          // retry replace (3,3,3) with version 2, table should have 1 row (2,2,2).
          spark.conf.set("spark.databricks.delta.write.txnVersion", "2")
          runReplace((3, 3, 3))
          assertTable(1, 2, (2, 2, 2))
          // run replace (4,4,4) with version 3, table should have 1 row (4,4,4).
          spark.conf.set("spark.databricks.delta.write.txnVersion", "3")
          runReplace((4, 4, 4))
          assertTable(1, 3, (4, 4, 4))
          // run replace (5,5,5) with version 3, table should have 1 row (4,4,4).
          spark.conf.set("spark.databricks.delta.write.txnVersion", "3")
          runReplace((5, 5, 5))
          assertTable(1, 3, (4, 4, 4))
          // clean up
          spark.conf.unset("spark.databricks.delta.write.txnAppId")
          spark.conf.unset("spark.databricks.delta.write.txnVersion")
        }
      }
    }
  }

  test("idempotent write: auto reset txnVersion") {
    withTempDir { tableDir =>
      val tableName = "myAutoResetTable"
      spark.conf.set("spark.databricks.delta.write.txnAppId", "autoReset")
      spark.sql(s"CREATE TABLE $tableName (col1 INT, col2 INT) USING DELTA LOCATION '" +
        tableDir.getCanonicalPath + "'")

      // this write is done with txn version 0
      spark.conf.set("spark.databricks.delta.write.txnVersion", "0")
      spark.sql(s"INSERT INTO $tableName (col1, col2) VALUES (1, 0)")
      // this write should be skipped as the version is not reset so it will be applied
      // with the same version
      spark.sql(s"INSERT INTO $tableName (col1, col2) VALUES (2, 0)")
      assert(spark.sql(s"SELECT * FROM $tableName").count() == 1)

      // now enable auto reset
      spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "true")

      // this write should be skipped as it is using the same txnVersion as the first write
      spark.conf.set("spark.databricks.delta.write.txnVersion", "0")
      spark.sql(s"INSERT INTO $tableName (col1, col2) VALUES (3, 0)")
      // this should throw an exception as the txn version is automatically reset
      val e1 = intercept[IllegalArgumentException] {
        spark.sql(s"INSERT INTO $tableName (col1, col2) VALUES (4, 0)")
      }
      assert(e1.getMessage == "Invalid options for idempotent Dataframe writes: " +
        "Both spark.databricks.delta.write.txnAppId and spark.databricks.delta.write.txnVersion " +
        "must be specified for idempotent Delta writes")
      // this write should succeed as it's using a newer version than the latest
      spark.conf.set("spark.databricks.delta.write.txnVersion", "10")
      spark.sql(s"INSERT INTO $tableName (col1, col2) VALUES (2, 0)")
      // this should throw an exception as the txn version is automatically reset
      val e2 = intercept[IllegalArgumentException] {
        spark.sql(s"INSERT INTO $tableName (col1, col2) VALUES (3, 0)")
      }
      assert(e2.getMessage == "Invalid options for idempotent Dataframe writes: " +
        "Both spark.databricks.delta.write.txnAppId and spark.databricks.delta.write.txnVersion " +
        "must be specified for idempotent Delta writes")

      val res = spark.sql(s"SELECT col1 FROM $tableName")
        .orderBy(asc("col1"))
        .collect()
      assert(res.length == 2)
      assert(res(0).getInt(0) == 1)
      assert(res(1).getInt(0) == 2)

      // clean up
      spark.conf.unset("spark.databricks.delta.write.txnAppId")
      spark.conf.unset("spark.databricks.delta.write.txnVersion")
    }
  }

  def idempotentWrite(
      mode: String,
      appId: String,
      seq: DataFrame,
      path: String,
      name: String,
      version: Long,
      expectedCount: Long,
      commitVersion: Int,
      isSaveAsTable: Boolean = true): Unit = {
    val df = seq.write.format("delta")
      .option(DeltaOptions.TXN_VERSION, version)
      .option(DeltaOptions.TXN_APP_ID, appId)
      .mode(mode)
    if (isSaveAsTable) {
      df.option("path", path).saveAsTable(name)
    } else {
      df.save(path)
    }
    val i = spark.read.format("delta").load(path).count()
    assert(i == expectedCount)
    val snapshot = DeltaLog.forTable(spark, path).update()
    assert(snapshot.version == (commitVersion - 1))
  }

  Seq((true, true), (true, false), (false, true), (false, false))
    .foreach {case (isSaveAsTable, isLegacy) =>
      val op = if (isSaveAsTable) "saveAsTable" else "save"
      val version = if (isLegacy) "legacy" else "non-legacy"
      val appId1 = "myAppId1"
      val appId2 = "myAppId2"
      val confs = if (isLegacy) Seq(SQLConf.USE_V1_SOURCE_LIST.key -> "tahoe,delta") else Seq.empty

      if (!(isSaveAsTable && isLegacy)) {
        test(s"Idempotent $version Dataframe $op: append") {
          withSQLConf(confs: _*) {
            withTempDir { dir =>
              val path = dir.getCanonicalPath
              val name = "append_table_t1"
              val mode = "append"
              sql("DROP TABLE IF EXISTS append_table_t1")
              val df = Seq((1, 2, 3), (4, 5, 6), (7, 8, 9)).toDF("a", "b", "c")
              // The first 2 runs must succeed increasing the expected count.
              idempotentWrite(mode, appId1, df, path, name, 1, 3, 1, isSaveAsTable)
              idempotentWrite(mode, appId1, df, path, name, 2, 6, 2, isSaveAsTable)

              // Even if the version is not consecutive, higher versions should commit successfully.
              idempotentWrite(mode, appId1, df, path, name, 5, 9, 3, isSaveAsTable)

              // This run should be ignored because it uses an older version.
              idempotentWrite(mode, appId1, df, path, name, 5, 9, 3, isSaveAsTable)

              // Use a different app ID, but same version. This should succeed.
              idempotentWrite(mode, appId2, df, path, name, 5, 12, 4, isSaveAsTable)
              idempotentWrite(mode, appId2, df, path, name, 5, 12, 4, isSaveAsTable)

              // Verify that specifying only one of the options -- either appId or version -- fails.
              val e1 = intercept[Exception] {
                val stage = df.write.format("delta").option(DeltaOptions.TXN_APP_ID, 1).mode(mode)
                if (isSaveAsTable) {
                  stage.option("path", path).saveAsTable(name)
                } else {
                  stage.save(path)
                }
              }
              assert(e1.getMessage.contains("Invalid options for idempotent Dataframe writes"))
              val e2 = intercept[Exception] {
                val stage = df.write.format("delta").option(DeltaOptions.TXN_VERSION, 1).mode(mode)
                if (isSaveAsTable) {
                  stage.option("path", path).saveAsTable(name)
                } else {
                  stage.save(path)
                }
              }
              assert(e2.getMessage.contains("Invalid options for idempotent Dataframe writes"))
            }
          }
        }
      }

      test(s"Idempotent $version Dataframe $op: overwrite") {
        withSQLConf(confs: _*) {
          withTempDir { dir =>
            val path = dir.getCanonicalPath
            val name = "overwrite_table_t1"
            val mode = "overwrite"
            sql("DROP TABLE IF EXISTS overwrite_table_t1")
            val df = Seq((1, 2, 3), (4, 5, 6), (7, 8, 9)).toDF("a", "b", "c")
            // The first 2 runs must succeed increasing the expected count.
            idempotentWrite(mode, appId1, df, path, name, 1, 3, 1, isSaveAsTable)
            idempotentWrite(mode, appId1, df, path, name, 2, 3, 2, isSaveAsTable)

            // Even if the version is not consecutive, higher versions should commit successfully.
            idempotentWrite(mode, appId1, df, path, name, 5, 3, 3, isSaveAsTable)

            // This run should be ignored because it uses an older version.
            idempotentWrite(mode, appId1, df, path, name, 5, 3, 3, isSaveAsTable)

            // Use a different app ID, but same version. This should succeed.
            idempotentWrite(mode, appId2, df, path, name, 5, 3, 4, isSaveAsTable)
            idempotentWrite(mode, appId2, df, path, name, 5, 3, 4, isSaveAsTable)

            // Verify that specifying only one of the options -- either appId or version -- fails.
            val e1 = intercept[Exception] {
              val stage = df.write.format("delta").option(DeltaOptions.TXN_APP_ID, 1).mode(mode)
              if (isSaveAsTable) stage.option("path", path).saveAsTable(name) else stage.save(path)
            }
            assert(e1.getMessage.contains("Invalid options for idempotent Dataframe writes"))
            val e2 = intercept[Exception] {
              val stage = df.write.format("delta").option(DeltaOptions.TXN_VERSION, 1).mode(mode)
              if (isSaveAsTable) stage.option("path", path).saveAsTable(name) else stage.save(path)
            }
            assert(e2.getMessage.contains("Invalid options for idempotent Dataframe writes"))
          }
        }
      }
  }

  test("idempotent writes in streaming foreachBatch") {
    // Function to get a checkpoint location and 2 table locations.
    def withTempDirs(f: (File, File, File) => Unit): Unit = {
      withTempDir { file1 =>
        withTempDir { file2 =>
          withTempDir { file3 =>
            f(file1, file2, file3)
          }
        }
      }
    }

    // In this test, we are going to run a streaming query in a deterministic way.
    // This streaming query uses foreachBatch to append data to two tables, and
    // depending on a boolean flag, the query can fail between the two table writes.
    // By setting this flag, we will test whether both tables are consistenly updated
    // when query resumes after failure - no duplicates, no data missing.

    withTempDirs { (checkpointDir, table1Dir, table2Dir) =>
      @volatile var shouldFail = false

      /* Function to write a batch's data to 2 tables */
      def runBatch(batch: DataFrame, appId: String, batchId: Long): Unit = {
        // Append to table 1
        batch.write.format("delta")
          .option(DeltaOptions.TXN_VERSION, batchId)
          .option(DeltaOptions.TXN_APP_ID, appId)
          .mode("append").save(table1Dir.getCanonicalPath)
        if (shouldFail) {
          throw new Exception("Terminating execution")
        } else {
          // Append to table 2
          batch.write.format("delta")
            .option(DeltaOptions.TXN_VERSION, batchId)
            .option(DeltaOptions.TXN_APP_ID, appId)
            .mode("append").save(table2Dir.getCanonicalPath)
        }
      }

      @volatile var query: StreamingQuery = null

      // Prepare a streaming query
      val inputData = MemoryStream[Int]
      val df = inputData.toDF()
      val streamWriter = df.writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreachBatch { (batch: DataFrame, id: Long) => {
          runBatch(batch, query.id.toString, id) }
        }

      /* Add data and run streaming query, then verify # rows in 2 tables */
      def runQuery(dataToAdd: Int, expectedTable1Count: Int, expectedTable2Count: Int): Unit = {
        inputData.addData(dataToAdd)
        query = streamWriter.start()
        try {
          query.processAllAvailable()
        } catch {
          case e: Exception =>
            assert(e.getMessage.contains("Terminating execution"))
        } finally {
          query.stop()
        }
        val t1Count = spark.read.format("delta").load(table1Dir.getCanonicalPath).count()
        assert(t1Count == expectedTable1Count)
        val t2Count = spark.read.format("delta").load(table2Dir.getCanonicalPath).count()
        assert(t2Count == expectedTable2Count)
      }

      // Run the query 3 times. First time without failure, both the output tables are updated.
      shouldFail = false
      runQuery(dataToAdd = 0, expectedTable1Count = 1, expectedTable2Count = 1)
      // Second time with failure. Only one of the tables should be updated.
      shouldFail = true
      runQuery(dataToAdd = 1, expectedTable1Count = 2, expectedTable2Count = 1)
      // Third time without failure. Both the tables should be consistently updated.
      shouldFail = false
      runQuery(dataToAdd = 2, expectedTable1Count = 3, expectedTable2Count = 3)
    }
  }


  test("parsing table name and alias using test helper") {
    import DeltaTestUtils.parseTableAndAlias
    // Parse table name from path and optional alias.
    assert(parseTableAndAlias("delta.`store_sales`") === "delta.`store_sales`" -> None)
    assert(parseTableAndAlias("delta.`store sales`") === "delta.`store sales`" -> None)
    assert(parseTableAndAlias("delta.`store_sales` s") === "delta.`store_sales`" -> Some("s"))
    assert(parseTableAndAlias("delta.`store sales` as s") === "delta.`store sales`" -> Some("s"))
    assert(parseTableAndAlias("delta.`store%sales` AS s") === "delta.`store%sales`" -> Some("s"))

    // Parse table name and optional alias.
    assert(parseTableAndAlias("store_sales") === "store_sales" -> None)
    assert(parseTableAndAlias("store sales") === "store" -> Some("sales"))
    assert(parseTableAndAlias("store_sales s") === "store_sales" -> Some("s"))
    assert(parseTableAndAlias("'store sales' as s") === "'store sales'" -> Some("s"))
    assert(parseTableAndAlias("'store%sales' AS s") === "'store%sales'" -> Some("s"))

    // Not properly supported: ambiguous without special handling for escaping.
    assert(parseTableAndAlias("'store sales'") === "'store" -> Some("sales'"))
  }
}


class DeltaNameColumnMappingSuite extends DeltaSuite
  with DeltaColumnMappingEnableNameMode {

  import testImplicits._

  override protected def runOnlyTests = Seq(
    "handle partition filters and data filters",
    "query with predicates should skip partitions",
    "valid replaceWhere",
    "batch write: append, overwrite where",
    "get touched files for update, delete and merge",
    "isBlindAppend with save and saveAsTable"
  )


  test(
    "dynamic partition overwrite with conflicting logical vs. physical named partition columns") {
    // It isn't sufficient to just test with column mapping enabled because the physical names are
    // generated automatically and thus are unique w.r.t. the logical names.
    // Instead we need to have: ColA.logicalName = ColB.physicalName,
    // which means we need to start with columnMappingMode=None, and then upgrade to
    // columnMappingMode=name and rename our columns

    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "true",
      DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey-> NoMapping.name) {
      withTempDir { tempDir =>
        def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

        Seq(("a", "x", 1), ("b", "y", 2), ("c", "x", 3)).toDF("part1", "part2", "value")
          .write
          .format("delta")
          .partitionBy("part1", "part2")
          .mode("append")
          .save(tempDir.getCanonicalPath)

        val protocol = DeltaLog.forTable(spark, tempDir).snapshot.protocol
        val (r, w) = if (protocol.supportsReaderFeatures || protocol.supportsWriterFeatures) {
          (TableFeatureProtocolUtils.TABLE_FEATURES_MIN_READER_VERSION,
            TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION)
        } else {
          (ColumnMappingTableFeature.minReaderVersion, ColumnMappingTableFeature.minWriterVersion)
        }

        spark.sql(
          s"""
             |ALTER TABLE delta.`${tempDir.getCanonicalPath}` SET TBLPROPERTIES (
             |  'delta.minReaderVersion' = '$r',
             |  'delta.minWriterVersion' = '$w',
             |  'delta.columnMapping.mode' = 'name'
             |)
             |""".stripMargin)

        spark.sql(
          s"""
             |ALTER TABLE delta.`${tempDir.getCanonicalPath}` RENAME COLUMN part1 TO temp
             |""".stripMargin)
        spark.sql(
          s"""
             |ALTER TABLE delta.`${tempDir.getCanonicalPath}` RENAME COLUMN part2 TO part1
             |""".stripMargin)
        spark.sql(
          s"""
             |ALTER TABLE delta.`${tempDir.getCanonicalPath}` RENAME COLUMN temp TO part2
             |""".stripMargin)

        Seq(("a", "x", 4), ("d", "x", 5)).toDF("part2", "part1", "value")
          .write
          .format("delta")
          .partitionBy("part2", "part1")
          .mode("overwrite")
          .option(DeltaOptions.PARTITION_OVERWRITE_MODE_OPTION, "dynamic")
          .save(tempDir.getCanonicalPath)
        checkDatasetUnorderly(data.select("part2", "part1", "value").as[(String, String, Int)],
          ("a", "x", 4), ("b", "y", 2), ("c", "x", 3), ("d", "x", 5))
      }
    }
  }

  test("replaceWhere dataframe V2 API with less than predicate") {
    withTempDir { dir =>
      val insertedDF = spark.range(10).toDF()

      insertedDF.write.format("delta").save(dir.toString)

      val otherDF = spark.range(start = 0, end = 4).toDF()
      otherDF.writeTo(s"delta.`${dir.toString}`").overwrite(col("id") < 6)
      checkAnswer(spark.read.load(dir.toString),
        insertedDF.filter(col("id") >= 6).union(otherDF))
    }
  }

  test("replaceWhere SQL - partition column - dynamic filter") {
    withTempDir { dir =>
      // create partitioned table
      spark.range(100).withColumn("part", 'id % 10)
        .write
        .format("delta")
        .partitionBy("part")
        .save(dir.toString)

      // ans will be used to replace the entire contents of the table
      val ans = spark.range(10)
        .withColumn("part", lit(0))

      ans.createOrReplaceTempView("replace")
      sql(s"INSERT INTO delta.`${dir.toString}` REPLACE WHERE part >=0 SELECT * FROM replace")
      checkAnswer(spark.read.format("delta").load(dir.toString), ans)
    }
  }

  test("replaceWhere SQL - partition column - static filter") {
    withTable("tbl") {
      // create partitioned table
      spark.range(100).withColumn("part", lit(0))
        .write
        .format("delta")
        .partitionBy("part")
        .saveAsTable("tbl")

      val partEq1DF = spark.range(10, 20)
        .withColumn("part", lit(1))
      partEq1DF.write.format("delta").mode("append").saveAsTable("tbl")


      val replacer = spark.range(10)
        .withColumn("part", lit(0))

      replacer.createOrReplaceTempView("replace")
      sql(s"INSERT INTO tbl REPLACE WHERE part=0 SELECT * FROM replace")
      checkAnswer(spark.read.format("delta").table("tbl"), replacer.union(partEq1DF))
    }
  }

  test("replaceWhere SQL - data column - dynamic") {
    withTable("tbl") {
      // write table
      spark.range(100).withColumn("col", lit(1))
        .write
        .format("delta")
        .saveAsTable("tbl")

      val colGt2DF = spark.range(100, 200)
        .withColumn("col", lit(3))

      colGt2DF.write
        .format("delta")
        .mode("append")
        .saveAsTable("tbl")

      val replacer = spark.range(10)
        .withColumn("col", lit(1))

      replacer.createOrReplaceTempView("replace")
      sql(s"INSERT INTO tbl REPLACE WHERE col < 2 SELECT * FROM replace")
      checkAnswer(
        spark.read.format("delta").table("tbl"),
        replacer.union(colGt2DF)
      )
    }
  }

  test("replaceWhere SQL - data column - static") {
    withTempDir { dir =>
      // write table
      spark.range(100).withColumn("col", lit(2))
        .write
        .format("delta")
        .save(dir.toString)

      val colEq2DF = spark.range(100, 200)
        .withColumn("col", lit(1))

      colEq2DF.write
        .format("delta")
        .mode("append")
        .save(dir.toString)

      val replacer = spark.range(10)
        .withColumn("col", lit(2))

      replacer.createOrReplaceTempView("replace")
      sql(s"INSERT INTO delta.`${dir.toString}` REPLACE WHERE col = 2 SELECT * FROM replace")
      checkAnswer(
        spark.read.format("delta").load(dir.toString),
        replacer.union(colEq2DF)
      )
    }
  }

  test("replaceWhere SQL - multiple predicates - static") {
    withTempDir { dir =>
      // write table
      spark.range(100).withColumn("col", lit(2))
        .write
        .format("delta")
        .save(dir.toString)

      spark.range(100, 200).withColumn("col", lit(5))
        .write
        .format("delta")
        .mode("append")
        .save(dir.toString)

      val colEq2DF = spark.range(100, 200)
        .withColumn("col", lit(1))

      colEq2DF.write
        .format("delta")
        .mode("append")
        .save(dir.toString)

      val replacer = spark.range(10)
        .withColumn("col", lit(2))

      replacer.createOrReplaceTempView("replace")
      sql(s"INSERT INTO delta.`${dir.toString}` REPLACE WHERE col = 2 OR col = 5 " +
        s"SELECT * FROM replace")
      checkAnswer(
        spark.read.format("delta").load(dir.toString),
        replacer.union(colEq2DF)
      )
    }
  }

  test("replaceWhere with less than predicate") {
    withTempDir { dir =>
      val insertedDF = spark.range(10).toDF()

      insertedDF.write.format("delta").save(dir.toString)

      val otherDF = spark.range(start = 0, end = 4).toDF()
      otherDF.write.format("delta").mode("overwrite")
        .option(DeltaOptions.REPLACE_WHERE_OPTION, "id < 6")
        .save(dir.toString)
      checkAnswer(spark.read.load(dir.toString),
        insertedDF.filter(col("id") >= 6).union(otherDF))
    }
  }

  test("replaceWhere SQL with less than predicate") {
    withTempDir { dir =>
      val insertedDF = spark.range(10).toDF()

      insertedDF.write.format("delta").save(dir.toString)

      val otherDF = spark.range(start = 0, end = 4).toDF()
      otherDF.createOrReplaceTempView("replace")

      sql(
        s"""
           |INSERT INTO delta.`${dir.getAbsolutePath}`
           |REPLACE WHERE id < 6
           |SELECT * FROM replace
           |""".stripMargin)
      checkAnswer(spark.read.load(dir.toString),
        insertedDF.filter(col("id") >= 6).union(otherDF))
    }
  }
}
