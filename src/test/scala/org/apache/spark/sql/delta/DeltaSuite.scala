/*
 * Copyright 2019 Databricks, Inc.
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

import org.apache.spark.sql.delta.actions.{Action, FileAction}
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.InSet
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.OPTIMIZER_METADATA_ONLY
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.util.Utils

class DeltaSuite extends QueryTest
  with SharedSparkSession  with SQLTestUtils {

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
        (1 -> 1), (3 -> 1), (5 -> 1), (7 -> 1), (9 -> 1))
      checkDatasetUnorderly(
        ds.where("part = 0"),
        (0 -> 0), (2 -> 0), (4 -> 0), (6 -> 0), (8 -> 0))
      // data filter
      checkDatasetUnorderly(
        ds.where("value >= 5"),
        (5 -> 1), (6 -> 0), (7 -> 1), (8 -> 0), (9 -> 1))
      checkDatasetUnorderly(
        ds.where("value < 5"),
        (0 -> 0), (1 -> 1), (2 -> 0), (3 -> 1), (4 -> 0))
      // partition filter + data filter
      checkDatasetUnorderly(
        ds.where("part = 1 and value >= 5"),
        (5 -> 1), (7 -> 1), (9 -> 1))
      checkDatasetUnorderly(
        ds.where("part = 1 and value < 5"),
        (1 -> 1), (3 -> 1))
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
        checkDatasetUnorderly(ds, (1 -> "a"), (2 -> "b"))
      }
    }
  }

  test("SC-8078: read deleted directory") {
    val tempDir = Utils.createTempDir()
    val path = new Path(tempDir.getCanonicalPath)
    Seq(1).toDF().write.format("delta").save(tempDir.toString)

    val df = spark.read.format("delta").load(tempDir.toString)
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    fs.delete(path, true)

    val e = intercept[FileNotFoundException] {
      withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0s") {
        checkAnswer(df, Row(1) :: Nil)
      }
    }.getMessage
    assert(e.contains("No delta log found"))
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
    assert(e2.contains("Data written into Delta needs to contain at least one non-partitioned"))

    var e3 = intercept[AnalysisException] {
      Seq(6).toDF()
        .withColumn("is_odd", $"value" % 2 =!= 0)
        .write
        .format("delta")
        .mode("overwrite")
        .option(DeltaOptions.REPLACE_WHERE_OPTION, "not_a_column = true")
        .save(tempDir.toString)
    }.getMessage
    assert(e3 == "Predicate references non-partition column 'not_a_column'. Only the " +
      "partition columns may be referenced: [is_odd];")

    var e4 = intercept[AnalysisException] {
      Seq(6).toDF()
        .withColumn("is_odd", $"value" % 2 =!= 0)
        .write
        .format("delta")
        .mode("overwrite")
        .option(DeltaOptions.REPLACE_WHERE_OPTION, "value = 1")
        .save(tempDir.toString)
    }.getMessage
    assert(e4 == "Predicate references non-partition column 'value'. Only the " +
      "partition columns may be referenced: [is_odd];")

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

      assert(e.getMessage.contains("doesn't exist"))
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

      assert(files.forall(path => path.contains("by4=") && path.contains("/by8=")),
        s"${files.toSeq.mkString("\n")}\ndidn't contain partition columns by4 and by8")
    }
  }

  test("columns with commas as partition columns") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }

      spark.range(100).select('id, 'id % 4 as "by,4")
        .write
        .format("delta")
        .partitionBy("by,4")
        .save(tempDir.toString)

      val files = spark.read.format("delta").load(tempDir.toString).inputFiles

      assert(files.forall(path => path.contains("by,4=")),
        s"${files.toSeq.mkString("\n")}\ndidn't contain partition columns by,4")
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

  test("metadataOnly query") {
    withSQLConf(OPTIMIZER_METADATA_ONLY.key -> "true") {
      withTable("tahoe_test") {
        Seq(1L -> "a").toDF("dataCol", "partCol")
          .write
          .mode(SaveMode.Overwrite)
          .partitionBy("partCol")
          .format("delta")
          .saveAsTable("tahoe_test")
        checkAnswer(
          sql("select count(distinct partCol) FROM tahoe_test"),
          Row(1))
      }
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

      assert(files.forall(path => path.contains("by4=")),
        s"${files.toSeq.mkString("\n")}\ndidn't contain partition columns by4")

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
      val deltaLog = DeltaLog.forTable(spark, tempDir)
      val numParts = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_SNAPSHOT_PARTITIONS)
      assert(deltaLog.snapshot.state.rdd.getNumPartitions == numParts)
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
        val deltaLog = DeltaLog.forTable(spark, tempDir)
        assert(deltaLog.snapshot.state.rdd.getNumPartitions == 410)
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
        val inputFiles =
          TahoeLogFileIndex(spark, deltaLog, new Path(tempDir.getCanonicalPath))
            .inputFiles.toSeq
        assert(inputFiles.size == 5)

        val filesToDelete = inputFiles.filter(_.split("/").last.startsWith("part-00001"))
        assert(filesToDelete.size == 1)
        filesToDelete.foreach { f =>
          val deleted = tryDeleteNonRecursive(
            tempDirPath.getFileSystem(spark.sessionState.newHadoopConf()),
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
        val inputFiles =
          TahoeLogFileIndex(spark, deltaLog, new Path(tempDir.getCanonicalPath))
            .inputFiles.toSeq
        assert(inputFiles.size == 5)

        val filesToCorrupt = inputFiles.filter(_.split("/").last.startsWith("part-00001"))
        assert(filesToCorrupt.size == 1)
        val fs = tempDirPath.getFileSystem(spark.sessionState.newHadoopConf())
        filesToCorrupt.foreach { f =>
          val filePath = new Path(tempDirPath, f)
          fs.create(filePath, true).close()
        }

        val thrown = intercept[SparkException] {
          data.toDF().count()
        }
        assert(thrown.getMessage().contains("is not a Parquet file"))
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

        val inputFiles =
          TahoeLogFileIndex(spark, deltaLog, new Path(tempDir.getCanonicalPath))
            .inputFiles.toSeq

        val filesToDelete = inputFiles.take(4)
        filesToDelete.foreach { f =>
          val deleted = tryDeleteNonRecursive(
            tempDirPath.getFileSystem(spark.sessionState.newHadoopConf()),
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

      val inputFiles =
        TahoeLogFileIndex(spark, deltaLog, new Path(tempDir.getCanonicalPath))
          .inputFiles.toSeq

      val filesToDelete = inputFiles.take(4)
      filesToDelete.foreach { f =>
        val deleted = tryDeleteNonRecursive(
          tempDirPath.getFileSystem(spark.sessionState.newHadoopConf()),
          new Path(tempDirPath, f))
        assert(deleted)
      }

      val thrown = intercept[SparkException] {
        data.toDF().count()
      }
      assert(thrown.getMessage().contains("FileNotFound"))
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

  test("SC-15200: SaveAsTable on empty dataframe should create table") {
    withTable("sc15200test") {
      spark.range(0).selectExpr("id", "id as id2")
        .write.format("delta").partitionBy("id").saveAsTable("sc15200test")
      checkAnswer(spark.table("sc15200test"), Seq.empty)
    }
  }

  test("support for setting dataChange to false") {
    val tempDir = Utils.createTempDir()

    spark.range(100)
      .write
      .format("delta")
      .save(tempDir.toString)

    val df = spark.read.format("delta").load(tempDir.toString)

    df
      .write
      .format("delta")
      .mode("overwrite")
      .option("dataChange", "false")
      .save(tempDir.toString)

    val deltaLog = DeltaLog.forTable(spark, tempDir)
    val version = deltaLog.snapshot.version
    val commitActions = deltaLog.store.read(FileNames.deltaFile(deltaLog.logPath, version))
      .map(Action.fromJson)
    val fileActions = commitActions.collect { case a: FileAction => a }

     assert(fileActions.forall(!_.dataChange))
  }

  test("dataChange is by default set to true") {
    val tempDir = Utils.createTempDir()

    spark.range(100)
      .write
      .format("delta")
      .save(tempDir.toString)

    val df = spark.read.format("delta").load(tempDir.toString)

    df
      .write
      .format("delta")
      .mode("overwrite")
      .save(tempDir.toString)

    val deltaLog = DeltaLog.forTable(spark, tempDir)
    val version = deltaLog.snapshot.version
    val commitActions = deltaLog.store.read(FileNames.deltaFile(deltaLog.logPath, version))
      .map(Action.fromJson)
    val fileActions = commitActions.collect { case a: FileAction => a }

    assert(fileActions.forall(_.dataChange))
  }
}
