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

import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaExceptionTestUtils, DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * Common functions used across CONVERT TO DELTA test suites. We separate out these functions
 * so that we can re-use them in tests using Hive support. Tests that leverage Hive support cannot
 * extend the `SharedSparkSession`, therefore we keep this utility class as bare-bones as possible.
 */
trait ConvertToDeltaTestUtils extends QueryTest
    with DeltaExceptionTestUtils { self: DeltaSQLTestUtils =>

  protected def collectStatisticsStringOption(collectStats: Boolean): String = Option(collectStats)
    .filterNot(identity).map(_ => "NO STATISTICS").getOrElse("")

  protected def simpleDF = spark.range(100)
    .withColumn("key1", col("id") % 2)
    .withColumn("key2", col("id") % 3 cast "String")

  protected def convertToDelta(identifier: String, partitionSchema: Option[String] = None,
      collectStats: Boolean = true): Unit

  protected val blockNonDeltaMsg = "A transaction log for Delta was found at"
  protected val parquetOnlyMsg = "CONVERT TO DELTA only supports parquet tables"
  protected val invalidParquetMsg = " not a Parquet file. Expected magic number at tail"
  // scalastyle:off deltahadoopconfiguration
  protected def sessionHadoopConf = spark.sessionState.newHadoopConf
  // scalastyle:on deltahadoopconfiguration

  protected def deltaRead(df: => DataFrame): Boolean = {
    val analyzed = df.queryExecution.analyzed
    analyzed.find {
      case DeltaTable(_: TahoeLogFileIndex) => true
      case _ => false
    }.isDefined
  }

  protected def writeFiles(
      dir: String,
      df: DataFrame,
      format: String = "parquet",
      partCols: Seq[String] = Nil,
      mode: String = "overwrite"): Unit = {
    if (partCols.nonEmpty) {
      df.write.partitionBy(partCols: _*).format(format).mode(mode).save(dir)
    } else {
      df.write.format(format).mode(mode).save(dir)
    }
  }
}

trait ConvertToDeltaSuiteBaseCommons extends ConvertToDeltaTestUtils
  with SharedSparkSession
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest
  with DeltaTestUtilsForTempViews

/** Tests for CONVERT TO DELTA that can be leveraged across SQL and Scala APIs. */
trait ConvertToDeltaSuiteBase extends ConvertToDeltaSuiteBaseCommons
  with ConvertToDeltaHiveTableTests {

  import org.apache.spark.sql.functions._
  import testImplicits._

  // Use different batch sizes to cover different merge schema code paths.
  protected def testSchemaMerging(testName: String)(block: => Unit): Unit = {
    Seq("1", "5").foreach { batchSize =>
      test(s"$testName - batch size: $batchSize") {
        withSQLConf(
          DeltaSQLConf.DELTA_IMPORT_BATCH_SIZE_SCHEMA_INFERENCE.key -> batchSize) {
          block
        }
      }
    }
  }

  test("convert with collectStats true") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir, simpleDF)
      convertToDelta(s"parquet.`$tempDir`", collectStats = true)
      val deltaLog = DeltaLog.forTable(spark, tempDir)
      val history = io.delta.tables.DeltaTable.forPath(tempDir).history()
      checkAnswer(
        spark.read.format("delta").load(tempDir),
        simpleDF
      )
      assert(history.count == 1)
      val statsDf = deltaLog.unsafeVolatileSnapshot.allFiles
          .select(from_json($"stats", deltaLog.unsafeVolatileSnapshot.statsSchema)
          .as("stats")).select("stats.*")
      assert(statsDf.filter($"numRecords".isNull).count == 0)
      assert(statsDf.agg(sum("numRecords")).as[Long].head() == simpleDF.count)
    }
  }

  test("convert with collectStats true but config set to false -> Do not collect stats") {
    withTempDir { dir =>
      withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
        val tempDir = dir.getCanonicalPath
        writeFiles(tempDir, simpleDF)
        convertToDelta(s"parquet.`$tempDir`", collectStats = true)
        val deltaLog = DeltaLog.forTable(spark, tempDir)
        val history = io.delta.tables.DeltaTable.forPath(tempDir).history()
        checkAnswer(
          spark.read.format("delta").load(tempDir),
          simpleDF
        )
        assert(history.count == 1)
        val statsDf = deltaLog.unsafeVolatileSnapshot.allFiles
          .select(from_json($"stats", deltaLog.unsafeVolatileSnapshot.statsSchema)
            .as("stats")).select("stats.*")
        assert(statsDf.filter($"numRecords".isNotNull).count == 0)
      }
    }
  }

  test("negative case: convert a non-delta path falsely claimed as parquet") {
    Seq("orc", "json", "csv").foreach { format =>
      withTempDir { dir =>
        val tempDir = dir.getCanonicalPath
        writeFiles(tempDir, simpleDF, format)
        // exception from executor reading parquet footer
        intercept[SparkException] {
          convertToDelta(s"parquet.`$tempDir`")
        }
      }
    }
  }

  test("negative case: convert non-parquet path to delta") {
    Seq("orc", "json", "csv").foreach { format =>
      withTempDir { dir =>
        val tempDir = dir.getCanonicalPath
        writeFiles(tempDir, simpleDF, format)
        val ae = intercept[AnalysisException] {
          convertToDelta(s"$format.`$tempDir`")
        }
        assert(ae.getMessage.contains(parquetOnlyMsg))
      }
    }
  }

  test("negative case: convert non-parquet file to delta") {
    Seq("orc", "json", "csv").foreach { format =>
      withTempDir { dir =>
        val tempDir = dir.getCanonicalPath
        writeFiles(tempDir, simpleDF, format)

        val se = intercept[SparkException] {
          convertToDelta(s"parquet.`$tempDir`")
        }
        assert(se.getMessage.contains(invalidParquetMsg))
      }
    }
  }

  test("filter non-parquet file for schema inference when not using catalog schema") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir + "/part=1/", Seq(1).toDF("corrupted_id"), format = "orc")
      writeFiles(tempDir + "/part=2/", Seq(2).toDF("id"))

      val tableName = "pqtable"
      withTable(tableName) {
        // Create a catalog table on top of the parquet table with the wrong schema
        // The schema should be picked from the parquet data files
        sql(s"CREATE TABLE $tableName (key1 long, key2 string) " +
          s"USING PARQUET PARTITIONED BY (part string) LOCATION '$dir'")
        // Required for discovering partition of the table
        sql(s"MSCK REPAIR TABLE $tableName")

        withSQLConf(
          "spark.sql.files.ignoreCorruptFiles" -> "false",
          DeltaSQLConf.DELTA_CONVERT_USE_CATALOG_SCHEMA.key -> "false") {
          val se = intercept[SparkException] {
            convertToDelta(tableName)
          }
          assert(se.getMessage.contains(invalidParquetMsg))
        }

        withSQLConf(
          "spark.sql.files.ignoreCorruptFiles" -> "true",
          DeltaSQLConf.DELTA_CONVERT_USE_CATALOG_SCHEMA.key -> "false") {

          convertToDelta(tableName)

          val tableId = TableIdentifier(tableName, Some("default"))
          val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, tableId)
          val expectedSchema = StructType(
            StructField("id", IntegerType, true) :: StructField("part", StringType, true) :: Nil)
          // Schema is inferred from the data
          assert(snapshot.schema.equals(expectedSchema))
        }
      }
    }
  }

  test("filter non-parquet files during delta conversion") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir + "/part=1/", Seq(1).toDF("id"), format = "json")
      writeFiles(tempDir + "/part=2/", Seq(2).toDF("id"))
      withSQLConf("spark.sql.files.ignoreCorruptFiles" -> "true") {
        convertToDelta(s"parquet.`$tempDir`", Some("part string"))
        checkAnswer(spark.read.format("delta").load(tempDir), Row(2, "2") :: Nil)
      }
    }
  }

  testQuietlyWithTempView("negative case: convert temp views to delta") { isSQLTempView =>
    val tableName = "pqtbl"
    withTable(tableName) {
      // Create view
      simpleDF.write.format("parquet").saveAsTable(tableName)
      createTempViewFromTable(tableName, isSQLTempView, format = Some("parquet"))

      // Attempt to convert to delta
      val ae = intercept[AnalysisException] {
        convertToDelta("v")
      }

      assert(ae.getMessage.contains("Converting a view to a Delta table") ||
        ae.getMessage.contains("Table default.v not found") ||
        ae.getMessage.contains("Table or view 'v' not found in database 'default'") ||
        ae.getMessage.contains("table or view `default`.`v` cannot be found"))
    }
  }

  test("negative case: missing data source name") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir, simpleDF, "parquet", Seq("key1", "key2"))
      val ae = intercept[AnalysisException] {
        convertToDelta(s"`$tempDir`", None)
      }
      assert(ae.getMessage.contains(parquetOnlyMsg))
    }
  }

  test("negative case: # partitions unmatched") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      writeFiles(path, simpleDF, partCols = Seq("key1", "key2"))

      val ae = intercept[AnalysisException] {
        convertToDelta(s"parquet.`$path`", Some("key1 long"))
      }
      assert(ae.getMessage.contains("Expecting 1 partition column(s)"))
    }
  }

  test("negative case: unmatched partition column names") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      writeFiles(path, simpleDF, partCols = Seq("key1", "key2"))

      val ae = intercept[AnalysisException] {
        convertToDelta(s"parquet.`$path`", Some("key1 long, key22 string"))
      }
      assert(ae.getMessage.contains("Expecting partition column "))
    }
  }

  test("negative case: failed to cast partition value") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val df = simpleDF.withColumn("partKey", lit("randomstring"))
      writeFiles(path, df, partCols = Seq("partKey"))
      val ae = intercept[RuntimeException] {
        convertToDelta(s"parquet.`$path`", Some("partKey int"))
      }
      assert(ae.getMessage.contains("Failed to cast partition value"))
    }
  }

  test("negative case: inconsistent directory structure") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir, simpleDF)
      writeFiles(tempDir + "/key1=1/", simpleDF)

      var ae = intercept[AnalysisException] {
        convertToDelta(s"parquet.`$tempDir`")
      }
      assert(ae.getMessage.contains("Expecting 0 partition column"))

      ae = intercept[AnalysisException] {
        convertToDelta(s"parquet.`$tempDir`", Some("key1 string"))
      }
      assert(ae.getMessage.contains("Expecting 1 partition column"))
    }
  }

  test("negative case: empty and non-existent root dir") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      val re = intercept[FileNotFoundException] {
        convertToDelta(s"parquet.`$tempDir`")
      }
      assert(re.getMessage.contains("No file found in the directory"))
      Utils.deleteRecursively(dir)

      val ae = intercept[FileNotFoundException] {
        convertToDelta(s"parquet.`$tempDir`")
      }
      assert(ae.getMessage.contains("doesn't exist"))
    }
  }

  testSchemaMerging("negative case: merge type conflict - string vs int") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir + "/part=1/", Seq(1).toDF("id"))
      for (i <- 2 to 8 by 2) {
        writeFiles(tempDir + s"/part=$i/", Seq(1).toDF("id"))
      }
      for (i <- 3 to 9 by 2) {
        writeFiles(tempDir + s"/part=$i/", Seq("1").toDF("id"))
      }

      val ex = interceptWithUnwrapping[SparkException] {
        convertToDelta(s"parquet.`$tempDir`", Some("part string"))
      }
      assert(ex.getMessage.contains("Failed to merge"))
      assert(ex.getMessage.contains("/part="), "Error message should contain the file name")
    }
  }

  test("convert a streaming parquet path: use metadata") {
    val stream = MemoryStream[Int]
    val df = stream.toDS().toDF()

    withTempDir { outputDir =>
      val checkpoint = new File(outputDir, "_check").toString
      val dataLocation = new File(outputDir, "data").toString
      val options = Map("checkpointLocation" -> checkpoint)

      // Add initial data to parquet file sink
      stream.addData(1, 2, 3)
      df.writeStream
        .options(options)
        .format("parquet")
        .trigger(Trigger.AvailableNow())
        .start(dataLocation)
        .awaitTermination()

      // Add non-streaming data: this should be ignored in conversion.
      spark.range(10, 20).write.mode("append").parquet(dataLocation)
      sql(s"CONVERT TO DELTA parquet.`$dataLocation`")

      // Write data to delta
      stream.addData(4, 5, 6)
      df.writeStream
        .options(options)
        .format("delta")
        .trigger(Trigger.AvailableNow())
        .start(dataLocation)
        .awaitTermination()

      // Should only read streaming data.
      checkAnswer(
        spark.read.format("delta").load(dataLocation),
        (1 to 6).map { Row(_) }
      )
    }
  }

  test("convert a streaming parquet path: ignore metadata") {
    val stream = MemoryStream[Int]
    val df = stream.toDS().toDF("col1")

    withTempDir { outputDir =>
      val checkpoint = new File(outputDir, "_check").toString
      val dataLocation = new File(outputDir, "data").toString
      val options = Map(
        "checkpointLocation" -> checkpoint
      )

      // Add initial data to parquet file sink
      stream.addData(1 to 5)
      df.writeStream
        .options(options)
        .format("parquet")
        .trigger(Trigger.AvailableNow())
        .start(dataLocation)
        .awaitTermination()

      // Add non-streaming data: this should not be ignored in conversion.
      spark.range(11, 21).select('id.cast("int") as "col1")
        .write.mode("append").parquet(dataLocation)

      withSQLConf(("spark.databricks.delta.convert.useMetadataLog", "false")) {
        sql(s"CONVERT TO DELTA parquet.`$dataLocation`")
      }

      // Write data to delta
      stream.addData(6 to 10)
      df.writeStream
        .options(options)
        .format("delta")
        .trigger(Trigger.AvailableNow())
        .start(dataLocation)
        .awaitTermination()

      // Should read all data not just streaming data
      checkAnswer(
        spark.read.format("delta").load(dataLocation),
        (1 to 20).map { Row(_) }
      )
    }
  }

  test("convert a parquet path") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir, simpleDF, partCols = Seq("key1", "key2"))
      convertToDelta(s"parquet.`$tempDir`", Some("key1 long, key2 string"))


      // reads actually went through Delta
      assert(deltaRead(spark.read.format("delta").load(tempDir).select("id")))

      // query through Delta is correct
      checkAnswer(
        spark.read.format("delta").load(tempDir).where("key1 = 0").select("id"),
        simpleDF.filter("id % 2 == 0").select("id"))


      // delta writers went through
      writeFiles(
        tempDir, simpleDF, format = "delta", partCols = Seq("key1", "key2"), mode = "append")

      checkAnswer(
        spark.read.format("delta").load(tempDir).where("key1 = 1").select("id"),
        simpleDF.union(simpleDF).filter("id % 2 == 1").select("id"))
    }
  }

  private def testSpecialCharactersInDirectoryNames(c: String, expectFailure: Boolean): Unit = {
    test(s"partition column names and values contain '$c'") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath

        val key1 = s"${c}key1${c}${c}"
        val key2 = s"${c}key2${c}${c}"

        val valueA = s"${c}some${c}${c}value${c}A"
        val valueB = s"${c}some${c}${c}value${c}B"
        val valueC = s"${c}some${c}${c}value${c}C"
        val valueD = s"${c}some${c}${c}value${c}D"

        val df1 = spark.range(3)
          .withColumn(key1, lit(valueA))
          .withColumn(key2, lit(valueB))
        val df2 = spark.range(4, 7)
          .withColumn(key1, lit(valueC))
          .withColumn(key2, lit(valueD))
        val df = df1.union(df2)
        writeFiles(path, df, format = "parquet", partCols = Seq(key1, key2))

        if (expectFailure) {
          val e = intercept[AnalysisException] {
            convertToDelta(s"parquet.`$path`", Some(s"`$key1` string, `$key2` string"))
          }
          assert(e.getMessage.contains("invalid character"))
        } else {
          convertToDelta(s"parquet.`$path`", Some(s"`$key1` string, `$key2` string"))

          // missing one char from valueA, so no match
          checkAnswer(
            spark.read.format("delta").load(path).where(s"`$key1` = '${c}some${c}value${c}A'")
              .select("id"), Nil)

          checkAnswer(
            spark.read.format("delta").load(path)
              .where(s"`$key1` = '$valueA' and `$key2` = '$valueB'").select("id"),
            Row(0) :: Row(1) :: Row(2) :: Nil)

          checkAnswer(
            spark.read.format("delta").load(path).where(s"`$key2` = '$valueD' and id > 4")
              .select("id"),
            Row(5) :: Row(6) :: Nil)
        }
      }
    }
  }

  " ,;{}()\n\t=".foreach { char =>
    testSpecialCharactersInDirectoryNames(char.toString, expectFailure = true)
  }
  testSpecialCharactersInDirectoryNames("%!@#$%^&*-", expectFailure = false)
  testSpecialCharactersInDirectoryNames("?.+<_>|/", expectFailure = false)

  test("can ignore empty sub-directories") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      val fs = new Path(tempDir).getFileSystem(sessionHadoopConf)

      writeFiles(tempDir + "/key1=1/", Seq(1).toDF)
      assert(fs.mkdirs(new Path(tempDir + "/key1=2/")))
      assert(fs.mkdirs(new Path(tempDir + "/random_dir/")))
      convertToDelta(s"parquet.`$tempDir`", Some("key1 string"))
      checkAnswer(spark.read.format("delta").load(tempDir), Row(1, "1"))
    }
  }

  test("allow file names to have = character") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir + "/part=1/", Seq(1).toDF("id"))

      val fs = new Path(tempDir).getFileSystem(sessionHadoopConf)
      // Rename the parquet file in partition "part=1" with something containing "="
      val files = fs.listStatus(new Path(tempDir + "/part=1/"))
        .map(_.getPath)
        .filter(path => !path.getName.startsWith("_") && !path.getName.startsWith("."))

      assert(files.length == 1)
      fs.rename(
        files.head, new Path(files.head.getParent.getName, "some-data-id=1.snappy.parquet"))

      convertToDelta(s"parquet.`$tempDir`", Some("part string"))
      checkAnswer(spark.read.format("delta").load(tempDir), Row(1, "1"))
    }
  }

  test("allow file names to not have .parquet suffix") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir + "/part=1/", Seq(1).toDF("id"))
      writeFiles(tempDir + "/part=2/", Seq(2).toDF("id"))

      val fs = new Path(tempDir).getFileSystem(sessionHadoopConf)
      // Remove the suffix of the parquet file in partition "part=1"
      val files = fs.listStatus(new Path(tempDir + "/part=1/"))
        .map(_.getPath)
        .filter(path => !path.getName.startsWith("_") && !path.getName.startsWith("."))

      assert(files.length == 1)
      fs.rename(files.head, new Path(files.head.getParent.toString, "unknown_suffix"))

      convertToDelta(s"parquet.`$tempDir`", Some("part string"))
      checkAnswer(spark.read.format("delta").load(tempDir), Row(1, "1") :: Row(2, "2") :: Nil)
    }
  }

  test("backticks") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir, simpleDF)

      // wrap parquet with backticks should work
      convertToDelta(s"`parquet`.`$tempDir`", None)
      checkAnswer(spark.read.format("delta").load(tempDir), simpleDF)

      // path with no backticks should fail parsing
      intercept[ParseException] {
        convertToDelta(s"parquet.$tempDir")
      }
    }
  }

  test("overlapping partition and data columns") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      val df = spark.range(1)
        .withColumn("partKey1", lit("1"))
        .withColumn("partKey2", lit("2"))
      df.write.parquet(tempDir + "/partKey1=1")
      convertToDelta(s"parquet.`$tempDir`", Some("partKey1 int"))

      // Same as in [[HadoopFsRelation]], for common columns,
      // respecting the order of data schema but the type of partition schema
      checkAnswer(spark.read.format("delta").load(tempDir), Row(0, 1, "2"))
    }
  }

  test("some partition value is null") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      val df1 = Seq(0).toDF("id")
        .withColumn("key1", lit("A1"))
        .withColumn("key2", lit(null))

      val df2 = Seq(1).toDF("id")
        .withColumn("key1", lit(null))
        .withColumn("key2", lit(100))

      writeFiles(tempDir, df1.union(df2), partCols = Seq("key1", "key2"))
      convertToDelta(s"parquet.`$tempDir`", Some("key1 string, key2 int"))
      checkAnswer(
        spark.read.format("delta").load(tempDir).where("key2 is null")
          .select("id"), Row(0))
      checkAnswer(
        spark.read.format("delta").load(tempDir).where("key1 is null")
          .select("id"), Row(1))
      checkAnswer(
        spark.read.format("delta").load(tempDir).where("key1 = 'A1'")
          .select("id"), Row(0))
      checkAnswer(
        spark.read.format("delta").load(tempDir).where("key2 = 100")
          .select("id"), Row(1))
    }
  }

  test("converting tables with dateType partition columns") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      val df1 = Seq(0).toDF("id").withColumn("key1", lit("2019-11-22").cast("date"))

      val df2 = Seq(1).toDF("id").withColumn("key1", lit(null))

      writeFiles(tempDir, df1.union(df2), partCols = Seq("key1"))
      convertToDelta(s"parquet.`$tempDir`", Some("key1 date"))
      checkAnswer(
        spark.read.format("delta").load(tempDir).where("key1 is null").select("id"),
        Row(1))
      checkAnswer(
        spark.read.format("delta").load(tempDir).where("key1 = '2019-11-22'").select("id"),
        Row(0))
    }
  }

  test("empty string partition value will be read back as null") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      val df1 = Seq(0).toDF("id")
        .withColumn("key1", lit("A1"))
        .withColumn("key2", lit(""))

      val df2 = Seq(1).toDF("id")
        .withColumn("key1", lit(""))
        .withColumn("key2", lit(""))

      writeFiles(tempDir, df1.union(df2), partCols = Seq("key1", "key2"))
      convertToDelta(s"parquet.`$tempDir`", Some("key1 string, key2 string"))
      checkAnswer(
        spark.read.format("delta").load(tempDir).where("key1 is null and key2 is null")
          .select("id"), Row(1))
      checkAnswer(
        spark.read.format("delta").load(tempDir).where("key1 = 'A1'")
          .select("id"), Row(0))
    }
  }

  testSchemaMerging("can merge schema with different columns") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir + "/part=1/", Seq(1).toDF("id1"))
      writeFiles(tempDir + "/part=2/", Seq(2).toDF("id2"))
      writeFiles(tempDir + "/part=3/", Seq(3).toDF("id3"))

      convertToDelta(s"parquet.`$tempDir`", Some("part string"))

      // spell out the columns as intra-batch and inter-batch merging logic may order
      // the columns differently
      val cols = Seq("id1", "id2", "id3", "part")
      checkAnswer(
        spark.read.format("delta").load(tempDir).where("id2 = 2")
          .select(cols.head, cols.tail: _*),
        Row(null, 2, null, "2") :: Nil)
    }
  }

  testSchemaMerging("can merge schema with different nullability") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir + "/part=1/", Seq(1).toDF("id"))
      val schema = new StructType().add(StructField("id", IntegerType, false))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1))), schema)
      writeFiles(tempDir + "/part=2/", df)

      convertToDelta(s"parquet.`$tempDir`", Some("part string"))
      val fields = spark.read.format("delta").load(tempDir).schema.fields.toSeq
      assert(fields.map(_.name) === Seq("id", "part"))
      assert(fields.map(_.nullable) === Seq(true, true))
    }
  }

  testSchemaMerging("can upcast in schema merging: short vs int") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir + "/part=1/", Seq(1 << 20).toDF("id"))
      writeFiles(tempDir + "/part=2/",
        Seq(1).toDF("id").select(col("id") cast ShortType))

      convertToDelta(s"parquet.`$tempDir`", Some("part string"))
      checkAnswer(
        spark.read.format("delta").load(tempDir), Row(1 << 20, "1") :: Row(1, "2") :: Nil)

      val expectedSchema = new StructType().add("id", IntegerType).add("part", StringType)
      val deltaLog = DeltaLog.forTable(spark, tempDir)
      assert(deltaLog.update().metadata.schema === expectedSchema)
    }
  }

  test("can fetch global configs") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val deltaLog = DeltaLog.forTable(spark, path)
      withSQLConf("spark.databricks.delta.properties.defaults.appendOnly" -> "true") {
        writeFiles(path, simpleDF.coalesce(1))
        convertToDelta(s"parquet.`$path`")
      }
      assert(deltaLog.snapshot.metadata.configuration("delta.appendOnly") === "true")
    }
  }

  test("convert to delta with string partition columns") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir, simpleDF, partCols = Seq("key1", "key2"))
      convertToDelta(s"parquet.`$tempDir`", Some("key1 long, key2 string"))

      // reads actually went through Delta
      assert(deltaRead(spark.read.format("delta").load(tempDir).select("id")))

      // query through Delta is correct
      checkAnswer(
        spark.read.format("delta").load(tempDir).where("key1 = 0").select("id"),
        simpleDF.filter("id % 2 == 0").select("id"))

      // delta writers went through
      writeFiles(
        tempDir, simpleDF, format = "delta", partCols = Seq("key1", "key2"), mode = "append")

      checkAnswer(
        spark.read.format("delta").load(tempDir).where("key1 = 1").select("id"),
        simpleDF.union(simpleDF).filter("id % 2 == 1").select("id"))
    }
  }

  test("convert a delta path falsely claimed as parquet") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir, simpleDF, "delta")

      // Convert to delta
      convertToDelta(s"parquet.`$tempDir`")

      // Verify that table converted to delta
      checkAnswer(
        spark.read.format("delta").load(tempDir).where("key1 = 1").select("id"),
        simpleDF.filter("id % 2 == 1").select("id"))
    }
  }

  test("converting a delta path should not error for idempotency") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir, simpleDF, "delta")
      convertToDelta(s"delta.`$tempDir`")

      checkAnswer(
        spark.read.format("delta").load(tempDir).where("key1 = 1").select("id"),
        simpleDF.filter("id % 2 == 1").select("id"))
    }
  }

  test("partition column name starting with underscore and dot") {
    withTempDir { dir =>
      val df = spark.range(100)
        .withColumn("_key1", col("id") % 2)
        .withColumn(".key2", col("id") % 7 cast "String")

      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir, df, partCols = Seq("_key1", ".key2"))

      convertToDelta(s"parquet.`$tempDir`", Some("_key1 long, `.key2` string"))

      checkAnswer(sql(s"SELECT * FROM delta.`$tempDir`"), df)
    }
  }
}

/**
 * Tests that involve tables defined in a Catalog such as Hive. We test in the sql as well as
 * hive package, where the hive package uses a proper HiveExternalCatalog to alter table definitions
 * in the HiveMetaStore. This test trait *should not* extend SharedSparkSession so that it can be
 * mixed in with the Hive test utilities.
 */
trait ConvertToDeltaHiveTableTests extends ConvertToDeltaTestUtils with DeltaSQLTestUtils {

  // Test conversion with and without the new CatalogFileManifest.
  protected def testCatalogFileManifest(testName: String)(block: (Boolean) => Unit): Unit = {
    Seq(true, false).foreach { useCatalogFileManifest =>
      test(s"$testName - $useCatalogFileManifest") {
        withSQLConf(
          DeltaSQLConf.DELTA_CONVERT_USE_CATALOG_PARTITIONS.key
            -> useCatalogFileManifest.toString) {
          block(useCatalogFileManifest)
        }
      }
    }
  }

  protected def testCatalogSchema(testName: String)(testFn: (Boolean) => Unit): Unit = {
    Seq(true, false).foreach {
      useCatalogSchema =>
        test(s"$testName - $useCatalogSchema") {
          withSQLConf(
            DeltaSQLConf.DELTA_CONVERT_USE_CATALOG_SCHEMA.key -> useCatalogSchema.toString) {
            testFn(useCatalogSchema)
          }
        }
    }
  }

  protected def getPathForTableName(tableName: String): String = {
    spark
      .sessionState
      .catalog
      .getTableMetadata(TableIdentifier(tableName, Some("default"))).location.getPath
  }

  protected def verifyExternalCatalogMetadata(tableName: String): Unit = {
    val catalog = spark.sessionState.catalog.externalCatalog.getTable("default", tableName)
    // Hive automatically adds some properties
    val cleanProps = catalog.properties.filterKeys(_ != "transient_lastDdlTime")
    assert(catalog.schema.isEmpty,
      s"Schema wasn't empty in the catalog for table $tableName: ${catalog.schema}")
    assert(catalog.partitionColumnNames.isEmpty, "Partition columns weren't empty in the " +
      s"catalog for table $tableName: ${catalog.partitionColumnNames}")
    assert(cleanProps.isEmpty,
      s"Table properties weren't empty for table $tableName: $cleanProps")
  }

  testQuietly("negative case: converting non-parquet table") {
    val tableName = "csvtable"
    withTable(tableName) {
      // Create a csv table
      simpleDF.write.partitionBy("key1", "key2").format("csv").saveAsTable(tableName)

      // Attempt to convert to delta
      val ae = intercept[AnalysisException] {
        convertToDelta(tableName, Some("key1 long, key2 string"))
      }

      // Get error message
      assert(ae.getMessage.contains(parquetOnlyMsg))
    }
  }

  testQuietly("negative case: convert parquet path to delta when there is a database called " +
    "parquet but no table or path exists") {
    val dbName = "parquet"
    withDatabase(dbName) {
      withTempDir { dir =>
        sql(s"CREATE DATABASE $dbName")

        val tempDir = dir.getCanonicalPath
        // Attempt to convert to delta
        val ae = intercept[FileNotFoundException] {
          convertToDelta(s"parquet.`$tempDir`")
        }

        // Get error message
        assert(ae.getMessage.contains("No file found in the directory"))
      }
    }
  }

  testQuietly("negative case: convert views to delta") {
    val viewName = "view"
    val tableName = "pqtbl"
    withTable(tableName) {
      // Create view
      simpleDF.write.format("parquet").saveAsTable(tableName)
      sql(s"CREATE VIEW $viewName as SELECT * from $tableName")

      // Attempt to convert to delta
      val ae = intercept[AnalysisException] {
        convertToDelta(viewName)
      }

      assert(ae.getMessage.contains("Converting a view to a Delta table"))
    }
  }

  testQuietly("negative case: converting a table that doesn't exist but the database does") {
    val dbName = "db"
    withDatabase(dbName) {
      sql(s"CREATE DATABASE $dbName")

      // Attempt to convert to delta
      val ae = intercept[AnalysisException] {
        convertToDelta(s"$dbName.faketable", Some("key1 long, key2 string"))
      }

      assert(ae.getMessage.contains("Table or view 'faketable' not found") ||
        ae.getMessage.contains(s"table or view `$dbName`.`faketable` cannot be found"))
    }
  }

  testQuietly("negative case: unmatched partition schema") {
    val tableName = "pqtable"
    withTable(tableName) {
      // Create a partitioned parquet table
      simpleDF.write.partitionBy("key1", "key2").format("parquet").saveAsTable(tableName)

      // Check the partition schema in the catalog, key1's data type is original Long.
      assert(spark.sessionState.catalog.getTableMetadata(
        TableIdentifier(tableName, Some("default"))).partitionSchema
        .equals(
          (new StructType)
            .add(StructField("key1", LongType, true))
            .add(StructField("key2", StringType, true))
        ))

      // Convert to delta with partition schema mismatch on key1's data type, which is String.
      val ae = intercept[AnalysisException] {
        convertToDelta(tableName, Some("key1 string, key2 string"))
      }

      assert(ae.getMessage.contains("CONVERT TO DELTA was called with a partition schema " +
        "different from the partition schema inferred from the catalog"))
    }
  }

  testQuietly("convert two external tables pointing to same underlying files " +
    "with differing table properties should error if conf enabled otherwise merge properties") {
    val externalTblName = "extpqtbl"
    val secondExternalTbl = "othertbl"
    withTable(externalTblName, secondExternalTbl) {
      withTempDir { dir =>
        val path = dir.getCanonicalPath

        // Create external table
        sql(s"CREATE TABLE $externalTblName " +
          s"USING PARQUET LOCATION '$path' TBLPROPERTIES ('abc'='def', 'def'='ghi') AS SELECT 1")

        // Create second external table with different table properties
        sql(s"CREATE TABLE $secondExternalTbl " +
          s"USING PARQUET LOCATION '$path' TBLPROPERTIES ('abc'='111', 'jkl'='mno')")

        // Convert first table to delta
        convertToDelta(externalTblName)

        // Verify that files converted to delta
        checkAnswer(
          sql(s"select * from delta.`$path`"), Row(1))

        // Verify first table converted to delta
        assert(spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(externalTblName, Some("default"))).provider.contains("delta"))

        // Attempt to convert second external table to delta
        val ae = intercept[AnalysisException] {
          convertToDelta(secondExternalTbl)
        }

        assert(
          ae.getMessage.contains("You are trying to convert a table which already has a delta") &&
            ae.getMessage.contains("convert.metadataCheck.enabled"))

        // Disable convert metadata check
        withSQLConf(DeltaSQLConf.DELTA_CONVERT_METADATA_CHECK_ENABLED.key -> "false") {
          // Convert second external table to delta
          convertToDelta(secondExternalTbl)

          // Check delta table configuration has updated properties
          assert(DeltaLog.forTable(spark, path).startTransaction().metadata.configuration ==
            Map("abc" -> "111", "def" -> "ghi", "jkl" -> "mno"))
        }
      }
    }
  }

  testQuietly("convert two external tables pointing to the same underlying files") {
    val externalTblName = "extpqtbl"
    val secondExternalTbl = "othertbl"
    withTable(externalTblName, secondExternalTbl) {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        writeFiles(path, simpleDF, "delta")
        val deltaLog = DeltaLog.forTable(spark, path)

        // Create external table
        sql(s"CREATE TABLE $externalTblName (key1 long, key2 string) " +
          s"USING PARQUET LOCATION '$path'")

        // Create second external table
        sql(s"CREATE TABLE $secondExternalTbl (key1 long, key2 string) " +
          s"USING PARQUET LOCATION '$path'")

        assert(deltaLog.update().version == 0)

        // Convert first table to delta
        convertToDelta(externalTblName)

        // Convert should not update version since delta log metadata is not changing
        assert(deltaLog.update().version == 0)
        // Check that the metadata in the catalog was emptied and pushed to the delta log
        verifyExternalCatalogMetadata(externalTblName)

        // Convert second external table to delta
        convertToDelta(secondExternalTbl)
        verifyExternalCatalogMetadata(secondExternalTbl)

        // Verify that underlying files converted to delta
        checkAnswer(
          sql(s"select id from delta.`$path` where key1 = 1"),
          simpleDF.filter("id % 2 == 1").select("id"))

        // Verify catalog table provider is 'delta' for both tables
        assert(spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(externalTblName, Some("default"))).provider.contains("delta"))

        assert(spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(secondExternalTbl, Some("default"))).provider.contains("delta"))

      }
    }
  }

  testQuietly("convert an external parquet table") {
    val tableName = "pqtbl"
    val externalTblName = "extpqtbl"
    withTable(tableName) {
      simpleDF.write.format("parquet").saveAsTable(tableName)

      // Get where the table is stored and try to access it using parquet rather than delta
      val path = getPathForTableName(tableName)

      // Create external table
      sql(s"CREATE TABLE $externalTblName (key1 long, key2 string) " +
        s"USING PARQUET LOCATION '$path'")

      // Convert to delta
      sql(s"convert to delta $externalTblName")

      assert(spark.sessionState.catalog.getTableMetadata(
        TableIdentifier(externalTblName, Some("default"))).provider.contains("delta"))

      // Verify that table converted to delta
      checkAnswer(
        sql(s"select key2 from delta.`$path` where key1 = 1"),
        simpleDF.filter("id % 2 == 1").select("key2"))

      checkAnswer(
        sql(s"select key2 from $externalTblName where key1 = 1"),
        simpleDF.filter("id % 2 == 1").select("key2"))
    }
  }

  testCatalogSchema("convert a parquet table with catalog schema") {
    useCatalogSchema => {
      withTempDir {
        dir =>
          // Create a parquet table with all 3 columns: id, key1 and key2
          val tempDir = dir.getCanonicalPath
          writeFiles(tempDir, simpleDF)

          val tableName = "pqtable"
          withTable(tableName) {
            // Create a catalog table on top of the parquet table excluding column id
            sql(s"CREATE TABLE $tableName (key1 long, key2 string) " +
              s"USING PARQUET LOCATION '$dir'")

            convertToDelta(tableName)

          val tableId = TableIdentifier(tableName, Some("default"))
          val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, tableId)
            val catalog_columns = Seq[StructField](
              StructField("key1", LongType, true),
              StructField("key2", StringType, true)
            )

            if (useCatalogSchema) {
              // Catalog schema is used, column id is excluded.
              assert(snapshot.metadata.schema.equals(StructType(catalog_columns)))
            } else {
              // Schema is inferred from the data, all 3 columns are included.
              assert(snapshot.metadata.schema
                .equals(StructType(StructField("id", LongType, true) +: catalog_columns)))
            }
          }
      }
    }
  }

  testQuietly("converting a delta table should not error for idempotency") {
    val tableName = "deltatbl"
    val format = "delta"
    withTable(tableName) {
      simpleDF.write.partitionBy("key1", "key2").format(format).saveAsTable(tableName)
      convertToDelta(tableName)

      // reads actually went through Delta
      val path = getPathForTableName(tableName)
      checkAnswer(
        sql(s"select id from $format.`$path` where key1 = 1"),
        simpleDF.filter("id % 2 == 1").select("id"))
    }
  }

  testQuietly("convert to delta using table name without database name") {
    val tableName = "pqtable"
    withTable(tableName) {
      // Create a parquet table
      simpleDF.write.partitionBy("key1", "key2").format("parquet").saveAsTable(tableName)

      // Convert to delta using only table name
      convertToDelta(tableName, Some("key1 long, key2 string"))

      // reads actually went through Delta
      val path = getPathForTableName(tableName)
      checkAnswer(
        sql(s"select id from delta.`$path` where key1 = 1"),
        simpleDF.filter("id % 2 == 1").select("id"))
    }
  }

  testQuietly("convert a parquet table to delta with database name as parquet") {
    val dbName = "parquet"
    val tableName = "pqtbl"
    withDatabase(dbName) {
      withTable(dbName + "." + tableName) {
        sql(s"CREATE DATABASE $dbName")
        val table = TableIdentifier(tableName, Some(dbName))
        simpleDF.write.partitionBy("key1", "key2")
          .format("parquet").saveAsTable(dbName + "." + tableName)

        convertToDelta(dbName + "." + tableName, Some("key1 long, key2 string"))

        // reads actually went through Delta
        val path = spark
          .sessionState
          .catalog
          .getTableMetadata(table).location.getPath

        checkAnswer(
          sql(s"select id from delta.`$path` where key1 = 1"),
          simpleDF.filter("id % 2 == 1").select("id"))
      }
    }
  }

  testQuietly("convert a parquet path to delta while database called parquet exists") {
    val dbName = "parquet"
    withDatabase(dbName) {
      withTempDir { dir =>
        // Create a database called parquet
        sql(s"CREATE DATABASE $dbName")

        // Create a parquet table at given path
        val tempDir = dir.getCanonicalPath
        writeFiles(tempDir, simpleDF, partCols = Seq("key1", "key2"))

        // Convert should convert the path instead of trying to find a table in that database
        convertToDelta(s"parquet.`$tempDir`", Some("key1 long, key2 string"))

        // reads actually went through Delta
        checkAnswer(
          sql(s"select id from delta.`$tempDir` where key1 = 1"),
          simpleDF.filter("id % 2 == 1").select("id"))
      }
    }
  }

  testQuietly("convert a delta table where metadata does not reflect that the table is " +
    "already converted should update the metadata") {
    val tableName = "deltatbl"
    withTable(tableName) {
      simpleDF.write.partitionBy("key1", "key2").format("parquet").saveAsTable(tableName)

      // Get where the table is stored and try to access it using parquet rather than delta
      val path = getPathForTableName(tableName)

      // Convert using path so that metadata is not updated
      convertToDelta(s"parquet.`$path`", Some("key1 long, key2 string"))

      // Call convert again
      convertToDelta(s"default.$tableName", Some("key1 long, key2 string"))

      // Metadata should be updated so we can use table name
      checkAnswer(
        sql(s"select id from default.$tableName where key1 = 1"),
        simpleDF.filter("id % 2 == 1").select("id"))
    }
  }

  testQuietly("convert a parquet table using table name") {
    val tableName = "pqtable2"
    withTable(tableName) {
      // Create a parquet table
      simpleDF.write.partitionBy("key1", "key2").format("parquet").saveAsTable(tableName)

      // Convert to delta
      convertToDelta(s"default.$tableName", Some("key1 long, key2 string"))

      // Get where the table is stored and try to access it using parquet rather than delta
      val path = getPathForTableName(tableName)


      // reads actually went through Delta
      assert(deltaRead(sql(s"select id from default.$tableName")))

      // query through Delta is correct
      checkAnswer(
        sql(s"select id from default.$tableName where key1 = 0"),
        simpleDF.filter("id % 2 == 0").select("id"))


      // delta writers went through
      writeFiles(path, simpleDF, format = "delta", partCols = Seq("key1", "key2"), mode = "append")

      checkAnswer(
        sql(s"select id from default.$tableName where key1 = 1"),
        simpleDF.union(simpleDF).filter("id % 2 == 1").select("id"))
    }
  }

  testQuietly("Convert a partitioned parquet table with partition schema autofill") {
    val tableName = "ppqtable"
    withTable(tableName) {
      // Create a partitioned parquet table
      simpleDF.write.partitionBy("key1", "key2").format("parquet").saveAsTable(tableName)

      // Convert to delta without partition schema, partition schema is autofill from catalog
      convertToDelta(tableName)

      // Verify that table is converted to delta
      assert(spark.sessionState.catalog.getTableMetadata(
        TableIdentifier(tableName, Some("default"))).provider.contains("delta"))

      // Check the partition schema in the transaction log
      val tableId = TableIdentifier(tableName, Some("default"))
      assert(DeltaLog.forTableWithSnapshot(spark, tableId)._2.metadata.partitionSchema.equals(
        (new StructType())
          .add(StructField("key1", LongType, true))
          .add(StructField("key2", StringType, true))
      ))

      // Check data in the converted delta table.
      checkAnswer(
        sql(s"SELECT id from default.$tableName where key2 = '2'"),
        simpleDF.filter("id % 3 == 2").select("id"))
    }
  }

  testCatalogFileManifest("convert partitioned parquet table with catalog partitions") {
    useCatalogFileManifest => {
      val tableName = "ppqtable"
      withTable(tableName) {
        simpleDF.write.partitionBy("key1").format("parquet").saveAsTable(tableName)
        val path = getPathForTableName(tableName)

        // Create an orphan partition
        val df = spark.range(100, 200)
          .withColumn("key1", lit(2))
          .withColumn("key2", col("id") % 4 cast "String")

        df.write.partitionBy("key1")
          .format("parquet")
          .mode("Append")
          .save(path)

        // The path should contains 3 partitions.
        val partitionDirs = new File(path).listFiles().filter(_.isDirectory)
        assert(partitionDirs.map(_.getName).sorted
          .sameElements(Array("key1=0", "key1=1", "key1=2")))

        // Catalog only contains 2 partitions.
        assert(spark.sessionState.catalog
          .listPartitions(TableIdentifier(tableName, Some("default"))).size == 2)

        // Convert table to delta
        convertToDelta(tableName)

        // Verify that table is converted to delta
        assert(spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(tableName, Some("default"))).provider.contains("delta"))

        // Check data in the converted delta table.
        if (useCatalogFileManifest) {
          // Partition "key1=2" is pruned.
          checkAnswer(sql(s"SELECT DISTINCT key1 from default.${tableName}"), spark.range(2).toDF())
        } else {
          // All partitions are preserved.
          checkAnswer(sql(s"SELECT DISTINCT key1 from default.${tableName}"), spark.range(3).toDF())
        }
      }
    }
  }

  test("external tables use correct path scheme") {
    withTempDir { dir =>
      withTable("externalTable") {
        withSQLConf(("fs.s3.impl", classOf[S3LikeLocalFileSystem].getCanonicalName)) {
          sql(s"CREATE TABLE externalTable USING parquet LOCATION 's3://$dir' AS SELECT 1")

          // Ideally we would test a successful conversion with a remote filesystem, but there's
          // no good way to set one up in unit tests. So instead we delete the data, and let the
          // FileNotFoundException tell us which scheme it was using to look for it.
          Utils.deleteRecursively(dir)

          val ex = intercept[FileNotFoundException] {
            convertToDelta("default.externalTable", None)
          }

          // If the path incorrectly used the default scheme, this would be file: at the end.
          assert(ex.getMessage.contains(s"s3:$dir doesn't exist"))
        }
      }
    }
  }

  test("can convert a partition-like table path") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      writeFiles(path, simpleDF, partCols = Seq("key1", "key2"))

      val basePath = s"$path/key1=1/"
      convertToDelta(s"parquet.`$basePath`", Some("key2 string"))

      checkAnswer(
        sql(s"select id from delta.`$basePath` where key2 = '1'"),
        simpleDF.filter("id % 2 == 1").filter("id % 3 == 1").select("id"))
    }
  }

  test("can convert table with partition overwrite") {
    val tableName = "ppqtable"
    withTable(tableName) {
      // Create table with original partitions of "key1=0" and "key1=1".
      val df = spark.range(0, 100)
        .withColumn("key1", col("id") % 2)
        .withColumn("key2", col("id") % 3 cast "String")
      df.write.format("parquet").partitionBy("key1").mode("append").saveAsTable(tableName)
      checkAnswer(sql(s"SELECT id FROM $tableName"), df.select("id"))

      val dataDir =
        spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName)).location.toString

      // Create orphan partition "key1=0;key2=3" with additional column.
      val df1 = spark.range(100, 120, 2)
        .withColumn("key1", col("id") % 2)
        .withColumn("key2", lit("3"))
      df1.write.format("parquet").partitionBy("key1", "key2").mode("append").save(dataDir)

      // Point table partition "key1=0" to the path of orphan partition "key1=0;key2=3"
      sql(s"ALTER TABLE $tableName PARTITION (key1=0) SET LOCATION '$dataDir/key1=0/key2=3/'")
      checkAnswer(sql(s"SELECT id FROM $tableName WHERE key1 = 0"), df1.select("id"))

      // ConvertToDelta should work without inferring the partition values from partition path.
      convertToDelta(tableName)

      // Verify that table is converted to delta
      assert(spark.sessionState.catalog.getTableMetadata(
        TableIdentifier(tableName, Some("default"))).provider.contains("delta"))

      // Check data in the converted delta table.
      checkAnswer(sql(s"SELECT id FROM $tableName WHERE key1 = 0"), df1.select("id"))
    }
  }

  test(s"catalog partition values contain special characters") {
    // Add interesting special characters here for test
    val specialChars = " ,;{}()\n\t=!@#$%^&*-?.+<_>|/"
    val tableName = "ppqtable"
    withTable(tableName) {
      val valueA = s"${specialChars}some${specialChars}${specialChars}value${specialChars}A"
      val valueB = s"${specialChars}some${specialChars}${specialChars}value${specialChars}B"
      val valueC = s"${specialChars}some${specialChars}${specialChars}value${specialChars}C"
      val valueD = s"${specialChars}some${specialChars}${specialChars}value${specialChars}D"

      val df1 = spark.range(3).withColumn("key1", lit(valueA)).withColumn("key2", lit(valueB))
      val df2 = spark.range(4, 7).withColumn("key1", lit(valueC)).withColumn("key2", lit(valueD))
      df1.union(df2).write.format("parquet").partitionBy("key1", "key2").saveAsTable(tableName)

      convertToDelta(tableName, Some("key1 string, key2 string"))

      // missing one char from valueA, so no match
      checkAnswer(
        spark.table(tableName)
          .where(s"key1 = '${specialChars}some${specialChars}value${specialChars}A'")
          .select("id"), Nil)

      checkAnswer(
        spark.table(tableName).where(s"key1 = '$valueA' and key2 = '$valueB'")
          .select("id"),
        Row(0) :: Row(1) :: Row(2) :: Nil)

      checkAnswer(
        spark.table(tableName).where(s"key2 = '$valueD' and id > 4")
          .select("id"),
        Row(5) :: Row(6) :: Nil)
    }
  }
}
