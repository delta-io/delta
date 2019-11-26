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

import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class ConvertToDeltaSuite
  extends ConvertToDeltaSuiteBase  with org.apache.spark.sql.delta.test.DeltaSQLCommandTest

trait ConvertToDeltaSuiteBase extends QueryTest
    with SharedSparkSession {

  import org.apache.spark.sql.functions._
  import testImplicits._

  protected def simpleDF = spark.range(100)
    .withColumn("key1", col("id") % 2)
    .withColumn("key2", col("id") % 3 cast "String")


  protected val blockNonDeltaMsg = "A transaction log for Delta Lake was found at"
  protected val parquetOnlyMsg = "CONVERT TO DELTA only supports parquet tables"

  protected def deltaRead(df: DataFrame): Boolean = {
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

  protected def convertToDelta(identifier: String, partitionSchema: Option[String] = None): Unit = {
    if (partitionSchema.isDefined) {
      io.delta.tables.DeltaTable.convertToDelta(
        spark,
        identifier,
        StructType.fromDDL(partitionSchema.get)
      )
    } else {
      io.delta.tables.DeltaTable.convertToDelta(
        spark,
        identifier
      )
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
      val re = intercept[RuntimeException] {
        convertToDelta(s"parquet.`$tempDir`")
      }
      assert(re.getMessage.contains("No file found in the directory"))
      Utils.deleteRecursively(dir)

      val ae = intercept[AnalysisException] {
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

      val exception = intercept[Exception] {
        convertToDelta(s"parquet.`$tempDir`", Some("part string"))
      }

      val realCause = exception match {
        case se: SparkException => se.getCause
        case ae: AnalysisException => ae
      }
      assert(realCause.getMessage.contains("Failed to merge"))
      assert(exception.isInstanceOf[AnalysisException] ||
        realCause.getCause.getMessage.contains("/part="),
        "Error message should contain the file name")
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

  private def testSpecialCharactersInDirectoryNames(c: String): Unit = {
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

  testSpecialCharactersInDirectoryNames(" ")
  testSpecialCharactersInDirectoryNames("=")
  testSpecialCharactersInDirectoryNames("%!@# $%^&* ()-")
  testSpecialCharactersInDirectoryNames(" ?. +<>|={}/")

  test("can ignore empty sub-directories") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      val sessionHadoopConf = spark.sessionState.newHadoopConf
      val fs = new Path(tempDir).getFileSystem(sessionHadoopConf)

      writeFiles(tempDir + "/key1=1/", Seq(1).toDF)
      assert(fs.mkdirs(new Path(tempDir + "/key1=2/")))
      assert(fs.mkdirs(new Path(tempDir + "/random_dir/")))
      convertToDelta(s"parquet.`$tempDir`", Some("key1 string"))
      checkAnswer(spark.read.format("delta").load(tempDir), Row(1, "1"))
    }
  }

  test("allow file names to not have .parquet suffix") {
    withTempDir { dir =>
      val tempDir = dir.getCanonicalPath
      writeFiles(tempDir + "/part=1/", Seq(1).toDF("id"))
      writeFiles(tempDir + "/part=2/", Seq(2).toDF("id"))

      val sessionHadoopConf = spark.sessionState.newHadoopConf
      val fs = new Path(tempDir).getFileSystem(sessionHadoopConf)
      def listFileNames: Array[String] =
        fs.listStatus(new Path(tempDir + "/part=1/"))
          .map(_.getPath)
          .filter(path => !path.getName.startsWith("_") && !path.getName.startsWith("."))
          .map(_.toUri.toString)

      val fileNames = listFileNames
      assert(fileNames.size == 1)
      fs.rename(new Path(fileNames.head), new Path(fileNames.head.stripSuffix(".parquet")))

      val newFileNames = listFileNames
      assert(fileNames === newFileNames.map(_ + ".parquet"))
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
}
