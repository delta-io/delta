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

import java.io.File
import java.net.URI
import java.util.Random

import org.apache.spark.sql.delta.commands.DeltaGenerateCommand
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.util.Progressable
import org.apache.spark.sql._
import org.apache.spark.sql.delta.hooks.{GenerateSymlinkManifest, GenerateJsonManifest, JsonManifest}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

class GenerateSymlinkManifestSuite
  extends DeltaGenerateManifestSuiteBase
    with DeltaSQLCommandTest {
  override protected def generateSymlinkManifest(tablePath: String): Unit = {
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    GenerateSymlinkManifest.generateFullManifest(spark, deltaLog)
  }

  override protected def getManifestLocation(): String =
    GenerateSymlinkManifest.MANIFEST_LOCATION

  override protected def mode(): String = GenerateSymlinkManifest.manifestType.mode

  override protected def readManifestAsDataFilePaths(spark: SparkSession,
                                                     manifestPath: String): Dataset[String] = {
    import spark.implicits._
    spark.read.text(manifestPath).select("value").as[String]
  }

  override protected def name(): String = GenerateSymlinkManifest.name

  override protected def deltaConfig(): DeltaConfig[Boolean] =
    DeltaConfigs.SYMLINK_FORMAT_MANIFEST_ENABLED
}

class JsonGenerateManifestSuite
  extends DeltaGenerateManifestSuiteBase
    with DeltaSQLCommandTest {
  override protected def generateSymlinkManifest(tablePath: String): Unit = {
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    GenerateJsonManifest.generateFullManifest(spark, deltaLog)
  }

  override protected def getManifestLocation(): String =
    GenerateJsonManifest.MANIFEST_LOCATION

  override protected def mode(): String = GenerateJsonManifest.manifestType.mode

  override protected def readManifestAsDataFilePaths(spark: SparkSession,
                                                     manifestPath: String): Dataset[String] = {
    import spark.implicits._
    spark.read
      .text(manifestPath)
      .select(from_json($"value".cast(StringType),
        Encoders.product[JsonManifest].schema).as("data"))
      .select("data.*")
      .as[JsonManifest]
      .flatMap(m => m.entries)
      .map(e => e.url)
  }

  override protected def name(): String = GenerateJsonManifest.name

  override protected def deltaConfig(): DeltaConfig[Boolean] =
    DeltaConfigs.JSON_FORMAT_MANIFEST_ENABLED
}

trait DeltaGenerateManifestSuiteBase extends QueryTest with SharedSparkSession {

  import testImplicits._

  test("full manifest: non-partitioned table") {
    withTempDir { tablePath =>
      tablePath.delete()

      def write(parallelism: Int): Unit = {
        spark
          .createDataset(spark.sparkContext.parallelize(1 to 100, parallelism))
          .write
          .format("delta")
          .mode("overwrite")
          .save(tablePath.toString)
      }

      write(7)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 7)

      // Reduce files
      write(5)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 7)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 5)

      // Remove all data
      spark.emptyDataset[Int].write.format("delta").mode("overwrite").save(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 5)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 1)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)

      // delete all data
      write(5)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 1)
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tablePath.toString)
      deltaTable.delete()
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 0)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)
    }
  }

  test("full manifest: partitioned table") {
    withTempDir { tablePath =>
      tablePath.delete()

      def write(parallelism: Int, partitions1: Int, partitions2: Int): Unit = {
        spark.createDataset(spark.sparkContext.parallelize(1 to 100, parallelism)).toDF("value")
          .withColumn("part1", $"value" % partitions1)
          .withColumn("part2", $"value" % partitions2)
          .write.format("delta").partitionBy("part1", "part2")
          .mode("overwrite").save(tablePath.toString)
      }

      write(10, 10, 10)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)
      generateSymlinkManifest(tablePath.toString)
      // 10 files each in ../part1=X/part2=X/ for X = 0 to 9
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 100)

      // Reduce # partitions on both dimensions
      write(1, 1, 1)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 100)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 1)

      // Increase # partitions on both dimensions
      write(5, 5, 5)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 1)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 25)

      // Increase # partitions on only one dimension
      write(5, 10, 5)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 25)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 50)

      // Remove all data
      spark.emptyDataset[Int].toDF("value")
        .withColumn("part1", $"value" % 10)
        .withColumn("part2", $"value" % 10)
        .write.format("delta").mode("overwrite").save(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 50)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 0)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)

      // delete all data
      write(5, 5, 5)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 25)
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tablePath.toString)
      deltaTable.delete()
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 0)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)
    }
  }

  test("full manifest: throw error on non delta table paths") {
    withTempDir { dir =>
      val modeStr = mode()

      var e = intercept[AnalysisException] {
        spark.sql(s"GENERATE $modeStr FOR TABLE delta.`$dir`")
      }
      assert(e.getMessage.contains("not found"))

      spark.range(2).write.format("parquet").mode("overwrite").save(dir.toString)

      e = intercept[AnalysisException] {
        spark.sql(s"GENERATE $modeStr FOR TABLE delta.`$dir`")
      }
      assert(e.getMessage.contains("table not found"))

      e = intercept[AnalysisException] {
        spark.sql(s"GENERATE $modeStr FOR TABLE parquet.`$dir`")
      }
      assert(e.getMessage.contains("not found"))
    }
  }
  test("incremental manifest: table property controls post commit manifest generation") {
    withTempDir { tablePath =>
      tablePath.delete()

      def writeWithIncrementalManifest(enabled: Boolean, numFiles: Int): Unit = {
        withIncrementalManifest(tablePath, enabled) {
          spark.createDataset(spark.sparkContext.parallelize(1 to 100, numFiles))
            .write.format("delta").mode("overwrite").save(tablePath.toString)
        }
      }

      writeWithIncrementalManifest(enabled = false, numFiles = 1)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)

      // Enabling it should automatically generate manifest files
      writeWithIncrementalManifest(enabled = true, numFiles = 2)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 2)

      // Disabling it should stop updating existing manifest files
      writeWithIncrementalManifest(enabled = false, numFiles = 3)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 2)
    }
  }

  test("incremental manifest: unpartitioned table") {
    withTempDir { tablePath =>
      tablePath.delete()

      def write(numFiles: Int): Unit = withIncrementalManifest(tablePath, enabled = true) {
        spark.createDataset(spark.sparkContext.parallelize(1 to 100, numFiles))
          .write.format("delta").mode("overwrite").save(tablePath.toString)
      }

      write(1)
      // first write won't generate automatic manifest as mode enable after first write
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)

      // Increase files
      write(7)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 7)

      // Reduce files
      write(5)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 5)

      // Remove all data
      spark.emptyDataset[Int].write.format("delta").mode("overwrite").save(tablePath.toString)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 1)
    }
  }

  test("incremental manifest: partitioned table") {
    withTempDir { tablePath =>
      tablePath.delete()

      def writePartitioned(parallelism: Int, numPartitions1: Int, numPartitions2: Int): Unit = {
        withIncrementalManifest(tablePath, enabled = true) {
          val input =
            if (parallelism == 0) spark.emptyDataset[Int]
            else spark.createDataset(spark.sparkContext.parallelize(1 to 100, parallelism))
          input.toDF("value")
            .withColumn("part1", $"value" % numPartitions1)
            .withColumn("part2", $"value" % numPartitions2)
            .write.format("delta").partitionBy("part1", "part2")
            .mode("overwrite").save(tablePath.toString)
        }
      }

      writePartitioned(1, 1, 1)
      // Manifests wont be generated in the first write because `withIncrementalManifest` will
      // enable manifest generation only after the first write defines the table log.
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)

      writePartitioned(10, 10, 10)
      // 10 files each in ../part1=X/part2=X/ for X = 0 to 9 (so only 10 subdirectories)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 100)

      // Update such that 1 file is removed and 1 file is added in another partition
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tablePath.toString)
      deltaTable.updateExpr("value = 1", Map("part1" -> "0", "value" -> "-1"))
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 100)

      // Delete such that 1 file is removed
      deltaTable.delete("value = -1")
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 99)

      // Reduce # partitions on both dimensions
      writePartitioned(1, 1, 1)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 1)

      // Increase # partitions on both dimensions
      writePartitioned(5, 5, 5)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 25)

      // Increase # partitions on only one dimension
      writePartitioned(5, 10, 5)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 50)

      // Remove all data
      writePartitioned(0, 1, 1)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 0)
    }
  }

  test("incremental manifest: generate full manifest if manifest did not exist") {
    withTempDir { tablePath =>

      def write(numPartitions: Int): Unit = {
        spark.range(0, 100, 1, 1).toDF("value").withColumn("part", $"value" % numPartitions)
          .write.format("delta").partitionBy("part").mode("append").save(tablePath.toString)
      }

      write(10)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)

      withIncrementalManifest(tablePath, enabled = true) {
        write(1)  // update only one partition
      }
      // Manifests should be generated for all partitions
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 11)
    }
  }

  test("incremental manifest: failure to generate manifest throws exception") {
    withTempDir { tablePath =>
      tablePath.delete()

      import SymlinkManifestFailureTestFileSystem._

      withSQLConf(
          s"fs.$SCHEME.impl" -> classOf[SymlinkManifestFailureTestFileSystem].getName,
          s"fs.$SCHEME.impl.disable.cache" -> "true",
          s"fs.AbstractFileSystem.$SCHEME.impl" ->
            classOf[SymlinkManifestFailureTestAbstractFileSystem].getName,
          s"fs.AbstractFileSystem.$SCHEME.impl.disable.cache" -> "true") {
        def write(numFiles: Int): Unit = withIncrementalManifest(tablePath, enabled = true) {
          spark.createDataset(spark.sparkContext.parallelize(1 to 100, numFiles))
            .write.format("delta").mode("overwrite").save(s"$SCHEME://$tablePath")
        }

        write(1) // first write enables the property
        val ex = catalyst.util.quietly {
          intercept[RuntimeException] { write(2) }
        }

        assert(ex.getMessage().contains(name()))
        assert(ex.getCause().toString.contains("Test exception"))
      }
    }
  }

  test("special partition column names") {

    def assertColNames(inputStr: String): Unit = withClue(s"input: $inputStr") {
      withTempDir { tablePath =>
        tablePath.delete()
        val inputLines = inputStr.trim.stripMargin.trim.split("\n").toSeq
        require(inputLines.size > 0)
        val input = spark.read.json(inputLines.toDS)
        val partitionCols = input.schema.fieldNames
        val inputWithValue = input.withColumn("value", lit(1))

        inputWithValue.write.format("delta").partitionBy(partitionCols: _*).save(tablePath.toString)
        generateSymlinkManifest(tablePath.toString)
        assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = inputLines.size)
      }
    }

    intercept[AnalysisException] {
      assertColNames("""{ " " : 0 }""")
    }
    assertColNames("""{ "%" : 0 }""")
    assertColNames("""{ "a.b." : 0 }""")
    assertColNames("""{ "a/b." : 0 }""")
    assertColNames("""{ "a_b" : 0 }""")
    intercept[AnalysisException] {
      assertColNames("""{ "a b" : 0 }""")
    }
  }

  test("special partition column values") {
    withTempDir { tablePath =>
      tablePath.delete()
      val inputStr = """
          |{ "part1" : 1,    "part2": "$0$", "value" : 1 }
          |{ "part1" : null, "part2": "_1_", "value" : 1 }
          |{ "part1" : 1,    "part2": "",    "value" : 1 }
          |{ "part1" : null, "part2": " ",   "value" : 1 }
          |{ "part1" : 1,    "part2": "  ",  "value" : 1 }
          |{ "part1" : null, "part2": "/",   "value" : 1 }
          |{ "part1" : 1,    "part2": null,  "value" : 1 }
          |"""
      val input = spark.read.json(inputStr.trim.stripMargin.trim.split("\n").toSeq.toDS)
      input.write.format("delta").partitionBy("part1", "part2").save(tablePath.toString)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 7)
    }
  }

  test("root table path with escapable chars like space") {
    withTempDir { p =>
      val tablePath = new File(p.toString, "path with space")
      spark.createDataset(spark.sparkContext.parallelize(1 to 100, 1)).toDF("value")
        .withColumn("part", $"value" % 2)
        .write.format("delta").partitionBy("part").save(tablePath.toString)

      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 2)
    }
  }

  test("full manifest: scala api") {
    withTempDir { tablePath =>
      tablePath.delete()

      def write(parallelism: Int): Unit = {
        spark.createDataset(spark.sparkContext.parallelize(1 to 100, parallelism))
          .write.format("delta").mode("overwrite").save(tablePath.toString)
      }

      write(7)
      assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)

      // Create a Delta table and call the scala api for generating manifest files
      val deltaTable = io.delta.tables.DeltaTable.forPath(tablePath.getAbsolutePath)
      deltaTable.generate(mode())
      assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 7)
    }
  }

  test("full manifest: SQL command") {
    withTable("deltaTable") {
      withTempDir { tablePath =>
        tablePath.delete()

        def write(parallelism: Int, partitions1: Int, partitions2: Int): Unit = {
          spark.createDataset(spark.sparkContext.parallelize(1 to 100, parallelism)).toDF("value")
            .withColumn("part1", $"value" % partitions1)
            .withColumn("part2", $"value" % partitions2)
            .write.format("delta").partitionBy("part1", "part2")
            .mode("overwrite")
            .option("path", tablePath.toString)
            .save(tablePath.getAbsolutePath)
        }

        def randomUpperCase(str: String) : String = {
          if (str.isEmpty) {
            str
          } else {
            val random = new Random()
            val upper = str.length
            val randomLen = random.nextInt(upper)
            val randomIndexes = Seq.fill(randomLen)(random.nextInt(upper))
            val randomStr = randomIndexes.foldLeft(str.toCharArray)(
              (arr, index) => {
                arr.update(index, arr(index).toUpper)
                arr
              })
            new String(randomStr)
          }
        }

        val path = tablePath.getAbsolutePath

        val modeStr = mode()
        write(10, 10, 10)
        assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 0)
        spark.sql(s"""GENERATE ${modeStr.toUpperCase()} FOR TABLE delta.`$path`""")

        // 10 files each in ../part1=X/part2=X/ for X = 0 to 9
        assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 100)

        // Reduce # partitions on both dimensions
        write(1, 1, 1)
        assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 100)
        spark.sql(s"""GENERATE ${randomUpperCase(modeStr)} FOR TABLE delta.`$path`""")
        assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 1)

        // Increase # partitions on both dimensions
        write(5, 5, 5)
        assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 1)
        spark.sql(s"GENERATE ${randomUpperCase(modeStr)} FOR TABLE delta.`$path`")
        assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 25)

        // Increase # partitions on only one dimension
        write(5, 10, 5)
        assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 25)
        spark.sql(s"GENERATE ${modeStr.toLowerCase()} FOR TABLE delta.`$path`")
        assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 50)

        // Remove all data
        spark.emptyDataset[Int].toDF("value")
          .withColumn("part1", $"value" % 10)
          .withColumn("part2", $"value" % 10)
          .write.format("delta").mode("overwrite").save(tablePath.toString)
        assertManifest(tablePath, expectSameFiles = false, expectedNumFiles = 50)
        spark.sql(s"GENERATE ${modeStr.toLowerCase()} FOR TABLE delta.`$path`")
        assertManifest(tablePath, expectSameFiles = true, expectedNumFiles = 0)
        assert(spark.read.format("delta")
          .load(tablePath.getAbsolutePath).count() == 0)
      }
    }
  }

  test("full manifest: SQL command - throw error on unsupported mode") {
    withTempDir { tablePath =>
      spark.range(2).write.format("delta").save(tablePath.getAbsolutePath)
      val e = intercept[IllegalArgumentException] {
        spark.sql(s"GENERATE xyz FOR TABLE delta.`${tablePath.getAbsolutePath}`")
      }
      assert(e.toString.contains("not supported"))
      DeltaGenerateCommand.modeNameToGenerationFunc.keys.foreach { modeName =>
        assert(e.toString.contains(modeName))
      }
    }
  }

  def assertManifest(tablePath: File, expectSameFiles: Boolean, expectedNumFiles: Int): Unit = {
    val deltaSnapshot = DeltaLog.forTable(spark, tablePath.toString).update()
    val manifestPath = new File(tablePath, getManifestLocation())

    if (!manifestPath.exists) {
      assert(expectedNumFiles == 0 && !expectSameFiles)
      return
    }

    // Validate the expected number of files are present in the manifest
    val filesInManifest = readManifestAsDataFilePaths(spark, manifestPath.toString)
      .map { _.stripPrefix("file:") }
      .toDF("file")
    assert(filesInManifest.count() == expectedNumFiles)

    // Validate that files in the latest version of DeltaLog is same as those in the manifest
    val filesInLog = deltaSnapshot.allFiles.map { addFile =>
      // Note: this unescapes the relative path in `addFile`
      DeltaFileOperations.absolutePath(tablePath.toString, addFile.path).toString
    }.toDF("file")
    if (expectSameFiles) {
      checkAnswer(filesInManifest, filesInLog.toDF())

      // Validate that each file in the manifest is actually present in table. This mainly checks
      // whether the file names in manifest are not escaped and therefore are readable directly
      // by Hadoop APIs.
      val fs = new Path(manifestPath.toString).getFileSystem(spark.sessionState.newHadoopConf())
      filesInManifest.as[String].collect().foreach { p =>
        assert(fs.exists(new Path(p)), s"path $p in manifest not found in file system")
      }
    } else {
      assert(filesInManifest.as[String].collect().toSet != filesInLog.as[String].collect().toSet)
    }

    // If there are partitioned files, make sure the partitions values read from them are the
    // same as those in the table.
    val partitionCols = deltaSnapshot.metadata.partitionColumns.map(x => s"`$x`")
    if (partitionCols.nonEmpty && expectSameFiles && expectedNumFiles > 0) {
      val partitionsInManifest = spark.read.text(manifestPath.toString)
        .selectExpr(partitionCols: _*).distinct()
      val partitionsInData = spark.read.format("delta").load(tablePath.toString)
        .selectExpr(partitionCols: _*).distinct()
      checkAnswer(partitionsInManifest, partitionsInData)
    }
  }

  protected def withIncrementalManifest(tablePath: File, enabled: Boolean)(func: => Unit): Unit = {
    if (tablePath.exists()) {
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      val latestMetadata = deltaLog.update().metadata
      if (deltaConfig().fromMetaData(latestMetadata) != enabled) {
        // Update the metadata of the table
        val config = Map(deltaConfig().key -> enabled.toString)
        val txn = deltaLog.startTransaction()
        val metadata = txn.metadata
        val newMetadata = metadata.copy(configuration = metadata.configuration ++ config)
        txn.commit(newMetadata :: Nil, DeltaOperations.SetTableProperties(config))
      }
    }
    func
  }

  protected def generateSymlinkManifest(tablePath: String)
  protected def getManifestLocation(): String
  protected def mode(): String
  protected def readManifestAsDataFilePaths(spark: SparkSession,
                                            manifestPath: String): Dataset[String]
  protected def name(): String
  protected def deltaConfig(): DeltaConfig[Boolean]

}

class SymlinkManifestFailureTestAbstractFileSystem(
    uri: URI,
    conf: org.apache.hadoop.conf.Configuration)
  extends org.apache.hadoop.fs.DelegateToFileSystem(
    uri,
    new SymlinkManifestFailureTestFileSystem,
    conf,
    SymlinkManifestFailureTestFileSystem.SCHEME,
    false) {

  // Implementation copied from RawLocalFs
  import org.apache.hadoop.fs.local.LocalConfigKeys
  import org.apache.hadoop.fs._

  override def getUriDefaultPort(): Int = -1
  override def getServerDefaults(): FsServerDefaults = LocalConfigKeys.getServerDefaults()
  override def isValidName(src: String): Boolean = true
}


class SymlinkManifestFailureTestFileSystem extends RawLocalFileSystem {

  private var uri: URI = _
  override def getScheme: String = SymlinkManifestFailureTestFileSystem.SCHEME
  private val expectedFailurePath = Seq(GenerateSymlinkManifest.MANIFEST_LOCATION,
    GenerateJsonManifest.MANIFEST_LOCATION)

  override def initialize(name: URI, conf: Configuration): Unit = {
    uri = URI.create(name.getScheme + ":///")
    super.initialize(name, conf)
  }

  override def getUri(): URI = if (uri == null) {
    // RawLocalFileSystem's constructor will call this one before `initialize` is called.
    // Just return the super's URI to avoid NPE.
    super.getUri
  } else {
    uri
  }

  // Create function used by the parquet writer
  override def create(path: Path,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    val currentPath = path.toString
    if (expectedFailurePath.exists(currentPath.contains(_))) {
      throw new RuntimeException("Test exception")
    }
    super.create(path, overwrite, bufferSize, replication, blockSize, null)
  }
}

object SymlinkManifestFailureTestFileSystem {
  val SCHEME = "testScheme"
}
