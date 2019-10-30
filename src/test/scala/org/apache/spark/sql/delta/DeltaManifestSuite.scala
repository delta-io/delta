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

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Paths

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

class DeltaManifestSuite extends QueryTest
  with ManifestWriterProvider
  with SharedSparkSession
  with SQLTestUtils {

  val schema = new StructType()
    .add(StructField("id", IntegerType, false))
    .add(StructField("type", StringType, false))
    .add(StructField("group", StringType, false))
    .add(StructField("value", StringType, false))

  test("Support for hive SymlinkTextInputFormat manifest generation") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }

      val testPath = tempDir.getCanonicalPath
      val df = createDataFrame(Seq(
        Row(1, "t1", "g1", "t1 - g1 - 1"),
        Row(2, "t1", "g1", "t1 - g1 - 2"),
        Row(3, "t1", "g1", "t1 - g1 - 3"),
        Row(4, "t1", "g2", "t1 - g2 - 4")
      ))

      df
        .write
        .format("delta")
        .mode("append")
        .save(testPath)

      val writer = createManifestWriter(spark)
      val log = DeltaLog.forTable(spark, tempDir)
      val manifest = writer.write(log.fs, log.snapshot)
      val manifestsPaths = listFiles(log.fs, manifest.path).map(_.toString).toSeq
      val inputFiles = spark.read.format("delta")
          .load(tempDir.toString)
          .inputFiles
          .toSeq

      assert(manifestsPaths.size === 1)
      assertManifestContains(log.fs, inputFiles, manifestsPaths.head)
    }
  }

  test("Support for hive SymlinkTextInputFormat manifest generation multiple paths") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }

      val testPath = tempDir.getCanonicalPath
      val df1 = createDataFrame(Seq(
        Row(1, "t1", "g1", "t1 - g1 - 1"),
        Row(2, "t1", "g1", "t1 - g1 - 2"),
        Row(3, "t1", "g1", "t1 - g1 - 3"),
        Row(4, "t1", "g2", "t1 - g2 - 4")
      ))

      val df2 = createDataFrame(Seq(
        Row(5, "t2", "g2", "t2 - g2 - 5"),
        Row(6, "t2", "g2", "t2 - g2 - 6"),
        Row(7, "t2", "g2", "t2 - g2 - 7"),
        Row(8, "t2", "g1", "t2 - g1 - 8")
      ))

      df1
        .write
        .format("delta")
        .mode("append")
        .save(testPath)

      df2
        .write
        .format("delta")
        .mode("append")
        .save(testPath)

      val writer = createManifestWriter(spark)
      val deltaLog = DeltaLog.forTable(spark, tempDir)
      val manifest1 = writer.write(deltaLog.fs, deltaLog.getSnapshotAt(0), tempDir + "/_manifest1")
      val manifest2 = writer.write(deltaLog.fs, deltaLog.getSnapshotAt(1), tempDir + "/_manifest2")
      val manifestsPaths1 = listFiles(deltaLog.fs, manifest1.path).map(_.toString).toSeq
      val manifestsPaths2 = listFiles(deltaLog.fs, manifest2.path).map(_.toString).toSeq

      val inputFiles1 = spark.read.format("delta")
        .option("versionAsOf", 0)
        .load(tempDir.toString)
        .inputFiles
        .toSeq

      val inputFiles2 = spark.read.format("delta")
        .option("versionAsOf", 1)
        .load(tempDir.toString)
        .inputFiles
        .toSeq

      assert(manifestsPaths1.size === 1)
      assertManifestContains(deltaLog.fs, inputFiles1, manifestsPaths1.head)
      assert(manifestsPaths1.head === "file:" + tempDir + "/_manifest1/manifest.txt")

      assert(manifestsPaths2.size === 1)
      assertManifestContains(deltaLog.fs, inputFiles2, manifestsPaths2.head)
      assert(manifestsPaths2.head === "file:" + tempDir + "/_manifest2/manifest.txt")
    }
  }

  test("Support for hive SymlinkTextInputFormat manifest generation with partitions") {
    withTempDir { tempDir =>
      if (tempDir.exists()) {
        assert(tempDir.delete())
      }

      val testPath = tempDir.getCanonicalPath
      val df = createDataFrame(Seq(
        Row(1, "t1", "g1", "t1 - g1 - 1"),
        Row(2, "t1", "g1", "t1 - g1 - 2"),
        Row(3, "t1", "g1", "t1 - g1 - 3"),
        Row(4, "t1", "g2", "t1 - g2 - 4"),

        Row(5, "t2", "g2", "t2 - g2 - 5"),
        Row(6, "t2", "g2", "t2 - g2 - 6"),
        Row(7, "t2", "g2", "t2 - g2 - 7"),
        Row(8, "t2", "g1", "t2 - g1 - 8")
      ))

      df
        .write
        .format("delta")
        .partitionBy("type", "group")
        .mode("append")
        .save(testPath)

      val writer = createManifestWriter(spark)
      val log = DeltaLog.forTable(spark, tempDir)
      val manifest = writer.write(log.fs, log.snapshot)
      val expectedPath = "file:" + testPath + "/_symlink_format_manifest"
      val manifestsPaths = listFiles(log.fs, manifest.path).map(_.toString).toSeq
      val inputFiles = spark.read.format("delta")
        .load(tempDir.toString)
        .inputFiles
        .toSeq

      val filePartitions = inputFiles
        .map(Paths.get(_))
        .groupBy(_.getParent.toString.substring(testPath.length + 5))
        .mapValues(_.map(_.toString))

      assert(filePartitions.size === 4)
      assert(manifestsPaths.size === 4)

      assert(filePartitions.contains("/type=t1/group=g1"))
      assert(filePartitions.contains("/type=t1/group=g2"))
      assert(filePartitions.contains("/type=t2/group=g1"))
      assert(filePartitions.contains("/type=t2/group=g2"))

      assert(filePartitions("/type=t1/group=g1").size === 1)
      assert(filePartitions("/type=t1/group=g2").size === 1)
      assert(filePartitions("/type=t2/group=g1").size === 1)
      assert(filePartitions("/type=t2/group=g2").size === 1)

      assert(manifestsPaths.contains(expectedPath + "/type=t1/group=g1/manifest.txt"))
      assert(manifestsPaths.contains(expectedPath + "/type=t1/group=g2/manifest.txt"))
      assert(manifestsPaths.contains(expectedPath + "/type=t2/group=g1/manifest.txt"))
      assert(manifestsPaths.contains(expectedPath + "/type=t2/group=g2/manifest.txt"))

      assertManifestContains(
        log.fs,
        filePartitions("/type=t1/group=g1"),
        expectedPath + "/type=t1/group=g1/manifest.txt"
      )

      assertManifestContains(
        log.fs,
        filePartitions("/type=t1/group=g2"),
        expectedPath + "/type=t1/group=g2/manifest.txt"
      )

      assertManifestContains(
        log.fs,
        filePartitions("/type=t2/group=g1"),
        expectedPath + "/type=t2/group=g1/manifest.txt"
      )

      assertManifestContains(
        log.fs,
        filePartitions("/type=t2/group=g2"),
        expectedPath + "/type=t2/group=g2/manifest.txt"
      )
    }
  }

  def assertManifestContains(fs: FileSystem, files: Seq[String], path: String): Unit = {
    val lines = readLines(fs, path)

    assert(lines.size === files.size)
    files.foreach(f => assert(lines.contains(f)))
  }

  def createDataFrame(rows: Seq[Row]): DataFrame = {
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  def readLines(fs: FileSystem, path: String): Seq[String] = {
    val stream = fs.open(new Path(path))

    try {
      val reader = new BufferedReader(new InputStreamReader(stream, UTF_8))
      val lines = IOUtils.readLines(reader)

      lines.asScala.map(_.trim)
    } finally {
      stream.close()
    }
  }

  def listFiles(fs: FileSystem, path: String): Iterator[Path] = {
    val inner = fs.listFiles(new Path(path), true);
    val iterator = new Iterator[Path] {
      def hasNext = inner.hasNext
      def next() = inner.next().getPath
    }

    iterator
  }
}
