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
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.Duration

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.concurrency.PhaseLockingTestMixin
import org.apache.spark.sql.delta.concurrency.TransactionExecutionTestMixin
import org.apache.spark.sql.delta.util.{JsonUtils}
import org.apache.spark.util.ThreadUtils
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.hadoop.fs.{Path, FileSystem, FileStatus}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql._
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.functions.{lit, col}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.SparkException

class DeltaFsckSuite extends QueryTest
  with SharedSparkSession
  with DeltaColumnMappingTestUtils
  with SQLTestUtils
  with PhaseLockingTestMixin
  with TransactionExecutionTestMixin
  with DeltaSQLCommandTest {
  private def tryDeleteNonRecursive(fs: FileSystem, path: Path): Boolean = {
    try fs.delete(path, false) catch {
      case _: FileNotFoundException => true
    }
  }

  private def checkTableIsBroken(tablePath: String, failedSelectExpect: Boolean = true) {
    val selectCommand = s"SELECT SUM(HASH(*)) FROM delta.`$tablePath`;";
    var failedSelect = false
    try {
      spark.sql(selectCommand).show()
    } catch {
      case e: SparkException =>
        if (e.getMessage.contains("does not exist")) {
          failedSelect = true
        }
    }
    assert(failedSelect == failedSelectExpect)
  }

  test("FSCK simple test one file removed") {
    withTempDir { dir =>
      val directory = new File(dir, "test with space")
      val tablePath = directory.getCanonicalPath
      spark.range(30)
        .write
        .format("delta")
        .save(tablePath.toString)

      val inputFiles = spark.read.format("delta")
                                  .load(tablePath).inputFiles

      // Remove a file
      val indexToDelete = inputFiles.length / 2

      val fileToDelete = new Path(inputFiles(indexToDelete))
      val dataPath = new Path(tablePath)

      // scalastyle:off deltahadoopconfiguration
      val fs = dataPath.getFileSystem(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration

      // Get the files referenced within the Delta Log
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      var snapshot = deltaLog.snapshot
      var allFiles = snapshot.allFilesViaStateReconstruction.collect()

      // Make sure the file exists in the Delta Log
      val existingFilesCount = allFiles.count(file => fileToDelete.toString.contains(file.path))
      assert(existingFilesCount == 1)

      // Delete a parquet file
      fs.delete(fileToDelete, true)

      // Make sure the table is corrupted before fsck
      checkTableIsBroken(tablePath)

      // Fix the table
      spark.sql(s"FSCK REPAIR TABLE delta.`${tablePath}`;")
      checkTableIsBroken(tablePath, false)

      // Make sure the removed file is not referenced in the delta log anymore
      snapshot = deltaLog.update()
      allFiles = snapshot.allFilesViaStateReconstruction.collect()
      val fileExists = allFiles.exists(file => fileToDelete.toString.contains(file.path))
      assert(!fileExists)
    }
  }

  test("FSCK simple test multiple files removed") {
    withTempDir { dir =>
      val directory = new File(dir, "test")
      val tablePath = directory.getCanonicalPath

      // Create a table in multiple files
      spark.sql(s"CREATE TABLE delta.`$tablePath` (Id INT) USING DELTA;")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(2);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(3);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(4);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(5);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(6);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(7);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(8);")

      val inputFiles = spark.read.format("delta")
                                  .load(tablePath).inputFiles

      // Remove 3 files
      val indicesToDelete: Seq[Int] = Seq(0, inputFiles.length / 2, inputFiles.length - 1)
      val filesToDelete: Seq[Path] = indicesToDelete.map(index => new Path(inputFiles(index)))
      val dataPath = new Path(tablePath)

      // scalastyle:off deltahadoopconfiguration
      val fs = dataPath.getFileSystem(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration

      // Get the files referenced within the Delta Log
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      var snapshot = deltaLog.snapshot
      var allFiles = snapshot.allFilesViaStateReconstruction.collect()

      // Make sure the files exist in the Delta Log
      val existingFilesCount = allFiles.count(file => filesToDelete.exists(fileToDelete =>
        fileToDelete.toString.contains(file.path)))
      assert(existingFilesCount == 3)
      // Delete parquet files
      filesToDelete.foreach(fs.delete(_, true))

      // Make sure the table is corrupted before fsck
      checkTableIsBroken(tablePath)
      // Fix the table
      spark.sql(s"FSCK REPAIR TABLE delta.`${tablePath}`;")

      checkTableIsBroken(tablePath, false)

      // Make sure the removed files are not referenced in the delta log anymore
      snapshot = deltaLog.update()
      allFiles = snapshot.allFilesViaStateReconstruction.collect()
      assert(allFiles.forall(file => filesToDelete.forall(!_.toString.contains(file))))
    }
  }

  test("FSCK simple test table syntax") {
    val tbl = "tbl"
    withTable(tbl) {
      val table = spark.range(30)
        .write
        .format("delta")
        .saveAsTable(tbl)
      val inputFiles = spark.read.format("delta")
                                  .table(tbl).inputFiles

      // Remove the middle file
      val indexToDelete = inputFiles.length / 2

      val fileToDelete = new Path(inputFiles(indexToDelete))

      val deltaLog = DeltaLog.forTable(spark, new TableIdentifier(tbl))
      val dataPath = deltaLog.dataPath

      // scalastyle:off deltahadoopconfiguration
      val fs = dataPath.getFileSystem(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      // Delete a parquet file
      fs.delete(fileToDelete, true)

      // Make sure the table is corrupted before fsck
      checkTableIsBroken(dataPath.toString)

      // Fix the table
      spark.sql(s"FSCK REPAIR TABLE $tbl;")

      checkTableIsBroken(dataPath.toString, false)

      // Make sure the removed file is not referenced in the delta log anymore
      val snapshot = deltaLog.update()
      val referencedFiles = snapshot.allFilesViaStateReconstruction.collect()
      val fileExists = referencedFiles.exists(file => fileToDelete.toString.contains(file.path))
      assert(!fileExists)
      assert(referencedFiles.length == inputFiles.length - 1)
    }
  }

  test("FSCK with partitions") {
    withTempDir { dir =>
      val directory = new File(dir, "test")
      val tablePath = directory.getCanonicalPath
      spark.range(10, 20).withColumn("col1", lit(3))
        .withColumn("col2", col("id") - 6)
        .write
        .format("delta")
        .partitionBy("col1", "col2")
        .save(tablePath.toString)

      val inputFiles = spark.read.format("delta")
                                  .load(tablePath).inputFiles
      val indexToDelete: Int = inputFiles.length / 2
      val fileToDelete = new Path(inputFiles(indexToDelete))
      val dataPath = new Path(tablePath)

      // scalastyle:off deltahadoopconfiguration
      val fs = dataPath.getFileSystem(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration

      // Get the files referenced within the Delta Log
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      var snapshot = deltaLog.snapshot
      var allFiles = snapshot.allFilesViaStateReconstruction.collect()

      // Make sure the file exists in the Delta Log
      val existingFilesBefore = allFiles.map(file => fileToDelete.toString.contains(file.path))
      assert(existingFilesBefore.count(_ == true) == 1)

      // Delete a parquet file
      fs.delete(fileToDelete, true)

      // Make sure the table is corrupted before fsck
      checkTableIsBroken(tablePath)

      // Fix the table
      spark.sql(s"FSCK REPAIR TABLE delta.`${tablePath}`;")

      checkTableIsBroken(tablePath, false)

      // Make sure the removed file is not referenced in the delta log anymore
      snapshot = deltaLog.update()
      allFiles = snapshot.allFilesViaStateReconstruction.collect()
      val fileExists = allFiles.exists(file => fileToDelete.toString.contains(file.path))
      assert(!fileExists)
    }
  }

  test("FSCK clone test") {
    withTempDir { dir =>
      val directory = new File(dir, "test")
      val tablePath = directory.getCanonicalPath
      // Create the original table
      spark.sql(s"CREATE TABLE delta.`$tablePath` (Id INT, Col1 INT, Col2 INT, Col3 STRING) "
        + "USING DELTA PARTITIONED BY (Col1, Col2);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(7, 2, 1, 'ABCF');")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(0, 1, 2, 'AD');")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(2, 5, 0, 'ABC');")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1, 11, 1, 'CBC');")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(2, 2, 7, 'ACC');")

      // Create a new table by cloning the old one
      val newDirectory = new File(dir, "test CLONE")
      val newTablePath = newDirectory.getCanonicalPath
      spark.sql(s"CREATE TABLE delta.`$newTablePath` SHALLOW CLONE delta.`$tablePath`;")

      // Add more entries to the cloned table
      spark.sql(s"INSERT INTO TABLE delta.`$newTablePath` VALUES(0, 11, 1, 'BCBC');")
      spark.sql(s"INSERT INTO TABLE delta.`$newTablePath` VALUES(2, 4, 7, 'BACC');")
      spark.sql(s"INSERT INTO TABLE delta.`$newTablePath` VALUES(2, 5, 1, 'BBC');")
      spark.sql(s"INSERT INTO TABLE delta.`$newTablePath` VALUES(1, 9, 1, 'BBC');")

      // Remove 4 parquet files
      val inputFiles = spark.read.format("delta")
                                  .load(newTablePath).inputFiles
      val indicesToDelete: Seq[Int] = Seq(0, inputFiles.length / 3,
        inputFiles.length / 2,
        inputFiles.length - 1)

      val filesToDelete: Seq[Path] = indicesToDelete.map(index => new Path(inputFiles(index)))
      val clonedDataPath = new Path(newTablePath)

      // scalastyle:off deltahadoopconfiguration
      val fs = clonedDataPath.getFileSystem(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration

      // Get the files referenced within the Delta Log
      val deltaLog = DeltaLog.forTable(spark, newTablePath.toString)
      var snapshot = deltaLog.snapshot
      var allFilesEncoded = snapshot.allFilesViaStateReconstruction.collect()
      var allFiles = allFilesEncoded.map(file =>
        URLDecoder.decode(file.path, StandardCharsets.UTF_8.toString))

      // Make sure the files exist in the Delta Log
      val existingFilesBefore = allFiles.map(file => filesToDelete.map(fileToDelete =>
        fileToDelete.toString.contains(file)).count(_ == true))
      assert(existingFilesBefore.count(_ == 1) == 4)

      // Delete parquet files
      for (file <- filesToDelete) {
        fs.delete(file, true)
      }

      // Make sure the table is corrupted before fsck
      checkTableIsBroken(newTablePath)

      // Fix the table
      val output = spark.sql(s"FSCK REPAIR TABLE delta.`${newTablePath}`;")
      assert(output.count() == 4)

      checkTableIsBroken(newTablePath, false)
      // Make sure the removed files are not referenced in the delta log anymore
      snapshot = deltaLog.update()
      allFilesEncoded = snapshot.allFilesViaStateReconstruction.collect()
      allFiles = allFilesEncoded.map(file => URLDecoder.decode(file.path,
        StandardCharsets.UTF_8.toString))
      assert(allFiles.forall(file => filesToDelete.forall(!_.toString.contains(file))))
      // Make sure no files are missing
      val outputDryRun = spark.sql(s"FSCK REPAIR TABLE delta.`${newTablePath}` DRY RUN;")
      assert(outputDryRun.count() == 0)
    }
  }

  test("FSCK Vacuum and restore test") {
    withTempDir { dir =>
      val directory = new File(dir, "test")
      val tablePath = directory.getCanonicalPath
      val selectCommand = s"SELECT * FROM delta.`$tablePath`;";
      spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      // Create the original table
      spark.sql(s"CREATE TABLE delta.`$tablePath` (Id INT, Col1 INT, Col2 INT, Col3 STRING) "
        + "USING DELTA PARTITIONED BY (Col1, Col2);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(7, 2, 1, 'ABCF');")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(0, 1, 2, 'AD');")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(2, 5, 0, 'ABC');")
      // Delete and add back some files
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE Col1 = 1;")
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE Col1 = 2;")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1, 11, 1, 'CBC');")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(2, 2, 7, 'ACC');")
      // Vacuum the table
      spark.sql(s"VACUUM delta.`$tablePath` RETAIN 0 HOURS;")
      // Restore the version of the table when deleted files were present
      withSQLConf(("spark.sql.files.ignoreMissingFiles", "true")) {
        spark.sql(s"RESTORE TABLE delta.`$tablePath` TO VERSION AS OF 3;")
      }

      // Make sure the table is corrupted before fsck
      checkTableIsBroken(tablePath)

      spark.sql(s"FSCK REPAIR TABLE delta.`$tablePath`;")

      // Make sure select works now
      checkTableIsBroken(tablePath, false)
    }
  }

  test("FSCK Change DRY RUN limit") {
    withSQLConf(DeltaSQLConf.FSCK_MAX_NUM_ENTRIES_IN_RESULT.key -> "2") {
      withTempDir { dir =>
        val directory = new File(dir, "test")
        val tablePath = directory.getCanonicalPath

        // Create a table in multiple files
        spark.sql(s"CREATE TABLE delta.`$tablePath` (Id INT) USING DELTA;")
        spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1);")
        spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(2);")
        spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(3);")
        spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(4);")
        spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(5);")
        spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(6);")
        spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(7);")

        val inputFiles = spark.read.format("delta")
                                    .load(tablePath).inputFiles

        // Remove 3 files
        val indicesToDelete: Seq[Int] = Seq(0, inputFiles.length / 2, inputFiles.length - 1)
        val filesToDelete: Seq[Path] = indicesToDelete.map(index => new Path(inputFiles(index)))
        val dataPath = new Path(tablePath)

        // scalastyle:off deltahadoopconfiguration
        val fs = dataPath.getFileSystem(spark.sessionState.newHadoopConf())
        // scalastyle:on deltahadoopconfiguration

        // Get the files referenced within the Delta Log
        val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
        var snapshot = deltaLog.snapshot
        var allFiles = snapshot.allFilesViaStateReconstruction.collect()

        // Make sure the files exist in the Delta Log
        val existingFilesCount = allFiles.count(file => filesToDelete.exists(fileToDelete =>
          fileToDelete.toString.contains(file.path)))
        assert(existingFilesCount == 3)
        // Delete parquet files
        filesToDelete.foreach(fs.delete(_, true))

        // Make sure the table is corrupted before fsck
        checkTableIsBroken(tablePath)

        val events = com.databricks.spark.util.Log4jUsageLogger.track {
          // Fix the table
          assert(spark.sql(s"FSCK REPAIR TABLE delta.`${tablePath}` DRY RUN;").count() == 2)
        }
        val fsckStatsRecords = events
          .filter(event => {
            event.tags.get("opType") === Some("delta.fsck.stats")})
        assert(fsckStatsRecords.length === 1)
        JsonUtils.mapper.readValue[Option[Map[String, Any]]](fsckStatsRecords.head.blob) match {
          case Some(map: Map[String, Any]) =>
            assert(map.get("numMissingFiles").map(_.asInstanceOf[Int]) === Some(3))
        }
        // Make sure the table is corrupted after fsck dry run
        checkTableIsBroken(tablePath)
      }
    }
  }

  test("FSCK Make sure other files are not deleted") {
    withTempDir { dir =>
      val directory = new File(dir, "test")
      val tablePath = directory.getCanonicalPath

      // Create a table in multiple files
      spark.sql(s"CREATE TABLE delta.`$tablePath` (Id INT) USING DELTA;")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(2);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(3);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(4);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(5);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(6);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(7);")

      val inputFiles = spark.read.format("delta")
                                  .load(tablePath).inputFiles

      // Remove 3 files
      val indicesToDelete: Seq[Int] = Seq(0, inputFiles.length / 2, inputFiles.length - 1)
      val filesToDelete: Seq[Path] = indicesToDelete.map(index => new Path(inputFiles(index)))
      val dataPath = new Path(tablePath)

      // scalastyle:off deltahadoopconfiguration
      val fs = dataPath.getFileSystem(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration

      // Get the files referenced within the Delta Log
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      var snapshot = deltaLog.snapshot
      var allFiles = snapshot.allFilesViaStateReconstruction.collect()

      // Make sure the files exist in the Delta Log
      val existingFilesCount = allFiles.count(file => filesToDelete.exists(fileToDelete =>
        fileToDelete.toString.contains(file.path)))
      assert(existingFilesCount == 3)

      val allFilesStrings: Seq[Path] = allFiles.map(path => new Path(path.path))
      val notDeletedFiles = allFiles.map(_.path).filter(f =>
        filesToDelete.forall(!_.toString.contains(f)))
      filesToDelete.foreach(fs.delete(_, true))

      // Make sure the table is corrupted before fsck
      checkTableIsBroken(tablePath)
      // Fix the table
      spark.sql(s"FSCK REPAIR TABLE delta.`${tablePath}`;")

      checkTableIsBroken(tablePath, false)

      // Make sure the removed files are not referenced in the delta log anymore
      snapshot = deltaLog.update()
      allFiles = snapshot.allFilesViaStateReconstruction.collect()
      val currentReferencedFiles = allFiles.map(_.path).sorted
      assert(notDeletedFiles.sorted === currentReferencedFiles)
    }
  }

  test("FSCK DRY RUN test") {
    withTempDir { dir =>
      val directory = new File(dir, "test")
      val tablePath = directory.getCanonicalPath
      spark.range(20).withColumn("col", lit(2))
        .write
        .format("delta")
        .save(tablePath.toString)

      val inputFiles = spark.read.format("delta")
        .load(tablePath).inputFiles
      val indexToDelete: Int = inputFiles.length / 2
      val fileToDelete = new Path(inputFiles(indexToDelete))
      val dataPath = new Path(tablePath)

      // scalastyle:off deltahadoopconfiguration
      val fs = dataPath.getFileSystem(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration

      // Count the number of Delta Logs before FSCK
      val logPath = DeltaLog.forTable(spark, tablePath.toString).logPath;
      val fileIterator = fs.listFiles(logPath, false)
      val filesAndDirs: Array[FileStatus] = Iterator.continually(fileIterator)
        .takeWhile(_.hasNext)
        .map(_.next)
        .toArray

      val lastDeltaLogPath = filesAndDirs.filter(_.getPath.getName.endsWith(".json"))
      val numLogsBefore = lastDeltaLogPath.length

      // Get the files referenced within the Delta Log
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      var snapshot = deltaLog.snapshot
      val allFilesBefore = snapshot.allFilesViaStateReconstruction.collect()

      // Make sure the file exists in the Delta Log
      val filesToDeleteBEfore = allFilesBefore.map(file =>
        fileToDelete.toString.contains(file.path))
      assert(filesToDeleteBEfore.count(_ == true) == 1)

      // Delete a parquet file
      fs.delete(fileToDelete, true)
      // Make sure the table is corrupted before fsck
      checkTableIsBroken(tablePath)

      // Fix the table
      spark.sql(s"FSCK REPAIR TABLE delta.`${tablePath}` DRY RUN;")

      // Make sure the removed file is not referenced in the delta log anymore
      snapshot = deltaLog.update()
      val allFilesAfter = snapshot.allFilesViaStateReconstruction.collect()
      val existingFilesAfter = allFilesAfter.map(file =>
        fileToDelete.toString.contains(file.path))

      // The file is still present in the delta log
      assert(existingFilesAfter.count(_ == true) == 1)

      // The file still doesn't exist on disk
      assert(!fs.exists(fileToDelete))

      val fileIteratorAfter = fs.listFiles(logPath, false)
      val filesAndDirsAfter: Array[FileStatus] = Iterator.continually(fileIteratorAfter)
        .takeWhile(_.hasNext)
        .map(_.next)
        .toArray
      val lastDeltaLogPathAfter = filesAndDirsAfter.filter(_.getPath.getName.endsWith(".json"))
      val numLogsAfter = lastDeltaLogPathAfter.length

      // Make sure the number of delta logs is still the same and that select still fails
      assert(numLogsAfter == numLogsBefore)
      checkTableIsBroken(tablePath)
    }
  }

  test("FSCK Call FSCK on partitions") {
    withTempDir { dir =>
      val directory = new File(dir, "test")
      val tablePath = directory.getCanonicalPath

      // Create a table in multiple files
      spark.sql(s"CREATE TABLE delta.`$tablePath` (Id INT, Col1 INT, Col2 INT) "
        + "USING DELTA PARTITIONED BY (Col1, Col2);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1, 1, 2);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1, 2, 3);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1, 3, 1);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1, 1, 4);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1, 5, 6);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1, 9, 6);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1, 1, 7);")

      val inputFiles = spark.read.format("delta")
                                  .load(tablePath).inputFiles

      // Remove 3 files
      val indicesToDelete: Seq[Int] = Seq(0, inputFiles.length / 2, inputFiles.length - 1)
      val filesToDelete: Seq[Path] = indicesToDelete.map(index => new Path(inputFiles(index)))
      val dataPath = new Path(tablePath)

      // scalastyle:off deltahadoopconfiguration
      val fs = dataPath.getFileSystem(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration

      // Get the files referenced within the Delta Log
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      val snapshot = deltaLog.snapshot
      val allFiles = snapshot.allFilesViaStateReconstruction.collect()
      // Make sure the files exist in the Delta Log
      val existingFilesCount = allFiles.count(file => filesToDelete.exists(fileToDelete =>
        fileToDelete.toString.contains(file.path)))
      assert(existingFilesCount == 3)
      // Delete parquet files
      filesToDelete.foreach(fs.delete(_, true))
      // Make sure the table is corrupted before fsck
      checkTableIsBroken(tablePath)

      var failedWithPartitions = false
      val e = intercept[Exception] {
        spark.sql(s"FSCK REPAIR TABLE delta.`$tablePath/Col1=3/Col2=1`;")
      }
      assert(e.getMessage.contains("FSCK REPAIR TABLE is only supported for Delta tables"))
      checkTableIsBroken(tablePath)
    }
  }

  test("FSCK verify output") {
    withTempDir { dir =>
      val directory = new File(dir, "test")
      val tablePath = directory.getCanonicalPath
      // Create a table in multiple files
      spark.sql(s"CREATE TABLE delta.`$tablePath` (Id INT) USING DELTA;")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(2);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(3);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(4);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(5);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(6);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(7);")
      val inputFiles = spark.read.format("delta")
                                  .load(tablePath).inputFiles
      // Remove 3 files
      val indicesToDelete: Seq[Int] = Seq(0, inputFiles.length / 2, inputFiles.length - 1)
      val filesToDelete: Seq[Path] = indicesToDelete.map(index => new Path(inputFiles(index)))
      var dataPath = new Path(tablePath)
      // scalastyle:off deltahadoopconfiguration
      val fs = dataPath.getFileSystem(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      // Get the files referenced within the Delta Log
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      val snapshot = deltaLog.snapshot
      val allFiles = snapshot.allFilesViaStateReconstruction.collect()
      // Make sure the files exist in the Delta Log
      val existingFilesCount = allFiles.count(file =>
        filesToDelete.exists(fileToDelete => fileToDelete.toString.contains(file.path)))
      assert(existingFilesCount == 3)
      // Delete parquet files
      filesToDelete.foreach(fs.delete(_, true))
      // Make sure the table is corrupted before fsck
      checkTableIsBroken(tablePath)
      // Fix the table
      val output = spark.sql(s"FSCK REPAIR TABLE delta.`${tablePath}`;")
      checkTableIsBroken(tablePath, false)
      // Make sure the output rows are correct
      checkAnswer(output, filesToDelete.map(file => Row(file.toString.split("/").last, true)))
    }
  }

  test("FSCK Remove partition directory") {
    withTempDir { dir =>
      val directory = new File(dir, "test")
      val tablePath = directory.getCanonicalPath

      // Create a table in multiple files
      spark.sql(s"CREATE TABLE delta.`$tablePath` "
        + "(Id INT, Col1 INT, Col2 INT) USING DELTA PARTITIONED BY (Col1, Col2);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1, 1, 2);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(2, 2, 3);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(0, 3, 1);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(2, 1, 4);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1, 5, 6);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(5, 3, 1);")
      spark.sql(s"INSERT INTO TABLE delta.`$tablePath` VALUES(1, 1, 7);")

      val inputFiles = spark.read.format("delta")
                                  .load(tablePath).inputFiles
      val dataPath = new Path(tablePath)

      // scalastyle:off deltahadoopconfiguration
      val fs = dataPath.getFileSystem(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration

      // Delete files in the partition path
      val fileToDelete = new Path(s"$tablePath/Col1=3/Col2=1")
      // Delete parquet files
      fs.delete(fileToDelete, true)
      // Make sure the table is corrupted before fsck
      checkTableIsBroken(tablePath)
      // Make sure two of the files were deleted
      val fsckOutput = spark.sql(s"FSCK REPAIR TABLE delta.`$tablePath`;")
      assert(fsckOutput.count() == 2)
      checkTableIsBroken(tablePath, false)
      // Make sure that both of the deleted files contain the partition path
      for (row <- fsckOutput.collect) {
        assert(row.getString(0).contains("Col1=3/Col2=1"))
      }
    }
  }

  test("FSCK metrics in Delta table history") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        val directory = new File(tempDir, "test")
        val tablePath = directory.getCanonicalPath
        spark.range(30)
          .write
          .format("delta")
          .save(tablePath.toString)
        val inputFiles = spark.read.format("delta")
                                    .load(tablePath).inputFiles
        // Remove a file
        val indexToDelete = inputFiles.length / 2
        val fileToDelete = new Path(inputFiles(indexToDelete))
        val dataPath = new Path(tablePath)
        // scalastyle:off deltahadoopconfiguration
        val fs = dataPath.getFileSystem(spark.sessionState.newHadoopConf())
        // scalastyle:on deltahadoopconfiguration
        // Delete a parquet file
        fs.delete(fileToDelete, true)
        checkTableIsBroken(tablePath)
        // Fix the table
        spark.sql(s"FSCK REPAIR TABLE delta.`${tablePath}`;")
        import io.delta.tables.DeltaTable
        val actualOperationMetricsAndName = DeltaTable.forPath(spark, tablePath)
          .history(1)
          .select("operationMetrics", "operation")
          .head
        val actualOperationMetrics = actualOperationMetricsAndName
          .getMap(0)
          .asInstanceOf[Map[String, String]]
        val expectedMetrics = Map("numMissingFiles" -> Some("1"),
                                     "numFilesScanned" -> Some(s"${inputFiles.length}"))
        // Test that the metric exists.
        Seq(
          "numFilesScanned",
          "numMissingFiles",
          "executionTimeMs"
        ).foreach(metric => {
          if (expectedMetrics.contains(metric)) {
              assert(expectedMetrics(metric) == actualOperationMetrics.get(metric))
            }})
        val operationName = actualOperationMetricsAndName(1).asInstanceOf[String]
        assert(operationName === DeltaOperations.FSCK_OPERATION_NAME)
        checkTableIsBroken(tablePath, false)
    }
  }
}

test("FSCK special characters test") {
  withTempDir { dir =>
    var tblName = s"test"
    // Create the original table
    val tblPath = dir + tblName
    spark.sql(s"CREATE TABLE `$tblName` (Id INT, Col1 INT, Col2 INT, Col3 STRING) "
      + s"USING DELTA PARTITIONED BY (Col1, Col2) "
      + s"LOCATION '$tblPath';")
    spark.sql(s"INSERT INTO TABLE `$tblName` VALUES(7, 2, 1, 'ABCF');")
    spark.sql(s"INSERT INTO TABLE `$tblName` VALUES(0, 1, 2, 'AD');")
    spark.sql(s"INSERT INTO TABLE `$tblName` VALUES(2, 5, 0, 'ABC');")
    spark.sql(s"INSERT INTO TABLE `$tblName` VALUES(1, 11, 1, 'CBC');")
    spark.sql(s"INSERT INTO TABLE `$tblName` VALUES(2, 2, 7, 'ACC');")

    // Create a new table by cloning the old one
    val newTablePath = dir + "test CLONE"
    spark.sql(s"CREATE TABLE delta.`$newTablePath` SHALLOW CLONE `$tblName`;")

    // Add more entries to the cloned table
    spark.sql(s"INSERT INTO TABLE delta.`$newTablePath` VALUES(0, 11, 1, 'BCBC');")
    spark.sql(s"INSERT INTO TABLE delta.`$newTablePath` VALUES(2, 4, 7, 'BACC');")
    spark.sql(s"INSERT INTO TABLE delta.`$newTablePath` VALUES(2, 5, 1, 'BBC');")
    spark.sql(s"INSERT INTO TABLE delta.`$newTablePath` VALUES(1, 9, 1, 'BBC');")
    // Remove 4 parquet files
    val inputFiles = spark.read.format("delta")
                                .load(newTablePath).inputFiles
    val indicesToDelete: Seq[Int] = Seq(0, inputFiles.length / 3,
    inputFiles.length / 2,
    inputFiles.length - 1)
    val filesToDelete: Seq[Path] = indicesToDelete.map(index => new Path(inputFiles(index)))
    val clonedDataPath = new Path(newTablePath)

    // scalastyle:off deltahadoopconfiguration
    val fs = clonedDataPath.getFileSystem(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration

    // Get the files referenced within the Delta Log
    val deltaLog = DeltaLog.forTable(spark, newTablePath.toString)
    var snapshot = deltaLog.snapshot
    var allFilesEncoded = snapshot.allFilesViaStateReconstruction.collect()
    var allFiles = allFilesEncoded.map(file => URLDecoder.decode(file.path,
      StandardCharsets.UTF_8.toString))
    // Make sure the files exist in the Delta Log
    val existingFilesBefore = allFiles.map(file => filesToDelete.map(fileToDelete =>
      fileToDelete.toString.contains(file)).count(_ == true))
    assert(existingFilesBefore.count(_ == 1) == 4)
    // Delete parquet files
    for (file <- filesToDelete) {
      fs.delete(file, true)
    }
    // Make sure the table is corrupted before fsck
    checkTableIsBroken(newTablePath)
    // Fix the table
    val output = spark.sql(s"FSCK REPAIR TABLE delta.`${newTablePath}`;")
    assert(output.count() == 4)
    checkTableIsBroken(newTablePath, false)
    // Make sure the removed files are not referenced in the delta log anymore
    snapshot = deltaLog.update()
    allFilesEncoded = snapshot.allFilesViaStateReconstruction.collect()
    allFiles = allFilesEncoded.map(file => URLDecoder.decode(file.path,
      StandardCharsets.UTF_8.toString))
    assert(allFiles.forall(file => filesToDelete.forall(!_.toString.contains(file))))
  }
}

test("FSCK ignore non-404 errors in DRY RUN mode") {
    import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
    import org.apache.spark.sql.delta.test.DeltaTestImplicits._
    withTempDir { dir =>
      val directory = new File(dir, "test")
      val tablePath = directory.getCanonicalPath
      spark.range(1)
        .write
        .format("delta")
        .save(tablePath.toString)
      val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
      val action = createTestAddFile(encodedPath = "invalidprotocol://myfile.parquet")
      deltaLog.startTransaction().commitManually(action)
      spark.sql(s"FSCK REPAIR TABLE delta.`${tablePath}` DRY RUN;")
      val e = intercept[SparkException] {
        spark.sql(s"FSCK REPAIR TABLE delta.`${tablePath}`;")
      }.getMessage
      assert(e.contains("[FAILED_EXECUTE_UDF]"))
    }
  }

  test("FSCK no double commit with concurrent calls") {
    withTempDir { tempDir =>
      val directory = new File(tempDir, "test")
      val tablePath = directory.getCanonicalPath
      spark.range(10, 20).withColumn("col1", lit(3))
        .withColumn("col2", col("id") - 6)
        .write
        .format("delta")
        .partitionBy("col1", "col2")
        .save(tablePath.toString)
      val inputFiles = spark.read.format("delta")
                                  .load(tablePath).inputFiles
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      // Remove 3 files
      val indicesToDelete: Seq[Int] = Seq(0, inputFiles.length / 2, inputFiles.length - 1)
      val filesToDelete: Seq[Path] = indicesToDelete.map(index => new Path(inputFiles(index)))

      // scalastyle:off deltahadoopconfiguration
      val dataPath = new Path(tablePath)
      val fs = dataPath.getFileSystem(spark.sessionState.newHadoopConf())
      val logPath = deltaLog.logPath
      // scalastyle:on deltahadoopconfiguration

      val versionNumBefore = deltaLog.update().version
      // Delete parquet files
      filesToDelete.foreach(fs.delete(_, true))
      // Make sure the table is corrupted before fsck
      checkTableIsBroken(tablePath)
      def txnA(): Array[Row] = {
        spark.sql(s"FSCK REPAIR TABLE delta.`${tablePath}`;").collect()
      }
      def txnB(): Array[Row] = {
        spark.sql(s"FSCK REPAIR TABLE delta.`${tablePath}`;").collect()
      }
      val (futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      val e = intercept[SparkException] {
        ThreadUtils.awaitResult(futureA, Duration.Inf)
      }
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      assert(e.getCause.getMessage.contains("DELTA_CONCURRENT_DELETE_DELETE"))
      checkTableIsBroken(tablePath, false)
      val snapshot = deltaLog.update()
      val allFilesEncoded = snapshot.allFilesViaStateReconstruction.collect()
      val allFiles = allFilesEncoded.map(file => URLDecoder.decode(file.path,
        StandardCharsets.UTF_8.toString))
      assert(allFiles.forall(file => filesToDelete.forall(!_.toString.contains(file))))
      val versionNumAfter = snapshot.version
      // Make sure the number of delta logs is updated by 1
      // Note that I'm subtracting 1 because FSCK command will increment the version by 1
      assert(versionNumAfter - 1 == versionNumBefore)
      // Make sure only three files were deleted
      assert(allFilesEncoded.length == inputFiles.length - 3)
    }
  }
}
