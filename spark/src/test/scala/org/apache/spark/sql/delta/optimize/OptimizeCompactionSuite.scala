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

package org.apache.spark.sql.delta.optimize

import java.io.File

import scala.collection.JavaConverters._

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaColumnMappingSelectedTestMixin, DeltaSQLCommandTest}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import io.delta.tables.DeltaTable

import org.scalatest.concurrent.TimeLimits.failAfter
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType

/**
 * Base class containing tests for Delta table Optimize (file compaction)
 */
trait OptimizeCompactionSuiteBase extends QueryTest
  with SharedSparkSession
  with DeletionVectorsTestUtils
  with DeltaColumnMappingTestUtils {

  import testImplicits._

  def executeOptimizeTable(table: String, condition: Option[String] = None)
  def executeOptimizePath(path: String, condition: Option[String] = None)

  test("optimize command: with database and table name") {
    withTempDir { tempDir =>
      val dbName = "delta_db"
      val tableName = s"$dbName.delta_optimize"
      withDatabase(dbName) {
        spark.sql(s"create database $dbName")
        withTable(tableName) {
          appendToDeltaTable(Seq(1, 2, 3).toDF(), tempDir.toString, partitionColumns = None)
          appendToDeltaTable(Seq(4, 5, 6).toDF(), tempDir.toString, partitionColumns = None)

          spark.sql(s"create table $tableName using delta location '$tempDir'")

          val deltaLog = DeltaLog.forTable(spark, tempDir)
          val versionBeforeOptimize = deltaLog.snapshot.version
          executeOptimizeTable(tableName)

          deltaLog.update()
          assert(deltaLog.snapshot.version === versionBeforeOptimize + 1)
          checkDatasetUnorderly(spark.table(tableName).as[Int], 1, 2, 3, 4, 5, 6)
        }
      }
    }
  }

  test("optimize command") {
    withTempDir { tempDir =>
      appendToDeltaTable(Seq(1, 2, 3).toDF(), tempDir.toString, partitionColumns = None)
      appendToDeltaTable(Seq(4, 5, 6).toDF(), tempDir.toString, partitionColumns = None)

      def data: DataFrame = spark.read.format("delta").load(tempDir.toString)

      val deltaLog = DeltaLog.forTable(spark, tempDir)
      val versionBeforeOptimize = deltaLog.snapshot.version
      executeOptimizePath(tempDir.getCanonicalPath)
      deltaLog.update()
      assert(deltaLog.snapshot.version === versionBeforeOptimize + 1)
      checkDatasetUnorderly(data.toDF().as[Int], 1, 2, 3, 4, 5, 6)

      // Make sure thread pool is shut down
      assert(Thread.getAllStackTraces.keySet.asScala
        .filter(_.getName.startsWith("OptimizeJob")).isEmpty)
    }
  }

  test("optimize command: predicate on non-partition column") {
    withTempDir { tempDir =>
      val path = new File(tempDir, "testTable").getCanonicalPath
      val partitionColumns = Some(Seq("id"))
      appendToDeltaTable(
        Seq(1, 2, 3).toDF("value").withColumn("id", 'value % 2),
        path,
        partitionColumns)

      val e = intercept[AnalysisException] {
        // Should fail when predicate is on a non-partition column
        executeOptimizePath(path, Some("value < 4"))
      }
      assert(e.getMessage.contains("Predicate references non-partition column 'value'. " +
                                       "Only the partition columns may be referenced: [id]"))
    }
  }

  test("optimize command: on partitioned table - all partitions") {
    withTempDir { tempDir =>
      val path = new File(tempDir, "testTable").getCanonicalPath
      val partitionColumns = Some(Seq("id"))
      appendToDeltaTable(
        Seq(1, 2, 3).toDF("value").withColumn("id", 'value % 2),
        path,
        partitionColumns)

      appendToDeltaTable(
        Seq(4, 5, 6).toDF("value").withColumn("id", 'value % 2),
        path,
        partitionColumns)

      val deltaLogBefore = DeltaLog.forTable(spark, path)
      val txnBefore = deltaLogBefore.startTransaction();
      val fileListBefore = txnBefore.filterFiles();
      val versionBefore = deltaLogBefore.snapshot.version

      val id = "id".phy(deltaLogBefore)

      // Expect each partition have more than one file
      (0 to 1).foreach(partId =>
        assert(fileListBefore.count(_.partitionValues === Map(id -> partId.toString)) > 1))

      executeOptimizePath(path)

      val deltaLogAfter = DeltaLog.forTable(spark, path)
      val txnAfter = deltaLogAfter.startTransaction();
      val fileListAfter = txnAfter.filterFiles();

      (0 to 1).foreach(partId =>
        assert(fileListAfter.count(_.partitionValues === Map(id -> partId.toString)) === 1))

      // version is incremented
      assert(deltaLogAfter.snapshot.version === versionBefore + 1)

      // data should remain the same after the OPTIMIZE
      checkDatasetUnorderly(
        spark.read.format("delta").load(path).select("value").as[Long],
        (1L to 6L): _*)
    }
  }

  test(
    s"optimize command with DVs") {
    withTempDir { tempDir =>
      val path = tempDir.getAbsolutePath
      withSQLConf(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true") {
        // Create 10 files each with 1000 records
        spark.range(start = 0, end = 10000, step = 1, numPartitions = 10)
          .toDF("id")
          .withColumn(colName = "extra", lit("just a random text to fill up the space....."))
          .write.format("delta").mode("append").save(path) // v0

        val deltaLog = DeltaLog.forTable(spark, path)
        val filesV0 = deltaLog.unsafeVolatileSnapshot.allFiles.collect()
        assert(filesV0.size == 10)

        // Default `optimize.maxDeletedRowsRatio` is 0.05.
        // Delete slightly more than threshold ration in two files, less in one of the file
        val file0 = filesV0(1)
        val file1 = filesV0(4)
        val file2 = filesV0(8)
        deleteRows(deltaLog, file0, approxPhyRows = 1000, ratioOfRowsToDelete = 0.06d) // v1
        deleteRows(deltaLog, file1, approxPhyRows = 1000, ratioOfRowsToDelete = 0.06d) // v2
        deleteRows(deltaLog, file2, approxPhyRows = 1000, ratioOfRowsToDelete = 0.01d) // v3

        // Add a one small file, so that the file selection is based on both the file size and
        // deleted rows ratio
        spark.range(start = 1, end = 2, step = 1, numPartitions = 1)
            .toDF("id").withColumn(colName = "extra", lit(""))
          .write.format("delta").mode("append").save(path) // v4
        val smallFiles = addedFiles(deltaLog.getChanges(startVersion = 4).next()._2)
        assert(smallFiles.size == 1)

        // Save the data before optimize for comparing it later with optimize
        val data = spark.read.format("delta").load(path)

        // Set a low value for minFileSize so that the criteria for file selection is based on DVs
        // and not based on the file size.
        val targetSmallSize = smallFiles(0).size +  10 // A number just higher than the `smallFile`
        withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_MIN_FILE_SIZE.key -> targetSmallSize.toString) {
          executeOptimizePath(path) // v5
        }
        val changes = deltaLog.getChanges(startVersion = 5).next()._2

        // We expect the two files containing more than the threshold rows to be compacted.
        var expectedRemoveFiles = Set(file0.path, file1.path)
        // Expect the small file also to be compacted always
        expectedRemoveFiles += smallFiles(0).path

        assert(removedFiles(changes).map(_.path).toSet === expectedRemoveFiles)

        assert(addedFiles(changes).size == 1) // Expect one new file added

        // Verify the final data after optimization hasn't changed.
        checkAnswer(spark.read.format("delta").load(path), data)
      }
    }
  }

  private def removedFiles(actions: Seq[Action]): Seq[RemoveFile] = {
    actions.filter(_.isInstanceOf[RemoveFile]).map(_.asInstanceOf[RemoveFile])
  }

  private def addedFiles(actions: Seq[Action]): Seq[AddFile] = {
    actions.filter(_.isInstanceOf[AddFile]).map(_.asInstanceOf[AddFile])
  }

  def appendRowsToDeltaTable(
      path: String,
      numFiles: Int,
      numRowsPerFiles: Int,
      partitionColumns: Option[Seq[String]],
      partitionValues: Seq[Int]): Unit = {
    partitionValues.foreach { partition =>
      (0 until numFiles).foreach { _ =>
        appendToDeltaTable(
          (0 until numRowsPerFiles).toDF("value").withColumn("id", lit(partition)),
          path,
          partitionColumns)
      }
    }
  }

  def testOptimizeCompactWithLargeFile(
      name: String, unCompactablePartitions: Seq[Int], compactablePartitions: Seq[Int]) {
    test(name) {
      withTempDir { tempDir =>
          val path = new File(tempDir, "testTable").getCanonicalPath
          val partitionColumns = Some(Seq("id"))
          // Create un-compactable partitions.
          appendRowsToDeltaTable(
            path, numFiles = 1, numRowsPerFiles = 200, partitionColumns, unCompactablePartitions)
          // Create compactable partitions with 5 files
          appendRowsToDeltaTable(
            path, numFiles = 5, numRowsPerFiles = 10, partitionColumns, compactablePartitions)

          val deltaLogBefore = DeltaLog.forTable(spark, path)
          val txnBefore = deltaLogBefore.startTransaction()
          val fileListBefore = txnBefore.filterFiles()
          val versionBefore = deltaLogBefore.snapshot.version

          val id = "id".phy(deltaLogBefore)
          unCompactablePartitions.foreach(partId =>
            assert(fileListBefore.count(_.partitionValues === Map(id -> partId.toString)) == 1))
          compactablePartitions.foreach(partId =>
            assert(fileListBefore.count(_.partitionValues === Map(id -> partId.toString)) == 5))
          // Optimize compact all partitions
          spark.sql(s"OPTIMIZE '$path'")

          val deltaLogAfter = DeltaLog.forTable(spark, path)
          val txnAfter = deltaLogAfter.startTransaction();
          val fileListAfter = txnAfter.filterFiles();
          // All partitions should only contains single file.
          (unCompactablePartitions ++ compactablePartitions).foreach(partId =>
            assert(fileListAfter.count(_.partitionValues === Map(id -> partId.toString)) === 1))
          // version is incremented
          assert(deltaLogAfter.snapshot.version === versionBefore + 1)
      }
    }
  }
  testOptimizeCompactWithLargeFile(
    "optimize command: interleaves compactable/un-compactable partitions",
    unCompactablePartitions = Seq(1, 3, 5),
    compactablePartitions = Seq(2, 4, 6))

  testOptimizeCompactWithLargeFile(
    "optimize command: first two and last two partitions are un-compactable",
    unCompactablePartitions = Seq(1, 2, 5, 6),
    compactablePartitions = Seq(3, 4))

  testOptimizeCompactWithLargeFile(
    "optimize command: only first and last partition are compactable",
    unCompactablePartitions = Seq(2, 3, 4, 5),
    compactablePartitions = Seq(1, 6))

  testOptimizeCompactWithLargeFile(
    "optimize command: only first partition is un-compactable",
    unCompactablePartitions = Seq(1),
    compactablePartitions = Seq(2, 3, 4, 5, 6))

  testOptimizeCompactWithLargeFile(
    "optimize command: only first partition is compactable",
    unCompactablePartitions = Seq(2, 3, 4, 5, 6),
    compactablePartitions = Seq(1))

  test("optimize command: on partitioned table - selected partitions") {
    withTempDir { tempDir =>
      val path = new File(tempDir, "testTable").getCanonicalPath
      val partitionColumns = Some(Seq("id"))
      appendToDeltaTable(
        Seq(1, 2, 3).toDF("value").withColumn("id", 'value % 2),
        path,
        partitionColumns)

      appendToDeltaTable(
        Seq(4, 5, 6).toDF("value").withColumn("id", 'value % 2),
        path,
        partitionColumns)

      val deltaLogBefore = DeltaLog.forTable(spark, path)
      val txnBefore = deltaLogBefore.startTransaction();
      val fileListBefore = txnBefore.filterFiles()

      val id = "id".phy(deltaLogBefore)

      assert(fileListBefore.length >= 3)
      assert(fileListBefore.count(_.partitionValues === Map(id -> "0")) > 1)

      val versionBefore = deltaLogBefore.snapshot.version
      executeOptimizePath(path, Some("id = 0"))

      val deltaLogAfter = DeltaLog.forTable(spark, path)
      val txnAfter = deltaLogBefore.startTransaction();
      val fileListAfter = txnAfter.filterFiles()

      assert(fileListBefore.length > fileListAfter.length)
      // Optimized partition should contain only one file
      assert(fileListAfter.count(_.partitionValues === Map(id -> "0")) === 1)

      // File counts in partitions that are not part of the OPTIMIZE should remain the same
      assert(fileListAfter.count(_.partitionValues === Map(id -> "1")) ===
                 fileListAfter.count(_.partitionValues === Map(id -> "1")))

      // version is incremented
      assert(deltaLogAfter.snapshot.version === versionBefore + 1)

      // data should remain the same after the OPTIMIZE
      checkDatasetUnorderly(
        spark.read.format("delta").load(path).select("value").as[Long],
        (1L to 6L): _*)
    }
  }

  test("optimize command: on null partition columns") {
    withTempDir { tempDir =>
      val path = new File(tempDir, "testTable").getCanonicalPath
      val partitionColumn = "part"

      (1 to 5).foreach { _ =>
        appendToDeltaTable(
          Seq(("a", 1), ("b", 2), (null.asInstanceOf[String], 3), ("", 4))
              .toDF(partitionColumn, "value"),
          path,
          Some(Seq(partitionColumn)))
      }

      val deltaLogBefore = DeltaLog.forTable(spark, path)
      val txnBefore = deltaLogBefore.startTransaction();
      val fileListBefore = txnBefore.filterFiles()
      val versionBefore = deltaLogBefore.snapshot.version

      val partitionColumnPhysicalName = partitionColumn.phy(deltaLogBefore)

      // we have only 1 partition here
      val filesInEachPartitionBefore = groupInputFilesByPartition(
        fileListBefore.map(_.path).toArray, deltaLogBefore)

      // There exist at least one file in each partition
      assert(filesInEachPartitionBefore.forall(_._2.length > 1))

      // And there is a partition for null values
      assert(filesInEachPartitionBefore.keys.exists(
        _ === (partitionColumnPhysicalName, nullPartitionValue)))

      executeOptimizePath(path)

      val deltaLogAfter = DeltaLog.forTable(spark, path)
      val txnAfter = deltaLogBefore.startTransaction();
      val fileListAfter = txnAfter.filterFiles()

      // Number of files is less than before optimize
      assert(fileListBefore.length > fileListAfter.length)

      // Optimized partition should contain only one file in null partition
      assert(fileListAfter.count(
        _.partitionValues === Map[String, String](partitionColumnPhysicalName -> null)) === 1)

      // version is incremented
      assert(deltaLogAfter.snapshot.version === versionBefore + 1)

      // data should remain the same after the OPTIMIZE
      checkAnswer(
        spark.read.format("delta").load(path).groupBy(partitionColumn).count(),
        Seq(Row("a", 5), Row("b", 5), Row(null, 10)))
    }
  }

  test("optimize command: on table with multiple partition columns") {
    withTempDir { tempDir =>
      val path = new File(tempDir, "testTable").getCanonicalPath
      val partitionColumns = Seq("date", "part")

      Seq(10, 100).foreach { count =>
        appendToDeltaTable(
          spark.range(count)
              .select('id, lit("2017-10-10").cast("date") as "date", 'id % 5 as "part"),
          path,
          Some(partitionColumns))
      }

      val deltaLogBefore = DeltaLog.forTable(spark, path)
      val txnBefore = deltaLogBefore.startTransaction();
      val fileListBefore = txnBefore.filterFiles()
      val versionBefore = deltaLogBefore.snapshot.version

      val date = "date".phy(deltaLogBefore)
      val part = "part".phy(deltaLogBefore)

      val fileCountInTestPartitionBefore = fileListBefore
          .count(_.partitionValues === Map[String, String](date -> "2017-10-10", part -> "3"))

      executeOptimizePath(path, Some("date = '2017-10-10' and part = 3"))

      val deltaLogAfter = DeltaLog.forTable(spark, path)
      val txnAfter = deltaLogBefore.startTransaction();
      val fileListAfter = txnAfter.filterFiles()

      // Number of files is less than before optimize
      assert(fileListBefore.length > fileListAfter.length)

      // Optimized partition should contain only one file in null partition and less number
      // of files than before optimize
      val fileCountInTestPartitionAfter = fileListAfter
          .count(_.partitionValues === Map[String, String](date -> "2017-10-10", part -> "3"))
      assert(fileCountInTestPartitionAfter === 1L)
      assert(fileCountInTestPartitionBefore > fileCountInTestPartitionAfter,
             "Expected the partition to count less number of files after optimzie.")

      // version is incremented
      assert(deltaLogAfter.snapshot.version === versionBefore + 1)
    }
  }

  test("optimize - multiple jobs start executing at once ") {
    // The idea here is to make sure multiple optimize jobs execute concurrently. We can
    // block the writes of one batch with a countdown latch that will unblock only
    // after the second batch also tries to write.

    val numPartitions = 2
    withTempDir { tempDir =>
      spark.range(100)
          .withColumn("pCol", 'id % numPartitions)
          .repartition(10)
          .write
          .format("delta")
          .partitionBy("pCol")
          .save(tempDir.getAbsolutePath)

      // We have two partitions so we would have two tasks. We can make sure we have two batches
      withSQLConf(
        ("fs.AbstractFileSystem.block.impl",
          classOf[BlockWritesAbstractFileSystem].getCanonicalName),
        ("fs.block.impl", classOf[BlockWritesLocalFileSystem].getCanonicalName)) {

        val path = s"block://${tempDir.getAbsolutePath}"
        val deltaLog = DeltaLog.forTable(spark, path)
        require(deltaLog.snapshot.numOfFiles === 20) // 10 files in each partition
        // block the first write until the second batch can attempt to write.
        BlockWritesLocalFileSystem.blockUntilConcurrentWrites(numPartitions)
        failAfter(60.seconds) {
          executeOptimizePath(path)
        }
        assert(deltaLog.snapshot.numOfFiles === numPartitions) // 1 file per partition
      }
    }
  }

  test("optimize command with multiple partition predicates") {
    withTempDir { tempDir =>
      def writeData(count: Int): Unit = {
        spark.range(count).select('id, lit("2017-10-10").cast("date") as "date", 'id % 5 as "part")
            .write
            .partitionBy("date", "part")
            .format("delta")
            .mode("append")
            .save(tempDir.getAbsolutePath)
      }

      writeData(10)
      writeData(100)

      executeOptimizePath(tempDir.getAbsolutePath, Some("date = '2017-10-10' and part = 3"))

      val df = spark.read.format("delta").load(tempDir.getAbsolutePath)
      val deltaLog = loadDeltaLog(tempDir.getAbsolutePath)
      val part = "part".phy(deltaLog)
      val files = groupInputFilesByPartition(df.inputFiles, deltaLog)
      assert(files.filter(_._1._1 == part).minBy(_._2.length)._1 === (part, "3"),
        "part 3 should have been optimized and have least amount of files")
    }
  }

  test("optimize command with multiple partition predicates with multiple where") {
    withTempDir { tempDir =>
      def writeData(count: Int): Unit = {
        spark.range(count).select('id, lit("2017-10-10").cast("date") as "date", 'id % 5 as "part")
          .write
          .partitionBy("date", "part")
          .format("delta")
          .mode("append")
          .save(tempDir.getAbsolutePath)
      }

      writeData(10)
      writeData(100)

      DeltaTable.forPath(tempDir.getAbsolutePath).optimize()
        .where("part = 3")
        .where("date = '2017-10-10'")
        .executeCompaction()

      val df = spark.read.format("delta").load(tempDir.getAbsolutePath)
      val deltaLog = loadDeltaLog(tempDir.getAbsolutePath)
      val part = "part".phy(deltaLog)
      val files = groupInputFilesByPartition(df.inputFiles, deltaLog)
      assert(files.filter(_._1._1 == part).minBy(_._2.length)._1 === (part, "3"),
        "part 3 should have been optimized and have least amount of files")
    }
  }

  def optimizeWithBatching(
      batchSize: String,
      expectedCommits: Int,
      condition: Option[String],
      partitionFileCount: Map[String, Int]): Unit = {
    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_BATCH_SIZE.key -> batchSize) {
      withTempDir { tempDir =>
        def writeData(count: Int): Unit = {
          spark.range(count).select('id, 'id % 5 as "part")
            .coalesce(1)
            .write
            .partitionBy("part")
            .format("delta")
            .mode("append")
            .save(tempDir.getAbsolutePath)
        }

        writeData(10)
        writeData(100)

        val data = spark.read.format("delta").load(tempDir.getAbsolutePath()).collect()

        executeOptimizePath(tempDir.getAbsolutePath, condition)

        val df = spark.read.format("delta").load(tempDir.getAbsolutePath)
        checkAnswer(df, data)

        val deltaLog = loadDeltaLog(tempDir.getAbsolutePath)

        val commits = deltaLog.history.getHistory(None)
        assert(commits.filter(_.operation == "OPTIMIZE").length == expectedCommits)

        val files = groupInputFilesByPartition(df.inputFiles, deltaLog)
        for ((part, fileCount) <- partitionFileCount) {
          assert(files(("part", part)).length == fileCount)
        }
      }
    }
  }

  test("optimize command with batching") {
    // Batch size of 1 byte means each bin will run in its own batch, and lead to 5 batches,
    // one for each partition.
    Seq(("1", 5), ("1g", 1)).foreach { case (batchSize, optimizeCommits) =>
      // All partitions should be one file after optimizing
      val partitionFileCount = (0 to 4).map(_.toString -> 1).toMap

      optimizeWithBatching(batchSize, optimizeCommits, None, partitionFileCount)
    }
  }

  test("optimize command with where clause and batching") {
    // Batch size of 1 byte means each bin will run in its own batch, and lead to 2 batches
    // for the two partitions we are optimizing.
    Seq(("1", 2), ("1g", 1)).foreach { case (batchSize, optimizeCommits) =>
      // First two partitions should have 1 file, last 3 should have two
      val partitionFileCount = Map(
        "0" -> 1,
        "1" -> 1,
        "2" -> 2,
        "3" -> 2,
        "4" -> 2
      )
      val files = optimizeWithBatching(batchSize, optimizeCommits, Some("part <= 1"),
        partitionFileCount)
    }
  }

  test("optimize an empty table with batching") {
    // Batch size of 1 byte means each bin will run in its own batch
    withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_BATCH_SIZE.key -> "1") {
      withTempDir { tempDir =>
        DeltaTable.create(spark)
          .location(tempDir.getAbsolutePath())
          .addColumn("id", LongType)
          .addColumn("part", LongType)
          .partitionedBy("part")
          .execute()

        // Just make sure it succeeds
        executeOptimizePath(tempDir.getAbsolutePath)

        assert(spark.read.format("delta").load(tempDir.getAbsolutePath()).count() == 0)
      }
    }
  }

  /**
   * Utility method to append the given data to the Delta table located at the given path.
   * Optionally partitions the data.
   */
  protected def appendToDeltaTable[T](
      data: Dataset[T], tablePath: String, partitionColumns: Option[Seq[String]] = None): Unit = {
    var df = data.repartition(1).write;
    partitionColumns.map(columns => {
      df = df.partitionBy(columns: _*)
    })
    df.format("delta").mode("append").save(tablePath)
  }
}

/**
 * Runs optimize compaction tests using OPTIMIZE SQL
 */
class OptimizeCompactionSQLSuite extends OptimizeCompactionSuiteBase
    with DeltaSQLCommandTest {
  import testImplicits._

  def executeOptimizeTable(table: String, condition: Option[String] = None): Unit = {
    val conditionClause = condition.map(c => s"WHERE $c").getOrElse("")
    spark.sql(s"OPTIMIZE $table $conditionClause")
  }

  def executeOptimizePath(path: String, condition: Option[String] = None): Unit = {
    executeOptimizeTable(s"'$path'", condition)
  }

  test("optimize command: missing path") {
    val e = intercept[ParseException] {
      spark.sql(s"OPTIMIZE")
    }
    assert(e.getMessage.contains("OPTIMIZE"))
  }

  test("optimize command: missing predicate on path") {
    val e = intercept[ParseException] {
      spark.sql(s"OPTIMIZE /doesnt/exist WHERE")
    }
    assert(e.getMessage.contains("OPTIMIZE"))
  }

  test("optimize command: non boolean expression") {
    val e = intercept[ParseException] {
      spark.sql(s"OPTIMIZE /doesnt/exist WHERE 1+1")
    }
    assert(e.getMessage.contains("OPTIMIZE"))
  }

  test("optimize with partition value containing space") {
    withTempDir { tempDir =>
      val baseDf = Seq(("a space", 1), ("other", 2)).toDF("name", "value")

      def write(): Unit = {
        baseDf.write
          .format("delta")
          .partitionBy("name")
          .mode("append")
          .save(tempDir.getAbsolutePath)
      }

      write()
      write()

      sql(s"optimize '${tempDir.getAbsolutePath}'")
      val df = spark.read.format("delta").load(tempDir.getAbsolutePath)
      assert(df.inputFiles.length === 2, "2 files for 2 partitions")
      checkAnswer(
        df,
        baseDf.union(baseDf))
    }
  }

  test("optimize command: subquery predicate") {
    val tableName = "myTable"
    withTable(tableName) {
      spark.sql(s"create table $tableName (p int, id int) using delta partitioned by(p)")
      val e = intercept[DeltaAnalysisException] {
        spark.sql(s"optimize $tableName where p >= (select p from $tableName where id > 5)")
      }
      checkError(e, "DELTA_UNSUPPORTED_SUBQUERY_IN_PARTITION_PREDICATES",
        "0AKDC", Map.empty[String, String])
    }
  }
}

/**
 * Runs optimize compaction tests using OPTIMIZE Scala APIs
 */
class OptimizeCompactionScalaSuite extends OptimizeCompactionSuiteBase
    with DeltaSQLCommandTest {
  def executeOptimizeTable(table: String, condition: Option[String] = None): Unit = {
    if (condition.isDefined) {
      DeltaTable.forName(table).optimize().where(condition.get).executeCompaction()
    } else {
      DeltaTable.forName(table).optimize().executeCompaction()
    }
  }

  def executeOptimizePath(path: String, condition: Option[String] = None): Unit = {
    if (condition.isDefined) {
      DeltaTable.forPath(path).optimize().where(condition.get).executeCompaction()
    } else {
      DeltaTable.forPath(path).optimize().executeCompaction()
    }
  }
}

trait OptimizeCompactionColumnMappingSuiteBase extends DeltaColumnMappingSelectedTestMixin {
  override protected def runOnlyTests = Seq(
    "optimize command: on table with multiple partition columns",
    "optimize command: on null partition columns"
  )
}

class OptimizeCompactionIdColumnMappingSuite extends OptimizeCompactionSQLSuite
  with DeltaColumnMappingEnableIdMode
  with OptimizeCompactionColumnMappingSuiteBase {
}

class OptimizeCompactionNameColumnMappingSuite extends OptimizeCompactionSQLSuite
  with DeltaColumnMappingEnableNameMode
  with OptimizeCompactionColumnMappingSuiteBase
