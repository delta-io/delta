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
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import org.apache.spark.sql.delta.DeltaOperations.{Delete, Write}
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile, Metadata, RemoveFile}
import org.apache.spark.sql.delta.commands.VacuumCommand
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.scalatest.GivenWhenThen

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ManualClock

trait DeltaVacuumSuiteBase extends QueryTest
  with SharedSparkSession
  with GivenWhenThen
  with SQLTestUtils
  with DeletionVectorsTestUtils
  with DeltaTestUtilsForTempViews {

  protected def withEnvironment(f: (File, ManualClock) => Unit): Unit = {
    withTempDir { file =>
      val clock = new ManualClock()
      withSQLConf("spark.databricks.delta.retentionDurationCheck.enabled" -> "false") {
        f(file, clock)
      }
    }
  }

  protected def defaultTombstoneInterval: Long = {
    DeltaConfigs.getMilliSeconds(
      IntervalUtils.safeStringToInterval(
        UTF8String.fromString(DeltaConfigs.TOMBSTONE_RETENTION.defaultValue)))
  }

  /** Lists the data files in a given dir recursively. */
  protected def listDataFiles(spark: SparkSession, tableDir: String): Seq[String] = {
    val result = ArrayBuffer.empty[String]
    // scalastyle:off deltahadoopconfiguration
    val fs = FileSystem.get(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    val iterator = fs.listFiles(fs.makeQualified(new Path(tableDir)), true)
    while (iterator.hasNext) {
      val path = iterator.next().getPath.toUri.toString
      if (path.endsWith(".parquet") && !path.contains(".checkpoint")) {
        result += path
      }
    }
    result.toSeq
  }

  protected def assertNumFiles(
      deltaLog: DeltaLog,
      addFiles: Int,
      addFilesWithDVs: Int,
      dvFiles: Int,
      dataFiles: Int): Unit = {
    assert(deltaLog.update().allFiles.count() === addFiles)
    assert(getFilesWithDeletionVectors(deltaLog).size === addFilesWithDVs)
    assert(listDeletionVectors(deltaLog).size === dvFiles)
    assert(listDataFiles(spark, deltaLog.dataPath.toString).size === dataFiles)
  }

  implicit def fileToPathString(f: File): String = new Path(f.getAbsolutePath).toString

  trait Operation
  /**
   * Write a file to the given absolute or relative path. Could be inside or outside the Reservoir
   * base path. The file can be committed to the action log to be tracked, or left out for deletion.
   */
  case class CreateFile(
      path: String,
      commitToActionLog: Boolean,
      partitionValues: Map[String, String] = Map.empty) extends Operation
  /** Create a directory at the given path. */
  case class CreateDirectory(path: String) extends Operation
  /**
   * Logically deletes a file in the action log. Paths can be absolute or relative paths, and can
   * point to files inside and outside a reservoir.
   */
  case class LogicallyDeleteFile(path: String) extends Operation
  /** Check that the given paths exist. */
  case class CheckFiles(paths: Seq[String], exist: Boolean = true) extends Operation
  /** Garbage collect the reservoir. */
  case class GC(
      dryRun: Boolean,
      expectedDf: Seq[String],
      retentionHours: Option[Double] = None) extends Operation
  /** Garbage collect the reservoir. */
  case class ExecuteVacuumInScala(
      deltaTable: io.delta.tables.DeltaTable,
      expectedDf: Seq[String],
      retentionHours: Option[Double] = None) extends Operation
  /** Advance the time. */
  case class AdvanceClock(timeToAdd: Long) extends Operation
  /** Execute SQL command */
  case class ExecuteVacuumInSQL(
      identifier: String,
      expectedDf: Seq[String],
      retentionHours: Option[Long] = None,
      dryRun: Boolean = false) extends Operation {
    def sql: String = {
      val retainStr = retentionHours.map { h => s"RETAIN $h HOURS"}.getOrElse("")
      val dryRunStr = if (dryRun) "DRY RUN" else ""
      s"VACUUM $identifier $retainStr $dryRunStr"
    }
  }
  /**
   * Expect a failure with the given exception type. Expect the given `msg` fragments as the error
   * message.
   */
  case class ExpectFailure[T <: Throwable](
      action: Operation,
      expectedError: Class[T],
      msg: Seq[String]) extends Operation

  protected def createFile(
      reservoirBase: String,
      filePath: String,
      file: File,
      clock: ManualClock,
      partitionValues: Map[String, String] = Map.empty): AddFile = {
    FileUtils.write(file, "gibberish")
    file.setLastModified(clock.getTimeMillis())
    AddFile(filePath, partitionValues, 10L, clock.getTimeMillis(), dataChange = true)
  }

  protected def gcTest(deltaLog: DeltaLog, clock: ManualClock)(actions: Operation*): Unit = {
    import testImplicits._
    val basePath = deltaLog.dataPath.toString
    val fs = new Path(basePath).getFileSystem(deltaLog.newDeltaHadoopConf())
    actions.foreach {
      case CreateFile(path, commit, partitionValues) =>
        Given(s"*** Writing file to $path. Commit to log: $commit")
        val sanitizedPath = new Path(path).toUri.toString
        val file = new File(
          fs.makeQualified(DeltaFileOperations.absolutePath(basePath, sanitizedPath)).toUri)
        if (commit) {
          if (!DeltaTableUtils.isDeltaTable(spark, new Path(basePath))) {
            // initialize the table
            deltaLog.startTransaction().commitManually()
          }
          val txn = deltaLog.startTransaction()
          val action = createFile(basePath, sanitizedPath, file, clock, partitionValues)
          txn.commit(Seq(action), Write(SaveMode.Append))
        } else {
          createFile(basePath, path, file, clock)
        }
      case CreateDirectory(path) =>
        Given(s"*** Creating directory at $path")
        val dir = new File(DeltaFileOperations.absolutePath(basePath, path).toUri)
        assert(dir.mkdir(), s"Couldn't create directory at $path")
        assert(dir.setLastModified(clock.getTimeMillis()))
      case LogicallyDeleteFile(path) =>
        Given(s"*** Removing files")
        val txn = deltaLog.startTransaction()
        // scalastyle:off
        val metrics = Map[String, SQLMetric](
          "numRemovedFiles" -> createMetric(sparkContext, "number of files removed."),
          "numAddedFiles" -> createMetric(sparkContext, "number of files added."),
          "numDeletedRows" -> createMetric(sparkContext, "number of rows deleted."),
          "numCopiedRows" -> createMetric(sparkContext, "total number of rows.")
        )
        txn.registerSQLMetrics(spark, metrics)
        txn.commit(Seq(RemoveFile(path, Option(clock.getTimeMillis()))),
          Delete(Seq(Literal.TrueLiteral)))
      // scalastyle:on
      case e: ExecuteVacuumInSQL =>
        Given(s"*** Executing SQL: ${e.sql}")
        val qualified = e.expectedDf.map(p => fs.makeQualified(new Path(p)).toString)
        val df = spark.sql(e.sql).as[String]
        checkDatasetUnorderly(df, qualified: _*)
      case CheckFiles(paths, exist) =>
        Given(s"*** Checking files exist=$exist")
        paths.foreach { p =>
          val sp = new Path(p).toUri.toString
          val f = new File(fs.makeQualified(DeltaFileOperations.absolutePath(basePath, sp)).toUri)
          val res = if (exist) f.exists() else !f.exists()
          assert(res, s"Expectation: exist=$exist, paths: $p")
        }
      case GC(dryRun, expectedDf, retention) =>
        Given("*** Garbage collecting Reservoir")
        val result = VacuumCommand.gc(spark, deltaLog, dryRun, retention, clock = clock)
        val qualified = expectedDf.map(p => fs.makeQualified(new Path(p)).toString)
        checkDatasetUnorderly(result.as[String], qualified: _*)
      case ExecuteVacuumInScala(deltaTable, expectedDf, retention) =>
        Given("*** Garbage collecting Reservoir using Scala")
        val result = if (retention.isDefined) {
          deltaTable.vacuum(retention.get)
        } else {
          deltaTable.vacuum()
        }
        if(expectedDf == Seq()) {
          assert(result === spark.emptyDataFrame)
        } else {
          val qualified = expectedDf.map(p => fs.makeQualified(new Path(p)).toString)
          checkDatasetUnorderly(result.as[String], qualified: _*)
        }
      case AdvanceClock(timeToAdd: Long) =>
        Given(s"*** Advancing clock by $timeToAdd millis")
        clock.advance(timeToAdd)
      case ExpectFailure(action, failure, msg) =>
        Given(s"*** Expecting failure of ${failure.getName} for action: $action")
        val e = intercept[Exception](gcTest(deltaLog, clock)(action))
        assert(e.getClass === failure)
        assert(
          msg.forall(m =>
            e.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT))),
          e.getMessage + "didn't contain: " + msg.mkString("[", ", ", "]"))
    }
  }

  protected def vacuumSQLTest(tablePath: String, identifier: String) {
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    val committedFile = "committedFile.txt"
    val notCommittedFile = "notCommittedFile.txt"

    gcTest(deltaLog, new ManualClock())(
      // Prepare the table with files with timestamp of epoch-time 0 (i.e. 01-01-1970 00:00)
      CreateFile(committedFile, commitToActionLog = true),
      CreateFile(notCommittedFile, commitToActionLog = false),
      CheckFiles(Seq(committedFile, notCommittedFile)),

      // Dry run should return the not committed file and but not delete files
      ExecuteVacuumInSQL(
        identifier,
        expectedDf = Seq(new File(tablePath, notCommittedFile).toString),
        dryRun = true),
      CheckFiles(Seq(committedFile, notCommittedFile)),

      // Actual run should delete the not committed file but delete the not-committed file
      ExecuteVacuumInSQL(identifier, Seq(tablePath)),
      CheckFiles(Seq(committedFile)),
      CheckFiles(Seq(notCommittedFile), exist = false), // file ts older than default retention

      // Logically delete the file.
      LogicallyDeleteFile(committedFile),
      CheckFiles(Seq(committedFile)),

      // Vacuum with 0 retention should actually delete the file.
      ExecuteVacuumInSQL(identifier, Seq(tablePath), Some(0)),
      CheckFiles(Seq(committedFile), exist = false))
  }

  protected def vacuumScalaTest(deltaTable: io.delta.tables.DeltaTable, tablePath: String) {
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    val committedFile = "committedFile.txt"
    val notCommittedFile = "notCommittedFile.txt"

    gcTest(deltaLog, new ManualClock())(
      // Prepare the table with files with timestamp of epoch-time 0 (i.e. 01-01-1970 00:00)
      CreateFile(committedFile, commitToActionLog = true),
      CreateFile(notCommittedFile, commitToActionLog = false),
      CheckFiles(Seq(committedFile, notCommittedFile)),

      // Actual run should delete the not committed file and but not delete files
      ExecuteVacuumInScala(deltaTable, Seq()),
      CheckFiles(Seq(committedFile)),
      CheckFiles(Seq(notCommittedFile), exist = false), // file ts older than default retention

      // Logically delete the file.
      LogicallyDeleteFile(committedFile),
      CheckFiles(Seq(committedFile)),

      // Vacuum with 0 retention should actually delete the file.
      ExecuteVacuumInScala(deltaTable, Seq(), Some(0)),
      CheckFiles(Seq(committedFile), exist = false))
  }

  /**
   * Helper method to tell us if the given filePath exists. Thus, it can be used to detect if a
   * file has been deleted.
   */
  protected def pathExists(deltaLog: DeltaLog, filePath: String): Boolean = {
    val fs = deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())
    fs.exists(DeltaFileOperations.absolutePath(deltaLog.dataPath.toString, filePath))
  }

  /**
   * Helper method to get all of the [[AddCDCFile]]s that exist in the delta table
   */
  protected def getCDCFiles(deltaLog: DeltaLog): Seq[AddCDCFile] = {
    val changes = deltaLog.getChanges(startVersion = 0, failOnDataLoss = true)
    changes.flatMap(_._2).collect { case a: AddCDCFile => a }.toList
  }

  protected def testCDCVacuumForUpdateMerge(): Unit = {
    withSQLConf(
      DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true",
      DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false"
    ) {
      withTempDir { dir =>
        // create table - version 0
        spark.range(10)
          .repartition(1)
          .write
          .format("delta")
          .save(dir.getAbsolutePath)
        val deltaTable = io.delta.tables.DeltaTable.forPath(dir.getAbsolutePath)
        val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)

        // update table - version 1
        deltaTable.update(expr("id == 0"), Map("id" -> lit("11")))

        // merge table - version 2
        deltaTable.as("target")
          .merge(
            spark.range(0, 12).toDF().as("src"),
            "src.id = target.id")
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute()

        val df1 = sql(s"SELECT * FROM delta.`${dir.getAbsolutePath}`").collect()

        val changes = getCDCFiles(deltaLog)
        var numExpectedChangeFiles = 2

        assert(changes.size === numExpectedChangeFiles)

        // vacuum will not delete the cdc files if they are within retention
        sql(s"VACUUM '${dir.getAbsolutePath}' RETAIN 100 HOURS")
        changes.foreach { change =>
          assert(pathExists(deltaLog, change.path)) // cdc file exists
        }

        // vacuum will delete the cdc files if they are outside retention
        sql(s"VACUUM '${dir.getAbsolutePath}' RETAIN 0 HOURS")
        changes.foreach { change =>
          assert(!pathExists(deltaLog, change.path)) // cdc file has been removed
        }

        // try reading the table
        checkAnswer(sql(s"SELECT * FROM delta.`${dir.getAbsolutePath}`"), df1)

        // try reading cdc data
        val e = intercept[SparkException] {
          spark.read
            .format("delta")
            .option(DeltaOptions.CDC_READ_OPTION, "true")
            .option("startingVersion", 1)
            .option("endingVersion", 2)
            .load(dir.getAbsolutePath)
            .count()
        }
        // QueryExecutionErrors.readCurrentFileNotFoundError
        var expectedErrorMessage = "It is possible the underlying files have been updated."
        assert(e.getMessage.contains(expectedErrorMessage))
      }
    }
  }

  protected def testCDCVacuumForTombstones(): Unit = {
    withSQLConf(
      DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true",
      DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false"
    ) {
      withTempDir { dir =>
        // create table - version 0
        spark.range(0, 10, 1, 1)
          .withColumn("part", col("id") % 2)
          .write
          .format("delta")
          .partitionBy("part")
          .save(dir.getAbsolutePath)
        val deltaTable = io.delta.tables.DeltaTable.forPath(dir.getAbsolutePath)
        val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)

        // create version 1 - delete single row should generate one cdc file
        deltaTable.delete(col("id") === lit(9))
        val changes = getCDCFiles(deltaLog)
        assert(changes.size === 1)
        val cdcPath = changes.head.path
        assert(pathExists(deltaLog, cdcPath))
        val df1 = sql(s"SELECT * FROM delta.`${dir.getAbsolutePath}`").collect()

        // vacuum will not delete the cdc files if they are within retention
        sql(s"VACUUM '${dir.getAbsolutePath}' RETAIN 100 HOURS")
        assert(pathExists(deltaLog, cdcPath)) // cdc path exists

        // vacuum will delete the cdc files when they are outside retention
        // one cdc file and one RemoveFile should be deleted by vacuum
        sql(s"VACUUM '${dir.getAbsolutePath}' RETAIN 0 HOURS")
        assert(!pathExists(deltaLog, cdcPath)) // cdc file is removed

        // try reading the table
        checkAnswer(sql(s"SELECT * FROM delta.`${dir.getAbsolutePath}`"), df1)

        // create version 2 - partition delete - does not create new cdc files
        deltaTable.delete(col("part") === lit(0))

        assert(getCDCFiles(deltaLog).size === 1) // still just the one cdc file from before.

        // try reading cdc data
        val e = intercept[SparkException] {
          spark.read
            .format("delta")
            .option(DeltaOptions.CDC_READ_OPTION, "true")
            .option("startingVersion", 1)
            .option("endingVersion", 2)
            .load(dir.getAbsolutePath)
            .count()
        }
        // QueryExecutionErrors.readCurrentFileNotFoundError
        var expectedErrorMessage = "It is possible the underlying files have been updated."
        assert(e.getMessage.contains(expectedErrorMessage))
      }
    }
  }
}

class DeltaVacuumSuite
  extends DeltaVacuumSuiteBase with DeltaSQLCommandTest {
  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.sources.parallelPartitionDiscovery.parallelism", "2")
  }

  testQuietly("basic case - SQL command on path-based tables with direct 'path'") {
    withEnvironment { (tempDir, _) =>
      vacuumSQLTest(tablePath = tempDir.getAbsolutePath, identifier = s"'$tempDir'")
    }
  }

  testQuietly("basic case - SQL command on path-based table with delta.`path`") {
    withEnvironment { (tempDir, _) =>
      vacuumSQLTest(tablePath = tempDir.getAbsolutePath, identifier = s"delta.`$tempDir`")
    }
  }

  testQuietly("basic case - SQL command on name-based table") {
    val tableName = "deltaTable"
    withEnvironment { (_, _) =>
      withTable(tableName) {
        import testImplicits._
        spark.emptyDataset[Int].write.format("delta").saveAsTable(tableName)
        val tablePath =
          new File(spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName)).location)
        vacuumSQLTest(tablePath, tableName)
      }
    }
  }

  testQuietlyWithTempView("basic case - SQL command on temp view not supported") { isSQLTempView =>
    val tableName = "deltaTable"
    val viewName = "v"
    withEnvironment { (_, _) =>
      withTable(tableName) {
        import testImplicits._
        spark.emptyDataset[Int].write.format("delta").saveAsTable(tableName)
        createTempViewFromTable(tableName, isSQLTempView)
        val tablePath = new File(
          spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName)).location)
        val e = intercept[AnalysisException] {
          vacuumSQLTest(tablePath, viewName)
        }
        assert(e.getMessage.contains("not found") ||
          e.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND"))
      }
    }
  }

  test("basic case - Scala on path-based table") {
    withEnvironment { (tempDir, _) =>
      import testImplicits._
      spark.emptyDataset[Int].write.format("delta").save(tempDir.getAbsolutePath)
      val deltaTable = io.delta.tables.DeltaTable.forPath(tempDir.getAbsolutePath)
      vacuumScalaTest(deltaTable, tempDir.getAbsolutePath)
    }
  }

  test("basic case - Scala on name-based table") {
    val tableName = "deltaTable"
    withEnvironment { (tempDir, _) =>
      withTable(tableName) {
        // Initialize the table so that we can create the DeltaTable object
        import testImplicits._
        spark.emptyDataset[Int].write.format("delta").saveAsTable(tableName)
        val deltaTable = io.delta.tables.DeltaTable.forName(tableName)
        val tablePath =
          new File(spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName)).location)
        vacuumScalaTest(deltaTable, tablePath)
      }
    }
  }

  test("don't delete data in a non-reservoir") {
    withEnvironment { (tempDir, clock) =>
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath, clock)
      gcTest(deltaLog, clock)(
        CreateFile("file1.txt", commitToActionLog = false),
        CreateDirectory("abc"),
        ExpectFailure(
          GC(dryRun = false, Nil), classOf[IllegalArgumentException], Seq("no state defined"))
      )
    }
  }

  test("invisible files and dirs") {
    withEnvironment { (tempDir, clock) =>
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath, clock)
      gcTest(deltaLog, clock)(
        CreateFile("file1.txt", commitToActionLog = true),
        CreateFile("_hidden_dir/000001.text", commitToActionLog = false),
        CreateFile(".hidden.txt", commitToActionLog = false),
        AdvanceClock(defaultTombstoneInterval + 1000),
        GC(dryRun = false, Seq(tempDir)),
        CheckFiles(Seq(
          "file1.txt", "_delta_log", "_hidden_dir", "_hidden_dir/000001.text", ".hidden.txt"))
      )
    }
  }

  test("partition column name starting with underscore") {
    // We should be able to see inside partition directories to GC them, even if they'd normally
    // be considered invisible because of their name.
    withEnvironment { (tempDir, clock) =>
      val deltaLog = DeltaLog.forTable(spark, tempDir, clock)
      val txn = deltaLog.startTransaction()
      val schema = new StructType().add("_underscore_col_", IntegerType).add("n", IntegerType)
      val metadata =
        Metadata(schemaString = schema.json, partitionColumns = Seq("_underscore_col_"))
      txn.commit(metadata :: Nil, DeltaOperations.CreateTable(metadata, isManaged = true))
      gcTest(deltaLog, clock)(
        CreateFile("file1.txt", commitToActionLog = true, Map("_underscore_col_" -> "10")),
        CreateFile("_underscore_col_=10/test.txt", true, Map("_underscore_col_" -> "10")),
        CheckFiles(Seq("file1.txt", "_underscore_col_=10")),
        LogicallyDeleteFile("_underscore_col_=10/test.txt"),
        AdvanceClock(defaultTombstoneInterval + 1000),
        GC(dryRun = false, Seq(tempDir)),
        CheckFiles(Seq("file1.txt")),
        CheckFiles(Seq("_underscore_col_=10/test.txt"), exist = false)
      )
    }
  }

  test("multiple levels of empty directory deletion") {
    withEnvironment { (tempDir, clock) =>
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath, clock)
      gcTest(deltaLog, clock)(
        CreateFile("file1.txt", commitToActionLog = true),
        CreateFile("abc/def/file2.txt", commitToActionLog = false),
        AdvanceClock(defaultTombstoneInterval + 1000),
        GC(dryRun = false, Seq(tempDir)),
        CheckFiles(Seq("file1.txt", "abc", "abc/def")),
        CheckFiles(Seq("abc/def/file2.txt"), exist = false),
        GC(dryRun = false, Seq(tempDir)),
        // we need two GCs to guarantee the deletion of the directories
        GC(dryRun = false, Seq(tempDir)),
        CheckFiles(Seq("file1.txt")),
        CheckFiles(Seq("abc", "abc/def"), exist = false)
      )
    }
  }

  test("gc doesn't delete base path") {
    withEnvironment { (tempDir, clock) =>
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath, clock)
      gcTest(deltaLog, clock)(
        CreateFile("file1.txt", commitToActionLog = true),
        AdvanceClock(100),
        LogicallyDeleteFile("file1.txt"),
        AdvanceClock(defaultTombstoneInterval + 1000),
        GC(dryRun = false, Seq(tempDir.toString)),
        CheckFiles(Seq("file1.txt"), exist = false),
        GC(dryRun = false, Seq(tempDir.toString)) // shouldn't throw an error
      )
    }
  }

  testQuietly("correctness test") {
    withEnvironment { (tempDir, clock) =>

      val reservoirDir = new File(tempDir.getAbsolutePath, "reservoir")
      assert(reservoirDir.mkdirs())
      val externalDir = new File(tempDir.getAbsolutePath, "external")
      assert(externalDir.mkdirs())
      val deltaLog = DeltaLog.forTable(spark, reservoirDir, clock)

      val externalFile = new File(externalDir, "file4.txt").getAbsolutePath

      gcTest(deltaLog, clock)(
        // Create initial state
        CreateFile("file1.txt", commitToActionLog = true),
        CreateDirectory("abc"),
        CreateFile("abc/file2.txt", commitToActionLog = true),
        CheckFiles(Seq("file1.txt", "abc", "abc/file2.txt")),

        // Nothing should be deleted here, since we didn't logically delete any file
        AdvanceClock(defaultTombstoneInterval + 1000),
        GC(dryRun = false, Seq(reservoirDir.toString)),
        CheckFiles(Seq("file1.txt", "abc", "abc/file2.txt")),

        // Create an untracked file
        CreateFile("file3.txt", commitToActionLog = false),
        CheckFiles(Seq("file3.txt")),
        GC(dryRun = false, Seq(reservoirDir.toString)),
        CheckFiles(Seq("file3.txt")),
        AdvanceClock(defaultTombstoneInterval - 1000), // file is still new
        GC(dryRun = false, Seq(reservoirDir.toString)),
        CheckFiles(Seq("file3.txt")),
        AdvanceClock(2000),
        GC(dryRun = true, Seq(new File(reservoirDir, "file3.txt").toString)),
        // nothing should be deleted
        CheckFiles(Seq("file1.txt", "abc", "abc/file2.txt", "file3.txt")),
        GC(dryRun = false, Seq(reservoirDir.toString)), // file3.txt should be deleted
        CheckFiles(Seq("file1.txt", "abc", "abc/file2.txt")),
        CheckFiles(Seq("file3.txt"), exist = false),

        // Verify tombstones
        LogicallyDeleteFile("abc/file2.txt"),
        GC(dryRun = false, Seq(reservoirDir.toString)),
        CheckFiles(Seq("file1.txt", "abc", "abc/file2.txt")),
        AdvanceClock(defaultTombstoneInterval - 1000),
        GC(dryRun = false, Seq(reservoirDir.toString)),
        CheckFiles(Seq("file1.txt", "abc", "abc/file2.txt")),
        AdvanceClock(2000), // tombstone should expire
        GC(dryRun = false, Seq(reservoirDir.toString)),
        CheckFiles(Seq("file1.txt", "abc")),
        CheckFiles(Seq("abc/file2.txt"), exist = false),
        GC(dryRun = false, Seq(reservoirDir.toString)), // Second gc should clear empty directory
        CheckFiles(Seq("file1.txt")),
        CheckFiles(Seq("abc"), exist = false),

        // Make sure that files outside the reservoir are not affected
        CreateFile(externalFile, commitToActionLog = true),
        AdvanceClock(100),
        CheckFiles(Seq("file1.txt", externalFile)),
        LogicallyDeleteFile(externalFile),
        AdvanceClock(defaultTombstoneInterval * 2),
        CheckFiles(Seq("file1.txt", externalFile))
      )
    }
  }

  test("parallel file delete") {
    withEnvironment { (tempDir, clock) =>
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath, clock)
      withSQLConf("spark.databricks.delta.vacuum.parallelDelete.enabled" -> "true") {
        gcTest(deltaLog, clock)(
          CreateFile("file1.txt", commitToActionLog = true),
          CreateFile("file2.txt", commitToActionLog = true),
          LogicallyDeleteFile("file1.txt"),
          CheckFiles(Seq("file1.txt", "file2.txt")),
          AdvanceClock(defaultTombstoneInterval + 1000),
          GC(dryRun = false, Seq(tempDir)),
          CheckFiles(Seq("file1.txt"), exist = false),
          CheckFiles(Seq("file2.txt")),
          GC(dryRun = false, Seq(tempDir)), // shouldn't throw an error with no files to delete
          CheckFiles(Seq("file2.txt"))
        )
      }
    }
  }

  test("retention duration must be greater than 0") {
    withEnvironment { (tempDir, clock) =>
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath, clock)
      gcTest(deltaLog, clock)(
        CreateFile("file1.txt", commitToActionLog = true),
        CheckFiles(Seq("file1.txt")),
        ExpectFailure(
          GC(false, Seq(tempDir), Some(-2)),
          classOf[IllegalArgumentException],
          Seq("Retention", "less than", "0"))
      )
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tempDir.getAbsolutePath)
      gcTest(deltaLog, clock)(
        CreateFile("file2.txt", commitToActionLog = true),
        CheckFiles(Seq("file2.txt")),
        ExpectFailure(
          ExecuteVacuumInScala(deltaTable, Seq(), Some(-2)),
          classOf[IllegalArgumentException],
          Seq("Retention", "less than", "0"))
      )
    }
  }

  test("deleting directories") {
    withEnvironment { (tempDir, clock) =>
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath, clock)
      gcTest(deltaLog, clock)(
        CreateFile("abc/def/file1.txt", commitToActionLog = true),
        CreateFile("abc/def/file2.txt", commitToActionLog = true),
        CreateDirectory("ghi"),
        CheckFiles(Seq("abc", "abc/def", "ghi")),
        GC(dryRun = true, Seq(new File(tempDir, "ghi"))),
        GC(dryRun = false, Seq(tempDir)),
        CheckFiles(Seq("abc", "abc/def")),
        CheckFiles(Seq("ghi"), exist = false)
      )
    }
  }

  test("deleting files with special characters in path") {
    withEnvironment { (tempDir, clock) =>
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath, clock)
      gcTest(deltaLog, clock)(
        CreateFile("abc def/#1/file1.txt", commitToActionLog = true),
        CreateFile("abc def/#1/file2.txt", commitToActionLog = false),
        CheckFiles(Seq("abc def", "abc def/#1")),
        AdvanceClock(defaultTombstoneInterval + 1000),
        GC(dryRun = true, Seq(new File(tempDir, "abc def/#1/file2.txt"))),
        GC(dryRun = false, Seq(tempDir)),
        CheckFiles(Seq("abc def/#1", "abc def/#1/file1.txt")),
        CheckFiles(Seq("abc def/#1/file2.txt"), exist = false)
      )
    }
  }

  testQuietly("additional retention duration check with vacuum command") {
    withEnvironment { (tempDir, clock) =>
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath, clock)
      withSQLConf("spark.databricks.delta.retentionDurationCheck.enabled" -> "true") {
        gcTest(deltaLog, clock)(
          CreateFile("file1.txt", commitToActionLog = true),
          CheckFiles(Seq("file1.txt")),
          ExpectFailure(
            GC(false, Nil, Some(0)),
            classOf[IllegalArgumentException],
            Seq("spark.databricks.delta.retentionDurationCheck.enabled = false", "168 hours"))
        )
      }

      gcTest(deltaLog, clock)(
        CreateFile("file2.txt", commitToActionLog = true),
        CheckFiles(Seq("file2.txt")),
        GC(false, Seq(tempDir.toString), Some(0))
      )
    }
  }

  test("vacuum for a partition path") {
    withEnvironment { (tempDir, _) =>
      import testImplicits._
      val path = tempDir.getCanonicalPath
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .partitionBy("v2")
        .save(path)

      val ex = intercept[AnalysisException] {
        sql(s"vacuum '$path/v2=a' retain 0 hours")
      }
      assert(ex.getMessage.contains(
        s"Please provide the base path ($path) when Vacuuming Delta tables."))
    }
  }

  test(s"vacuum table with DVs and zero retention policy throws exception by default") {
    val targetDF = spark.range(0, 100, 1, 2)
      .withColumn("value", col("id"))

      withTempDeltaTable(targetDF, enableDVs = true) { (targetTable, targetLog) =>
        // Add some DVs.
        targetTable().delete("id < 10")
        val e = intercept[IllegalArgumentException] {
          spark.sql(s"VACUUM delta.`${targetLog.dataPath}` RETAIN 0 HOURS")
        }
        assert(e.getMessage.contains(
          "Are you sure you would like to vacuum files with such a low retention period?"))
      }
  }

  test(s"vacuum after purge with zero retention policy") {
    val tableName = "testTable"
    withDeletionVectorsEnabled() {
      withSQLConf(
          DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
        withTable(tableName) {
          // Create a Delta Table with 5 files of 10 rows, and delete half rows from first 4 files.
          spark.range(0, 50, step = 1, numPartitions = 5)
            .write.format("delta").saveAsTable(tableName)
          val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
          spark.sql(s"DELETE from $tableName WHERE ID % 2 = 0 and ID < 40")
          assertNumFiles(deltaLog, addFiles = 5, addFilesWithDVs = 4, dvFiles = 1, dataFiles = 5)

          purgeDVs(tableName)

          assertNumFiles(deltaLog, addFiles = 5, addFilesWithDVs = 0, dvFiles = 1, dataFiles = 9)
          spark.sql(s"VACUUM $tableName RETAIN 0 HOURS")
          assertNumFiles(deltaLog, addFiles = 5, addFilesWithDVs = 0, dvFiles = 0, dataFiles = 5)

          checkAnswer(
            spark.read.table(tableName),
            Seq.range(0, 50).filterNot(x => x < 40 && x % 2 == 0).toDF)
        }
      }
    }
  }

  // Helper method to remove the DVs in Delta table and rewrite the data files
  def purgeDVs(tableName: String): Unit = {
    withSQLConf(
      // Set the max file size to low so that we always rewrite the single file without DVs
      // and not combining with other data files.
      DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> "2") {
      spark.sql(s"REORG TABLE $tableName APPLY (PURGE)")
    }
  }

  test(s"vacuum after purging deletion vectors") {
    val tableName = "testTable"
    val clock = new ManualClock()
    withDeletionVectorsEnabled() {
      withSQLConf(
          DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
        withTable(tableName) {
          // Create Delta table with 5 files of 10 rows.
          spark.range(0, 50, step = 1, numPartitions = 5)
            .write.format("delta").saveAsTable(tableName)
          val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
          assertNumFiles(deltaLog, addFiles = 5, addFilesWithDVs = 0, dvFiles = 0, dataFiles = 5)

          // Delete 1 row from each file. DVs will be packed to one DV file.
          val deletedRows1 = Seq(0, 10, 20, 30, 40)
          val deletedRowsStr1 = deletedRows1.mkString("(", ",", ")")
          spark.sql(s"DELETE FROM $tableName WHERE id IN $deletedRowsStr1")
          val timestampV1 = deltaLog.update().timestamp
          assertNumFiles(deltaLog, addFiles = 5, addFilesWithDVs = 5, dvFiles = 1, dataFiles = 5)

          // Delete all rows from the first file. An ephemeral DV will still be created.
          Thread.sleep(1000) // Ensure it's been at least 1000 ms since V1
          spark.sql(s"DELETE FROM $tableName WHERE id < 10")
          val timestampV2 = deltaLog.update().timestamp
          assertNumFiles(deltaLog, addFiles = 4, addFilesWithDVs = 4, dvFiles = 2, dataFiles = 5)
          val expectedAnswerV2 = Seq.range(0, 50).filterNot(deletedRows1.contains).filterNot(_ < 10)

          // Delete 1 more row from each file.
          Thread.sleep(1000) // Ensure it's been at least 1000 ms since V2
          val deletedRows2 = Seq(11, 21, 31, 41)
          val deletedRowsStr2 = deletedRows2.mkString("(", ",", ")")
          spark.sql(s"DELETE FROM $tableName WHERE id IN $deletedRowsStr2")
          val timestampV3 = deltaLog.update().timestamp
          assertNumFiles(deltaLog, addFiles = 4, addFilesWithDVs = 4, dvFiles = 3, dataFiles = 5)
          val expectedAnswerV3 = expectedAnswerV2.filterNot(deletedRows2.contains)

          // Delete DVs by rewriting the data files with DVs.
          Thread.sleep(1000) // Ensure it's been at least 1000 ms since V3
          purgeDVs(tableName)

          val numFilesAfterPurge = 4
          val timestampV4 = deltaLog.update().timestamp
          assertNumFiles(deltaLog, addFiles = numFilesAfterPurge, addFilesWithDVs = 0, dvFiles = 3,
            dataFiles = 9)

          // Run VACUUM with nothing expired. It should not delete anything.
          clock.setTime(System.currentTimeMillis())
          VacuumCommand.gc(spark, deltaLog, retentionHours = Some(1), clock = clock, dryRun = false)
          assertNumFiles(deltaLog, addFiles = numFilesAfterPurge, addFilesWithDVs = 0, dvFiles = 3,
            dataFiles = 9)

          // Run VACUUM @ V1.
          // We need to add 1000 ms for local filesystems that only write modificationTimes to the s
          clock.setTime(timestampV1 + TimeUnit.HOURS.toMillis(1) + 1000)
          VacuumCommand.gc(spark, deltaLog, retentionHours = Some(1), clock = clock, dryRun = false)
          assertNumFiles(deltaLog, addFiles = numFilesAfterPurge, addFilesWithDVs = 0, dvFiles = 3,
            dataFiles = 9)

          // Run VACUUM @ V2. It should delete the ephemeral DV and the removed Parquet file.
          clock.setTime(timestampV2 + TimeUnit.HOURS.toMillis(1) + 1000)
          VacuumCommand.gc(spark, deltaLog, retentionHours = Some(1), clock = clock, dryRun = false)
          assertNumFiles(deltaLog, addFiles = numFilesAfterPurge, addFilesWithDVs = 0, dvFiles = 2,
            dataFiles = 8)
          checkAnswer(
            spark.sql(s"SELECT * FROM $tableName VERSION AS OF 2"), expectedAnswerV2.toDF)

          // Run VACUUM @ V3. It should delete the persistent DVs from V1.
          clock.setTime(timestampV3 + TimeUnit.HOURS.toMillis(1) + 1000)
          VacuumCommand.gc(spark, deltaLog, retentionHours = Some(1), clock = clock, dryRun = false)
          assertNumFiles(deltaLog, addFiles = numFilesAfterPurge, addFilesWithDVs = 0, dvFiles = 1,
            dataFiles = 8)
          checkAnswer(
            spark.sql(s"SELECT * FROM $tableName VERSION AS OF 3"), expectedAnswerV3.toDF)

          // Run VACUUM @ V4. It should delete the Parquet files and DVs of V3.
          clock.setTime(timestampV4 + TimeUnit.HOURS.toMillis(1) + 1000)
          VacuumCommand.gc(spark, deltaLog, retentionHours = Some(1), clock = clock, dryRun = false)
          assertNumFiles(deltaLog, addFiles = numFilesAfterPurge, addFilesWithDVs = 0, dvFiles = 0,
            dataFiles = 4)
          checkAnswer(
            spark.sql(s"SELECT * FROM $tableName VERSION AS OF 4"), expectedAnswerV3.toDF)

          // Run VACUUM with zero retention period. It should not delete anything.
          clock.setTime(timestampV4 + TimeUnit.HOURS.toMillis(1) + 1000)
          VacuumCommand.gc(spark, deltaLog, retentionHours = Some(0), clock = clock, dryRun = false)
          assertNumFiles(deltaLog, addFiles = numFilesAfterPurge, addFilesWithDVs = 0, dvFiles = 0,
            dataFiles = 4)

          // Last version should still be readable.
          checkAnswer(spark.sql(s"SELECT * FROM $tableName"), expectedAnswerV3.toDF)
        }
      }
    }
  }

  for (partitioned <- DeltaTestUtils.BOOLEAN_DOMAIN) {
    test(s"delete persistent deletion vectors - partitioned = $partitioned") {
      val targetDF = spark.range(0, 100, 1, 10).toDF
        .withColumn("v", col("id"))
        .withColumn("partCol", lit(0))
      val partitionBy = if (partitioned) Seq("partCol") else Seq.empty
      withSQLConf(
          DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE.key -> "0",
          DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
        withDeletionVectorsEnabled() {
          withTempDeltaTable(
              targetDF,
              partitionBy = partitionBy) { (targetTable, targetLog) =>
            val targetDir = targetLog.dataPath

            // Add a DV to all files and check that DVs are not deleted.
            targetTable().delete("id % 2 == 0")

            assert(listDeletionVectors(targetLog).size == 10)
            targetTable().vacuum(0)
            assert(listDeletionVectors(targetLog).size == 10)
            checkAnswer(sql(s"select count(*) from delta.`$targetDir`"), Row(50))

            // Update the DV of the first file by deleting two rows and check that previous DV is
            // deleted.
            targetTable().delete("id  < 10 AND id % 3 == 0")

            assert(listDeletionVectors(targetLog).size == 11)
            targetTable().vacuum(0)
            assert(listDeletionVectors(targetLog).size == 10)
            checkAnswer(sql(s"select count(*) from delta.`$targetDir`"), Row(48))

            // Delete all rows in first 5 files and check that DVs are not deleted due to
            // the retention period, but deleted after that.
            targetTable().delete("id < 50")

            assert(listDeletionVectors(targetLog).size == 15)
            targetTable().vacuum(10)
            assert(listDeletionVectors(targetLog).size == 15)
            targetTable().vacuum(0)
            assert(listDeletionVectors(targetLog).size == 5)
            checkAnswer(sql(s"select count(*) from delta.`$targetDir`"), Row(25))
          }
        }
      }
    }
  }

  test("vacuum a non-existent path and a non Delta table") {
    def assertNotADeltaTableException(path: String): Unit = {
      for (table <- Seq(s"'$path'", s"delta.`$path`")) {
        val e = intercept[AnalysisException] {
          sql(s"vacuum $table")
        }
        Seq("VACUUM", "only supported for Delta tables").foreach { msg =>
          assert(e.getMessage.contains(msg))
        }
      }
    }
    withTempPath { tempDir =>
      assert(!tempDir.exists())
      assertNotADeltaTableException(tempDir.getCanonicalPath)
    }
    withTempPath { tempDir =>
      spark.range(1, 10).write.parquet(tempDir.getCanonicalPath)
      assertNotADeltaTableException(tempDir.getCanonicalPath)
    }
  }

  test("vacuum for cdc - update/merge") {
    testCDCVacuumForUpdateMerge()
  }

  test("vacuum for cdc - delete tombstones") {
    testCDCVacuumForTombstones()
  }

  private def getFromHistory(history: DataFrame, key: String, pos: Integer): Map[String, String] = {
    val op = history.select(key).take(pos + 1)
    if (pos == 0) {
      op.head.getMap(0).asInstanceOf[Map[String, String]]
    } else {
      op.tail.head.getMap(0).asInstanceOf[Map[String, String]]
    }
  }

  private def testEventLogging(
      isDryRun: Boolean,
      loggingEnabled: Boolean,
      retentionHours: Long,
      timeGapHours: Long): Unit = {

    test(s"vacuum event logging dryRun=$isDryRun loggingEnabled=$loggingEnabled" +
      s" retentionHours=$retentionHours timeGap=$timeGapHours") {
      withSQLConf(DeltaSQLConf.DELTA_VACUUM_LOGGING_ENABLED.key -> loggingEnabled.toString) {

        withEnvironment { (dir, clock) =>
          spark.range(2).write.format("delta").save(dir.getAbsolutePath)
          val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath, clock)
          val expectedReturn = if (isDryRun) {
            // dry run returns files that will be deleted
            Seq(new Path(dir.getAbsolutePath, "file1.txt").toString)
          } else {
            Seq(dir.getAbsolutePath)
          }

          gcTest(deltaLog, clock)(
            CreateFile("file1.txt", commitToActionLog = true),
            CreateFile("file2.txt", commitToActionLog = true),
            LogicallyDeleteFile("file1.txt"),
            AdvanceClock(timeGapHours * 1000 * 60 * 60),
            GC(dryRun = isDryRun, expectedReturn, Some(retentionHours))
          )
          val deltaTable = io.delta.tables.DeltaTable.forPath(deltaLog.dataPath.toString)
          val history = deltaTable.history()
          if (isDryRun || !loggingEnabled) {
            // We do not record stats when logging is disabled or dryRun
            assert(history.select("operation").head() == Row("DELETE"))
          } else {
            assert(history.select("operation").head() == Row("VACUUM END"))
            assert(history.select("operation").collect()(1) == Row("VACUUM START"))

            val operationParamsBegin = getFromHistory(history, "operationParameters", 1)
            val operationParamsEnd = getFromHistory(history, "operationParameters", 0)
            val operationMetricsBegin = getFromHistory(history, "operationMetrics", 1)
            val operationMetricsEnd = getFromHistory(history, "operationMetrics", 0)

            val filesDeleted = if (retentionHours > timeGapHours) { 0 } else { 1 }
            assert(operationParamsBegin("retentionCheckEnabled") === "false")
            assert(operationMetricsBegin("numFilesToDelete") === filesDeleted.toString)
            assert(operationMetricsBegin("sizeOfDataToDelete") === (filesDeleted * 9).toString)
            assert(
              operationParamsBegin("specifiedRetentionMillis") ===
                (retentionHours * 60 * 60 * 1000).toString)
            assert(
              operationParamsBegin("defaultRetentionMillis") ===
                DeltaLog.tombstoneRetentionMillis(deltaLog.snapshot.metadata).toString)

            assert(operationParamsEnd === Map("status" -> "COMPLETED"))
            assert(operationMetricsEnd === Map("numDeletedFiles" -> filesDeleted.toString,
              "numVacuumedDirectories" -> "1"))
          }
        }
      }
    }
  }

  testEventLogging(
    isDryRun = false,
    loggingEnabled = true,
    retentionHours = 5,
    timeGapHours = 10
  )

  testEventLogging(
    isDryRun = true, // dry run will not record the vacuum
    loggingEnabled = true,
    retentionHours = 5,
    timeGapHours = 10
  )

  testEventLogging(
    isDryRun = false,
    loggingEnabled = false,
    retentionHours = 5,
    timeGapHours = 0
  )

  testEventLogging(
    isDryRun = false,
    loggingEnabled = true,
    retentionHours = 20, // vacuum will not delete any files
    timeGapHours = 10
  )
}
