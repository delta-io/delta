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

import org.apache.spark.sql.delta.{CatalogOwnedTableFeature, DeltaUnsupportedOperationException}
import org.apache.spark.sql.delta.DeltaOperations.{Delete, Write}
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.DeltaVacuumSuiteShims._
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile, Metadata, RemoveFile}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.VacuumCommand
import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedCommitCoordinatorProvider
import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTestBaseSuite
import org.apache.spark.sql.delta.coordinatedcommits.TrackingInMemoryCommitCoordinatorBuilder
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{DeltaCommitFileProvider, DeltaFileOperations, FileNames}
import org.apache.spark.sql.util.ScalaExtensions._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.scalatest.GivenWhenThen

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ManualClock

trait DeltaVacuumSuiteBase extends QueryTest
  with SharedSparkSession
  with GivenWhenThen
  with DeltaSQLTestUtils
  with DeletionVectorsTestUtils
  with DeltaTestUtilsForTempViews
  with CatalogOwnedTestBaseSuite {

  private def executeWithEnvironment(file: File)(f: (File, ManualClock) => Unit): Unit = {
    val clock = new ManualClock()
    withSQLConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
      f(file, clock)
    }
  }

  protected def isLiteVacuum: Boolean = false

  protected def testFullVacuumOnly(
      testName: String, testTags: org.scalatest.Tag*)(
      testFun: => Any): Unit = {
    // Certain tests are not valid for lite vacuum as lite vacuum doesn't care about
    // the files not tracked by the delta log.
    if (isLiteVacuum) {
      ignore(testName + " (full Vacuum only)", testTags: _*)(testFun)
    } else {
      test(testName, testTags: _*)(testFun)
    }
  }

  protected def withEnvironment(f: (File, ManualClock) => Unit): Unit =
    withTempDir(file => executeWithEnvironment(file)(f))

  protected def withEnvironment(prefix: String)(f: (File, ManualClock) => Unit): Unit =
    withTempDir(prefix)(file => executeWithEnvironment(file)(f))

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
  case class GCByInventory(dryRun: Boolean, expectedDf: Seq[String],
      retentionHours: Option[Double] = None,
      inventory: Option[DataFrame] = Option.empty[DataFrame]) extends Operation
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

  private final val RANDOM_FILE_CONTENT = "gibberish"

  protected def createFile(
      reservoirBase: String,
      filePath: String,
      file: File,
      clock: ManualClock,
      partitionValues: Map[String, String] = Map.empty): AddFile = {
    FileUtils.write(file, RANDOM_FILE_CONTENT)
    file.setLastModified(clock.getTimeMillis())
    createTestAddFile(
      encodedPath = filePath,
      partitionValues = partitionValues,
      modificationTime = clock.getTimeMillis())
  }

  protected def gcTest(table: DeltaTableV2, clock: ManualClock)(actions: Operation*): Unit = {
    import testImplicits._
    val basePath = table.deltaLog.dataPath.toString
    val fs = new Path(basePath).getFileSystem(table.deltaLog.newDeltaHadoopConf())
    actions.foreach {
      case CreateFile(path, commit, partitionValues) =>
        Given(s"*** Writing file to $path. Commit to log: $commit")
        val sanitizedPath = new Path(path).toUri.toString
        val file = new File(
          fs.makeQualified(DeltaFileOperations.absolutePath(basePath, sanitizedPath)).toUri)
        if (commit) {
          if (!DeltaTableUtils.isDeltaTable(spark, new Path(basePath))) {
            // initialize the table
            val version = table.startTransaction().commitManually()
            setCommitClock(table, version, clock)
          }
          val txn = table.startTransaction()
          val action = createFile(basePath, sanitizedPath, file, clock, partitionValues)
          val version = txn.commit(Seq(action), Write(SaveMode.Append))
          setCommitClock(table, version, clock)
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
        val txn = table.startTransaction()
        // scalastyle:off
        val metrics = Map[String, SQLMetric](
          "numRemovedFiles" -> createMetric(sparkContext, "number of files removed."),
          "numAddedFiles" -> createMetric(sparkContext, "number of files added."),
          "numDeletedRows" -> createMetric(sparkContext, "number of rows deleted."),
          "numCopiedRows" -> createMetric(sparkContext, "total number of rows.")
        )
        txn.registerSQLMetrics(spark, metrics)
        val encodedPath = new Path(path).toUri.toString
        val size = Some(RANDOM_FILE_CONTENT.length.toLong)
        val version = txn.commit(
          Seq(RemoveFile(encodedPath, Option(clock.getTimeMillis()), size = size)),
          Delete(Seq(Literal.TrueLiteral)))
        setCommitClock(table, version, clock)
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
        val result = VacuumCommand.gc(spark, table, dryRun, retention, clock = clock)
        val qualified = expectedDf.map(p => fs.makeQualified(new Path(p)).toString)
        checkDatasetUnorderly(result.as[String], qualified: _*)
      case GCByInventory(dryRun, expectedDf, retention, inventory) =>
        Given("*** Garbage collecting using inventory")
        val result =
          VacuumCommand.gc(spark, table, dryRun, retention, inventory, clock = clock)
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
        val e = intercept[Exception](gcTest(table, clock)(action))
        assert(e.getClass === failure)
        assert(
          msg.forall(m =>
            e.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT))),
          e.getMessage + "didn't contain: " + msg.mkString("[", ", ", "]"))
    }
  }

  protected def vacuumSQLTest(table: DeltaTableV2, tableName: String) {
    val committedFile = "committedFile.txt"
    val notCommittedFile = "notCommittedFile.txt"

    val expectedDf = Option.when(!isLiteVacuum)(new Path(table.path, notCommittedFile).toString)
    gcTest(table, new ManualClock())(
      // Prepare the table with files with timestamp of epoch-time 0 (i.e. 01-01-1970 00:00)
      CreateFile(committedFile, commitToActionLog = true),
      CreateFile(notCommittedFile, commitToActionLog = false),
      CheckFiles(Seq(committedFile, notCommittedFile)),

      // Dry run should return the not committed file and but not delete files
      ExecuteVacuumInSQL(tableName, expectedDf = expectedDf.toSeq, dryRun = true),
      CheckFiles(Seq(committedFile, notCommittedFile)),

      // Actual run should not delete the committed file but delete the not-committed file
      ExecuteVacuumInSQL(tableName, Seq(table.path.toString)),
      CheckFiles(Seq(committedFile)),
      // File ts older than default retention
      // However, non committed files are not deleted by lite vacuum.
      CheckFiles(Seq(notCommittedFile), exist = isLiteVacuum),

      // Logically delete the file.
      LogicallyDeleteFile(committedFile),
      CheckFiles(Seq(committedFile)),

      // Vacuum with 0 retention should actually delete the file.
      ExecuteVacuumInSQL(tableName, Seq(table.path.toString), Some(0)),
      CheckFiles(Seq(committedFile), exist = false))
  }

  protected def vacuumScalaTest(deltaTable: io.delta.tables.DeltaTable, tablePath: String) {
    val table = DeltaTableV2(spark, new Path(tablePath), options = Map.empty, "test")
    val committedFile = "committedFile.txt"
    val notCommittedFile = "notCommittedFile.txt"

    gcTest(table, new ManualClock())(
      // Prepare the table with files with timestamp of epoch-time 0 (i.e. 01-01-1970 00:00)
      CreateFile(committedFile, commitToActionLog = true),
      CreateFile(notCommittedFile, commitToActionLog = false),
      CheckFiles(Seq(committedFile, notCommittedFile)),

      // Actual run should delete the not committed file and but not delete files
      ExecuteVacuumInScala(deltaTable, Seq()),
      CheckFiles(Seq(committedFile)),
      // File ts older than default retention
      // However, non committed files are not deleted by lite vacuum.
      CheckFiles(Seq(notCommittedFile), exist = isLiteVacuum),

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

  protected def deleteCommitFile(table: DeltaTableV2, version: Long) = {
    new File(DeltaCommitFileProvider(table.update()).deltaFile(version).toUri).delete()
  }

  /**
   * Helper method to get all of the [[AddCDCFile]]s that exist in the delta table
   */
  protected def getCDCFiles(deltaLog: DeltaLog): Seq[AddCDCFile] = {
    val changes = deltaLog.getChanges(
      startVersion = 0, catalogTableOpt = None, failOnDataLoss = true)
    changes.flatMap(_._2).collect { case a: AddCDCFile => a }.toList
  }

  protected def setCommitClock(table: DeltaTableV2, version: Long, clock: ManualClock) = {
    val f = new File(DeltaCommitFileProvider(table.update()).deltaFile(version).toUri)
    f.setLastModified(clock.getTimeMillis())
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

class DeltaVacuumSuite extends DeltaVacuumSuiteBase with DeltaSQLCommandTest {
  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.sources.parallelPartitionDiscovery.parallelism", "2")
  }

  testQuietly("basic case - SQL command on path-based tables with direct 'path'") {
    withEnvironment { (tempDir, _) =>
      val table = DeltaTableV2(spark, tempDir)
      vacuumSQLTest(table, tableName = s"'$tempDir'")
    }
  }

  testQuietly("basic case - SQL command on path-based table with delta.`path`") {
    withEnvironment { (tempDir, _) =>
      val table = DeltaTableV2(spark, tempDir)
      vacuumSQLTest(table, tableName = s"delta.`$tempDir`")
    }
  }

  testQuietly("basic case - SQL command on name-based table") {
    val tableName = "deltaTable"
    withEnvironment { (_, _) =>
      withTable(tableName) {
        import testImplicits._
        spark.emptyDataset[Int].write.format("delta").saveAsTable(tableName)
        val table = DeltaTableV2(spark, new TableIdentifier(tableName))
        vacuumSQLTest(table, tableName)
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
        val table = DeltaTableV2(spark, new TableIdentifier(tableName))
        val e = intercept[AnalysisException] {
          vacuumSQLTest(table, viewName)
        }
        assert(e.getMessage.contains(SQL_COMMAND_ON_TEMP_VIEW_NOT_SUPPORTED_ERROR_MSG))
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
      val table = DeltaTableV2(spark, tempDir, clock)
      gcTest(table, clock)(
        CreateFile("file1.txt", commitToActionLog = false),
        CreateDirectory("abc"),
        ExpectFailure(
          GC(dryRun = false, Nil), classOf[IllegalArgumentException], Seq("no state defined"))
      )
    }
  }

  test("invisible files and dirs") {
    withEnvironment { (tempDir, clock) =>
      val table = DeltaTableV2(spark, tempDir, clock)
      gcTest(table, clock)(
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
      val table = DeltaTableV2(spark, tempDir, clock)
      val txn = table.startTransaction()
      val schema = new StructType().add("_underscore_col_", IntegerType).add("n", IntegerType)
      val metadata =
        Metadata(schemaString = schema.json, partitionColumns = Seq("_underscore_col_"))
      val version = txn.commitActions(
        DeltaOperations.CreateTable(metadata, isManaged = true), metadata)
      setCommitClock(table, version, clock)
      gcTest(table, clock)(
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

  test("schema validation for vacuum by using inventory dataframe") {
    withEnvironment { (tempDir, clock) =>
      val table = DeltaTableV2(spark, tempDir, clock)
      val txn = table.startTransaction()
      val schema = new StructType().add("_underscore_col_", IntegerType).add("n", IntegerType)
      val metadata =
        Metadata(schemaString = schema.json, partitionColumns = Seq("_underscore_col_"))
      val version = txn.commitActions(
        DeltaOperations.CreateTable(metadata, isManaged = true), metadata)
      setCommitClock(table, version, clock)
      val inventorySchema = StructType(
        Seq(
          StructField("file", StringType),
          StructField("size", LongType),
          StructField("isDir", BooleanType),
          StructField("modificationTime", LongType)
        ))
      val inventory = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq.empty[Row]), inventorySchema)
      gcTest(table, clock)(
        ExpectFailure(
          GCByInventory(dryRun = false, expectedDf = Seq(tempDir), inventory = Some(inventory)),
          classOf[DeltaAnalysisException],
          Seq( "The schema for the specified INVENTORY",
            "does not contain all of the required fields.",
            "Required fields are:",
            s"${VacuumCommand.INVENTORY_SCHEMA.treeString}")
        )
      )
    }
  }

  test("run vacuum by using inventory dataframe") {
    withEnvironment { (tempDir, clock) =>
      val table = DeltaTableV2(spark, tempDir, clock)
      val txn = table.startTransaction()
      val schema = new StructType().add("_underscore_col_", IntegerType).add("n", IntegerType)

      // Vacuum should consider partition folders even for clean up even though it starts with `_`
      val metadata =
        Metadata(schemaString = schema.json, partitionColumns = Seq("_underscore_col_"))
      val version = txn.commitActions(
        DeltaOperations.CreateTable(metadata, isManaged = true), metadata)
      setCommitClock(table, version, clock)
      val dataPath = table.deltaLog.dataPath
      // Create a Seq of Rows containing the data
      val data = Seq(
        Row(s"${dataPath}", 300000L, true, 0L),
        Row(s"${dataPath}/file1.txt", 300000L, false, 0L),
        Row(s"${dataPath}/file2.txt", 300000L, false, 0L),
        Row(s"${dataPath}/_underscore_col_=10/test.txt", 300000L, false, 0L),
        Row(s"${dataPath}/_underscore_col_=10/test2.txt", 300000L, false, 0L),
        // Below file is not within Delta table path and should be ignored by vacuum
        Row(s"/tmp/random/_underscore_col_=10/test2.txt", 300000L, false, 0L),
        // Below are Delta table root location and vacuum must safely handle them
        Row(s"${dataPath}", 300000L, true, 0L)
      )
      val inventory = spark.createDataFrame(spark.sparkContext.parallelize(data),
        VacuumCommand.INVENTORY_SCHEMA)
      gcTest(table, clock)(
        CreateFile("file1.txt", commitToActionLog = true, Map("_underscore_col_" -> "10")),
        CreateFile("file2.txt", commitToActionLog = false, Map("_underscore_col_" -> "10")),
        CreateFile("_underscore_col_=10/test.txt", true, Map("_underscore_col_" -> "10")),
        CreateFile("_underscore_col_=10/test2.txt", false, Map("_underscore_col_" -> "10")),
        CheckFiles(Seq("file1.txt", "_underscore_col_=10", "file2.txt")),
        LogicallyDeleteFile("_underscore_col_=10/test.txt"),
        AdvanceClock(defaultTombstoneInterval + 1000),
        GCByInventory(dryRun = true, expectedDf = Seq(
          s"${dataPath}/file2.txt",
          s"${dataPath}/_underscore_col_=10/test.txt",
          s"${dataPath}/_underscore_col_=10/test2.txt"
        ), inventory = Some(inventory)),
        GCByInventory(dryRun = false, expectedDf = Seq(tempDir), inventory = Some(inventory)),
        CheckFiles(Seq("file1.txt")),
        CheckFiles(Seq("file2.txt", "_underscore_col_=10/test.txt",
          "_underscore_col_=10/test2.txt"), exist = false)
      )
    }
  }

  test("vacuum using inventory delta table and should not touch hidden files") {
    withSQLConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
      withEnvironment { (tempDir, clock) =>
        import testImplicits._
        val path = s"""${tempDir.getCanonicalPath}_data"""
        val inventoryPath = s"""${tempDir.getCanonicalPath}_inventory"""

        // Define test delta table
        val data = Seq(
          (10, 1, "a"),
          (10, 2, "a"),
          (10, 3, "a"),
          (10, 4, "a"),
          (10, 5, "a")
        )
        data.toDF("v1", "v2", "v3")
          .write
          .partitionBy("v1", "v2")
          .format("delta")
          .save(path)
        val table = DeltaTableV2(spark, new File(path), clock)
        val dataPath = table.deltaLog.dataPath
        val reservoirData = Seq(
          Row(s"${dataPath}/file1.txt", 300000L, false, 0L),
          Row(s"${dataPath}/file2.txt", 300000L, false, 0L),
          Row(s"${dataPath}/_underscore_col_=10/test.txt", 300000L, false, 0L),
          Row(s"${dataPath}/_underscore_col_=10/test2.txt", 300000L, false, 0L)
        )
        spark.createDataFrame(
          spark.sparkContext.parallelize(reservoirData), VacuumCommand.INVENTORY_SCHEMA)
          .write
          .format("delta")
          .save(inventoryPath)
        gcTest(table, clock)(
          CreateFile("file1.txt", commitToActionLog = false),
          CreateFile("file2.txt", commitToActionLog = false),
          // Delta marks dirs starting with `_` as hidden unless specified as partition folder
          CreateFile("_underscore_col_=10/test.txt", false),
          CreateFile("_underscore_col_=10/test2.txt", false),
          AdvanceClock(defaultTombstoneInterval + 1000)
        )
        sql(s"vacuum delta.`$path` using inventory delta.`$inventoryPath` retain 0 hours")
        gcTest(table, clock)(
          CheckFiles(Seq("file1.txt", "file2.txt"), exist = false),
          // hidden files must not be dropped
          CheckFiles(Seq("_underscore_col_=10/test.txt", "_underscore_col_=10/test2.txt"))
        )
      }
    }
  }

  test("vacuum using inventory query and should not touch hidden files") {
    withSQLConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
      withEnvironment { (tempDir, clock) =>
        import testImplicits._
        val path = s"""${tempDir.getCanonicalPath}_data"""
        val reservoirPath = s"""${tempDir.getCanonicalPath}_reservoir"""

        // Define test delta table
        val data = Seq(
          (10, 1, "a"),
          (10, 2, "a"),
          (10, 3, "a"),
          (10, 4, "a"),
          (10, 5, "a")
        )
        data.toDF("v1", "v2", "v3")
          .write
          .partitionBy("v1", "v2")
          .format("delta")
          .save(path)
        val table = DeltaTableV2(spark, new File(path), clock)
        val dataPath = table.deltaLog.dataPath
        val reservoirData = Seq(
          Row(s"${dataPath}/file1.txt", 300000L, false, 0L),
          Row(s"${dataPath}/file2.txt", 300000L, false, 0L),
          Row(s"${dataPath}/_underscore_col_=10/test.txt", 300000L, false, 0L),
          Row(s"${dataPath}/_underscore_col_=10/test2.txt", 300000L, false, 0L)
        )
        spark.createDataFrame(
          spark.sparkContext.parallelize(reservoirData), VacuumCommand.INVENTORY_SCHEMA)
          .write
          .format("delta")
          .save(reservoirPath)
        gcTest(table, clock)(
          CreateFile("file1.txt", commitToActionLog = false),
          CreateFile("file2.txt", commitToActionLog = false),
          // Delta marks dirs starting with `_` as hidden unless specified as partition folder
          CreateFile("_underscore_col_=10/test.txt", false),
          CreateFile("_underscore_col_=10/test2.txt", false)
        )
        sql(s"""vacuum delta.`$path`
             |using inventory (select * from delta.`$reservoirPath`)
             |retain 0 hours""".stripMargin)
        gcTest(table, clock)(
          AdvanceClock(defaultTombstoneInterval + 1000),
          CheckFiles(Seq("file1.txt", "file2.txt"), exist = false),
          // hidden files must not be dropped
          CheckFiles(Seq("_underscore_col_=10/test.txt", "_underscore_col_=10/test2.txt"))
        )
      }
    }
  }

  // Since lite vacuum uses delta log, it doesn't delete empty directories.
  testFullVacuumOnly("multiple levels of empty directory deletion") {
    withEnvironment { (tempDir, clock) =>
      val table = DeltaTableV2(spark, tempDir, clock)
      gcTest(table, clock)(
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
      val table = DeltaTableV2(spark, tempDir, clock)
      gcTest(table, clock)(
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
      val table = DeltaTableV2(spark, reservoirDir, clock)

      val externalFile = new File(externalDir, "file4.txt").getAbsolutePath

      gcTest(table, clock)(
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
        // Since file3.txt is not committed, it's not tracked by lite vacuum.
        GC(dryRun = true,
          Option.when(!isLiteVacuum)(new File(reservoirDir, "file3.txt").toString).toSeq),
        // nothing should be deleted
        CheckFiles(Seq("file1.txt", "abc", "abc/file2.txt", "file3.txt")),
        GC(dryRun = false, Seq(reservoirDir.toString)),
        CheckFiles(Seq("file1.txt", "abc", "abc/file2.txt")),
        // Since file3.txt is not committed, it would be deleted only if it's non-lite-vacuum
        CheckFiles(Seq("file3.txt"), exist = isLiteVacuum),

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
        // Second gc should clear empty directory if it's not lite vacuum
        GC(dryRun = false, Seq(reservoirDir.toString)),
        CheckFiles(Seq("file1.txt")),
        CheckFiles(Seq("abc"), exist = isLiteVacuum),

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
      val table = DeltaTableV2(spark, tempDir, clock)
      withSQLConf("spark.databricks.delta.vacuum.parallelDelete.enabled" -> "true") {
        gcTest(table, clock)(
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
    withSQLConf("spark.databricks.delta.vacuum.retentionWindowIgnore.enabled" -> "false") {
      withEnvironment { (tempDir, clock) =>
        val table = DeltaTableV2(spark, tempDir, clock)
        gcTest(table, clock)(
          CreateFile("file1.txt", commitToActionLog = true),
          CheckFiles(Seq("file1.txt")),
          ExpectFailure(
            GC(false, Seq(tempDir), Some(-2)),
            classOf[IllegalArgumentException],
            Seq("Retention", "less than", "0"))
        )
        val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tempDir.getAbsolutePath)
        gcTest(table, clock)(
          CreateFile("file2.txt", commitToActionLog = true),
          CheckFiles(Seq("file2.txt")),
          ExpectFailure(
            ExecuteVacuumInScala(deltaTable, Seq(), Some(-2)),
            classOf[IllegalArgumentException],
            Seq("Retention", "less than", "0"))
        )
      }
    }
  }

  test("deleting directories") {
    withEnvironment { (tempDir, clock) =>
      val table = DeltaTableV2(spark, tempDir, clock)
      gcTest(table, clock)(
        CreateFile("abc/def/file1.txt", commitToActionLog = true),
        CreateFile("abc/def/file2.txt", commitToActionLog = true),
        CreateDirectory("ghi"),
        CheckFiles(Seq("abc", "abc/def", "ghi")),
        // Since "ghi" is a empty directory not tracked by the delta log,
        // lite Vacuum won't delete it.
        GC(dryRun = true, Option.when(!isLiteVacuum)(new File(tempDir, "ghi").toString).toSeq),
        GC(dryRun = false, Seq(tempDir)),
        CheckFiles(Seq("abc", "abc/def")),
        CheckFiles(Seq("ghi"), exist = isLiteVacuum)
      )
    }
  }

  test("deleting files with special characters in path") {
    withEnvironment { (tempDir, clock) =>
      val table = DeltaTableV2(spark, tempDir, clock)
      // Non committed files are not deleted by lite vacuum.
      val expected = Option.when(!isLiteVacuum)(new File(tempDir, "abc def/#1/file2.txt").toString)
      gcTest(table, clock)(
        CreateFile("abc def/#1/file1.txt", commitToActionLog = true),
        CreateFile("abc def/#1/file2.txt", commitToActionLog = false),
        CheckFiles(Seq("abc def", "abc def/#1")),
        AdvanceClock(defaultTombstoneInterval + 1000),
        GC(dryRun = true, expected.toSeq),
        GC(dryRun = false, Seq(tempDir)),
        CheckFiles(Seq("abc def/#1", "abc def/#1/file1.txt")),
        CheckFiles(Seq("abc def/#1/file2.txt"), exist = isLiteVacuum)
      )
    }
  }

  testQuietly("additional retention duration check with vacuum command") {
    withEnvironment { (tempDir, clock) =>
      val table = DeltaTableV2(spark, tempDir, clock)
      withSQLConf("spark.databricks.delta.retentionDurationCheck.enabled" -> "true") {
        gcTest(table, clock)(
          CreateFile("file1.txt", commitToActionLog = true),
          CheckFiles(Seq("file1.txt")),
          ExpectFailure(
            GC(false, Nil, Some(0)),
            classOf[IllegalArgumentException],
            Seq("spark.databricks.delta.retentionDurationCheck.enabled = false", "168 hours"))
        )
      }

      gcTest(table, clock)(
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
      assert(
        ex.getMessage.contains(
          s"`$path/v2=a` is not a Delta table. VACUUM is only supported for Delta tables."))
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


  // Since lite vacuum uses delta log, it doesn't delete uniform metadata directories
  // as they are not reachable through delta log.
  testFullVacuumOnly("gc metadata dir when uniform disabled") {
    withEnvironment { (tempDir, clock) =>
      spark.emptyDataset[Int].write.format("delta").save(tempDir)
      val table = DeltaTableV2(spark, tempDir, clock)
      gcTest(table, clock)(
        CreateDirectory("metadata"),
        CreateFile("metadata/file1.json", false),

        AdvanceClock(defaultTombstoneInterval + 1000),
        GC(dryRun = false, Seq(tempDir)),
        CheckFiles(Seq("metadata/file1.json"), exist = false),
        GC(dryRun = false, Seq(tempDir)), // Second GC clears empty dir
        CheckFiles(Seq("metadata"), exist = false)
      )
    }
  }

  test("hudi metadata dir") {
    withEnvironment { (tempDir, clock) =>
      spark.emptyDataset[Int].write.format("delta").save(tempDir)
      val table = DeltaTableV2(spark, tempDir, clock)
      gcTest(table, clock)(
        CreateDirectory(".hoodie"),
        CreateFile(".hoodie/00001.commit", false),

        AdvanceClock(defaultTombstoneInterval + 1000),
        GC(dryRun = false, Seq(tempDir)),
        CheckFiles(Seq(".hoodie", ".hoodie/00001.commit"))
      )
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
    import org.apache.spark.sql.delta.test.DeltaTestImplicits.DeltaTableV2ObjectTestHelper
    val tableName = "testTable"
    withDeletionVectorsEnabled() {
      withSQLConf(
          DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false",
          // Disable the following check since the test relies on time travel beyond
          // deletedFileRetentionDuration.
          DeltaSQLConf.ENFORCE_TIME_TRAVEL_WITHIN_DELETED_FILE_RETENTION_DURATION.key -> "false") {
        withTable(tableName) {
          // Create Delta table with 5 files of 10 rows.
          spark.range(0, 50, step = 1, numPartitions = 5)
            .write
            .format("delta")
            .option("delta.deletedFileRetentionDuration", "interval 1 hours")
            .saveAsTable(tableName)
          // The following is done to ensure deltaLog object uses the same clock that Vacuum
          // logic uses.
          val deltaLogThrowaway = DeltaLog.forTable(spark, TableIdentifier(tableName))
          val tablePath = deltaLogThrowaway.dataPath
          DeltaLog.clearCache()
          val clock = new ManualClock(System.currentTimeMillis())
          val deltaLog = DeltaLog.forTable(spark, tablePath, clock)
          val deltaTable = DeltaTableV2(spark, TableIdentifier(tableName))
          assertNumFiles(deltaLog, addFiles = 5, addFilesWithDVs = 0, dvFiles = 0, dataFiles = 5)

          // Delete 1 row from each file. DVs will be packed to one DV file.
          val deletedRows1 = Seq(0, 10, 20, 30, 40)
          val deletedRowsStr1 = deletedRows1.mkString("(", ",", ")")
          spark.sql(s"DELETE FROM $tableName WHERE id IN $deletedRowsStr1")
          val snapshotV1 = deltaTable.update()
          // We retrieve both timestamp and file modification time b/c when ICT is enabled,
          // timestamp represents ICT instead of file modification time. Lite vacuum relies on
          // both the ICT and the file modification time to determine cleanup behavior.
          val timestampV1 = snapshotV1.timestamp
          val fileModificationTimeV1 = snapshotV1.logSegment.lastCommitFileModificationTimestamp
          assertNumFiles(deltaLog, addFiles = 5, addFilesWithDVs = 5, dvFiles = 1, dataFiles = 5)

          // Delete all rows from the first file. An ephemeral DV will still be created.
          // We need to add 1000 ms for local filesystems that only write modificationTimes to the
          // second precision.
          Thread.sleep(1000) // Ensure it's been at least 1000 ms since V1
          // Assign clock to the current system time so that the ICT falls within
          // (fileModificationTimeV(X-1) + 1000, fileModificationTimeV(X+1)]. This ensures we can
          // later manually adjust the clock time to test both full and lite vacuum behavior.
          clock.setTime(System.currentTimeMillis())
          spark.sql(s"DELETE FROM $tableName WHERE id < 10")
          val snapshotV2 = deltaTable.update()
          val timestampV2 = snapshotV2.timestamp
          val fileModificationTimeV2 = snapshotV2.logSegment.lastCommitFileModificationTimestamp
          assertNumFiles(deltaLog, addFiles = 4, addFilesWithDVs = 4, dvFiles = 2, dataFiles = 5)
          val expectedAnswerV2 = Seq.range(0, 50).filterNot(deletedRows1.contains).filterNot(_ < 10)

          // Delete 1 more row from each file.
          Thread.sleep(1000) // Ensure it's been at least 1000 ms since V2
          clock.setTime(System.currentTimeMillis())
          val deletedRows2 = Seq(11, 21, 31, 41)
          val deletedRowsStr2 = deletedRows2.mkString("(", ",", ")")
          spark.sql(s"DELETE FROM $tableName WHERE id IN $deletedRowsStr2")
          val snapshotV3 = deltaTable.update()
          val timestampV3 = snapshotV3.timestamp
          val fileModificationTimeV3 = snapshotV3.logSegment.lastCommitFileModificationTimestamp
          assertNumFiles(deltaLog, addFiles = 4, addFilesWithDVs = 4, dvFiles = 3, dataFiles = 5)
          val expectedAnswerV3 = expectedAnswerV2.filterNot(deletedRows2.contains)

          // Delete DVs by rewriting the data files with DVs.
          Thread.sleep(1000) // Ensure it's been at least 1000 ms since V3
          clock.setTime(System.currentTimeMillis())
          purgeDVs(tableName)

          val numFilesAfterPurge = 4
          val snapshotV4 = deltaTable.update()
          val timestampV4 = snapshotV4.timestamp
          val fileModificationTimeV4 = snapshotV4.logSegment.lastCommitFileModificationTimestamp
          assertNumFiles(deltaLog, addFiles = numFilesAfterPurge, addFilesWithDVs = 0, dvFiles = 3,
            dataFiles = 9)

          // Run VACUUM with nothing expired. It should not delete anything.
          clock.setTime(System.currentTimeMillis())
          VacuumCommand.gc(
            spark, deltaTable, retentionHours = Some(1), clock = clock, dryRun = false)
          assertNumFiles(deltaLog, addFiles = numFilesAfterPurge, addFilesWithDVs = 0, dvFiles = 3,
            dataFiles = 9)

          val oneHour = TimeUnit.HOURS.toMillis(1)
          // Run VACUUM @ V1.
          // The clock time must be set such that: (X is the version where we run VACUUM)
          // 1. (clock time - retention time) falls within
          //    (fileModificationTimeV(X), fileModificationTimeV(X+1)] for both lite and full
          //    vacuum, since both use file modification time to determine if the files are valid
          //    for cleanup.
          // 2. (clock time - retention time) falls within [timestampV(X), timestampV(X+1)) for
          //    lite vacuum, since it uses [[DeltaHistoryManager(deltaLog).getActiveCommitAtTime]]
          //    which depends on timestamp to capture files of commit-X as candidates for cleanup.
          clock.setTime(Math.max(fileModificationTimeV1 + 1, timestampV1) + oneHour)
          VacuumCommand.gc(
            spark, deltaTable, retentionHours = Some(1), clock = clock, dryRun = false)
          assertNumFiles(deltaLog, addFiles = numFilesAfterPurge, addFilesWithDVs = 0, dvFiles = 3,
            dataFiles = 9)

          // Run VACUUM @ V2. It should delete the ephemeral DV and the removed Parquet file.
          // Since ephemeral DV is not GC'ed by Lite Vacuum, the number of DVs we expect will be
          // one more in case of lite Vacuum
          val numDVstoAdd = if (isLiteVacuum) 1 else 0
          clock.setTime(Math.max(fileModificationTimeV2 + 1, timestampV2) + oneHour)
          VacuumCommand.gc(
            spark, deltaTable, retentionHours = Some(1), clock = clock, dryRun = false)
          assertNumFiles(deltaLog, addFiles = numFilesAfterPurge, addFilesWithDVs = 0,
            dvFiles = 2 + numDVstoAdd, dataFiles = 8)
          checkAnswer(
            spark.sql(s"SELECT * FROM $tableName VERSION AS OF 2"), expectedAnswerV2.toDF)

          // Run VACUUM @ V3. It should delete the persistent DVs from V1.
          clock.setTime(Math.max(fileModificationTimeV3 + 1, timestampV3) + oneHour)
          VacuumCommand.gc(
            spark, deltaTable, retentionHours = Some(1), clock = clock, dryRun = false)
          assertNumFiles(deltaLog, addFiles = numFilesAfterPurge, addFilesWithDVs = 0,
            dvFiles = 1 + numDVstoAdd, dataFiles = 8)
          checkAnswer(
            spark.sql(s"SELECT * FROM $tableName VERSION AS OF 3"), expectedAnswerV3.toDF)

          // Run VACUUM @ V4. It should delete the Parquet files and DVs of V3.
          clock.setTime(Math.max(fileModificationTimeV4 + 1, timestampV4) + oneHour)
          VacuumCommand.gc(
            spark, deltaTable, retentionHours = Some(1), clock = clock, dryRun = false)
          assertNumFiles(deltaLog, addFiles = numFilesAfterPurge, addFilesWithDVs = 0,
            dvFiles = 0 + numDVstoAdd, dataFiles = 4)
          checkAnswer(
            spark.sql(s"SELECT * FROM $tableName VERSION AS OF 4"), expectedAnswerV3.toDF)

          // Run VACUUM with zero retention period. It should not delete anything.
          clock.setTime(Math.max(fileModificationTimeV4 + 1, timestampV4) + oneHour)
          VacuumCommand.gc(
            spark, deltaTable, retentionHours = Some(0), clock = clock, dryRun = false)
          assertNumFiles(deltaLog, addFiles = numFilesAfterPurge, addFilesWithDVs = 0,
            dvFiles = 0 + numDVstoAdd, dataFiles = 4)

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
            // with lite vacuum ephemeral dvs are not going to be GC'ed. So, the dvs we expect
            // will be 5 more for lite vacuum
            val dvsToAdd = if (isLiteVacuum) 5 else 0
            targetTable().delete("id < 50")

            assert(listDeletionVectors(targetLog).size == 15)
            targetTable().vacuum(10)
            assert(listDeletionVectors(targetLog).size == 15)
            targetTable().vacuum(0)
            assert(listDeletionVectors(targetLog).size == 5 + dvsToAdd)
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
        assert(e.getMessage.contains("is not a Delta table."))
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
          clock.setTime(System.currentTimeMillis())
          spark
            .range(2)
            .write
            .format("delta")
            .option("delta.deletedFileRetentionDuration", s"interval $retentionHours hours")
            .save(dir.getAbsolutePath)
          // The following is done to ensure deltaLog object uses the same clock that Vacuum
          // logic uses.
          DeltaLog.clearCache()
          DeltaLog.forTable(spark, dir, clock)
          val table = DeltaTableV2(spark, dir, clock)

          setCommitClock(table, 0L, clock)
          val expectedReturn = if (isDryRun) {
            // dry run returns files that will be deleted
            Seq(new Path(dir.getAbsolutePath, "file1.txt").toString)
          } else {
            Seq(dir.getAbsolutePath)
          }

          gcTest(table, clock)(
            CreateFile("file1.txt", commitToActionLog = true),
            CreateFile("file2.txt", commitToActionLog = true),
            LogicallyDeleteFile("file1.txt"),
            AdvanceClock(timeGapHours * 1000 * 60 * 60),
            GC(dryRun = isDryRun, expectedReturn, Some(retentionHours))
          )
          val deltaTable = io.delta.tables.DeltaTable.forPath(table.deltaLog.dataPath.toString)
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

            if (retentionHours == 0) {
              assert(
                operationParamsBegin("specifiedRetentionMillis") ===
                  (retentionHours * 60 * 60 * 1000).toString)
            }
            assert(
              operationParamsBegin("defaultRetentionMillis") ===
                DeltaLog.tombstoneRetentionMillis(table.initialSnapshot.metadata).toString)

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
    retentionHours = 0,
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

  test(s"vacuum sql syntax checks") {
    val tableName = "testTable"
    withTable(tableName) {
      withDeletionVectorsEnabled() {
        withSQLConf(
          DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false",
          DeltaSQLConf.LITE_VACUUM_ENABLED.key -> "false"
        ) {
          spark.range(0, 50, step = 1, numPartitions = 5).write.format("delta")
            .saveAsTable(tableName)
          var e = intercept[AnalysisException] {
            spark.sql(s"Vacuum $tableName DRY RUN DRY RUN")
          }
          assert(e.getMessage.contains("Found duplicate clauses: DRY RUN"))

          e = intercept[AnalysisException] {
            spark.sql(s"Vacuum $tableName RETAIN 200 HOURS RETAIN 200 HOURS")
          }
          assert(e.getMessage.contains("Found duplicate clauses: RETAIN"))

          e = intercept[AnalysisException] {
            spark.sql(s"Vacuum $tableName FULL LITE")
          }
          assert(e.getMessage.contains("Found duplicate clauses: LITE/FULL"))

          e = intercept[AnalysisException] {
            spark.sql(s"Vacuum $tableName USING INVENTORY $tableName INVENTORY $tableName")
          }
          assert(e.getMessage.contains("Syntax error at or near"))

          e = intercept[AnalysisException] {
            spark.sql(s"Vacuum $tableName USING INVENTORY $tableName LITE")
          }
          assert(e.getMessage.contains("Inventory option is not compatible with LITE"))

          // create an uncommitted file. Presence or lack of this file will help us
          // validate that we ran the right type of Vacuum.
          val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
          val basePath = deltaLog.dataPath.toString
          val clock = new ManualClock()
          val fs = new Path(basePath).getFileSystem(deltaLog.newDeltaHadoopConf())
          val sanitizedPath = new Path("UnCommittedFile.parquet").toUri.toString
          val file = new File(
            fs.makeQualified(DeltaFileOperations.absolutePath(basePath, sanitizedPath)).toUri)
          createFile(basePath, sanitizedPath, file, clock)

          spark.sql(s"DELETE from $tableName WHERE ID % 2 = 0 and ID < 40")
          assertNumFiles(deltaLog, addFiles = 5, addFilesWithDVs = 4, dvFiles = 1, dataFiles = 6)
          purgeDVs(tableName)

          assertNumFiles(deltaLog, addFiles = 5, addFilesWithDVs = 0, dvFiles = 1,
            dataFiles = 10) // 9 file actions + one  uncommitted file

          spark.sql(s"Vacuum $tableName LITE DRY RUN RETAIN 0 HOURS")
          // DRY RUN option doesn't change anything.
          assertNumFiles(deltaLog, addFiles = 5, addFilesWithDVs = 0, dvFiles = 1,
            dataFiles = 10)

          // LITE will be able to GC 4 files removed by DELETE.
          spark.sql(s"Vacuum $tableName LITE RETAIN 0 HOURS")
          assertNumFiles(deltaLog, addFiles = 5, addFilesWithDVs = 0, dvFiles = 0,
            dataFiles = 6)

          // Default is full and it's able to delete the 'notCommittedFile.parquet'
          spark.sql(s"Vacuum $tableName RETAIN 0 HOURS")
          assertNumFiles(deltaLog, addFiles = 5, addFilesWithDVs = 0, dvFiles = 0,
            dataFiles = 5)
          // Create the uncommittedFile file again to make sure explicit vacuum full works as
          // expected.
          createFile(basePath, sanitizedPath, file, clock)
          assertNumFiles(deltaLog, addFiles = 5, addFilesWithDVs = 0, dvFiles = 0,
            dataFiles = 6)
          spark.sql(s"Vacuum $tableName FULL RETAIN 0 HOURS")
          assertNumFiles(deltaLog, addFiles = 5, addFilesWithDVs = 0, dvFiles = 0,
            dataFiles = 5)
        }
      }
    }
  }

  test("running vacuum on a catalog owned managed table should fail") {
    CatalogOwnedCommitCoordinatorProvider.clearBuilders()
    CatalogOwnedCommitCoordinatorProvider.registerBuilder(
      "spark_catalog", TrackingInMemoryCommitCoordinatorBuilder(batchSize = 3))
    withTable("t1") {
      spark.sql(s"CREATE TABLE t1 (id INT) USING delta TBLPROPERTIES " +
        s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")
      checkError(
        intercept[DeltaUnsupportedOperationException] {
          spark.sql(s"VACUUM t1")
        },
        "DELTA_UNSUPPORTED_VACUUM_ON_MANAGED_TABLE"
      )
      checkError(
        intercept[DeltaUnsupportedOperationException] {
          spark.sql(s"VACUUM t1 DRY RUN")
        },
        "DELTA_UNSUPPORTED_VACUUM_ON_MANAGED_TABLE"
      )
    }
    CatalogOwnedCommitCoordinatorProvider.clearBuilders()
  }
}

class DeltaLiteVacuumSuite
  extends DeltaVacuumSuite {
  override def isLiteVacuum: Boolean = true

  private var oldValue: Boolean = false

  override def beforeAll(): Unit = {
    super.beforeAll()
    oldValue = spark.conf.get(DeltaSQLConf.LITE_VACUUM_ENABLED)
    spark.conf.set(DeltaSQLConf.LITE_VACUUM_ENABLED.key, "true")
  }

  override def afterAll(): Unit = {
    spark.conf.set(DeltaSQLConf.LITE_VACUUM_ENABLED.key, oldValue)
    super.afterAll()
  }

  test("lite vacuum not possible - commit 0 is missing") {
    withSQLConf(
      DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false"
    ) {
      withTempDir { dir =>
        // create table versions 0 and 1
        spark.range(10)
          .write
          .format("delta")
          .save(dir.getAbsolutePath)
        spark.range(10)
          .write
          .format("delta")
          .mode("append")
          .save(dir.getAbsolutePath)
        val deltaTable = io.delta.tables.DeltaTable.forPath(dir.getAbsolutePath)
        val table = DeltaTableV2(spark, new Path(dir.getAbsolutePath))
        deltaTable.delete()
        // Checkpoints will allow us to construct the table snapshot
        table.deltaLog.createCheckpointAtVersion(2L)
        deleteCommitFile(table, 0L) // delete version 0

        val e = intercept[DeltaIllegalStateException] {
          VacuumCommand.gc(spark, table, dryRun = true, retentionHours = Some(0))
        }
        assert(e.getMessage.contains("VACUUM LITE cannot delete all eligible files as some files" +
          " are not referenced by the Delta log. Please run VACUUM FULL."))
      }
    }
  }

  test("lite vacuum not possible - commits since last vacuum is missing") {
    withSQLConf(
      DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false"
    ) {
      withTempDir { dir =>
        // create table - version 0
        spark.range(10)
          .write
          .format("delta")
          .save(dir.getAbsolutePath)
        val deltaTable = io.delta.tables.DeltaTable.forPath(dir.getAbsolutePath)
        val table = DeltaTableV2(spark, new Path(dir.getAbsolutePath))
        deltaTable.delete() // version 1
        // The following Vacuum saves latestCommitVersionOutsideOfRetentionWindow as 1
        VacuumCommand.gc(spark, table, dryRun = false, retentionHours = Some(0))
        spark.range(10)
          .write
          .format("delta")
          .mode("append")
          .save(dir.getAbsolutePath) // version 2
        deltaTable.delete() // version 3
        // Checkpoint will allow us to construct the table snapshot
        table.deltaLog.createCheckpointAtVersion(3L)
        // Deleting version 0 shouldn't fail the vacuum since
        // latestCommitVersionOutsideOfRetentionWindow is already at 1
        deleteCommitFile(table, 0L)// delete version 0.
        VacuumCommand.gc(spark, table, dryRun = true, retentionHours = Some(0))
        // Since commit versions 1 and 2 are required for lite vacuum, deleting them will
        // fail the command.
        for (i <- 1 to 2) {
          deleteCommitFile(table, i)
        }

        val e = intercept[DeltaIllegalStateException] {
          VacuumCommand.gc(spark, table, dryRun = true, retentionHours = Some(0))
        }
        assert(e.getMessage.contains("VACUUM LITE cannot delete all eligible files as some files" +
          " are not referenced by the Delta log. Please run VACUUM FULL."))
      }
    }
  }

  test("repeated invocations for lite vacuum is a no-op and doesn't throw any exception") {
    withEnvironment { (tempDir, clock) =>
      val reservoirDir = new File(tempDir.getAbsolutePath, "reservoir")
      val table = DeltaTableV2(spark, reservoirDir, clock)

      gcTest(table, clock)(
        // create 2  files
        CreateFile("file1.txt", commitToActionLog = true),
        CreateFile("file2.txt", commitToActionLog = true),
        LogicallyDeleteFile("file1.txt"),
        LogicallyDeleteFile("file2.txt"),
        CheckFiles(Seq("file1.txt", "file2.txt")),
        AdvanceClock(defaultTombstoneInterval + 1000),
        GC(dryRun = true, Seq(reservoirDir.toString + "/file1.txt",
          reservoirDir.toString + "/file2.txt")),
        GC(dryRun = false, Seq(reservoirDir.toString)),
        CheckFiles(Seq("file1.txt", "file2.txt"), exist = false),
        AdvanceClock(defaultTombstoneInterval + 1000),
        GC(dryRun = true, Seq()),
        GC(dryRun = false, Seq(reservoirDir.toString)),
        AdvanceClock(defaultTombstoneInterval + 1000),
        GC(dryRun = true, Seq()),
        GC(dryRun = false, Seq(reservoirDir.toString))
      )
    }
  }

  test("Vacuum retain argument is ignored if it's not 0 hours") {
    withEnvironment { (tempDir, clock) =>
      val reservoirDir = new File(tempDir.getAbsolutePath, "reservoir")
      val table = DeltaTableV2(spark, reservoirDir, clock)

      gcTest(table, clock)(
        // create 2  files
        CreateFile("file1.txt", commitToActionLog = true),
        CreateFile("file2.txt", commitToActionLog = true),
        LogicallyDeleteFile("file1.txt"),
        AdvanceClock(defaultTombstoneInterval + 1000),
        LogicallyDeleteFile("file2.txt"),
        CheckFiles(Seq("file1.txt", "file2.txt")),
        AdvanceClock((24 * 60 * 60 * 1000) + 1000), // 24 hours + 1000 ms
        // 24 hours retain argument is ignored and only file1.txt is eligible for GC
        GC(dryRun = true, Seq(reservoirDir.toString + "/file1.txt"), retentionHours = Some(24)),
        GC(dryRun = false, Seq(reservoirDir.toString), retentionHours = Some(24)),
        CheckFiles(Seq("file2.txt"))
      )
    }
  }
}
