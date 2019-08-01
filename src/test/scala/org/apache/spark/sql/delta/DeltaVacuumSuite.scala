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
import java.util.Locale

import org.apache.spark.sql.delta.DeltaOperations.{Delete, Write}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, RemoveFile}
import org.apache.spark.sql.delta.commands.{DeltaVacuumStats, VacuumCommand}
import org.apache.spark.sql.delta.util.{DeltaFileOperations, JsonUtils}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.scalatest.GivenWhenThen

import org.apache.spark.sql.{QueryTest, SaveMode}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.ManualClock

trait DeltaVacuumSuiteBase extends QueryTest with SharedSQLContext with GivenWhenThen {

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
        CreateFile("file1.txt", commitToActionLog = true),
        CreateFile("_underscore_col_=10/test.txt", commitToActionLog = true),
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

  testQuietly("gc test") {
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
      gcTest(deltaLog, clock)(
        CreateFile("file1.txt", commitToActionLog = true),
        CheckFiles(Seq("file1.txt")),
        ExpectFailure(
          GCScalaApi(Seq(), Some(-2)),
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
        CreateFile("file1.txt", commitToActionLog = true),
        CheckFiles(Seq("file1.txt")),
        GC(false, Seq(tempDir.toString), Some(0))
      )
    }
  }

  test("scala api test") {
    withEnvironment { (tempDir, clock) =>
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath, clock)
      gcTest(deltaLog, clock)(
        CreateFile("file1.txt", commitToActionLog = true),
        CreateFile("file2.txt", commitToActionLog = false),
        GCScalaApi(expectedDf = Seq()),
        CheckFiles(Seq("file1.txt")),
        GCScalaApi(expectedDf = Seq(), retentionHours = Some(0)),
        CheckFiles(Seq("file2.txt"), exist = false),
        CreateFile("file2.txt", commitToActionLog = false),
        CheckFiles(Seq("file2.txt")),
        GCScalaApi(expectedDf = Seq(), retentionHours = Some(0)),
        CheckFiles(Seq("file2.txt"), exist = false)
      )
    }
  }

  protected def withEnvironment(f: (File, ManualClock) => Unit): Unit = {
    withTempDir { file =>
      val clock = new ManualClock()
      withSQLConf("spark.databricks.delta.retentionDurationCheck.enabled" -> "false") {
        f(file, clock)
      }
    }
  }

  protected def defaultTombstoneInterval: Long = {
    CalendarInterval.fromString(DeltaConfigs.TOMBSTONE_RETENTION.defaultValue).milliseconds()
  }

  implicit def fileToPathString(f: File): String = new Path(f.getAbsolutePath).toString

  trait Action
  /**
   * Write a file to the given absolute or relative path. Could be inside or outside the Reservoir
   * base path. The file can be committed to the action log to be tracked, or left out for deletion.
   */
  case class CreateFile(path: String, commitToActionLog: Boolean) extends Action
  /** Create a directory at the given path. */
  case class CreateDirectory(path: String) extends Action
  /**
   * Logically deletes a file in the action log. Paths can be absolute or relative paths, and can
   * point to files inside and outside a reservoir.
   */
  case class LogicallyDeleteFile(path: String) extends Action
  /** Check that the given paths exist. */
  case class CheckFiles(paths: Seq[String], exist: Boolean = true) extends Action
  /** Garbage collect the reservoir. */
  case class GC(
      dryRun: Boolean,
      expectedDf: Seq[String],
      retentionHours: Option[Double] = None) extends Action
  /** Garbage collect the reservoir. */
  case class GCScalaApi(
      expectedDf: Seq[String],
      retentionHours: Option[Double] = None) extends Action
  /** Advance the time. */
  case class AdvanceClock(timeToAdd: Long) extends Action
  /** Execute SQL command */
  case class ExecuteSQL(sql: String, expectedDf: Seq[String]) extends Action
  /**
   * Expect a failure with the given exception type. Expect the given `msg` fragments as the error
   * message.
   */
  case class ExpectFailure[T <: Throwable](
      action: Action,
      expectedError: Class[T],
      msg: Seq[String]) extends Action

  protected def createFile(
      reservoirBase: String,
      filePath: String,
      file: File,
      clock: ManualClock): AddFile = {
    FileUtils.write(file, "gibberish")
    file.setLastModified(clock.getTimeMillis())
    AddFile(filePath, Map.empty, 10L, clock.getTimeMillis(), dataChange = true)
  }

  protected def gcTest(deltaLog: DeltaLog, clock: ManualClock)(actions: Action*): Unit = {
    import testImplicits._
    val basePath = deltaLog.dataPath.toString
    val fs = new Path(basePath).getFileSystem(spark.sessionState.newHadoopConf())
    actions.foreach {
      case CreateFile(path, commit) =>
        Given(s"*** Writing file to $path. Commit to log: $commit")
        val sanitizedPath = new Path(path).toUri.toString
        val file = new File(
          fs.makeQualified(DeltaFileOperations.absolutePath(basePath, sanitizedPath)).toUri)
        if (commit) {
          val txn = deltaLog.startTransaction()
          val action = createFile(basePath, sanitizedPath, file, clock)
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
        txn.commit(Seq(RemoveFile(path, Option(clock.getTimeMillis()))), Delete("true" :: Nil))
      // scalastyle:on
      case ExecuteSQL(statement, expectedDf) =>
        Given(s"*** Executing SQL: $statement")
        val qualified = expectedDf.map(p => fs.makeQualified(new Path(p)).toString)
        checkDatasetUnorderly(spark.sql(statement).as[String], qualified: _*)
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
      case GCScalaApi(expectedDf, retention) =>
        Given("*** Garbage collecting Reservoir using Scala")
        val deltaTable = io.delta.tables.DeltaTable.forPath(spark, deltaLog.dataPath.toString)
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
}

class DeltaVacuumSuite
  extends DeltaVacuumSuiteBase