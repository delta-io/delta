/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults

import java.io.File

import scala.collection.JavaConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.data.ColumnarBatch
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.utils.CloseableIterator
import io.delta.kernel.internal.ActionCommitLog.DeltaAction
import io.delta.kernel.internal.actions.{AddFile, RemoveFile}
import io.delta.kernel.internal.util.{FileNames, VectorUtils}
import io.delta.kernel.Table
import io.delta.kernel.exceptions.{KernelException, TableNotFoundException}
import io.delta.kernel.internal.TableImpl
import io.delta.kernel.internal.fs.Path

import org.apache.spark.sql.delta.actions.{Action => SparkAction, AddFile => SparkAddFile, RemoveFile => SparkRemoveFile}
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.funsuite.AnyFunSuite

class TableChangesSuite extends AnyFunSuite with TestUtils {

  /* actionSet including all currently supported actions */
  val FULL_ACTION_SET: Set[DeltaAction] = DeltaAction.values().toSet

  //////////////////////////////////////////////////////////////////////////////////
  // TableImpl.getChangesByVersion tests
  //////////////////////////////////////////////////////////////////////////////////

  /**
   * For the given parameters, read the table changes from Kernel using
   * TableImpl.getChangesByVersion and compare results with Spark
   */
  def testGetChangesByVersionVsSpark(
    tablePath: String,
    startVersion: Long,
    endVersion: Long,
    actionSet: Set[DeltaAction]): Unit = {

    val sparkChanges = DeltaLog.forTable(spark, tablePath)
      .getChanges(startVersion)
      .filter(_._1 <= endVersion) // Spark API does not have endVersion

    val kernelChanges = Table.forPath(defaultEngine, tablePath)
      .asInstanceOf[TableImpl]
      .getChangesByVersion(defaultEngine, startVersion, endVersion, actionSet.asJava)
      .toSeq

    // Check schema is as expected (version + timestamp column + the actions requested)
    kernelChanges.foreach { batch =>
      batch.getSchema.fields().asScala sameElements
        (Seq("version", "timestamp") ++ actionSet.map(_.colName))
    }

    compareActions(kernelChanges, pruneSparkActionsByActionSet(sparkChanges, actionSet))
  }

  // Golden table from Delta Standalone test
  test("getChangesByVersion - golden table deltalog-getChanges valid queries") {
    withGoldenTable("deltalog-getChanges") { tablePath =>
      testGetChangesByVersionVsSpark(
        tablePath,
        0,
        2,
        Set(DeltaAction.REMOVE)
      )
      testGetChangesByVersionVsSpark(
        tablePath,
        0,
        2,
        Set(DeltaAction.ADD)
      )
      testGetChangesByVersionVsSpark(
        tablePath,
        0,
        2,
        FULL_ACTION_SET
      )
      testGetChangesByVersionVsSpark(
        tablePath,
        1,
        2,
        FULL_ACTION_SET
      )
      testGetChangesByVersionVsSpark(
        tablePath,
        0,
        0,
        FULL_ACTION_SET
      )
    }
  }

  test("getChangesByVersion - returns correct timestamps") {
    withTempDir { tempDir =>

      def generateCommits(path: String, commits: Long*): Unit = {
        commits.zipWithIndex.foreach { case (ts, i) =>
          spark.range(i*10, i*10 + 10).write.format("delta").mode("append").save(path)
          val file = new File(FileNames.deltaFile(new Path(path, "_delta_log"), i))
          file.setLastModified(ts)
        }
      }

      val start = 1540415658000L
      val minuteInMilliseconds = 60000L
      generateCommits(tempDir.getCanonicalPath, start, start + 20 * minuteInMilliseconds,
        start + 40 * minuteInMilliseconds)
      val versionToTimestamp: Map[Long, Long] = Map(
        0L -> start,
        1L -> (start + 20 * minuteInMilliseconds),
        2L -> (start + 40 * minuteInMilliseconds)
      )

      // Check the timestamps are returned correctly
      Table.forPath(defaultEngine, tempDir.getCanonicalPath)
        .asInstanceOf[TableImpl]
        .getChangesByVersion(defaultEngine, 0, 2, Set(DeltaAction.ADD).asJava)
        .toSeq
        .flatMap(_.getRows.toSeq)
        .foreach { row =>
          val version = row.getLong(0)
          val timestamp = row.getLong(1)
          assert(timestamp == versionToTimestamp(version),
            f"Expected timestamp ${versionToTimestamp(version)} for version $version but" +
              f"Kernel returned timestamp $timestamp")
        }

      // Check contents as well
      testGetChangesByVersionVsSpark(
        tempDir.getCanonicalPath,
        0,
        2,
        FULL_ACTION_SET
      )
    }
  }

  test("getChangesByVersion - empty _delta_log folder") {
    withTempDir { tempDir =>
      new File(tempDir, "delta_log").mkdirs()
      intercept[TableNotFoundException] {
        Table.forPath(defaultEngine, tempDir.getCanonicalPath)
          .asInstanceOf[TableImpl]
          .getChangesByVersion(defaultEngine, 0, 2, FULL_ACTION_SET.asJava)
      }
    }
  }

  test("getChangesByVersion - empty folder no _delta_log dir") {
    withTempDir { tempDir =>
      intercept[TableNotFoundException] {
        Table.forPath(defaultEngine, tempDir.getCanonicalPath)
          .asInstanceOf[TableImpl]
          .getChangesByVersion(defaultEngine, 0, 2, FULL_ACTION_SET.asJava)
      }
    }
  }

  test("getChangesByVersion - non-empty folder not a delta table") {
    withTempDir { tempDir =>
      spark.range(20).write.format("parquet").mode("overwrite").save(tempDir.getCanonicalPath)
      intercept[TableNotFoundException] {
        Table.forPath(defaultEngine, tempDir.getCanonicalPath)
          .asInstanceOf[TableImpl]
          .getChangesByVersion(defaultEngine, 0, 2, FULL_ACTION_SET.asJava)
      }
    }
  }

  test("getChangesByVersion - directory does not exist") {
    intercept[TableNotFoundException] {
      Table.forPath(defaultEngine, "/fake/table/path")
        .asInstanceOf[TableImpl]
        .getChangesByVersion(defaultEngine, 0, 2, FULL_ACTION_SET.asJava)
    }
  }

  test("getChangesByVersion - golden table deltalog-getChanges invalid queries") {
    withGoldenTable("deltalog-getChanges") { tablePath =>
      def getChangesByVersion(
        startVersion: Long, endVersion: Long): CloseableIterator[ColumnarBatch] = {
        Table.forPath(defaultEngine, tablePath)
          .asInstanceOf[TableImpl]
          .getChangesByVersion(defaultEngine, startVersion, endVersion, FULL_ACTION_SET.asJava)
      }

      // startVersion after latest available version
      assert(intercept[KernelException]{
        getChangesByVersion(3, 8)
      }.getMessage.contains("no log files found in the requested version range"))

      // endVersion larger than latest available version
      assert(intercept[KernelException]{
        getChangesByVersion(0, 8)
      }.getMessage.contains("no log file found for version 8"))

      // invalid start version
      assert(intercept[KernelException]{
        getChangesByVersion(-1, 2)
      }.getMessage.contains("Invalid version range"))

      // invalid end version
      assert(intercept[KernelException]{
        getChangesByVersion(2, 1)
      }.getMessage.contains("Invalid version range"))
    }
  }

  test("getChangesByVersion - with truncated log") {
    withTempDir { tempDir =>
      // PREPARE TEST TABLE
      val tablePath = tempDir.getCanonicalPath
      // Write versions [0, 10] (inclusive) including a checkpoint
      (0 to 10).foreach { i =>
        spark.range(i*10, i*10 + 10).write
          .format("delta")
          .mode("append")
          .save(tablePath)
      }
      val log = org.apache.spark.sql.delta.DeltaLog.forTable(
        spark, new org.apache.hadoop.fs.Path(tablePath))
      val deltaCommitFileProvider = org.apache.spark.sql.delta.util.DeltaCommitFileProvider(
        log.unsafeVolatileSnapshot)
      // Delete the log files for versions 0-9, truncating the table history to version 10
      (0 to 9).foreach { i =>
        val jsonFile = deltaCommitFileProvider.deltaFile(i)
        new File(new org.apache.hadoop.fs.Path(log.logPath, jsonFile).toUri).delete()
      }
      // Create version 11 that overwrites the whole table
      spark.range(50).write
        .format("delta")
        .mode("overwrite")
        .save(tablePath)
      // Create version 12 that appends new data
      spark.range(10).write
        .format("delta")
        .mode("append")
        .save(tablePath)

      // TEST ERRORS
      // endVersion before earliest available version
      assert(intercept[KernelException] {
        Table.forPath(defaultEngine, tablePath)
          .asInstanceOf[TableImpl]
          .getChangesByVersion(defaultEngine, 0, 9, FULL_ACTION_SET.asJava)
      }.getMessage.contains("no log files found in the requested version range"))

      // startVersion less than the earliest available version
      assert(intercept[KernelException] {
        Table.forPath(defaultEngine, tablePath)
          .asInstanceOf[TableImpl]
          .getChangesByVersion(defaultEngine, 5, 11, FULL_ACTION_SET.asJava)
      }.getMessage.contains("no log file found for version 5"))

      // TEST VALID CASES
      testGetChangesByVersionVsSpark(
        tablePath,
        10,
        12,
        FULL_ACTION_SET
      )
      testGetChangesByVersionVsSpark(
        tablePath,
        11,
        12,
        FULL_ACTION_SET
      )
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // Helpers to compare actions returned between Kernel and Spark
  //////////////////////////////////////////////////////////////////////////////////

  // Standardize actions with case classes, keeping just a few fields to compare
  trait StandardAction

  case class StandardRemove(
    path: String,
    dataChange: Boolean,
    partitionValues: Map[String, String]) extends StandardAction

  case class StandardAdd(
    path: String,
    partitionValues: Map[String, String],
    size: Long,
    modificationTime: Long,
    dataChange: Boolean) extends StandardAction

  def standardizeKernelAction(row: Row): Option[StandardAction] = {
    val actionIdx = (2 until row.getSchema.length()).find(!row.isNullAt(_)).getOrElse(
      return None
    )

    row.getSchema.at(actionIdx).getName match {
      case DeltaAction.REMOVE.colName =>
        val removeRow = row.getStruct(actionIdx)
        val partitionValues: Map[String, String] = { // partitionValues is nullable for removes
          if (removeRow.isNullAt(RemoveFile.FULL_SCHEMA.indexOf("partitionValues"))) {
            null
          } else {
            VectorUtils.toJavaMap[String, String](
              removeRow.getMap(RemoveFile.FULL_SCHEMA.indexOf("partitionValues"))).asScala.toMap
          }
        }
        Some(StandardRemove(
          removeRow.getString(RemoveFile.FULL_SCHEMA.indexOf("path")),
          removeRow.getBoolean(RemoveFile.FULL_SCHEMA.indexOf("dataChange")),
          partitionValues
        ))

      case DeltaAction.ADD.colName =>
        val addRow = row.getStruct(actionIdx)
        Some(StandardAdd(
          addRow.getString(AddFile.FULL_SCHEMA.indexOf("path")),
          VectorUtils.toJavaMap[String, String](
            addRow.getMap(AddFile.FULL_SCHEMA.indexOf("partitionValues"))).asScala.toMap,
          addRow.getLong(AddFile.FULL_SCHEMA.indexOf("size")),
          addRow.getLong(AddFile.FULL_SCHEMA.indexOf("modificationTime")),
          addRow.getBoolean(AddFile.FULL_SCHEMA.indexOf("dataChange"))
        ))

      // TODO add more actions as we add support
      case _ =>
        throw new RuntimeException("Encountered an action that hasn't been added as an option yet")
    }
  }

  def standardizeSparkAction(action: SparkAction): Option[StandardAction] = action match {
    case remove: SparkRemoveFile =>
      Some(StandardRemove(remove.path, remove.dataChange, remove.partitionValues))
    case add: SparkAddFile =>
      Some(StandardAdd(
        add.path, add.partitionValues, add.size, add.modificationTime, add.dataChange))

    // TODO add more actions as we add support
    case _ => None
  }

  /**
   * When we query the Spark actions using DeltaLog::getChanges ALL action types are returned. Since
   * Kernel only returns actions in the provided `actionSet` this FX prunes the Spark actions to
   * match `actionSet`.
   */
  def pruneSparkActionsByActionSet(
    sparkActions: Iterator[(Long, Seq[SparkAction])],
    actionSet: Set[DeltaAction]): Iterator[(Long, Seq[SparkAction])] = {
    sparkActions.map { case (version, actions) =>
      (version,
        actions.filter {
          case _: SparkRemoveFile => actionSet.contains(DeltaAction.REMOVE)
          case _: SparkAddFile => actionSet.contains(DeltaAction.ADD)
          // TODO add more actions as we add support
          case _ => false
        }
      )
    }
  }

  def compareActions(
    kernelActions: Seq[ColumnarBatch],
    sparkActions: Iterator[(Long, Seq[SparkAction])]): Unit = {

    val standardKernelActions: Seq[(Long, StandardAction)] = {
      kernelActions.flatMap(_.getRows.toSeq)
        .map(row => (row.getLong(0), standardizeKernelAction(row)))
        .filter(_._2.nonEmpty)
        .map(t => (t._1, t._2.get))
    }

    val standardSparkActions: Seq[(Long, StandardAction)] =
      sparkActions.flatMap { case (version, actions) =>
        actions.map(standardizeSparkAction(_)).flatten.map((version, _))
      }.toSeq

    assert(standardKernelActions sameElements standardSparkActions,
      f"Kernel actions did not match Spark actions.\n" +
        f"Kernel actions: $standardKernelActions\n" +
        f"Spark actions: $standardSparkActions"
    )
  }
}
