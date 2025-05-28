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
import scala.collection.immutable

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.Table
import io.delta.kernel.data.ColumnarBatch
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.exceptions.{KernelException, TableNotFoundException}
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction
import io.delta.kernel.internal.TableImpl
import io.delta.kernel.internal.actions.{AddCDCFile, AddFile, CommitInfo, Metadata, Protocol, RemoveFile}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.{FileNames, ManualClock, VectorUtils}
import io.delta.kernel.utils.CloseableIterator

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Action => SparkAction, AddCDCFile => SparkAddCDCFile, AddFile => SparkAddFile, CommitInfo => SparkCommitInfo, Metadata => SparkMetadata, Protocol => SparkProtocol, RemoveFile => SparkRemoveFile, SetTransaction => SparkSetTransaction}
import org.apache.spark.sql.delta.test.DeltaTestImplicits.OptimisticTxnTestHelper

import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.scalatest.funsuite.AnyFunSuite

class TableChangesSuite extends AnyFunSuite with TestUtils with DeltaTableWriteSuiteBase {

  /* actionSet including all currently supported actions */
  val FULL_ACTION_SET: Set[DeltaAction] = DeltaAction.values().toSet

  //////////////////////////////////////////////////////////////////////////////////
  // TableImpl.getChangesByVersion tests
  //////////////////////////////////////////////////////////////////////////////////

  /**
   * For the given parameters, read the table changes from Kernel using
   * TableImpl.getChangesByVersion and compare results with Spark
   */
  def testGetChangesVsSpark(
      tablePath: String,
      startVersion: Long,
      endVersion: Long,
      actionSet: Set[DeltaAction]): Unit = {

    val sparkChanges = DeltaLog.forTable(spark, tablePath)
      .getChanges(startVersion)
      .filter(_._1 <= endVersion) // Spark API does not have endVersion

    val kernelChanges = Table.forPath(defaultEngine, tablePath)
      .asInstanceOf[TableImpl]
      .getChanges(defaultEngine, startVersion, endVersion, actionSet.asJava)
      .toSeq

    // Check schema is as expected (version + timestamp column + the actions requested)
    kernelChanges.foreach { batch =>
      batch.getSchema.fields().asScala sameElements
        (Seq("version", "timestamp") ++ actionSet.map(_.colName))
    }

    compareActions(kernelChanges, pruneSparkActionsByActionSet(sparkChanges, actionSet))
  }

  Seq(true, false).foreach { ictEnabled =>
    test(s"getChanges should return the same results as Spark [ictEnabled: $ictEnabled]") {
      withTempDir { tempDir =>
        val tablePath = tempDir.getCanonicalPath()
        // The code to create this table is copied from GoldenTables.scala.
        // The part that enables ICT is a new addition.
        val log = DeltaLog.forTable(spark, new HadoopPath(tablePath))

        val schema = new StructType()
          .add("part", IntegerType)
          .add("id", IntegerType)
        val configuration = if (ictEnabled) {
          Map("delta.enableInCommitTimestamps" -> "true")
        } else {
          Map.empty[String, String]
        }
        val metadata = SparkMetadata(schemaString = schema.json, configuration = configuration)

        val add1 = SparkAddFile("fake/path/1", Map.empty, 1, 1, dataChange = true)
        val txn1 = log.startTransaction()
        txn1.commitManually(metadata :: add1 :: Nil: _*)

        val addCDC2 = SparkAddCDCFile(
          "fake/path/2",
          Map("partition_foo" -> "partition_bar"),
          1,
          Map("tag_foo" -> "tag_bar"))
        val remove2 = SparkRemoveFile("fake/path/1", Some(100), dataChange = true)
        val txn2 = log.startTransaction()
        txn2.commitManually(addCDC2 :: remove2 :: Nil: _*)

        val setTransaction3 = SparkSetTransaction("fakeAppId", 3L, Some(200))
        val txn3 = log.startTransaction()
        val latestTableProtocol = log.snapshot.protocol
        txn3.commitManually(latestTableProtocol :: setTransaction3 :: Nil: _*)

        // request subset of actions
        testGetChangesVsSpark(
          tablePath,
          0,
          2,
          Set(DeltaAction.REMOVE))
        testGetChangesVsSpark(
          tablePath,
          0,
          2,
          Set(DeltaAction.ADD))
        testGetChangesVsSpark(
          tablePath,
          0,
          2,
          Set(DeltaAction.ADD, DeltaAction.REMOVE, DeltaAction.METADATA, DeltaAction.PROTOCOL))
        // request full actions, various versions
        testGetChangesVsSpark(
          tablePath,
          0,
          2,
          FULL_ACTION_SET)
        testGetChangesVsSpark(
          tablePath,
          1,
          2,
          FULL_ACTION_SET)
        testGetChangesVsSpark(
          tablePath,
          0,
          0,
          FULL_ACTION_SET)
      }
    }
  }

  Seq(Some(0), Some(1), None).foreach { ictEnablementVersion =>
    test("getChanges - returns correct timestamps " +
      s"[ictEnablementVersion = ${ictEnablementVersion.getOrElse("None")}]") {
      withTempDirAndEngine { (tempDir, engine) =>
        def generateCommits(tablePath: String, commits: Long*): Unit = {
          commits.zipWithIndex.foreach { case (ts, i) =>
            val tableProperties = if (ictEnablementVersion.contains(i)) {
              Map("delta.enableInCommitTimestamps" -> "true")
            } else {
              Map.empty[String, String]
            }
            val clock = new ManualClock(ts)
            appendData(
              engine,
              tablePath,
              isNewTable = i == 0,
              schema = testSchema,
              data = immutable.Seq(Map.empty[String, Literal] -> dataBatches2),
              clock = clock,
              tableProperties = tableProperties)
            // Only set the file modification time if ICT has not been enabled yet.
            if (!ictEnablementVersion.exists(_ <= i)) {
              val file = new File(FileNames.deltaFile(new Path(tablePath, "_delta_log"), i))
              file.setLastModified(ts)
            }
          }
        }

        val start = 1540415658000L
        val minuteInMilliseconds = 60000L
        generateCommits(
          tempDir,
          start,
          start + 20 * minuteInMilliseconds,
          start + 40 * minuteInMilliseconds)
        val versionToTimestamp: Map[Long, Long] = Map(
          0L -> start,
          1L -> (start + 20 * minuteInMilliseconds),
          2L -> (start + 40 * minuteInMilliseconds))

        // Check the timestamps are returned correctly
        Table.forPath(defaultEngine, tempDir)
          .asInstanceOf[TableImpl]
          .getChanges(defaultEngine, 0, 2, Set(DeltaAction.ADD).asJava)
          .toSeq
          .flatMap(_.getRows.toSeq)
          .foreach { row =>
            val version = row.getLong(0)
            val timestamp = row.getLong(1)
            assert(
              timestamp == versionToTimestamp(version),
              f"Expected timestamp ${versionToTimestamp(version)} for version $version but" +
                f"Kernel returned timestamp $timestamp")
          }

        // Check contents as well
        testGetChangesVsSpark(
          tempDir,
          0,
          2,
          FULL_ACTION_SET)
      }
    }
  }

  test("getChanges - empty _delta_log folder") {
    withTempDir { tempDir =>
      new File(tempDir, "delta_log").mkdirs()
      intercept[TableNotFoundException] {
        Table.forPath(defaultEngine, tempDir.getCanonicalPath)
          .asInstanceOf[TableImpl]
          .getChanges(defaultEngine, 0, 2, FULL_ACTION_SET.asJava)
      }
    }
  }

  test("getChanges - empty folder no _delta_log dir") {
    withTempDir { tempDir =>
      intercept[TableNotFoundException] {
        Table.forPath(defaultEngine, tempDir.getCanonicalPath)
          .asInstanceOf[TableImpl]
          .getChanges(defaultEngine, 0, 2, FULL_ACTION_SET.asJava)
      }
    }
  }

  test("getChanges - non-empty folder not a delta table") {
    withTempDir { tempDir =>
      spark.range(20).write.format("parquet").mode("overwrite").save(tempDir.getCanonicalPath)
      intercept[TableNotFoundException] {
        Table.forPath(defaultEngine, tempDir.getCanonicalPath)
          .asInstanceOf[TableImpl]
          .getChanges(defaultEngine, 0, 2, FULL_ACTION_SET.asJava)
      }
    }
  }

  test("getChanges - directory does not exist") {
    intercept[TableNotFoundException] {
      Table.forPath(defaultEngine, "/fake/table/path")
        .asInstanceOf[TableImpl]
        .getChanges(defaultEngine, 0, 2, FULL_ACTION_SET.asJava)
    }
  }

  test("getChanges - golden table deltalog-getChanges invalid queries") {
    withGoldenTable("deltalog-getChanges") { tablePath =>
      def getChangesByVersion(
          startVersion: Long,
          endVersion: Long): CloseableIterator[ColumnarBatch] = {
        Table.forPath(defaultEngine, tablePath)
          .asInstanceOf[TableImpl]
          .getChanges(defaultEngine, startVersion, endVersion, FULL_ACTION_SET.asJava)
      }

      // startVersion after latest available version
      assert(intercept[KernelException] {
        getChangesByVersion(3, 8)
      }.getMessage.contains("no log files found in the requested version range"))

      // endVersion larger than latest available version
      assert(intercept[KernelException] {
        getChangesByVersion(0, 8)
      }.getMessage.contains("no log file found for version 8"))

      // invalid start version
      assert(intercept[KernelException] {
        getChangesByVersion(-1, 2)
      }.getMessage.contains("Invalid version range"))

      // invalid end version
      assert(intercept[KernelException] {
        getChangesByVersion(2, 1)
      }.getMessage.contains("Invalid version range"))
    }
  }

  test("getChanges - with truncated log") {
    withTempDir { tempDir =>
      // PREPARE TEST TABLE
      val tablePath = tempDir.getCanonicalPath
      // Write versions [0, 10] (inclusive) including a checkpoint
      (0 to 10).foreach { i =>
        spark.range(i * 10, i * 10 + 10).write
          .format("delta")
          .mode("append")
          .save(tablePath)
      }
      val log = org.apache.spark.sql.delta.DeltaLog.forTable(
        spark,
        new org.apache.hadoop.fs.Path(tablePath))
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
          .getChanges(defaultEngine, 0, 9, FULL_ACTION_SET.asJava)
      }.getMessage.contains("no log files found in the requested version range"))

      // startVersion less than the earliest available version
      assert(intercept[KernelException] {
        Table.forPath(defaultEngine, tablePath)
          .asInstanceOf[TableImpl]
          .getChanges(defaultEngine, 5, 11, FULL_ACTION_SET.asJava)
      }.getMessage.contains("no log file found for version 5"))

      // TEST VALID CASES
      testGetChangesVsSpark(
        tablePath,
        10,
        12,
        FULL_ACTION_SET)
      testGetChangesVsSpark(
        tablePath,
        11,
        12,
        FULL_ACTION_SET)
    }
  }

  test("getChanges - table with a lot of changes") {
    withTempDir { tempDir =>
      spark.sql(
        f"""
          |CREATE TABLE delta.`${tempDir.getCanonicalPath}` (id LONG, month LONG)
          |USING DELTA
          |PARTITIONED BY (month)
          |TBLPROPERTIES (delta.enableChangeDataFeed = true)
          |""".stripMargin)
      spark.range(100).withColumn("month", col("id") % 12 + 1)
        .write
        .format("delta")
        .mode("append")
        .save(tempDir.getCanonicalPath)
      spark.sql( // cdc actions
        f"""
           |UPDATE delta.`${tempDir.getCanonicalPath}` SET month = 1 WHERE id < 10
           |""".stripMargin)
      spark.sql(
        f"""
           |DELETE FROM delta.`${tempDir.getCanonicalPath}` WHERE month = 12
           |""".stripMargin)
      spark.sql(
        f"""
           |DELETE FROM delta.`${tempDir.getCanonicalPath}` WHERE id = 52
           |""".stripMargin)
      spark.range(100, 150).withColumn("month", col("id") % 12)
        .write
        .format("delta")
        .mode("overwrite")
        .save(tempDir.getCanonicalPath)
      spark.sql( // change metadata
        f"""
           |ALTER TABLE delta.`${tempDir.getCanonicalPath}`
           |ADD CONSTRAINT validMonth CHECK (month <= 12)
           |""".stripMargin)

      // Check all actions are correctly retrieved
      testGetChangesVsSpark(
        tempDir.getCanonicalPath,
        0,
        6,
        FULL_ACTION_SET)
      // Check some subset of actions
      testGetChangesVsSpark(
        tempDir.getCanonicalPath,
        0,
        6,
        Set(DeltaAction.ADD))
    }
  }

  test("getChanges - fails when protocol is not readable by Kernel") {
    // Existing tests suffice to check if the protocol column is present/dropped correctly
    // We test our protocol checks for table features in TableFeaturesSuite
    // Min reader version is too high
    assert(intercept[KernelException] {
      // Use toSeq because we need to consume the iterator to force the exception
      Table.forPath(defaultEngine, goldenTablePath("deltalog-invalid-protocol-version"))
        .asInstanceOf[TableImpl]
        .getChanges(defaultEngine, 0, 0, FULL_ACTION_SET.asJava).toSeq
    }.getMessage.contains("Unsupported Delta protocol reader version"))
    // We still get an error if we don't request the protocol file action
    assert(intercept[KernelException] {
      Table.forPath(defaultEngine, goldenTablePath("deltalog-invalid-protocol-version"))
        .asInstanceOf[TableImpl]
        .getChanges(defaultEngine, 0, 0, Set(DeltaAction.ADD).asJava).toSeq
    }.getMessage.contains("Unsupported Delta protocol reader version"))
  }

  withGoldenTable("commit-info-containing-arbitrary-operationParams-types") { tablePath =>
    test("getChanges - commit info with arbitrary operationParams types") {
      // Check all actions are correctly retrieved
      testGetChangesVsSpark(
        tablePath,
        0,
        2,
        FULL_ACTION_SET)
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

  case class StandardMetadata(
      id: String,
      schemaString: String,
      partitionColumns: Seq[String],
      configuration: Map[String, String]) extends StandardAction

  case class StandardProtocol(
      minReaderVersion: Int,
      minWriterVersion: Int,
      readerFeatures: Set[String],
      writerFeatures: Set[String]) extends StandardAction

  case class StandardCommitInfo(
      operation: String,
      operationMetrics: Map[String, String]) extends StandardAction

  case class StandardCdc(
      path: String,
      partitionValues: Map[String, String],
      size: Long,
      tags: Map[String, String]) extends StandardAction

  def standardizeKernelAction(row: Row): Option[StandardAction] = {
    val actionIdx = (2 until row.getSchema.length()).find(!row.isNullAt(_)).getOrElse(
      return None)

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
          partitionValues))

      case DeltaAction.ADD.colName =>
        val addRow = row.getStruct(actionIdx)
        Some(StandardAdd(
          addRow.getString(AddFile.FULL_SCHEMA.indexOf("path")),
          VectorUtils.toJavaMap[String, String](
            addRow.getMap(AddFile.FULL_SCHEMA.indexOf("partitionValues"))).asScala.toMap,
          addRow.getLong(AddFile.FULL_SCHEMA.indexOf("size")),
          addRow.getLong(AddFile.FULL_SCHEMA.indexOf("modificationTime")),
          addRow.getBoolean(AddFile.FULL_SCHEMA.indexOf("dataChange"))))

      case DeltaAction.METADATA.colName =>
        val metadataRow = row.getStruct(actionIdx)
        Some(StandardMetadata(
          metadataRow.getString(Metadata.FULL_SCHEMA.indexOf("id")),
          metadataRow.getString(Metadata.FULL_SCHEMA.indexOf("schemaString")),
          VectorUtils.toJavaList(
            metadataRow.getArray(Metadata.FULL_SCHEMA.indexOf("partitionColumns"))).asScala.toSeq,
          VectorUtils.toJavaMap[String, String](
            metadataRow.getMap(Metadata.FULL_SCHEMA.indexOf("configuration"))).asScala.toMap))

      case DeltaAction.PROTOCOL.colName =>
        val protocolRow = row.getStruct(actionIdx)
        val readerFeatures =
          if (protocolRow.isNullAt(Protocol.FULL_SCHEMA.indexOf("readerFeatures"))) {
            Seq()
          } else {
            VectorUtils.toJavaList(
              protocolRow.getArray(Protocol.FULL_SCHEMA.indexOf("readerFeatures"))).asScala
          }
        val writerFeatures =
          if (protocolRow.isNullAt(Protocol.FULL_SCHEMA.indexOf("writerFeatures"))) {
            Seq()
          } else {
            VectorUtils.toJavaList(
              protocolRow.getArray(Protocol.FULL_SCHEMA.indexOf("writerFeatures"))).asScala
          }

        Some(StandardProtocol(
          protocolRow.getInt(Protocol.FULL_SCHEMA.indexOf("minReaderVersion")),
          protocolRow.getInt(Protocol.FULL_SCHEMA.indexOf("minWriterVersion")),
          readerFeatures.toSet,
          writerFeatures.toSet))

      case DeltaAction.COMMITINFO.colName =>
        val commitInfoRow = row.getStruct(actionIdx)
        val operationIdx = CommitInfo.FULL_SCHEMA.indexOf("operation")
        val operationMetricsIdx = CommitInfo.FULL_SCHEMA.indexOf("operationMetrics")

        Some(StandardCommitInfo(
          if (commitInfoRow.isNullAt(operationIdx)) null else commitInfoRow.getString(operationIdx),
          if (commitInfoRow.isNullAt(operationMetricsIdx)) {
            Map.empty
          } else {
            VectorUtils.toJavaMap[String, String](
              commitInfoRow.getMap(operationMetricsIdx)).asScala.toMap
          }))

      case DeltaAction.CDC.colName =>
        val cdcRow = row.getStruct(actionIdx)
        val tags: Map[String, String] = {
          if (cdcRow.isNullAt(AddCDCFile.FULL_SCHEMA.indexOf("tags"))) {
            null
          } else {
            VectorUtils.toJavaMap[String, String](
              cdcRow.getMap(AddCDCFile.FULL_SCHEMA.indexOf("tags"))).asScala.toMap
          }
        }
        Some(StandardCdc(
          cdcRow.getString(AddCDCFile.FULL_SCHEMA.indexOf("path")),
          VectorUtils.toJavaMap[String, String](
            cdcRow.getMap(AddCDCFile.FULL_SCHEMA.indexOf("partitionValues"))).asScala.toMap,
          cdcRow.getLong(AddCDCFile.FULL_SCHEMA.indexOf("size")),
          tags))

      case _ =>
        throw new RuntimeException("Encountered an action that hasn't been added as an option yet")
    }
  }

  def standardizeSparkAction(action: SparkAction): Option[StandardAction] = action match {
    case remove: SparkRemoveFile =>
      Some(StandardRemove(remove.path, remove.dataChange, remove.partitionValues))
    case add: SparkAddFile =>
      Some(StandardAdd(
        add.path,
        add.partitionValues,
        add.size,
        add.modificationTime,
        add.dataChange))
    case metadata: SparkMetadata =>
      Some(StandardMetadata(
        metadata.id,
        metadata.schemaString,
        metadata.partitionColumns,
        metadata.configuration))
    case protocol: SparkProtocol =>
      Some(StandardProtocol(
        protocol.minReaderVersion,
        protocol.minWriterVersion,
        protocol.readerFeatures.getOrElse(Set.empty),
        protocol.writerFeatures.getOrElse(Set.empty)))
    case commitInfo: SparkCommitInfo =>
      Some(StandardCommitInfo(
        commitInfo.operation,
        commitInfo.operationMetrics.getOrElse(Map.empty)))
    case cdc: SparkAddCDCFile =>
      Some(StandardCdc(cdc.path, cdc.partitionValues, cdc.size, cdc.tags))
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
      (
        version,
        actions.filter {
          case _: SparkRemoveFile => actionSet.contains(DeltaAction.REMOVE)
          case _: SparkAddFile => actionSet.contains(DeltaAction.ADD)
          case _: SparkMetadata => actionSet.contains(DeltaAction.METADATA)
          case _: SparkProtocol => actionSet.contains(DeltaAction.PROTOCOL)
          case _: SparkCommitInfo => actionSet.contains(DeltaAction.COMMITINFO)
          case _: SparkAddCDCFile => actionSet.contains(DeltaAction.CDC)
          case _ => false
        })
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

    assert(
      standardKernelActions sameElements standardSparkActions,
      f"Kernel actions did not match Spark actions.\n" +
        f"Kernel actions: $standardKernelActions\n" +
        f"Spark actions: $standardSparkActions")
  }
}
