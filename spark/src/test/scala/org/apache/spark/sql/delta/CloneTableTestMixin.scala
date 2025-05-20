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

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.actions.{AddFile, FileAction, RemoveFile, SingleAction}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.{CloneDeltaSource, CloneSource, CloneSourceFormat}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames.unsafeDeltaFile
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path
import org.scalatest.{BeforeAndAfterAll, Tag}

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

/** Common test setup and utils for CLONE TABLE tests. */
trait CloneTableTestMixin extends DeltaColumnMappingTestUtils
  with BeforeAndAfterAll
  with DeltaTestUtilsBase {
  self: QueryTest with SharedSparkSession =>

  protected val TAG_HAS_SHALLOW_CLONE = new Tag("SHALLOW CLONE")
  protected val TAG_MODIFY_PROTOCOL = new Tag("CHANGES PROTOCOL")
  protected val TAG_CHANGE_COLUMN_MAPPING_MODE = new Tag("CHANGES COLUMN MAPPING MODE")
  protected val TAG_USES_CONVERT_TO_DELTA = new Tag("USES CONVERT TO DELTA")

  // scalastyle:off argcount
  protected def cloneTable(
      source: String,
      target: String,
      isShallow: Boolean,
      sourceIsTable: Boolean = false,
      targetIsTable: Boolean = false,
      targetLocation: Option[String] = None,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None,
      isCreate: Boolean = true,
      isReplace: Boolean = false,
      tableProperties: Map[String, String] = Map.empty): Unit
  // scalastyle:on argcount

  protected def withSourceTargetDir(f: (String, String) => Unit): Unit = {
    withTempDir { dir =>
      val firstDir = new File(dir, "source").getCanonicalPath
      val secondDir = new File(dir, "clone").getCanonicalPath
      f(firstDir, secondDir)
    }
  }

  protected def cloneTypeStr(isShallow: Boolean): String = {
    "SHALLOW"
  }

  /**
   * Run the given test function for SHALLOW clone.
   */
  protected def testAllClones(testName: String, testTags: org.scalatest.Tag*)
      (testFunc: (String, String, Boolean) => Unit): Unit = {
    val tags = Seq(TAG_HAS_SHALLOW_CLONE)
    cloneTest(s"$testName", testTags ++ tags: _*) {
      (source, target) => testFunc(source, target, true)
    }
  }

  protected def cloneTest(
      testName: String, testTags: org.scalatest.Tag*)(f: (String, String) => Unit): Unit = {
    if (testTags.exists(_.name == TAG_CHANGE_COLUMN_MAPPING_MODE.name) &&
        columnMappingMode != "none") {
      ignore(testName + " (not supporting changing column mapping mode)") {
        withSourceTargetDir(f)
      }
    } else {
      test(testName, testTags: _*) {
        withSourceTargetDir(f)
      }
    }
  }

  // Extracted function so it can be overriden in subclasses.
  protected def uniqueFileActionGroupBy(action: FileAction): String = {
    val filePath = action.pathAsUri.toString
    val dvId = action match {
      case add: AddFile => Option(add.deletionVector).map(_.uniqueId).getOrElse("")
      case remove: RemoveFile => Option(remove.deletionVector).map(_.uniqueId).getOrElse("")
      case _ => ""
    }
    filePath + dvId
  }

  import testImplicits._
  // scalastyle:off
  protected def runAndValidateClone(
      source: String,
      target: String,
      isShallow: Boolean,
      sourceIsTable: Boolean = false,
      targetIsTable: Boolean = false,
      targetLocation: Option[String] = None,
      sourceVersion: Option[Long] = None,
      sourceTimestamp: Option[String] = None,
      isCreate: Boolean = true,
      // If we are doing a replace on an existing table
      isReplaceOperation: Boolean = false,
      // If we are doing a replace, whether it is on a Delta table
      isReplaceDelta: Boolean = true,
      tableProperties: Map[String, String] = Map.empty,
      commitLargeMetricsMap: Map[String, String] = Map.empty,
      expectedDataframe: DataFrame = spark.emptyDataFrame)
      (f: () => Unit =
        () => cloneTable(
          source,
          target,
          isShallow,
          sourceIsTable,
          targetIsTable,
          targetLocation,
          sourceVersion,
          sourceTimestamp,
          isCreate,
          isReplaceOperation,
          tableProperties)): Unit = {
    // scalastyle:on
    // Truncate table before REPLACE
    try {
      if (isReplaceOperation) {
        val targetTbl = if (targetIsTable) {
          target
        } else {
          s"delta.`$target`"
        }
        sql(s"DELETE FROM $targetTbl")
      }
    } catch {
      case _: Throwable =>
        // ignore all
    }

    // Check logged blob for expected values
    val allLogs = Log4jUsageLogger.track {
      f()
    }
    verifyAllCloneOperationsEmitted(allLogs,
      isReplaceOperation && isReplaceDelta,
      commitLargeMetricsMap)

    val blob = JsonUtils.fromJson[Map[String, Any]](allLogs
      .filter(_.metric == "tahoeEvent")
      .filter(_.tags.get("opType").contains("delta.clone"))
      .filter(_.blob.contains("source"))
      .map(_.blob).last)

    val sourceIdent = resolveTableIdentifier(source, Some("delta"), sourceIsTable)
    val (cloneSource: CloneSource, sourceDf: DataFrame) = {
      val sourceLog = DeltaLog.forTable(spark, sourceIdent)
      val timeTravelSpec: Option[DeltaTimeTravelSpec] =
        if (sourceVersion.isDefined || sourceTimestamp.isDefined) {
          Some(DeltaTimeTravelSpec(sourceTimestamp.map(Literal(_)), sourceVersion, None))
        } else {
          None
        }
      val deltaTable = DeltaTableV2(spark, sourceLog.dataPath, timeTravelOpt = timeTravelSpec)
      val sourceData = DataFrameUtils.ofRows(
        spark,
        LogicalRelation(sourceLog.createRelation(
          snapshotToUseOpt = Some(deltaTable.initialSnapshot),
          isTimeTravelQuery = sourceVersion.isDefined || sourceTimestamp.isDefined)))
      (new CloneDeltaSource(deltaTable), sourceData)
    }

    val targetLog = if (targetIsTable) {
      DeltaLog.forTable(spark, TableIdentifier(target))
    } else {
      DeltaLog.forTable(spark, target)
    }

    val sourceSnapshot = cloneSource.snapshot

    val sourcePath = cloneSource.dataPath
    // scalastyle:off deltahadoopconfiguration
    val fs = sourcePath.getFileSystem(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    val qualifiedSourcePath = fs.makeQualified(sourcePath)
    val logSource = if (sourceIsTable) {
      val catalog = CatalogManager.SESSION_CATALOG_NAME
      s"$catalog.default.$source".toLowerCase(Locale.ROOT)
    } else {
      s"delta.`$qualifiedSourcePath`"
    }

    val rawTarget = new Path(targetLocation.getOrElse(targetLog.dataPath.toString))
    // scalastyle:off deltahadoopconfiguration
    val targetFs = rawTarget.getFileSystem(targetLog.newDeltaHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    val qualifiedTarget = targetFs.makeQualified(rawTarget)

    // Check whether recordEvent operation is of correct form
    assert(blob("source") != null)
    val actualLogSource = blob("source").toString
    assert(actualLogSource === logSource)
    if (source != target) {
      assert(blob("sourceVersion") === sourceSnapshot.get.version)
    }
    val replacingDeltaTable = isReplaceOperation && isReplaceDelta
    assert(blob("sourcePath") === qualifiedSourcePath.toString)
    assert(blob("target") === qualifiedTarget.toString)
    assert(blob("isReplaceDelta") === replacingDeltaTable)
    assert(blob("sourceTableSize") === cloneSource.sizeInBytes)
    assert(blob("sourceNumOfFiles") === cloneSource.numOfFiles)
    assert(blob("partitionBy") === cloneSource.metadata.partitionColumns)

    // Check whether resulting metadata of target and source at version is the same
    compareMetadata(
      cloneSource,
      targetLog.unsafeVolatileSnapshot,
      targetLocation.isEmpty && targetIsTable,
      isReplaceOperation)

    val commit = unsafeDeltaFile(targetLog.logPath, targetLog.unsafeVolatileSnapshot.version)
    val hadoopConf = targetLog.newDeltaHadoopConf()
    val filePaths: Seq[FileAction] = targetLog.store.read(commit, hadoopConf).flatMap { line =>
      JsonUtils.fromJson[SingleAction](line) match {
        case a if a.add != null => Some(a.add)
        case a if a.remove != null => Some(a.remove)
        case _ => None
      }
    }
    assert(filePaths.groupBy(uniqueFileActionGroupBy(_)).forall(_._2.length === 1),
      "A file was added and removed in the same commit")
    // Check whether the resulting datasets are the same
    val targetDf = DataFrameUtils.ofRows(
      spark,
      LogicalRelation(targetLog.createRelation()))
    checkAnswer(
      targetDf,
      sourceDf)
  }


  protected def verifyAllCloneOperationsEmitted(
      allLogs: Seq[UsageRecord],
      emitHandleExistingTable: Boolean,
      commitLargeMetricsMap: Map[String, String] = Map.empty): Unit = {
    val cloneLogs = allLogs
      .filter(_.metric === "sparkOperationDuration")
      .filter(_.opType.isDefined)
      .filter(_.opType.get.typeName.contains("delta.clone"))

    assert(cloneLogs.count(_.opType.get.typeName.equals("delta.clone.makeAbsolute")) == 1)

    val commitStatsUsageRecords = allLogs
      .filter(_.metric === "tahoeEvent")
      .filter(_.tags.get("opType") === Some("delta.commit.stats"))
    assert(commitStatsUsageRecords.length === 1)
    val commitStatsMap = JsonUtils.fromJson[Map[String, Any]](commitStatsUsageRecords.head.blob)
    commitLargeMetricsMap.foreach { case (name, expectedValue) =>
      assert(commitStatsMap(name).toString == expectedValue,
        s"Expected value for $name metrics did not match with the captured value")
    }
  }

  private def compareMetadata(
      cloneSource: CloneSource,
      targetLog: Snapshot,
      targetIsTable: Boolean,
      isReplace: Boolean = false): Unit = {
    val sourceMetadata = cloneSource.metadata
    val targetMetadata = targetLog.metadata

    assert(sourceMetadata.schema === targetMetadata.schema &&
      sourceMetadata.configuration === targetMetadata.configuration &&
      sourceMetadata.dataSchema === targetMetadata.dataSchema &&
      sourceMetadata.partitionColumns === targetMetadata.partitionColumns &&
      sourceMetadata.format === sourceMetadata.format)

    // Protocol should be changed, if source.protocol >= target.protocol, otherwise target must
    // retain it's existing protocol version (i.e. no downgrades).
    assert(cloneSource.protocol === targetLog.protocol || (
      cloneSource.protocol.minReaderVersion <= targetLog.protocol.minReaderVersion &&
        cloneSource.protocol.minWriterVersion <= targetLog.protocol.minWriterVersion))

    assert(targetLog.setTransactions.isEmpty)

    if (!isReplace) {
      assert(sourceMetadata.id != targetMetadata.id &&
        targetMetadata.name === null &&
        targetMetadata.description === null)
    }
  }

  protected def resolveTableIdentifier(
    name: String, format: Option[String], isTable: Boolean): TableIdentifier = {
    if (isTable) {
      TableIdentifier(name)
    } else {
      TableIdentifier(name, format)
    }
  }
}

trait CloneTableSQLTestMixin extends CloneTableTestMixin {
  self: QueryTest with SharedSparkSession =>

  // scalastyle:off argcount
  override protected def cloneTable(
      source: String,
      target: String,
      isShallow: Boolean,
      sourceIsTable: Boolean = false,
      targetIsTable: Boolean = false,
      targetLocation: Option[String] = None,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None,
      isCreate: Boolean = true,
      isReplace: Boolean = false,
      tableProperties: Map[String, String] = Map.empty): Unit = {
    val commandSql = CloneTableSQLTestUtils.buildCloneSqlString(
      source, target,
      sourceIsTable,
      targetIsTable,
      "delta",
      targetLocation,
      versionAsOf,
      timestampAsOf,
      isCreate,
      isReplace,
      tableProperties)
    sql(commandSql)
  }
  // scalastyle:on argcount
}

trait CloneTableScalaTestMixin extends CloneTableTestMixin {
  self: QueryTest with SharedSparkSession =>

  // scalastyle:off argcount
  override protected def cloneTable(
      source: String,
      target: String,
      isShallow: Boolean,
      sourceIsTable: Boolean = false,
      targetIsTable: Boolean = false,
      targetLocation: Option[String] = None,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None,
      isCreate: Boolean = true,
      isReplace: Boolean = false,
      tableProperties: Map[String, String] = Map.empty): Unit = {
    val table = if (sourceIsTable) {
      io.delta.tables.DeltaTable.forName(spark, source)
    } else {
      io.delta.tables.DeltaTable.forPath(spark, source)
    }

    if (versionAsOf.isDefined) {
      table.cloneAtVersion(versionAsOf.get,
        target, isShallow = isShallow, replace = isReplace, tableProperties)
    } else if (timestampAsOf.isDefined) {
      table.cloneAtTimestamp(timestampAsOf.get,
        target, isShallow = isShallow, replace = isReplace, tableProperties)
    } else {
      table.clone(target, isShallow = isShallow, replace = isReplace, tableProperties)
    }
  }
  // scalastyle:on argcount
}
