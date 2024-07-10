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

// scalastyle:off import.ordering.noEmptyLine
import java.io.File
import java.net.URI
import java.util.Locale

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions.{FileAction, Metadata, Protocol, SetTransaction, SingleAction, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands._
import org.apache.spark.sql.delta.coordinatedcommits._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaColumnMappingSelectedTestMixin, DeltaSQLCommandTest}
import org.apache.spark.sql.delta.util.FileNames.{checksumFile, unsafeDeltaFile}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
import org.scalatest.Tag

import org.apache.spark.{DebugFilesystem, SparkException, TaskFailedReason}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.streaming.{CheckpointFileManager, FileSystemBasedCheckpointFileManager, MemoryStream}
import org.apache.spark.sql.functions.{col, floor, from_json}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.util.Utils
// scalastyle:on import.ordering.noEmptyLine

trait CloneTableSuiteBase extends QueryTest
  with SharedSparkSession
  with DeltaColumnMappingTestUtils
  with DeltaSQLCommandTest
  with CoordinatedCommitsBaseSuite
  with CoordinatedCommitsTestUtils {

  protected val TAG_HAS_SHALLOW_CLONE = new Tag("SHALLOW CLONE")
  protected val TAG_MODIFY_PROTOCOL = new Tag("CHANGES PROTOCOL")
  protected val TAG_CHANGE_COLUMN_MAPPING_MODE = new Tag("CHANGES COLUMN MAPPING MODE")
  protected val TAG_USES_CONVERT_TO_DELTA = new Tag("USES CONVERT TO DELTA")

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
  protected def uniqueFileActionGroupBy(action: FileAction): String = action.pathAsUri.toString

  import testImplicits._
  // scalastyle:off
  protected def runAndValidateClone(
      source: String,
      target: String,
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
      val deltaTable = DeltaTableV2(spark, sourceLog.dataPath, None, None, timeTravelSpec)
      val sourceData = Dataset.ofRows(
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
    val targetDf = Dataset.ofRows(
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
      .filter(
        _.tags.get("opType") === Some("delta.commit.stats"))
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

  protected def deleteSourceAndCompareData(
      source: String,
      actual: => DataFrame,
      expected: DataFrame): Unit = {
    Utils.deleteRecursively(new File(source))
    checkAnswer(actual, expected)
  }

  // scalastyle:off argcount
  protected def cloneTable(
      source: String,
      target: String,
      sourceIsTable: Boolean = false,
      targetIsTable: Boolean = false,
      targetLocation: Option[String] = None,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None,
      isCreate: Boolean = true,
      isReplace: Boolean = false,
      tableProperties: Map[String, String] = Map.empty): Unit
  // scalastyle:on argcount

  protected def verifyAllFilePaths(
      table: String,
      targetIsTable: Boolean = false,
      expectAbsolute: Boolean): Unit = {
    val targetLog = if (targetIsTable) {
      DeltaLog.forTable(spark, TableIdentifier(table))
    } else {
      DeltaLog.forTable(spark, table)
    }
    assert(targetLog.unsafeVolatileSnapshot.allFiles.collect()
          .forall(p => new Path(p.pathAsUri).isAbsolute == expectAbsolute))
  }

  protected def customConvertToDelta(internal: String, external: String): Unit = {
    ConvertToDeltaCommand(
      TableIdentifier(external, Some("parquet")),
      Option(new StructType().add("part", IntegerType)),
      collectStats = true,
      Some(internal)).run(spark)
  }

  protected def resolveTableIdentifier(
    name: String, format: Option[String], isTable: Boolean): TableIdentifier = {
    if (isTable) {
      TableIdentifier(name)
    } else {
      TableIdentifier(name, format)
    }
  }

   // Test a basic clone with different syntaxes
  protected def testSyntax(
      source: String,
      target: String,
      sqlString: String,
      targetIsTable: Boolean = false): Unit = {
    withTable(source) {
      spark.range(5).write.format("delta").saveAsTable(source)
      runAndValidateClone(
        source,
        target,
        sourceIsTable = true,
        targetIsTable = targetIsTable) {
        () => sql(sqlString)
      }
    }
  }

  cloneTest("simple shallow clone", TAG_HAS_SHALLOW_CLONE) { (source, clone) =>
    val df1 = Seq(1, 2, 3, 4, 5).toDF("id").withColumn("part", 'id % 2)
    val df2 = Seq(8, 9, 10).toDF("id").withColumn("part", 'id % 2)
    df1.write.format("delta").partitionBy("part").mode("append").save(source)
    df2.write.format("delta").mode("append").save(source)

    runAndValidateClone(
      source,
      clone
    )()
    // no files should be copied
    val cloneDir = new File(clone).list()
    assert(cloneDir.length === 1,
      s"There should only be a _delta_log directory but found:\n${cloneDir.mkString("\n")}")

    val cloneLog = DeltaLog.forTable(spark, clone)
    assert(cloneLog.snapshot.version === 0)
    assert(cloneLog.snapshot.metadata.partitionColumns === Seq("part"))
    val files = cloneLog.snapshot.allFiles.collect()
    assert(files.forall(_.pathAsUri.toString.startsWith("file:/")), "paths must be absolute")

    checkAnswer(
      spark.read.format("delta").load(clone),
      df1.union(df2)
    )
  }

  cloneTest("shallow clone a shallow clone", TAG_HAS_SHALLOW_CLONE) { (source, clone) =>
    val shallow1 = new File(clone, "shallow1").getCanonicalPath
    val shallow2 = new File(clone, "shallow2").getCanonicalPath
    val df1 = Seq(1, 2, 3, 4, 5).toDF("id").withColumn("part", 'id % 2)
    df1.write.format("delta").partitionBy("part").mode("append").save(source)

    runAndValidateClone(
      source,
      shallow1
    )()

    runAndValidateClone(
      shallow1,
      shallow2
    )()

    deleteSourceAndCompareData(shallow1, spark.read.format("delta").load(shallow2), df1)
  }

  testAllClones(s"validate commitLarge usage metrics") { (source, clone, isShallow) =>
    val df1 = Seq(1, 2, 3, 4, 5).toDF("id").withColumn("part", 'id % 5)
    df1.write.format("delta").partitionBy("part").mode("append").save(source)
    val df2 = Seq(1, 2).toDF("id").withColumn("part", 'id % 5)
    df2.write.format("delta").partitionBy("part").mode("append").save(source)

    val numAbsolutePathsInAdd = if (isShallow) 7 else 0
    val commitLargeMetricsMap = Map(
      "numAdd" -> "7",
      "numRemove" -> "0",
      "numFilesTotal" -> "7",
      "numCdcFiles" -> "0",
      "commitVersion" -> "0",
      "readVersion" -> "0",
      "numAbsolutePathsInAdd" -> s"$numAbsolutePathsInAdd",
      "startVersion" -> "-1",
      "numDistinctPartitionsInAdd" -> "-1") // distinct Parts are not tracked in commitLarge flow
    runAndValidateClone(
      source,
      clone,
      commitLargeMetricsMap = commitLargeMetricsMap)()

    checkAnswer(
      spark.read.format("delta").load(clone),
      df1.union(df2)
    )
  }

  cloneTest("shallow clone across file systems", TAG_HAS_SHALLOW_CLONE) { (source, clone) =>
    withSQLConf(
        "fs.s3.impl" -> classOf[S3LikeLocalFileSystem].getName,
        "fs.s3.impl.disable.cache" -> "true") {
      val df1 = Seq(1, 2, 3, 4, 5).toDF("id")
      df1.write.format("delta").mode("append").save(s"s3:$source")

      runAndValidateClone(
        s"s3:$source",
        s"file:$clone"
      )()

      checkAnswer(
        spark.read.format("delta").load(clone),
        df1
      )

      val cloneLog = DeltaLog.forTable(spark, clone)
      assert(cloneLog.snapshot.version === 0)
      val files = cloneLog.snapshot.allFiles.collect()
      assert(files.forall(_.pathAsUri.toString.startsWith("s3:/")))
    }
  }

  testAllClones("Negative test: clone into a non-empty directory that has a path based " +
    "delta table") { (source, clone, isShallow) =>
    // Create table to clone
    spark.range(5).write.format("delta").mode("append").save(source)

    // Table already exists at destination directory
    spark.range(5).write.format("delta").mode("append").save(clone)

    // Clone should fail since destination directory is non-empty
    val ex = intercept[AnalysisException] {
      runAndValidateClone(
        source,
        clone
      )()
    }
    assert(ex.getMessage.contains("is not empty"))
  }

  cloneTest("Negative test: cloning into a non-empty parquet directory",
      TAG_HAS_SHALLOW_CLONE) { (source, clone) =>
    // Create table to clone
    spark.range(5).write.format("delta").mode("append").save(source)

    // Table already exists at destination directory
    spark.range(5).write.format("parquet").mode("overwrite").save(clone)

    // Clone should fail since destination directory is non-empty
    val ex = intercept[AnalysisException] {
      sql(s"CREATE TABLE delta.`$clone` SHALLOW CLONE delta.`$source`")
    }
    assert(ex.getMessage.contains("is not empty and also not a Delta table"))
  }

  testAllClones(
    "Changes to clones only affect the cloned directory") { (source, target, isShallow) =>
    // Create base directory
    Seq(1, 2, 3, 4, 5).toDF("id").write.format("delta").save(source)

    // Create a clone
    runAndValidateClone(
      source,
      target
    )()

    // Write to clone should be visible
    Seq(6, 7, 8).toDF("id").write.format("delta").mode("append").save(target)
    assert(spark.read.format("delta").load(target).count() === 8)

    // Write to clone should not be visible in original table
    assert(spark.read.format("delta").load(source).count() === 5)
  }

  testAllClones("simple clone of source using table name") { (_, target, isShallow) =>
    val tableName = "source"
    withTable(tableName) {
      spark.range(5).write.format("delta").saveAsTable(tableName)
      runAndValidateClone(
        tableName,
        target,
        sourceIsTable = true)()
    }
  }

  testAllClones("Clone a time traveled source") { (_, target, isShallow) =>
    val tableName = "source"
    withTable(tableName) {
      spark.range(5).write.format("delta").saveAsTable(tableName)
      spark.range(5).write.format("delta").mode("append").saveAsTable(tableName)
      spark.range(5).write.format("delta").mode("append").saveAsTable(tableName)
      spark.range(5).write.format("delta").mode("append").saveAsTable(tableName)
      assert(DeltaLog.forTableWithSnapshot(spark, TableIdentifier(tableName))._2.version === 3)

      runAndValidateClone(
        tableName,
        target,
        sourceIsTable = true,
        sourceVersion = Some(2))()
      assert(spark.read.format("delta").load(target).count() === 15)
    }
  }

  Seq(true, false).foreach { isCreate =>
    cloneTest(s"create or replace table - shallow, isCreate: $isCreate",
        TAG_HAS_SHALLOW_CLONE) { (_, _) =>
      val tbl = "source"
      val target = "target"
      withTable(tbl, target) {
        spark.range(5).write.format("delta").saveAsTable(tbl)
        spark.range(25).write.format("delta").saveAsTable(target)

        runAndValidateClone(
          tbl,
          target,
          sourceIsTable = true,
          targetIsTable = true,
          isCreate = isCreate,
          isReplaceOperation = true)()
      }
    }
  }

  Seq(true, false).foreach { isCreate =>
    Seq("parquet", "json").foreach { format =>
      cloneTest(s"create or replace non Delta table - shallow, isCreate: $isCreate, " +
          s"format: $format", TAG_HAS_SHALLOW_CLONE) { (_, _) =>
        val tbl = "source"
        val target = "target"
        withTable(tbl, target) {
          spark.range(5).write.format("delta").saveAsTable(tbl)
          spark.range(25).write.format(format).saveAsTable(target)

          runAndValidateClone(
            tbl,
            target,
            sourceIsTable = true,
            targetIsTable = true,
            isCreate = isCreate,
            isReplaceOperation = true,
            isReplaceDelta = false)()
        }
      }
    }
  }

  Seq(true, false).foreach { isCreate =>
    cloneTest(s"shallow clone a table unto itself, isCreate: $isCreate",
        TAG_HAS_SHALLOW_CLONE) { (_, _) =>
      val tbl = "source"
      withTable(tbl) {
        spark.range(5).write.format("delta").saveAsTable(tbl)

        runAndValidateClone(
          tbl,
          tbl,
          sourceIsTable = true,
          targetIsTable = true,
          isCreate = isCreate,
          isReplaceOperation = true)()

        val allFiles =
          DeltaLog.forTableWithSnapshot(spark, TableIdentifier(tbl))._2.allFiles.collect()
        allFiles.foreach { file =>
          assert(!file.pathAsUri.isAbsolute, "File paths should not be absolute")
        }
      }
    }
  }

  cloneTest("CLONE ignores reader/writer session defaults", TAG_HAS_SHALLOW_CLONE) {
    (source, clone) =>
      if (coordinatedCommitsEnabledInTests) {
        cancel("Expects base protocol version")
      }
      withSQLConf(
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "1",
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "1") {
        // Create table without a default property setting.
        spark.range(1L).write.format("delta").mode("overwrite").save(source)
        val oldProtocol = DeltaLog.forTable(spark, source).update().protocol
        assert(oldProtocol === Protocol(1, 1))
        // Just use something that can be default.
        withSQLConf(
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "2",
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "2",
          TableFeatureProtocolUtils.defaultPropertyKey(TestWriterFeature) -> "enabled") {
          // Clone in a session with default properties and check that they aren't merged
          // (i.e. target properties are identical to source properties).
          runAndValidateClone(
            source,
            clone
          )()
        }

        val log = DeltaLog.forTable(spark, clone)
        val targetProtocol = log.update().protocol
        assert(targetProtocol === oldProtocol)
      }
  }

  testAllClones("clone a time traveled source using timestamp") { (source, clone, isShallow) =>
    // Create source
    spark.range(5).write.format("delta").save(source)
    spark.range(5).write.format("delta").mode("append").save(source)
    spark.range(5).write.format("delta").mode("append").save(source)
    assert(spark.read.format("delta").load(source).count() === 15)

    // Get time corresponding to date
    val desiredTime = "1996-01-12"
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val time = format.parse(desiredTime).getTime

    // Change modification time of commit
    val path = new Path(source + "/_delta_log/00000000000000000000.json")
    // scalastyle:off deltahadoopconfiguration
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    fs.setTimes(path, time, 0)
    if (coordinatedCommitsEnabledInTests) {
      InCommitTimestampTestUtils.overwriteICTInDeltaFile(
        DeltaLog.forTable(spark, source),
        path,
        Some(time))
    }

    runAndValidateClone(
      source,
      clone,
      sourceTimestamp = Some(desiredTime))()
  }

  cloneTest("clones take protocol from the source",
    TAG_HAS_SHALLOW_CLONE, TAG_MODIFY_PROTOCOL, TAG_CHANGE_COLUMN_MAPPING_MODE) { (source, clone) =>
    // Change protocol versions of (read, write) = (2, 3). We cannot initialize this to (0, 0)
    // because min reader and writer versions are at least 1.
    val defaultNewTableProtocol = Protocol.forNewTable(spark, metadataOpt = None)
    val sourceProtocol = Protocol(2, 3)
    // Make sure this is actually an upgrade. Downgrades are not supported, and if it's the same
    // version, we aren't testing anything there.
    assert(sourceProtocol.minWriterVersion > defaultNewTableProtocol.minWriterVersion &&
      sourceProtocol.minReaderVersion > defaultNewTableProtocol.minReaderVersion)
    val log = DeltaLog.forTable(spark, source)
    // make sure to have a dummy schema because we can't have empty schema table by default
    val newSchema = new StructType().add("id", IntegerType, nullable = true)
    log.createLogDirectoriesIfNotExists()
    log.store.write(
      unsafeDeltaFile(log.logPath, 0),
      Iterator(Metadata(schemaString = newSchema.json).json, sourceProtocol.json),
      overwrite = false,
      log.newDeltaHadoopConf())
    log.update()

    // Validate that clone has the new protocol version
    runAndValidateClone(
      source,
      clone
    )()
  }

  testAllClones("clones take the set transactions of the source") { (_, target, isShallow) =>
    withTempDir { dir =>
      // Create source
      val path = dir.getCanonicalPath
      spark.range(5).write.format("delta").save(path)

      // Add a Set Transaction
      val log = DeltaLog.forTable(spark, path)
      val txn = log.startTransaction()
      val setTxn = SetTransaction("app-id", 0, Some(0L)) :: Nil
      val op = DeltaOperations.StreamingUpdate(OutputMode.Complete(), "app-id", 0L)
      txn.commit(setTxn, op)
      log.update()

      runAndValidateClone(
        path,
        target
      )()
    }
  }

  testAllClones("CLONE with table properties to disable DV") { (source, target, isShallow) =>
    withSQLConf(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true") {
      spark.range(10).write.format("delta").save(source)
      spark.sql(s"DELETE FROM delta.`$source` WHERE id = 1")
    }
    intercept[DeltaCommandUnsupportedWithDeletionVectorsException] {
      runAndValidateClone(
        source,
        target,
        tableProperties = Map(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key -> "false"))()
    }.getErrorClass === "DELTA_ADDING_DELETION_VECTORS_DISALLOWED"
  }

  for(targetExists <- BOOLEAN_DOMAIN)
  testAllClones(s"CLONE respects table features set by table property override, " +
    s"targetExists=$targetExists", TAG_MODIFY_PROTOCOL) {
    (source, target, isShallow) =>
      spark.range(10).write.format("delta").save(source)

      if (targetExists) {
        spark.range(0).write.format("delta").save(target)
      }

      val tblPropertyOverrides =
        Seq(
          s"delta.feature.${TestWriterFeature.name}" -> "enabled",
          "delta.minWriterVersion" -> s"$TABLE_FEATURES_MIN_WRITER_VERSION").toMap
      cloneTable(
        source,
        target,
        isReplace = true,
        tableProperties = tblPropertyOverrides)

      val targetLog = DeltaLog.forTable(spark, target)
      assert(targetLog.update().protocol.isFeatureSupported(TestWriterFeature))
  }

  case class TableFeatureWithProperty(
      feature: TableFeature,
      property: DeltaConfig[Boolean])

  // Delta properties that automatically cause a version upgrade when enabled via ALTER TABLE.
  final val featuresWithAutomaticProtocolUpgrade: Seq[TableFeatureWithProperty] = Seq(
    TableFeatureWithProperty(ChangeDataFeedTableFeature, DeltaConfigs.CHANGE_DATA_FEED),
    TableFeatureWithProperty(
      DeletionVectorsTableFeature, DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION))
  // This test ensures this upgrade also happens when enabled during a CLONE.
  for (featureWithProperty <- featuresWithAutomaticProtocolUpgrade)
    testAllClones("Cloning a table with new table properties" +
      s" that force protocol version upgrade - ${featureWithProperty.property.key}"
    ) { (source, target, isShallow) =>
      if (coordinatedCommitsEnabledInTests) {
        cancel("table needs to start with default protocol versions but enabling " +
          "coordinatedCommits upgrades table protocol version.")
      }
      import DeltaTestUtils.StrictProtocolOrdering

      spark.range(5).write.format("delta").save(source)
      val sourceDeltaLog = DeltaLog.forTable(spark, source)
      val sourceSnapshot = sourceDeltaLog.update()
      // This only works if the featureWithProperty is not enabled by default.
      assert(!featureWithProperty.property.fromMetaData(sourceSnapshot.metadata))
      // Check that the original version is not already sufficient for the featureWithProperty.
      assert(!StrictProtocolOrdering.fulfillsVersionRequirements(
        actual = sourceSnapshot.protocol,
        requirement = featureWithProperty.feature.minProtocolVersion
      ))

      // Clone the table, enabling the featureWithProperty in an override.
      val tblProperties = Map(featureWithProperty.property.key -> "true")
      cloneTable(
        source,
        target,
        isReplace = true,
        tableProperties = tblProperties)

      val targetDeltaLog = DeltaLog.forTable(spark, target)
      val targetSnapshot = targetDeltaLog.update()
      assert(targetSnapshot.metadata.configuration ===
        tblProperties ++ sourceSnapshot.metadata.configuration)
      // Check that the protocol has been upgraded.
      assert(StrictProtocolOrdering.fulfillsVersionRequirements(
        actual = targetSnapshot.protocol,
        requirement = featureWithProperty.feature.minProtocolVersion
      ))
    }

  testAllClones("Cloning a table without DV property should not upgrade protocol version"
  ) { (source, target, isShallow) =>
    if (coordinatedCommitsEnabledInTests) {
      cancel("table needs to start with default protocol versions but enabling " +
        "coordinatedCommits upgrades table protocol version.")
    }
    import DeltaTestUtils.StrictProtocolOrdering

    spark.range(5).write.format("delta").save(source)
    withSQLConf(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true") {
      val sourceDeltaLog = DeltaLog.forTable(spark, source)
      val sourceSnapshot = sourceDeltaLog.update()
      // Should not be enabled, just because it's allowed.
      assert(!DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.fromMetaData(sourceSnapshot.metadata))
      // Check that the original version is not already sufficient for the feature.
      assert(!StrictProtocolOrdering.fulfillsVersionRequirements(
        actual = sourceSnapshot.protocol,
        requirement = DeletionVectorsTableFeature.minProtocolVersion
      ))

      // Clone the table.
      cloneTable(
        source,
        target,
        isReplace = true)

      val targetDeltaLog = DeltaLog.forTable(spark, target)
      val targetSnapshot = targetDeltaLog.update()
      // Protocol should not have been upgraded.
      assert(sourceSnapshot.protocol === targetSnapshot.protocol)
    }
  }

  testAllClones(s"Clone should pick commit coordinator from session settings") {
      (source, target, isShallow) =>
    val someOtherCommitCoordinatorName = "some-other-commit-coordinator"
    case class SomeValidCommitCoordinatorBuilder() extends CommitCoordinatorBuilder {
      val commitCoordinator = new InMemoryCommitCoordinator(batchSize = 1000L)
      override def getName: String = someOtherCommitCoordinatorName
      override def build(spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient =
        commitCoordinator
    }
    CommitCoordinatorProvider.registerBuilder(SomeValidCommitCoordinatorBuilder())

    // Create source table
    val sourceLog = DeltaLog.forTable(spark, source)
    spark.range(5).write.format("delta").save(source)
    spark.range(5, 10).write.format("delta").mode("append").save(source)
    spark.range(10, 15).write.format("delta").mode("append").save(source)

    // CLONE should pick the commit coordinator from the session settings over the
    // commit coordinator from the source table.
    withCustomCoordinatedCommitsTableProperties(someOtherCommitCoordinatorName) {
      cloneTable(
        source,
        target,
        isReplace = true)
      val targetLog = DeltaLog.forTable(spark, target)
      val targetCommitCoordinator =
        targetLog.update().tableCommitCoordinatorClientOpt.get.commitCoordinatorClient
      assert(targetCommitCoordinator.asInstanceOf[InMemoryCommitCoordinator].batchSize == 1000)
    }
    checkAnswer(
      spark.read.format("delta").load(source),
      spark.read.format("delta").load(target))
  }

  testAllClones(s"Clone should ignore commit coordinator if it is not set in session settings") {
      (source, target, isShallow) =>
    // Create source table
    val sourceLog = DeltaLog.forTable(spark, source)
    spark.range(5).write.format("delta").save(source)
    spark.range(5, 10).write.format("delta").mode("append").save(source)
    spark.range(10, 15).write.format("delta").mode("append").save(source)

    // Commit-Coordinator for target should not be set because it is unset in session.
    withoutCoordinatedCommitsDefaultTableProperties {
      cloneTable(
        source,
        target,
        isReplace = true)
      val targetLog = DeltaLog.forTable(spark, target)
      assert(targetLog.update().tableCommitCoordinatorClientOpt.isEmpty)
    }
    checkAnswer(
      spark.read.format("delta").load(source),
      spark.read.format("delta").load(target))
  }

  testAllClones(s"Clone should give highest priority to commit coordinator specified directly in " +
      s"clone command") { (source, target, isShallow) =>
    val someOtherCommitCoordinatorName1 = "some-other-commit-coordinator-1"
    case class SomeValidCommitCoordinatorBuilder1() extends CommitCoordinatorBuilder {
      val commitCoordinator = new InMemoryCommitCoordinator(batchSize = 1000L)
      override def getName: String = someOtherCommitCoordinatorName1
      override def build(spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient =
        commitCoordinator
    }
    CommitCoordinatorProvider.registerBuilder(SomeValidCommitCoordinatorBuilder1())
    val someOtherCommitCoordinatorName2 = "some-other-commit-coordinator-2"
    case class SomeValidCommitCoordinatorBuilder2() extends CommitCoordinatorBuilder {
      val commitCoordinator = new InMemoryCommitCoordinator(batchSize = 2000L)
      override def getName: String = someOtherCommitCoordinatorName2
      override def build(spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient =
        commitCoordinator
    }
    CommitCoordinatorProvider.registerBuilder(SomeValidCommitCoordinatorBuilder2())

    // Create source table
    val sourceLog = DeltaLog.forTable(spark, source)
    spark.range(5).write.format("delta").save(source)
    spark.range(5, 10).write.format("delta").mode("append").save(source)
    spark.range(10, 15).write.format("delta").mode("append").save(source)

    // When commit-coordinator is specified in both the spark session (with batchSize=1000 here)
    // and CLONE command overrides (with batchSize=2000 here, CLONE should give priority to
    // properties explicitly overridden with the CLONE command.
    withCustomCoordinatedCommitsTableProperties(someOtherCommitCoordinatorName1) {
      val e1 = intercept[IllegalArgumentException] {
        val properties = Map(
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key ->
            someOtherCommitCoordinatorName2)
        cloneTable(
          source,
          target,
          isReplace = true,
          tableProperties = properties)
      }
      assert(e1.getMessage.contains("During CLONE, either all coordinated commits " +
        "configurations i.e.\"delta.coordinatedCommits.commitCoordinator-preview\", " +
        "\"delta.coordinatedCommits.commitCoordinatorConf-preview\" must be overridden or none " +
        "of them."))

      val e2 = intercept[IllegalArgumentException] {
        val properties = Map(
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key ->
            someOtherCommitCoordinatorName2,
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.key -> "{}",
          DeltaConfigs.COORDINATED_COMMITS_TABLE_CONF.key -> "{}")
        cloneTable(
          source,
          target,
          isReplace = true,
          tableProperties = properties)
      }
      assert(e2.getMessage.contains("Configuration \"delta.coordinatedCommits.tableConf-preview\"" +
        " cannot be overridden with CLONE command."))

      val properties = Map(
        DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key ->
          someOtherCommitCoordinatorName2,
        DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.key -> "{}")
      cloneTable(
        source,
        target,
        isReplace = true,
        tableProperties = properties)

      val targetLog = DeltaLog.forTable(spark, target)
      val targetCommitStore =
        targetLog.update().tableCommitCoordinatorClientOpt.get.commitCoordinatorClient
      assert(targetCommitStore.asInstanceOf[InMemoryCommitCoordinator].batchSize == 2000L)
    }
    checkAnswer(
      spark.read.format("delta").load(source),
      spark.read.format("delta").load(target))
  }
}


trait CloneTableColumnMappingSuiteBase
  extends CloneTableSuiteBase
    with DeltaColumnMappingSelectedTestMixin
{

  override protected def runOnlyTests: Seq[String] = Seq(
    "simple shallow clone",
    "shallow clone a shallow clone",
    "create or replace table - shallow, isCreate: false",
    "create or replace table - shallow, isCreate: true",
    "shallow clone a table unto itself, isCreate: false",
    "shallow clone a table unto itself, isCreate: true",
    "Clone a time traveled source",

    "validate commitLarge usage metrics",
    "clones take the set transactions of the source",
    "block changing column mapping mode and modify max id modes under CLONE"
  )

  import testImplicits._

  testAllClones("block changing column mapping mode and modify max id modes under CLONE") {
    (_, _, isShallow) =>
      val df1 = Seq(1, 2, 3, 4, 5).toDF("id").withColumn("part", 'id % 2)

      // block setting max id
      def validateModifyMaxIdError(f: => Any): Unit = {
        val e = intercept[UnsupportedOperationException] { f }
        assert(e.getMessage == DeltaErrors.cannotModifyTableProperty(
          DeltaConfigs.COLUMN_MAPPING_MAX_ID.key
        ).getMessage)
      }

      withSourceTargetDir { (source, target) =>
        df1.write.format("delta").partitionBy("part").mode("append").save(source)
        // change max id w/ table property should be blocked
        validateModifyMaxIdError {
          cloneTable(
            source,
            target,
            tableProperties = Map(
              DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> "123123"
          ))
        }
        // change max id w/ SQLConf should be blocked by table property guard
        validateModifyMaxIdError {
          withMaxColumnIdConf("123123") {
            cloneTable(
              source,
              target
            )
          }
        }
      }

      // block changing column mapping mode
      def validateChangeModeError(f: => Any): Unit = {
        val e = intercept[ColumnMappingUnsupportedException] { f }
        assert(e.getMessage.contains("Changing column mapping mode from"))
      }

      val currentMode = columnMappingModeString

      // currentMode to otherMode
      val otherMode = if (currentMode == "id") "name" else "id"
      withSourceTargetDir { (source, target) =>
        df1.write.format("delta").partitionBy("part").mode("append").save(source)
        // change mode w/ table property should be blocked
        validateChangeModeError {
          cloneTable(
            source,
            target,
            tableProperties = Map(
              DeltaConfigs.COLUMN_MAPPING_MODE.key -> otherMode
          ))
        }
      }

      withSourceTargetDir { (source, target) =>
        df1.write.format("delta").partitionBy("part").mode("append").save(source)
        // change mode w/ SQLConf should have no effects
        withColumnMappingConf(otherMode) {
          cloneTable(
            source,
            target
          )
        }
        assert(DeltaLog.forTable(spark, target).snapshot.metadata.columnMappingMode.name ==
          currentMode)
      }

      // currentMode to none
      withSourceTargetDir { (source, target) =>
        df1.write.format("delta").partitionBy("part").mode("append").save(source)
        // change mode w/ table property
        validateChangeModeError {
          cloneTable(
            source,
            target,
            tableProperties = Map(
              DeltaConfigs.COLUMN_MAPPING_MODE.key -> "none"
          ))
        }
      }
      withSourceTargetDir { (source, target) =>
        df1.write.format("delta").partitionBy("part").mode("append").save(source)
        // change mode w/ SQLConf should have no effects
        withColumnMappingConf("none") {
          cloneTable(
            source,
            target
          )
        }
        assert(DeltaLog.forTable(spark, target).snapshot.metadata.columnMappingMode.name ==
          currentMode)
      }
  }
}

trait CloneTableColumnMappingNameSuiteBase extends CloneTableColumnMappingSuiteBase {
  override protected def customConvertToDelta(internal: String, external: String): Unit = {
    withColumnMappingConf("none") {
      super.customConvertToDelta(internal, external)
      sql(
        s"""ALTER TABLE delta.`$internal` SET TBLPROPERTIES (
           |${DeltaConfigs.COLUMN_MAPPING_MODE.key} = 'name',
           |${DeltaConfigs.MIN_READER_VERSION.key} = '2',
           |${DeltaConfigs.MIN_WRITER_VERSION.key} = '5'
           |)""".stripMargin)
        .collect()
    }
  }
}
