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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.reflect.ClassTag
import scala.util.matching.Regex

import org.apache.spark.sql.delta.DeltaTestUtils.Plans
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames
import io.delta.tables.{DeltaTable => IODeltaTable}
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkContext
import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler.{JobFailed, SparkListener, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, RDDScanExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.util.Utils

trait DeltaTestUtilsBase {
  import DeltaTestUtils.TableIdentifierOrPath

  final val BOOLEAN_DOMAIN: Seq[Boolean] = Seq(true, false)

  class PlanCapturingListener() extends QueryExecutionListener {

    private[this] var capturedPlans = List.empty[Plans]

    def plans: Seq[Plans] = capturedPlans.reverse

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      capturedPlans ::= Plans(
          qe.analyzed,
          qe.optimizedPlan,
          qe.sparkPlan,
          qe.executedPlan)
    }

    override def onFailure(
      funcName: String, qe: QueryExecution, error: Exception): Unit = {}
  }

  /**
   * Run a thunk with physical plans for all queries captured and passed into a provided buffer.
   */
  def withLogicalPlansCaptured[T](
      spark: SparkSession,
      optimizedPlan: Boolean)(
      thunk: => Unit): Seq[LogicalPlan] = {
    val planCapturingListener = new PlanCapturingListener

    spark.sparkContext.listenerBus.waitUntilEmpty(15000)
    spark.listenerManager.register(planCapturingListener)
    try {
      thunk
      spark.sparkContext.listenerBus.waitUntilEmpty(15000)
      planCapturingListener.plans.map { plans =>
        if (optimizedPlan) plans.optimized else plans.analyzed
      }
    } finally {
      spark.listenerManager.unregister(planCapturingListener)
    }
  }

  /**
   * Run a thunk with physical plans for all queries captured and passed into a provided buffer.
   */
  def withPhysicalPlansCaptured[T](
      spark: SparkSession)(
      thunk: => Unit): Seq[SparkPlan] = {
    val planCapturingListener = new PlanCapturingListener

    spark.sparkContext.listenerBus.waitUntilEmpty(15000)
    spark.listenerManager.register(planCapturingListener)
    try {
      thunk
      spark.sparkContext.listenerBus.waitUntilEmpty(15000)
      planCapturingListener.plans.map(_.sparkPlan)
    } finally {
      spark.listenerManager.unregister(planCapturingListener)
    }
  }

  /**
   * Run a thunk with logical and physical plans for all queries captured and passed
   * into a provided buffer.
   */
  def withAllPlansCaptured[T](
      spark: SparkSession)(
      thunk: => Unit): Seq[Plans] = {
    val planCapturingListener = new PlanCapturingListener

    spark.sparkContext.listenerBus.waitUntilEmpty(15000)
    spark.listenerManager.register(planCapturingListener)
    try {
      thunk
      spark.sparkContext.listenerBus.waitUntilEmpty(15000)
      planCapturingListener.plans
    } finally {
      spark.listenerManager.unregister(planCapturingListener)
    }
  }

  def countSparkJobs(sc: SparkContext, f: => Unit): Int = {
    val jobs: concurrent.Map[Int, Long] = new ConcurrentHashMap[Int, Long]().asScala
    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        jobs.put(jobStart.jobId, jobStart.stageInfos.map(_.numTasks).sum)
      }
      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = jobEnd.jobResult match {
        case JobFailed(_) => jobs.remove(jobEnd.jobId)
        case _ => // On success, do nothing.
      }
    }
    sc.addSparkListener(listener)
    try {
      sc.listenerBus.waitUntilEmpty(15000)
      f
      sc.listenerBus.waitUntilEmpty(15000)
    } finally {
      sc.removeSparkListener(listener)
    }
    // Spark will always log a job start/end event even when the job does not launch any task.
    jobs.values.count(_ > 0)
  }

  protected def getfindTouchedFilesJobPlans(plans: Seq[Plans]): SparkPlan = {
    // The expected plan for touched file computation is of the format below.
    // The data column should be pruned from both leaves.
    // HashAggregate(output=[count#3463L])
    // +- HashAggregate(output=[count#3466L])
    //   +- Project
    //      +- Filter (isnotnull(count#3454L) AND (count#3454L > 1))
    //         +- HashAggregate(output=[count#3454L])
    //            +- HashAggregate(output=[_row_id_#3418L, sum#3468L])
    //               +- Project [_row_id_#3418L, UDF(_file_name_#3422) AS one#3448]
    //                  +- BroadcastHashJoin [id#3342L], [id#3412L], Inner, BuildLeft
    //                     :- Project [id#3342L]
    //                     :  +- Filter isnotnull(id#3342L)
    //                     :     +- FileScan parquet [id#3342L,part#3343L]
    //                     +- Filter isnotnull(id#3412L)
    //                        +- Project [...]
    //                           +- Project [...]
    //                             +- FileScan parquet [id#3412L,part#3413L]
    // Note: It can be RDDScanExec instead of FileScan if the source was materialized.
    // We pick the first plan starting from FileScan and ending in HashAggregate as a
    // stable heuristic for the one we want.
    plans.map(_.executedPlan)
      .filter {
        case WholeStageCodegenExec(hash: HashAggregateExec) =>
          hash.collectLeaves().size == 2 &&
            hash.collectLeaves()
              .forall { s =>
                s.isInstanceOf[FileSourceScanExec] ||
                  s.isInstanceOf[RDDScanExec]
              }
        case _ => false
      }.head
  }

  /**
   * Separate name- from path-based SQL table identifiers.
   */
  def getTableIdentifierOrPath(sqlIdentifier: String): TableIdentifierOrPath = {
    // Match: delta.`path`[[ as] alias] or tahoe.`path`[[ as] alias]
    val pathMatcher: Regex = raw"(?:delta|tahoe)\.`([^`]+)`(?:(?: as)? (.+))?".r
    // Match: db.table[[ as] alias]
    val qualifiedDbMatcher: Regex = raw"`?([^\.` ]+)`?\.`?([^\.` ]+)`?(?:(?: as)? (.+))?".r
    // Match: table[[ as] alias]
    val unqualifiedNameMatcher: Regex = raw"([^ ]+)(?:(?: as)? (.+))?".r
    sqlIdentifier match {
      case pathMatcher(path, alias) =>
        TableIdentifierOrPath.Path(path, Option(alias))
      case qualifiedDbMatcher(dbName, tableName, alias) =>
        TableIdentifierOrPath.Identifier(TableIdentifier(tableName, Some(dbName)), Option(alias))
      case unqualifiedNameMatcher(tableName, alias) =>
        TableIdentifierOrPath.Identifier(TableIdentifier(tableName), Option(alias))
    }
  }

  /**
   * Produce a DeltaTable instance given a `TableIdentifierOrPath` instance.
   */
  def getDeltaTableForIdentifierOrPath(
      spark: SparkSession,
      identifierOrPath: TableIdentifierOrPath): IODeltaTable = {
    identifierOrPath match {
      case TableIdentifierOrPath.Identifier(id, optionalAlias) =>
        val table = IODeltaTable.forName(spark, id.unquotedString)
        optionalAlias.map(table.as(_)).getOrElse(table)
      case TableIdentifierOrPath.Path(path, optionalAlias) =>
        val table = IODeltaTable.forPath(spark, path)
        optionalAlias.map(table.as(_)).getOrElse(table)
    }
  }

  @deprecated("Use checkError() instead")
  protected def errorContains(errMsg: String, str: String): Unit = {
    assert(errMsg.toLowerCase(Locale.ROOT).contains(str.toLowerCase(Locale.ROOT)))
  }

  /** Utility method to check exception `e` is of type `E` or a cause of it is of type `E` */
  def findIfResponsible[E <: Throwable: ClassTag](e: Throwable): Option[E] = e match {
    case culprit: E => Some(culprit)
    case _ =>
      val children = Option(e.getCause).iterator ++ e.getSuppressed.iterator
      children
        .map(findIfResponsible[E](_))
        .collectFirst { case Some(culprit) => culprit }
  }
}

trait DeltaCheckpointTestUtils
  extends DeltaTestUtilsBase { self: SparkFunSuite with SharedSparkSession =>

  def testDifferentCheckpoints(testName: String)
      (f: (CheckpointPolicy.Policy, Option[V2Checkpoint.Format]) => Unit): Unit = {
    test(s"$testName [Checkpoint V1]") {
      withSQLConf(DeltaConfigs.CHECKPOINT_POLICY.defaultTablePropertyKey ->
        CheckpointPolicy.Classic.name) {
        f(CheckpointPolicy.Classic, None)
      }
    }
    for (checkpointFormat <- V2Checkpoint.Format.ALL)
    test(s"$testName [Checkpoint V2, format: ${checkpointFormat.name}]") {
      withSQLConf(
        DeltaConfigs.CHECKPOINT_POLICY.defaultTablePropertyKey -> CheckpointPolicy.V2.name,
        DeltaSQLConf.CHECKPOINT_V2_TOP_LEVEL_FILE_FORMAT.key -> checkpointFormat.name
      ) {
        f(CheckpointPolicy.V2, Some(checkpointFormat))
      }
    }
  }

  /**
   * Helper method to get the dataframe corresponding to the files which has the file actions for a
   * given checkpoint.
   */
  def getCheckpointDfForFilesContainingFileActions(
      log: DeltaLog,
      checkpointFile: Path): DataFrame = {
    val ci = CheckpointInstance.apply(checkpointFile)
    val allCheckpointFiles = log
        .listFrom(ci.version)
        .filter(FileNames.isCheckpointFile)
        .filter(f => CheckpointInstance(f.getPath) == ci)
        .toSeq
    val fileActionsFileIndex = ci.format match {
      case CheckpointInstance.Format.V2 =>
        val incompleteCheckpointProvider = ci.getCheckpointProvider(log, allCheckpointFiles)
        val df = log.loadIndex(incompleteCheckpointProvider.topLevelFileIndex.get, Action.logSchema)
        val sidecarFileStatuses = df.as[SingleAction].collect().map(_.unwrap).collect {
          case sf: SidecarFile => sf
        }.map(sf => sf.toFileStatus(log.logPath))
        DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, sidecarFileStatuses)
      case CheckpointInstance.Format.SINGLE | CheckpointInstance.Format.WITH_PARTS =>
        DeltaLogFileIndex(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET,
          allCheckpointFiles.toArray)
      case _ =>
        throw new Exception(s"Unexpected checkpoint format for file $checkpointFile")
    }
    fileActionsFileIndex.files
      .map(fileStatus => spark.read.parquet(fileStatus.getPath.toString))
      .reduce(_.union(_))
  }
}

object DeltaTestUtils extends DeltaTestUtilsBase {

  sealed trait TableIdentifierOrPath
  object TableIdentifierOrPath {
    case class Identifier(id: TableIdentifier, alias: Option[String])
      extends TableIdentifierOrPath
    case class Path(path: String, alias: Option[String]) extends TableIdentifierOrPath
  }

  case class Plans(
      analyzed: LogicalPlan,
      optimized: LogicalPlan,
      sparkPlan: SparkPlan,
      executedPlan: SparkPlan)

  /**
   * Creates an AddFile that can be used for tests where the exact parameters do not matter.
   */
  def createTestAddFile(
      path: String = "foo",
      partitionValues: Map[String, String] = Map.empty,
      size: Long = 1L,
      modificationTime: Long = 1L,
      dataChange: Boolean = true,
      stats: String = "{\"numRecords\": 1}"): AddFile = {
    AddFile(path, partitionValues, size, modificationTime, dataChange, stats)
  }

  /**
   * Extracts the table name and alias (if any) from the given string. Correctly handles whitespaces
   * in table name but doesn't support whitespaces in alias.
   */
  def parseTableAndAlias(table: String): (String, Option[String]) = {
    // Matches 'delta.`path` AS alias' (case insensitive).
    val deltaPathWithAsAlias = raw"(?i)(delta\.`.+`)(?: AS) (\S+)".r
    // Matches 'delta.`path` alias'.
    val deltaPathWithAlias = raw"(delta\.`.+`) (\S+)".r
    // Matches 'delta.`path`'.
    val deltaPath = raw"(delta\.`.+`)".r
    // Matches 'tableName AS alias' (case insensitive).
    val tableNameWithAsAlias = raw"(?i)(.+)(?: AS) (\S+)".r
    // Matches 'tableName alias'.
    val tableNameWithAlias = raw"(.+) (.+)".r

    table match {
      case deltaPathWithAsAlias(tableName, alias) => tableName -> Some(alias)
      case deltaPathWithAlias(tableName, alias) => tableName -> Some(alias)
      case deltaPath(tableName) => tableName -> None
      case tableNameWithAsAlias(tableName, alias) => tableName -> Some(alias)
      case tableNameWithAlias(tableName, alias) => tableName -> Some(alias)
      case tableName => tableName -> None
    }
  }

  /**
   * Implements an ordering where `x < y` iff both reader and writer versions of
   * `x` are strictly less than those of `y`.
   *
   * Can be used to conveniently check that this relationship holds in tests/assertions
   * without having to write out the conjunction of the two subconditions every time.
   */
  case object StrictProtocolOrdering extends PartialOrdering[Protocol] {
    override def tryCompare(x: Protocol, y: Protocol): Option[Int] = {
      if (x.minReaderVersion == y.minReaderVersion &&
        x.minWriterVersion == y.minWriterVersion) {
        Some(0)
      } else if (x.minReaderVersion < y.minReaderVersion &&
        x.minWriterVersion < y.minWriterVersion) {
        Some(-1)
      } else if (x.minReaderVersion > y.minReaderVersion &&
        x.minWriterVersion > y.minWriterVersion) {
        Some(1)
      } else {
        None
      }
    }

    override def lteq(x: Protocol, y: Protocol): Boolean =
      x.minReaderVersion <= y.minReaderVersion && x.minWriterVersion <= y.minWriterVersion

    // Just a more readable version of `lteq`.
    def fulfillsVersionRequirements(actual: Protocol, requirement: Protocol): Boolean =
      lteq(requirement, actual)
  }
}

trait DeltaTestUtilsForTempViews
  extends SharedSparkSession
  with DeltaTestUtilsBase {

  def testWithTempView(testName: String)(testFun: Boolean => Any): Unit = {
    Seq(true, false).foreach { isSQLTempView =>
      val tempViewUsed = if (isSQLTempView) "SQL TempView" else "Dataset TempView"
      test(s"$testName - $tempViewUsed") {
        withTempView("v") {
          testFun(isSQLTempView)
        }
      }
    }
  }

  def testQuietlyWithTempView(testName: String)(testFun: Boolean => Any): Unit = {
    Seq(true, false).foreach { isSQLTempView =>
      val tempViewUsed = if (isSQLTempView) "SQL TempView" else "Dataset TempView"
      testQuietly(s"$testName - $tempViewUsed") {
        withTempView("v") {
          testFun(isSQLTempView)
        }
      }
    }
  }

  def createTempViewFromTable(
      tableName: String,
      isSQLTempView: Boolean,
      format: Option[String] = None): Unit = {
    if (isSQLTempView) {
      sql(s"CREATE OR REPLACE TEMP VIEW v AS SELECT * from $tableName")
    } else {
      spark.read.format(format.getOrElse("delta")).table(tableName).createOrReplaceTempView("v")
    }
  }

  def createTempViewFromSelect(text: String, isSQLTempView: Boolean): Unit = {
    if (isSQLTempView) {
      sql(s"CREATE OR REPLACE TEMP VIEW v AS $text")
    } else {
      sql(text).createOrReplaceTempView("v")
    }
  }

  def testErrorMessageAndClass(
      isSQLTempView: Boolean,
      ex: AnalysisException,
      expectedErrorMsgForSQLTempView: String = null,
      expectedErrorMsgForDataSetTempView: String = null,
      expectedErrorClassForSQLTempView: String = null,
      expectedErrorClassForDataSetTempView: String = null): Unit = {
    if (isSQLTempView) {
      if (expectedErrorMsgForSQLTempView != null) {
        errorContains(ex.getMessage, expectedErrorMsgForSQLTempView)
      }
      if (expectedErrorClassForSQLTempView != null) {
        assert(ex.getErrorClass == expectedErrorClassForSQLTempView)
      }
    } else {
      if (expectedErrorMsgForDataSetTempView != null) {
        errorContains(ex.getMessage, expectedErrorMsgForDataSetTempView)
      }
      if (expectedErrorClassForDataSetTempView != null) {
        assert(ex.getErrorClass == expectedErrorClassForDataSetTempView, ex.getMessage)
      }
    }
  }
}

/**
 * Trait collecting helper methods for DML tests e.p. creating a test table for each test and
 * cleaning it up after each test.
 */
trait DeltaDMLTestUtils
  extends DeltaTestUtilsBase
  with BeforeAndAfterEach {
  self: SharedSparkSession =>

  protected var tempDir: File = _

  protected var deltaLog: DeltaLog = _

  protected def tempPath: String = tempDir.getCanonicalPath

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    // Using a space in path to provide coverage for special characters.
    tempDir = Utils.createTempDir(namePrefix = "spark test")
    deltaLog = DeltaLog.forTable(spark, new Path(tempPath))
  }

  override protected def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
      DeltaLog.clearCache()
    } finally {
      super.afterEach()
    }
  }

  protected def append(df: DataFrame, partitionBy: Seq[String] = Nil): Unit = {
    val dfw = df.write.format("delta").mode("append")
    if (partitionBy.nonEmpty) {
      dfw.partitionBy(partitionBy: _*)
    }
    dfw.save(tempPath)
  }

  protected def withKeyValueData(
      source: Seq[(Int, Int)],
      target: Seq[(Int, Int)],
      isKeyPartitioned: Boolean = false,
      sourceKeyValueNames: (String, String) = ("key", "value"),
      targetKeyValueNames: (String, String) = ("key", "value"))(
      thunk: (String, String) => Unit = null): Unit = {

    import testImplicits._

    append(target.toDF(targetKeyValueNames._1, targetKeyValueNames._2).coalesce(2),
      if (isKeyPartitioned) Seq(targetKeyValueNames._1) else Nil)
    withTempView("source") {
      source.toDF(sourceKeyValueNames._1, sourceKeyValueNames._2).createOrReplaceTempView("source")
      thunk("source", s"delta.`$tempPath`")
    }
  }

  protected def readDeltaTable(path: String): DataFrame = {
    spark.read.format("delta").load(path)
  }

  protected def getDeltaFileStmt(path: String): String = s"SELECT * FROM delta.`$path`"

  /**
   * Finds the latest operation of the given type that ran on the test table and returns the
   * dataframe with the changes of the corresponding table version.
   *
   * @param operation Delta operation name, see [[DeltaOperations]].
   */
  protected def getCDCForLatestOperation(deltaLog: DeltaLog, operation: String): DataFrame = {
    val latestOperation = deltaLog.history
      .getHistory(None)
      .find(_.operation == operation)
    assert(latestOperation.nonEmpty, s"Couldn't find a ${operation} operation to check CDF")

    val latestOperationVersion = latestOperation.get.version
    assert(latestOperationVersion.nonEmpty,
      s"Latest ${operation} operation doesn't have a version associated with it")

    CDCReader
      .changesToBatchDF(
        deltaLog,
        latestOperationVersion.get,
        latestOperationVersion.get,
        spark)
      .drop(CDCReader.CDC_COMMIT_TIMESTAMP)
      .drop(CDCReader.CDC_COMMIT_VERSION)
  }
}
