/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.metrics

import java.io.File
import java.util
import java.util.Optional
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.Table
import io.delta.kernel.data.{ColumnarBatch, ColumnVector, Row}
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.engine._
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.hook.LogCompactionHook
import io.delta.kernel.internal.lang.Lazy
import io.delta.kernel.internal.metrics.{LogReplayReport, LogReplayTelemetry}
import io.delta.kernel.internal.replay.{ActionsIterator, LogReplay}
import io.delta.kernel.internal.snapshot.LogSegment
import io.delta.kernel.metrics.MetricsReport
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class LogReplayTelemetrySuite extends AnyFunSuite with MetricsReportTestUtils {
  private val logReplayMetricsEnabledKey = "delta.kernel.default.log-replay.metrics.enabled"

  test("DefaultEngine log replay metrics option defaults to false") {
    assert(!DefaultEngine.create(new Configuration()).isLogReplayMetricsEnabled)

    val falseConf = new Configuration()
    falseConf.set(logReplayMetricsEnabledKey, "false")
    assert(!DefaultEngine.create(falseConf).isLogReplayMetricsEnabled)

    val trueConf = new Configuration()
    trueConf.set(logReplayMetricsEnabledKey, "true")
    assert(DefaultEngine.create(trueConf).isLogReplayMetricsEnabled)

    val malformedConf = new Configuration()
    malformedConf.set(logReplayMetricsEnabledKey, "not-a-boolean")
    assert(!DefaultEngine.create(malformedConf).isLogReplayMetricsEnabled)
  }

  test("default and custom Engines do not emit log replay reports when disabled") {
    withTempDir { tempDir =>
      spark.range(1).write.format("delta").save(tempDir.getCanonicalPath)

      assert(logReplayReports(
        DefaultEngine.create(new Configuration()),
        tempDir.getCanonicalPath).isEmpty)
      assert(logReplayReports(
        customEngine(DefaultEngine.create(new Configuration())),
        tempDir.getCanonicalPath).isEmpty)
    }
  }

  test("enabled log replay emits one preflight and one final report") {
    withTempDir { tempDir =>
      spark.range(1).write.format("delta").save(tempDir.getCanonicalPath)

      val conf = new Configuration()
      conf.set(logReplayMetricsEnabledKey, "true")
      val reports = logReplayReports(DefaultEngine.create(conf), tempDir.getCanonicalPath)

      assert(reports.map(_.getPhase) == Seq(
        LogReplayReport.Phase.PREFLIGHT,
        LogReplayReport.Phase.FINAL))
      assert(reports.head.getOutcome == null)
      assert(reports.last.getOutcome == LogReplayReport.Outcome.SUCCESS)
      assert(reports.head.getLogReplayId == reports.last.getLogReplayId)
      assert(reports.head.getOperationType == "LogReplay")
      assert(reports.head.getReportUUID != reports.last.getReportUUID)
      assert(reports.head.getTablePath.endsWith(tempDir.getCanonicalPath))
      assert(reports.head.getTableVersion == 0L)
      assert(reports.head.getSnapshotReportUUID == reports.last.getSnapshotReportUUID)
    }
  }

  test("enabled early-close replay emits preflight only") {
    withTempDir { tempDir =>
      spark.range(1).write.format("delta").save(tempDir.getCanonicalPath)

      val conf = new Configuration()
      conf.set(logReplayMetricsEnabledKey, "true")
      val reports = ArrayBuffer.empty[MetricsReport]
      val engine = reportingEngine(DefaultEngine.create(conf), reports)
      val snapshot = Table.forPath(engine, tempDir.getCanonicalPath).getLatestSnapshot(engine)
      val scanFiles = snapshot.getScanBuilder().build().getScanFiles(engine)
      scanFiles.close()

      val logReplayReports = reports.collect { case report: LogReplayReport => report }.toSeq
      assert(logReplayReports.map(_.getPhase) == Seq(LogReplayReport.Phase.PREFLIGHT))
      assert(logReplayReports.head.getOutcome == null)
      assertArtifactMetrics(
        logReplayReports.head.getDeltaArtifacts,
        count = 1,
        knownSizeCount = 1,
        unknownSizeCount = 0)
      assertArtifactMetrics(
        logReplayReports.head.getCompactionArtifacts,
        count = 0,
        knownSizeCount = 0,
        unknownSizeCount = 0)
      assertArtifactMetrics(
        logReplayReports.head.getCheckpointArtifacts,
        count = 0,
        knownSizeCount = 0,
        unknownSizeCount = 0)
      assertArtifactMetrics(
        logReplayReports.head.getSidecarArtifacts,
        count = 0,
        knownSizeCount = 0,
        unknownSizeCount = 0)
    }
  }

  test("preflight telemetry precedes replay reads and final telemetry waits for exhaustion") {
    withTempDir { tempDir =>
      spark.range(4).repartition(2).write.format("delta").save(tempDir.getCanonicalPath)

      val conf = new Configuration()
      conf.set(logReplayMetricsEnabledKey, "true")
      val baseEngine = DefaultEngine.create(conf)
      val reports = ArrayBuffer.empty[MetricsReport]
      var firstReplayReadStarted = false
      val engine = new Engine {
        private val reporter = new MetricsReporter {
          override def report(report: MetricsReport): Unit = reports += report
        }
        private val timingJsonHandler = new JsonHandler {
          override def parseJson(
              jsonStringVector: ColumnVector,
              outputSchema: StructType,
              selectionVector: Optional[ColumnVector]): ColumnarBatch =
            baseEngine.getJsonHandler.parseJson(jsonStringVector, outputSchema, selectionVector)

          override def readJsonFiles(
              fileIter: CloseableIterator[FileStatus],
              physicalSchema: StructType,
              predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
            firstReplayReadStarted = true
            assert(reports.collect { case report: LogReplayReport => report.getPhase } ==
              Seq(LogReplayReport.Phase.PREFLIGHT))
            baseEngine.getJsonHandler.readJsonFiles(fileIter, physicalSchema, predicate)
          }

          override def writeJsonFileAtomically(
              filePath: String,
              data: CloseableIterator[Row],
              overwrite: Boolean): Unit =
            baseEngine.getJsonHandler.writeJsonFileAtomically(filePath, data, overwrite)
        }

        override def getExpressionHandler: ExpressionHandler = baseEngine.getExpressionHandler
        override def getJsonHandler: JsonHandler = timingJsonHandler
        override def getFileSystemClient: FileSystemClient = baseEngine.getFileSystemClient
        override def getParquetHandler: ParquetHandler = baseEngine.getParquetHandler
        override def getMetricsReporters: util.List[MetricsReporter] =
          util.Collections.singletonList(reporter)
        override def isLogReplayMetricsEnabled: Boolean = true
      }

      val snapshot =
        Table.forPath(baseEngine, tempDir.getCanonicalPath).getLatestSnapshot(baseEngine)
      val scanFiles = snapshot.getScanBuilder().build().getScanFiles(engine)
      try {
        assert(!firstReplayReadStarted)
        assert(reports.collect { case report: LogReplayReport => report.getPhase } ==
          Seq(LogReplayReport.Phase.PREFLIGHT))

        assert(scanFiles.hasNext)
        assert(firstReplayReadStarted)
        assert(reports.collect { case report: LogReplayReport => report.getPhase } ==
          Seq(LogReplayReport.Phase.PREFLIGHT))

        while (scanFiles.hasNext) {
          scanFiles.next()
        }
        assert(reports.collect { case report: LogReplayReport => report.getPhase } ==
          Seq(LogReplayReport.Phase.PREFLIGHT, LogReplayReport.Phase.FINAL))
      } finally {
        scanFiles.close()
      }
    }
  }

  test("enabled ordinary replay failure reports preflight and final failure") {
    withTempDir { tempDir =>
      spark.range(1).write.format("delta").save(tempDir.getCanonicalPath)

      val conf = new Configuration()
      conf.set(logReplayMetricsEnabledKey, "true")
      val baseEngine = DefaultEngine.create(conf)
      val snapshot = Table.forPath(baseEngine, tempDir.getCanonicalPath)
        .getLatestSnapshot(baseEngine)
      val reports = ArrayBuffer.empty[MetricsReport]
      val replayFailure = new RuntimeException("expected replay failure")
      val engine = reportingEngineWithJsonFailure(baseEngine, reports, replayFailure)

      val thrown = intercept[RuntimeException] {
        val scanFiles = snapshot.getScanBuilder().build().getScanFiles(engine)
        try {
          scanFiles.hasNext
        } finally {
          scanFiles.close()
        }
      }

      assert(thrown.getCause eq replayFailure)
      val logReplayReports = reports.collect { case report: LogReplayReport => report }.toSeq
      assert(logReplayReports.map(_.getPhase) == Seq(
        LogReplayReport.Phase.PREFLIGHT,
        LogReplayReport.Phase.FINAL))
      assert(logReplayReports.map(_.getOutcome) == Seq(
        null,
        LogReplayReport.Outcome.FAILURE))
      assertArtifactMetrics(
        logReplayReports.head.getDeltaArtifacts,
        count = 1,
        knownSizeCount = 1,
        unknownSizeCount = 0)
      assertArtifactMetrics(
        logReplayReports.last.getDeltaArtifacts,
        count = 1,
        knownSizeCount = 1,
        unknownSizeCount = 0)
      assert(logReplayReports.last.getSidecarArtifacts.getCount == 0)
    }
  }

  test("ordinary replay failure survives a throwing reporter and reaches later reporters") {
    withTempDir { tempDir =>
      spark.range(1).write.format("delta").save(tempDir.getCanonicalPath)

      val conf = new Configuration()
      conf.set(logReplayMetricsEnabledKey, "true")
      val baseEngine = DefaultEngine.create(conf)
      val snapshot = Table.forPath(baseEngine, tempDir.getCanonicalPath)
        .getLatestSnapshot(baseEngine)
      val reports = ArrayBuffer.empty[MetricsReport]
      val replayFailure = new RuntimeException("expected replay failure")
      val engine = reportingEngineWithJsonFailure(
        baseEngine,
        reports,
        replayFailure,
        throwingReporter = true)

      val thrown = intercept[RuntimeException] {
        val scanFiles = snapshot.getScanBuilder().build().getScanFiles(engine)
        scanFiles.hasNext
      }

      assert(thrown.getCause eq replayFailure)
      val logReplayReports = reports.collect { case report: LogReplayReport => report }.toSeq
      assert(logReplayReports.map(_.getPhase) == Seq(
        LogReplayReport.Phase.PREFLIGHT,
        LogReplayReport.Phase.FINAL))
      assert(logReplayReports.map(_.getOutcome) == Seq(
        null,
        LogReplayReport.Outcome.FAILURE))
    }
  }

  test("successful replay survives a throwing reporter and reaches later reporters") {
    withTempDir { tempDir =>
      spark.range(1).write.format("delta").save(tempDir.getCanonicalPath)

      val conf = new Configuration()
      conf.set(logReplayMetricsEnabledKey, "true")
      val baseEngine = DefaultEngine.create(conf)
      val snapshot = Table.forPath(baseEngine, tempDir.getCanonicalPath)
        .getLatestSnapshot(baseEngine)
      val reports = ArrayBuffer.empty[MetricsReport]
      val collectingEngine = reportingEngine(baseEngine, reports)
      val engine = new Engine {
        private val reporters = util.Arrays.asList(
          new MetricsReporter {
            override def report(report: MetricsReport): Unit = {
              if (report.isInstanceOf[LogReplayReport]) {
                throw new RuntimeException("reporter failure")
              }
            }
          },
          collectingEngine.getMetricsReporters.get(0))

        override def getExpressionHandler: ExpressionHandler = collectingEngine.getExpressionHandler
        override def getJsonHandler: JsonHandler = collectingEngine.getJsonHandler
        override def getFileSystemClient: FileSystemClient = collectingEngine.getFileSystemClient
        override def getParquetHandler: ParquetHandler = collectingEngine.getParquetHandler
        override def getMetricsReporters: util.List[MetricsReporter] = reporters
        override def isLogReplayMetricsEnabled: Boolean = true
      }

      val scanFiles = snapshot.getScanBuilder().build().getScanFiles(engine)
      try {
        while (scanFiles.hasNext) {
          scanFiles.next()
        }
      } finally {
        scanFiles.close()
      }

      val logReplayReports = reports.collect { case report: LogReplayReport => report }.toSeq
      assert(logReplayReports.map(_.getPhase) == Seq(
        LogReplayReport.Phase.PREFLIGHT,
        LogReplayReport.Phase.FINAL))
      assert(logReplayReports.map(_.getOutcome) == Seq(
        null,
        LogReplayReport.Outcome.SUCCESS))
    }
  }

  test("successful replay skips telemetry when metrics reporters accessor fails") {
    withTempDir { tempDir =>
      spark.range(1).write.format("delta").save(tempDir.getCanonicalPath)

      val conf = new Configuration()
      conf.set(logReplayMetricsEnabledKey, "true")
      val baseEngine = DefaultEngine.create(conf)
      val snapshot = Table.forPath(baseEngine, tempDir.getCanonicalPath)
        .getLatestSnapshot(baseEngine)
      val reporterAccesses = new AtomicInteger()
      val engine = new Engine {
        override def getExpressionHandler: ExpressionHandler = baseEngine.getExpressionHandler
        override def getJsonHandler: JsonHandler = baseEngine.getJsonHandler
        override def getFileSystemClient: FileSystemClient = baseEngine.getFileSystemClient
        override def getParquetHandler: ParquetHandler = baseEngine.getParquetHandler
        override def getMetricsReporters: util.List[MetricsReporter] = {
          if (reporterAccesses.incrementAndGet() <= 2) {
            throw new RuntimeException("metrics reporter accessor failure")
          }
          util.Collections.emptyList()
        }
        override def isLogReplayMetricsEnabled: Boolean = true
      }

      val scanFiles = snapshot.getScanBuilder().build().getScanFiles(engine)
      try {
        while (scanFiles.hasNext) {
          scanFiles.next()
        }
      } finally {
        scanFiles.close()
      }

      assert(reporterAccesses.get == 3)
    }
  }

  test("synthetic OutOfMemoryError reader failure emits preflight only") {
    withTempDir { tempDir =>
      spark.range(1).write.format("delta").save(tempDir.getCanonicalPath)

      val conf = new Configuration()
      conf.set(logReplayMetricsEnabledKey, "true")
      val baseEngine = DefaultEngine.create(conf)
      val snapshot = Table.forPath(baseEngine, tempDir.getCanonicalPath)
        .getLatestSnapshot(baseEngine)
      val reports = ArrayBuffer.empty[MetricsReport]
      val replayError = new OutOfMemoryError("synthetic reader failure")
      val engine = reportingEngineWithJsonFailure(baseEngine, reports, replayError)

      val thrown = intercept[OutOfMemoryError] {
        val scanFiles = snapshot.getScanBuilder().build().getScanFiles(engine)
        scanFiles.hasNext
      }

      assert(thrown eq replayError)
      assert(reports.collect { case report: LogReplayReport => report.getPhase } ==
        Seq(LogReplayReport.Phase.PREFLIGHT))
    }
  }

  test("v2 checkpoint sidecar resume reports pagination-selected artifacts") {
    val tablePath = getTestResourceFilePath("kernel-pagination-v2-checkpoint-parquet")
    val deltaLogPath = new File(tablePath, "_delta_log")
    val checkpointManifest = deltaLogPath.listFiles()
      .find(file =>
        file.getName.startsWith("00000000000000000002.checkpoint") &&
          file.getName.endsWith(".parquet"))
      .getOrElse(fail("missing V2 checkpoint manifest"))
    val sidecarSizes = spark.read.parquet(checkpointManifest.getCanonicalPath)
      .selectExpr("sidecar.sizeInBytes")
      .where("sidecar IS NOT NULL")
      .collect()
      .map(_.getLong(0))
      .toSeq
    assert(sidecarSizes.size == 2)

    val conf = new Configuration()
    conf.set(logReplayMetricsEnabledKey, "true")
    val baseEngine = DefaultEngine.create(conf)
    val snapshot = Table.forPath(baseEngine, tablePath).getSnapshotAsOfVersion(baseEngine, 2)

    val firstPageReports = ArrayBuffer.empty[MetricsReport]
    val firstPage = snapshot.getScanBuilder().buildPaginated(1, Optional.empty())
      .getScanFiles(reportingEngine(baseEngine, firstPageReports))
    val resumeToken =
      try {
        while (firstPage.hasNext) {
          firstPage.next()
        }
        firstPage.getCurrentPageToken.get()
      } finally {
        firstPage.close()
      }

    val firstPageLogReplayReports =
      firstPageReports.collect { case report: LogReplayReport => report }.toSeq
    assert(firstPageLogReplayReports.map(_.getPhase) == Seq(LogReplayReport.Phase.PREFLIGHT))
    assertKnownSizes(
      firstPageLogReplayReports.head.getCheckpointArtifacts,
      Seq(checkpointManifest.length()))
    assertArtifactMetrics(
      firstPageLogReplayReports.head.getSidecarArtifacts,
      count = 0,
      knownSizeCount = 0,
      unknownSizeCount = 0)

    val resumedPageReports = ArrayBuffer.empty[MetricsReport]
    val resumedPage = snapshot.getScanBuilder().buildPaginated(100, Optional.of(resumeToken))
      .getScanFiles(reportingEngine(baseEngine, resumedPageReports))
    try {
      while (resumedPage.hasNext) {
        resumedPage.next()
      }
    } finally {
      resumedPage.close()
    }

    val resumedPageLogReplayReports =
      resumedPageReports.collect { case report: LogReplayReport => report }.toSeq
    assert(resumedPageLogReplayReports.map(_.getPhase) == Seq(
      LogReplayReport.Phase.PREFLIGHT,
      LogReplayReport.Phase.FINAL))
    resumedPageLogReplayReports.foreach { report =>
      assertKnownSizes(report.getCheckpointArtifacts, Seq(checkpointManifest.length()))
      assertArtifactMetrics(
        report.getDeltaArtifacts,
        count = 0,
        knownSizeCount = 0,
        unknownSizeCount = 0)
      assertArtifactMetrics(
        report.getCompactionArtifacts,
        count = 0,
        knownSizeCount = 0,
        unknownSizeCount = 0)
    }
    assertArtifactMetrics(
      resumedPageLogReplayReports.head.getSidecarArtifacts,
      count = 0,
      knownSizeCount = 0,
      unknownSizeCount = 0)
    assertKnownSizes(resumedPageLogReplayReports.last.getSidecarArtifacts, sidecarSizes)
  }

  test("compaction-resolved replay reports only the files it consumes") {
    withTempDir { tempDir =>
      val tablePath = tempDir.getCanonicalPath
      (0 to 3).foreach { value =>
        spark.range(value * 10, (value + 1) * 10).write
          .format("delta")
          .mode(if (value == 0) "error" else "append")
          .save(tablePath)
      }

      val conf = new Configuration()
      conf.set(logReplayMetricsEnabledKey, "true")
      val baseEngine = DefaultEngine.create(conf)
      new LogCompactionHook(
        new Path(s"file:$tablePath"),
        new Path(s"file:$tablePath", "_delta_log"),
        0,
        2,
        0).threadSafeInvoke(baseEngine)

      val reports = ArrayBuffer.empty[MetricsReport]
      val engine = reportingEngine(baseEngine, reports)
      val snapshot = Table.forPath(engine, tablePath).getLatestSnapshot(engine)
      val rawDeltas = snapshot.asInstanceOf[SnapshotImpl].getLogSegment.getDeltas.size()
      assert(rawDeltas == 4)
      val scanFiles = snapshot.getScanBuilder().build().getScanFiles(engine)
      try {
        while (scanFiles.hasNext) {
          scanFiles.next()
        }
      } finally {
        scanFiles.close()
      }

      val finalReport = reports.collect { case report: LogReplayReport => report }
        .find(_.getPhase == LogReplayReport.Phase.FINAL)
        .getOrElse(fail("missing final log replay report"))
      assertArtifactMetrics(
        finalReport.getDeltaArtifacts,
        count = 1,
        knownSizeCount = 1,
        unknownSizeCount = 0)
      assertArtifactMetrics(
        finalReport.getCompactionArtifacts,
        count = 1,
        knownSizeCount = 1,
        unknownSizeCount = 0)
      assertArtifactMetrics(
        finalReport.getCheckpointArtifacts,
        count = 0,
        knownSizeCount = 0,
        unknownSizeCount = 0)
    }
  }

  test("disabled replay does not aggregate statuses or report telemetry") {
    val sizeAccesses = new AtomicInteger()
    val pathAccesses = new AtomicInteger()
    val topLevelFile = new FileStatus(
      "/table/_delta_log/00000000000000000000.json",
      10,
      0) {
      override def getPath: String = {
        pathAccesses.incrementAndGet()
        super.getPath
      }

      override def getSize: Long = {
        sizeAccesses.incrementAndGet()
        super.getSize
      }

      override def isSizeKnown: Boolean = {
        sizeAccesses.incrementAndGet()
        super.isSizeKnown
      }
    }
    val logSegment = new LogSegment(
      new Path("/table/_delta_log"),
      0,
      util.Collections.singletonList(topLevelFile),
      util.Collections.emptyList(),
      util.Collections.emptyList(),
      topLevelFile,
      Optional.empty(),
      Optional.empty())
    sizeAccesses.set(0)
    pathAccesses.set(0)
    val reports = ArrayBuffer.empty[MetricsReport]
    val engine = collectingEngine(reports)
    val replay = new LogReplay(
      engine,
      new Path("/table"),
      new Lazy(() => logSegment),
      new Lazy(() => Optional.empty()))

    val scanFiles = replay.getAddFilesAsColumnarBatches(
      engine,
      false,
      Optional.empty(),
      null,
      Optional.empty(),
      false,
      UUID.randomUUID())
    scanFiles.close()

    assert(reports.isEmpty)
    assert(sizeAccesses.get == 0)
    assert(pathAccesses.get == 1)
  }

  test("enabled replay shares one selected status representation") {
    val pathAccesses = new AtomicInteger()
    val topLevelFile = new FileStatus(
      "/table/_delta_log/00000000000000000000.json",
      10,
      0) {
      override def getPath: String = {
        pathAccesses.incrementAndGet()
        super.getPath
      }
    }
    val reports = ArrayBuffer.empty[MetricsReport]
    val baseEngine = collectingEngine(reports)
    val engine = new Engine {
      override def getExpressionHandler: ExpressionHandler = baseEngine.getExpressionHandler
      override def getJsonHandler: JsonHandler = baseEngine.getJsonHandler
      override def getFileSystemClient: FileSystemClient = baseEngine.getFileSystemClient
      override def getParquetHandler: ParquetHandler = baseEngine.getParquetHandler
      override def getMetricsReporters: util.List[MetricsReporter] = baseEngine.getMetricsReporters
      override def isLogReplayMetricsEnabled: Boolean = true
    }

    val selectedReplayFiles = ActionsIterator.selectFilesForReplay(
      util.Collections.singletonList(topLevelFile),
      Optional.empty())
    val telemetry = new LogReplayTelemetry(
      engine,
      selectedReplayFiles,
      Optional.empty(),
      "/table",
      0L,
      UUID.randomUUID())
    val iterator = ActionsIterator.fromSelectedReplayFiles(
      engine,
      selectedReplayFiles,
      new StructType(),
      new StructType(),
      Optional.empty(),
      Optional.empty(),
      Optional.of(telemetry))

    telemetry.reportPreflight()
    iterator.close()

    assert(pathAccesses.get == 1)
    assert(reports.collect { case report: LogReplayReport => report.getPhase } ==
      Seq(LogReplayReport.Phase.PREFLIGHT))
  }

  Seq(LogReplayReport.Outcome.SUCCESS, LogReplayReport.Outcome.FAILURE).foreach { outcome =>
    test(s"throwing reporter does not prevent later reporter from receiving $outcome telemetry") {
      val collected = ArrayBuffer.empty[MetricsReport]
      val baseEngine = DefaultEngine.create(new Configuration())
      val engine = new Engine {
        private val reporters = util.Arrays.asList(
          new MetricsReporter {
            override def report(report: MetricsReport): Unit = {
              throw new RuntimeException("reporter failure")
            }
          },
          new MetricsReporter {
            override def report(report: MetricsReport): Unit = collected += report
          })

        override def getExpressionHandler: ExpressionHandler = baseEngine.getExpressionHandler
        override def getJsonHandler: JsonHandler = baseEngine.getJsonHandler
        override def getFileSystemClient: FileSystemClient = baseEngine.getFileSystemClient
        override def getParquetHandler: ParquetHandler = baseEngine.getParquetHandler
        override def getMetricsReporters: util.List[MetricsReporter] = reporters
      }

      val telemetry = new LogReplayTelemetry(
        engine,
        ActionsIterator.selectFilesForReplay(
          util.Collections.singletonList(
            FileStatus.of("/table/_delta_log/00000000000000000000.json")),
          Optional.empty()),
        Optional.empty(),
        "/table",
        0L,
        UUID.randomUUID())
      telemetry.reportPreflight()
      telemetry.reportFinal(outcome)

      assert(collected.collect { case report: LogReplayReport => report.getPhase } ==
        Seq(LogReplayReport.Phase.PREFLIGHT, LogReplayReport.Phase.FINAL))
      assert(collected.collect { case report: LogReplayReport => report.getOutcome } ==
        Seq(null, outcome))
    }
  }

  private def logReplayReports(baseEngine: Engine, tablePath: String): Seq[LogReplayReport] = {
    val reports = ArrayBuffer.empty[MetricsReport]
    val engine = reportingEngine(baseEngine, reports)
    val snapshot = Table.forPath(engine, tablePath).getLatestSnapshot(engine)
    val scanFiles = snapshot.getScanBuilder().build().getScanFiles(engine)
    try {
      while (scanFiles.hasNext) {
        scanFiles.next()
      }
    } finally {
      scanFiles.close()
    }
    reports.collect { case report: LogReplayReport => report }.toSeq
  }

  private def customEngine(baseEngine: Engine): Engine = new Engine {
    override def getExpressionHandler: ExpressionHandler = baseEngine.getExpressionHandler
    override def getJsonHandler: JsonHandler = baseEngine.getJsonHandler
    override def getFileSystemClient: FileSystemClient = baseEngine.getFileSystemClient
    override def getParquetHandler: ParquetHandler = baseEngine.getParquetHandler
  }

  private def reportingEngine(
      baseEngine: Engine,
      reports: ArrayBuffer[MetricsReport]): Engine = new Engine {
    private val reporter = new MetricsReporter {
      override def report(report: MetricsReport): Unit = reports += report
    }

    override def getExpressionHandler: ExpressionHandler = baseEngine.getExpressionHandler
    override def getJsonHandler: JsonHandler = baseEngine.getJsonHandler
    override def getFileSystemClient: FileSystemClient = baseEngine.getFileSystemClient
    override def getParquetHandler: ParquetHandler = baseEngine.getParquetHandler
    override def getMetricsReporters: util.List[MetricsReporter] =
      util.Collections.singletonList(reporter)
    override def isLogReplayMetricsEnabled: Boolean = baseEngine.isLogReplayMetricsEnabled
  }

  private def reportingEngineWithJsonFailure(
      baseEngine: Engine,
      reports: ArrayBuffer[MetricsReport],
      replayFailure: Throwable,
      throwingReporter: Boolean = false): Engine = {
    val wrappedEngine = reportingEngine(baseEngine, reports)
    new Engine {
      private val reporters =
        if (throwingReporter) {
          util.Arrays.asList(
            new MetricsReporter {
              override def report(report: MetricsReport): Unit = {
                if (report.isInstanceOf[LogReplayReport]) {
                  throw new RuntimeException("reporter failure")
                }
              }
            },
            wrappedEngine.getMetricsReporters.get(0))
        } else {
          wrappedEngine.getMetricsReporters
        }

      private val failingJsonHandler = new JsonHandler {
        override def parseJson(
            jsonStringVector: ColumnVector,
            outputSchema: StructType,
            selectionVector: Optional[ColumnVector]): ColumnarBatch =
          wrappedEngine.getJsonHandler.parseJson(jsonStringVector, outputSchema, selectionVector)

        override def readJsonFiles(
            fileIter: CloseableIterator[FileStatus],
            physicalSchema: StructType,
            predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] =
          throw replayFailure

        override def writeJsonFileAtomically(
            filePath: String,
            data: CloseableIterator[Row],
            overwrite: Boolean): Unit =
          wrappedEngine.getJsonHandler.writeJsonFileAtomically(filePath, data, overwrite)
      }

      override def getExpressionHandler: ExpressionHandler = wrappedEngine.getExpressionHandler
      override def getJsonHandler: JsonHandler = failingJsonHandler
      override def getFileSystemClient: FileSystemClient = wrappedEngine.getFileSystemClient
      override def getParquetHandler: ParquetHandler = wrappedEngine.getParquetHandler
      override def getMetricsReporters: util.List[MetricsReporter] =
        reporters
      override def isLogReplayMetricsEnabled: Boolean =
        wrappedEngine.isLogReplayMetricsEnabled
    }
  }

  private def collectingEngine(reports: ArrayBuffer[MetricsReport]): Engine = new Engine {
    private val reporter = new MetricsReporter {
      override def report(report: MetricsReport): Unit = reports += report
    }

    override def getExpressionHandler: ExpressionHandler = null
    override def getJsonHandler: JsonHandler = null
    override def getFileSystemClient: FileSystemClient = null
    override def getParquetHandler: ParquetHandler = null
    override def getMetricsReporters: util.List[MetricsReporter] =
      util.Collections.singletonList(reporter)
  }

  private def assertArtifactMetrics(
      metrics: LogReplayReport.ArtifactMetrics,
      count: Long,
      knownSizeCount: Long,
      unknownSizeCount: Long): Unit = {
    assert(metrics.getCount == count)
    assert(metrics.getKnownSizeCount == knownSizeCount)
    assert(metrics.getUnknownSizeCount == unknownSizeCount)
    if (knownSizeCount == 0) {
      assert(metrics.getTotalKnownSizeInBytes == 0)
      assert(metrics.getMinKnownSizeInBytes == null)
      assert(metrics.getMaxKnownSizeInBytes == null)
      assert(metrics.getSizeStandardDeviationInBytes == null)
    } else {
      assert(metrics.getTotalKnownSizeInBytes > 0)
      assert(metrics.getMinKnownSizeInBytes > 0)
      assert(metrics.getMaxKnownSizeInBytes >= metrics.getMinKnownSizeInBytes)
      assert(metrics.getSizeStandardDeviationInBytes >= 0)
    }
  }

  private def assertKnownSizes(
      metrics: LogReplayReport.ArtifactMetrics,
      expectedSizes: Seq[Long]): Unit = {
    assert(metrics.getCount == expectedSizes.size)
    assert(metrics.getKnownSizeCount == expectedSizes.size)
    assert(metrics.getUnknownSizeCount == 0)
    assert(metrics.getTotalKnownSizeInBytes == expectedSizes.sum)
    assert(metrics.getMinKnownSizeInBytes == expectedSizes.min)
    assert(metrics.getMaxKnownSizeInBytes == expectedSizes.max)
    val mean = expectedSizes.sum.toDouble / expectedSizes.size
    val expectedStandardDeviation =
      math.sqrt(expectedSizes.map(size => math.pow(size - mean, 2)).sum / expectedSizes.size)
    assert(math.abs(metrics.getSizeStandardDeviationInBytes - expectedStandardDeviation) < 1e-9)
  }
}
