/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.replay

import java.io.{InterruptedIOException, UncheckedIOException}
import java.lang.{Long => JLong}
import java.util.{Arrays, Collections, Optional}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.Meta
import io.delta.kernel.data.{ColumnarBatch, ColumnVector, Row}
import io.delta.kernel.engine._
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.checkpoints.SidecarFile
import io.delta.kernel.internal.metrics.{LogReplayReport, LogReplayTelemetry}
import io.delta.kernel.metrics.MetricsReport
import io.delta.kernel.test.{BaseMockJsonHandler, MockEngineUtils, VectorTestUtils}
import io.delta.kernel.types.{DataType, StructType}
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import org.scalatest.funsuite.AnyFunSuite

class ActionsIteratorSuite extends AnyFunSuite with MockEngineUtils with VectorTestUtils {

  /**
   * Test for ActionsIterator resource leak fix validation
   *
   * This test validates that the fix applied in ActionsIterator.java prevents resource
   * leaks by ensuring that CloseableIterators are properly closed when exceptions occur.
   *
   * The specific fix being tested: Utils.closeCloseablesSilently(dataIter) in the catch block of
   * readCommitOrCompactionFile method.
   */
  test("ActionsIterator readCommitOrCompactionFile resource cleanup") {
    var iteratorClosed = false

    val engine = mockEngine(jsonHandler = new BaseMockJsonHandler {
      override def readJsonFiles(
          fileIter: CloseableIterator[FileStatus],
          physicalSchema: StructType,
          predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {

        // Return an empty iterator that tracks closure
        new CloseableIterator[ColumnarBatch] {
          override def hasNext(): Boolean =
            throw new NoSuchElementException("This is a test exception")
          override def next(): ColumnarBatch =
            throw new UnsupportedOperationException("Not needed for this test")
          override def close(): Unit = iteratorClosed = true
        }
      }
    })

    val testFile = FileStatus.of(
      "/path/to/00000000000000000000.json",
      100L,
      System.currentTimeMillis())
    val files = Collections.singletonList(testFile)
    val schema = new StructType()

    val actionsIterator =
      new ActionsIterator(engine, files, schema, Optional.empty[Predicate]())

    assertThrows[NoSuchElementException] {
      actionsIterator.hasNext()
    }

    // Verify that resources were cleaned up
    assert(iteratorClosed, "Internal iterator should be closed after exception in ActionsIterator")
  }

  /**
   * When the calling thread is interrupted before next() begins an NIO read, ActionsIterator
   * must surface the interrupt as a typed InterruptedIOException (wrapped in
   * UncheckedIOException, since next() does not declare checked exceptions) so that engine
   * interrupt-handling (e.g. Spark's StreamExecution.isInterruptionException) recognizes
   * it as a clean shutdown rather than a real error.
   */
  test("ActionsIterator.next() throws InterruptedIOException when thread is interrupted") {
    val engine = mockEngine(jsonHandler = new BaseMockJsonHandler {
      override def readJsonFiles(
          fileIter: CloseableIterator[FileStatus],
          physicalSchema: StructType,
          predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
        // Return a non-empty iterator so hasNext() succeeds and next() is reached.
        new CloseableIterator[ColumnarBatch] {
          override def hasNext(): Boolean = true
          override def next(): ColumnarBatch =
            throw new UnsupportedOperationException("next() should not be called after interrupt")
          override def close(): Unit = {}
        }
      }
    })

    val testFile = FileStatus.of(
      "/path/to/00000000000000000000.json",
      100L,
      System.currentTimeMillis())
    val actionsIterator =
      new ActionsIterator(
        engine,
        Collections.singletonList(testFile),
        new StructType(),
        Optional.empty[Predicate]())

    Thread.currentThread().interrupt()
    try {
      val ex = intercept[UncheckedIOException] {
        actionsIterator.next()
      }
      assert(ex.getCause.isInstanceOf[InterruptedIOException])
      assert(ex.getCause.getMessage == "Thread was interrupted")
    } finally {
      // Clear the interrupt flag so it doesn't leak into subsequent tests.
      Thread.interrupted()
    }
  }

  test("preflight telemetry is reported before the first JSON replay read") {
    val reports = ArrayBuffer.empty[MetricsReport]
    var preflightReportedBeforeRead = false
    val engine = new Engine {
      override def getExpressionHandler: ExpressionHandler = null
      override def getJsonHandler: JsonHandler = new BaseMockJsonHandler {
        override def readJsonFiles(
            fileIter: CloseableIterator[FileStatus],
            physicalSchema: StructType,
            predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
          preflightReportedBeforeRead = reports.exists {
            case report: LogReplayReport => report.getPhase == LogReplayReport.Phase.PREFLIGHT
            case _ => false
          }
          new CloseableIterator[ColumnarBatch] {
            override def hasNext: Boolean = false
            override def next(): ColumnarBatch = throw new NoSuchElementException()
            override def close(): Unit = {}
          }
        }
      }
      override def getFileSystemClient: FileSystemClient = null
      override def getParquetHandler: ParquetHandler = null
      override def getMetricsReporters: java.util.List[MetricsReporter] =
        Collections.singletonList(new MetricsReporter {
          override def report(report: MetricsReport): Unit = reports += report
        })
    }
    val telemetry = new LogReplayTelemetry(
      engine,
      ActionsIterator.selectFilesForReplay(Collections.emptyList(), Optional.empty()),
      Optional.empty(),
      "/table",
      0L,
      java.util.UUID.randomUUID())
    telemetry.reportPreflight()
    val actionsIterator = new ActionsIterator(
      engine,
      Collections.singletonList(FileStatus.of("/table/_delta_log/00000000000000000000.json")),
      new StructType(),
      Optional.empty[Predicate]())

    actionsIterator.hasNext()

    assert(preflightReportedBeforeRead)
  }

  test("continuation telemetry excludes consumed multipart checkpoints and V2 sidecars") {
    val reports = ArrayBuffer.empty[MetricsReport]
    val engine = new Engine {
      override def getExpressionHandler: ExpressionHandler = null
      override def getJsonHandler: JsonHandler = null
      override def getFileSystemClient: FileSystemClient = null
      override def getParquetHandler: ParquetHandler = null
      override def getMetricsReporters: java.util.List[MetricsReporter] =
        Collections.singletonList(new MetricsReporter {
          override def report(report: MetricsReport): Unit = reports += report
        })
    }
    val consumedCheckpoint = FileStatus.of(
      "/table/_delta_log/00000000000000000010.checkpoint.0000000003.0000000003.parquet",
      30L,
      1L)
    val resumedCheckpoint = FileStatus.of(
      "/table/_delta_log/00000000000000000010.checkpoint.0000000002.0000000003.parquet",
      20L,
      1L)
    val v2Manifest = FileStatus.of(
      "/table/_delta_log/00000000000000000010.checkpoint.uuid.parquet",
      40L,
      1L)
    val paginationContext = PaginationContext.forPageWithPageToken(
      "/table",
      10L,
      1,
      1,
      100L,
      new PageToken(
        resumedCheckpoint.getPath,
        0L,
        Optional.of(1L),
        Meta.KERNEL_VERSION,
        "/table",
        10L,
        1,
        1))
    val selectedTopLevelFiles = ActionsIterator.selectFilesForReplay(
      Arrays.asList(consumedCheckpoint, resumedCheckpoint, v2Manifest),
      Optional.of(paginationContext))
    assert(selectedTopLevelFiles.getFiles.asScala.map(_.getFile) ==
      Seq(resumedCheckpoint, v2Manifest))
    val telemetry = new LogReplayTelemetry(
      engine,
      selectedTopLevelFiles,
      Optional.of(10L),
      "/table",
      10L,
      java.util.UUID.randomUUID())
    val sidecarPaths = Seq("first.parquet", "second.parquet", "third.parquet")
    val sidecarVector = new ColumnVector {
      private val children = Seq(
        stringVector(sidecarPaths),
        longVector(Seq(10L, 20L, 30L).map(JLong.valueOf)),
        longVector(Seq(100L, 200L, 300L).map(JLong.valueOf)))

      override def getDataType: DataType = SidecarFile.READ_SCHEMA
      override def getSize: Int = sidecarPaths.size
      override def close(): Unit = {}
      override def isNullAt(rowId: Int): Boolean = false
      override def getChild(ordinal: Int): ColumnVector = children(ordinal)
    }
    val manifestBatch = new ColumnarBatch {
      private val schema =
        new StructType().add(LogReplay.SIDECAR_FIELD_NAME, SidecarFile.READ_SCHEMA)

      override def getSchema: StructType = schema
      override def getColumnVector(ordinal: Int): ColumnVector = sidecarVector
      override def getSize: Int = sidecarPaths.size
      override def withDeletedColumnAt(ordinal: Int): ColumnarBatch = this
    }
    val iterator = new ActionsIterator(
      engine,
      Collections.emptyList(),
      new StructType(),
      new StructType(),
      Optional.empty(),
      Optional.of(paginationContext),
      Optional.of(telemetry))

    telemetry.reportPreflight()
    iterator.extractSidecarsFromBatch(
      FileStatus.of("/table/_delta_log/00000000000000000010.checkpoint.parquet", 1L, 1L),
      10L,
      manifestBatch)
    telemetry.reportFinal(LogReplayReport.Outcome.SUCCESS)

    val logReplayReports = reports.collect { case report: LogReplayReport => report }
    assert(logReplayReports.map(_.getPhase) == Seq(
      LogReplayReport.Phase.PREFLIGHT,
      LogReplayReport.Phase.FINAL))
    logReplayReports.foreach { report =>
      assert(report.getCheckpointArtifacts.getCount == 2L)
      assert(report.getCheckpointArtifacts.getKnownSizeCount == 2L)
      assert(report.getCheckpointArtifacts.getTotalKnownSizeInBytes == 60L)
    }
    assert(logReplayReports.head.getSidecarArtifacts.getCount == 0L)
    assert(logReplayReports.last.getSidecarArtifacts.getCount == 2)
    assert(logReplayReports.last.getSidecarArtifacts.getTotalKnownSizeInBytes == 50L)
  }
}
