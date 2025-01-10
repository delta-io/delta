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
import java.util
import java.util.{Collections, Objects, Optional}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.{Engine, ExpressionHandler, FileSystemClient, JsonHandler, MetricsReporter, ParquetHandler}
import io.delta.kernel.metrics.{MetricsReport, SnapshotReport, TransactionReport}
import io.delta.kernel.{Operation, Snapshot, Table, Transaction, TransactionBuilder, TransactionCommitResult}
import io.delta.kernel.data.Row
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.metrics.Timer
import io.delta.kernel.internal.util.{FileNames, Utils}
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.actions.{RemoveFile, SingleAction}
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.{CloseableIterable, CloseableIterator, DataFileStatus}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite to test the Kernel-API created [[MetricsReport]]s. This suite is in the defaults
 * package to be able to use real tables and avoid having to mock both file listings AND file
 * contents.
 */
class MetricsReportSuite extends AnyFunSuite with TestUtils {

  ///////////////////////////
  // SnapshotReport tests //
  //////////////////////////

  /**
   * Given a function [[f]] that generates a snapshot from a [[Table]], runs [[f]] and looks for
   * a generated [[SnapshotReport]]. Exactly 1 [[SnapshotReport]] is expected. Times and returns
   * the duration it takes to run [[f]]. Uses a custom engine to collect emitted metrics reports.
   *
   * @param f function to generate a snapshot from a [[Table]] and engine
   * @param path path of the table to query
   * @param expectException whether we expect [[f]] to throw an exception, which if so, is caught
   *                        and returned with the other results
   * @returns (SnapshotReport, durationToRunF, ExceptionIfThrown)
   */
  def getSnapshotReport(
    f: (Table, Engine) => Snapshot,
    path: String,
    expectException: Boolean
  ): (SnapshotReport, Long, Option[Exception]) = {
    val timer = new Timer()

    val (metricsReports, exception) = collectMetricsReports(
      engine => {
        val table = Table.forPath(engine, path)
        timer.time(() => f(table, engine)) // Time the actual operation
      },
      expectException
    )

    val snapshotReports = metricsReports.filter(_.isInstanceOf[SnapshotReport])
    assert(snapshotReports.length == 1, "Expected exactly 1 SnapshotReport")
    (snapshotReports.head.asInstanceOf[SnapshotReport], timer.totalDurationNs(), exception)
  }

  /**
   * Given a table path and a function [[f]] to generate a snapshot, runs [[f]] and collects the
   * generated [[SnapshotReport]]. Checks that the report is as expected.
   *
   * @param f function to generate a snapshot from a [[Table]] and engine
   * @param tablePath table path to query from
   * @param expectException whether we expect f to throw an exception, if so we will check that the
   *                        report contains the thrown exception
   * @param expectedVersion the expected version for the SnapshotReport
   * @param expectedProvidedTimestamp the expected providedTimestamp for the SnapshotReport
   * @param expectNonEmptyTimestampToVersionResolutionDuration whether we expect
   *                                                           timestampToVersionResolution-
   *                                                           DurationNs to be non-empty (should
   *                                                           be true for any time-travel by
   *                                                           timestamp queries)
   * @param expectNonZeroLoadProtocolAndMetadataDuration whether we expect
   *                                                     loadInitialDeltaActionsDurationNs to be
   *                                                     non-zero (should be true except when an
   *                                                     exception is thrown before log replay)
   */
  def checkSnapshotReport(
    f: (Table, Engine) => Snapshot,
    path: String,
    expectException: Boolean,
    expectedVersion: Optional[Long],
    expectedProvidedTimestamp: Optional[Long],
    expectNonEmptyTimestampToVersionResolutionDuration: Boolean,
    expectNonZeroLoadProtocolAndMetadataDuration: Boolean
  ): Unit = {

    val (snapshotReport, duration, exception) = getSnapshotReport(f, path, expectException)

    // Verify contents
    assert(snapshotReport.getTablePath == resolvePath(path))
    assert(snapshotReport.getOperationType == "Snapshot")
    exception match {
      case Some(e) =>
        assert(snapshotReport.getException().isPresent &&
          Objects.equals(snapshotReport.getException().get(), e))
      case None => assert(!snapshotReport.getException().isPresent)
    }
    assert(snapshotReport.getReportUUID != null)
    assert(Objects.equals(snapshotReport.getVersion, expectedVersion),
      s"Expected version $expectedVersion found ${snapshotReport.getVersion}")
    assert(Objects.equals(snapshotReport.getProvidedTimestamp, expectedProvidedTimestamp))

    // Since we cannot know the actual durations of these we sanity check that they are > 0 and
    // less than the total operation duration whenever they are expected to be non-zero/non-empty
    if (expectNonEmptyTimestampToVersionResolutionDuration) {
      assert(snapshotReport.getSnapshotMetrics.getTimestampToVersionResolutionDurationNs.isPresent)
      assert(snapshotReport.getSnapshotMetrics.getTimestampToVersionResolutionDurationNs.get > 0)
      assert(snapshotReport.getSnapshotMetrics.getTimestampToVersionResolutionDurationNs.get <
        duration)
    } else {
      assert(!snapshotReport.getSnapshotMetrics.getTimestampToVersionResolutionDurationNs.isPresent)
    }
    if (expectNonZeroLoadProtocolAndMetadataDuration) {
      assert(snapshotReport.getSnapshotMetrics.getLoadInitialDeltaActionsDurationNs > 0)
      assert(snapshotReport.getSnapshotMetrics.getLoadInitialDeltaActionsDurationNs < duration)
    } else {
      assert(snapshotReport.getSnapshotMetrics.getLoadInitialDeltaActionsDurationNs == 0)
    }
  }

  test("SnapshotReport valid queries") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      // Set up delta table with version 0, 1
      spark.range(10).write.format("delta").mode("append").save(path)
      val version0timestamp = System.currentTimeMillis
      // Since filesystem modification time might be truncated to the second, we sleep to make sure
      // the next commit is after this timestamp
      Thread.sleep(1000)
      spark.range(10).write.format("delta").mode("append").save(path)

      // Test getLatestSnapshot
      checkSnapshotReport(
        (table, engine) => table.getLatestSnapshot(engine),
        path,
        expectException = false,
        expectedVersion = Optional.of(1),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = true
      )

      // Test getSnapshotAsOfVersion
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfVersion(engine, 0),
        path,
        expectException = false,
        expectedVersion = Optional.of(0),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = true
      )

      // Test getSnapshotAsOfTimestamp
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfTimestamp(engine, version0timestamp),
        path,
        expectException = false,
        expectedVersion = Optional.of(0),
        expectedProvidedTimestamp = Optional.of(version0timestamp),
        expectNonEmptyTimestampToVersionResolutionDuration = true,
        expectNonZeroLoadProtocolAndMetadataDuration = true
      )
    }
  }

  test("Snapshot report - invalid time travel parameters") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      // Set up delta table with version 0
      spark.range(10).write.format("delta").mode("append").save(path)

      // Test getSnapshotAsOfVersion with version 1 (does not exist)
      // This fails during log segment building
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfVersion(engine, 1),
        path,
        expectException = true,
        expectedVersion = Optional.of(1),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = false
      )

      // Test getSnapshotAsOfTimestamp with timestamp=0 (does not exist)
      // This fails during timestamp -> version resolution
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfTimestamp(engine, 0),
        path,
        expectException = true,
        expectedVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.of(0),
        expectNonEmptyTimestampToVersionResolutionDuration = true,
        expectNonZeroLoadProtocolAndMetadataDuration = false
      )

      // Test getSnapshotAsOfTimestamp with timestamp=currentTime (does not exist)
      // This fails during timestamp -> version resolution
      val currentTimeMillis = System.currentTimeMillis
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfTimestamp(engine, currentTimeMillis),
        path,
        expectException = true,
        expectedVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.of(currentTimeMillis),
        expectNonEmptyTimestampToVersionResolutionDuration = true,
        expectNonZeroLoadProtocolAndMetadataDuration = false
      )
    }
  }

  test("Snapshot report - table does not exist") {
    withTempDir { tempDir =>
      // This fails during either log segment building or timestamp -> version resolution
      val path = tempDir.getCanonicalPath

      // Test getLatestSnapshot
      checkSnapshotReport(
        (table, engine) => table.getLatestSnapshot(engine),
        path,
        expectException = true,
        expectedVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = false
      )

      // Test getSnapshotAsOfVersion
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfVersion(engine, 0),
        path,
        expectException = true,
        expectedVersion = Optional.of(0),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = false
      )

      // Test getSnapshotAsOfTimestamp
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfTimestamp(engine, 1000),
        path,
        expectException = true,
        expectedVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.of(1000),
        expectNonEmptyTimestampToVersionResolutionDuration = true,
        expectNonZeroLoadProtocolAndMetadataDuration = false
      )
    }
  }

  test("Snapshot report - log is corrupted") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      // Set up table with non-contiguous version (0, 2) which will fail during log segment building
      // for all the following queries
      (0 until 3).foreach( _ =>
        spark.range(3).write.format("delta").mode("append").save(path))
      assert(
        new File(FileNames.deltaFile(new Path(tempDir.getCanonicalPath, "_delta_log"), 1)).delete())

      // Test getLatestSnapshot
      checkSnapshotReport(
        (table, engine) => table.getLatestSnapshot(engine),
        path,
        expectException = true,
        expectedVersion = Optional.empty(),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = false
      )

      // Test getSnapshotAsOfVersion
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfVersion(engine, 2),
        path,
        expectException = true,
        expectedVersion = Optional.of(2),
        expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
        expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
        expectNonZeroLoadProtocolAndMetadataDuration = false
      )

      // Test getSnapshotAsOfTimestamp
      val version2Timestamp = new File(
        FileNames.deltaFile(new Path(tempDir.getCanonicalPath, "_delta_log"), 2)).lastModified()
      checkSnapshotReport(
        (table, engine) => table.getSnapshotAsOfTimestamp(engine, version2Timestamp),
        tempDir.getCanonicalPath,
        expectException = true,
        expectedVersion = Optional.of(2),
        expectedProvidedTimestamp = Optional.of(version2Timestamp),
        expectNonEmptyTimestampToVersionResolutionDuration = true,
        expectNonZeroLoadProtocolAndMetadataDuration = false
      )
    }
  }

  test("Snapshot report - missing metadata") {
    // This fails during P&M loading for all of the following queries
    val path = goldenTablePath("deltalog-state-reconstruction-without-metadata")

    // Test getLatestSnapshot
    checkSnapshotReport(
      (table, engine) => table.getLatestSnapshot(engine),
      path,
      expectException = true,
      expectedVersion = Optional.of(0),
      expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
      expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
      expectNonZeroLoadProtocolAndMetadataDuration = true
    )

    // Test getSnapshotAsOfVersion
    checkSnapshotReport(
      (table, engine) => table.getSnapshotAsOfVersion(engine, 0),
      path,
      expectException = true,
      expectedVersion = Optional.of(0),
      expectedProvidedTimestamp = Optional.empty(), // No time travel by timestamp
      expectNonEmptyTimestampToVersionResolutionDuration = false, // No time travel by timestamp
      expectNonZeroLoadProtocolAndMetadataDuration = true
    )

    // Test getSnapshotAsOfTimestamp
    // We use the timestamp of version 0
    val version0Timestamp = new File(FileNames.deltaFile(new Path(path, "_delta_log"), 0))
      .lastModified()
    checkSnapshotReport(
      (table, engine) => table.getSnapshotAsOfTimestamp(engine, version0Timestamp),
      path,
      expectException = true,
      expectedVersion = Optional.of(0),
      expectedProvidedTimestamp = Optional.of(version0Timestamp),
      expectNonEmptyTimestampToVersionResolutionDuration = true,
      expectNonZeroLoadProtocolAndMetadataDuration = true
    )
  }

  /////////////////////////////
  // TransactionReport tests //
  ////////////////////////////

  /**
   * Creates a [[Transaction]] using `getTransaction`, requests actions to commit using
   * `generateCommitActions`, and commits them to the transaction. Uses a custom engine for all
   * of these operations that collects any emitted metrics reports. Exactly 1 [[TransactionReport]]
   * is expected to be emitted, and at most one [[SnapshotReport]]. Also times and returns the
   * duration it takes for [[Transaction#commit]] to finish.
   *
   * @param getTransaction given an engine return a started [[Transaction]]
   * @param generateCommitActions given a [[Transaction]] and engine generates actions to commit
   * @param expectException whether we expect committing to throw an exception, which if so, is
   *                        caught and returned with the other results
   * @return (TransactionReport, durationToCommit, SnapshotReportIfPresent, ExceptionIfThrown)
   */
  def getTransactionAndSnapshotReport(
    getTransaction: Engine => Transaction,
    generateCommitActions: (Transaction, Engine) => CloseableIterable[Row],
    expectException: Boolean
  ): (TransactionReport, Long, Option[SnapshotReport], Option[Exception]) = {
    val timer = new Timer()

    val (metricsReports, exception) = collectMetricsReports(
      engine => {
        val transaction = getTransaction(engine)
        val actionsToCommit = generateCommitActions(transaction, engine)
        timer.time(() => transaction.commit(engine, actionsToCommit)) // Time the actual operation
      },
      expectException
    )

    val transactionReports = metricsReports.filter(_.isInstanceOf[TransactionReport])
    assert(transactionReports.length == 1, "Expected exactly 1 TransactionReport")
    val snapshotReports = metricsReports.filter(_.isInstanceOf[SnapshotReport])
    assert(snapshotReports.length <= 1, "Expected at most 1 SnapshotReport")
    (transactionReports.head.asInstanceOf[TransactionReport], timer.totalDurationNs(),
      snapshotReports.headOption.map(_.asInstanceOf[SnapshotReport]), exception)
  }

  /**
   * Builds a transaction using `buildTransaction` for the table at the provided path. Commits
   * to the transaction the actions generated by `generateCommitActions` and collects any emitted
   * [[TransactionReport]]. Checks that the report is as expected
   *
   * @param generateCommitActions function to generate commit actions from a transaction and engine
   * @param path table path to commit to
   * @param expectException whether we expect committing to throw an exception
   * @param expectedSnapshotVersion expected snapshot version for the transaction
   * @param expectedNumAddFiles expected number of add files recorded in the metrics
   * @param expectedNumRemoveFiles expected number of remove files recorded in the metrics
   * @param expectedNumTotalActions expected number of total actions recorded in the metrics
   * @param expectedCommitVersion expected commit version if not `expectException`
   * @param expectedNumAttempts expected number of commit attempts
   * @param buildTransaction function to build a transaction from a transaction builder
   * @param engineInfo engine info to create the transaction with
   * @param operation operation to create the transaction with
   */
  // scalastyle:off
  def checkTransactionReport(
    generateCommitActions: (Transaction, Engine) => CloseableIterable[Row],
    path: String,
    expectException: Boolean,
    expectedSnapshotVersion: Long,
    expectedNumAddFiles: Long = 0,
    expectedNumRemoveFiles: Long = 0,
    expectedNumTotalActions: Long = 0,
    expectedCommitVersion: Option[Long] = None,
    expectedNumAttempts: Long = 1,
    buildTransaction: (TransactionBuilder, Engine) => Transaction = (tb, e) => tb.build(e),
    engineInfo: String = "test-engine-info",
    operation: Operation = Operation.MANUAL_UPDATE
  ): Unit = {
    // scalastyle:on
    assert(expectException == expectedCommitVersion.isEmpty)

    val (transactionReport, duration, snapshotReportOpt, exception) =
      getTransactionAndSnapshotReport(
        engine => buildTransaction(
          Table.forPath(engine, path).createTransactionBuilder(engine, engineInfo, operation),
          engine
        ), generateCommitActions, expectException)

    // Verify contents
    assert(transactionReport.getTablePath == resolvePath(path))
    assert(transactionReport.getOperationType == "Transaction")
    exception match {
      case Some(e) =>
        assert(transactionReport.getException().isPresent &&
          Objects.equals(transactionReport.getException().get(), e))
      case None => assert(!transactionReport.getException().isPresent)
    }
    assert(transactionReport.getReportUUID != null)
    assert(transactionReport.getOperation == operation.toString)
    assert(transactionReport.getEngineInfo == engineInfo)

    assert(transactionReport.getSnapshotVersion == expectedSnapshotVersion)
    if (expectedSnapshotVersion < 0) {
      // This was for a new table, there is no corresponding SnapshotReport
      assert(!transactionReport.getSnapshotReportUUID.isPresent)
    } else {
      assert(snapshotReportOpt.exists { snapshotReport =>
        snapshotReport.getVersion.toScala.contains(expectedSnapshotVersion) &&
          transactionReport.getSnapshotReportUUID.toScala.contains(snapshotReport.getReportUUID)
      })
    }
    assert(transactionReport.getCommittedVersion.toScala == expectedCommitVersion)

    // Since we cannot know the actual duration of commit we sanity check that they are > 0 and
    // less than the total operation duration
    assert(transactionReport.getTransactionMetrics.getTotalCommitDuration > 0)
    assert(transactionReport.getTransactionMetrics.getTotalCommitDuration < duration)

    assert(transactionReport.getTransactionMetrics.getNumCommitAttempts == expectedNumAttempts)
    assert(transactionReport.getTransactionMetrics.getNumAddFiles == expectedNumAddFiles)
    assert(transactionReport.getTransactionMetrics.getNumRemoveFiles == expectedNumRemoveFiles)
    assert(transactionReport.getTransactionMetrics.getNumTotalActions == expectedNumTotalActions)
  }

  def generateAppendActions(fileStatusIter: CloseableIterator[DataFileStatus])
    (trans: Transaction, engine: Engine): CloseableIterable[Row] = {
    val transState = trans.getTransactionState(engine)
    CloseableIterable.inMemoryIterable(
      Transaction.generateAppendActions(engine, transState, fileStatusIter,
        Transaction.getWriteContext(engine, transState, Collections.emptyMap()))
    )
  }

  test("TransactionReport: Basic append to existing table + update metadata") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      // Set up delta table with version 0
      spark.range(10).write.format("delta").mode("append").save(path)

      // Commit 1 AddFiles
      checkTransactionReport(
        generateCommitActions = generateAppendActions(fileStatusIter1),
        path,
        expectException = false,
        expectedSnapshotVersion = 0,
        expectedNumAddFiles = 1,
        expectedNumTotalActions = 2, // commitInfo + addFile
        expectedCommitVersion = Some(1)
      )

      // Commit 2 AddFiles
      checkTransactionReport(
        generateCommitActions = generateAppendActions(fileStatusIter2),
        path,
        expectException = false,
        expectedSnapshotVersion = 1,
        expectedNumAddFiles = 2,
        expectedNumTotalActions = 3, // commitInfo + addFile
        expectedCommitVersion = Some(2),
        engineInfo = "foo",
        operation = Operation.WRITE
      )

      // Update metadata only
      checkTransactionReport(
        generateCommitActions = (_, _) => CloseableIterable.emptyIterable(),
        path,
        expectException = false,
        expectedSnapshotVersion = 2,
        expectedNumTotalActions = 2, // metadata, commitInfo
        expectedCommitVersion = Some(3),
        buildTransaction = (transBuilder, engine) => {
          transBuilder
            .withTableProperties(engine, Map(TableConfig.CHECKPOINT_INTERVAL.getKey -> "2").asJava)
            .build(engine)
        }
      )
    }
  }

  test("TransactionReport: Create new empty table and then append") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      checkTransactionReport(
        generateCommitActions = (_, _) => CloseableIterable.emptyIterable(),
        path,
        expectException = false,
        expectedSnapshotVersion = -1,
        expectedNumTotalActions = 3, // protocol, metadata, commitInfo
        expectedCommitVersion = Some(0),
        buildTransaction = (transBuilder, engine) => {
          transBuilder
            .withSchema(engine, new StructType().add("id", IntegerType.INTEGER))
            .build(engine)
        }
      )

      // Commit 2 AddFiles
      checkTransactionReport(
        generateCommitActions = generateAppendActions(fileStatusIter2),
        path,
        expectException = false,
        expectedSnapshotVersion = 0,
        expectedNumAddFiles = 2,
        expectedNumTotalActions = 3, // commitInfo + addFile
        expectedCommitVersion = Some(1)
      )
    }
  }

  test("TransactionReport: Create new non-empty table with insert") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      checkTransactionReport(
        generateCommitActions = generateAppendActions(fileStatusIter1),
        path,
        expectException = false,
        expectedSnapshotVersion = -1,
        expectedNumAddFiles = 1,
        expectedNumTotalActions = 4, // protocol, metadata, commitInfo
        expectedCommitVersion = Some(0),
        buildTransaction = (transBuilder, engine) => {
          transBuilder
            .withSchema(engine, new StructType().add("id", IntegerType.INTEGER))
            .build(engine)
        }
      )
    }
  }

  test("TransactionReport: manually commit a remove file") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      // Set up delta table with version 0
      spark.range(10).write.format("delta").mode("append").save(path)

      val removeFileRow: Row = {
        val fieldMap: Map[Integer, AnyRef] = Map(
          Integer.valueOf(RemoveFile.FULL_SCHEMA.indexOf("path")) -> "/path/for/remove/file",
          Integer.valueOf(RemoveFile.FULL_SCHEMA.indexOf("dataChange")) -> java.lang.Boolean.TRUE
        )
        new GenericRow(RemoveFile.FULL_SCHEMA, fieldMap.asJava)
      }

      checkTransactionReport(
        generateCommitActions = (_, _) => CloseableIterable.inMemoryIterable(
          Utils.toCloseableIterator(
            Seq(SingleAction.createRemoveFileSingleAction(removeFileRow)).iterator.asJava
        )),
        path,
        expectException = false,
        expectedSnapshotVersion = 0,
        expectedNumRemoveFiles = 1,
        expectedNumTotalActions = 2, // commitInfo + removeFile
        expectedCommitVersion = Some(1)
      )
    }
  }

  test("TransactionReport: retry with a concurrent append") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      // Set up delta table with version 0
      spark.range(10).write.format("delta").mode("append").save(path)

      checkTransactionReport(
        generateCommitActions = (trans, engine) => {
          spark.range(10).write.format("delta").mode("append").save(path)
          generateAppendActions(fileStatusIter1)(trans, engine)
        },
        path,
        expectException = false,
        expectedSnapshotVersion = 0,
        expectedNumAddFiles = 1,
        expectedNumTotalActions = 2, // commitInfo + removeFile
        expectedCommitVersion = Some(2),
        expectedNumAttempts = 2
      )
    }
  }

  test("TransactionReport: fail due to conflicting write") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      // Set up delta table with version 0
      spark.range(10).write.format("delta").mode("append").save(path)

      checkTransactionReport(
        generateCommitActions = (trans, engine) => {
          spark.sql("ALTER TABLE delta.`" + path + "` ADD COLUMN newCol INT")
          generateAppendActions(fileStatusIter1)(trans, engine)
        },
        path,
        expectException = true,
        expectedSnapshotVersion = 0
      )
    }
  }

  test("TransactionReport: fail due to too many tries") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      // Set up delta table with version 0
      spark.range(10).write.format("delta").mode("append").save(path)

      // This writes a concurrent append everytime the iterable is asked for an iterator. This means
      // there should be a conflicting transaction committed everytime Kernel tries to commit
      def actionsIterableWithConcurrentAppend(
        trans: Transaction, engine: Engine): CloseableIterable[Row] = {
        val transState = trans.getTransactionState(engine)
        val writeContext = Transaction.getWriteContext(engine, transState, Collections.emptyMap())

        new CloseableIterable[Row] {

          override def iterator(): CloseableIterator[Row] = {
            spark.range(10).write.format("delta").mode("append").save(path)
            Transaction.generateAppendActions(engine, transState, fileStatusIter1, writeContext)
          }

          override def close(): Unit = ()
        }
      }

      checkTransactionReport(
        generateCommitActions = actionsIterableWithConcurrentAppend,
        path,
        expectException = true,
        expectedSnapshotVersion = 0,
        expectedNumAttempts = 200
      )
    }
  }

  ///////////////////////////
  // Test Helper Constants //
  ///////////////////////////

  private def fileStatusIter1 = Utils.toCloseableIterator(
    Seq(new DataFileStatus("/path/to/file", 100, 100, Optional.empty())).iterator.asJava
  )

  private def fileStatusIter2 = Utils.toCloseableIterator(
    Seq(
      new DataFileStatus("/path/to/file1", 100, 100, Optional.empty()),
      new DataFileStatus("/path/to/file2", 100, 100, Optional.empty())
    ).iterator.asJava
  )

  /////////////////////////
  // Test Helper Methods //
  /////////////////////////

  // For now this just uses the default engine since we have no need to override it, if we would
  // like to use a specific engine in the future for other tests we can simply add another arg here
  /**
   * Executes [[f]] using a special engine implementation to collect and return metrics reports.
   * If [[expectException]], catches any exception thrown by [[f]] and returns it with the reports.
   */
  def collectMetricsReports(
    f: Engine => Unit, expectException: Boolean): (Seq[MetricsReport], Option[Exception]) = {
    // Initialize a buffer for any metric reports and wrap the engine so that they are recorded
    val reports = ArrayBuffer.empty[MetricsReport]
    if (expectException) {
      val e = intercept[Exception](
        f(new EngineWithInMemoryMetricsReporter(reports, defaultEngine))
      )
      (reports, Some(e))
    } else {
      f(new EngineWithInMemoryMetricsReporter(reports, defaultEngine))
      (reports, Option.empty)
    }
  }

  def resolvePath(path: String): String = {
    defaultEngine.getFileSystemClient.resolvePath(path)
  }

  /**
   * Wraps an {@link Engine} to implement the metrics reporter such that it appends any reports
   * to the provided in memory buffer.
   */
  class EngineWithInMemoryMetricsReporter(buf: ArrayBuffer[MetricsReport], baseEngine: Engine)
    extends Engine {

    private val metricsReporter = new MetricsReporter {
      override def report(report: MetricsReport): Unit = buf.append(report)
    }

    private val metricsReporters = new util.ArrayList[MetricsReporter]() {{
      addAll(baseEngine.getMetricsReporters)
      add(metricsReporter)
    }}

    override def getExpressionHandler: ExpressionHandler = baseEngine.getExpressionHandler

    override def getJsonHandler: JsonHandler = baseEngine.getJsonHandler

    override def getFileSystemClient: FileSystemClient = baseEngine.getFileSystemClient

    override def getParquetHandler: ParquetHandler = baseEngine.getParquetHandler

    override def getMetricsReporters(): java.util.List[MetricsReporter] = {
      metricsReporters
    }
  }
}
