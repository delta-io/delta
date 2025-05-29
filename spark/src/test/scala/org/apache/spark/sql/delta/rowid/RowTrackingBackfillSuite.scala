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

package org.apache.spark.sql.delta.rowid

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionException

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions.{AddFile, Protocol, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.backfill.{BackfillBatch, BackfillBatchStats, BackfillCommandStats, RowTrackingBackfillBatch, RowTrackingBackfillCommand, RowTrackingBackfillExecutor}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

/** Test-only object that overrides a method to force a failure. */
case class FailingRowTrackingBackfillBatch(filesInBatch: Seq[AddFile])
  extends BackfillBatch {
  override val backfillBatchStatsOpType = DeltaUsageLogsOpTypes.BACKFILL_BATCH
  override def prepareFilesAndCommit(
      txn: OptimisticTransaction,
      batchId: Int): Unit = {
    throw new IllegalStateException("mock exception for test")
  }
}

class RowTrackingBackfillSuite
    extends RowIdTestUtils
    with SharedSparkSession {
  protected val initialNumRows = 1000
  protected val testTableName = "target"
  protected val numFilesInTable = 10

  override def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_ROW_TRACKING_BACKFILL_ENABLED.key, "true")

  protected def createTable(tableName: String): Unit = {
    // We disable Optimize Write to ensure the right number of files are created.
    withSQLConf(
      DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false"
    ) {
      spark.range(start = 0, end = initialNumRows, step = 1, numPartitions = numFilesInTable)
        .write
        .format("delta")
        .saveAsTable(tableName)

      val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(tableName))
      assert(snapshot.allFiles.count() === numFilesInTable)
    }
  }

  /** Create the default test table used by this suite, which has row IDs disabled. */
  protected def withRowIdDisabledTestTable()(f: => Unit): Unit = {
    withRowTrackingEnabled(enabled = false) {
      withTable(testTableName) {
        createTable(testTableName)
        val (log, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(testTableName))
        assertRowIdsAreNotSet(log)
        assert(!RowTracking.isSupported(snapshot.protocol))
        assert(!RowId.isEnabled(snapshot.protocol, snapshot.metadata))
        f
      }
    }
  }

  /** Check the number of backfill commits in the Delta history. */
  protected def assertNumBackfillCommits(expectedNumCommits: Int): Unit = {
    val log = DeltaLog.forTable(spark, TableIdentifier(testTableName))
    val actualNumBackfillCommits = log.history.getHistory(None)
      .filter(_.operation == DeltaOperations.ROW_TRACKING_BACKFILL_OPERATION_NAME)
    assert(actualNumBackfillCommits.size === expectedNumCommits)
  }

  /** Check the protocol, the number of backfill commits and the table property. */
  protected def assertBackfillWasSuccessful(expectedNumCommits: Int): Unit = {
    val (log, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(testTableName))
    assert(RowTracking.isSupported(snapshot.protocol))
    assertNumBackfillCommits(expectedNumCommits)
    assert(RowId.isEnabled(snapshot.protocol, snapshot.metadata))
  }

  test("getCandidateFilesToBackfill returns right files on tables with row IDs disabled") {
    withRowIdDisabledTestTable() {
      val (log, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(testTableName))
      val initialFilesInTable = snapshot.allFiles.collect().toSet
      assert(initialFilesInTable.size === numFilesInTable,
        "We expect the AddFiles to be unique in this table.")
      // ALl files in a table with row ID disabled should require backfill.
      val filesToBackfillBeforeTableFeatureSupport =
        RowTrackingBackfillExecutor.getCandidateFilesToBackfill(log.update()).collect()
      assert(filesToBackfillBeforeTableFeatureSupport.toSet === initialFilesInTable)
      // Let's add table feature support without enabling row ID, i.e., force new files to have
      // base row IDs. This is not the same as enabling the table property. This does not trigger
      // backfill commits.
      sql(
        s"""ALTER TABLE $testTableName
           |SET TBLPROPERTIES('$rowTrackingFeatureName' = 'supported')""".stripMargin)
      val snapshotAfterTableSupport = log.update()
      assert(RowTracking.isSupported(snapshotAfterTableSupport.protocol))
      assert(!RowId.isEnabled(
        snapshotAfterTableSupport.protocol, snapshotAfterTableSupport.metadata))
      spark.range(end = 1).write.mode("append").insertInto(testTableName)
      val snapshotAfterInsert = log.update()
      assert(snapshotAfterInsert.allFiles.count() === numFilesInTable + 1)
      // Only the files before the table feature support should need backfill.
      val filesToBackfillAfterTableFeatureSupport =
        RowTrackingBackfillExecutor.getCandidateFilesToBackfill(snapshotAfterInsert).collect()
      assert(filesToBackfillAfterTableFeatureSupport.toSet === initialFilesInTable)
    }
  }

  test("Trigger backfill by calling command directly") {
    // No one should be calling this directly. We just want to unit test outside of ALTER TABLE.
    withRowIdDisabledTestTable() {
      val log = DeltaLog.forTable(spark, TableIdentifier(testTableName))
      RowTrackingBackfillCommand(
        log,
        nameOfTriggeringOperation = DeltaOperations.OP_SET_TBLPROPERTIES,
        None).run(spark)

      val snapshot = log.update()
      assert(RowTracking.isSupported(snapshot.protocol))
      assert(!RowId.isEnabled(snapshot.protocol, snapshot.metadata))
      assertNumBackfillCommits(expectedNumCommits = 1)
    }
  }

  test("Calling the command directly should not trigger backfill when " +
    "Row Tracking Backfill is not enabled") {
    withSQLConf(DeltaSQLConf.DELTA_ROW_TRACKING_BACKFILL_ENABLED.key -> "false") {
      // No one should be calling this directly. We just want to unit test outside of ALTER TABLE.
      withRowIdDisabledTestTable() {
        val log = DeltaLog.forTable(spark, TableIdentifier(testTableName))

        val ex = intercept[UnsupportedOperationException] {
          RowTrackingBackfillCommand(
            log,
            nameOfTriggeringOperation = DeltaOperations.OP_SET_TBLPROPERTIES,
            None).run(spark)
        }

        assert(ex.getMessage === "Cannot enable Row IDs on an existing table.")
      }
    }
  }

  test("Trigger backfill using ALTER TABLE") {
    withRowIdDisabledTestTable() {
      val log = DeltaLog.forTable(spark, TableIdentifier(testTableName))
      validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 1) {
        triggerBackfillOnTestTableUsingAlterTable(testTableName, initialNumRows, log)
      }
      assertBackfillWasSuccessful(expectedNumCommits = 1)

      val snapshot = log.update()
      assertRowIdsAreValid(log)
      assert(RowTracking.isSupported(snapshot.protocol))
      assert(RowId.isEnabled(snapshot.protocol, snapshot.metadata))

      val prevHighWatermark = RowId.extractHighWatermark(log.update()).get

      // Commits after the backfill command should have row IDs.
      val numNewRows = 100
      spark.range(end = numNewRows).write.insertInto(testTableName)
      assertRowIdsAreValid(log)
      assertHighWatermarkIsCorrectAfterUpdate(log, prevHighWatermark, numNewRows)
    }
  }

  test("Backfill respects the max file limit per commit") {
    val maxNumFilesPerCommit = 3
    withRowIdDisabledTestTable() {
      withSQLConf(
        DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT.key ->
          maxNumFilesPerCommit.toString) {
        val expectedNumBackfillCommits =
          Math.ceil(numFilesInTable.toDouble / maxNumFilesPerCommit.toDouble).toInt
        validateSuccessfulBackfillMetrics(expectedNumBackfillCommits) {
          val log = DeltaLog.forTable(spark, TableIdentifier(testTableName))
          triggerBackfillOnTestTableUsingAlterTable(testTableName, initialNumRows, log)
        }
        assertBackfillWasSuccessful(expectedNumBackfillCommits)
      }
    }
  }

  test("Backfill on table with row tracking already enabled") {
    withRowTrackingEnabled(enabled = true) {
      withTable(testTableName) {
        createTable(testTableName)
        val (log, snapshot1) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(testTableName))
        var snapshot = snapshot1
        assert(RowTracking.isSupported(snapshot.protocol))
        assert(RowId.isEnabled(snapshot.protocol, snapshot.metadata))

        // ALTER TABLE should do nothing other than set the table properties.
        validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 0) {
          triggerBackfillOnTestTableUsingAlterTable(testTableName, initialNumRows, log)
          assertNumBackfillCommits(expectedNumCommits = 0)
        }

        // Now, let's test UNSET TBLPROPERTIES
        val rowTrackingKey = DeltaConfigs.ROW_TRACKING_ENABLED.key
        sql(s"ALTER TABLE $testTableName UNSET TBLPROPERTIES('$rowTrackingKey')")
        // This should not downgrade the table and it should not trigger any backfill.
        // It will only change the table property.
        snapshot = log.update()
        assertNumBackfillCommits(expectedNumCommits = 0)
        assert(RowTracking.isSupported(snapshot.protocol))
        assert(!RowId.isEnabled(snapshot.protocol, snapshot.metadata))
        val prevMaterializedColumnName = extractMaterializedRowIdColumnName(log)
        // If we re-enable the table property again, we should expect the materialized column name
        // to be the same.
        validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 0) {
          triggerBackfillOnTestTableUsingAlterTable(testTableName, initialNumRows, log)
        }
        assert(prevMaterializedColumnName === extractMaterializedRowIdColumnName(log))
      }
    }
  }

  test("Backfill on table that enabled the table feature separately") {
    withRowIdDisabledTestTable() {
      val log = DeltaLog.forTable(spark, TableIdentifier(testTableName))
      // User decides to enable the table feature separately first.
      sql(
        s"""ALTER TABLE $testTableName
           |SET TBLPROPERTIES('$rowTrackingFeatureName' = 'supported')""".stripMargin)
      val snapshotAfterTableSupport = log.update()
      assert(RowTracking.isSupported(snapshotAfterTableSupport.protocol))
      assert(!RowId.isEnabled(
        snapshotAfterTableSupport.protocol, snapshotAfterTableSupport.metadata))

      val deltaHistory = log.history.getHistory(None)
      // 1 commit to create table, 1 commit to upgrade protocol
      assert(deltaHistory.size === 2)

      // New data is inserted into the table. The new files should have base row ID.
      val numNewRowsInserted = 1
      spark.range(end = numNewRowsInserted).write.mode("append").insertInto(testTableName)
      val snapshotAfterInsert = log.update()
      assert(snapshotAfterInsert.allFiles.count() === numFilesInTable + 1)
      val numRowsInTableAfterInsert = initialNumRows + numNewRowsInserted

      // Trigger backfill. Only the files before the table feature support should need backfill.
      validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 1) {
        triggerBackfillOnTestTableUsingAlterTable(testTableName, numRowsInTableAfterInsert, log)
        assertBackfillWasSuccessful(expectedNumCommits = 1)
      }

      // We should not try to upgrade the protocol again.
      val deltaHistory2 = log.history.getHistory(None)
      // We should have 3 more commits since the protocol upgrade:
      // 1 commit to insert, 1 commit to backfill, 1 commit to set tbl properties.
      assert(deltaHistory2.size === 5)
      assertRowIdsAreValid(log)
    }
  }

  test("Backfill should be idempotent") {
    withRowIdDisabledTestTable() {
      val log = DeltaLog.forTable(spark, TableIdentifier(testTableName))
      validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 1) {
        triggerBackfillOnTestTableUsingAlterTable(testTableName, initialNumRows, log)
      }

      val snapshot = log.update()
      assert(RowId.isEnabled(snapshot.protocol, snapshot.metadata))

      val materializedColumnName = extractMaterializedRowIdColumnName(log)
      val deltaHistory = log.history.getHistory(None)
      // 1 commit to create table, 1 commit to upgrade protocol, 1 commit for Backfill,
      // 1 commit to set row tracking to enabled.
      assert(deltaHistory.size === 4)

      // We should not upgrade the protocol again and we should not backfill.
      validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 0) {
        triggerBackfillOnTestTableUsingAlterTable(testTableName, initialNumRows, log)
      }
      assert(extractMaterializedRowIdColumnName(log) === materializedColumnName,
        "the materialized column name should not change")
      val deltaHistory2 = log.history.getHistory(None)
      // 1 more commit to SET TBLPROPERTIES, nothing else.
      assert(deltaHistory2.size === 5)
      assert(deltaHistory2.head.operation === DeltaOperations.OP_SET_TBLPROPERTIES)
    }
  }

  test("ALTER TABLE that don't enable row tracking should not backfill") {
    withRowIdDisabledTestTable() {
      val rowTrackingKey = DeltaConfigs.ROW_TRACKING_ENABLED.key
      sql(s"ALTER TABLE $testTableName SET TBLPROPERTIES('$rowTrackingKey' = false)")
      // This should not upgrade the table protocol and it should not trigger any backfill.
      // It will only set the table property.
      val (log, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(testTableName))
      assertNumBackfillCommits(expectedNumCommits = 0)
      assert(!RowTracking.isSupported(snapshot.protocol))
      assert(!RowId.isEnabled(snapshot.protocol, snapshot.metadata))
      assert(!lastCommitHasRowTrackingEnablementOnlyTag(log),
          "RowTrackingEnablementOnly tag should not be set if the table property value is false")
    }
  }

  test("Trigger backfill using ALTER TABLE, but another property fails") {
    withRowIdDisabledTestTable() {
      // Enable column mapping
      val columnMappingMode = DeltaConfigs.COLUMN_MAPPING_MODE.key
      val minReaderKey = DeltaConfigs.MIN_READER_VERSION.key
      val minWriterKey = DeltaConfigs.MIN_WRITER_VERSION.key
      val rowTrackingKey = DeltaConfigs.ROW_TRACKING_ENABLED.key
      sql(s"""ALTER TABLE $testTableName SET TBLPROPERTIES(
             |'$minReaderKey' = '2',
             |'$minWriterKey' = '5',
             |'$columnMappingMode'='name')""".stripMargin)

      val log = DeltaLog.forTable(spark, TableIdentifier(testTableName))
      assert(!lastCommitHasRowTrackingEnablementOnlyTag(log),
          "RowTrackingEnablementOnly tag should not be set for other table properties")

      // Try to enable row IDs at the same time as we set column mapping mode to id.
      // This should fail due to illegal column mapping mode change.
      intercept[ColumnMappingUnsupportedException] {
        sql(s"ALTER TABLE $testTableName SET " +
          s"TBLPROPERTIES('$columnMappingMode'='id', '$rowTrackingKey'=true)")
      }
      // Despite the failure, there are side effects: the protocol is still upgraded and
      // backfill commits occurred. However, row IDs should still be disabled because we were unable
      // to set the property.
      val snapshot = log.update()
      assert(RowTracking.isSupported(snapshot.protocol))
      assertNumBackfillCommits(expectedNumCommits = 1)
      assert(!RowId.isEnabled(snapshot.protocol, snapshot.metadata))
    }
  }

  /**
   * Validate that if a user triggers backfill with other protocol changes within the ALTER TABLE
   * command, we end up with a correct final protocol object.
   *
   * @param tableBelowTableFeatureLevel : Boolean indicating whether the test table has a protocol
   *                                    writer version below table feature support prior to calling
   *                                    the ALTER TABLE command that triggers backfill.
   * @param isOtherTableFeatureLegacy   : Boolean indicating whether the other table feature being
   *                                    supported along row tracking enablement is legacy (i.e. it
   *                                    requires minWriterVersion and minReaderVersion below table
   *                                    feature level).
   */
  private def checkProtocolUpgradeWithBackfill(
      tableBelowTableFeatureLevel: Boolean,
      isOtherTableFeatureLegacy: Boolean): Unit = {
    val initialMinReaderVersion = ColumnMappingTableFeature.minReaderVersion
    val initialMinWriterVersion = ColumnMappingTableFeature.minWriterVersion
    withSQLConf(
      DeltaConfigs.MIN_WRITER_VERSION.defaultTablePropertyKey -> initialMinWriterVersion.toString,
      DeltaConfigs.MIN_READER_VERSION.defaultTablePropertyKey -> initialMinReaderVersion.toString
    ) {
      withRowIdDisabledTestTable() {
        val (log, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(testTableName))
        val prevProtocol = snapshot.protocol
        val expectedInitialProtocol = Protocol(initialMinReaderVersion, initialMinWriterVersion)
        assert(prevProtocol === expectedInitialProtocol)

        // Build the TBLPROPERTIES String for the ALTER TABLE command.
        val rowTrackingKey = DeltaConfigs.ROW_TRACKING_ENABLED.key
        var propertiesMap: Map[String, String] = Map(rowTrackingKey -> "true")
        if (isOtherTableFeatureLegacy) {
          val columnMappingMode = DeltaConfigs.COLUMN_MAPPING_MODE.key
          propertiesMap = propertiesMap ++ Map(columnMappingMode -> "name")
        } else {
          val deletionVectorsKey = DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key
          propertiesMap = propertiesMap ++ Map(deletionVectorsKey -> "true")
        }
        val tblPropertiesString = propertiesMap.map {
          case (key, value) => s"'$key'='$value'"
        }.mkString(", ")

        sql(s"ALTER TABLE $testTableName SET TBLPROPERTIES($tblPropertiesString)")
        assert(!lastCommitHasRowTrackingEnablementOnlyTag(log),
            "RowTrackingEnablementOnly tag should not be set if ALTER TABLE is changing" +
            " multiple table properties")

        val expectedFinalProtocol = if (isOtherTableFeatureLegacy) {
          Protocol(
            minReaderVersion = ColumnMappingTableFeature.minReaderVersion,
            minWriterVersion = TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION)
            .withFeature(RowTrackingFeature)
            .withFeature(ColumnMappingTableFeature)
            .merge(prevProtocol)
        } else {
          Protocol(
            minReaderVersion = TableFeatureProtocolUtils.TABLE_FEATURES_MIN_READER_VERSION,
            minWriterVersion = TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION)
            .withFeature(RowTrackingFeature)
            .withFeature(DeletionVectorsTableFeature)
            .merge(prevProtocol)
        }

        assertBackfillWasSuccessful(expectedNumCommits = 1)
        val finalSnapshot = log.update()
        assert(finalSnapshot.protocol === expectedFinalProtocol)
      }
    }
  }

  for {
    tableBelowTableFeatureLevel <- BOOLEAN_DOMAIN
    isOtherTableFeatureLegacy <- BOOLEAN_DOMAIN
  } {
    test("ALTER TABLE does other protocol upgrade with backfill, " +
      s"tableBelowTableFeatureLevel=$tableBelowTableFeatureLevel, " +
      s"isOtherTableFeatureLegacy=$isOtherTableFeatureLegacy") {
      checkProtocolUpgradeWithBackfill(tableBelowTableFeatureLevel, isOtherTableFeatureLegacy)
    }
  }

  test("lower MIN_WRITER_VERSION along with Row ID prop can upgrade protocol") {
    withRowIdDisabledTestTable() {
      val (log, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(testTableName))
      val prevProtocol = snapshot.protocol
      assert(prevProtocol.minWriterVersion <
        TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION)

      // Try to enable row IDs at the same time as we enable column mapping mode.
      val columnMappingMode = DeltaConfigs.COLUMN_MAPPING_MODE.key
      val minReaderKey = DeltaConfigs.MIN_READER_VERSION.key
      val minWriterKey = DeltaConfigs.MIN_WRITER_VERSION.key
      val rowTrackingKey = DeltaConfigs.ROW_TRACKING_ENABLED.key
      // If we specify lower minWriterKey along with the row tracking prop, we will
      // do an implicit upgrade to minWriterKey = 7.
      sql(
        s"""ALTER TABLE $testTableName SET TBLPROPERTIES(
           |'$minReaderKey' = '2',
           |'$minWriterKey' = '5',
           |'$columnMappingMode'='name',
           |'$rowTrackingKey'=true
           |)""".stripMargin)
      val afterProtocol = log.update().protocol
      assert(afterProtocol.minReaderVersion === 2)
      assert(
        afterProtocol.minWriterVersion ===
          TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION)
      assert(afterProtocol.readerFeatures === None)
      assert(
        afterProtocol.writerFeatures === Some((
          prevProtocol.implicitlyAndExplicitlySupportedFeatures ++
            Protocol(2, 5).implicitlySupportedFeatures ++
            Set(
              ColumnMappingTableFeature,
              DomainMetadataTableFeature, // Required by Row Tracking
              RowTrackingFeature))
          .map(_.name)))
    }
  }

  test("Backfill works with more than 1 batch in parallel") {
    val expectedNumBackfillCommits = 4
    val maxNumFilesPerCommit = Math.ceil(
      numFilesInTable.toDouble / expectedNumBackfillCommits.toDouble).toInt
    val maxNumBatchesInParallel = expectedNumBackfillCommits
    withRowIdDisabledTestTable() {
      withSQLConf(
        DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT.key ->
          maxNumFilesPerCommit.toString,
        DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_BATCHES_IN_PARALLEL.key ->
          maxNumBatchesInParallel.toString
      ) {
        validateSuccessfulBackfillMetrics(expectedNumBackfillCommits) {
          val log = DeltaLog.forTable(spark, TableIdentifier(testTableName))
          triggerBackfillOnTestTableUsingAlterTable(testTableName, initialNumRows, log)
        }
      }
    }
  }

  test("BackfillCommandStats metrics are correct in case of failure") {
    withRowIdDisabledTestTable() {
      val (log, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(testTableName))

      val allFiles = snapshot.allFiles.collect()
      val numFilesInSuccessfulBackfillBatch = 3
      val filesInSuccessfulBackfill = allFiles.take(numFilesInSuccessfulBackfillBatch)
      val numFilesInFailingBackfillBatch = 2
      val filesInFailingBackfill = allFiles.takeRight(numFilesInFailingBackfillBatch)
      val maxNumBatchesInParallel = 1
      // ordered list in order to force the successful batch to execute before the failing batch.
      val batchIterator = List(
        RowTrackingBackfillBatch(filesInSuccessfulBackfill),
        FailingRowTrackingBackfillBatch(filesInFailingBackfill)
      ).iterator

      val txn = log.startTransaction()
      val backfillStats = BackfillCommandStats(
        transactionId = txn.txnId,
        nameOfTriggeringOperation = DeltaOperations.OP_SET_TBLPROPERTIES,
        maxNumBatchesInParallel = maxNumBatchesInParallel)
      val backfillExecutor = new RowTrackingBackfillExecutor(
        spark,
        txn,
        FileMetadataMaterializationTracker.noopTracker,
        maxNumBatchesInParallel,
        backfillStats
      )

      val backfillUsageRecords = Log4jUsageLogger.track {
        intercept[ExecutionException] {
          backfillExecutor.run(batchIterator)
        }
      }.filter(_.metric == "tahoeEvent")

      val backfillBatchRecords = backfillUsageRecords
        .filter(_.tags.get("opType").contains(DeltaUsageLogsOpTypes.BACKFILL_BATCH))
      val backfillBatchStatsSeq = backfillBatchRecords.map { backfillBatchRecord =>
        JsonUtils.fromJson[BackfillBatchStats](backfillBatchRecord.blob)
      }

      // Check parent backfill stats. The total execution time and wasSuccessful are manipulated in
      // RowTrackingBackfillCommand, not BackfillExecutor so it is still 0 and false respectively.
      assert(backfillStats.numFailedBatches === 1)
      assert(backfillStats.numSuccessfulBatches === 1)
      assert(backfillStats.maxNumBatchesInParallel === maxNumBatchesInParallel)
      assert(backfillStats.transactionId === txn.txnId)
      assert(backfillStats.nameOfTriggeringOperation === DeltaOperations.OP_SET_TBLPROPERTIES)

      // Check the individual batch stats
      assert(backfillBatchStatsSeq.size === 2)
      backfillBatchStatsSeq.foreach { backfillBatchStat =>
        backfillBatchStat.batchId match {
          case 0 =>
            assert(backfillBatchStat.wasSuccessful)
            assert(backfillBatchStat.initialNumFiles === numFilesInSuccessfulBackfillBatch)
            assert(backfillBatchStat.totalExecutionTimeInMs > 0)
          case 1 =>
            assert(!backfillBatchStat.wasSuccessful)
            assert(backfillBatchStat.initialNumFiles === numFilesInFailingBackfillBatch)
            // Failing batch can have totalExecutionTimeInMs be 0 because it ends faster.
            assert(backfillBatchStat.totalExecutionTimeInMs >= 0)
          case id => fail(s"Unexpected batch id $id for RowTrackingBackfillBatch.")
        }
        assert(backfillBatchStat.parentTransactionId === txn.txnId)
      }
    }
  }

  test("BackfillBatchStats failure leads to correct metrics") {
    withRowIdDisabledTestTable() {
      val table = DeltaTableV2(spark, TableIdentifier(testTableName))

      val filesInBackfillBatch = table.snapshot.allFiles.head(2)
      val batch = FailingRowTrackingBackfillBatch(filesInBackfillBatch)
      val batchId = 17
      val numSuccessfulBatch = new AtomicInteger(0)
      val numFailedBatch = new AtomicInteger(0)

      val txn = table.startTransactionWithInitialSnapshot()

      val backfillUsageRecords = Log4jUsageLogger.track {
        intercept[IllegalStateException] {
          batch.execute(txn, batchId, numSuccessfulBatch, numFailedBatch)
        }
      }.filter(_.metric == "tahoeEvent")

      val backfillBatchRecords = backfillUsageRecords
        .filter(_.tags.get("opType").contains(DeltaUsageLogsOpTypes.BACKFILL_BATCH))
      assert(backfillBatchRecords.size === 1, "Row Tracking Backfill should have " +
        "only been executed once.")
      val backfillBatchStats =
        JsonUtils.fromJson[BackfillBatchStats](backfillBatchRecords.head.blob)

      assert(numSuccessfulBatch.get() === 0)
      assert(numFailedBatch.get() === 1)
      assert(backfillBatchStats.batchId === batchId)
      assert(!backfillBatchStats.wasSuccessful)
      assert(backfillBatchStats.initialNumFiles === filesInBackfillBatch.length)
      // Failing batch can have totalExecutionTimeInMs be 0 because it ends faster.
      assert(backfillBatchStats.totalExecutionTimeInMs >= 0)
      assert(backfillBatchStats.parentTransactionId === txn.txnId)
      assert(backfillBatchStats.transactionId != null && backfillBatchStats.transactionId.nonEmpty)
    }
  }
}
