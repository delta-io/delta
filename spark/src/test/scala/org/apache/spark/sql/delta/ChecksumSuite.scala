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
import java.util.{Locale, TimeZone}

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.DeltaTestUtils._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTestBaseSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

class ChecksumSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaTestUtilsBase
  with DeltaSQLCommandTest
  with DeltaSQLTestUtils
  with CatalogOwnedTestBaseSuite {

  override def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.INCREMENTAL_COMMIT_FORCE_VERIFY_IN_TESTS, false)

  test(s"A Checksum should be written after every commit when " +
    s"${DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key} is true") {
    def testChecksumFile(writeChecksumEnabled: Boolean): Unit = {
      // Set up the log by explicitly creating the table otherwise we can't
      // construct the DeltaLog via the table name.
      withTempTable(createTable = true) { tableName =>
        withSQLConf(
          DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> writeChecksumEnabled.toString) {
          def checksumExists(deltaLog: DeltaLog, version: Long): Boolean = {
            val checksumFile = new File(FileNames.checksumFile(deltaLog.logPath, version).toUri)
            checksumFile.exists()
          }

          // Commit the txn
          val log = DeltaLog.forTable(spark, TableIdentifier(tableName))
          val txn = log.startTransaction(log.initialCatalogTable)
          val txnCommitVersion = txn.commit(Seq.empty, DeltaOperations.Truncate())
          assert(checksumExists(log, txnCommitVersion) == writeChecksumEnabled)
        }
      }
    }

    testChecksumFile(writeChecksumEnabled = true)
    testChecksumFile(writeChecksumEnabled = false)
  }

  private def setTimeZone(timeZone: String): Unit = {
    spark.sql(s"SET spark.sql.session.timeZone = $timeZone")
    TimeZone.setDefault(TimeZone.getTimeZone(timeZone))
  }

  test("Incremental checksums: post commit snapshot should have a checksum " +
      "without triggering state reconstruction") {
    for (incrementalCommitEnabled <- BOOLEAN_DOMAIN) {
      withSQLConf(
        DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "false",
        DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> incrementalCommitEnabled.toString,
        DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_FORCE_VERIFICATION_MODE_FOR_NON_UTC_ENABLED.key ->
          "false"
      ) {
        withTempTable(createTable = false) { tableName =>
          // Set the timezone to UTC to avoid triggering force verification of all files in CRC
          // for non utc environments.
          setTimeZone("UTC")
          val df = spark.range(1)
          df.write.format("delta").mode("append").saveAsTable(tableName)
          val log = DeltaLog.forTable(spark, TableIdentifier(tableName))
          log
            .startTransaction()
            .commit(Seq(createTestAddFile()), DeltaOperations.Write(SaveMode.Append))
          val postCommitSnapshot = log.snapshot
          assert(postCommitSnapshot.version == 1)
          assert(!postCommitSnapshot.stateReconstructionTriggered)
          assert(postCommitSnapshot.checksumOpt.isDefined == incrementalCommitEnabled)

          postCommitSnapshot.checksumOpt.foreach { incrementalChecksum =>
            val checksumFromStateReconstruction = postCommitSnapshot.computeChecksum
            assert(incrementalChecksum.copy(txnId = None) == checksumFromStateReconstruction)
          }
        }
      }
    }
  }

  def testIncrementalChecksumWrites(tableMutationOperation: String => Unit): Unit = {
    withTempTable(createTable = false) { tableName =>
      withSQLConf(
        DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "true",
        DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key ->"true") {
        val df = spark.range(10).withColumn("id2", col("id") % 2)
        df.write
          .format("delta")
          .partitionBy("id")
          .mode("append")
          .saveAsTable(tableName)

        tableMutationOperation(tableName)
        val log = DeltaLog.forTable(spark, TableIdentifier(tableName))
        val checksumOpt = log.snapshot.checksumOpt
        assert(checksumOpt.isDefined)
        val checksum = checksumOpt.get
        val computedChecksum = log.snapshot.computeChecksum
        assert(checksum.copy(txnId = None) === computedChecksum)
      }
    }
  }

  test("Incremental checksums: INSERT") {
    testIncrementalChecksumWrites { tableName =>
      sql(s"INSERT INTO $tableName SELECT *, 1 FROM range(10, 20)")
    }
  }

  test("Incremental checksums: UPDATE") {
    testIncrementalChecksumWrites { tableName =>
      sql(s"UPDATE $tableName SET id2 = id + 1 WHERE id % 2 = 0")
    }
  }

  test("Incremental checksums: DELETE") {
    testIncrementalChecksumWrites { tableName =>
      sql(s"DELETE FROM $tableName WHERE id % 2 = 0")
    }
  }

  test("Checksum validation should happen on checkpoint") {
    withSQLConf(
      DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "true",
      DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
      // Disabled for this test because with it enabled, a corrupted Protocol
      // or Metadata will trigger a failure earlier than the full validation.
      DeltaSQLConf.USE_PROTOCOL_AND_METADATA_FROM_CHECKSUM_ENABLED.key -> "false"
    ) {
      withTempTable(createTable = false) { tableName =>
        spark
          .range(10)
          .write
          .format("delta")
          .saveAsTable(tableName)
        spark.range(1)
          .write
          .format("delta")
          .mode("append")
          .saveAsTable(tableName)
        val log = DeltaLog.forTable(spark, TableIdentifier(tableName))
        val checksumOpt = log.readChecksum(1)
        assert(checksumOpt.isDefined)
        val checksum = checksumOpt.get
        // Corrupt the checksum file.
        val corruptedChecksum = checksum.copy(
          protocol =
            checksum.protocol.copy(minReaderVersion = checksum.protocol.minReaderVersion + 1),
          metadata = checksum.metadata.copy(description = "corrupted"),
          numProtocol = 2,
          numMetadata = 2,
          tableSizeBytes = checksum.tableSizeBytes + 1,
          numFiles = checksum.numFiles + 1)
        val corruptedChecksumJson = JsonUtils.toJson(corruptedChecksum)
        log.store.write(
          FileNames.checksumFile(log.logPath, 1),
          Seq(corruptedChecksumJson).toIterator,
          overwrite = true)
        DeltaLog.clearCache()
        val log2 = DeltaLog.forTable(spark, TableIdentifier(tableName))
        val usageLogs = Log4jUsageLogger.track {
          intercept[DeltaIllegalStateException] {
            log2.checkpoint()
          }
        }
        val validationFailureLogs = filterUsageRecords(usageLogs, "delta.checksum.invalid")
        assert(validationFailureLogs.size == 1)
        validationFailureLogs.foreach { log =>
          val usageLogBlob = JsonUtils.fromJson[Map[String, Any]](log.blob)
          val mismatchingFieldsOpt = usageLogBlob.get("mismatchingFields")
          assert(mismatchingFieldsOpt.isDefined)
          val mismatchingFieldsSet = mismatchingFieldsOpt.get.asInstanceOf[Seq[String]].toSet
          val expectedMismatchingFields = Set(
            "protocol",
            "metadata",
            "numOfProtocol",
            "numOfMetadata",
            "tableSizeBytes",
            "numFiles"
          )
          assert(mismatchingFieldsSet === expectedMismatchingFields)
        }
      }
    }
  }

  test("incremental commit verify mode should always detect invalid .crc") {
    withSQLConf(
      DeltaSQLConf.INCREMENTAL_COMMIT_VERIFY.key -> "true",
      DeltaSQLConf.DELTA_CHECKSUM_MISMATCH_IS_FATAL.key -> "false",
      DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key -> "false",
      DeltaSQLConf.USE_PROTOCOL_AND_METADATA_FROM_CHECKSUM_ENABLED.key -> "true"
    ) {
      // Explicitly create the table w/o any AddFile for the subsequent
      // DeltaLog construction.
      withTempTable(createTable = true) { tableName =>
        import testImplicits._
        val numAddFiles = 10

        // Procedure:
        // 1. Populate the table with several files
        // 2. Start a new transaction
        // 3. Intentionally try to commit the same files again
        //    a. Silently duplicated AddFile breaks incremental commit invariants
        //    b. Incrementally computed .crc is thus invalid
        //    c. Incremental commit verification should detect the "invalid" .crc
        //    d. Post-commit snapshot should have empty checksumOpt
        // 4. Clear the delta log cache so we pick up the correct (fallback) .crc
        // 5. Create a new snapshot and manually validate the .crc

        val files = (1 to numAddFiles).map(i => createTestAddFile(encodedPath = i.toString))
        DeltaLog
          .forTable(spark, TableIdentifier(tableName))
          .startTransaction()
          .commitWriteAppend(files: _*)

        val log = DeltaLog.forTable(spark, TableIdentifier(tableName))
        val txn = log.startTransaction()
        val expected =
          s"""
             |FileSizeHistogram mismatch in file sizes
             |FileSizeHistogram mismatch in file counts
             |Table size (bytes) - Expected: ${2*numAddFiles} Computed: $numAddFiles
             |Number of files - Expected: ${2*numAddFiles} Computed: $numAddFiles
          """.stripMargin.trim

        val Seq(corruptionReport) = collectUsageLogs("delta.checksum.invalid") {
          // Intentionally re-add the same files, without identifying them as duplicates
          txn.commitWriteAppend(files: _*)
        }
        val error = JsonUtils.fromJson[Map[String, Any]](corruptionReport.blob).get("error")
        assert(error.exists(_.asInstanceOf[String].contains(expected)))
        assert(log.snapshot.checksumOpt.isEmpty)
      }
    }
  }

  test("force checksum validation due to stale checkpoint") {
    withSQLConf(
      DeltaSQLConf.INCREMENTAL_COMMIT_VERIFY.key -> "false",
      // Set this to 0 to ensure that validation is not
      // skipped due the checkpoint not being old enough
      DeltaSQLConf.FORCED_CHECKSUM_VALIDATION_MIN_TIME_INTERVAL_MINUTES.key -> "0",
      DeltaSQLConf.DELTA_CHECKSUM_MISMATCH_IS_FATAL.key -> "true",
      DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key -> "false",
      DeltaSQLConf.FORCED_CHECKSUM_VALIDATION_INTERVAL.key -> "999"
    ) {
      withTempTable(createTable = false) { tableName =>
        spark
          .range(4)
          .write
          .format("delta")
          .saveAsTable(tableName)
        // Create checkpoint at version 0
        DeltaLog.forTable(spark, TableIdentifier(tableName)).checkpoint()
        def validateAttemptedTransactionFails: Unit = {
          DeltaLog.clearCache()
          val usageLogs = Log4jUsageLogger.track {
            intercept[DeltaIllegalStateException] {
              DeltaLog
              .forTable(spark, TableIdentifier(tableName))
              .startTransaction()
            }
          }
          val validationFailureLogs = filterUsageRecords(usageLogs, "delta.checksum.invalid")
          assert(validationFailureLogs.size == 1)
          validationFailureLogs.foreach { log =>
            val usageLogBlob = JsonUtils.fromJson[Map[String, Any]](log.blob)
            val mismatchingFieldsOpt = usageLogBlob.get("mismatchingFields")
            assert(mismatchingFieldsOpt.isDefined)
            val mismatchingFieldsSet = mismatchingFieldsOpt.get.asInstanceOf[Seq[String]].toSet
            val expectedMismatchingFields = Set(
              "numOfProtocol",
              "numOfMetadata",
              "tableSizeBytes",
              "numFiles"
            )
            assert(mismatchingFieldsSet === expectedMismatchingFields)
          }
        }
        // Write 4 commits. Also, corrupt every checksum file.
        (1 to 4).foreach { version =>
          spark.range(1)
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(tableName)
          // Corrupt the checksum file.
          val log = DeltaLog
            .forTable(spark, TableIdentifier(tableName))
          val checksum = log.readChecksum(version).get
          val corruptedChecksum = checksum.copy(
            numProtocol = 2,
            numMetadata = 2,
            tableSizeBytes = checksum.tableSizeBytes + 1,
            numFiles = checksum.numFiles + 1)
          val corruptedChecksumJson = JsonUtils.toJson(corruptedChecksum)
          log.store.write(
            FileNames.checksumFile(log.logPath, version),
            Seq(corruptedChecksumJson).toIterator,
            overwrite = true)

          withSQLConf(
            // Set the forced checksum validation interval to the current version
            // so that validation is triggered
            DeltaSQLConf.FORCED_CHECKSUM_VALIDATION_INTERVAL.key -> version.toString
          ) {
            validateAttemptedTransactionFails
          }
          withSQLConf(
            // Set the validation interval to a value smaller than the current version
            // so that validation is triggered
            DeltaSQLConf.FORCED_CHECKSUM_VALIDATION_INTERVAL.key -> "0"
          ) {
            validateAttemptedTransactionFails
          }
          withSQLConf(
            DeltaSQLConf.FORCED_CHECKSUM_VALIDATION_INTERVAL.key -> "0",
            // Validation should only be triggered if the checkpoint was
            // created more than 999 minutes ago. Which should not be
            // the case here.
            DeltaSQLConf.FORCED_CHECKSUM_VALIDATION_MIN_TIME_INTERVAL_MINUTES.key -> "999"
          ) {
            DeltaLog.clearCache()
            DeltaLog
              .forTable(spark, TableIdentifier(tableName))
              .startTransaction()
          }
        }
      }
    }
  }

  test("SnapshotState fast path from CRC: produces same state as state reconstruction") {
    withSQLConf(
      DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "true",
      DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key -> "true"
    ) {
      withTempTable(createTable = false) { tableName =>
        spark.range(10).withColumn("id2", col("id") % 2)
          .write.format("delta").partitionBy("id").saveAsTable(tableName)
        sql(s"INSERT INTO $tableName SELECT *, 1 FROM range(10, 20)")

        // First, capture the state reconstruction result by loading with the fast path off.
        DeltaLog.clearCache()
        val baselineState = withSQLConf(
          DeltaSQLConf.USE_SNAPSHOT_STATE_FROM_CHECKSUM_ENABLED.key -> "false") {
          val log = DeltaLog.forTable(spark, TableIdentifier(tableName))
          val s = log.update()
          assert(s.checksumOpt.isDefined, "test setup should have produced a CRC file")
          val state = s.computedStateFromStateReconstruction
          assert(s.stateReconstructionTriggered)
          state
        }

        // Now reload with the fast path on and confirm equivalence.
        DeltaLog.clearCache()
        withSQLConf(
          DeltaSQLConf.USE_SNAPSHOT_STATE_FROM_CHECKSUM_ENABLED.key -> "true") {
          val log = DeltaLog.forTable(spark, TableIdentifier(tableName))
          val s = log.update()
          assert(s.checksumOpt.isDefined)
          assert(!s.stateReconstructionTriggered,
            "state reconstruction should not have run before computedState is accessed")

          val fastState = s.computeStateFromChecksum
          assert(fastState.isDefined, "fast path should succeed when CRC has all required fields")
          assert(!s.stateReconstructionTriggered,
            "computeStateFromChecksum must not trigger state reconstruction")

          val fast = fastState.get
          assert(fast.sizeInBytes == baselineState.sizeInBytes)
          assert(fast.numOfFiles == baselineState.numOfFiles)
          assert(fast.numOfMetadata == baselineState.numOfMetadata)
          assert(fast.numOfProtocol == baselineState.numOfProtocol)
          assert(fast.numOfSetTransactions == baselineState.numOfSetTransactions)
          assert(fast.setTransactions.toSet == baselineState.setTransactions.toSet)
          assert(fast.domainMetadata.toSet == baselineState.domainMetadata.toSet)
          assert(fast.metadata == baselineState.metadata)
          assert(fast.protocol == baselineState.protocol)

          // computedState should use the fast path (verified by no state reconstruction).
          val computed = s.computedState
          assert(!s.stateReconstructionTriggered)
          assert(computed.numOfFiles == baselineState.numOfFiles)

          // The numOfRemoves lazy val resolves from `stateDS` on first access. For this
          // table (no deletes) it is zero and matches state-reconstruction's value.
          assert(s.numOfRemoves == 0L)
        }
      }
    }
  }

  test("SnapshotState fast path from CRC: domainMetadata excludes removed (tombstoned) " +
       "domains and matches state reconstruction") {
    withSQLConf(
      DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "true",
      DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key -> "true"
    ) {
      withTempTable(createTable = false) { tableName =>
        sql(
          s"""
             | CREATE TABLE $tableName (id LONG) USING delta
             | TBLPROPERTIES
             | ('${TableFeatureProtocolUtils.propertyKey(DomainMetadataTableFeature)}' = 'enabled')
             |""".stripMargin)
        val deltaTable = DeltaTableV2(spark, TableIdentifier(tableName))
        // Add two domains, then remove one. The active state therefore contains a domain
        // whose add precedes a later removal tombstone -- the case that distinguishes
        // "all domain metadata" from "live domain metadata".
        deltaTable.startTransactionWithInitialSnapshot().commit(
          DomainMetadata("testDomain1", "", removed = false) ::
            DomainMetadata("testDomain2", "", removed = false) :: Nil,
          Truncate())
        deltaTable.startTransaction().commit(
          DomainMetadata("testDomain1", "", removed = true) :: Nil, Truncate())

        val activeDomain = DomainMetadata("testDomain2", "", removed = false)

        // Baseline via state reconstruction (fast path off): the removed domain is dropped.
        DeltaLog.clearCache()
        val baseline = withSQLConf(
          DeltaSQLConf.USE_SNAPSHOT_STATE_FROM_CHECKSUM_ENABLED.key -> "false") {
          val s = DeltaLog.forTable(spark, TableIdentifier(tableName)).update()
          val state = s.computedStateFromStateReconstruction
          assert(state.domainMetadata.toSet == Set(activeDomain),
            s"state reconstruction should drop the removed domain, got ${state.domainMetadata}")
          state
        }

        // Fast path on: the CRC persists only the live domain, so the fast-path
        // SnapshotState.domainMetadata must equal state reconstruction's (no tombstone).
        DeltaLog.clearCache()
        withSQLConf(
          DeltaSQLConf.USE_SNAPSHOT_STATE_FROM_CHECKSUM_ENABLED.key -> "true") {
          val s = DeltaLog.forTable(spark, TableIdentifier(tableName)).update()
          assert(s.checksumOpt.flatMap(_.domainMetadata).map(_.toSet).contains(Set(activeDomain)),
            "the CRC should persist only the live domain metadata")
          val fast = s.computeStateFromChecksum
          assert(fast.isDefined, "fast path should succeed when the CRC has domain metadata")
          assert(!s.stateReconstructionTriggered,
            "computeStateFromChecksum must not trigger state reconstruction")
          assert(fast.get.domainMetadata.toSet == baseline.domainMetadata.toSet)
          assert(!fast.get.domainMetadata.exists(_.domain == "testDomain1"),
            "the removed domain must not appear in the fast-path state")
        }
      }
    }
  }

  test("SnapshotState fast path from CRC: numOfRemoves is lazily and exactly computed " +
       "when tombstones exist") {
    withSQLConf(
      DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "true",
      DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key -> "true"
    ) {
      withTempTable(createTable = false) { tableName =>
        // Build a table with tombstones by overwriting an initial dataset, so the active
        // state contains real RemoveFile actions whose count is > 0.
        spark.range(20).withColumn("g", col("id") % 4)
          .write.format("delta").partitionBy("g").saveAsTable(tableName)
        spark.range(10, 30).withColumn("g", col("id") % 4)
          .write.format("delta").mode("overwrite").partitionBy("g").saveAsTable(tableName)
        sql(s"DELETE FROM $tableName WHERE id >= 25")

        // Baseline: capture the exact numOfRemoves via state reconstruction.
        DeltaLog.clearCache()
        val baselineRemoves = withSQLConf(
          DeltaSQLConf.USE_SNAPSHOT_STATE_FROM_CHECKSUM_ENABLED.key -> "false") {
          val log = DeltaLog.forTable(spark, TableIdentifier(tableName))
          val s = log.update()
          val n = s.numOfRemoves
          assert(s.stateReconstructionTriggered)
          assert(n > 0L, s"test setup should produce tombstones, got $n")
          n
        }

        // Fast path: accessing computedState must not trigger state reconstruction. Reading
        // numOfRemoves triggers it exactly once and returns the exact baseline value.
        DeltaLog.clearCache()
        withSQLConf(
          DeltaSQLConf.USE_SNAPSHOT_STATE_FROM_CHECKSUM_ENABLED.key -> "true") {
          val log = DeltaLog.forTable(spark, TableIdentifier(tableName))
          val s = log.update()
          assert(s.checksumOpt.isDefined)

          // Touching computedState via the fast path must not trigger state reconstruction.
          val _ = s.computedState
          assert(!s.stateReconstructionTriggered,
            "fast path must not trigger state reconstruction")

          // The lazy val triggers exactly one computation that materializes the state
          // reconstruction DataFrame and returns the precise count.
          val resolved = s.numOfRemoves
          assert(s.stateReconstructionTriggered,
            "accessing numOfRemoves should have triggered state reconstruction")
          assert(resolved == baselineRemoves,
            s"lazy numOfRemoves $resolved should equal baseline $baselineRemoves")

          // Subsequent accesses are cached.
          assert(s.numOfRemoves == baselineRemoves)
        }
      }
    }
  }

  test("SnapshotState fast path from CRC: falls back when conf is disabled") {
    withSQLConf(
      DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "true",
      DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
      DeltaSQLConf.USE_SNAPSHOT_STATE_FROM_CHECKSUM_ENABLED.key -> "false"
    ) {
      withTempTable(createTable = false) { tableName =>
        spark.range(5).write.format("delta").saveAsTable(tableName)
        DeltaLog.clearCache()
        val log = DeltaLog.forTable(spark, TableIdentifier(tableName))
        val s = log.update()
        assert(s.checksumOpt.isDefined)
        assert(s.computeStateFromChecksum.isEmpty,
          "fast path should not be taken when conf is disabled")
      }
    }
  }

  test("SnapshotState fast path from CRC: falls back when CRC lacks setTransactions") {
    withSQLConf(
      DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "true",
      DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key -> "false",
      DeltaSQLConf.USE_SNAPSHOT_STATE_FROM_CHECKSUM_ENABLED.key -> "true"
    ) {
      withTempTable(createTable = false) { tableName =>
        spark.range(5).write.format("delta").saveAsTable(tableName)
        DeltaLog.clearCache()
        val log = DeltaLog.forTable(spark, TableIdentifier(tableName))
        val s = log.update()
        assert(s.checksumOpt.isDefined)
        assert(s.checksumOpt.get.setTransactions.isEmpty,
          "test setup should produce a CRC without setTransactions")
        assert(s.computeStateFromChecksum.isEmpty,
          "fast path must fall back when CRC is missing setTransactions")
      }
    }
  }

  for ((label, mutateChecksum, expectedAction) <- Seq(
    ("protocol",
      (cs: VersionChecksum) =>
        cs.copy(protocol =
          cs.protocol.copy(minReaderVersion = cs.protocol.minReaderVersion + 1)),
      "protocol"),
    ("metadata",
      (cs: VersionChecksum) =>
        cs.copy(metadata = cs.metadata.copy(description = "deliberately-corrupted")),
      "metadata"))) {
    test(s"SnapshotState fast path from CRC: throws when CRC $label disagrees with " +
         s"snapshot's resolved $label") {
      // Resolve the snapshot's protocol/metadata via state reconstruction (not CRC) so the
      // validation in computeStateFromChecksum has independent values to compare against.
      withSQLConf(
        DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "true",
        DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key -> "true",
        DeltaSQLConf.USE_PROTOCOL_AND_METADATA_FROM_CHECKSUM_ENABLED.key -> "false",
        DeltaSQLConf.USE_SNAPSHOT_STATE_FROM_CHECKSUM_ENABLED.key -> "true"
      ) {
        withTempTable(createTable = false) { tableName =>
          spark.range(5).write.format("delta").saveAsTable(tableName)
          val log = DeltaLog.forTable(spark, TableIdentifier(tableName))
          val version = log.update().version
          val originalChecksum = log.readChecksum(version).get
          val corruptedChecksum = mutateChecksum(originalChecksum)
          log.store.write(
            FileNames.checksumFile(log.logPath, version),
            Iterator(JsonUtils.toJson(corruptedChecksum)),
            overwrite = true)
          DeltaLog.clearCache()
          val freshLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
          val s = freshLog.update()
          assert(s.checksumOpt.isDefined)
          val ex = intercept[DeltaIllegalStateException] {
            s.computeStateFromChecksum
          }
          assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(expectedAction),
            s"expected exception message to mention '$expectedAction', got: ${ex.getMessage}")
        }
      }
    }
  }

  test("VersionChecksum round-trips lastManifestCommit and omits it when None") {
    val lmc = LastManifestCommit(contentRootVersion = 41, version = 43)
    val base = VersionChecksum(
      txnId = None,
      tableSizeBytes = 100,
      numFiles = 1,
      numDeletedRecordsOpt = None,
      numDeletionVectorsOpt = None,
      numMetadata = 1,
      numProtocol = 1,
      inCommitTimestampOpt = None,
      setTransactions = None,
      domainMetadata = None,
      metadata = Metadata(),
      protocol = Protocol(),
      fileSizeHistogram = None,
      deletedRecordCountsHistogramOpt = None,
      allFiles = None,
      lastManifestCommit = Some(lmc))

    val json = JsonUtils.toJson(base)
    assert(JsonUtils.fromJson[VersionChecksum](json).lastManifestCommit.contains(lmc))

    // None serializes as absent (mapper uses Include.NON_ABSENT), so existing CRCs stay unchanged.
    val jsonWithoutLmc = JsonUtils.toJson(base.copy(lastManifestCommit = None))
    assert(!jsonWithoutLmc.contains("lastManifestCommit"))
    // A CRC written before this field existed deserializes to None.
    assert(JsonUtils.fromJson[VersionChecksum](jsonWithoutLmc).lastManifestCommit.isEmpty)
  }
}

class ChecksumWithCatalogOwnedBatch1Suite extends ChecksumSuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(1)
}

class ChecksumWithCatalogOwnedBatch2Suite extends ChecksumSuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(2)
}

class ChecksumWithCatalogOwnedBatch100Suite extends ChecksumSuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(100)
}
