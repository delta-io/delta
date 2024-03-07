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

import java.nio.charset.StandardCharsets.UTF_8

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.actions.{Action, CommitInfo}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ManualClock

class InCommitTimestampSuite
  extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey, "true")
  }

  private def getInCommitTimestamp(deltaLog: DeltaLog, version: Long): Long = {
    val commitInfo = DeltaHistoryManager.getCommitInfoOpt(
      deltaLog.store, deltaLog.logPath, version, deltaLog.newDeltaHadoopConf())
    commitInfo.get.inCommitTimestamp.get
  }

  def filterUsageRecords(usageRecords: Seq[UsageRecord], opType: String): Seq[UsageRecord] = {
    usageRecords.filter { r =>
      r.tags.get("opType").contains(opType) || r.opType.map(_.typeName).contains(opType)
    }
  }

  test("Enable ICT on commit 0") {
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
      val deltaLog =
        DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      val ver0Snapshot = deltaLog.snapshot
      assert(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(ver0Snapshot.metadata))
      assert(ver0Snapshot.timestamp == getInCommitTimestamp(deltaLog, 0))
    }
  }

  test("Create a non-inCommitTimestamp table and then enable timestamp") {
    withSQLConf(
      DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> false.toString
    ) {
      withTempDir { tempDir =>
        spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
        spark.sql(s"INSERT INTO delta.`$tempDir` VALUES 10")
        val ver1Snapshot = DeltaLog.forTable(spark, tempDir.getAbsolutePath).snapshot
        // File timestamp should be the same as snapshot.getTimestamp when inCommitTimestamp is not
        // enabled
        assert(
          ver1Snapshot.logSegment.lastCommitTimestamp == ver1Snapshot.timestamp)

        spark.sql(s"ALTER TABLE delta.`${tempDir.getAbsolutePath}`" +
          s"SET TBLPROPERTIES ('${DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key}' = 'true')")

        val ver2Snapshot = DeltaLog.forTable(spark, tempDir.getAbsolutePath).snapshot
        // File timestamp should be different from snapshot.getTimestamp when inCommitTimestamp is
        // enabled
        assert(ver2Snapshot.timestamp == getInCommitTimestamp(ver2Snapshot.deltaLog, version = 2))

        assert(ver2Snapshot.timestamp > ver1Snapshot.timestamp)

        spark.sql(s"INSERT INTO delta.`$tempDir` VALUES 11")

        val ver3Snapshot = DeltaLog.forTable(spark, tempDir.getAbsolutePath).snapshot

        assert(ver3Snapshot.timestamp > ver2Snapshot.timestamp)
      }
    }
  }

  test("InCommitTimestamps are monotonic even when the clock is skewed") {
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
      val startTime = System.currentTimeMillis()
      val clock = new ManualClock(startTime)
      // Clear the cache to ensure that a new DeltaLog is created with the new clock.
      DeltaLog.clearCache()
      val deltaLog =
        DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)
      // Move backwards in time.
      deltaLog.startTransaction().commit(Seq(createTestAddFile("1")), ManualUpdate)
      val ver1Timestamp = deltaLog.snapshot.timestamp
      clock.setTime(startTime - 10000)
      deltaLog.startTransaction().commit(Seq(createTestAddFile("2")), ManualUpdate)
      val ver2Timestamp = deltaLog.snapshot.timestamp
      assert(ver2Timestamp > ver1Timestamp)
    }
  }

  test("Conflict resolution of timestamps") {
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
      val startTime = System.currentTimeMillis()
      val clock = new ManualClock(startTime)
      // Clear the cache to ensure that a new DeltaLog is created with the new clock.
      DeltaLog.clearCache()
      val deltaLog =
        DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)
      val txn1 = deltaLog.startTransaction()
      clock.setTime(startTime)
      deltaLog.startTransaction().commit(Seq(createTestAddFile("1")), ManualUpdate)
      // Move time backwards for the conflicting commit.
      clock.setTime(startTime - 10000)
      val usageRecords = Log4jUsageLogger.track {
        txn1.commit(Seq(createTestAddFile("2")), ManualUpdate)
      }
      // Make sure that this transaction resulted in a conflict.
      assert(filterUsageRecords(usageRecords, "delta.commit.retry").length == 1)
      assert(getInCommitTimestamp(deltaLog, 2) > getInCommitTimestamp(deltaLog, 1))
    }
  }

  test("Missing CommitInfo should result in a DELTA_MISSING_COMMIT_INFO exception") {
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
      val deltaLog =
        DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      deltaLog.startTransaction().commit(Seq(createTestAddFile("1")), ManualUpdate)
      // Remove CommitInfo from the commit.
      val actions = deltaLog.store.readAsIterator(
        FileNames.deltaFile(deltaLog.logPath, 1), deltaLog.newDeltaHadoopConf())
      val actionsWithoutCommitInfo = actions.filterNot(Action.fromJson(_).isInstanceOf[CommitInfo])
      deltaLog.store.write(
        FileNames.deltaFile(deltaLog.logPath, 1),
        actionsWithoutCommitInfo,
        overwrite = true,
        deltaLog.newDeltaHadoopConf())

      DeltaLog.clearCache()
      val latestSnapshot = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath)).snapshot
      val e = intercept[DeltaIllegalStateException] {
        latestSnapshot.timestamp
      }
      checkError(
        exception = e,
        errorClass = "DELTA_MISSING_COMMIT_INFO",
        parameters = Map(
          "featureName" -> InCommitTimestampTableFeature.name,
          "version" -> "1"))
    }
  }

  test("Missing CommitInfo.commitTimestamp should result in a " +
    "DELTA_MISSING_COMMIT_TIMESTAMP exception") {
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
      val deltaLog =
        DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      deltaLog.startTransaction().commit(Seq(createTestAddFile("1")), ManualUpdate)
      // Remove CommitInfo.commitTimestamp from the commit.
      val actions = deltaLog.store.readAsIterator(
        FileNames.deltaFile(deltaLog.logPath, 1),
        deltaLog.newDeltaHadoopConf()).toList
      val actionsWithoutCommitInfoCommitTimestamp =
        actions.map(Action.fromJson).map {
          case ci: CommitInfo =>
            ci.copy(inCommitTimestamp = None).json
          case other =>
            other.json
        }.toIterator
      deltaLog.store.write(
        FileNames.deltaFile(deltaLog.logPath, 1),
        actionsWithoutCommitInfoCommitTimestamp,
        overwrite = true,
        deltaLog.newDeltaHadoopConf())

      DeltaLog.clearCache()
      val latestSnapshot = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath)).snapshot
      val e = intercept[DeltaIllegalStateException] {
        latestSnapshot.timestamp
      }
      checkError(
        exception = e,
        errorClass = "DELTA_MISSING_COMMIT_TIMESTAMP",
        parameters = Map("featureName" -> InCommitTimestampTableFeature.name, "version" -> "1"))
    }
  }

  test("InCommitTimestamp is equal to snapshot.timestamp") {
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
      val deltaLog =
        DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      val ver0Snapshot = deltaLog.snapshot

      assert(ver0Snapshot.timestamp == getInCommitTimestamp(deltaLog, 0))
    }
  }

  test("CREATE OR REPLACE should not disable ICT") {
    withSQLConf(
      DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> false.toString
    ) {
      withTempDir { tempDir =>
        spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
        spark.sql(
          s"ALTER TABLE delta.`${tempDir.getAbsolutePath}`" +
            s"SET TBLPROPERTIES ('${DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key}' = 'true')")

        spark.sql(
          s"CREATE OR REPLACE TABLE delta.`${tempDir.getAbsolutePath}` (id long) USING delta")

        val deltaLogAfterCreateOrReplace =
          DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        val snapshot = deltaLogAfterCreateOrReplace.snapshot
        assert(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(snapshot.metadata))
        assert(snapshot.timestamp ==
          getInCommitTimestamp(deltaLogAfterCreateOrReplace, snapshot.version))
      }
    }
  }

  test("Enablement tracking properties should not be added if ICT is enabled on commit 0") {
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
      val deltaLog =
        DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      val ver0Snapshot = deltaLog.snapshot

      val observedEnablementTimestamp =
        DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.fromMetaData(ver0Snapshot.metadata)
      val observedEnablementVersion =
        DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.fromMetaData(ver0Snapshot.metadata)
      assert(observedEnablementTimestamp.isEmpty)
      assert(observedEnablementVersion.isEmpty)
    }
  }

  test("Enablement tracking works when ICT is enabled post commit 0") {
    withSQLConf(
      DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> false.toString
    ) {
      withTempDir { tempDir =>
        spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
        spark.sql(s"INSERT INTO delta.`$tempDir` VALUES 10")

        spark.sql(
          s"ALTER TABLE delta.`${tempDir.getAbsolutePath}`" +
            s"SET TBLPROPERTIES ('${DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key}' = 'true')")

        val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
        val ver2Snapshot = deltaLog.snapshot
        val observedEnablementTimestamp =
          DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.fromMetaData(ver2Snapshot.metadata)
        val observedEnablementVersion =
          DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.fromMetaData(ver2Snapshot.metadata)
        assert(observedEnablementTimestamp.isDefined)
        assert(observedEnablementVersion.isDefined)
        assert(observedEnablementTimestamp.get == getInCommitTimestamp(deltaLog, version = 2))
        assert(observedEnablementVersion.get == 2)
      }
    }
  }

  test("Conflict resolution of enablement version") {
    withSQLConf(
      DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> false.toString
    ) {
      withTempDir { tempDir =>
        spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
        spark.sql(s"INSERT INTO delta.`$tempDir` VALUES 10")
        val startTime = System.currentTimeMillis()
        val clock = new ManualClock(startTime)
        val deltaLog =
          DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath), clock)
        val snapshot = deltaLog.snapshot
        val txn1 = deltaLog.startTransaction()
        clock.setTime(startTime)
        deltaLog.startTransaction().commit(Seq(createTestAddFile("1")), ManualUpdate)
        val ictEnablementMetadataConfig = snapshot.metadata.configuration ++ Map(
          DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key -> "true")
        val ictEnablementMetadata =
          snapshot.metadata.copy(configuration = ictEnablementMetadataConfig)
        val usageRecords = Log4jUsageLogger.track {
          txn1.commit(Seq(ictEnablementMetadata), ManualUpdate)
        }
        val ver3Snapshot = deltaLog.update()
        val observedEnablementTimestamp =
          DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.fromMetaData(ver3Snapshot.metadata)
        val observedEnablementVersion =
          DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.fromMetaData(ver3Snapshot.metadata)
        // Make sure that this transaction resulted in a conflict.
        assert(filterUsageRecords(usageRecords, "delta.commit.retry").length == 1)
        assert(observedEnablementTimestamp.get == getInCommitTimestamp(deltaLog, version = 3))
        assert(observedEnablementVersion.get == 3)
      }
    }
  }

  test("commitLarge should correctly set the enablement tracking properties") {
    withTempDir { tempDir =>
      spark.range(2).write.format("delta").save(tempDir.getAbsolutePath)
      val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
      val ver0Snapshot = deltaLog.snapshot
      assert(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(ver0Snapshot.metadata))
      // Disable ICT in version 1.
      spark.sql(s"ALTER TABLE delta.`${tempDir.getAbsolutePath}`" +
          s"SET TBLPROPERTIES ('${DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key}' = 'false')")
      assert(!DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(deltaLog.update().metadata))

      // Use a restore command to return the table state to version 0.
      // This should internally invoke commitLarge and the enablement tracking properties should be
      // updated correctly.
      val usageRecords = Log4jUsageLogger.track {
        spark.sql(s"RESTORE TABLE delta.`${tempDir.getAbsolutePath}` TO VERSION AS OF 0")
      }
      val ver2Snapshot = deltaLog.update()
      assert(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(ver2Snapshot.metadata))
      val observedEnablementTimestamp =
        DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.fromMetaData(ver2Snapshot.metadata)
      val observedEnablementVersion =
        DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.fromMetaData(ver2Snapshot.metadata)
      assert(filterUsageRecords(usageRecords, "delta.commit.large").length == 1)
      assert(observedEnablementTimestamp.isDefined)
      assert(observedEnablementVersion.isDefined)
      assert(observedEnablementTimestamp.get == getInCommitTimestamp(deltaLog, version = 2))
      assert(observedEnablementVersion.get == 2)
    }
  }

  test("postCommitSnapshot.timestamp should be populated by protocolMetadataAndICTReconstruction " +
     "when the table has no checkpoints") {
    withTempDir { tempDir =>
      var deltaLog: DeltaLog = null
      var timestamp = -1L
      spark.range(1).write.format("delta").save(tempDir.getAbsolutePath)
      DeltaLog.clearCache()
      val usageRecords = Log4jUsageLogger.track {
        deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        timestamp = deltaLog.snapshot.timestamp
      }
      assert(timestamp == getInCommitTimestamp(deltaLog, 0))
      // No explicit read.
      assert(filterUsageRecords(usageRecords, "delta.inCommitTimestamp.read").isEmpty)
    }
  }

  test("snapshot.timestamp should be populated by protocolMetadataAndICTReconstruction " +
     "during cold reads of checkpoints + deltas") {
    withTempDir { tempDir =>
      var deltaLog: DeltaLog = null
      var timestamp = -1L
      spark.range(1).write.format("delta").save(tempDir.getAbsolutePath)
      deltaLog = DeltaLog
        .forTable(spark, new Path(tempDir.getCanonicalPath))
      deltaLog.createCheckpointAtVersion(0)
      deltaLog.startTransaction().commit(Seq(createTestAddFile("c1")), ManualUpdate)

      val usageRecords = Log4jUsageLogger.track {
        DeltaLog.clearCache() // Clear the post-commit snapshot from the cache.
        deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        timestamp = deltaLog.snapshot.timestamp
      }
      assert(deltaLog.snapshot.checkpointProvider.version == 0)
      assert(deltaLog.snapshot.version == 1)
      assert(timestamp == getInCommitTimestamp(deltaLog, 1))
      // No explicit read.
      assert(filterUsageRecords(usageRecords, "delta.inCommitTimestamp.read").isEmpty)
    }
  }

  test("snapshot.timestamp cannot be populated by protocolMetadataAndICTReconstruction " +
     "during cold reads of checkpoints") {
    withTempDir { tempDir =>
      var deltaLog: DeltaLog = null
      var timestamp = -1L
      spark.range(1).write.format("delta").save(tempDir.getAbsolutePath)
      DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath)).createCheckpointAtVersion(0)
      val usageRecords = Log4jUsageLogger.track {
        DeltaLog.clearCache() // Clear the post-commit snapshot from the cache.
        deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        timestamp = deltaLog.snapshot.timestamp
      }
      assert(deltaLog.snapshot.checkpointProvider.version == 0)
      assert(timestamp == getInCommitTimestamp(deltaLog, 0))
      assert(filterUsageRecords(usageRecords, "delta.inCommitTimestamp.read").length == 1)
    }
  }

  test("Exceptions during ICT reads from file should be logged") {
    withTempDir { tempDir =>
      spark.range(10).write.format("delta").save(tempDir.getAbsolutePath)
      val deltaLog =
        DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      deltaLog.startTransaction().commit(Seq(createTestAddFile("1")), ManualUpdate)
      // Remove CommitInfo from the commit.
      val actions = deltaLog.store.readAsIterator(
        FileNames.deltaFile(deltaLog.logPath, 1), deltaLog.newDeltaHadoopConf())
      val actionsWithoutCommitInfo = actions.filterNot(Action.fromJson(_).isInstanceOf[CommitInfo])
      deltaLog.store.write(
        FileNames.deltaFile(deltaLog.logPath, 1),
        actionsWithoutCommitInfo,
        overwrite = true,
        deltaLog.newDeltaHadoopConf())

      DeltaLog.clearCache()
      val latestSnapshot = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath)).snapshot
      val usageRecords = Log4jUsageLogger.track {
        try {
          latestSnapshot.timestamp
        } catch {
          case _ : DeltaIllegalStateException => ()
        }
      }
      val ictReadLog = filterUsageRecords(usageRecords, "delta.inCommitTimestamp.read").head
      val blob = JsonUtils.fromJson[Map[String, String]](ictReadLog.blob)
      assert(blob("version") == "1")
      assert(blob("checkpointVersion") == "-1")
      assert(blob("exceptionMessage").startsWith("[DELTA_MISSING_COMMIT_INFO]"))
      assert(blob("exceptionStackTrace").contains(Snapshot.getClass.getName.stripSuffix("$")))
    }
  }
}

