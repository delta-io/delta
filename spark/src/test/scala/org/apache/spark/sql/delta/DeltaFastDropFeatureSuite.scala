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
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ManualClock

class DeltaFastDropFeatureSuite
  extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with DeletionVectorsTestUtils
    with DeltaRetentionSuiteBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.FAST_DROP_FEATURE_ENABLED.key, true.toString)
    enableDeletionVectors(spark, false, false, false)
  }

  val barrierVersionPropKey = DeltaConfigs.REQUIRE_CHECKPOINT_PROTECTION_BEFORE_VERSION.key

  protected def createTableWithFeature(
      deltaLog: DeltaLog,
      feature: TableFeature,
      featurePropertyEnablement: String): Unit = {
    sql(
      s"""CREATE TABLE delta.`${deltaLog.dataPath}` (id bigint) USING delta
         |TBLPROPERTIES (
         |delta.feature.${feature.name} = 'supported',
         |$featurePropertyEnablement
         |)""".stripMargin)
  }

  protected def dropTableFeature(
      deltaLog: DeltaLog,
      feature: TableFeature,
      truncateHistory: Boolean = false): Unit = {
    val dropFeatureSQL =
      s"""ALTER TABLE delta.`${deltaLog.dataPath}`
         |DROP FEATURE ${feature.name}
         |${if (truncateHistory) "TRUNCATE HISTORY" else ""}""".stripMargin

    sql(dropFeatureSQL)
  }

  protected def getLogFiles(dir: File): Seq[File] = Nil

  protected def getDeltaVersions(dir: Path): Set[Long] = {
    getFileVersions(getDeltaFiles(new File(dir.toUri)))
  }

  protected def getCheckpointVersions(dir: Path): Set[Long] = {
    getFileVersions(getCheckpointFiles(new File(dir.toUri)))
  }

  protected def setModificationTimes(
      log: DeltaLog,
      startVersion: Long,
      endVersion: Long,
      daysToAdd: Int): Unit = {
    val fs = log.logPath.getFileSystem(log.newDeltaHadoopConf())
    for (version <- startVersion to endVersion) {
      setModificationTime(log, System.currentTimeMillis(), version.toInt, daysToAdd, fs)
    }
  }

  test("Dropping reader+writer feature") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)

      createTableWithFeature(deltaLog,
        TestRemovableReaderWriterFeature,
        s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'")

      // Add some data. This is optional to create a more realistic scenario.
      spark.range(0, 20).write.format("delta").mode("append").save(dir.getCanonicalPath)
      spark.range(20, 40).write.format("delta").mode("append").save(dir.getCanonicalPath)

      dropTableFeature(deltaLog, TestRemovableReaderWriterFeature)

      val snapshot = deltaLog.update()
      assert(snapshot.protocol.readerAndWriterFeatures.contains(CheckpointProtectionTableFeature))
      assert(!snapshot.protocol.readerAndWriterFeatures.contains(TestRemovableReaderWriterFeature))
      assert(snapshot.metadata.configuration.contains(barrierVersionPropKey))
      assert(snapshot.metadata.configuration(barrierVersionPropKey).toInt === snapshot.version)
      assert(getCheckpointVersions(deltaLog.logPath).filter(_ <= snapshot.version).size === 4)
    }
  }

  test("Dropping writer feature") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)

      createTableWithFeature(deltaLog,
        TestRemovableWriterFeature,
        s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'")

      // Add some data. This is optional to create a more realistic scenario.
      spark.range(0, 20).write.format("delta").mode("append").save(dir.getCanonicalPath)
      spark.range(20, 40).write.format("delta").mode("append").save(dir.getCanonicalPath)

      dropTableFeature(deltaLog, TestRemovableWriterFeature)

      // Writer features do not require any checkpoint barriers.
      val snapshot = deltaLog.update()
      assert(!snapshot.protocol.readerAndWriterFeatures.contains(CheckpointProtectionTableFeature))
      assert(!snapshot.protocol.readerAndWriterFeatures.contains(TestRemovableWriterFeature))
      assert(!snapshot.metadata.configuration.contains(barrierVersionPropKey))
      assert(getCheckpointVersions(deltaLog.logPath).size === 0)
    }
  }

  test("Dropping a legacy reader+writer feature") {
    withTempDir { dir =>
      withSQLConf(DeltaSQLConf.TABLE_FEATURES_TEST_FEATURES_ENABLED.key -> false.toString) {
        val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)

        sql(
          s"""CREATE TABLE delta.`${deltaLog.dataPath}` (id bigint) USING delta
             |TBLPROPERTIES (
             |delta.minReaderVersion=2,
             |delta.minWriterVersion=5
             |)""".stripMargin)

        assert(deltaLog.update().protocol === Protocol(2, 5))

        // Add some data. This is optional to create a more realistic scenario.
        spark.range(0, 20).write.format("delta").mode("append").save(dir.getCanonicalPath)
        spark.range(20, 40).write.format("delta").mode("append").save(dir.getCanonicalPath)

        dropTableFeature(deltaLog, ColumnMappingTableFeature)

        val snapshot = deltaLog.update()
        assert(deltaLog.update().protocol === Protocol(1, 7).withFeatures(Seq(
          InvariantsTableFeature,
          AppendOnlyTableFeature,
          CheckConstraintsTableFeature,
          ChangeDataFeedTableFeature,
          GeneratedColumnsTableFeature,
          CheckpointProtectionTableFeature)))
        assert(snapshot.metadata.configuration.contains(barrierVersionPropKey))
        assert(snapshot.metadata.configuration(barrierVersionPropKey).toInt === snapshot.version)
        assert(getCheckpointVersions(deltaLog.logPath).filter(_ <= snapshot.version).size === 4)
      }
    }
  }

  test("Dropping multiple features") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)

      createTableWithFeature(deltaLog,
        TestRemovableReaderWriterFeature,
        s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'")

      // Add some data. This is optional to create a more realistic scenario.
      spark.range(0, 20).write.format("delta").mode("append").save(dir.getCanonicalPath)
      spark.range(20, 40).write.format("delta").mode("append").save(dir.getCanonicalPath)

      sql(
        s"""ALTER TABLE delta.`${dir.getCanonicalPath}` SET TBLPROPERTIES (
           |delta.feature.${VacuumProtocolCheckTableFeature.name} = 'supported'
           |)""".stripMargin)

      dropTableFeature(deltaLog, TestRemovableReaderWriterFeature)

      // Add some data. This is optional to create a more realistic scenario.
      spark.range(40, 60).write.format("delta").mode("append").save(dir.getCanonicalPath)

      dropTableFeature(deltaLog, VacuumProtocolCheckTableFeature)

      // When multiple features are dropped, the barrier version must contain the version of the
      // last dropped feature.
      val snapshot = deltaLog.update()
      assert(snapshot.protocol.readerAndWriterFeatures.contains(CheckpointProtectionTableFeature))
      assert(!snapshot.protocol.readerAndWriterFeatures.contains(TestRemovableReaderWriterFeature))
      assert(!snapshot.protocol.readerAndWriterFeatures.contains(VacuumProtocolCheckTableFeature))
      assert(snapshot.metadata.configuration.contains(barrierVersionPropKey))
      assert(snapshot.metadata.configuration(barrierVersionPropKey).toInt === snapshot.version)
      assert(getCheckpointVersions(deltaLog.logPath).filter(_ <= snapshot.version).size === 8)
    }
  }

  test("Drop feature with history truncation option") {
    // When using the TRUNCATE HISTORY option we fallback to the legacy implementation.
    withTempDir { dir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val deltaLog = DeltaLog.forTable(spark, new Path(dir.getAbsolutePath), clock)

      createTableWithFeature(deltaLog,
        TestRemovableReaderWriterFeature,
        s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'")

      // Add some data. This is optional to create a more realistic scenario.
      spark.range(0, 20).write.format("delta").mode("append").save(dir.getCanonicalPath)
      spark.range(20, 40).write.format("delta").mode("append").save(dir.getCanonicalPath)

      val e = intercept[DeltaTableFeatureException] {
        dropTableFeature(deltaLog, TestRemovableReaderWriterFeature, truncateHistory = true)
      }
      checkError(
        e,
        "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD",
        parameters = Map(
          "feature" -> TestRemovableReaderWriterFeature.name,
          "logRetentionPeriodKey" -> "delta.logRetentionDuration",
          "logRetentionPeriod" -> "30 days",
          "truncateHistoryLogRetentionPeriod" -> "24 hours"))

      clock.advance(TimeUnit.HOURS.toMillis(24) + TimeUnit.MINUTES.toMillis(5))

      dropTableFeature(deltaLog, TestRemovableReaderWriterFeature, truncateHistory = true)

      val snapshot = deltaLog.update()
      assert(!snapshot.protocol.readerAndWriterFeatures.contains(CheckpointProtectionTableFeature))
      assert(!snapshot.protocol.readerAndWriterFeatures.contains(TestRemovableReaderWriterFeature))
      assert(!snapshot.metadata.configuration.contains(barrierVersionPropKey))
    }
  }

  test("Mixing drop feature implementations") {
    // When using the TRUNCATE HISTORY option we fallback to the legacy implementation.
    withTempDir { dir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val deltaLog = DeltaLog.forTable(spark, new Path(dir.getAbsolutePath), clock)

      createTableWithFeature(deltaLog,
        TestRemovableReaderWriterFeature,
        s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'")

      // Add some data. This is optional to create a more realistic scenario.
      spark.range(0, 20).write.format("delta").mode("append").save(dir.getCanonicalPath)
      spark.range(20, 40).write.format("delta").mode("append").save(dir.getCanonicalPath)

      val e = intercept[DeltaTableFeatureException] {
        dropTableFeature(deltaLog, TestRemovableReaderWriterFeature, truncateHistory = true)
      }
      checkError(
        e,
        "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD",
        parameters = Map(
          "feature" -> TestRemovableReaderWriterFeature.name,
          "logRetentionPeriodKey" -> "delta.logRetentionDuration",
          "logRetentionPeriod" -> "30 days",
          "truncateHistoryLogRetentionPeriod" -> "24 hours"))

      clock.advance(TimeUnit.HOURS.toMillis(24) + TimeUnit.MINUTES.toMillis(5))

      // Adds the CheckpointProtectionTableFeature.
      dropTableFeature(deltaLog, TestRemovableReaderWriterFeature, truncateHistory = false)

      val snapshot = deltaLog.update()
      assert(snapshot.protocol.readerAndWriterFeatures.contains(CheckpointProtectionTableFeature))
      assert(!snapshot.protocol.readerAndWriterFeatures.contains(TestRemovableReaderWriterFeature))
      assert(snapshot.metadata.configuration.contains(barrierVersionPropKey))
      assert(snapshot.metadata.configuration(barrierVersionPropKey).toInt === snapshot.version)

      // Two checkpoints were created in the first invocation of the legacy implementation. Four
      // more checkpoints were created in the second invocation.
      assert(getCheckpointVersions(deltaLog.logPath).filter(_ <= snapshot.version).size === 6)
    }
  }

  test("Drop CheckpointProtectionTableFeature") {
    withTempDir { dir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val deltaLog = DeltaLog.forTable(spark, new Path(dir.getAbsolutePath), clock)

      createTableWithFeature(deltaLog,
        TestRemovableReaderWriterFeature,
        s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'")

      // Add some data. This is optional to create a more realistic scenario.
      spark.range(0, 20).write.format("delta").mode("append").save(dir.getCanonicalPath)
      spark.range(20, 40).write.format("delta").mode("append").save(dir.getCanonicalPath)

      // Adds the CheckpointProtectionTableFeature.
      dropTableFeature(deltaLog, TestRemovableReaderWriterFeature)

      // More data. This is optional to create a more realistic scenario.
      spark.range(40, 60).write.format("delta").mode("append").save(dir.getCanonicalPath)

      val checkpointProtectionVersion =
        CheckpointProtectionTableFeature.getCheckpointProtectionVersion(deltaLog.update())

      val e = intercept[DeltaTableFeatureException] {
        dropTableFeature(deltaLog, CheckpointProtectionTableFeature, truncateHistory = true)
      }
      checkError(
        e,
        "DELTA_FEATURE_DROP_CHECKPOINT_PROTECTION_WAIT_FOR_RETENTION_PERIOD",
        parameters = Map("truncateHistoryLogRetentionPeriod" -> "24 hours"))

      clock.advance(TimeUnit.HOURS.toMillis(48))

      dropTableFeature(deltaLog, CheckpointProtectionTableFeature, truncateHistory = true)

      val snapshot = deltaLog.update()
      val protocol = snapshot.protocol
      assert(!protocol.readerAndWriterFeatures.contains(CheckpointProtectionTableFeature))
      assert(!protocol.readerAndWriterFeatures.contains(TestRemovableReaderWriterFeature))
      assert(!snapshot.metadata.configuration.contains(barrierVersionPropKey))
      assert(getDeltaVersions(deltaLog.logPath).min >= checkpointProtectionVersion)
    }
  }

  test("Drop CheckpointProtectionTableFeature with fast drop feature") {
    withTempDir { dir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val deltaLog = DeltaLog.forTable(spark, new Path(dir.getAbsolutePath), clock)

      createTableWithFeature(deltaLog,
        TestRemovableReaderWriterFeature,
        s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'")

      // Add some data. This is optional to create a more realistic scenario.
      spark.range(0, 20).write.format("delta").mode("append").save(dir.getCanonicalPath)
      spark.range(20, 40).write.format("delta").mode("append").save(dir.getCanonicalPath)

      // Adds the CheckpointProtectionTableFeature.
      dropTableFeature(deltaLog, TestRemovableReaderWriterFeature)

      // This is optional since we won't be allowed to drop CheckpointProtectionTableFeature anyway.
      // However, we show that in a scenario were the feature would normally dropped, it did not
      // because we used the fast drop feature command.
      clock.advance(TimeUnit.HOURS.toMillis(48))

      val e = intercept[DeltaTableFeatureException] {
        dropTableFeature(deltaLog, CheckpointProtectionTableFeature)
      }
      checkError(
        e,
        "DELTA_FEATURE_CAN_ONLY_DROP_CHECKPOINT_PROTECTION_WITH_HISTORY_TRUNCATION",
        parameters = Map.empty)
    }
  }

  test("Attempt dropping CheckpointProtectionTableFeature within the retention period") {
    withTempDir { dir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val deltaLog = DeltaLog.forTable(spark, new Path(dir.getAbsolutePath), clock)

      createTableWithFeature(deltaLog,
        TestRemovableReaderWriterFeature,
        s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'")

      // Add some data. This is optional to create a more realistic scenario.
      spark.range(0, 20).write.format("delta").mode("append").save(dir.getCanonicalPath)
      spark.range(20, 40).write.format("delta").mode("append").save(dir.getCanonicalPath)

      // Adds the CheckpointProtectionTableFeature.
      dropTableFeature(deltaLog, TestRemovableReaderWriterFeature)

      // More data. This is optional to create a more realistic scenario.
      spark.range(40, 60).write.format("delta").mode("append").save(dir.getCanonicalPath)
      deltaLog.checkpoint(deltaLog.update())

      val e1 = intercept[DeltaTableFeatureException] {
        dropTableFeature(deltaLog, CheckpointProtectionTableFeature, truncateHistory = true)
      }
      checkError(
        e1,
        "DELTA_FEATURE_DROP_CHECKPOINT_PROTECTION_WAIT_FOR_RETENTION_PERIOD",
        parameters = Map("truncateHistoryLogRetentionPeriod" -> "24 hours"))

      // TestRemovableReaderWriterFeature traces still exist in history.
      clock.advance(TimeUnit.HOURS.toMillis(15))

      val e2 = intercept[DeltaTableFeatureException] {
        dropTableFeature(deltaLog, CheckpointProtectionTableFeature, truncateHistory = true)
      }
      checkError(
        e2,
        "DELTA_FEATURE_DROP_CHECKPOINT_PROTECTION_WAIT_FOR_RETENTION_PERIOD",
        parameters = Map("truncateHistoryLogRetentionPeriod" -> "24 hours"))
    }
  }

  test("Drop CheckpointProtectionTableFeature when history is already truncated") {
    withTempDir { dir =>
      val startTS = System.currentTimeMillis()
      val clock = new ManualClock(startTS)
      val deltaLog = DeltaLog.forTable(spark, new Path(dir.getAbsolutePath), clock)

      createTableWithFeature(deltaLog,
        TestRemovableReaderWriterFeature,
        s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'")

      // Add some data. This is optional to create a more realistic scenario.
      spark.range(0, 20).write.format("delta").mode("append").save(dir.getCanonicalPath)
      spark.range(20, 40).write.format("delta").mode("append").save(dir.getCanonicalPath)

      // Adds the CheckpointProtectionTableFeature.
      dropTableFeature(deltaLog, TestRemovableReaderWriterFeature)

      val v1 = deltaLog.update().version

      // Default log retention is 30 days.
      clock.advance(TimeUnit.DAYS.toMillis(32))

      // More data and checkpoints. Data is optional but the checkpoints are used
      // to cleanup the logs below.
      spark.range(40, 60).write.format("delta").mode("append").save(dir.getCanonicalPath)
      deltaLog.checkpoint(deltaLog.update())
      spark.range(60, 80).write.format("delta").mode("append").save(dir.getCanonicalPath)
      deltaLog.checkpoint(deltaLog.update())

      val v2 = deltaLog.update().version
      setModificationTimes(deltaLog, startVersion = v1 + 1, endVersion = v2, daysToAdd = 32)

      deltaLog.cleanUpExpiredLogs(deltaLog.update())

      // Add some data. This is optional to create a more realistic scenario.
      spark.range(80, 100).write.format("delta").mode("append").save(dir.getCanonicalPath)
      spark.range(100, 120).write.format("delta").mode("append").save(dir.getCanonicalPath)

      val v3 = deltaLog.update().version
      setModificationTimes(deltaLog, startVersion = v2 + 1, endVersion = v3, daysToAdd = 32)

      clock.advance(TimeUnit.HOURS.toMillis(48))

      // Add some data. This is optional to create a more realistic scenario.
      spark.range(120, 140).write.format("delta").mode("append").save(dir.getCanonicalPath)
      spark.range(140, 160).write.format("delta").mode("append").save(dir.getCanonicalPath)

      // At this point history before the atomic cleanup version should already be clean.
      val deltaVersionsBeforeDrop = getDeltaVersions(deltaLog.logPath)
      val atomicHistoryCleanupVersion =
        CheckpointProtectionTableFeature.getCheckpointProtectionVersion(deltaLog.update())
      assert(deltaVersionsBeforeDrop.min >= atomicHistoryCleanupVersion)

      val v4 = deltaLog.update().version
      setModificationTimes(deltaLog, startVersion = v3 + 1, endVersion = v4, daysToAdd = 34)
      dropTableFeature(deltaLog, CheckpointProtectionTableFeature, truncateHistory = true)

      val snapshot = deltaLog.update()
      val protocol = snapshot.protocol
      assert(!protocol.readerAndWriterFeatures.contains(CheckpointProtectionTableFeature))
      assert(!protocol.readerAndWriterFeatures.contains(TestRemovableReaderWriterFeature))
      assert(!snapshot.metadata.configuration.contains(barrierVersionPropKey))

      // No other commits should have been truncated.
      assert(getDeltaVersions(deltaLog.logPath).min === deltaVersionsBeforeDrop.min)
    }
  }
}
