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
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.actions.{Action, AddFile, DeletionVectorDescriptor, Protocol, RemoveFile}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.AlterTableUnsetPropertiesDeltaCommand
import org.apache.spark.sql.delta.commands.DeltaReorgTableCommand
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.functions.{col, not}
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
    spark.conf.set(DeltaSQLConf.FAST_DROP_FEATURE_GENERATE_DV_TOMBSTONES.key, true.toString)
    enableDeletionVectors(spark, false, false, false)
  }

  val barrierVersionPropKey = DeltaConfigs.REQUIRE_CHECKPOINT_PROTECTION_BEFORE_VERSION.key

  protected def createTableWithFeature(
      deltaLog: DeltaLog,
      feature: TableFeature,
      featurePropertyEnablement: Option[String] = None): Unit = {
    val props = Seq(s"delta.feature.${feature.name} = 'supported'") ++ featurePropertyEnablement

    sql(
      s"""CREATE TABLE delta.`${deltaLog.dataPath}` (id bigint) USING delta
         |TBLPROPERTIES (
         |${props.mkString(",")}
         |)""".stripMargin)

    assert(deltaLog.update().protocol.readerAndWriterFeatures.contains(feature))
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

    val snapshot = deltaLog.update()
    assert(!snapshot.protocol.readerAndWriterFeatures.contains(feature))
    assert(truncateHistory ||
      !feature.asInstanceOf[RemovableFeature].requiresHistoryProtection ||
      snapshot.protocol.readerAndWriterFeatures.contains(CheckpointProtectionTableFeature))
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
      startTime: Long = System.currentTimeMillis(),
      startVersion: Long,
      endVersion: Long,
      daysToAdd: Int): Unit = {
    val fs = log.logPath.getFileSystem(log.newDeltaHadoopConf())
    for (version <- startVersion to endVersion) {
      setModificationTime(log, startTime, version.toInt, daysToAdd, fs)
    }
  }

  protected def addData(dir: File, start: Int, end: Int): Unit =
    spark.range(start, end).write.format("delta").mode("append").save(dir.getCanonicalPath)

  test("Dropping reader+writer feature") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)

      createTableWithFeature(deltaLog,
        TestRemovableReaderWriterFeature,
        Some(s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'"))

      // Add some data. This is optional to create a more realistic scenario.
      addData(dir, start = 0, end = 20)
      addData(dir, start = 20, end = 40)

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
        Some(s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'"))

      // Add some data. This is optional to create a more realistic scenario.
      addData(dir, start = 0, end = 20)
      addData(dir, start = 20, end = 40)

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
        addData(dir, start = 0, end = 20)
        addData(dir, start = 20, end = 40)

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
        Some(s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'"))

      // Add some data. This is optional to create a more realistic scenario.
      addData(dir, start = 0, end = 20)
      addData(dir, start = 20, end = 40)

      sql(
        s"""ALTER TABLE delta.`${dir.getCanonicalPath}` SET TBLPROPERTIES (
           |delta.feature.${VacuumProtocolCheckTableFeature.name} = 'supported'
           |)""".stripMargin)

      dropTableFeature(deltaLog, TestRemovableReaderWriterFeature)

      // Add some data. This is optional to create a more realistic scenario.
      addData(dir, start = 40, end = 60)

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
        Some( s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'"))

      // Add some data. This is optional to create a more realistic scenario.
      addData(dir, start = 0, end = 20)
      addData(dir, start = 20, end = 40)

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
        Some(s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'"))

      // Add some data. This is optional to create a more realistic scenario.
      addData(dir, start = 0, end = 20)
      addData(dir, start = 20, end = 40)

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
      val expectedResult = 5
      assert(getCheckpointVersions(
        deltaLog.logPath).filter(_ <= snapshot.version).size === expectedResult)
    }
  }

  test("Drop CheckpointProtectionTableFeature") {
    withTempDir { dir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val deltaLog = DeltaLog.forTable(spark, new Path(dir.getAbsolutePath), clock)

      createTableWithFeature(deltaLog,
        TestRemovableReaderWriterFeature,
        Some(s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'"))

      // Add some data. This is optional to create a more realistic scenario.
      addData(dir, start = 0, end = 20)
      addData(dir, start = 20, end = 40)

      // Adds the CheckpointProtectionTableFeature.
      dropTableFeature(deltaLog, TestRemovableReaderWriterFeature)

      // More data. This is optional to create a more realistic scenario.
      addData(dir, start = 40, end = 60)

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
        Some(s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'"))

      // Add some data. This is optional to create a more realistic scenario.
      addData(dir, start = 0, end = 20)
      addData(dir, start = 20, end = 40)

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
        Some(s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'"))

      // Add some data. This is optional to create a more realistic scenario.
      addData(dir, start = 0, end = 20)
      addData(dir, start = 20, end = 40)

      // Adds the CheckpointProtectionTableFeature.
      dropTableFeature(deltaLog, TestRemovableReaderWriterFeature)

      // More data. This is optional to create a more realistic scenario.
      addData(dir, start = 40, end = 60)
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
        Some(s"${TestRemovableReaderWriterFeature.TABLE_PROP_KEY} = 'true'"))

      // Add some data. This is optional to create a more realistic scenario.
      addData(dir, start = 0, end = 20)
      addData(dir, start = 20, end = 40)

      // Adds the CheckpointProtectionTableFeature.
      dropTableFeature(deltaLog, TestRemovableReaderWriterFeature)

      val v1 = deltaLog.update().version

      // Default log retention is 30 days.
      clock.advance(TimeUnit.DAYS.toMillis(32))

      // More data and checkpoints. Data is optional but the checkpoints are used
      // to cleanup the logs below.
      addData(dir, start = 40, end = 60)
      deltaLog.checkpoint(deltaLog.update())
      addData(dir, start = 60, end = 80)
      deltaLog.checkpoint(deltaLog.update())

      val v2 = deltaLog.update().version
      setModificationTimes(deltaLog, startVersion = v1 + 1, endVersion = v2, daysToAdd = 32)

      deltaLog.cleanUpExpiredLogs(deltaLog.update())

      // Add some data. This is optional to create a more realistic scenario.
      addData(dir, start = 80, end = 100)
      addData(dir, start = 100, end = 120)

      val v3 = deltaLog.update().version
      setModificationTimes(deltaLog, startVersion = v2 + 1, endVersion = v3, daysToAdd = 32)

      clock.advance(TimeUnit.HOURS.toMillis(48))

      // Add some data. This is optional to create a more realistic scenario.
      addData(dir, start = 120, end = 140)
      addData(dir, start = 140, end = 160)

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

  for (timeTravelMethod <- Seq("restoreSQL", "restoreSQLTS", "selectSQL", "selectSQLTS",
                               "getSnapshotAt", "restoreToVersion", "restoreToTS",
                               "sparkVersion", "sparkTS"))
  test(s"Protocol is validated when time traveling - time-travel method: $timeTravelMethod") {
    withTempDir { dir =>
      def getTimestampForVersion(version: Long): String = {
        val logPath = new Path(dir.getCanonicalPath, "_delta_log")
        val file = new File(new Path(logPath, f"$version%020d.json").toString)
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        sdf.format(file.lastModified())
      }

      val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
      createTableWithFeature(deltaLog, TestUnsupportedReaderWriterFeature)

      // Add some data. This is optional to create a more realistic scenario.
      addData(dir, start = 0, end = 20)
      addData(dir, start = 20, end = 40)

      val versionBeforeRemoval = deltaLog.update().version
      val tsBeforeRemoval = getTimestampForVersion(versionBeforeRemoval)

      // Adds the CheckpointProtectionTableFeature.
      dropTableFeature(deltaLog, TestUnsupportedReaderWriterFeature)

      // More data. This is optional to create a more realistic scenario.
      addData(dir, start = 40, end = 60)
      addData(dir, start = 60, end = 80)

      withSQLConf(DeltaSQLConf.UNSUPPORTED_TESTING_FEATURES_ENABLED.key -> true.toString) {
        val e = intercept[DeltaUnsupportedTableFeatureException] {
          val table = io.delta.tables.DeltaTable.forPath(dir.toString)
          DeltaLog.clearCache()
          val tablePath = s"delta.`${dir.getCanonicalPath}`"
          timeTravelMethod match {
            case "restoreSQL" => sql(s"RESTORE $tablePath TO VERSION AS OF $versionBeforeRemoval")
            case "restoreSQLTS" => sql(s"RESTORE $tablePath TO TIMESTAMP AS OF '$tsBeforeRemoval'")
            case "selectSQL" => sql(s"SELECT * FROM $tablePath VERSION AS OF $versionBeforeRemoval")
            case "selectSQLTS" =>
              sql(s"SELECT * FROM $tablePath TIMESTAMP AS OF '$tsBeforeRemoval'")
            case "getSnapshotAt" => deltaLog.getSnapshotAt(versionBeforeRemoval)
            case "restoreToVersion" => table.restoreToVersion(versionBeforeRemoval)
            case "restoreToTS" => table.restoreToTimestamp(tsBeforeRemoval)
            case "sparkVersion" => spark.read.format("delta")
              .option("versionAsOf", versionBeforeRemoval).load(dir.getCanonicalPath)
            case "sparkTS" => spark.read.format("delta")
              .option("timestampAsOf", tsBeforeRemoval).load(dir.getCanonicalPath)
            case _ => assert(false, "non existent time travel method.")
          }
        }
        assert(e.getErrorClass === "DELTA_UNSUPPORTED_FEATURES_FOR_READ")
      }
    }
  }

  for (downgradeAllowed <- BOOLEAN_DOMAIN)
  test(s"Restore table works with fast drop feature - downgradeAllowed: $downgradeAllowed") {
    withSQLConf(
        DeltaSQLConf.RESTORE_TABLE_PROTOCOL_DOWNGRADE_ALLOWED.key -> downgradeAllowed.toString) {
      withTempDir { dir =>
        import org.apache.spark.sql.delta.implicits._

        val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
        createTableWithFeature(deltaLog, TestRemovableReaderWriterFeature)

        // Add some data.
        addData(dir, start = 0, end = 20)
        addData(dir, start = 20, end = 40)

        val versionBeforeRemoval = deltaLog.update().version

        // Adds the CheckpointProtectionTableFeature.
        dropTableFeature(deltaLog, TestRemovableReaderWriterFeature)

        // More data. This is optional to create a more realistic scenario.
        addData(dir, start = 40, end = 60)
        addData(dir, start = 60, end = 80)

        sql(s"RESTORE delta.`${dir.getCanonicalPath}` TO VERSION AS OF $versionBeforeRemoval")

        val protocol = deltaLog.update().protocol
        assert(protocol.readerAndWriterFeatures.contains(TestRemovableReaderWriterFeature))
        assert(protocol.readerAndWriterFeatures
          .contains(CheckpointProtectionTableFeature) === !downgradeAllowed)

        val targetTable = io.delta.tables.DeltaTable.forPath(dir.getCanonicalPath)
        assert(targetTable.toDF.as[Long].collect().sorted === Seq.range(0, 40))
      }
    }
  }

  private def createTableWithDeletionVectors(deltaLog: DeltaLog): Unit = {
    withSQLConf(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> true.toString,
        DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> true.toString) {
      val dir = deltaLog.dataPath
      val targetDF = spark.range(start = 0, end = 100, step = 1, numPartitions = 4)
      targetDF.write.format("delta").save(dir.toString)

      val targetTable = io.delta.tables.DeltaTable.forPath(dir.toString)

      // Add some DVs.
      targetTable.delete("id < 5 or id >= 95")

      // Add some more DVs for the same set of files.
      targetTable.delete("id < 10 or id >= 90")

      // Assert that DVs exist.
      assert(deltaLog.update().numDeletionVectorsOpt === Some(2L))
    }
  }

  private def dropDeletionVectors(deltaLog: DeltaLog, truncateHistory: Boolean = false): Unit = {
    sql(s"""ALTER TABLE delta.`${deltaLog.dataPath}`
         |DROP FEATURE deletionVectors
         |${if (truncateHistory) "TRUNCATE HISTORY" else ""}
         |""".stripMargin)

    val snapshot = deltaLog.update()
    val protocol = snapshot.protocol
    assert(snapshot.numDeletionVectorsOpt.getOrElse(0L) === 0)
    assert(snapshot.numDeletedRecordsOpt.getOrElse(0L) === 0)
    assert(truncateHistory ||
      !protocol.readerFeatureNames.contains(DeletionVectorsTableFeature.name))
  }

  private def validateTombstones(log: DeltaLog): Unit = {
    import org.apache.spark.sql.delta.implicits._

    val snapshot = log.update()
    val dvPath = DeletionVectorDescriptor.urlEncodedPath(col("deletionVector"), log.dataPath)
    val isDVTombstone = DeletionVectorDescriptor.isDeletionVectorPath(col("path"))
    val isInlineDeletionVector = DeletionVectorDescriptor.isInline(col("deletionVector"))

    val uniqueDvsFromParquetRemoveFiles = snapshot
      .tombstones
      .filter("deletionVector IS NOT NULL")
      .filter(not(isInlineDeletionVector))
      .filter(not(isDVTombstone))
      .select(dvPath)
      .distinct()
      .as[String]

    val dvTombstones = snapshot
      .tombstones
      .filter(isDVTombstone)
      .select("path")
      .as[String]

    val dvTombstonesSet = dvTombstones.collect().toSet

    assert(dvTombstonesSet.nonEmpty)
    assert(uniqueDvsFromParquetRemoveFiles.collect().toSet === dvTombstonesSet)
  }

  for (withCommitLarge <- BOOLEAN_DOMAIN)
  test("DV tombstones are created when dropping DVs" +
      s"withCommitLarge: $withCommitLarge") {
    val threshold = if (withCommitLarge) 0 else 10000
    withSQLConf(
        DeltaSQLConf.FAST_DROP_FEATURE_DV_TOMBSTONE_COUNT_THRESHOLD.key -> threshold.toString) {
      withTempPath { dir =>
        val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)
        createTableWithDeletionVectors(deltaLog)
        dropDeletionVectors(deltaLog)
        validateTombstones(deltaLog)

        val targetTable = io.delta.tables.DeltaTable.forPath(dir.toString)
        assert(targetTable.toDF.collect().length === 80)
        // DV Tombstones are recorded in the snapshot state.
        assert(deltaLog.update().numOfRemoves === 8)
      }
    }
  }

  test("DV tombstones are generated when no action is taken in pre-downgrade") {
    withTempPath { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)
      createTableWithDeletionVectors(deltaLog)

      // Remove all DV traces in advance. Table will look clean in DROP FEATURE.
      val table = DeltaTableV2(spark, deltaLog.dataPath)
      val properties = Seq(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key)
      AlterTableUnsetPropertiesDeltaCommand(table, properties, ifExists = true).run(spark)

      import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
      val catalog = spark.sessionState.catalogManager.currentCatalog.asTableCatalog
      val tableId = Seq(table.name()).asIdentifier
      DeltaReorgTableCommand(ResolvedTable.create(catalog, tableId, table))(Nil).run(spark)
      assert(deltaLog.update().numDeletedRecordsOpt.forall(_ == 0))

      dropDeletionVectors(deltaLog)
      validateTombstones(deltaLog)
    }
  }

  test("We only create missing DV tombstones when dropping DVs") {
    withTempPath { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)
      createTableWithDeletionVectors(deltaLog)
      dropDeletionVectors(deltaLog)
      validateTombstones(deltaLog)

      // Re enable the feature and add more DVs. The delete touches a new file as well as a file
      // that already contains a DV within the retention period.
      sql(
        s"""ALTER TABLE delta.`${dir.getAbsolutePath}`
           |SET TBLPROPERTIES (
           |delta.feature.${DeletionVectorsTableFeature.name} = 'enabled',
           |${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key} = 'true'
           |)""".stripMargin)

      withSQLConf(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> true.toString) {
        val targetTable = io.delta.tables.DeltaTable.forPath(dir.toString)
        targetTable.delete("id > 20 and id <= 30")
        assert(deltaLog.update().numDeletionVectorsOpt === Some(2L))
      }

      sql(s"ALTER TABLE delta.`${dir.getAbsolutePath}` DROP FEATURE deletionVectors")

      validateTombstones(deltaLog)
    }
  }

  test("We do not create tombstones when there are no RemoveFiles within the retention period") {
    withTempPath { dir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val deltaLog = DeltaLog.forTable(spark, new Path(dir.getAbsolutePath), clock)
      createTableWithDeletionVectors(deltaLog)

      // Pretend tombstone retention period has passed (default 1 week).
      val clockAdvanceMillis = DeltaLog.tombstoneRetentionMillis(deltaLog.update().metadata)
      clock.advance(clockAdvanceMillis + TimeUnit.DAYS.toMillis(3))

      dropDeletionVectors(deltaLog)

      assert(deltaLog.update().tombstones.collect().forall(_.isDVTombstone == false))
    }
  }

  test("We create DV tombstones when mixing drop feature implementations") {
    // When using the TRUNCATE HISTORY option we fallback to the legacy implementation.
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)
      createTableWithDeletionVectors(deltaLog)

      val e = intercept[DeltaTableFeatureException] {
        dropDeletionVectors(deltaLog, truncateHistory = true)
      }
      checkError(
        e,
        "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD",
        parameters = Map(
          "feature" -> "deletionVectors",
          "logRetentionPeriodKey" -> "delta.logRetentionDuration",
          "logRetentionPeriod" -> "30 days",
          "truncateHistoryLogRetentionPeriod" -> "24 hours"))

      validateTombstones(deltaLog)
      dropDeletionVectors(deltaLog, truncateHistory = false)
      validateTombstones(deltaLog)
    }
  }

  test("DV tombstones are not created for inline DVs") {
    withSQLConf(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> true.toString) {
      withTempPath { dir =>
        val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)
        val targetTable = () => io.delta.tables.DeltaTable.forPath(dir.toString)

        spark.range(start = 0, end = 100, step = 1, numPartitions = 1)
          .write.format("delta").save(dir.toString)

        def removeRowsWithInlineDV(add: AddFile, markedRows: Long*): (AddFile, RemoveFile) = {
          val bitmap = RoaringBitmapArray(markedRows: _*)
          val serializedBitmap = bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
          val cardinality = markedRows.size
          val dv = DeletionVectorDescriptor.inlineInLog(serializedBitmap, cardinality)

          add.removeRows(
            deletionVector = dv,
            updateStats = true)
        }

        // There should be a single AddFile.
        val snapshot = deltaLog.update()
        val addFile = snapshot.allFiles.first()
        val (newAddFile, newRemoveFile) = removeRowsWithInlineDV(addFile, 3, 34, 67)
        val actionsToCommit: Seq[Action] = Seq(newAddFile, newRemoveFile)

        deltaLog.startTransaction(catalogTableOpt = None, snapshotOpt = Some(snapshot))
          .commit(actionsToCommit, new DeltaOperations.TestOperation)

        // Verify the table is
        assert(deltaLog.update().numDeletedRecordsOpt.exists(_ === 3))
        assert(deltaLog.update().numDeletionVectorsOpt.exists(_ === 1))

        assert(targetTable().toDF.collect().length === 97)

        dropDeletionVectors(deltaLog)
        // No DV tombstones should be have been created.
        assert(deltaLog.update().tombstones.collect().forall(_.isDVTombstone == false))

        assert(targetTable().toDF.collect().length === 97)
        assert(deltaLog.update().numDeletedRecordsOpt.forall(_ === 0))
        assert(deltaLog.update().numDeletionVectorsOpt.forall(_ === 0))
      }
    }
  }

  for (generateDVTombstones <- BOOLEAN_DOMAIN)
  test(s"Vacuum does not delete deletion vector files." +
      s"generateDVTombstones: $generateDVTombstones") {
    val targetDF = spark.range(start = 0, end = 100, step = 1, numPartitions = 4)
    withSQLConf(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> true.toString,
        DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> true.toString,
        DeltaSQLConf.FAST_DROP_FEATURE_GENERATE_DV_TOMBSTONES.key -> generateDVTombstones.toString,
        // With this config we pretend the client does not support DVs. Therefore, it will not
        // discover DVs from the RemoveFile actions.
        DeltaSQLConf.FAST_DROP_FEATURE_DV_DISCOVERY_IN_VACUUM_DISABLED.key -> true.toString) {
      withTempPath { dir =>
        val targetLog = DeltaLog.forTable(spark, dir.getAbsolutePath)
        targetDF.write.format("delta").save(dir.toString)
        val targetTable = io.delta.tables.DeltaTable.forPath(dir.toString)

        // Add some DVs.
        targetTable.delete("id < 5 or id >= 95")
        val versionWithDVs = targetLog.update().version

        // Unfortunately, there is no point in advancing the clock because the deletion timestamps
        // in the RemoveFiles do not use the clock. Instead, we set the creation time back 10 days
        // to all files created so far. These will be eligible for vacuum.
        val fs = targetLog.logPath.getFileSystem(targetLog.newDeltaHadoopConf())
        val allFiles = DeltaFileOperations.localListDirs(
          hadoopConf = targetLog.newDeltaHadoopConf(),
          dirs = Seq(dir.getCanonicalPath),
          recursive = false)
        allFiles.foreach { p =>
          fs.setTimes(p.getHadoopPath, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(10), 0)
        }

        // Add new DVs for the same set of files.
        targetTable.delete("id < 10 or id >= 90")

        // Assert that DVs exist.
        assert(targetLog.update().numDeletionVectorsOpt === Some(2L))

        sql(s"ALTER TABLE delta.`${dir.getAbsolutePath}` DROP FEATURE deletionVectors")

        val snapshot = targetLog.update()
        val protocol = snapshot.protocol
        assert(snapshot.numDeletionVectorsOpt.getOrElse(0L) === 0)
        assert(snapshot.numDeletedRecordsOpt.getOrElse(0L) === 0)
        assert(!protocol.readerFeatureNames.contains(DeletionVectorsTableFeature.name))

        targetTable.delete("id < 15 or id >= 85")

        // The DV files are outside the retention period. However, the DVs are still referenced in
        // the history. Normally we should not delete any DVs.
        sql(s"VACUUM '${dir.getAbsolutePath}'")

        val query =
          sql(s"SELECT * FROM delta.`${dir.getAbsolutePath}` VERSION AS OF $versionWithDVs")

        if (generateDVTombstones) {
          // At version 1 we only deleted 10 rows.
          assert(query.collect().length === 90)
        } else {
          val e = intercept[SparkException] {
            query.collect()
          }
          val msg = e.getCause.getMessage
          assert(msg.contains("RowIndexFilterFileNotFoundException") ||
            msg.contains(".bin does not exist"))
        }
      }
    }
  }

  test("DV tombstones do not generate CDC") {
    import org.apache.spark.sql.delta.commands.cdc.CDCReader
    withTempPath { dir =>
      withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> true.toString) {
        val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)
        createTableWithDeletionVectors(deltaLog)
        val versionBeforeDrop = deltaLog.update().version
        dropDeletionVectors(deltaLog)

        val deleteCountInDropFeature = CDCReader
          .changesToBatchDF(deltaLog, versionBeforeDrop + 1, deltaLog.update().version, spark)
          .filter(s"${CDCReader.CDC_TYPE_COLUMN_NAME} = '${CDCReader.CDC_TYPE_DELETE_STRING}'")
          .count()
        assert(deleteCountInDropFeature === 0)
      }
    }
  }

  for (incrementalCommitEnabled <- BOOLEAN_DOMAIN)
  test("Checksum computation does not take into account DV tombstones" +
      s"incrementalCommitEnabled: $incrementalCommitEnabled") {
    withTempPaths(2) { dirs =>
      var checksumWithDVTombstones: VersionChecksum = null
      withSQLConf(
          DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> incrementalCommitEnabled.toString,
          DeltaSQLConf.FAST_DROP_FEATURE_GENERATE_DV_TOMBSTONES.key -> true.toString) {
        val deltaLog = DeltaLog.forTable(spark, dirs.head.getAbsolutePath)
        createTableWithDeletionVectors(deltaLog)
        dropDeletionVectors(deltaLog)
        val snapshot = deltaLog.update()
        checksumWithDVTombstones = snapshot.checksumOpt.getOrElse(snapshot.computeChecksum)
      }

      var checksumWithoutDVTombstones: VersionChecksum = null
      withSQLConf(
          DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> incrementalCommitEnabled.toString,
          DeltaSQLConf.FAST_DROP_FEATURE_GENERATE_DV_TOMBSTONES.key -> false.toString) {
        val deltaLog = DeltaLog.forTable(spark, dirs.last.getAbsolutePath)
        createTableWithDeletionVectors(deltaLog)
        dropDeletionVectors(deltaLog)
        val snapshot = deltaLog.update()
        checksumWithoutDVTombstones = snapshot.checksumOpt.getOrElse(snapshot.computeChecksum)
      }

      // DV tombstones do not affect the number of files.
      assert(checksumWithoutDVTombstones.numFiles === checksumWithDVTombstones.numFiles)
    }
  }
}
