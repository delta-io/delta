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
import java.util.Locale
import java.util.concurrent.TimeUnit

import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions}
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils._
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.{AlterTableDropFeatureDeltaCommand, AlterTableSetPropertiesDeltaCommand, AlterTableUnsetPropertiesDeltaCommand}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames.{deltaFile, DeltaFile}
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.{SparkConf, SparkThrowable}
import org.apache.spark.sql.{AnalysisException, QueryTest, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.DateTimeConstants
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.ManualClock

trait DeltaProtocolVersionSuiteBase extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  // `.schema` generates NOT NULL columns which requires writer protocol 2. We convert all to
  // NULLable to avoid silent writer protocol version bump.
  private lazy val testTableSchema = spark.range(1).schema.asNullable

  // This is solely a test hook. Users cannot create new Delta tables with protocol lower than
  // that of their current version.
  protected def createTableWithProtocol(
      protocol: Protocol,
      path: File,
      schema: StructType = testTableSchema): DeltaLog = {
    val log = DeltaLog.forTable(spark, path)
    log.ensureLogDirectoryExist()
    log.store.write(
      deltaFile(log.logPath, 0),
      Iterator(Metadata(schemaString = schema.json).json, protocol.json),
      overwrite = false,
      log.newDeltaHadoopConf())
    log.update()
    log
  }

  test("protocol for empty folder") {
    def testEmptyFolder(
        readerVersion: Int,
        writerVersion: Int,
        features: Iterable[TableFeature] = Seq.empty,
        sqlConfs: Iterable[(String, String)] = Seq.empty,
        expectedProtocol: Protocol): Unit = {
      withTempDir { path =>
        val configs = Seq(
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> readerVersion.toString,
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> writerVersion.toString) ++
          features.map(defaultPropertyKey(_) -> FEATURE_PROP_ENABLED) ++
          sqlConfs
        withSQLConf(configs: _*) {
          val log = DeltaLog.forTable(spark, path)
          assert(log.update().protocol === expectedProtocol)
        }
      }
    }

    testEmptyFolder(1, 1, expectedProtocol = Protocol(1, 1))
    testEmptyFolder(1, 2, expectedProtocol = Protocol(1, 2))
    testEmptyFolder(
      readerVersion = 1,
      writerVersion = 1,
      sqlConfs = Seq((DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")),
      expectedProtocol = Protocol(1, 1).merge(ChangeDataFeedTableFeature.minProtocolVersion))
    testEmptyFolder(
      readerVersion = 1,
      writerVersion = 1,
      features = Seq(TestLegacyReaderWriterFeature),
      expectedProtocol =
        Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
          .withFeature(TestLegacyReaderWriterFeature))
    testEmptyFolder(
      readerVersion = 1,
      writerVersion = 1,
      features = Seq(TestWriterFeature),
      expectedProtocol = Protocol(1, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestWriterFeature))
    testEmptyFolder(
      readerVersion = TABLE_FEATURES_MIN_READER_VERSION,
      writerVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
      features = Seq(TestLegacyReaderWriterFeature),
      expectedProtocol =
        Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
          .withFeature(TestLegacyReaderWriterFeature))
    testEmptyFolder(
      readerVersion = 1,
      writerVersion = 1,
      features = Seq(TestWriterFeature),
      sqlConfs = Seq((DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")),
      expectedProtocol = Protocol(
        1,
        TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeatures(Seq(TestWriterFeature, ChangeDataFeedTableFeature)))
    testEmptyFolder(
      readerVersion = 1,
      writerVersion = 1,
      features = Seq(TestLegacyReaderWriterFeature),
      sqlConfs = Seq((DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")),
      expectedProtocol = Protocol(
        TABLE_FEATURES_MIN_READER_VERSION,
        TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeatures(Seq(TestLegacyReaderWriterFeature, ChangeDataFeedTableFeature)))
    testEmptyFolder(
      readerVersion = 1,
      writerVersion = 1,
      features = Seq(TestWriterFeature, TestLegacyReaderWriterFeature),
      expectedProtocol = Protocol(
        TABLE_FEATURES_MIN_READER_VERSION,
        TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeatures(Seq(TestWriterFeature, TestLegacyReaderWriterFeature)))
  }

  test("upgrade to current version") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(1, 1), path)
      assert(log.snapshot.protocol === Protocol(1, 1))
      log.upgradeProtocol(Action.supportedProtocolVersion())
      assert(log.snapshot.protocol === Action.supportedProtocolVersion())
    }
  }

  test("upgrade to a version with DeltaTable API") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(0, 0), path)
      assert(log.snapshot.protocol === Protocol(0, 0))
      val table = io.delta.tables.DeltaTable.forPath(spark, path.getCanonicalPath)
      table.upgradeTableProtocol(1, 1)
      assert(log.snapshot.protocol === Protocol(1, 1))
      table.upgradeTableProtocol(1, 2)
      assert(log.snapshot.protocol === Protocol(1, 2))
      table.upgradeTableProtocol(1, 3)
      assert(log.snapshot.protocol === Protocol(1, 3))
      intercept[DeltaTableFeatureException] {
        table.upgradeTableProtocol(
          TABLE_FEATURES_MIN_READER_VERSION,
          writerVersion = 1)
      }
      intercept[IllegalArgumentException] {
        table.upgradeTableProtocol(
          TABLE_FEATURES_MIN_READER_VERSION + 1,
          TABLE_FEATURES_MIN_WRITER_VERSION)
      }
      intercept[IllegalArgumentException] {
        table.upgradeTableProtocol(
          TABLE_FEATURES_MIN_READER_VERSION,
          TABLE_FEATURES_MIN_WRITER_VERSION + 1)
      }
    }
  }

  test("upgrade to support table features - no feature") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(1, 1), path)
      assert(log.snapshot.protocol === Protocol(1, 1))
      val table = io.delta.tables.DeltaTable.forPath(spark, path.getCanonicalPath)
      table.upgradeTableProtocol(1, TABLE_FEATURES_MIN_WRITER_VERSION)
      assert(
        log.snapshot.protocol === Protocol(
          minReaderVersion = 1,
          minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures = None,
          writerFeatures = Some(Set())))
      table.upgradeTableProtocol(
        TABLE_FEATURES_MIN_READER_VERSION,
        TABLE_FEATURES_MIN_WRITER_VERSION)
      assert(
        log.snapshot.protocol === Protocol(
          minReaderVersion = TABLE_FEATURES_MIN_READER_VERSION,
          minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures = Some(Set()),
          writerFeatures = Some(Set())))
    }
  }

  test("upgrade to support table features - writer-only feature") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(1, 2), path)
      assert(log.snapshot.protocol === Protocol(1, 2))
      val table = io.delta.tables.DeltaTable.forPath(spark, path.getCanonicalPath)
      table.upgradeTableProtocol(1, TABLE_FEATURES_MIN_WRITER_VERSION)
      assert(
        log.snapshot.protocol === Protocol(
          minReaderVersion = 1,
          minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures = None,
          writerFeatures =
            Some(Set(AppendOnlyTableFeature, InvariantsTableFeature).map(_.name))))
      table.upgradeTableProtocol(
        TABLE_FEATURES_MIN_READER_VERSION,
        TABLE_FEATURES_MIN_WRITER_VERSION)
      assert(
        log.snapshot.protocol === Protocol(
          minReaderVersion = TABLE_FEATURES_MIN_READER_VERSION,
          minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures = Some(Set()),
          writerFeatures =
            Some(Set(AppendOnlyTableFeature, InvariantsTableFeature).map(_.name))))
    }
  }

  test("upgrade to support table features - many features") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(2, 5), path)
      assert(log.snapshot.protocol === Protocol(2, 5))
      val table = io.delta.tables.DeltaTable.forPath(spark, path.getCanonicalPath)
      table.upgradeTableProtocol(2, TABLE_FEATURES_MIN_WRITER_VERSION)
      assert(
        log.snapshot.protocol === Protocol(
          minReaderVersion = 2,
          minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures = None,
          writerFeatures = Some(Set(
            AppendOnlyTableFeature,
            ChangeDataFeedTableFeature,
            CheckConstraintsTableFeature,
            ColumnMappingTableFeature,
            GeneratedColumnsTableFeature,
            InvariantsTableFeature,
            TestLegacyWriterFeature,
            TestRemovableLegacyWriterFeature,
            TestLegacyReaderWriterFeature,
            TestRemovableLegacyReaderWriterFeature)
            .map(_.name))))
      spark.sql(
        s"ALTER TABLE delta.`${path.getPath}` SET TBLPROPERTIES (" +
          s"  delta.feature.${TestWriterFeature.name}='enabled'" +
          s")")
      table.upgradeTableProtocol(
        TABLE_FEATURES_MIN_READER_VERSION,
        TABLE_FEATURES_MIN_WRITER_VERSION)
      assert(
        log.snapshot.protocol === Protocol(
          minReaderVersion = TABLE_FEATURES_MIN_READER_VERSION,
          minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures = Some(Set()),
          writerFeatures = Some(
            Set(
              AppendOnlyTableFeature,
              ChangeDataFeedTableFeature,
              CheckConstraintsTableFeature,
              ColumnMappingTableFeature,
              GeneratedColumnsTableFeature,
              InvariantsTableFeature,
              TestLegacyWriterFeature,
              TestRemovableLegacyWriterFeature,
              TestLegacyReaderWriterFeature,
              TestRemovableLegacyReaderWriterFeature,
              TestWriterFeature)
              .map(_.name))))
    }
  }

  test("protocol upgrade using SQL API") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(1, 2), path)

      assert(log.snapshot.protocol === Protocol(1, 2))
      sql(
        s"ALTER TABLE delta.`${path.getCanonicalPath}` " +
          "SET TBLPROPERTIES (delta.minWriterVersion = 3)")
      assert(log.snapshot.protocol === Protocol(1, 3))
      assertPropertiesAndShowTblProperties(log)
      sql(s"ALTER TABLE delta.`${path.getCanonicalPath}` " +
        s"SET TBLPROPERTIES (delta.minWriterVersion=$TABLE_FEATURES_MIN_WRITER_VERSION)")
      assert(
        log.snapshot.protocol === Protocol(
          minReaderVersion = 1,
          minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures = None,
          writerFeatures = Some(
            Set(AppendOnlyTableFeature, CheckConstraintsTableFeature, InvariantsTableFeature)
              .map(_.name))))
      assertPropertiesAndShowTblProperties(log, tableHasFeatures = true)
      sql(s"ALTER TABLE delta.`${path.getCanonicalPath}` " +
        s"SET TBLPROPERTIES (delta.minReaderVersion=$TABLE_FEATURES_MIN_READER_VERSION)")
      assert(
        log.snapshot.protocol === Protocol(
          minReaderVersion = TABLE_FEATURES_MIN_READER_VERSION,
          minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures = Some(Set()),
          writerFeatures = Some(
            Set(AppendOnlyTableFeature, CheckConstraintsTableFeature, InvariantsTableFeature)
              .map(_.name))))
      assertPropertiesAndShowTblProperties(log, tableHasFeatures = true)
    }
  }

  test("overwrite keeps the same protocol version") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(0, 0), path)
      spark.range(1)
        .write
        .format("delta")
        .mode("overwrite")
        .save(path.getCanonicalPath)
      log.update()
      assert(log.snapshot.protocol === Protocol(0, 0))
    }
  }

  test("overwrite keeps the same table properties") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(0, 0), path)
      spark.sql(
        s"ALTER TABLE delta.`${path.getCanonicalPath}` SET TBLPROPERTIES ('myProp'='true')")
      spark
        .range(1)
        .write
        .format("delta")
        .option("anotherProp", "true")
        .mode("overwrite")
        .save(path.getCanonicalPath)
      log.update()
      assert(log.snapshot.metadata.configuration.size === 1)
      assert(log.snapshot.metadata.configuration("myProp") === "true")
    }
  }

  test("overwrite keeps the same protocol version and features") {
    withTempDir { path =>
      val protocol = Protocol(0, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(AppendOnlyTableFeature)
      val log = createTableWithProtocol(protocol, path)
      spark
        .range(1)
        .write
        .format("delta")
        .mode("overwrite")
        .save(path.getCanonicalPath)
      log.update()
      assert(log.snapshot.protocol === protocol)
    }
  }

  test("overwrite with additional configs keeps the same protocol version and features") {
    withTempDir { path =>
      val protocol = Protocol(1, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(AppendOnlyTableFeature)
      val log = createTableWithProtocol(protocol, path)
      spark
        .range(1)
        .write
        .format("delta")
        .option("delta.feature.testWriter", "enabled")
        .option("delta.feature.testReaderWriter", "enabled")
        .mode("overwrite")
        .save(path.getCanonicalPath)
      log.update()
      assert(log.snapshot.protocol === protocol)
    }
  }

  test("overwrite with additional session defaults keeps the same protocol version and features") {
    withTempDir { path =>
      val protocol = Protocol(1, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(AppendOnlyTableFeature)
      val log = createTableWithProtocol(protocol, path)
      withSQLConf(
        s"$DEFAULT_FEATURE_PROP_PREFIX${TestLegacyWriterFeature.name}" -> "enabled") {
        spark
          .range(1)
          .write
          .format("delta")
          .option("delta.feature.testWriter", "enabled")
          .option("delta.feature.testReaderWriter", "enabled")
          .mode("overwrite")
          .save(path.getCanonicalPath)
      }
      log.update()
      assert(log.snapshot.protocol === protocol)
    }
  }

  test("access with protocol too high") {
    withTempDir { path =>
      val log = DeltaLog.forTable(spark, path)
      log.ensureLogDirectoryExist()
      log.store.write(
        deltaFile(log.logPath, 0),
        Iterator(Metadata().json, Protocol(Integer.MAX_VALUE, Integer.MAX_VALUE).json),
        overwrite = false,
        log.newDeltaHadoopConf())
      intercept[InvalidProtocolVersionException] {
        spark.range(1).write.format("delta").save(path.getCanonicalPath)
      }
    }
  }

  test("InvalidProtocolVersionException - error message with protocol too high - table path") {
    withTempDir { path =>
      spark.range(1).write.format("delta").save(path.getCanonicalPath)
      val (deltaLog, snapshot) = DeltaLog.forTableWithSnapshot(spark, path.getCanonicalPath)

      var tableReaderVersion = 4
      var tableWriterVersion = 7
      var version = snapshot.version + 1
      untrackedChangeProtocolVersion(deltaLog, version, tableReaderVersion, tableWriterVersion)

      val exceptionRead = intercept[InvalidProtocolVersionException] {
        spark.read.format("delta").load(path.getCanonicalPath)
      }
      assert(exceptionRead.getMessage == getExpectedProtocolErrorMessage(
        deltaLog.dataPath.toString,
        tableReaderVersion,
        tableWriterVersion))

      tableReaderVersion = 3
      tableWriterVersion = 8
      version = version + 1
      untrackedChangeProtocolVersion(deltaLog, version, tableReaderVersion, tableWriterVersion)

      val exceptionWrite = intercept[InvalidProtocolVersionException] {
        spark.range(1).write
          .mode("append")
          .option("mergeSchema", "true")
          .format("delta")
          .save(path.getCanonicalPath)
      }
      assert(exceptionWrite.getMessage == getExpectedProtocolErrorMessage(
        deltaLog.dataPath.toString,
        tableReaderVersion,
        tableWriterVersion))
    }
  }

  def testInvalidProtocolErrorMessageWithTableName(warm: Boolean): Unit = {
    val protocolTableName = "mytableprotocoltoohigh"
    withTable(protocolTableName) {
      spark.range(1).write.format("delta").saveAsTable(protocolTableName)
      val (deltaLog, snapshot) = DeltaLog.forTableWithSnapshot(
        spark,
        TableIdentifier(protocolTableName))

      var tableReaderVersion = 4
      var tableWriterVersion = 7
      var version = snapshot.version + 1
      untrackedChangeProtocolVersion(deltaLog, version, tableReaderVersion, tableWriterVersion)
      if (!warm) {
        DeltaLog.clearCache()
      }

      val exceptionRead = intercept[InvalidProtocolVersionException] {
        spark.read.format("delta").table(protocolTableName)
      }

      var pathInErrorMessage = "default." + protocolTableName
      val readErrorMessage = getExpectedProtocolErrorMessage(
        pathInErrorMessage,
        tableReaderVersion,
        tableWriterVersion)

      assert(exceptionRead.getMessage == readErrorMessage)

      tableReaderVersion = 3
      tableWriterVersion = 8
      version = version + 1
      untrackedChangeProtocolVersion(deltaLog, version, tableReaderVersion, tableWriterVersion)
      if (!warm) {
        DeltaLog.clearCache()
      }

      val exceptionWrite = intercept[InvalidProtocolVersionException] {
        spark.range(1).write
          .mode("append")
          .option("mergeSchema", "true")
          .format("delta")
          .saveAsTable(protocolTableName)
      }

      assert(exceptionWrite.getMessage ==
        getExpectedProtocolErrorMessage(pathInErrorMessage, tableReaderVersion, tableWriterVersion))

      // Restore the protocol version or the clean-up fails
      version = version + 1
      untrackedChangeProtocolVersion(deltaLog, version, 1, 2)
    }
  }

  test("InvalidProtocolVersionException - error message with table name - warm") {
    testInvalidProtocolErrorMessageWithTableName(true)
  }

  test("InvalidProtocolVersionException - error message with table name - cold") {
    testInvalidProtocolErrorMessageWithTableName(false)
  }

  test("InvalidProtocolVersionException - " +
    "incompatible protocol change during the transaction - table name") {
    for (incompatibleProtocol <- Seq(
      Protocol(minReaderVersion = Int.MaxValue),
      Protocol(minWriterVersion = Int.MaxValue),
      Protocol(minReaderVersion = Int.MaxValue, minWriterVersion = Int.MaxValue)
    )) {
      val tableName = "mytableprotocoltoohigh"
      withTable(tableName) {
        spark.range(0).write.format("delta").saveAsTable(tableName)
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
        val catalogTable = DeltaTableV2(spark, TableIdentifier(tableName)).catalogTable
        val txn = deltaLog.startTransaction(catalogTable)
        val currentVersion = txn.snapshot.version
        untrackedChangeProtocolVersion(deltaLog, currentVersion + 1, incompatibleProtocol)

        // Should detect the above incompatible protocol change and fail
        val exception = intercept[InvalidProtocolVersionException] {
          txn.commit(AddFile("test", Map.empty, 1, 1, dataChange = true) :: Nil, ManualUpdate)
        }

        var pathInErrorMessage = "default." + tableName
        val errorMessage = getExpectedProtocolErrorMessage(
          pathInErrorMessage,
          incompatibleProtocol.minReaderVersion,
          incompatibleProtocol.minWriterVersion)

        assert(exception.getMessage == errorMessage)
      }
    }
  }

  private def untrackedChangeProtocolVersion(
      log: DeltaLog,
      version: Long,
      tableProtocolReaderVersion: Int,
      tableProtocolWriterVersion: Int)
    {
      untrackedChangeProtocolVersion(
        log,
        version,
        Protocol(tableProtocolReaderVersion, tableProtocolWriterVersion))
    }

  private def untrackedChangeProtocolVersion(
      log: DeltaLog,
      version: Long,
      protocol: Protocol): Unit = {
    log.store.write(
      deltaFile(log.logPath, version),
      Iterator(
        Metadata().json,
        protocol.json),
      overwrite = false,
      log.newDeltaHadoopConf())
  }

  def getExpectedProtocolErrorMessage(
      tableNameOrPath: String,
      requiredReaderVersion: Int,
      requiredWriterVersion: Int): String = {
    // When testing flag is enabled Action.supportedReaderVersionNumbers includes 0.
    "[DELTA_INVALID_PROTOCOL_VERSION] " +
      "Unsupported Delta protocol version: table \"" + tableNameOrPath + "\" requires " +
      s"reader version $requiredReaderVersion and " +
      s"writer version $requiredWriterVersion, " +
      "but Delta Lake \"" + io.delta.VERSION + "\" supports reader versions 0, 1, 2, 3 " +
      "and writer versions 0, 1, 2, 3, 4, 5, 7. Please upgrade to a newer release."
  }

  test("protocol downgrade is a no-op") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(2, 3), path)
      assert(log.update().protocol === Protocol(2, 3))

      { // DeltaLog API. This API is internal-only and will fail when downgrade.

        val e = intercept[ProtocolDowngradeException] {
          log.upgradeProtocol(Protocol(1, 2))
        }
        assert(log.update().protocol == Protocol(2, 3))
        assert(e.getErrorClass.contains("DELTA_INVALID_PROTOCOL_DOWNGRADE"))
      }
      { // DeltaTable API
        val table = io.delta.tables.DeltaTable.forPath(spark, path.getCanonicalPath)
        val events = Log4jUsageLogger.track {
          table.upgradeTableProtocol(1, 2)
        }
        assert(log.update().protocol == Protocol(2, 3))
        assert(events.count(_.tags.get("opType").contains("delta.protocol.downgradeIgnored")) === 1)
      }
      { // SQL API
        val events = Log4jUsageLogger.track {
          sql(s"ALTER TABLE delta.`${path.getCanonicalPath}` " +
            "SET TBLPROPERTIES (delta.minWriterVersion = 2)")
        }
        assert(log.update().protocol == Protocol(2, 3))
        assert(events.count(_.tags.get("opType").contains("delta.protocol.downgradeIgnored")) === 1)
      }
    }
  }

  private case class SessionAndTableConfs(name: String, session: Seq[String], table: Seq[String])

  for (confs <- Seq(
      SessionAndTableConfs(
        "session",
        session = Seq(DeltaConfigs.CREATE_TABLE_IGNORE_PROTOCOL_DEFAULTS.defaultTablePropertyKey),
        table = Seq.empty[String]),
      SessionAndTableConfs(
        "table",
        session = Seq.empty[String],
        table = Seq(DeltaConfigs.CREATE_TABLE_IGNORE_PROTOCOL_DEFAULTS.key))))
    test(s"CREATE TABLE can ignore protocol defaults, configured in ${confs.name}") {
      withTempDir { path =>
        withSQLConf(
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "3",
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "7",
          defaultPropertyKey(ChangeDataFeedTableFeature) -> FEATURE_PROP_SUPPORTED) {
          withSQLConf(confs.session.map(_ -> "true"): _*) {
            spark
              .range(10)
              .write
              .format("delta")
              .options(confs.table.map(_ -> "true").toMap)
              .save(path.getCanonicalPath)
          }
        }

        val snapshot = DeltaLog.forTable(spark, path).update()
        assert(snapshot.protocol === Protocol(1, 1))
        assert(
          !snapshot.metadata.configuration
            .contains(DeltaConfigs.CREATE_TABLE_IGNORE_PROTOCOL_DEFAULTS.key))
      }
    }

  for (ignoreProtocolDefaults <- BOOLEAN_DOMAIN)
    for (op <- Seq(
        "ALTER TABLE",
        "SHALLOW CLONE",
        "RESTORE")) {
      test(s"$op always ignore protocol defaults (flag = $ignoreProtocolDefaults)"
      ) {
        withTempDir { path =>
          val expectedProtocol = if (ignoreProtocolDefaults) {
            Protocol(1, 1)
          } else {
            Protocol(
              spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION),
              spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION))
          }

          val cPath = path.getCanonicalPath
          spark
            .range(10)
            .write
            .format("delta")
            .option(
              DeltaConfigs.CREATE_TABLE_IGNORE_PROTOCOL_DEFAULTS.key,
              ignoreProtocolDefaults.toString)
            .save(cPath)
          val snapshot = DeltaLog.forTable(spark, path).update()
          assert(snapshot.protocol === expectedProtocol)
          assert(
            !snapshot.metadata.configuration
              .contains(DeltaConfigs.CREATE_TABLE_IGNORE_PROTOCOL_DEFAULTS.key))

          withSQLConf(
            DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "3",
            DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "7",
            defaultPropertyKey(ChangeDataFeedTableFeature) -> FEATURE_PROP_SUPPORTED) {
            val snapshotAfter = op match {
              case "ALTER TABLE" =>
                sql(s"ALTER TABLE delta.`$cPath` ALTER COLUMN id COMMENT 'hallo'")
                DeltaLog.forTable(spark, path).update()
              case "SHALLOW CLONE" =>
                var s: Snapshot = null
                withTempDir { cloned =>
                  sql(
                    s"CREATE TABLE delta.`${cloned.getCanonicalPath}` " +
                      s"SHALLOW CLONE delta.`$cPath`")
                  s = DeltaLog.forTable(spark, cloned).update()
                }
                s
              case "RESTORE" =>
                sql(s"INSERT INTO delta.`$cPath` VALUES (99)") // version 2
                sql(s"RESTORE TABLE delta.`$cPath` TO VERSION AS OF 1")
                DeltaLog.forTable(spark, path).update()
              case _ =>
                throw new RuntimeException("OP is invalid. Add a match!")
            }
            assert(snapshotAfter.protocol === expectedProtocol)
            assert(
              !snapshotAfter.metadata.configuration
                .contains(DeltaConfigs.CREATE_TABLE_IGNORE_PROTOCOL_DEFAULTS.key))
          }
        }
      }
    }

  test("concurrent upgrade") {
    withTempDir { path =>
      val newProtocol = Protocol()
      val log = createTableWithProtocol(Protocol(0, 0), path)

      // We have to copy out the internals of upgradeProtocol to induce the concurrency.
      val txn = log.startTransaction()
      log.upgradeProtocol(newProtocol)
      intercept[ProtocolChangedException] {
        txn.commit(Seq(newProtocol), DeltaOperations.UpgradeProtocol(newProtocol))
      }
    }
  }

  test("incompatible protocol change during the transaction") {
    for (incompatibleProtocol <- Seq(
      Protocol(minReaderVersion = Int.MaxValue),
      Protocol(minWriterVersion = Int.MaxValue),
      Protocol(minReaderVersion = Int.MaxValue, minWriterVersion = Int.MaxValue)
    )) {
      withTempDir { path =>
        spark.range(0).write.format("delta").save(path.getCanonicalPath)
        val deltaLog = DeltaLog.forTable(spark, path)
        val hadoopConf = deltaLog.newDeltaHadoopConf()
        val txn = deltaLog.startTransaction()
        val currentVersion = txn.snapshot.version
        deltaLog.store.write(
          deltaFile(deltaLog.logPath, currentVersion + 1),
          Iterator(incompatibleProtocol.json),
          overwrite = false,
          hadoopConf)

        // Should detect the above incompatible protocol change and fail
        intercept[InvalidProtocolVersionException] {
          txn.commit(AddFile("test", Map.empty, 1, 1, dataChange = true) :: Nil, ManualUpdate)
        }
        // Make sure we didn't commit anything
        val p = deltaFile(deltaLog.logPath, currentVersion + 2)
        assert(
          !p.getFileSystem(hadoopConf).exists(p),
          s"$p should not be committed")
      }
    }
  }

  import testImplicits._
  /** Creates a Delta table and checks the expected protocol version */
  private def testCreation(tableName: String, writerVersion: Int, tableInitialized: Boolean = false)
                          (fn: String => Unit): Unit = {
    withTempDir { dir =>
      withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "1") {
        withTable(tableName) {
          fn(dir.getCanonicalPath)

          val deltaLog = DeltaLog.forTable(spark, dir)
          assert((deltaLog.snapshot.version != 0) == tableInitialized)
          assert(deltaLog.snapshot.protocol.minWriterVersion === writerVersion)
          assert(deltaLog.snapshot.protocol.minReaderVersion === 1)
        }
      }
    }
  }

  test("can create table using the latest protocol with conf") {
    val readerVersion = Action.supportedProtocolVersion().minReaderVersion
    val writerVersion = Action.supportedProtocolVersion().minWriterVersion
    withTempDir { dir =>
      withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> writerVersion.toString,
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> readerVersion.toString) {
        sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta")
        val deltaLog = DeltaLog.forTable(spark, dir)
        assert(deltaLog.snapshot.protocol ===
               Action.supportedProtocolVersion(withAllFeatures = false))
      }
    }
  }

  test("can create table using features configured in session") {
    val readerVersion = Action.supportedProtocolVersion().minReaderVersion
    val writerVersion = Action.supportedProtocolVersion().minWriterVersion
    withTempDir { dir =>
      withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> writerVersion.toString,
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> readerVersion.toString,
        s"$DEFAULT_FEATURE_PROP_PREFIX${AppendOnlyTableFeature.name}" -> "enabled",
        s"$DEFAULT_FEATURE_PROP_PREFIX${TestReaderWriterFeature.name}" -> "enabled") {
        sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta")
        val deltaLog = DeltaLog.forTable(spark, dir)
        assert(
          deltaLog.snapshot.protocol ===
            Action
              .supportedProtocolVersion(withAllFeatures = false)
              .withFeatures(Set(AppendOnlyTableFeature, TestReaderWriterFeature)))
      }
    }
  }

  test("can create table using features configured in table properties and session") {
    withTempDir { dir =>
      withSQLConf(
        s"$DEFAULT_FEATURE_PROP_PREFIX${TestWriterFeature.name}" -> "enabled") {
        sql(
          s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
            "TBLPROPERTIES (" +
            s"  delta.feature.${AppendOnlyTableFeature.name}='enabled'," +
            s"  delta.feature.${TestLegacyReaderWriterFeature.name}='enabled'" +
            s")")
        val deltaLog = DeltaLog.forTable(spark, dir)
        assert(
          deltaLog.snapshot.protocol.minReaderVersion ===
            TABLE_FEATURES_MIN_READER_VERSION,
          "reader protocol version should support table features because we used the " +
            "'delta.feature.' config.")
        assert(
          deltaLog.snapshot.protocol.minWriterVersion ===
            TABLE_FEATURES_MIN_WRITER_VERSION,
          "reader protocol version should support table features because we used the " +
            "'delta.feature.' config.")
        assert(
          deltaLog.snapshot.protocol.readerAndWriterFeatureNames === Set(
            AppendOnlyTableFeature,
            TestLegacyReaderWriterFeature,
            TestWriterFeature).map(_.name))
      }
    }
  }

  test("creating a new table with default protocol") {
    val tableName = "delta_test"

    def testTableCreation(fn: String => Unit, tableInitialized: Boolean = false): Unit = {
      testCreation(tableName, 1, tableInitialized) { dir =>
        fn(dir)
      }
    }

    testTableCreation { dir => spark.range(10).write.format("delta").save(dir) }
    testTableCreation { dir =>
      spark.range(10).write.format("delta").option("path", dir).saveAsTable(tableName)
    }
    testTableCreation { dir =>
      spark.range(10).writeTo(tableName).using("delta").tableProperty("location", dir).create()
    }
    testTableCreation { dir =>
      sql(s"CREATE TABLE $tableName (id bigint) USING delta LOCATION '$dir'")
    }
    testTableCreation { dir =>
      sql(s"CREATE TABLE $tableName USING delta LOCATION '$dir' AS SELECT * FROM range(10)")
    }
    testTableCreation(dir => {
      val stream = MemoryStream[Int]
      stream.addData(1 to 10)
      val q = stream.toDF().writeStream.format("delta")
        .option("checkpointLocation", new File(dir, "_checkpoint").getCanonicalPath)
        .start(dir)
      q.processAllAvailable()
      q.stop()
    }
    )

    testTableCreation { dir =>
      spark.range(10).write.mode("append").parquet(dir)
      sql(s"CONVERT TO DELTA parquet.`$dir`")
    }
  }

  test(
    "creating a new table with default protocol - requiring more recent protocol version") {
    val tableName = "delta_test"
    def testTableCreation(fn: String => Unit, tableInitialized: Boolean = false): Unit =
      testCreation(tableName, 2, tableInitialized)(fn)

    testTableCreation { dir =>
      spark.range(10).writeTo(tableName).using("delta")
        .tableProperty("location", dir)
        .tableProperty("delta.appendOnly", "true")
        .create()
    }
    testTableCreation { dir =>
      sql(s"CREATE TABLE $tableName (id bigint) USING delta LOCATION '$dir' " +
        s"TBLPROPERTIES (delta.appendOnly = 'true')")
    }
    testTableCreation { dir =>
      sql(s"CREATE TABLE $tableName USING delta TBLPROPERTIES (delta.appendOnly = 'true') " +
        s"LOCATION '$dir' AS SELECT * FROM range(10)")
    }
    testTableCreation { dir =>
      sql(s"CREATE TABLE $tableName (id bigint NOT NULL) USING delta LOCATION '$dir'")
    }

    withSQLConf("spark.databricks.delta.properties.defaults.appendOnly" -> "true") {
      testTableCreation { dir => spark.range(10).write.format("delta").save(dir) }
      testTableCreation { dir =>
        spark.range(10).write.format("delta").option("path", dir).saveAsTable(tableName)
      }
      testTableCreation { dir =>
        spark.range(10).writeTo(tableName).using("delta").tableProperty("location", dir).create()
      }
      testTableCreation { dir =>
        sql(s"CREATE TABLE $tableName (id bigint) USING delta LOCATION '$dir'")
      }
      testTableCreation { dir =>
        sql(s"CREATE TABLE $tableName USING delta LOCATION '$dir' AS SELECT * FROM range(10)")
      }
      testTableCreation(dir => {
        val stream = MemoryStream[Int]
        stream.addData(1 to 10)
        val q = stream.toDF().writeStream.format("delta")
          .option("checkpointLocation", new File(dir, "_checkpoint").getCanonicalPath)
          .start(dir)
        q.processAllAvailable()
        q.stop()
      }
      )

      testTableCreation { dir =>
        spark.range(10).write.mode("append").parquet(dir)
        sql(s"CONVERT TO DELTA parquet.`$dir`")
      }
    }
  }

  test("replacing a new table with default protocol") {
    withTempDir { dir =>
      // In this test we go back and forth through protocol versions, testing the various syntaxes
      // of replacing tables
      val tbl = "delta_test"
      withTable(tbl) {
        withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "1") {
          sql(s"CREATE TABLE $tbl (id bigint) USING delta LOCATION '${dir.getCanonicalPath}'")
        }
        val deltaLog = DeltaLog.forTable(spark, dir)
        assert(deltaLog.snapshot.protocol.minWriterVersion === 1,
          "Should've picked up the protocol from the configuration")

        // Replace the table and make sure the config is picked up
        withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "2") {
          spark.range(10).writeTo(tbl).using("delta")
            .tableProperty("location", dir.getCanonicalPath).replace()
        }
        assert(deltaLog.snapshot.protocol.minWriterVersion === 2,
          "Should've picked up the protocol from the configuration")

        // Will not downgrade without special flag.
        withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "1") {
          sql(s"REPLACE TABLE $tbl (id bigint) USING delta LOCATION '${dir.getCanonicalPath}'")
          assert(deltaLog.snapshot.protocol.minWriterVersion === 2,
            "Should not pick up the protocol from the configuration")
        }

        // Replace with the old writer again
        withSQLConf(
            DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "1",
            DeltaSQLConf.REPLACE_TABLE_PROTOCOL_DOWNGRADE_ALLOWED.key -> "true") {
          sql(s"REPLACE TABLE $tbl (id bigint) USING delta LOCATION '${dir.getCanonicalPath}'")
          assert(deltaLog.snapshot.protocol.minWriterVersion === 1,
            "Should've created a new protocol")

          sql(s"CREATE OR REPLACE TABLE $tbl (id bigint NOT NULL) USING delta " +
            s"LOCATION '${dir.getCanonicalPath}'")
          assert(deltaLog.snapshot.protocol.minWriterVersion === 2,
            "Invariant should require the higher protocol")

          // Go back to version 1
          sql(s"REPLACE TABLE $tbl (id bigint) USING delta LOCATION '${dir.getCanonicalPath}'")
          assert(deltaLog.snapshot.protocol.minWriterVersion === 1,
            "Should've created a new protocol")

          // Check table properties with different syntax
          spark.range(10).writeTo(tbl).tableProperty("location", dir.getCanonicalPath)
            .tableProperty("delta.appendOnly", "true").using("delta").createOrReplace()
          assert(deltaLog.snapshot.protocol.minWriterVersion === 2,
            "appendOnly should require the higher protocol")
        }
      }
    }
  }

  test("create a table with no protocol") {
    withTempDir { path =>
      val log = DeltaLog.forTable(spark, path)
      log.ensureLogDirectoryExist()
      log.store.write(
        deltaFile(log.logPath, 0),
        Iterator(Metadata().json),
        overwrite = false,
        log.newDeltaHadoopConf())

      assert(intercept[DeltaIllegalStateException] {
        log.update()
      }.getErrorClass == "DELTA_STATE_RECOVER_ERROR")
      assert(intercept[DeltaIllegalStateException] {
        spark.read.format("delta").load(path.getCanonicalPath)
      }.getErrorClass == "DELTA_STATE_RECOVER_ERROR")
      assert(intercept[DeltaIllegalStateException] {
        spark.range(1).write.format("delta").mode(SaveMode.Overwrite).save(path.getCanonicalPath)
      }.getErrorClass == "DELTA_STATE_RECOVER_ERROR")
    }
  }

  test("bad inputs for default protocol versions") {
    val readerVersion = Action.supportedProtocolVersion().minReaderVersion
    val writerVersion = Action.supportedProtocolVersion().minWriterVersion
    withTempDir { path =>
      val dir = path.getCanonicalPath
      Seq("abc", "", "0", (readerVersion + 1).toString).foreach { conf =>
        val e = intercept[IllegalArgumentException] {
          withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> conf) {
            spark.range(10).write.format("delta").save(dir)
          }
        }
      }
      Seq("abc", "", "0", (writerVersion + 1).toString).foreach { conf =>
        intercept[IllegalArgumentException] {
          withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> conf) {
            spark.range(10).write.format("delta").save(dir)
          }
        }
      }
    }
  }

  test("table creation with protocol as table property") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "1") {
        sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
          "TBLPROPERTIES (delta.minWriterVersion=3)")

        assert(deltaLog.snapshot.protocol.minReaderVersion === 1)
        assert(deltaLog.snapshot.protocol.minWriterVersion === 3)
        assertPropertiesAndShowTblProperties(deltaLog)
      }
    }
  }

  test("table creation with writer-only features as table property") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(
        s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
          "TBLPROPERTIES (" +
          "  DeLtA.fEaTurE.APPendONly='eNAbled'," +
          "  delta.feature.testWriter='enabled'" +
          ")")

      assert(deltaLog.snapshot.protocol.minReaderVersion === 1)
      assert(
        deltaLog.snapshot.protocol.minWriterVersion === TABLE_FEATURES_MIN_WRITER_VERSION)
      assert(
        deltaLog.snapshot.protocol.readerAndWriterFeatureNames === Set(
          AppendOnlyTableFeature, TestWriterFeature).map(_.name))
      assertPropertiesAndShowTblProperties(deltaLog, tableHasFeatures = true)
    }
  }

  test(
    "table creation with legacy reader-writer features as table property") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(
        s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
          "TBLPROPERTIES (DeLtA.fEaTurE.testLEGACYReaderWritER='eNAbled')")

      assert(
        deltaLog.snapshot.protocol === Protocol(
          TABLE_FEATURES_MIN_READER_VERSION,
          TABLE_FEATURES_MIN_WRITER_VERSION).withFeature(TestLegacyReaderWriterFeature))
    }
  }

  test("table creation with native writer-only features as table property") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(
        s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
          "TBLPROPERTIES (DeLtA.fEaTurE.testWritER='eNAbled')")

      assert(
        deltaLog.snapshot.protocol.minReaderVersion === 1)
      assert(
        deltaLog.snapshot.protocol.minWriterVersion ===
          TABLE_FEATURES_MIN_WRITER_VERSION)
      assert(
        deltaLog.snapshot.protocol.readerAndWriterFeatureNames ===
          Set(TestWriterFeature.name))
      assertPropertiesAndShowTblProperties(deltaLog, tableHasFeatures = true)
    }
  }

  test("table creation with reader-writer features as table property") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(
        s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
          "TBLPROPERTIES (" +
          "  DeLtA.fEaTurE.testLEGACYReaderWritER='eNAbled'," +
          "  DeLtA.fEaTurE.testReaderWritER='enabled'" +
          ")")

      assert(
        deltaLog.snapshot.protocol.minReaderVersion === TABLE_FEATURES_MIN_READER_VERSION)
      assert(
        deltaLog.snapshot.protocol.minWriterVersion === TABLE_FEATURES_MIN_WRITER_VERSION)
      assert(
        deltaLog.snapshot.protocol.readerAndWriterFeatureNames === Set(
          TestLegacyReaderWriterFeature, TestReaderWriterFeature).map(_.name))
      assertPropertiesAndShowTblProperties(deltaLog, tableHasFeatures = true)
    }
  }

  test("table creation with feature as table property and supported protocol version") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(
        s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
          "TBLPROPERTIES (" +
          s"  DEltA.MINREADERversion='$TABLE_FEATURES_MIN_READER_VERSION'," +
          s"  DEltA.MINWRITERversion='$TABLE_FEATURES_MIN_WRITER_VERSION'," +
          "  DeLtA.fEaTurE.testLEGACYReaderWriter='eNAbled'" +
          ")")

      assert(
        deltaLog.snapshot.protocol === Protocol(
          minReaderVersion = TABLE_FEATURES_MIN_READER_VERSION,
          minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures =
            Some(Set(TestLegacyReaderWriterFeature.name)),
          writerFeatures =
            Some(Set(TestLegacyReaderWriterFeature.name))))
      assertPropertiesAndShowTblProperties(deltaLog, tableHasFeatures = true)
    }
  }

  test("table creation with feature as table property and supported writer protocol version") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(
        s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
          s"TBLPROPERTIES (" +
          s"  delta.minWriterVersion='$TABLE_FEATURES_MIN_WRITER_VERSION'," +
          s"  delta.feature.testLegacyWriter='enabled'" +
          s")")

      assert(
        deltaLog.snapshot.protocol === Protocol(
          minReaderVersion = 1,
          minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures = None,
          writerFeatures = Some(Set(TestLegacyWriterFeature.name))))
      assertPropertiesAndShowTblProperties(deltaLog, tableHasFeatures = true)
    }
  }

  test("table creation with automatically-enabled features") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(
        s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta TBLPROPERTIES (" +
          s"  ${TestReaderWriterMetadataAutoUpdateFeature.TABLE_PROP_KEY}='true'" +
          ")")
      assert(
        deltaLog.snapshot.protocol === Protocol(
          minReaderVersion = TABLE_FEATURES_MIN_READER_VERSION,
          minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures = Some(Set(TestReaderWriterMetadataAutoUpdateFeature.name)),
          writerFeatures = Some(Set(TestReaderWriterMetadataAutoUpdateFeature.name))))
      assertPropertiesAndShowTblProperties(deltaLog, tableHasFeatures = true)
    }
  }

  test("table creation with automatically-enabled legacy feature and unsupported protocol") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(
        s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta TBLPROPERTIES (" +
          "  delta.minReaderVersion='1'," +
          "  delta.minWriterVersion='2'," +
          "  delta.enableChangeDataFeed='true'" +
          ")")
      assert(deltaLog.snapshot.protocol.minReaderVersion === 1)
      assert(deltaLog.snapshot.protocol.minWriterVersion === 4)
    }
  }

  test("table creation with automatically-enabled native feature and unsupported protocol") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(
        s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta TBLPROPERTIES (" +
          "  delta.minReaderVersion='1'," +
          "  delta.minWriterVersion='2'," +
          s"  ${TestReaderWriterMetadataAutoUpdateFeature.TABLE_PROP_KEY}='true'" +
          ")")
      assert(
        deltaLog.snapshot.protocol === Protocol(
          minReaderVersion = TABLE_FEATURES_MIN_READER_VERSION,
          minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures = Some(Set(TestReaderWriterMetadataAutoUpdateFeature.name)),
          writerFeatures = Some(Set(TestReaderWriterMetadataAutoUpdateFeature.name))))
      assertPropertiesAndShowTblProperties(deltaLog, tableHasFeatures = true)
    }
  }

  test("table creation with feature as table property and unsupported protocol version") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(
        s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta TBLPROPERTIES (" +
          "  delta.minReaderVersion='1'," +
          "  delta.minWriterVersion='2'," +
          "  delta.feature.testWriter='enabled'" +
          ")")
      assert(
        deltaLog.snapshot.protocol === Protocol(
          minReaderVersion = 1,
          minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures = None,
          writerFeatures = Some(Set(TestWriterFeature.name))))
      assertPropertiesAndShowTblProperties(deltaLog, tableHasFeatures = true)
    }
  }

  def testCreateTable(
      name: String,
      props: Map[String, String],
      expectedExceptionClass: Option[String] = None,
      expectedFinalProtocol: Option[Protocol] = None): Unit = {
    test(s"create table - $name") {
      withTempDir { dir =>
        val log = DeltaLog.forTable(spark, dir)

        val propString = props.map(kv => s"'${kv._1}'='${kv._2}'").mkString(",")
        if (expectedExceptionClass.isDefined) {
          assert(intercept[DeltaTableFeatureException] {
            sql(
              s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
                s"TBLPROPERTIES ($propString)")
          }.getErrorClass === expectedExceptionClass.get)
        } else {
          sql(
            s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
              s"TBLPROPERTIES ($propString)")
        }
        expectedFinalProtocol match {
          case Some(p) => assert(log.update().protocol === p)
          case None => // Do nothing
        }
      }
    }
  }

  testCreateTable(
    "legacy protocol, legacy feature, metadata",
    Map("delta.appendOnly" -> "true"),
    expectedFinalProtocol = Some(Protocol(1, 2)))

  testCreateTable(
    "legacy protocol, legacy feature, feature property",
    Map(s"delta.feature.${TestLegacyReaderWriterFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestLegacyReaderWriterFeature)))

  testCreateTable(
    "legacy protocol, legacy writer feature, feature property",
    Map(s"delta.feature.${TestLegacyWriterFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(1, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestLegacyWriterFeature)))

  testCreateTable(
    "legacy protocol, native auto-update feature, metadata",
    Map(TestReaderWriterMetadataAutoUpdateFeature.TABLE_PROP_KEY -> "true"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataAutoUpdateFeature)))

  testCreateTable(
    "legacy protocol, native non-auto-update feature, metadata",
    Map(TestReaderWriterMetadataNoAutoUpdateFeature.TABLE_PROP_KEY -> "true"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataNoAutoUpdateFeature)))

  testCreateTable(
    "legacy protocol, native auto-update feature, feature property",
    Map(s"delta.feature.${TestReaderWriterMetadataAutoUpdateFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataAutoUpdateFeature)))

  testCreateTable(
    "legacy protocol, native non-auto-update feature, feature property",
    Map(s"delta.feature.${TestReaderWriterMetadataNoAutoUpdateFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataNoAutoUpdateFeature)))

  testCreateTable(
    "legacy protocol with supported version props, legacy feature, feature property",
    Map(
      DeltaConfigs.MIN_READER_VERSION.key ->
        TestLegacyReaderWriterFeature.minReaderVersion.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key ->
        TestLegacyReaderWriterFeature.minWriterVersion.toString,
      s"delta.feature.${TestLegacyReaderWriterFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestLegacyReaderWriterFeature)))

  testCreateTable(
    "legacy protocol with table feature version props, legacy feature, feature property",
    Map(
      DeltaConfigs.MIN_READER_VERSION.key -> TABLE_FEATURES_MIN_READER_VERSION.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> TABLE_FEATURES_MIN_WRITER_VERSION.toString,
      s"delta.feature.${TestLegacyReaderWriterFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestLegacyReaderWriterFeature)))

  testCreateTable(
    "legacy protocol with supported version props, native feature, feature property",
    Map(
      DeltaConfigs.MIN_READER_VERSION.key -> TABLE_FEATURES_MIN_READER_VERSION.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> TABLE_FEATURES_MIN_WRITER_VERSION.toString,
      s"delta.feature.${TestReaderWriterMetadataAutoUpdateFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataAutoUpdateFeature)))

  testCreateTable(
    "table features protocol, legacy feature, metadata",
    Map(
      DeltaConfigs.MIN_READER_VERSION.key -> TABLE_FEATURES_MIN_READER_VERSION.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> TABLE_FEATURES_MIN_WRITER_VERSION.toString,
      "delta.appendOnly" -> "true"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(AppendOnlyTableFeature)))

  testCreateTable(
    "table features protocol, legacy feature, feature property",
    Map(
      DeltaConfigs.MIN_READER_VERSION.key -> TABLE_FEATURES_MIN_READER_VERSION.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> TABLE_FEATURES_MIN_WRITER_VERSION.toString,
      s"delta.feature.${TestLegacyReaderWriterFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestLegacyReaderWriterFeature)))

  testCreateTable(
    "table features protocol, native auto-update feature, metadata",
    Map(
      DeltaConfigs.MIN_READER_VERSION.key -> TABLE_FEATURES_MIN_READER_VERSION.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> TABLE_FEATURES_MIN_WRITER_VERSION.toString,
      TestReaderWriterMetadataAutoUpdateFeature.TABLE_PROP_KEY -> "true"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataAutoUpdateFeature)))

  testCreateTable(
    "table features protocol, native non-auto-update feature, metadata",
    Map(
      DeltaConfigs.MIN_READER_VERSION.key -> TABLE_FEATURES_MIN_READER_VERSION.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> TABLE_FEATURES_MIN_WRITER_VERSION.toString,
      TestReaderWriterMetadataNoAutoUpdateFeature.TABLE_PROP_KEY -> "true"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataNoAutoUpdateFeature)))

  testCreateTable(
    "table features protocol, native auto-update feature, feature property",
    Map(
      DeltaConfigs.MIN_READER_VERSION.key -> TABLE_FEATURES_MIN_READER_VERSION.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> TABLE_FEATURES_MIN_WRITER_VERSION.toString,
      s"delta.feature.${TestReaderWriterMetadataAutoUpdateFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataAutoUpdateFeature)))

  testCreateTable(
    "table features protocol, native non-auto-update feature, feature property",
    Map(
      DeltaConfigs.MIN_READER_VERSION.key -> TABLE_FEATURES_MIN_READER_VERSION.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> TABLE_FEATURES_MIN_WRITER_VERSION.toString,
      s"delta.feature.${TestReaderWriterMetadataNoAutoUpdateFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataNoAutoUpdateFeature)))

  testCreateTable(
    name = "feature with a dependency",
    props = Map(
      DeltaConfigs.MIN_READER_VERSION.key -> TABLE_FEATURES_MIN_READER_VERSION.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> TABLE_FEATURES_MIN_WRITER_VERSION.toString,
      s"delta.feature.${TestFeatureWithDependency.name}" -> "supported"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeatures(Seq(TestFeatureWithDependency, TestReaderWriterFeature))))

  testCreateTable(
    name = "feature with a dependency, enabled using a feature property",
    props = Map(
      DeltaConfigs.MIN_READER_VERSION.key -> TABLE_FEATURES_MIN_READER_VERSION.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> TABLE_FEATURES_MIN_WRITER_VERSION.toString,
      TestFeatureWithDependency.TABLE_PROP_KEY -> "true"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeatures(Seq(TestFeatureWithDependency, TestReaderWriterFeature))))

  testCreateTable(
    name = "feature with a dependency that has a dependency",
    props = Map(
      DeltaConfigs.MIN_READER_VERSION.key -> TABLE_FEATURES_MIN_READER_VERSION.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> TABLE_FEATURES_MIN_WRITER_VERSION.toString,
      s"delta.feature.${TestFeatureWithTransitiveDependency.name}" -> "supported"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeatures(Seq(
          TestFeatureWithTransitiveDependency,
          TestFeatureWithDependency,
          TestReaderWriterFeature))))

  def testAlterTable(
      name: String,
      props: Map[String, String],
      expectedExceptionClass: Option[String] = None,
      expectedFinalProtocol: Option[Protocol] = None,
      tableProtocol: Protocol = Protocol(1, 1)): Unit = {
    test(s"alter table - $name") {
      withTempDir { dir =>
        val log = createTableWithProtocol(tableProtocol, dir)

        val propString = props.map(kv => s"'${kv._1}'='${kv._2}'").mkString(",")
        if (expectedExceptionClass.isDefined) {
          assert(intercept[DeltaTableFeatureException] {
            sql(s"ALTER TABLE delta.`${dir.getCanonicalPath}` SET TBLPROPERTIES ($propString)")
          }.getErrorClass === expectedExceptionClass.get)
        } else {
          sql(s"ALTER TABLE delta.`${dir.getCanonicalPath}` SET TBLPROPERTIES ($propString)")
        }
        expectedFinalProtocol match {
          case Some(p) => assert(log.update().protocol === p)
          case None => // Do nothing
        }
      }
    }
  }

  testAlterTable(
    name = "downgrade reader version is a no-op",
    tableProtocol = Protocol(2, 2),
    props = Map(DeltaConfigs.MIN_READER_VERSION.key -> "1"),
    expectedFinalProtocol = Some(Protocol(2, 2)))

  testAlterTable(
    name = "downgrade writer version is a no-op",
    tableProtocol = Protocol(2, 2),
    props = Map(DeltaConfigs.MIN_WRITER_VERSION.key -> "1"),
    expectedFinalProtocol = Some(Protocol(2, 2)))

  testAlterTable(
    name = "downgrade both reader and versions version is a no-op",
    tableProtocol = Protocol(2, 2),
    props = Map(
      DeltaConfigs.MIN_READER_VERSION.key -> "1",
      DeltaConfigs.MIN_WRITER_VERSION.key -> "1"),
    expectedFinalProtocol = Some(Protocol(2, 2)))

  testAlterTable(
    name = "downgrade reader but upgrade writer versions (legacy protocol)",
    tableProtocol = Protocol(2, 2),
    props = Map(
      DeltaConfigs.MIN_READER_VERSION.key -> "1",
      DeltaConfigs.MIN_WRITER_VERSION.key -> "5"),
    expectedFinalProtocol = Some(Protocol(2, 5)))

  testAlterTable(
    name = "downgrade reader but upgrade writer versions (table features protocol)",
    tableProtocol = Protocol(2, 2),
    props = Map(
      DeltaConfigs.MIN_READER_VERSION.key -> "1",
      DeltaConfigs.MIN_WRITER_VERSION.key -> "7"),
    expectedFinalProtocol = Some(
      Protocol(2, 7).withFeatures(
        Seq(AppendOnlyTableFeature, InvariantsTableFeature)))) // Features from writer version 2

  testAlterTable(
    name = "downgrade while enabling a feature will become an upgrade",
    tableProtocol = Protocol(2, 2),
    props = Map(
      DeltaConfigs.MIN_READER_VERSION.key -> "1",
      DeltaConfigs.MIN_WRITER_VERSION.key -> "1",
      DeltaConfigs.CHANGE_DATA_FEED.key -> "true"),
    expectedFinalProtocol = Some(Protocol(2, 4)))

  testAlterTable(
    "legacy protocol, legacy feature, metadata",
    Map("delta.appendOnly" -> "true"),
    expectedFinalProtocol = Some(Protocol(1, 2)))

  testAlterTable(
    "legacy protocol, legacy feature, feature property",
    Map(s"delta.feature.${TestLegacyReaderWriterFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestLegacyReaderWriterFeature)))

  testAlterTable(
    "legacy protocol, legacy writer feature, feature property",
    Map(s"delta.feature.${TestLegacyWriterFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(1, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestLegacyWriterFeature)
        .merge(Protocol(1, 2))),
    tableProtocol = Protocol(1, 2))

  testAlterTable(
    "legacy protocol, native auto-update feature, metadata",
    Map(TestReaderWriterMetadataAutoUpdateFeature.TABLE_PROP_KEY -> "true"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataAutoUpdateFeature)))

  testAlterTable(
    "legacy protocol, native non-auto-update feature, metadata",
    Map(TestReaderWriterMetadataNoAutoUpdateFeature.TABLE_PROP_KEY -> "true"),
    expectedExceptionClass = Some("DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT"))

  testAlterTable(
    "legacy protocol, native non-auto-update feature, metadata and feature property",
    Map(
      TestReaderWriterMetadataNoAutoUpdateFeature.TABLE_PROP_KEY -> "true",
      s"delta.feature.${TestReaderWriterMetadataNoAutoUpdateFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataNoAutoUpdateFeature)))

  testAlterTable(
    "legacy protocol, native auto-update feature, feature property",
    Map(s"delta.feature.${TestReaderWriterMetadataAutoUpdateFeature.name}" -> "supported"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataAutoUpdateFeature)))

  testAlterTable(
    "legacy protocol, native non-auto-update feature, feature property",
    Map(s"delta.feature.${TestReaderWriterMetadataNoAutoUpdateFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataNoAutoUpdateFeature)))

  testAlterTable(
    "legacy protocol with supported version props, legacy feature, feature property",
    Map(
      DeltaConfigs.MIN_READER_VERSION.key ->
        TestLegacyReaderWriterFeature.minReaderVersion.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key ->
        TestLegacyReaderWriterFeature.minWriterVersion.toString,
      s"delta.feature.${TestLegacyReaderWriterFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .merge(TestLegacyReaderWriterFeature.minProtocolVersion)))

  testAlterTable(
    "legacy protocol with table feature version props, legacy feature, feature property",
    Map(
      DeltaConfigs.MIN_READER_VERSION.key -> TABLE_FEATURES_MIN_READER_VERSION.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> TABLE_FEATURES_MIN_WRITER_VERSION.toString,
      s"delta.feature.${TestLegacyReaderWriterFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestLegacyReaderWriterFeature)))

  testAlterTable(
    "legacy protocol with supported version props, native feature, feature property",
    Map(
      DeltaConfigs.MIN_READER_VERSION.key -> TABLE_FEATURES_MIN_READER_VERSION.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> TABLE_FEATURES_MIN_WRITER_VERSION.toString,
      s"delta.feature.${TestReaderWriterMetadataAutoUpdateFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataAutoUpdateFeature)))

  testAlterTable(
    "table features protocol, legacy feature, metadata",
    Map("delta.appendOnly" -> "true"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(AppendOnlyTableFeature)),
    tableProtocol =
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION))

  testAlterTable(
    "table features protocol, legacy feature, feature property",
    Map(s"delta.feature.${TestLegacyReaderWriterFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestLegacyReaderWriterFeature)),
    tableProtocol =
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION))

  testAlterTable(
    "table features protocol, native auto-update feature, metadata",
    Map(TestReaderWriterMetadataAutoUpdateFeature.TABLE_PROP_KEY -> "true"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataAutoUpdateFeature)),
    tableProtocol =
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION))

  testAlterTable(
    "table features protocol, native non-auto-update feature, metadata",
    Map(TestReaderWriterMetadataNoAutoUpdateFeature.TABLE_PROP_KEY -> "true"),
    tableProtocol =
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION),
    expectedExceptionClass = Some("DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT"))

  testAlterTable(
    "table features protocol, native non-auto-update feature, metadata and feature property",
    Map(
      TestReaderWriterMetadataNoAutoUpdateFeature.TABLE_PROP_KEY -> "true",
      s"delta.feature.${TestReaderWriterMetadataNoAutoUpdateFeature.name}" -> "enabled"),
    tableProtocol =
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataNoAutoUpdateFeature)))

  testAlterTable(
    "table features protocol, native auto-update feature, feature property",
    Map(s"delta.feature.${TestReaderWriterMetadataAutoUpdateFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataAutoUpdateFeature)),
    tableProtocol =
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION))

  testAlterTable(
    "table features protocol, native non-auto-update feature, feature property",
    Map(s"delta.feature.${TestReaderWriterMetadataNoAutoUpdateFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataNoAutoUpdateFeature)),
    tableProtocol =
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION))

  testAlterTable(
    "feature property merges the old protocol",
    Map(s"delta.feature.${TestReaderWriterMetadataAutoUpdateFeature.name}" -> "enabled"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterMetadataAutoUpdateFeature).merge(Protocol(1, 2))),
    tableProtocol = Protocol(1, 2))

  testAlterTable(
    name = "feature with a dependency",
    tableProtocol = Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION),
    props = Map(s"delta.feature.${TestFeatureWithDependency.name}" -> "supported"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeatures(Seq(TestFeatureWithDependency, TestReaderWriterFeature))))

  testAlterTable(
    name = "feature with a dependency, enabled using a feature property",
    tableProtocol = Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION),
    props = Map(TestFeatureWithDependency.TABLE_PROP_KEY -> "true"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeatures(Seq(TestFeatureWithDependency, TestReaderWriterFeature))))

  testAlterTable(
    name = "feature with a dependency that has a dependency",
    tableProtocol = Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION),
    props = Map(s"delta.feature.${TestFeatureWithTransitiveDependency.name}" -> "supported"),
    expectedFinalProtocol = Some(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeatures(Seq(
          TestFeatureWithTransitiveDependency,
          TestFeatureWithDependency,
          TestReaderWriterFeature))))

  test("non-auto-update capable feature requires manual enablement (via feature prop)") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "1",
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "1") {
        spark.range(10).writeTo(s"delta.`${dir.getCanonicalPath}`").using("delta").create()
      }
      val expectedProtocolOnCreation = Protocol(1, 1)
      assert(deltaLog.update().protocol === expectedProtocolOnCreation)

      assert(intercept[DeltaTableFeatureException] {
        withSQLConf(defaultPropertyKey(TestWriterMetadataNoAutoUpdateFeature) -> "supported") {
          sql(
            s"ALTER TABLE delta.`${dir.getCanonicalPath}` SET TBLPROPERTIES (" +
              s"  '${TestWriterMetadataNoAutoUpdateFeature.TABLE_PROP_KEY}' = 'true')")
        }
      }.getErrorClass === "DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT",
      "existing tables should ignore session defaults.")

      sql(
        s"ALTER TABLE delta.`${dir.getCanonicalPath}` SET TBLPROPERTIES (" +
          s"  '${propertyKey(TestWriterMetadataNoAutoUpdateFeature)}' = 'supported'," +
          s"  '${TestWriterMetadataNoAutoUpdateFeature.TABLE_PROP_KEY}' = 'true')")
      assert(
        deltaLog.update().protocol ===
          expectedProtocolOnCreation
            .merge(TestWriterMetadataNoAutoUpdateFeature.minProtocolVersion)
            .withFeature(TestWriterMetadataNoAutoUpdateFeature))
    }
  }

  test("non-auto-update capable error message is correct") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)

      withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "1",
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "1") {
        spark.range(10).writeTo(s"delta.`${dir.getCanonicalPath}`")
          .tableProperty("delta.appendOnly", "true")
          .using("delta")
          .create()
        val protocolOfNewTable = Protocol(1, 2)
        assert(deltaLog.update().protocol === protocolOfNewTable)

        val e = intercept[DeltaTableFeatureException] {
          // ALTER TABLE must not consider this SQL config
          withSQLConf(defaultPropertyKey(TestWriterFeature) -> "supported") {
            sql(
              s"ALTER TABLE delta.`${dir.getCanonicalPath}` SET TBLPROPERTIES (" +
                s"  'delta.appendOnly' = 'false'," +
                s"  'delta.enableChangeDataFeed' = 'true'," +
                s"  '${TestReaderWriterMetadataAutoUpdateFeature.TABLE_PROP_KEY}' = 'true'," +
                s"  '${TestWriterMetadataNoAutoUpdateFeature.TABLE_PROP_KEY}' = 'true')")
          }
        }

        val unsupportedFeatures = TestWriterMetadataNoAutoUpdateFeature.name
        val supportedFeatures =
          (protocolOfNewTable.implicitlyAndExplicitlySupportedFeatures +
            ChangeDataFeedTableFeature +
            TestReaderWriterMetadataAutoUpdateFeature).map(_.name).toSeq.sorted.mkString(", ")
        assert(e.getErrorClass === "DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT")

        // `getMessageParameters` is available starting from Spark 3.4.
        // For now we have to check for substrings.
        assert(e.getMessage.contains(s" $unsupportedFeatures."))
        assert(e.getMessage.contains(s" $supportedFeatures."))

      }
    }
  }

  test("table creation with protocol as table property - property wins over conf") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "3") {
        sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
          "TBLPROPERTIES (delta.MINwriterVERsion=2)")

        assert(deltaLog.snapshot.protocol.minWriterVersion === 2)
        assertPropertiesAndShowTblProperties(deltaLog)
      }
    }
  }

  test("table creation with protocol as table property - feature requirements win SQL") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "1") {
        sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
          "TBLPROPERTIES (delta.minWriterVersion=1, delta.appendOnly=true)")

        assert(deltaLog.snapshot.protocol.minWriterVersion === 2)
        assertPropertiesAndShowTblProperties(deltaLog)
      }
    }
  }

  test("table creation with protocol as table property - feature requirements win DF") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "1") {
        spark.range(10).writeTo(s"delta.`${dir.getCanonicalPath}`")
          .tableProperty("delta.minWriterVersion", "1")
          .tableProperty("delta.appendOnly", "true")
          .using("delta")
          .create()

        assert(deltaLog.snapshot.protocol.minWriterVersion === 2)
        assertPropertiesAndShowTblProperties(deltaLog)
      }
    }
  }

  test("table creation with protocol as table property - default table properties") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      withSQLConf((DeltaConfigs.sqlConfPrefix + "minWriterVersion") -> "3") {
        spark.range(10).writeTo(s"delta.`${dir.getCanonicalPath}`")
          .using("delta")
          .create()

        assert(deltaLog.snapshot.protocol.minWriterVersion === 3)
        assertPropertiesAndShowTblProperties(deltaLog)
      }
    }
  }

  test("table creation with protocol as table property - explicit wins over conf") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      withSQLConf((DeltaConfigs.sqlConfPrefix + "minWriterVersion") -> "3") {
        spark.range(10).writeTo(s"delta.`${dir.getCanonicalPath}`")
          .tableProperty("delta.minWriterVersion", "2")
          .using("delta")
          .create()

        assert(deltaLog.snapshot.protocol.minWriterVersion === 2)
        assertPropertiesAndShowTblProperties(deltaLog)
      }
    }
  }

  test("table creation with protocol as table property - bad input") {
    withTempDir { dir =>
      val e = intercept[IllegalArgumentException] {
        sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
          "TBLPROPERTIES (delta.minWriterVersion='delta rulz')")
      }
      assert(e.getMessage.contains(" one of "))

      val e2 = intercept[AnalysisException] {
        sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
          "TBLPROPERTIES (delta.minWr1terVersion=2)") // Typo in minWriterVersion
      }
      assert(e2.getMessage.contains("Unknown configuration"))

      val e3 = intercept[IllegalArgumentException] {
        sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta " +
          "TBLPROPERTIES (delta.minWriterVersion='-1')")
      }
      assert(e3.getMessage.contains(" one of "))
    }
  }

  test("protocol as table property - desc table") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      withSQLConf(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "2") {
        spark.range(10).writeTo(s"delta.`${dir.getCanonicalPath}`")
          .using("delta")
          .tableProperty("delta.minWriterVersion", "3")
          .createOrReplace()
      }
      assert(deltaLog.snapshot.protocol.minWriterVersion === 3)

      val output = spark.sql(s"DESC EXTENDED delta.`${dir.getCanonicalPath}`").collect()
      assert(output.exists(_.toString.contains("delta.minWriterVersion")),
        s"minWriterVersion not found in: ${output.mkString("\n")}")
      assert(output.exists(_.toString.contains("delta.minReaderVersion")),
        s"minReaderVersion not found in: ${output.mkString("\n")}")
    }
  }

  test("auto upgrade protocol version - version 2") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(1, 1), path)
      spark.sql(s"""
                   |ALTER TABLE delta.`${log.dataPath.toString}`
                   |SET TBLPROPERTIES ('delta.appendOnly' = 'true')
                 """.stripMargin)
      assert(log.snapshot.protocol.minWriterVersion === 2)
    }
  }

  test("auto upgrade protocol version - version 3") {
    withTempDir { path =>
      val log = DeltaLog.forTable(spark, path)
      sql(s"CREATE TABLE delta.`${path.getCanonicalPath}` (id bigint) USING delta " +
        "TBLPROPERTIES (delta.minWriterVersion=2)")
      assert(log.update().protocol.minWriterVersion === 2)
      spark.sql(s"""
                   |ALTER TABLE delta.`${path.getCanonicalPath}`
                   |ADD CONSTRAINT test CHECK (id < 5)
                 """.stripMargin)
      assert(log.update().protocol.minWriterVersion === 3)
    }
  }

  test("auto upgrade protocol version even with explicit protocol version configs") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(1, 1), path)
      spark.sql(s"""
                   |ALTER TABLE delta.`${log.dataPath.toString}` SET TBLPROPERTIES (
                   |  'delta.minWriterVersion' = '2',
                   |  'delta.enableChangeDataFeed' = 'true'
                   |)""".stripMargin)
      assert(log.snapshot.protocol.minWriterVersion === 4)
    }
  }

  test("legacy feature can be listed during alter table with silent protocol upgrade") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(1, 1), path)
      spark.sql(s"""
                   |ALTER TABLE delta.`${log.dataPath.toString}` SET TBLPROPERTIES (
                   |  'delta.feature.testLegacyReaderWriter' = 'enabled'
                   |)""".stripMargin)
      assert(
        log.snapshot.protocol === Protocol(
          TABLE_FEATURES_MIN_READER_VERSION,
          TABLE_FEATURES_MIN_WRITER_VERSION).withFeature(TestLegacyReaderWriterFeature))
    }
  }

  test("legacy feature can be explicitly listed during alter table") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(2, TABLE_FEATURES_MIN_WRITER_VERSION), path)
      spark.sql(s"""
                   |ALTER TABLE delta.`${log.dataPath.toString}` SET TBLPROPERTIES (
                   |  'delta.feature.testLegacyReaderWriter' = 'enabled'
                   |)""".stripMargin)
      assert(log.snapshot.protocol === Protocol(
        TABLE_FEATURES_MIN_READER_VERSION,
        TABLE_FEATURES_MIN_WRITER_VERSION,
        readerFeatures = Some(Set(TestLegacyReaderWriterFeature.name)),
        writerFeatures = Some(Set(TestLegacyReaderWriterFeature.name))))
    }
  }

  test("native feature can be explicitly listed during alter table with silent protocol upgrade") {
    withTempDir { path =>
      val log = createTableWithProtocol(Protocol(1, 2), path)
      spark.sql(s"""
                   |ALTER TABLE delta.`${log.dataPath.toString}` SET TBLPROPERTIES (
                   |  'delta.feature.testReaderWriter' = 'enabled'
                   |)""".stripMargin)
      assert(
        log.snapshot.protocol ===
          TestReaderWriterFeature.minProtocolVersion
            .withFeature(TestReaderWriterFeature)
            .merge(Protocol(1, 2)))
    }
  }

  test("all active features are enabled in protocol") {
    withTempDir { path =>
      spark.range(10).write.format("delta").save(path.getCanonicalPath)
      val log = DeltaLog.forTable(spark, path)
      val snapshot = log.unsafeVolatileSnapshot
      val p = Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
      val m = snapshot.metadata.copy(configuration = snapshot.metadata.configuration ++ Map(
        DeltaConfigs.IS_APPEND_ONLY.key -> "false",
        DeltaConfigs.CHANGE_DATA_FEED.key -> "true"))
      log.store.write(
        deltaFile(log.logPath, snapshot.version + 1),
        Iterator(m.json, p.json),
        overwrite = false,
        log.newDeltaHadoopConf())
      val e = intercept[DeltaTableFeatureException] {
        spark.read.format("delta").load(path.getCanonicalPath).collect()
      }
      assert(e.getMessage.contains("enabled in metadata but not listed in protocol"))
      assert(e.getMessage.contains(": changeDataFeed."))
    }
  }

  test("table feature status") {
    withTempDir { path =>
      withSQLConf(
        defaultPropertyKey(ChangeDataFeedTableFeature) -> FEATURE_PROP_SUPPORTED,
        defaultPropertyKey(GeneratedColumnsTableFeature) -> FEATURE_PROP_ENABLED) {
        spark.range(10).write.format("delta").save(path.getCanonicalPath)
        val log = DeltaLog.forTable(spark, path)
        val protocol = log.update().protocol

        assert(protocol.isFeatureSupported(ChangeDataFeedTableFeature))
        assert(protocol.isFeatureSupported(GeneratedColumnsTableFeature))
      }
    }
  }

  private def replaceTableAs(path: File): Unit = {
    val p = path.getCanonicalPath
    sql(s"REPLACE TABLE delta.`$p` USING delta AS (SELECT * FROM delta.`$p`)")
  }

  test("REPLACE AS updates protocol when defaults are higher") {
    withTempDir { path =>
      spark
        .range(10)
        .write
        .format("delta")
        .option(DeltaConfigs.MIN_READER_VERSION.key, 1)
        .option(DeltaConfigs.MIN_WRITER_VERSION.key, 2)
        .mode("append")
        .save(path.getCanonicalPath)
      val log = DeltaLog.forTable(spark, path)
      assert(log.update().protocol === Protocol(1, 2))
      withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "2",
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "5") {
        replaceTableAs(path)
      }
      assert(log.update().protocol === Protocol(2, 5))
      withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "3",
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "7",
        TableFeatureProtocolUtils.defaultPropertyKey(TestReaderWriterFeature) -> "enabled") {
        replaceTableAs(path)
      }
      assert(
        log.update().protocol ===
          Protocol(2, 5).merge(Protocol(3, 7)).withFeature(TestReaderWriterFeature))
    }
  }

  for (p <- Seq(Protocol(2, 5), Protocol(3, 7).withFeature(TestReaderWriterFeature)))
    test(s"REPLACE AS keeps protocol when defaults are lower ($p)") {
      withTempDir { path =>
        spark
          .range(10)
          .write
          .format("delta")
          .option(DeltaConfigs.MIN_READER_VERSION.key, p.minReaderVersion)
          .option(DeltaConfigs.MIN_WRITER_VERSION.key, p.minWriterVersion)
          .options(
            p.readerAndWriterFeatureNames
              .flatMap(TableFeature.featureNameToFeature)
              .map(f => TableFeatureProtocolUtils.propertyKey(f) -> "enabled")
              .toMap)
          .mode("append")
          .save(path.getCanonicalPath)
        val log = DeltaLog.forTable(spark, path)
        assert(log.update().protocol === p)
        withSQLConf(
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "1",
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "2") {
          replaceTableAs(path)
        }
        assert(log.update().protocol === p.merge(Protocol(1, 2)))
        withSQLConf(
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "1",
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "2",
          TableFeatureProtocolUtils.defaultPropertyKey(TestReaderWriterFeature) -> "enabled") {
          replaceTableAs(path)
        }
        assert(
          log.update().protocol ===
            p
              .merge(Protocol(1, 2))
              .merge(
                TestReaderWriterFeature.minProtocolVersion.withFeature(TestReaderWriterFeature)))
      }
    }

  test("REPLACE AS can ignore protocol defaults") {
    withTempDir { path =>
      withSQLConf(
          DeltaConfigs.CREATE_TABLE_IGNORE_PROTOCOL_DEFAULTS.defaultTablePropertyKey -> "true") {
        spark.range(10).write.format("delta").save(path.getCanonicalPath)
      }
      val log = DeltaLog.forTable(spark, path)
      assert(log.update().protocol === Protocol(1, 1))

      withSQLConf(
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "3",
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "7",
          defaultPropertyKey(ChangeDataFeedTableFeature) -> FEATURE_PROP_SUPPORTED,
          DeltaConfigs.CREATE_TABLE_IGNORE_PROTOCOL_DEFAULTS.defaultTablePropertyKey -> "true") {
        replaceTableAs(path)
      }
      assert(log.update().protocol === Protocol(1, 1))
      assert(
        !log.update().metadata.configuration
          .contains(DeltaConfigs.CREATE_TABLE_IGNORE_PROTOCOL_DEFAULTS.key))
    }
  }

  test("protocol change logging") {
    withTempDir { path =>
      val dir = path.getCanonicalPath
      withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "1",
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "2") {
        assert(
          captureProtocolChangeEventBlob {
            sql(s"CREATE TABLE delta.`$dir` (id INT) USING delta")
          } === Map(
            "toProtocol" -> Map(
              "minReaderVersion" -> 1,
              "minWriterVersion" -> 2,
              "supportedFeatures" -> List("appendOnly", "invariants")
            )))
      }

      // Upgrade protocol
      assert(captureProtocolChangeEventBlob {
        sql(
          s"ALTER TABLE delta.`$dir` " +
            s"SET TBLPROPERTIES (${DeltaConfigs.MIN_WRITER_VERSION.key} = '7')")
      } === Map(
        "fromProtocol" -> Map(
          "minReaderVersion" -> 1,
          "minWriterVersion" -> 2,
          "supportedFeatures" -> List("appendOnly", "invariants")
        ),
        "toProtocol" -> Map(
          "minReaderVersion" -> 1,
          "minWriterVersion" -> 7,
          "supportedFeatures" -> List("appendOnly", "invariants")
        )))

      // Add feature
      assert(captureProtocolChangeEventBlob {
        sql(
          s"ALTER TABLE delta.`$dir` " +
            s"SET TBLPROPERTIES (${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key} = 'true')")
      } === Map(
        "fromProtocol" -> Map(
          "minReaderVersion" -> 1,
          "minWriterVersion" -> 7,
          "supportedFeatures" -> List("appendOnly", "invariants")
        ),
        "toProtocol" -> Map(
          "minReaderVersion" -> 3,
          "minWriterVersion" -> 7,
          "supportedFeatures" -> List("appendOnly", "deletionVectors", "invariants")
        )))
    }
  }

  test("protocol change logging using commitLarge") {
    withTempDir { path =>
      withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "1",
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "2") {
        assert(
          captureProtocolChangeEventBlob {
            sql(s"CREATE TABLE delta.`${path.getCanonicalPath}` (id INT) USING delta")
          } === Map(
            "toProtocol" -> Map(
              "minReaderVersion" -> 1,
              "minWriterVersion" -> 2,
              "supportedFeatures" -> List("appendOnly", "invariants")
            )))
      }

      // Clone table to invoke commitLarge
      withTempDir { clonedPath =>
        assert(
          captureProtocolChangeEventBlob {
            sql(s"CREATE TABLE delta.`${clonedPath.getCanonicalPath}` " +
              s"SHALLOW CLONE delta.`${path.getCanonicalPath}` " +
              s"TBLPROPERTIES (${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key} = 'true')")
          } === Map(
            "toProtocol" -> Map(
              "minReaderVersion" -> 3,
              "minWriterVersion" -> 7,
              "supportedFeatures" -> List("appendOnly", "deletionVectors", "invariants")
            )))
      }
    }
  }

  test("can't write to a table with identity columns (legacy protocol)") {
    withTempDir { dir =>
      val writerVersion = 6
      createTableWithProtocol(Protocol(1, writerVersion), dir)

      checkAnswer(
        sql(s"SELECT * FROM delta.`${dir.getCanonicalPath}`"),
        spark.range(0).toDF)
      assert(intercept[InvalidProtocolVersionException] {
        sql(s"INSERT INTO delta.`${dir.getCanonicalPath}` VALUES (9)")
      }.getMessage.contains(s" and writer version 6"))
    }
  }

  test("can't write to a table with identity columns (table features)") {
    withTempDir { dir =>
      val featureName = "identityColumns"
      createTableWithProtocol(
        Protocol(
          TABLE_FEATURES_MIN_READER_VERSION,
          TABLE_FEATURES_MIN_WRITER_VERSION,
          readerFeatures = Some(Set.empty),
          writerFeatures = Some(Set(featureName))),
        dir)

      checkAnswer(
        sql(s"SELECT * FROM delta.`${dir.getCanonicalPath}`"),
        spark.range(0).toDF)
      assert(intercept[DeltaTableFeatureException] {
        sql(s"INSERT INTO delta.`${dir.getCanonicalPath}` VALUES (9)")
      }.getMessage.contains(s"unsupported by this version of Delta Lake: $featureName"))
    }
  }

  def protocolWithFeatures(
      readerFeatures: Seq[TableFeature] = Seq.empty,
      writerFeatures: Seq[TableFeature] = Seq.empty): Protocol = {
    val readerFeaturesEnabled = readerFeatures.nonEmpty
    val writerFeaturesEnabled = readerFeatures.nonEmpty || writerFeatures.nonEmpty
    val minReaderVersion = if (readerFeaturesEnabled) TABLE_FEATURES_MIN_READER_VERSION else 1
    val minWriterVersion = if (writerFeaturesEnabled) TABLE_FEATURES_MIN_WRITER_VERSION else 1
    val readerFeatureNames =
      if (readerFeaturesEnabled) Some(readerFeatures.map(_.name).toSet) else None
    val writerFeatureNames = if (writerFeaturesEnabled) {
      Some((readerFeatures ++ writerFeatures).map(_.name).toSet)
    } else {
      None
    }

    Protocol(
      minReaderVersion = minReaderVersion,
      minWriterVersion = minWriterVersion,
      readerFeatures = readerFeatureNames,
      writerFeatures = writerFeatureNames)
  }

  def protocolWithReaderFeature(readerFeature: TableFeature): Protocol = {
    protocolWithFeatures(readerFeatures = Seq(readerFeature))
  }

  def protocolWithWriterFeature(writerFeature: TableFeature): Protocol = {
    protocolWithFeatures(writerFeatures = Seq(writerFeature))
  }

  def emptyProtocolWithWriterFeatures: Protocol =
    Protocol(
      minReaderVersion = 1,
      minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
      readerFeatures = None,
      writerFeatures = Some(Set.empty))

  def emptyProtocolWithReaderFeatures: Protocol =
    Protocol(
      minReaderVersion = TABLE_FEATURES_MIN_READER_VERSION,
      minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
      readerFeatures = Some(Set.empty),
      writerFeatures = Some(Set.empty))

  protected def createTableWithFeature(
      deltaLog: DeltaLog,
      feature: TableFeature,
      featureProperty: String): Unit = {
    sql(s"""CREATE TABLE delta.`${deltaLog.dataPath}` (id bigint) USING delta
           |TBLPROPERTIES (
           |delta.minReaderVersion = $TABLE_FEATURES_MIN_READER_VERSION,
           |delta.feature.${feature.name} = 'supported',
           |$featureProperty = "true"
           |)""".stripMargin)

    val expectedWriterFeatures = Some(Set(feature.name))
    val expectedReaderFeatures: Option[Set[String]] =
      if (feature.isReaderWriterFeature) expectedWriterFeatures else Some(Set.empty)

    assert(
      deltaLog.update().protocol === Protocol(
        minReaderVersion = TABLE_FEATURES_MIN_READER_VERSION,
        minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
        readerFeatures = expectedReaderFeatures,
        writerFeatures = expectedWriterFeatures))
  }

  /** Assumes there is at least 1 commit. */
  def getEarliestCommitVersion(deltaLog: DeltaLog): Long =
    deltaLog.listFrom(0L).collectFirst { case DeltaFile(_, v) => v }.get

  def testWriterFeatureRemoval(
      feature: TableFeature,
      featurePropertyKey: String): Unit = {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      createTableWithFeature(deltaLog, feature, featurePropertyKey)

      withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
        AlterTableDropFeatureDeltaCommand(
          DeltaTableV2(spark, deltaLog.dataPath),
          feature.name).run(spark)
      }

      // Writer feature is removed from the writer features set.
      val snapshot = deltaLog.update()
      assert(snapshot.protocol === Protocol(1, 1))
      assert(!snapshot.metadata.configuration.contains(featurePropertyKey))
      assertPropertiesAndShowTblProperties(deltaLog)
    }
  }

  def truncateHistoryDefaultLogRetention: CalendarInterval =
    DeltaConfigs.parseCalendarInterval(
      DeltaConfigs.TABLE_FEATURE_DROP_TRUNCATE_HISTORY_LOG_RETENTION.defaultValue)

  def testReaderFeatureRemoval(
      feature: TableFeature,
      featurePropertyKey: String,
      advanceClockPastRetentionPeriod: Boolean = true,
      truncateHistory: Boolean = false,
      truncateHistoryRetentionOpt: Option[String] = None): Unit = {
    withTempDir { dir =>
      val truncateHistoryRetention = truncateHistoryRetentionOpt
        .map(DeltaConfigs.parseCalendarInterval)
        .getOrElse(truncateHistoryDefaultLogRetention)
      val clock = new ManualClock(System.currentTimeMillis())
      val deltaLog = DeltaLog.forTable(spark, dir, clock)

      createTableWithFeature(deltaLog, feature, featurePropertyKey)

      if (truncateHistoryRetentionOpt.nonEmpty) {
        val propertyKey = DeltaConfigs.TABLE_FEATURE_DROP_TRUNCATE_HISTORY_LOG_RETENTION.key
        AlterTableSetPropertiesDeltaCommand(
          DeltaTableV2(spark, deltaLog.dataPath),
          Map(propertyKey -> truncateHistoryRetention.toString)).run(spark)
      }

      withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
        // First attempt should cleanup feature traces but fail with a message due to historical
        // log entries containing the feature.
        val e1 = intercept[DeltaTableFeatureException] {
          AlterTableDropFeatureDeltaCommand(
            DeltaTableV2(spark, deltaLog.dataPath),
            feature.name).run(spark)
        }
        checkError(
          exception = e1,
          errorClass = "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD",
          parameters = Map(
            "feature" -> feature.name,
            "logRetentionPeriodKey" -> "delta.logRetentionDuration",
            "logRetentionPeriod" -> "30 days",
            "truncateHistoryLogRetentionPeriod" -> truncateHistoryRetention.toString))

        // Add some more commits.
        spark.range(0, 100).write.format("delta").mode("append").save(dir.getCanonicalPath)
        spark.range(100, 120).write.format("delta").mode("append").save(dir.getCanonicalPath)

        // Table still contains historical data with the feature. Attempt should fail.
        val e2 = intercept[DeltaTableFeatureException] {
          AlterTableDropFeatureDeltaCommand(
            DeltaTableV2(spark, deltaLog.dataPath),
            feature.name).run(spark)
        }
        checkError(
          exception = e2,
          errorClass = "DELTA_FEATURE_DROP_HISTORICAL_VERSIONS_EXIST",
          parameters = Map(
            "feature" -> feature.name,
            "logRetentionPeriodKey" -> "delta.logRetentionDuration",
            "logRetentionPeriod" -> "30 days",
            "truncateHistoryLogRetentionPeriod" -> truncateHistoryRetention.toString))

        // Generate commit.
        spark.range(120, 140).write.format("delta").mode("append").save(dir.getCanonicalPath)

        // Pretend retention period has passed.
        if (advanceClockPastRetentionPeriod) {
          val clockAdvanceMillis = if (truncateHistory) {
            DeltaConfigs.getMilliSeconds(truncateHistoryRetention)
          } else {
            deltaLog.deltaRetentionMillis(deltaLog.update().metadata)
          }
          clock.advance(clockAdvanceMillis + TimeUnit.HOURS.toMillis(1))
        }

        val dropCommand = AlterTableDropFeatureDeltaCommand(
          DeltaTableV2(spark, deltaLog.dataPath),
          feature.name,
          truncateHistory)

        if (advanceClockPastRetentionPeriod) {
          // History is now clean. We should be able to remove the feature.
          dropCommand.run(spark)

          // Reader+writer feature is removed from the features set.
          val snapshot = deltaLog.update()
          assert(snapshot.protocol === Protocol(1, 1))
          assert(!snapshot.metadata.configuration.contains(featurePropertyKey))
          assertPropertiesAndShowTblProperties(deltaLog)
        } else {
          // When the clock did not advance the logs are not cleaned. We should detect there
          // are still versions that contain traces of the feature.
          val e3 = intercept[DeltaTableFeatureException] {
            dropCommand.run(spark)
          }
          checkError(
            exception = e3,
            errorClass = "DELTA_FEATURE_DROP_HISTORICAL_VERSIONS_EXIST",
            parameters = Map(
              "feature" -> feature.name,
              "logRetentionPeriodKey" -> "delta.logRetentionDuration",
              "logRetentionPeriod" -> "30 days",
              "truncateHistoryLogRetentionPeriod" -> truncateHistoryRetention.toString))
        }
      }

      // Verify commits before the checkpoint are cleaned.
      val earliestExpectedCommitVersion =
        if (advanceClockPastRetentionPeriod) {
          deltaLog.findEarliestReliableCheckpoint().get
        } else {
          0L
        }
      assert(getEarliestCommitVersion(deltaLog) === earliestExpectedCommitVersion)

      // Validate extra commits.
      val table = io.delta.tables.DeltaTable.forPath(deltaLog.dataPath.toString)
      assert(table.toDF.count() == 140)
    }
  }

  test("Remove writer feature") {
    testWriterFeatureRemoval(
      TestRemovableWriterFeature,
      TestRemovableWriterFeature.TABLE_PROP_KEY)
  }

  test("Remove legacy writer feature") {
    testWriterFeatureRemoval(
      TestRemovableLegacyWriterFeature,
      TestRemovableLegacyWriterFeature.TABLE_PROP_KEY)
  }


  for {
    advanceClockPastRetentionPeriod <- BOOLEAN_DOMAIN
    truncateHistory <- if (advanceClockPastRetentionPeriod) BOOLEAN_DOMAIN else Seq(false)
    retentionOpt <- if (truncateHistory) Seq(Some("12 hours"), None) else Seq(None)
  } test(s"Remove reader+writer feature " +
      s"advanceClockPastRetentionPeriod: $advanceClockPastRetentionPeriod " +
      s"truncateHistory: $truncateHistory " +
      s"retentionOpt: ${retentionOpt.getOrElse("None")}") {
    testReaderFeatureRemoval(
      TestRemovableReaderWriterFeature,
      TestRemovableReaderWriterFeature.TABLE_PROP_KEY,
      advanceClockPastRetentionPeriod,
      truncateHistory,
      retentionOpt)
  }

  test("Remove legacy reader+writer feature") {
    testReaderFeatureRemoval(
      TestRemovableLegacyReaderWriterFeature,
      TestRemovableLegacyReaderWriterFeature.TABLE_PROP_KEY)
  }

  test("Remove writer feature when table protocol does not support reader features") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
             |TBLPROPERTIES (
             |delta.feature.${TestWriterFeature.name} = 'supported',
             |delta.feature.${TestRemovableWriterFeature.name} = 'supported'
             |)""".stripMargin)

      val protocol = deltaLog.update().protocol
      assert(protocol === protocolWithFeatures(
        writerFeatures = Seq(TestWriterFeature, TestRemovableWriterFeature)))

      withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
        val command = AlterTableDropFeatureDeltaCommand(
          DeltaTableV2(spark, deltaLog.dataPath),
          TestRemovableWriterFeature.name)
        command.run(spark)

        assert(
          deltaLog.update().protocol === Protocol(
            minReaderVersion = 1,
            minWriterVersion = TABLE_FEATURES_MIN_WRITER_VERSION,
            readerFeatures = None,
            writerFeatures = Some(Set(TestWriterFeature.name))))
      }
    }
  }

  test("Remove a non-removable feature") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
             |TBLPROPERTIES (
             |delta.feature.${TestWriterMetadataNoAutoUpdateFeature.name} = 'supported'
             |)""".stripMargin)

      val expectedProtocol = protocolWithWriterFeature(TestWriterMetadataNoAutoUpdateFeature)
      assert(deltaLog.update().protocol === expectedProtocol)

      withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
        val command = AlterTableDropFeatureDeltaCommand(
          DeltaTableV2(spark, deltaLog.dataPath),
          TestWriterMetadataNoAutoUpdateFeature.name)

        val e = intercept[DeltaTableFeatureException] {
          command.run(spark)
        }
        checkError(
          exception = e,
          errorClass = "DELTA_FEATURE_DROP_NONREMOVABLE_FEATURE",
          parameters = Map("feature" -> TestWriterMetadataNoAutoUpdateFeature.name))
      }
    }
  }

  test("Remove an implicit writer feature") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
             |TBLPROPERTIES (
             |delta.minWriterVersion = 2)""".stripMargin)

      assert(deltaLog.update().protocol === Protocol(minReaderVersion = 1, minWriterVersion = 2))

      // Try removing AppendOnly which is an implicitly supported feature (writer version 2).
      withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
        val command = AlterTableDropFeatureDeltaCommand(
          DeltaTableV2(spark, deltaLog.dataPath),
          AppendOnlyTableFeature.name)
        val e = intercept[DeltaTableFeatureException] {
          command.run(spark)
        }
        checkError(
          exception = e,
          errorClass = "DELTA_FEATURE_DROP_NONREMOVABLE_FEATURE",
          parameters = Map("feature" -> AppendOnlyTableFeature.name))
      }
    }
  }

  test("Remove a feature not supported by the client") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(s"CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta")

      assert(
        deltaLog.update().protocol === Protocol(
          minReaderVersion = 1,
          minWriterVersion = 2,
          readerFeatures = None,
          writerFeatures = None))

      withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
        val command = AlterTableDropFeatureDeltaCommand(
          DeltaTableV2(spark, deltaLog.dataPath),
          "NonSupportedFeature")

        val e = intercept[DeltaTableFeatureException] {
          command.run(spark)
        }
        checkError(
          exception = e,
          errorClass = "DELTA_FEATURE_DROP_UNSUPPORTED_CLIENT_FEATURE",
          parameters = Map("feature" -> "NonSupportedFeature"))
      }
    }
  }

  for (withTableFeatures <- BOOLEAN_DOMAIN)
  test(s"Remove a feature not present in the protocol - withTableFeatures: $withTableFeatures") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      val (minReaderVersion, minWriterVersion) = if (withTableFeatures) {
        (TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
      } else {
        (1, 2)
      }
      sql(
        s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
           |TBLPROPERTIES (
           |delta.minReaderVersion = $minReaderVersion,
           |delta.minWriterVersion = $minWriterVersion)""".stripMargin)

      assert(
        deltaLog.update().protocol === Protocol(
          minReaderVersion = minReaderVersion,
          minWriterVersion = minWriterVersion,
          readerFeatures = if (withTableFeatures) Some(Set.empty) else None,
          writerFeatures = if (withTableFeatures) Some(Set.empty) else None))

      withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
        val command = AlterTableDropFeatureDeltaCommand(
          DeltaTableV2(spark, deltaLog.dataPath),
          TestRemovableWriterFeature.name)

        val e = intercept[DeltaTableFeatureException] {
          command.run(spark)
        }
        checkError(
          exception = e,
          errorClass = "DELTA_FEATURE_DROP_FEATURE_NOT_PRESENT",
          parameters = Map("feature" -> TestRemovableWriterFeature.name))
      }
    }
  }

  test("Reintroduce a feature after removing it") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)
      sql(s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
             |TBLPROPERTIES (
             |delta.feature.${TestRemovableWriterFeature.name} = 'supported'
             |)""".stripMargin)

      val protocol = deltaLog.update().protocol
      assert(protocol === protocolWithWriterFeature(TestRemovableWriterFeature))

      withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
        val command = AlterTableDropFeatureDeltaCommand(
          DeltaTableV2(spark, deltaLog.dataPath),
          TestRemovableWriterFeature.name)
        command.run(spark)
      }
      assert(deltaLog.update().protocol === Protocol(1, 1))

      sql(s"""ALTER TABLE delta.`${dir.getCanonicalPath}` SET TBLPROPERTIES (
             |delta.feature.${TestRemovableWriterFeature.name} = 'supported'
             |)""".stripMargin)

      val expectedProtocolAfterReintroduction =
        protocolWithFeatures(writerFeatures = Seq(TestRemovableWriterFeature))
      assert(deltaLog.update().protocol === expectedProtocolAfterReintroduction)
    }
  }

  test(s"Truncate history while dropping a writer feature") {
    withTempDir { dir =>
      val table = s"delta.`${dir.getCanonicalPath}`"
      val deltaLog = DeltaLog.forTable(spark, dir)

      createTableWithFeature(
        deltaLog,
        feature = TestRemovableWriterFeature,
        featureProperty = TestRemovableWriterFeature.TABLE_PROP_KEY)

      withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
        val e = intercept[DeltaTableFeatureException] {
          sql(s"""ALTER TABLE $table
                 |DROP FEATURE ${TestRemovableWriterFeature.name}
                 |TRUNCATE HISTORY""".stripMargin)
        }
        checkError(
          exception = e,
          errorClass = "DELTA_FEATURE_DROP_HISTORY_TRUNCATION_NOT_ALLOWED",
          parameters = Map.empty)
      }
    }
  }

  for {
    reEnablePropertyValue <- BOOLEAN_DOMAIN
    reDisable <- BOOLEAN_DOMAIN
  } test("Try removing reader+writer feature but re-enable feature after disablement " +
      s"reEnablePropertyValue: $reEnablePropertyValue " +
      s"reDisable: $reDisable") {
    withTempDir { dir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val deltaLog = DeltaLog.forTable(spark, dir, clock)

      createTableWithFeature(
        deltaLog,
        feature = TestRemovableReaderWriterFeature,
        featureProperty = TestRemovableReaderWriterFeature.TABLE_PROP_KEY)

      withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
        // Add some more commits.
        spark.range(0, 100).write.format("delta").mode("append").save(dir.getCanonicalPath)
        spark.range(100, 120).write.format("delta").mode("append").save(dir.getCanonicalPath)

        // First attempt should cleanup feature traces but fail with a message due to historical
        // log entries containing the feature.
        val e1 = intercept[DeltaTableFeatureException] {
          AlterTableDropFeatureDeltaCommand(
            DeltaTableV2(spark, deltaLog.dataPath),
            TestRemovableReaderWriterFeature.name).run(spark)
        }
        checkError(
          exception = e1,
          errorClass = "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD",
          parameters = Map(
            "feature" -> TestRemovableReaderWriterFeature.name,
            "logRetentionPeriodKey" -> "delta.logRetentionDuration",
            "logRetentionPeriod" -> "30 days",
            "truncateHistoryLogRetentionPeriod" -> truncateHistoryDefaultLogRetention.toString))

        val deltaRetentionMillis = deltaLog.deltaRetentionMillis(deltaLog.update().metadata)
        require(deltaRetentionMillis === TimeUnit.DAYS.toMillis(30))

        // Ten days have passed.
        clock.advance(TimeUnit.DAYS.toMillis(10))

        // Generate commit.
        spark.range(120, 140).write.format("delta").mode("append").save(dir.getCanonicalPath)

        // Add feature property again.
        val v2Table = DeltaTableV2(spark, deltaLog.dataPath)
        AlterTableSetPropertiesDeltaCommand(
          v2Table,
          Map(TestRemovableReaderWriterFeature.TABLE_PROP_KEY -> reEnablePropertyValue.toString))
          .run(spark)

        // Disable by removing property.
        if (reDisable) {
          val properties = Seq(TestRemovableReaderWriterFeature.TABLE_PROP_KEY)
          AlterTableUnsetPropertiesDeltaCommand(v2Table, properties, ifExists = true).run(spark)
        }

        // The retention period has passed since the disablement.
        clock.advance(
          deltaRetentionMillis - TimeUnit.DAYS.toMillis(10) + TimeUnit.HOURS.toMillis(1))

        // Cleanup logs.
        deltaLog.cleanUpExpiredLogs(deltaLog.update())

        // Feature was enabled again in the middle of the timeframe. The feature traces are
        // are cleaned up again and we get a new "Wait for retention period message."
        val e2 = intercept[DeltaTableFeatureException] {
          AlterTableDropFeatureDeltaCommand(
            DeltaTableV2(spark, deltaLog.dataPath),
            TestRemovableReaderWriterFeature.name).run(spark)
          }

        // If the property is re-disabled we pick up the issue during the history check.
        if (reDisable) {
          checkError(
            exception = e2,
            errorClass = "DELTA_FEATURE_DROP_HISTORICAL_VERSIONS_EXIST",
            parameters = Map(
              "feature" -> TestRemovableReaderWriterFeature.name,
              "logRetentionPeriodKey" -> "delta.logRetentionDuration",
              "logRetentionPeriod" -> "30 days",
              "truncateHistoryLogRetentionPeriod" -> truncateHistoryDefaultLogRetention.toString))
        } else {
          checkError(
            exception = e2,
            errorClass = "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD",
            parameters = Map(
              "feature" -> TestRemovableReaderWriterFeature.name,
              "logRetentionPeriodKey" -> "delta.logRetentionDuration",
              "logRetentionPeriod" -> "30 days",
              "truncateHistoryLogRetentionPeriod" -> truncateHistoryDefaultLogRetention.toString))
        }
      }
    }
  }

  test("Remove reader+writer feature with shortened retention period") {
    withTempDir { dir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val deltaLog = DeltaLog.forTable(spark, dir, clock)

      createTableWithFeature(
        deltaLog,
        feature = TestRemovableReaderWriterFeature,
        featureProperty = TestRemovableReaderWriterFeature.TABLE_PROP_KEY)

      withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
        // First attempt should cleanup feature traces but fail with a message due to historical
        // log entries containing the feature.
        val e1 = intercept[DeltaTableFeatureException] {
          AlterTableDropFeatureDeltaCommand(
            DeltaTableV2(spark, deltaLog.dataPath),
            TestRemovableReaderWriterFeature.name).run(spark)
        }
        checkError(
          exception = e1,
          errorClass = "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD",
          parameters = Map(
            "feature" -> TestRemovableReaderWriterFeature.name,
            "logRetentionPeriodKey" -> "delta.logRetentionDuration",
            "logRetentionPeriod" -> "30 days",
            "truncateHistoryLogRetentionPeriod" -> truncateHistoryDefaultLogRetention.toString))

        // Set retention period to a day.
        AlterTableSetPropertiesDeltaCommand(
          DeltaTableV2(spark, deltaLog.dataPath),
          Map(DeltaConfigs.LOG_RETENTION.key -> "1 DAY")).run(spark)

        // Metadata is not cleaned yet. Attempt should fail.
        val e2 = intercept[DeltaTableFeatureException] {
          AlterTableDropFeatureDeltaCommand(
            DeltaTableV2(spark, deltaLog.dataPath),
            TestRemovableReaderWriterFeature.name).run(spark)
        }
        checkError(
          exception = e2,
          errorClass = "DELTA_FEATURE_DROP_HISTORICAL_VERSIONS_EXIST",
          parameters = Map(
            "feature" -> TestRemovableReaderWriterFeature.name,
            "logRetentionPeriodKey" -> "delta.logRetentionDuration",
            "logRetentionPeriod" -> "1 days",
            "truncateHistoryLogRetentionPeriod" -> truncateHistoryDefaultLogRetention.toString))

        spark.range(1, 100).write.format("delta").mode("append").save(dir.getCanonicalPath)

        // Pretend retention period has passed.
        clock.advance(
          deltaLog.deltaRetentionMillis(deltaLog.update().metadata) +
          TimeUnit.HOURS.toMillis(1))

        // History is now clean. We should be able to remove the feature.
        AlterTableDropFeatureDeltaCommand(
          DeltaTableV2(spark, deltaLog.dataPath),
          TestRemovableReaderWriterFeature.name).run(spark)

        // Verify commits before the checkpoint are cleaned.
        val earliestExpectedCommitVersion = deltaLog.findEarliestReliableCheckpoint().get
        assert(getEarliestCommitVersion(deltaLog) === earliestExpectedCommitVersion)
      }

      // Reader+writer feature is removed from the features set.
      val snapshot = deltaLog.update()
      assert(snapshot.protocol === Protocol(1, 1))
      assert(!snapshot.metadata.configuration
        .contains(TestRemovableReaderWriterFeature.TABLE_PROP_KEY))
      assertPropertiesAndShowTblProperties(deltaLog)
    }
  }

  test("Try removing reader+writer feature after restore") {
    withTempDir { dir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val deltaLog = DeltaLog.forTable(spark, dir, clock)

      createTableWithFeature(
        deltaLog,
        feature = TestRemovableReaderWriterFeature,
        featureProperty = TestRemovableReaderWriterFeature.TABLE_PROP_KEY)

      withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
        val preRemovalVersion = deltaLog.update().version

        // Cleanup feature traces and throw message to wait retention period to expire.
        val e1 = intercept[DeltaTableFeatureException] {
          AlterTableDropFeatureDeltaCommand(
            DeltaTableV2(spark, deltaLog.dataPath),
            TestRemovableReaderWriterFeature.name).run(spark)
        }
        checkError(
          exception = e1,
          errorClass = "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD",
          parameters = Map(
            "feature" -> TestRemovableReaderWriterFeature.name,
            "logRetentionPeriodKey" -> "delta.logRetentionDuration",
            "logRetentionPeriod" -> "30 days",
            "truncateHistoryLogRetentionPeriod" -> truncateHistoryDefaultLogRetention.toString))

        // Add some more commits.
        spark.range(0, 100).write.format("delta").mode("append").save(dir.getCanonicalPath)
        spark.range(100, 120).write.format("delta").mode("append").save(dir.getCanonicalPath)

        // Restore table to an older version with feature traces.
        sql(s"RESTORE delta.`${deltaLog.dataPath}` TO VERSION AS OF $preRemovalVersion")

        // Drop command should detect that latest version has feature traces and run
        // preDowngrade again.
        val e2 = intercept[DeltaTableFeatureException] {
          AlterTableDropFeatureDeltaCommand(
            DeltaTableV2(spark, deltaLog.dataPath),
            TestRemovableReaderWriterFeature.name).run(spark)
        }
        checkError(
          exception = e2,
          errorClass = "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD",
          parameters = Map(
            "feature" -> TestRemovableReaderWriterFeature.name,
            "logRetentionPeriodKey" -> "delta.logRetentionDuration",
            "logRetentionPeriod" -> "30 days",
            "truncateHistoryLogRetentionPeriod" -> truncateHistoryDefaultLogRetention.toString))
      }
    }
  }

  test("Remove reader+writer feature after unrelated metadata change") {
    withTempDir { dir =>
      val clock = new ManualClock(System.currentTimeMillis())
      val deltaLog = DeltaLog.forTable(spark, dir, clock)

      createTableWithFeature(
        deltaLog,
        feature = TestRemovableReaderWriterFeature,
        featureProperty = TestRemovableReaderWriterFeature.TABLE_PROP_KEY)

      withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
        // First attempt should cleanup feature traces but fail with a message due to historical
        // log entries containing the feature.
        val e1 = intercept[DeltaTableFeatureException] {
          AlterTableDropFeatureDeltaCommand(
            DeltaTableV2(spark, deltaLog.dataPath),
            TestRemovableReaderWriterFeature.name).run(spark)
        }
        checkError(
          exception = e1,
          errorClass = "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD",
          parameters = Map(
            "feature" -> TestRemovableReaderWriterFeature.name,
            "logRetentionPeriodKey" -> "delta.logRetentionDuration",
            "logRetentionPeriod" -> "30 days",
            "truncateHistoryLogRetentionPeriod" -> truncateHistoryDefaultLogRetention.toString))

        // Add some more commits.
        spark.range(0, 100).write.format("delta").mode("append").save(dir.getCanonicalPath)
        spark.range(100, 120).write.format("delta").mode("append").save(dir.getCanonicalPath)

        // Pretend retention period has passed.
        clock.advance(deltaLog.deltaRetentionMillis(deltaLog.update().metadata) +
          TimeUnit.HOURS.toMillis(1))

        // Perform an unrelated metadata change.
        sql(s"ALTER TABLE delta.`${deltaLog.dataPath}` ADD COLUMN (value INT)")

        // The unrelated metadata change should not interfere with validation and we should
        // be able to downgrade the protocol.
        AlterTableDropFeatureDeltaCommand(
          DeltaTableV2(spark, deltaLog.dataPath),
          TestRemovableReaderWriterFeature.name).run(spark)

        // Verify commits before the checkpoint are cleaned.
        val earliestExpectedCommitVersion = deltaLog.findEarliestReliableCheckpoint().get
        assert(getEarliestCommitVersion(deltaLog) === earliestExpectedCommitVersion)
      }
    }
  }

  for {
    withCatalog <- BOOLEAN_DOMAIN
    quoteWith <- if (withCatalog) Seq ("none", "single", "backtick") else Seq("none")
  } test(s"Drop feature DDL - withCatalog=$withCatalog, quoteWith=$quoteWith") {
    withTempDir { dir =>
      val table = if (withCatalog) "table" else s"delta.`${dir.getCanonicalPath}`"
      if (withCatalog) sql(s"DROP TABLE IF EXISTS $table")
      sql(
        s"""CREATE TABLE $table (id bigint) USING delta
           |TBLPROPERTIES (
           |delta.feature.${TestRemovableWriterFeature.name} = 'supported'
           |)""".stripMargin)

      val deltaLog = if (withCatalog) {
        DeltaLog.forTable(spark, TableIdentifier(table))
      } else {
        DeltaLog.forTable(spark, dir)
      }

      AlterTableSetPropertiesDeltaCommand(
        DeltaTableV2(spark, deltaLog.dataPath),
        Map(TestRemovableWriterFeature.TABLE_PROP_KEY -> "true")).run(spark)

      val protocol = deltaLog.update().protocol
      assert(protocol === protocolWithWriterFeature(TestRemovableWriterFeature))

      val logs = Log4jUsageLogger.track {
        withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
          val featureName = quoteWith match {
            case "none" => s"${TestRemovableWriterFeature.name}"
            case "single" => s"'${TestRemovableWriterFeature.name}'"
            case "backtick" => s"`${TestRemovableWriterFeature.name}`"
          }
          sql(s"ALTER TABLE $table DROP FEATURE $featureName")
        }

        assert(deltaLog.update().protocol === Protocol(1, 1))
      }
      // Test that the write downgrade command was invoked.
      val expectedOpType = "delta.test.TestWriterFeaturePreDowngradeCommand"
      val blob = logs.collectFirst {
        case r if r.metric == MetricDefinitions.EVENT_TAHOE.name &&
          r.tags.get("opType").contains(expectedOpType) => r.blob
      }
      assert(blob.nonEmpty, s"Expecting an '$expectedOpType' event but didn't see any.")
    }
  }

  protected def testProtocolVersionDowngrade(
      initialMinReaderVersion: Int,
      initialMinWriterVersion: Int,
      featuresToAdd: Seq[TableFeature],
      featuresToRemove: Seq[TableFeature],
      expectedDowngradedProtocol: Protocol): Unit = {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)

      spark.sql(s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint) USING delta
                   |TBLPROPERTIES (
                   |delta.minReaderVersion = $initialMinReaderVersion,
                   |delta.minWriterVersion = $initialMinWriterVersion
                   |)""".stripMargin)

      // Upgrade protocol to table features.
      val newTBLProperties = featuresToAdd
        .map(f => s"delta.feature.${f.name}='supported'")
        .reduce(_ + ", " + _)
      spark.sql(
        s"""ALTER TABLE delta.`${dir.getPath}`
           |SET TBLPROPERTIES (
           |$newTBLProperties
           |)""".stripMargin)

      withSQLConf(DeltaSQLConf.TABLE_FEATURE_DROP_ENABLED.key -> true.toString) {
        for (feature <- featuresToRemove) {
          AlterTableDropFeatureDeltaCommand(DeltaTableV2(spark, deltaLog.dataPath), feature.name)
            .run(spark)
        }
      }

      assert(deltaLog.update().protocol === expectedDowngradedProtocol)
    }
  }

  test("Downgrade protocol version (1, 4)") {
    testProtocolVersionDowngrade(
      initialMinReaderVersion = 1,
      initialMinWriterVersion = 4,
      featuresToAdd = Seq(TestRemovableWriterFeature),
      featuresToRemove = Seq(TestRemovableWriterFeature),
      expectedDowngradedProtocol = Protocol(1, 4))
  }

  // Initial minReader version is (2, 4), however, there are no legacy features that require
  // reader version 2. Therefore, the protocol version is downgraded to (1, 4).
  test("Downgrade protocol version (2, 4)") {
    testProtocolVersionDowngrade(
      initialMinReaderVersion = 2,
      initialMinWriterVersion = 4,
      featuresToAdd = Seq(TestRemovableWriterFeature),
      featuresToRemove = Seq(TestRemovableWriterFeature),
      expectedDowngradedProtocol = Protocol(1, 4))
  }

  // Version (2, 5) enables column mapping which is a reader+writer feature and requires (2, 5).
  // Therefore, to downgrade from table features we need at least (2, 5).
  test("Downgrade protocol version (2, 5)") {
    testProtocolVersionDowngrade(
      initialMinReaderVersion = 2,
      initialMinWriterVersion = 5,
      featuresToAdd = Seq(TestRemovableWriterFeature),
      featuresToRemove = Seq(TestRemovableWriterFeature),
      expectedDowngradedProtocol = Protocol(2, 5))
  }


  test("Downgrade protocol version (1, 1)") {
    testProtocolVersionDowngrade(
      initialMinReaderVersion = 1,
      initialMinWriterVersion = 1,
      featuresToAdd = Seq(TestRemovableWriterFeature),
      featuresToRemove = Seq(TestRemovableWriterFeature),
      expectedDowngradedProtocol = Protocol(1, 1))
  }

  test("Downgrade protocol version on table created with table features") {
    // When the table is initialized with table features there are no active (implicit) legacy
    // features. After removing the last table feature we downgrade back to (1, 1).
    testProtocolVersionDowngrade(
      initialMinReaderVersion = 3,
      initialMinWriterVersion = 7,
      featuresToAdd = Seq(TestRemovableWriterFeature),
      featuresToRemove = Seq(TestRemovableWriterFeature),
      expectedDowngradedProtocol = Protocol(1, 1))
  }

  test("Downgrade protocol version on table created with writer features") {
    testProtocolVersionDowngrade(
      initialMinReaderVersion = 1,
      initialMinWriterVersion = 7,
      featuresToAdd = Seq(TestRemovableWriterFeature),
      featuresToRemove = Seq(TestRemovableWriterFeature),
      expectedDowngradedProtocol = Protocol(1, 1))
  }

  test("Protocol version downgrade on a table with table features and added legacy feature") {
    // Added legacy feature should be removed and the protocol should be downgraded to (2, 5).
    testProtocolVersionDowngrade(
      initialMinReaderVersion = 3,
      initialMinWriterVersion = 7,
      featuresToAdd =
        Seq(TestRemovableWriterFeature) ++ Protocol(2, 5).implicitlySupportedFeatures,
      featuresToRemove = Seq(TestRemovableWriterFeature),
      expectedDowngradedProtocol = Protocol(2, 5))

    // Added legacy feature should not be removed and the protocol should stay on (1, 7).
    testProtocolVersionDowngrade(
      initialMinReaderVersion = 1,
      initialMinWriterVersion = 7,
      featuresToAdd = Seq(TestRemovableWriterFeature, TestRemovableLegacyWriterFeature),
      featuresToRemove = Seq(TestRemovableWriterFeature),
      expectedDowngradedProtocol = Protocol(1, 7)
        .withFeature(TestRemovableLegacyWriterFeature))

    // Legacy feature was manually removed. Protocol should be downgraded to (1, 1).
    testProtocolVersionDowngrade(
      initialMinReaderVersion = 1,
      initialMinWriterVersion = 7,
      featuresToAdd = Seq(TestRemovableWriterFeature, TestRemovableLegacyWriterFeature),
      featuresToRemove = Seq(TestRemovableLegacyWriterFeature, TestRemovableWriterFeature),
      expectedDowngradedProtocol = Protocol(1, 1))

    // Start with writer table features and add a legacy reader+writer feature.
    testProtocolVersionDowngrade(
      initialMinReaderVersion = 1,
      initialMinWriterVersion = 7,
      featuresToAdd = Seq(TestRemovableWriterFeature, ColumnMappingTableFeature),
      featuresToRemove = Seq(TestRemovableWriterFeature),
      expectedDowngradedProtocol = Protocol(3, 7).withFeature(ColumnMappingTableFeature))

    // Remove reader+writer legacy feature as well.
    testProtocolVersionDowngrade(
      initialMinReaderVersion = 1,
      initialMinWriterVersion = 7,
      featuresToAdd = Seq(TestRemovableLegacyReaderWriterFeature, TestRemovableWriterFeature),
      featuresToRemove = Seq(TestRemovableLegacyReaderWriterFeature, TestRemovableWriterFeature),
      expectedDowngradedProtocol = Protocol(1, 1))
  }

  test("Protocol version is not downgraded when writer features exist") {
    testProtocolVersionDowngrade(
      initialMinReaderVersion = 1,
      initialMinWriterVersion = 7,
      featuresToAdd = Seq(TestRemovableWriterFeature, DomainMetadataTableFeature),
      featuresToRemove = Seq(TestRemovableWriterFeature),
      expectedDowngradedProtocol = protocolWithWriterFeature(DomainMetadataTableFeature))
  }

  test("Protocol version is not downgraded when reader+writer features exist") {
    testProtocolVersionDowngrade(
      initialMinReaderVersion = 3,
      initialMinWriterVersion = 7,
      featuresToAdd = Seq(TestRemovableReaderWriterFeature, DeletionVectorsTableFeature),
      featuresToRemove = Seq(TestRemovableReaderWriterFeature),
      expectedDowngradedProtocol = protocolWithReaderFeature(DeletionVectorsTableFeature))
  }

  test("Protocol version is not downgraded when both reader+writer and writer features exist") {
    testProtocolVersionDowngrade(
      initialMinReaderVersion = 3,
      initialMinWriterVersion = 7,
      featuresToAdd = Seq(TestRemovableReaderWriterFeature, TestRemovableWriterFeature),
      featuresToRemove = Seq(TestRemovableReaderWriterFeature),
      expectedDowngradedProtocol =
        Protocol(3, 7, Some(Set.empty), Some(Set(TestRemovableWriterFeature.name))))

    testProtocolVersionDowngrade(
      initialMinReaderVersion = 3,
      initialMinWriterVersion = 7,
      featuresToAdd = Seq(TestRemovableReaderWriterFeature, TestRemovableWriterFeature),
      featuresToRemove = Seq(TestRemovableWriterFeature),
      expectedDowngradedProtocol = protocolWithReaderFeature(TestRemovableReaderWriterFeature))
  }

  private def assertPropertiesAndShowTblProperties(
      deltaLog: DeltaLog,
      tableHasFeatures: Boolean = false): Unit = {
    val configs = deltaLog.snapshot.metadata.configuration.map { case (k, v) =>
      k.toLowerCase(Locale.ROOT) -> v
    }
    assert(!configs.contains(Protocol.MIN_READER_VERSION_PROP))
    assert(!configs.contains(Protocol.MIN_WRITER_VERSION_PROP))
    assert(!configs.exists(_._1.startsWith(FEATURE_PROP_PREFIX)))

    val tblProperties =
      sql(s"SHOW TBLPROPERTIES delta.`${deltaLog.dataPath.toString}`").collect()

    assert(
      tblProperties.exists(row => row.getAs[String]("key") == Protocol.MIN_READER_VERSION_PROP))
    assert(
      tblProperties.exists(row => row.getAs[String]("key") == Protocol.MIN_WRITER_VERSION_PROP))

    assert(tableHasFeatures === tblProperties.exists(row =>
      row.getAs[String]("key").startsWith(FEATURE_PROP_PREFIX)))
    val rows =
      tblProperties.filter(row =>
        row.getAs[String]("key").startsWith(FEATURE_PROP_PREFIX))
    for (row <- rows) {
      val name = row.getAs[String]("key").substring(FEATURE_PROP_PREFIX.length)
      val status = row.getAs[String]("value")
      assert(TableFeature.featureNameToFeature(name).isDefined)
      assert(status == FEATURE_PROP_SUPPORTED)
    }
  }

  private def captureProtocolChangeEventBlob(f: => Unit): Map[String, Any] = {
    val logs = Log4jUsageLogger.track(f)
    val blob = logs.collectFirst {
      case r if r.metric == MetricDefinitions.EVENT_TAHOE.name &&
        r.tags.get("opType").contains("delta.protocol.change") => r.blob
    }
    require(blob.nonEmpty, "Expecting a delta.protocol.change event but didn't see any.")
    blob.map(JsonUtils.fromJson[Map[String, Any]]).head
  }
}

class DeltaProtocolVersionSuite extends DeltaProtocolVersionSuiteBase
