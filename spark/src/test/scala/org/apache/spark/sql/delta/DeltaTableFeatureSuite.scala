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

import scala.collection.mutable

import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils._
import org.apache.spark.sql.delta.coordinatedcommits.{CommitCoordinatorProvider, InMemoryCommitCoordinatorBuilder}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames.unsafeDeltaFile

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DeltaTableFeatureSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  private lazy val testTableSchema = spark.range(1).schema

  // This is solely a test hook. Users cannot create new Delta tables with protocol lower than
  // that of their current version.
  protected def createTableWithProtocol(
      protocol: Protocol,
      path: File,
      schema: StructType = testTableSchema): DeltaLog = {
    val log = DeltaLog.forTable(spark, path)
    log.createLogDirectoriesIfNotExists()
    log.store.write(
      unsafeDeltaFile(log.logPath, 0),
      Iterator(Metadata(schemaString = schema.json).json, protocol.json),
      overwrite = false,
      log.newDeltaHadoopConf())
    log.update()
    log
  }

  test("all defined table features are registered") {
    import scala.reflect.runtime.{universe => ru}

    val subClassNames = mutable.Set[String]()
    def collect(clazz: ru.Symbol): Unit = {
      val collected = clazz.asClass.knownDirectSubclasses
      // add only table feature objects to the result set
      subClassNames ++= collected.filter(_.isModuleClass).map(_.name.toString)
      collected.filter(_.isAbstract).foreach(collect)
    }
    collect(ru.typeOf[TableFeature].typeSymbol)

    val registeredFeatures = TableFeature.allSupportedFeaturesMap.values
      .map(_.getClass.getSimpleName.stripSuffix("$")) // remove '$' from object names
      .toSet
    val notRegisteredFeatures = subClassNames.diff(registeredFeatures)

    assert(
      notRegisteredFeatures.isEmpty,
      "Expecting all defined table features are registered (either as prod or testing-only) " +
        s"but the followings are not: $notRegisteredFeatures")
  }

  test("adding feature requires supported protocol version") {
    assert(
      intercept[DeltaTableFeatureException] {
        Protocol(1, TABLE_FEATURES_MIN_WRITER_VERSION)
          .withFeature(TestLegacyReaderWriterFeature)
      }.getMessage.contains("Unable to enable table feature testLegacyReaderWriter because it " +
        "requires a higher reader protocol version"))

    assert(intercept[DeltaTableFeatureException] {
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, 6)
    }.getMessage.contains("Unable to upgrade only the reader protocol version"))

    assert(
      Protocol(2, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(AppendOnlyTableFeature)
        .readerAndWriterFeatureNames === Set(AppendOnlyTableFeature.name))

    assert(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestReaderWriterFeature)
        .readerAndWriterFeatureNames === Set(TestReaderWriterFeature.name))
  }

  test("adding feature automatically adds all dependencies") {
    assert(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestFeatureWithDependency)
        .readerAndWriterFeatureNames ===
        Set(TestFeatureWithDependency.name, TestReaderWriterFeature.name))

    assert(
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeature(TestFeatureWithTransitiveDependency)
        .readerAndWriterFeatureNames ===
        Set(
          TestFeatureWithTransitiveDependency.name,
          TestFeatureWithDependency.name,
          TestReaderWriterFeature.name))

    // Validate new protocol has required features enabled when a writer feature requires a
    // reader/write feature.
    val metadata = Metadata(
      configuration = Map(
        TableFeatureProtocolUtils.propertyKey(TestWriterFeatureWithTransitiveDependency) ->
          TableFeatureProtocolUtils.FEATURE_PROP_SUPPORTED))
    assert(
      Protocol
        .forNewTable(
          spark,
          Some(metadata))
        .readerAndWriterFeatureNames ===
        Set(
          AppendOnlyTableFeature.name,
          InvariantsTableFeature.name,
          TestWriterFeatureWithTransitiveDependency.name,
          TestFeatureWithDependency.name,
          TestReaderWriterFeature.name))
  }

  test("implicitly-enabled features") {
    assert(
      Protocol(2, 6).implicitlySupportedFeatures === Set(
        AppendOnlyTableFeature,
        ColumnMappingTableFeature,
        InvariantsTableFeature,
        CheckConstraintsTableFeature,
        ChangeDataFeedTableFeature,
        GeneratedColumnsTableFeature,
        IdentityColumnsTableFeature,
        TestLegacyWriterFeature,
        TestLegacyReaderWriterFeature,
        TestRemovableLegacyWriterFeature,
        TestRemovableLegacyReaderWriterFeature))
    assert(
      Protocol(2, 5).implicitlySupportedFeatures === Set(
        AppendOnlyTableFeature,
        ColumnMappingTableFeature,
        InvariantsTableFeature,
        CheckConstraintsTableFeature,
        ChangeDataFeedTableFeature,
        GeneratedColumnsTableFeature,
        TestLegacyWriterFeature,
        TestLegacyReaderWriterFeature,
        TestRemovableLegacyWriterFeature,
        TestRemovableLegacyReaderWriterFeature))
    assert(Protocol(2, TABLE_FEATURES_MIN_WRITER_VERSION).implicitlySupportedFeatures === Set())
    assert(
      Protocol(
        TABLE_FEATURES_MIN_READER_VERSION,
        TABLE_FEATURES_MIN_WRITER_VERSION).implicitlySupportedFeatures === Set())
  }

  test("implicit feature listing") {
    assert(
      intercept[DeltaTableFeatureException] {
        Protocol(1, 4).withFeature(TestLegacyReaderWriterFeature)
      }.getMessage.contains(
        "Unable to enable table feature testLegacyReaderWriter because it requires a higher " +
          "reader protocol version (current 1)"))

    assert(
      intercept[DeltaTableFeatureException] {
        Protocol(2, 4).withFeature(TestLegacyReaderWriterFeature)
      }.getMessage.contains(
        "Unable to enable table feature testLegacyReaderWriter because it requires a higher " +
          "writer protocol version (current 4)"))

    assert(
      intercept[DeltaTableFeatureException] {
        Protocol(1, TABLE_FEATURES_MIN_WRITER_VERSION).withFeature(TestLegacyReaderWriterFeature)
      }.getMessage.contains(
        "Unable to enable table feature testLegacyReaderWriter because it requires a higher " +
          "reader protocol version (current 1)"))

    val protocol =
      Protocol(2, TABLE_FEATURES_MIN_WRITER_VERSION).withFeature(TestLegacyReaderWriterFeature)
    assert(!protocol.readerFeatures.isDefined)
    assert(
      protocol.writerFeatures.get === Set(TestLegacyReaderWriterFeature.name))
  }

  test("merge protocols") {
    val tfProtocol1 = Protocol(1, TABLE_FEATURES_MIN_WRITER_VERSION)
    val tfProtocol2 =
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)

    assert(tfProtocol1.merge(Protocol(1, 2)) === Protocol(1, 2))
    assert(tfProtocol2.merge(Protocol(2, 6)) === Protocol(2, 6))
  }

  test("protocol upgrade compatibility") {
    assert(Protocol(1, 1).canUpgradeTo(Protocol(1, 1)))
    assert(Protocol(1, 1).canUpgradeTo(Protocol(2, 1)))
    assert(
      Protocol(1, 1).canUpgradeTo(
        Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)))
    assert(
      !Protocol(2, 3).canUpgradeTo(
        Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)))
    assert(
      !Protocol(2, 6).canUpgradeTo(
        Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
          .withFeatures(
            Seq(
              // With one feature not referenced, `canUpgradeTo` must be `false`.
              // AppendOnlyTableFeature,
              InvariantsTableFeature,
              CheckConstraintsTableFeature,
              ChangeDataFeedTableFeature,
              GeneratedColumnsTableFeature,
              IdentityColumnsTableFeature,
              ColumnMappingTableFeature,
              TestLegacyWriterFeature,
              TestLegacyReaderWriterFeature,
              TestRemovableLegacyWriterFeature,
              TestRemovableLegacyReaderWriterFeature))))
    assert(
      Protocol(2, 6).canUpgradeTo(
        Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
          .withFeatures(Seq(
            AppendOnlyTableFeature,
            InvariantsTableFeature,
            CheckConstraintsTableFeature,
            ChangeDataFeedTableFeature,
            GeneratedColumnsTableFeature,
            IdentityColumnsTableFeature,
            ColumnMappingTableFeature,
            TestLegacyWriterFeature,
            TestLegacyReaderWriterFeature,
            TestRemovableLegacyWriterFeature,
            TestRemovableLegacyReaderWriterFeature))))
  }

  test("protocol downgrade compatibility") {
    val tableFeatureProtocol =
      Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
    assert(Protocol(1, 7).withFeature(TestWriterFeature)
      .canDowngradeTo(Protocol(1, 7), droppedFeatureName = TestWriterFeature.name))
    // When there are no explicit features the protocol versions need to be downgraded
    // below table features. The new protocol versions need to match exactly the supported
    // legacy features.
    for (n <- 1 to 3) {
      assert(
        !Protocol(n, 7)
          .withFeatures(Seq(TestWriterFeature, AppendOnlyTableFeature))
          .canDowngradeTo(Protocol(1, 2), droppedFeatureName = TestWriterFeature.name))
      assert(
        Protocol(n, 7)
          .withFeatures(Seq(TestWriterFeature, AppendOnlyTableFeature, InvariantsTableFeature))
          .canDowngradeTo(Protocol(1, 2), droppedFeatureName = TestWriterFeature.name))
    }
    assert(tableFeatureProtocol.withFeatures(Seq(TestReaderWriterFeature))
      .canDowngradeTo(Protocol(1, 1), droppedFeatureName = TestReaderWriterFeature.name))
    assert(
      tableFeatureProtocol
        .withFeatures(Seq(TestReaderWriterFeature, TestRemovableLegacyReaderWriterFeature))
        .merge(Protocol(2, 5))
        .canDowngradeTo(Protocol(2, 5), droppedFeatureName = TestReaderWriterFeature.name))
    // Downgraded protocol must be able to support all legacy table features.
    assert(
      !tableFeatureProtocol
        .withFeatures(Seq(TestWriterFeature, AppendOnlyTableFeature, ColumnMappingTableFeature))
        .canDowngradeTo(Protocol(2, 4), droppedFeatureName = TestWriterFeature.name))
    assert(
      tableFeatureProtocol
        .withFeatures(Seq(TestWriterFeature, AppendOnlyTableFeature, ColumnMappingTableFeature))
        .merge(Protocol(2, 5))
        .canDowngradeTo(Protocol(2, 5), droppedFeatureName = TestWriterFeature.name))
  }

  test("add reader and writer feature descriptors") {
    var p = Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
    val name = AppendOnlyTableFeature.name
    p = p.withReaderFeatures(Seq(name))
    assert(p.readerFeatures === Some(Set(name)))
    assert(p.writerFeatures === Some(Set.empty))
    p = p.withWriterFeatures(Seq(name))
    assert(p.readerFeatures === Some(Set(name)))
    assert(p.writerFeatures === Some(Set(name)))
  }

  test("native automatically-enabled feature can't be implicitly enabled") {
    val p = Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
    assert(p.implicitlySupportedFeatures.isEmpty)
  }

  test("Table features are not automatically enabled by default table property settings") {
    withTable("tbl") {
      spark.range(10).write.format("delta").saveAsTable("tbl")
      val snapshot = DeltaLog.forTable(spark, TableIdentifier("tbl")).update()
      TableFeature.allSupportedFeaturesMap.values.foreach {
        case feature: FeatureAutomaticallyEnabledByMetadata =>
          assert(
            !feature.metadataRequiresFeatureToBeEnabled(
              snapshot.protocol, snapshot.metadata, spark),
            s"""
               |${feature.name} is automatically enabled by the default metadata. This will lead to
               |the inability of reading existing tables that do not have the feature enabled and
               |should not reach production! If this is only for testing purposes, ignore this test.
               """.stripMargin)
        case _ =>
      }
    }
  }

  test("Can enable legacy metadata table feature by setting default table property key") {
    withSQLConf(
      s"$DEFAULT_FEATURE_PROP_PREFIX${TestWriterFeature.name}" -> "enabled",
      DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name") {
      withTable("tbl") {
        spark.range(10).write.format("delta").saveAsTable("tbl")
        val log = DeltaLog.forTable(spark, TableIdentifier("tbl"))
        val protocol = log.update().protocol
        assert(protocol.readerAndWriterFeatureNames === Set(
          AppendOnlyTableFeature.name,
          InvariantsTableFeature.name,
          ColumnMappingTableFeature.name,
          TestWriterFeature.name))
      }
    }
  }

  test("CLONE does not take into account default table features") {
    withTable("tbl") {
      spark.range(0).write.format("delta").saveAsTable("tbl")
      val log = DeltaLog.forTable(spark, TableIdentifier("tbl"))
      val protocolBefore = log.update().protocol
      withSQLConf(defaultPropertyKey(TestWriterFeature) -> "enabled") {
        sql(buildTablePropertyModifyingCommand(
          commandName = "CLONE", targetTableName = "tbl", sourceTableName = "tbl")
        )
      }
      val protocolAfter = log.update().protocol
      assert(protocolBefore === protocolAfter)
    }
  }

  test("CLONE only enables enabled metadata table features") {
    withTable("src", "target") {
      withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key ->
          TABLE_FEATURES_MIN_WRITER_VERSION.toString,
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key ->
          TABLE_FEATURES_MIN_READER_VERSION.toString,
        DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name") {
        spark.range(0).write.format("delta").saveAsTable("src")
      }
      sql(buildTablePropertyModifyingCommand(
        commandName = "CLONE", targetTableName = "target", sourceTableName = "src"))
      val targetLog = DeltaLog.forTable(spark, TableIdentifier("target"))
      val protocol = targetLog.update().protocol
      assert(protocol.readerAndWriterFeatureNames === Set(
        ColumnMappingTableFeature.name))
    }
  }

  for(commandName <- Seq("ALTER", "REPLACE", "CREATE OR REPLACE", "CLONE")) {
    test(s"Can enable legacy metadata table feature during $commandName TABLE") {
      withSQLConf(
        s"${defaultPropertyKey(TestWriterFeature)}" -> "enabled") {
        withTable("tbl") {
          spark.range(0).write.format("delta").saveAsTable("tbl")
          val log = DeltaLog.forTable(spark, TableIdentifier("tbl"))

          val tblProperties = Seq("'delta.enableChangeDataFeed' = true")
          sql(buildTablePropertyModifyingCommand(
            commandName, targetTableName = "tbl", sourceTableName = "tbl", tblProperties))
          val protocol = log.update().protocol
          assert(protocol.readerAndWriterFeatureNames === Set(
            AppendOnlyTableFeature.name,
            InvariantsTableFeature.name,
            ChangeDataFeedTableFeature.name,
            TestWriterFeature.name))
        }
      }
    }
  }

  for(commandName <- Seq("ALTER", "CLONE", "REPLACE", "CREATE OR REPLACE")) {
    test("Enabling table feature on already existing table enables all table features " +
      s"up to the table's protocol version during $commandName TABLE") {
      withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name") {
        withTable("tbl") {
          spark.range(0).write.format("delta").saveAsTable("tbl")
          val log = DeltaLog.forTable(spark, TableIdentifier("tbl"))
          assert(log.update().protocol === Protocol(2, 7).withFeatures(Seq(
            AppendOnlyTableFeature,
            InvariantsTableFeature,
            ColumnMappingTableFeature)))
          val tblProperties = Seq(s"'$FEATURE_PROP_PREFIX${TestWriterFeature.name}' = 'enabled'",
            s"'delta.minWriterVersion' = $TABLE_FEATURES_MIN_WRITER_VERSION")
          sql(buildTablePropertyModifyingCommand(
            commandName, targetTableName = "tbl", sourceTableName = "tbl", tblProperties))
          val newProtocol = log.update().protocol
          assert(newProtocol.readerAndWriterFeatureNames === Set(
            AppendOnlyTableFeature.name,
            InvariantsTableFeature.name,
            ColumnMappingTableFeature.name,
            TestWriterFeature.name))
        }
      }
    }
  }

  for(commandName <- Seq("ALTER", "CLONE", "REPLACE", "CREATE OR REPLACE")) {
    test(s"Vacuum Protocol Check is disabled by default but can be enabled during $commandName") {
      val table = "tbl"
      withTable(table) {
        spark.range(0).write.format("delta").saveAsTable(table)
        val log = DeltaLog.forTable(spark, TableIdentifier(table))
        val protocol = log.update().protocol
        assert(!protocol.readerAndWriterFeatureNames.contains(VacuumProtocolCheckTableFeature.name))

        val tblProperties1 = Seq(s"'delta.minWriterVersion' = $TABLE_FEATURES_MIN_WRITER_VERSION")
        sql(buildTablePropertyModifyingCommand(
          commandName, targetTableName = table, sourceTableName = table, tblProperties1))
        val newProtocol1 = log.update().protocol
        assert(!newProtocol1.readerAndWriterFeatureNames.contains(
          VacuumProtocolCheckTableFeature.name))

        val tblProperties2 = Seq(s"'$FEATURE_PROP_PREFIX${VacuumProtocolCheckTableFeature.name}' " +
          s"= 'supported', 'delta.minWriterVersion' = $TABLE_FEATURES_MIN_WRITER_VERSION")
        sql(buildTablePropertyModifyingCommand(
          commandName, targetTableName = table, sourceTableName = table, tblProperties2))
        val newProtocol2 = log.update().protocol
        assert(newProtocol2.readerAndWriterFeatureNames.contains(
          VacuumProtocolCheckTableFeature.name))
      }
    }
  }

  test("drop table feature works with coordinated commits") {
    val table = "tbl"
    withTable(table) {
      spark.range(0).write.format("delta").saveAsTable(table)
      val log = DeltaLog.forTable(spark, TableIdentifier(table))
      val featureName = TestRemovableReaderWriterFeature.name
      assert(!log.update().protocol.readerAndWriterFeatureNames.contains(featureName))

      // Add coordinated commits table feature to the table
      CommitCoordinatorProvider.registerBuilder(InMemoryCommitCoordinatorBuilder(batchSize = 100))
      val tblProperties1 =
        Seq(s"'${DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key}' = 'in-memory'",
          s"'${DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.key}' = '{}'")
      sql(buildTablePropertyModifyingCommand(
        "ALTER", targetTableName = table, sourceTableName = table, tblProperties1))

      // Add TestRemovableReaderWriterFeature to the table in unbackfilled delta files
      val tblProperties2 = Seq(s"'$FEATURE_PROP_PREFIX$featureName' = 'supported', " +
        s"'delta.minWriterVersion' = $TABLE_FEATURES_MIN_WRITER_VERSION, " +
        s"'${TestRemovableReaderWriterFeature.TABLE_PROP_KEY}' = 'true'")
      sql(buildTablePropertyModifyingCommand(
        "ALTER", targetTableName = table, sourceTableName = table, tblProperties2))
      assert(log.update().protocol.readerAndWriterFeatureNames.contains(featureName))

      // Disable feature on the latest snapshot
      val tblProperties3 = Seq(s"'${TestRemovableReaderWriterFeature.TABLE_PROP_KEY}' = 'false'")
      sql(buildTablePropertyModifyingCommand(
        "ALTER", targetTableName = table, sourceTableName = table, tblProperties3))

      val tableFeature =
        TableFeature.featureNameToFeature(featureName).get.asInstanceOf[RemovableFeature]
      assert(tableFeature.historyContainsFeature(spark, log.update()))

      // Dropping feature should fail because the feature still has traces in deltas.
      val e = intercept[DeltaTableFeatureException] {
        sql(s"ALTER TABLE $table DROP FEATURE $featureName")
      }
      assert(e.getMessage.contains("DELTA_FEATURE_DROP_HISTORICAL_VERSIONS_EXIST"), e)

      // Add in a checkpoint and cleanUp up older logs containing feature traces
      log.startTransaction().commitManually()
      log.checkpoint()
      log.cleanUpExpiredLogs(log.update(), deltaRetentionMillisOpt = Some(-1000000000000L))
      sql(s"ALTER TABLE $table DROP FEATURE $featureName")
      assert(!log.update().protocol.readerAndWriterFeatureNames.contains(featureName))
    }
  }

  private def buildTablePropertyModifyingCommand(
      commandName: String,
      targetTableName: String,
      sourceTableName: String,
      tblProperties: Seq[String] = Seq.empty): String = {
    val commandStr = if (commandName == "CLONE") {
      "CREATE OR REPLACE"
    } else {
      commandName
    }

    val cloneClause = if (commandName == "CLONE") {
      s"SHALLOW CLONE $sourceTableName"
    } else {
      ""
    }

    val (usingDeltaClause, dataSourceClause) = if ("ALTER" != commandName &&
      "CLONE" != commandName) {
      ("USING DELTA", s"AS SELECT * FROM $sourceTableName")
    } else {
      ("", "")
    }
    var tblPropertiesClause = ""
    if (tblProperties.nonEmpty) {
      if (commandName == "ALTER") {
        tblPropertiesClause += "SET "
      }
      tblPropertiesClause += s"TBLPROPERTIES ${tblProperties.mkString("(", ",", ")")}"
    }
    s"""$commandStr TABLE $targetTableName
       |$usingDeltaClause
       |$cloneClause
       |$tblPropertiesClause
       |$dataSourceClause
       |""".stripMargin
  }
}
