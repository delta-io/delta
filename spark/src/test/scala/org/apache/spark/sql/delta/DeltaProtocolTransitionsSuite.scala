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

package org.apache.spark.sql.delta

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

class DeltaProtocolTransitionsSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  /*
  override def beforeAll(): Unit = {
    super.beforeAll()
    // spark.conf.set(DeltaSQLConf.TABLE_FEATURES_TEST_FEATURES_ENABLED.key, "false")
    SparkSession.setActiveSession(spark)
  }
  */

  protected def protocolToTBLProperties(
      protocol: Protocol,
      skipVersions: Boolean = false): Seq[String] = {
    val versionProperties = if (skipVersions && protocol.readerAndWriterFeatures.nonEmpty) {
      Nil
    } else {
      Seq(s"""
           |delta.minReaderVersion = ${protocol.minReaderVersion},
           |delta.minWriterVersion = ${protocol.minWriterVersion}""".stripMargin)
    }
    val featureProperties =
      protocol.readerAndWriterFeatureNames.map("delta.feature." + _ + " = 'Supported'")
    versionProperties ++ featureProperties
  }

  protected def testProtocolTransition(
     createTableProtocol: Option[Protocol] = None,
     alterTableProtocol: Option[Protocol] = None,
     dropFeatures: Seq[TableFeature] = Nil,
     expectedProtocol: Protocol): Unit = {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)

      val createTableSQLbase = s"CREATE TABLE delta.`${deltaLog.dataPath}` (id bigint) USING delta"
      val createTableSQL = createTableProtocol.map { p =>
        s"""$createTableSQLbase TBLPROPERTIES (
           |${protocolToTBLProperties(p).mkString(",")}
           |)""".stripMargin
      }.getOrElse(createTableSQLbase)

      // Create table.
      sql(createTableSQL)

      alterTableProtocol.map { p =>
        sql(s"""ALTER TABLE delta.`${deltaLog.dataPath}` SET TBLPROPERTIES (
           |${protocolToTBLProperties(p, skipVersions = true).mkString(",")}
           |)""".stripMargin)
      }

      // Drop features.
      dropFeatures.foreach { f =>
        sql(s"ALTER TABLE delta.`${deltaLog.dataPath}` DROP FEATURE ${f.name}")
      }

      assert(deltaLog.update().protocol === expectedProtocol)
    }
  }

  test("TEST") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)

      /*
      withSQLConf(
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> 1.toString,
          DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> 1.toString) {
        sql(s"CREATE TABLE delta.`${deltaLog.dataPath}` USING DELTA " +
          "TBLPROPERTIES ('delta.feature.checkConstraints' = 'supported')")

        assert(deltaLog.update().protocol === Protocol(1, 3))
      }
      */

      /*
      withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> 1.toString,
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> 2.toString) {
        sql(s"CREATE TABLE delta.`${deltaLog.dataPath}` USING DELTA " +
          "TBLPROPERTIES ('delta.feature.columnMapping' = 'supported')")

        assert(deltaLog.update().protocol === Protocol(1, 3))
      }
      */

      withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> 1.toString,
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> 1.toString) {
        sql(s"CREATE TABLE delta.`${deltaLog.dataPath}` USING DELTA")

        /*
        sql(s"ALTER TABLE delta.`${deltaLog.dataPath}` " +
          s"SET TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name')")
        */

        sql(s"ALTER TABLE delta.`${deltaLog.dataPath}` " +
          s"ADD CONSTRAINT c1 CHECK (4 > 0)")

        assert(deltaLog.update().protocol === Protocol(1, 3))
      }
    }
  }

  test("CREATE TABLE normalization") {
    testProtocolTransition(
      createTableProtocol = Some(Protocol(3, 7).withFeature(TestRemovableWriterFeature)),
      expectedProtocol = Protocol(1, 7).withFeature(TestRemovableWriterFeature))

    testProtocolTransition(
      createTableProtocol = Some(
        Protocol(3, 7).withFeatures(Seq(TestRemovableWriterFeature, ColumnMappingTableFeature))),
      expectedProtocol =
        Protocol(2, 7).withFeatures(Seq(TestRemovableWriterFeature, ColumnMappingTableFeature)))

    /*
    testProtocolTransition(
      createTableProtocol = Some(Protocol(
        minReaderVersion = 1,
        minWriterVersion = 7,
        readerFeatures = Some(Set(TestRemovableReaderWriterFeature.name)),
        writerFeatures = Some(Set(TestRemovableReaderWriterFeature.name)))),
      expectedProtocol = Protocol(3, 7).withFeature(TestRemovableReaderWriterFeature))

    testProtocolTransition(
      createTableProtocol = Some(Protocol(2, 7).withFeature(TestRemovableReaderWriterFeature)),
      expectedProtocol = Protocol(3, 7).withFeature(TestRemovableReaderWriterFeature))
    */
  }

  test("CREATE TABLE default protocol versions") {
    testProtocolTransition(
      createTableProtocol = Some(Protocol(1, 1)),
      expectedProtocol = Protocol(1, 1))

    testProtocolTransition(
      expectedProtocol = Protocol(1, 2))
  }

  // , (1, 5)
  for ((readerVersion, writerVersion) <- Seq((2, 1), (2, 2), (2, 3), (2, 4)))
  test("Invalid legacy protocol normalization" +
    s" - invalidProtocol($readerVersion, $writerVersion)") {

    val expectedReaderVersion = 1
    val expectedWriterVersion = Math.min(writerVersion, 4)

    testProtocolTransition(
      createTableProtocol = Some(Protocol(readerVersion, writerVersion)),
      expectedProtocol = Protocol(expectedReaderVersion, expectedWriterVersion))

    withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> readerVersion.toString,
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> writerVersion.toString) {
      testProtocolTransition(
        expectedProtocol = Protocol(expectedReaderVersion, expectedWriterVersion))

     testProtocolTransition(
       createTableProtocol = Some(Protocol(1, 1)),
       alterTableProtocol = Some(Protocol(readerVersion, writerVersion)),
       expectedProtocol = Protocol(expectedReaderVersion, expectedWriterVersion))
    }
  }

  test("ADD FEATURE normalization") {
    testProtocolTransition(
      createTableProtocol = Some(Protocol(1, 1)),
      alterTableProtocol = Some(Protocol(1, 4)),
      expectedProtocol = Protocol(1, 4))

    testProtocolTransition(
      createTableProtocol = Some(Protocol(1, 2)),
      alterTableProtocol = Some(Protocol(1, 4)),
      expectedProtocol = Protocol(1, 4))

    testProtocolTransition(
      createTableProtocol = Some(Protocol(1, 4)),
      alterTableProtocol = Some(Protocol(1, 2)),
      expectedProtocol = Protocol(1, 4))

    testProtocolTransition(
      createTableProtocol = Some(Protocol(1, 4)),
      alterTableProtocol = Some(Protocol(1, 4)),
      expectedProtocol = Protocol(1, 4))

    testProtocolTransition(
      createTableProtocol = Some(Protocol(1, 6)),
      alterTableProtocol = Some(Protocol(2, 5)),
      expectedProtocol = Protocol(2, 6))

    testProtocolTransition(
      createTableProtocol = Some(Protocol(2, 5)),
      alterTableProtocol = Some(Protocol(1, 6)),
      expectedProtocol = Protocol(2, 6))

    /*
    testProtocolTransition(
      createTableProtocol = Some(Protocol(1, 4)),
      alterTableProtocol = Some(Protocol(2, 7).withFeature(ColumnMappingTableFeature)),
      expectedProtocol = Protocol(2, 5))
    */

    testProtocolTransition(
      createTableProtocol = Some(Protocol(1, 7).withFeature(TestWriterFeature)),
      alterTableProtocol = Some(Protocol(1, 2)),
      expectedProtocol = Protocol(1, 7).withFeatures(
        Seq(TestWriterFeature, AppendOnlyTableFeature, InvariantsTableFeature)))

    testProtocolTransition(
      createTableProtocol =
        Some(Protocol(1, 7).withFeatures(Seq(TestWriterFeature, IdentityColumnsTableFeature))),
      alterTableProtocol = Some(Protocol(1, 2)),
      expectedProtocol = Protocol(1, 7).withFeatures(
        Seq(TestWriterFeature,
          AppendOnlyTableFeature,
          InvariantsTableFeature,
          IdentityColumnsTableFeature)))

    testProtocolTransition(
      createTableProtocol = Some(Protocol(1, 7).withFeature(InvariantsTableFeature)),
      alterTableProtocol = Some(Protocol(1, 2)),
      expectedProtocol = Protocol(1, 2))

    testProtocolTransition(
      createTableProtocol = Some(Protocol(1, 2)),
      alterTableProtocol = Some(Protocol(1, 7).withFeature(CheckConstraintsTableFeature)),
      expectedProtocol = Protocol(1, 3))
  }

  test("DROP FEATURE normalization") {
    testProtocolTransition(
      createTableProtocol = Some(Protocol(1, 3)),
      dropFeatures = Seq(CheckConstraintsTableFeature),
      expectedProtocol = Protocol(1, 2))

    /*
    testProtocolTransition(
      createTableProtocol = Some(Protocol(2, 5)),
      dropFeatures = Seq(ColumnMappingTableFeature),
      expectedProtocol = Protocol(1, 4))

    testProtocolTransition(
      createTableProtocol = Some(Protocol(2, 6)),
      dropFeatures = Seq(ColumnMappingTableFeature),
      expectedProtocol = Protocol(1, 6))
     */

    testProtocolTransition(
      createTableProtocol = Some(Protocol(1, 4)),
      dropFeatures = Seq(CheckConstraintsTableFeature),
      expectedProtocol = Protocol(1, 7).withFeatures(Seq(
        AppendOnlyTableFeature,
        InvariantsTableFeature,
        GeneratedColumnsTableFeature,
        ChangeDataFeedTableFeature)))

    testProtocolTransition(
      createTableProtocol = Some(Protocol(1, 7).withFeatures(
        Seq(AppendOnlyTableFeature, InvariantsTableFeature, TestRemovableWriterFeature))),
      dropFeatures = Seq(TestRemovableWriterFeature),
      expectedProtocol = Protocol(1, 2))

    testProtocolTransition(
      createTableProtocol = Some(Protocol(3, 7).withFeatures(Seq(
        InvariantsTableFeature,
        ColumnMappingTableFeature,
        TestRemovableWriterFeature,
        TestRemovableReaderWriterFeature))),
      dropFeatures = Seq(TestRemovableReaderWriterFeature),
      expectedProtocol = Protocol(2, 7).withFeatures(Seq(
        InvariantsTableFeature,
        ColumnMappingTableFeature,
        TestRemovableWriterFeature)))

    testProtocolTransition(
      createTableProtocol = Some(Protocol(3, 7).withFeatures(Seq(
        InvariantsTableFeature,
        TestRemovableWriterFeature,
        TestRemovableReaderWriterFeature))),
      dropFeatures = Seq(TestRemovableReaderWriterFeature),
      expectedProtocol = Protocol(1, 7).withFeatures(Seq(
        InvariantsTableFeature,
        TestRemovableWriterFeature)))
  }

  protected def testLegacyProtocolTransition(
      createTableColumns: Seq[(String, String)] = Seq.empty,
      createTableGeneratedColumns: Seq[(String, String, String)] = Seq.empty,
      createTableProperties: Seq[(String, String)] = Seq.empty,
      alterTableProperties: Seq[(String, String)] = Seq.empty,
      expectedProtocol: Protocol): Unit = {

    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir)

      val tableBuilder = io.delta.tables.DeltaTable.create(spark)
      tableBuilder.tableName(s"delta.`$dir`")

      createTableColumns.foreach { c =>
        tableBuilder.addColumn(c._1, c._2)
      }

      createTableGeneratedColumns.foreach { c =>
        val columnBuilder = io.delta.tables.DeltaTable.columnBuilder(spark, c._1)
        columnBuilder.dataType(c._2)
        columnBuilder.generatedAlwaysAs(c._3)
        tableBuilder.addColumn(columnBuilder.build())
      }

      createTableProperties.foreach { p =>
        tableBuilder.property(p._1, p._2)
      }

      tableBuilder.location(dir.getCanonicalPath)
      tableBuilder.execute()

      if (alterTableProperties.nonEmpty) {
        sql(
          s"""ALTER TABLE delta.`${deltaLog.dataPath}`
             |SET TBLPROPERTIES (
             |${alterTableProperties.map(p => s"'${p._1}' = '${p._2}'").mkString(",")}
             |)""".stripMargin)
      }

      assert(deltaLog.update().protocol === expectedProtocol)
    }
  }

  test("Default Enabled native features") {
    withSQLConf(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true") {
      testProtocolTransition(
        createTableProtocol = Some(Protocol(1, 4)),
        expectedProtocol = Protocol(3, 7).withFeatures(Seq(
          DeletionVectorsTableFeature,
          InvariantsTableFeature,
          AppendOnlyTableFeature,
          CheckConstraintsTableFeature,
          ChangeDataFeedTableFeature,
          GeneratedColumnsTableFeature)))

      testProtocolTransition(
        expectedProtocol = Protocol(3, 7).withFeatures(Seq(
          DeletionVectorsTableFeature,
          InvariantsTableFeature,
          AppendOnlyTableFeature)))
    }

    withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> 1.toString,
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> 7.toString,
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true") {
      testProtocolTransition(
        expectedProtocol = Protocol(3, 7).withFeature(DeletionVectorsTableFeature))
    }
  }

  test("Default Enabled legacy features") {
    testLegacyProtocolTransition(
      createTableProperties = Seq((DeltaConfigs.CHANGE_DATA_FEED.key, true.toString)),
      expectedProtocol = Protocol(1, 4))

    testLegacyProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 3.toString),
        (DeltaConfigs.CHANGE_DATA_FEED.key, true.toString)),
      expectedProtocol = Protocol(1, 4))

    withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      testLegacyProtocolTransition(expectedProtocol = Protocol(1, 4))
    }

    testLegacyProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 7.toString),
        (DeltaConfigs.CHANGE_DATA_FEED.key, true.toString)),
      expectedProtocol = Protocol(1, 7).withFeature(ChangeDataFeedTableFeature))

    withSQLConf(
      DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> 1.toString,
      DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> 7.toString,
      DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      testLegacyProtocolTransition(
        expectedProtocol = Protocol(1, 7).withFeature(ChangeDataFeedTableFeature))
    }

    withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> 1.toString,
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> 7.toString) {
      testLegacyProtocolTransition(
        createTableProperties = Seq((DeltaConfigs.CHANGE_DATA_FEED.key, true.toString)),
        expectedProtocol = Protocol(1, 7).withFeature(ChangeDataFeedTableFeature))
    }

    withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      testLegacyProtocolTransition(
        createTableProperties = Seq(
          ("delta.minReaderVersion", 1.toString),
          ("delta.minWriterVersion", 7.toString)),
        expectedProtocol = Protocol(1, 7).withFeature(ChangeDataFeedTableFeature))
    }
  }

  test("Enabling legacy features on a table") {
    testLegacyProtocolTransition(
      createTableColumns = Seq(("id", "INT")),
      createTableGeneratedColumns = Seq(("id2", "INT", "id + 1")),
      expectedProtocol = Protocol(1, 4))

    testLegacyProtocolTransition(
      createTableColumns = Seq(("id", "INT")),
      createTableGeneratedColumns = Seq(("id2", "INT", "id + 1")),
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 7.toString)),
      expectedProtocol = Protocol(1, 7).withFeature(GeneratedColumnsTableFeature))

    withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> 1.toString,
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> 7.toString) {
      testLegacyProtocolTransition(
        createTableColumns = Seq(("id", "INT")),
        createTableGeneratedColumns = Seq(("id2", "INT", "id + 1")),
        expectedProtocol = Protocol(1, 7).withFeature(GeneratedColumnsTableFeature))
    }

    testLegacyProtocolTransition(
      alterTableProperties = Seq((DeltaConfigs.CHANGE_DATA_FEED.key, "true")),
      expectedProtocol = Protocol(1, 4))

    testLegacyProtocolTransition(
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 7.toString),
        (DeltaConfigs.CHANGE_DATA_FEED.key, true.toString)),
      expectedProtocol = Protocol(1, 7).withFeatures(Seq(
        InvariantsTableFeature,
        AppendOnlyTableFeature,
        ChangeDataFeedTableFeature)))
  }

  test("Column Mapping does not require a manual protocol versions upgrade") {
    testLegacyProtocolTransition(
      createTableProperties = Seq((DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")),
      expectedProtocol = Protocol(2, 5))

    testLegacyProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 7.toString),
        (DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")),
      expectedProtocol = Protocol(2, 7).withFeature(ColumnMappingTableFeature))

    withSQLConf(
      DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> 1.toString,
      DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> 7.toString) {
      testLegacyProtocolTransition(
        createTableProperties = Seq((DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")),
        expectedProtocol = Protocol(2, 7).withFeature(ColumnMappingTableFeature))
    }

    testLegacyProtocolTransition(
      alterTableProperties = Seq((DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")),
      expectedProtocol = Protocol(2, 5))

    /*
    testLegacyProtocolTransition(
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 7.toString),
        (DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")),
      expectedProtocol = Protocol(2, 7).withFeature(ColumnMappingTableFeature))
    */
  }
}
