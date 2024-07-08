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

  protected def testProtocolTransition(
      createTableColumns: Seq[(String, String)] = Seq.empty,
      createTableGeneratedColumns: Seq[(String, String, String)] = Seq.empty,
      createTableProperties: Seq[(String, String)] = Seq.empty,
      alterTableProperties: Seq[(String, String)] = Seq.empty,
      dropFeatures: Seq[TableFeature] = Seq.empty,
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

      // Drop features.
      dropFeatures.foreach { f =>
        sql(s"ALTER TABLE delta.`${deltaLog.dataPath}` DROP FEATURE ${f.name}")
      }

      assert(deltaLog.update().protocol === expectedProtocol)
    }
  }

  test("CREATE TABLE default protocol versions") {
    testProtocolTransition(
      expectedProtocol = Protocol(1, 2))

    // Setting table versions overrides protocol versions.
    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 1.toString)),
      expectedProtocol = Protocol(1, 1))
  }

  test("CREATE TABLE normalization") {
    // Default protocol is taken into account.
    testProtocolTransition(
      createTableProperties = Seq(
        (s"delta.feature.${TestRemovableWriterFeature.name}", "supported")),
      expectedProtocol = Protocol(1, 7).withFeatures(Seq(
        InvariantsTableFeature,
        AppendOnlyTableFeature,
        TestRemovableWriterFeature)))

    // Default protocol is not taken into account because we explicitly set the protocol versions.
    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 3.toString),
        ("delta.minWriterVersion", 7.toString),
        (s"delta.feature.${TestRemovableWriterFeature.name}", "supported")),
      expectedProtocol = Protocol(1, 7).withFeature(TestRemovableWriterFeature))

    // Reader version normalizes correctly.
    testProtocolTransition(
      createTableProperties = Seq(
        (s"delta.feature.${TestRemovableWriterFeature.name}", "supported"),
        (s"delta.feature.${ColumnMappingTableFeature.name}", "supported")),
      expectedProtocol =
        Protocol(2, 7).withFeatures(Seq(
          AppendOnlyTableFeature,
          InvariantsTableFeature,
          TestRemovableWriterFeature,
          ColumnMappingTableFeature)))

    // Reader version denormalizes correctly.
    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 7.toString),
        (s"delta.feature.${TestRemovableReaderWriterFeature.name}", "supported")),
      expectedProtocol = Protocol(3, 7).withFeature(TestRemovableReaderWriterFeature))

    // Reader version denormalizes correctly.
    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 2.toString),
        ("delta.minWriterVersion", 7.toString),
        (s"delta.feature.${TestRemovableReaderWriterFeature.name}", "supported")),
      expectedProtocol = Protocol(3, 7).withFeature(TestRemovableReaderWriterFeature))
  }

  // , (1, 5)
  for ((readerVersion, writerVersion) <- Seq((2, 1), (2, 2), (2, 3), (2, 4)))
  test("Invalid legacy protocol normalization" +
    s" - invalidProtocol($readerVersion, $writerVersion)") {

    val expectedReaderVersion = 1
    val expectedWriterVersion = Math.min(writerVersion, 4)

    // Base case.
    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", readerVersion.toString),
        ("delta.minWriterVersion", writerVersion.toString)),
      expectedProtocol = Protocol(expectedReaderVersion, expectedWriterVersion))

    // Invalid legacy versions are normalized in default confs.
    withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> readerVersion.toString,
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> writerVersion.toString) {
      testProtocolTransition(
        expectedProtocol = Protocol(expectedReaderVersion, expectedWriterVersion))
    }

    // Invalid legacy versions are normalized in alter table.
   testProtocolTransition(
     createTableProperties = Seq(
       ("delta.minReaderVersion", 1.toString),
       ("delta.minWriterVersion", 1.toString)),
     alterTableProperties = Seq(
       ("delta.minReaderVersion", readerVersion.toString),
       ("delta.minWriterVersion", writerVersion.toString)),
     expectedProtocol = Protocol(expectedReaderVersion, expectedWriterVersion))
  }

  test("ADD FEATURE normalization") {
    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 1.toString)),
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 4.toString)),
      expectedProtocol = Protocol(1, 4))

    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 2.toString)),
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 4.toString)),
      expectedProtocol = Protocol(1, 4))

    // Setting lower legacy versions is noop.
    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 4.toString)),
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 2.toString)),
      expectedProtocol = Protocol(1, 4))

    // Setting the same legacy versions is noop.
    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 4.toString)),
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 4.toString)),
      expectedProtocol = Protocol(1, 4))

    // Setting legacy versions is an ADD operation.
    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 6.toString)),
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 2.toString),
        ("delta.minWriterVersion", 5.toString)),
      expectedProtocol = Protocol(2, 6))

    // The inverse of the above test.
    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 2.toString),
        ("delta.minWriterVersion", 5.toString)),
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 6.toString)),
      expectedProtocol = Protocol(2, 6))

    /*
    testLegacyProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 4.toString)),
      alterTableProperties = Seq(
        (s"delta.feature.${ColumnMappingTableFeature.name}", "supported")),
      expectedProtocol = Protocol(2, 5))

    testLegacyProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 4.toString)),
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 7.toString),
        (DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")),
      expectedProtocol = Protocol(2, 5))
    */

    // Adding a legacy protocol to a table features protocol adds the features
    // of the former to the later.
    testProtocolTransition(
      createTableProperties = Seq(
        (s"delta.feature.${TestWriterFeature.name}", "supported")),
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 3.toString)),
      expectedProtocol = Protocol(1, 7).withFeatures(Seq(
        AppendOnlyTableFeature,
        CheckConstraintsTableFeature,
        InvariantsTableFeature,
        TestWriterFeature)))

    // Variation of the above.
    testProtocolTransition(
      createTableProperties = Seq(
        (s"delta.feature.${TestWriterFeature.name}", "supported"),
        (s"delta.feature.${IdentityColumnsTableFeature.name}", "supported")),
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 3.toString)),
      expectedProtocol = Protocol(1, 7).withFeatures(Seq(
        AppendOnlyTableFeature,
        CheckConstraintsTableFeature,
        InvariantsTableFeature,
        IdentityColumnsTableFeature,
        TestWriterFeature)))

    // New feature is added to the table protocol features.
    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 3.toString)),
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 7.toString),
        (DeltaConfigs.CHANGE_DATA_FEED.key, true.toString)),
      expectedProtocol = Protocol(1, 7).withFeatures(Seq(
        AppendOnlyTableFeature,
        InvariantsTableFeature,
        CheckConstraintsTableFeature,
        ChangeDataFeedTableFeature)))

    // Addition result is normalized.
    testProtocolTransition(
      createTableProperties = Seq(
        (s"delta.feature.${InvariantsTableFeature.name}", "supported")),
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 2.toString)),
      expectedProtocol = Protocol(1, 2))

    // Variation of the above.
    testProtocolTransition(
      createTableProperties = Seq(
        (s"delta.feature.${CheckConstraintsTableFeature.name}", "supported")),
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 2.toString)),
      expectedProtocol = Protocol(1, 3))

    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 2.toString)),
      alterTableProperties = Seq(
        (s"delta.feature.${CheckConstraintsTableFeature.name}", "supported")),
      expectedProtocol = Protocol(1, 3))
  }

  test("DROP FEATURE normalization") {
    // Can drop features on legacy protocols and the result is normalized.
    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 3.toString)),
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

    // If the removal result does not match a legacy version use the denormalized form.
    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 4.toString)),
      dropFeatures = Seq(CheckConstraintsTableFeature),
      expectedProtocol = Protocol(1, 7).withFeatures(Seq(
        AppendOnlyTableFeature,
        InvariantsTableFeature,
        GeneratedColumnsTableFeature,
        ChangeDataFeedTableFeature)))

    // Normalization after dropping a table feature.
    testProtocolTransition(
      createTableProperties = Seq(
        (s"delta.feature.${TestRemovableWriterFeature.name}", "supported")),
      dropFeatures = Seq(TestRemovableWriterFeature),
      expectedProtocol = Protocol(1, 2))

    // Variation of the above. Because the default protocol is overwritten the result
    // is normalized to (1, 1).
    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 7.toString),
        (s"delta.feature.${TestRemovableWriterFeature.name}", "supported")),
      dropFeatures = Seq(TestRemovableWriterFeature),
      expectedProtocol = Protocol(1, 1))

    // Reader version is normalized correctly to 2 after dropping the reader feature.
    testProtocolTransition(
      createTableProperties = Seq(
        (s"delta.feature.${ColumnMappingTableFeature.name}", "supported"),
        (s"delta.feature.${TestRemovableWriterFeature.name}", "supported"),
        (s"delta.feature.${TestRemovableReaderWriterFeature.name}", "supported")),
      dropFeatures = Seq(TestRemovableReaderWriterFeature),
      expectedProtocol = Protocol(2, 7).withFeatures(Seq(
        InvariantsTableFeature,
        AppendOnlyTableFeature,
        ColumnMappingTableFeature,
        TestRemovableWriterFeature)))

    testProtocolTransition(
      createTableProperties = Seq(
        (s"delta.feature.${TestRemovableWriterFeature.name}", "supported"),
        (s"delta.feature.${TestRemovableReaderWriterFeature.name}", "supported")),
      dropFeatures = Seq(TestRemovableReaderWriterFeature),
      expectedProtocol = Protocol(1, 7).withFeatures(Seq(
        InvariantsTableFeature,
        AppendOnlyTableFeature,
        TestRemovableWriterFeature)))
  }

  test("Default Enabled native features") {
    withSQLConf(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true") {
      // Table protocol is taken into account when default table features exist.
      testProtocolTransition(
        createTableProperties = Seq(
          ("delta.minReaderVersion", 1.toString),
          ("delta.minWriterVersion", 4.toString)),
        expectedProtocol = Protocol(3, 7).withFeatures(Seq(
          DeletionVectorsTableFeature,
          InvariantsTableFeature,
          AppendOnlyTableFeature,
          CheckConstraintsTableFeature,
          ChangeDataFeedTableFeature,
          GeneratedColumnsTableFeature)))

      // Default protocol versions are taken into account when default features exist.
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
    testProtocolTransition(
      createTableProperties = Seq((DeltaConfigs.CHANGE_DATA_FEED.key, true.toString)),
      expectedProtocol = Protocol(1, 4))

    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 3.toString),
        (DeltaConfigs.CHANGE_DATA_FEED.key, true.toString)),
      expectedProtocol = Protocol(1, 4))

    withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      testProtocolTransition(expectedProtocol = Protocol(1, 4))
    }

    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 7.toString),
        (DeltaConfigs.CHANGE_DATA_FEED.key, true.toString)),
      expectedProtocol = Protocol(1, 7).withFeature(ChangeDataFeedTableFeature))

    withSQLConf(
      DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> 1.toString,
      DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> 7.toString,
      DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      testProtocolTransition(
        expectedProtocol = Protocol(1, 7).withFeature(ChangeDataFeedTableFeature))
    }

    withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> 1.toString,
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> 7.toString) {
      testProtocolTransition(
        createTableProperties = Seq((DeltaConfigs.CHANGE_DATA_FEED.key, true.toString)),
        expectedProtocol = Protocol(1, 7).withFeature(ChangeDataFeedTableFeature))
    }

    withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      testProtocolTransition(
        createTableProperties = Seq(
          ("delta.minReaderVersion", 1.toString),
          ("delta.minWriterVersion", 7.toString)),
        expectedProtocol = Protocol(1, 7).withFeature(ChangeDataFeedTableFeature))
    }
  }

  test("Enabling legacy features on a table") {
    testProtocolTransition(
      createTableColumns = Seq(("id", "INT")),
      createTableGeneratedColumns = Seq(("id2", "INT", "id + 1")),
      expectedProtocol = Protocol(1, 4))

    testProtocolTransition(
      createTableColumns = Seq(("id", "INT")),
      createTableGeneratedColumns = Seq(("id2", "INT", "id + 1")),
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 7.toString)),
      expectedProtocol = Protocol(1, 7).withFeature(GeneratedColumnsTableFeature))

    withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> 1.toString,
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> 7.toString) {
      testProtocolTransition(
        createTableColumns = Seq(("id", "INT")),
        createTableGeneratedColumns = Seq(("id2", "INT", "id + 1")),
        expectedProtocol = Protocol(1, 7).withFeature(GeneratedColumnsTableFeature))
    }

    testProtocolTransition(
      alterTableProperties = Seq((DeltaConfigs.CHANGE_DATA_FEED.key, "true")),
      expectedProtocol = Protocol(1, 4))

    testProtocolTransition(
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
    testProtocolTransition(
      createTableProperties = Seq((DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")),
      expectedProtocol = Protocol(2, 5))

    testProtocolTransition(
      createTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 7.toString),
        (DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")),
      expectedProtocol = Protocol(2, 7).withFeature(ColumnMappingTableFeature))

    withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> 1.toString,
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> 7.toString) {
      testProtocolTransition(
        createTableProperties = Seq((DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")),
        expectedProtocol = Protocol(2, 7).withFeature(ColumnMappingTableFeature))
    }

    testProtocolTransition(
      alterTableProperties = Seq((DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")),
      expectedProtocol = Protocol(2, 5))

    testProtocolTransition(
      alterTableProperties = Seq(
        ("delta.minReaderVersion", 1.toString),
        ("delta.minWriterVersion", 7.toString),
        (DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")),
      expectedProtocol = Protocol(2, 7).withFeatures(Seq(
        InvariantsTableFeature,
        AppendOnlyTableFeature,
        ColumnMappingTableFeature)))
  }
}
