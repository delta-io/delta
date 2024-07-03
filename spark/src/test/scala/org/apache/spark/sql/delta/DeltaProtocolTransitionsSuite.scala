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

  test("TABLE CREATION with enabled features by default") {
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

    /*
    withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      testProtocolTransition(
        createTableProtocol = Some(Protocol(1, 1)),
        expectedProtocol = Protocol(1, 7).withFeatures(Seq(ChangeDataFeedTableFeature)))
    }
    */
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
}
