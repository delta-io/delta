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

import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.Utils.try_element_at

import org.apache.spark.sql.{DataFrameWriter, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SQLTestUtils

trait UniversalFormatTestHelper {
  val allCompatObjects: Seq[IcebergCompatBase] =
    Seq(
      IcebergCompatV1,
      IcebergCompatV2
    )
  def compatObjectFromVersion(version: Int): IcebergCompatBase =
    allCompatObjects(version - 1)

  def getCompatVersionOtherThan(version: Int): Int = {
    val targetVersion = getCompatVersionsOtherThan(version).head
    assert(targetVersion != version)
    targetVersion
  }

  def getCompatVersionsOtherThan(version: Int): Seq[Int] = {
    allCompatObjects
      .filter(_.version != version)
      .map(_.version.toInt)
  }
}

trait UniversalFormatSuiteBase extends IcebergCompatUtilsBase
  with UniversalFormatTestHelper {

  protected def assertUniFormIcebergProtocolAndProperties(
      tableId: String, compatVersion: Int = compatVersion): Unit = {
    assertIcebergCompatProtocolAndProperties(tableId, compatObjectFromVersion(compatVersion))

    val snapshot = DeltaLog.forTable(spark, TableIdentifier(tableId)).update()
    assert(UniversalFormat.icebergEnabled(snapshot.metadata))
  }

  protected def getDfWriter(
      colName: String,
      mode: String,
      enableUniform: Boolean = true): DataFrameWriter[Row] = {
    var df = spark.range(10)
      .toDF(colName)
      .write
      .mode(mode)
      .format("delta")
    df = if (mode == "overwrite") df.option("overwriteSchema", "true") else df
    if (enableUniform) {
      df.option(s"delta.enableIcebergCompatV$compatVersion", "true")
      df.option("delta.universalFormat.enabledFormats", "iceberg")
    } else {
      df
    }
  }

  protected def assertAddFileIcebergCompatVersion(
      snapshot: Snapshot,
      icebergCompatVersion: Int,
      count: Int): Unit = {
    val addFilesWithTagCount = snapshot.allFiles
      .select("tags")
      .where(try_element_at(col("tags"), AddFile.Tags.ICEBERG_COMPAT_VERSION.name)
        === s"$icebergCompatVersion")
      .count()
    assert(addFilesWithTagCount == count)
  }

  protected def runReorgTableForUpgradeUniform(
      tableId: String,
      icebergCompatVersion: Int = compatVersion): Unit = {
    executeSql(s"""
           | REORG TABLE $tableId APPLY
           | (UPGRADE UNIFORM (ICEBERG_COMPAT_VERSION = $icebergCompatVersion))
           |""".stripMargin)
  }

  protected def checkFileNotRewritten(
      prevSnapshot: Snapshot,
      currSnapshot: Snapshot): Unit = {
    val prevFiles = prevSnapshot.allFiles.collect().map(f => (f.path, f.modificationTime))
    val currFiles = currSnapshot.allFiles.collect().map(f => (f.path, f.modificationTime))

    val unchangedFiles = currFiles.filter { case (path, time) =>
      prevFiles.find(_._1 == path).exists(_._2 == time)
    }
    assert(unchangedFiles.length == currFiles.length)
  }

  test("create new UniForm table while manually enabling IcebergCompat") {
    allReaderWriterVersions.foreach { case (r, w) =>
      withTempTableAndDir { case (id, loc) =>
        executeSql(s"""
               |CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
               |  'delta.universalFormat.enabledFormats' = 'iceberg',
               |  'delta.enableIcebergCompatV$compatVersion' = 'true',
               |  'delta.minReaderVersion' = $r,
               |  'delta.minWriterVersion' = $w
               |)""".stripMargin)

        assertUniFormIcebergProtocolAndProperties(id)
      }
    }
  }

  test("create new UniForm table while manually enabling IcebergCompat with no rw version") {
    withTempTableAndDir { case (id, loc) =>
      executeSql(s"""
             |CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
             |  'delta.universalFormat.enabledFormats' = 'iceberg',
             |  'delta.enableIcebergCompatV$compatVersion' = 'true'
             |)""".stripMargin)
      assertUniFormIcebergProtocolAndProperties(id)
    }
  }

  test("enable UniForm on existing table with IcebergCompat enabled") {
    allReaderWriterVersions.foreach { case (r, w) =>
      withTempTableAndDir { case (id, loc) =>
        executeSql(s"""
               |CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
               |  'delta.minReaderVersion' = $r,
               |  'delta.minWriterVersion' = $w,
               |  'delta.enableIcebergCompatV$compatVersion' = true
               |)""".stripMargin)

        executeSql(s"ALTER TABLE $id SET TBLPROPERTIES " +
          s"('delta.universalFormat.enabledFormats' = 'iceberg')")

        assertUniFormIcebergProtocolAndProperties(id)
      }
    }
  }

  test("enable UniForm on existing table but IcebergCompat isn't enabled - fail") {
    allReaderWriterVersions.foreach { case (r, w) =>
      withTempTableAndDir { case (id, loc) =>
        executeSql(s"""
               |CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
               |  'delta.minReaderVersion' = $r,
               |  'delta.minWriterVersion' = $w,
               |  'delta.enableIcebergCompatV$compatVersion' = false,
               |  'delta.feature.icebergCompatV$compatVersion' = 'supported'
               |)""".stripMargin)

        val e = intercept[DeltaUnsupportedOperationException] {
          executeSql(s"ALTER TABLE $id SET TBLPROPERTIES " +
            s"('delta.universalFormat.enabledFormats' = 'iceberg')")
        }
        assert(e.getErrorClass === "DELTA_UNIVERSAL_FORMAT_VIOLATION")
      }
    }
  }

  test("disabling UniForm will not disable IcebergCompat") {
    withTempTableAndDir { case (id, loc) =>
      executeSql(
        s"""
           |CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
           |  'delta.universalFormat.enabledFormats' = 'iceberg',
           |  'delta.enableIcebergCompatV$compatVersion' = 'true'
           |)""".stripMargin)

      assertUniFormIcebergProtocolAndProperties(id)

      executeSql(s"ALTER TABLE $id UNSET TBLPROPERTIES ('delta.universalFormat.enabledFormats')")

      assert(getProperties(id)(s"delta.enableIcebergCompatV$compatVersion").toBoolean)
    }
  }

  test("disabling IcebergCompat will disable UniForm if enabled") {
    allReaderWriterVersions.foreach { case (r, w) =>
      withTempTableAndDir { case (id, loc) =>
        executeSql(s"""
               |CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
               |  'delta.minReaderVersion' = $r,
               |  'delta.minWriterVersion' = $w,
               |  'delta.universalFormat.enabledFormats' = 'iceberg',
               |  'delta.enableIcebergCompatV$compatVersion' = true
               |)""".stripMargin)
        var tableprops = getProperties(id)
        assert(tableprops("delta.universalFormat.enabledFormats") === "iceberg")
        assert(tableprops(s"delta.enableIcebergCompatV$compatVersion").toBoolean)

        executeSql(s"""
               |ALTER TABLE $id SET TBLPROPERTIES (
               |'delta.enableIcebergCompatV$compatVersion' = false)
               |""".stripMargin)

        tableprops = getProperties(id)
        assert(!tableprops.contains("delta.universalFormat.enabledFormats"))
        assert(!tableprops(s"delta.enableIcebergCompatV$compatVersion").toBoolean)
      }
    }
  }

  test("REORG TABLE for table from None to corresponding icebergCompat version") {
    withTempTableAndDir { case (id, loc) =>
      executeSql(s"""
             | CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc
             | """.stripMargin)
      executeSql(s"""
             | INSERT INTO TABLE $id (ID)
             | VALUES (1),(2),(3),(4),(5),(6),(7)""".stripMargin)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(id))
      val snapshot = deltaLog.update()
      val prevNumAddFiles = snapshot.allFiles.collect().length
      assert(prevNumAddFiles === 1)
      assertAddFileIcebergCompatVersion(snapshot, icebergCompatVersion = compatVersion, count = 0)

      runReorgTableForUpgradeUniform(id, compatVersion)
      val updatedSnapshot = deltaLog.update()
      assert(updatedSnapshot.getProperties(s"delta.enableIcebergCompatV$compatVersion") === "true")

      compatVersion match {
        case 1 => checkFileNotRewritten(snapshot, updatedSnapshot)
        case num => assertAddFileIcebergCompatVersion(
          deltaLog.update(), icebergCompatVersion = num, count = prevNumAddFiles)
      }
    }
  }

  test("REORG TABLE for table from icebergCompatVx to icebergCompatVx, should skip rewrite") {
    withTempTableAndDir { case (id, loc) =>
      executeSql(s"""
             | CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
             |  'delta.universalFormat.enabledFormats' = 'iceberg',
             |  'delta.enableIcebergCompatV$compatVersion' = 'true'
             |)
             | """.stripMargin)
      executeSql(s"""
             | INSERT INTO TABLE $id (ID)
             | VALUES (1),(2),(3),(4),(5),(6),(7)""".stripMargin)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(id))
      val snapshot = deltaLog.update()
      val expectedNumAddFilesWithIcebergCompatVersion = compatVersion match {
        case 1 => 0
        case _ => 1
      }
      assertAddFileIcebergCompatVersion(
        snapshot,
        icebergCompatVersion = compatVersion,
        count = expectedNumAddFilesWithIcebergCompatVersion)

      runReorgTableForUpgradeUniform(id, compatVersion)
      val updatedSnapshot = deltaLog.update()
      assert(updatedSnapshot.getProperties(s"delta.enableIcebergCompatV$compatVersion") === "true")
      assert(snapshot.version == updatedSnapshot.version)
      checkFileNotRewritten(snapshot, updatedSnapshot)
    }
  }

  test("REORG TABLE: file would not be rewritten again if we run command twice") {
    withTempTableAndDir { case (id, loc) =>
      val anotherCompatVersion = getCompatVersionOtherThan(compatVersion)
      executeSql(s"""
             | CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
             |  'delta.universalFormat.enabledFormats' = 'iceberg',
             |  'delta.enableIcebergCompatV$anotherCompatVersion' = 'true'
             |)""".stripMargin)
      executeSql(s"""
             | INSERT INTO TABLE $id (ID)
             | VALUES (1),(2),(3),(4),(5),(6),(7)""".stripMargin)
      runReorgTableForUpgradeUniform(id, compatVersion)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(id))
      val snapshot1 = deltaLog.update()
      val expectedNumAddFilesWithIcebergCompatVersion = compatVersion match {
        case 1 => 0
        case _ => 1
      }
      assertAddFileIcebergCompatVersion(
        snapshot1,
        icebergCompatVersion = compatVersion,
        count = expectedNumAddFilesWithIcebergCompatVersion
      )

      runReorgTableForUpgradeUniform(id, compatVersion)
      val snapshot2 = deltaLog.update()
      checkFileNotRewritten(snapshot1, snapshot2)
    }
  }

  test("REORG TABLE: exception would be thrown for unsupported icebergCompatVersion") {
    withTempTableAndDir { case (id, loc) =>
      executeSql(s"""
             | CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
             |  'delta.columnMapping.mode' = 'name'
             |)
             | """.stripMargin)
      val e = intercept[DeltaUnsupportedOperationException] {
        runReorgTableForUpgradeUniform(id, 5)
      }
      assert(e.getErrorClass === "DELTA_ICEBERG_COMPAT_VIOLATION.COMPAT_VERSION_NOT_SUPPORTED")
    }
  }
}

trait UniFormWithIcebergCompatV1SuiteBase extends UniversalFormatSuiteBase {
  protected override val compatObject: IcebergCompatBase = IcebergCompatV1

  test("enable UniForm and V1 on existing table") {
    withTempTableAndDir { case (id, loc) =>
      executeSql(s"CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc")

      executeSql(s"""
             |ALTER TABLE $id SET TBLPROPERTIES (
             |  'delta.minReaderVersion' = 2,
             |  'delta.minWriterVersion' = 5,
             |  'delta.universalFormat.enabledFormats' = 'iceberg',
             |  'delta.enableIcebergCompatV1' = true,
             |  'delta.columnMapping.mode' = 'name'
             |)""".stripMargin)

      assertUniFormIcebergProtocolAndProperties(id)
    }
  }

  test("REORG TABLE for table from icebergCompatVx to icebergCompatV1, should skip rewrite") {
    getCompatVersionsOtherThan(1).foreach(originalVersion => {
      withTempTableAndDir { case (id, loc) =>
        executeSql(s"""
               | CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
               |  'delta.universalFormat.enabledFormats' = 'iceberg',
               |  'delta.enableIcebergCompatV$originalVersion' = 'true'
               |)
               | """.stripMargin)
        executeSql(s"""
               | INSERT INTO TABLE $id (ID)
               | VALUES (1),(2),(3),(4),(5),(6),(7)""".stripMargin)
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(id))
        val snapshot = deltaLog.update()
        assertAddFileIcebergCompatVersion(
          snapshot, icebergCompatVersion = originalVersion, count = 1)

        runReorgTableForUpgradeUniform(id, icebergCompatVersion = 1)
        val updatedSnapshot = deltaLog.update()
        assert(updatedSnapshot.getProperties("delta.enableIcebergCompatV1") === "true")
        assertAddFileIcebergCompatVersion(
          deltaLog.update(), icebergCompatVersion = originalVersion, count = 1)
        checkFileNotRewritten(snapshot, updatedSnapshot)
      }
    })
  }
}

trait UniFormWithIcebergCompatV2SuiteBase extends UniversalFormatSuiteBase {
  override val compatObject: IcebergCompatBase = IcebergCompatV2

  test("can downgrade from V2 to V1 with ALTER with UniForm enabled") {
    withTempTableAndDir {
      case (id, loc) =>
        executeSql(s"""
               |CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
               |  'delta.enableIcebergCompatV2' = 'true',
               |  'delta.universalFormat.enabledFormats' = 'iceberg'
               |)""".stripMargin)
        executeSql(s"""
               |ALTER TABLE $id SET TBLPROPERTIES (
               |  'delta.enableIcebergCompatV1' = true,
               |  'delta.enableIcebergCompatV2' = false
               |)""".stripMargin)
        assertUniFormIcebergProtocolAndProperties(id, 1)
    }
  }

  test("REORG TABLE for table from icebergCompatVx to icebergCompatV2") {
    val originalVersion = 1
    withTempTableAndDir { case (id, loc) =>
      executeSql(s"""
           | CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
           |  'delta.universalFormat.enabledFormats' = 'iceberg',
           |  'delta.enableIcebergCompatV$originalVersion' = 'true'
           |)""".stripMargin)
      executeSql(s"""
           | INSERT INTO TABLE $id (ID)
           | VALUES (1),(2),(3),(4),(5),(6),(7)""".stripMargin)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(id))
      val snapshot1 = deltaLog.update()
      assert(snapshot1.allFiles.collect().nonEmpty)
      assertAddFileIcebergCompatVersion(snapshot1, icebergCompatVersion = 2, count = 0)

      runReorgTableForUpgradeUniform(id, icebergCompatVersion = 2)
      val snapshot2 = deltaLog.update()
      assert(snapshot2.getProperties("delta.enableIcebergCompatV2") === "true")
      assert(snapshot2.getProperties("delta.enableDeletionVectors") === "false")
      assertAddFileIcebergCompatVersion(snapshot2, icebergCompatVersion = 2, count = 1)
    }
  }

  test(
    "REORG TABLE: new files would have ICEBERG_COMPAT_VERSION tag if enableIcebergCompat is on") {
    withTempTableAndDir { case (id, loc) =>
      executeSql(
        s"""
           | CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
           | 'delta.columnMapping.mode' = 'name'
           |)
           | """.stripMargin)
      executeSql(
        s"""
           | INSERT INTO TABLE $id (ID)
           | VALUES (1),(2),(3),(4),(5),(6),(7)""".stripMargin)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(id))
      val txn = deltaLog.startTransaction()
      val metadata = txn.metadata
      val enableIcebergCompatConf = Map(
        DeltaConfigs.ICEBERG_COMPAT_V1_ENABLED.key -> "false",
        DeltaConfigs.ICEBERG_COMPAT_V2_ENABLED.key -> "true")
      val newMetadata = metadata.copy(
        configuration = metadata.configuration ++ enableIcebergCompatConf)
      txn.updateMetadata(newMetadata)
      txn.commit(
        Nil,
        DeltaOperations.UpgradeUniformProperties(enableIcebergCompatConf)
      )
      assertAddFileIcebergCompatVersion(
        deltaLog.update(), icebergCompatVersion = 2, count = 0)

      // The new file would have the ICEBERG_COMPAT_VERSION tag while the exist files would not
      executeSql(s"""
             | INSERT INTO TABLE $id (ID)
             | VALUES (8),(9),(10)""".stripMargin)
      assertAddFileIcebergCompatVersion(
        deltaLog.update(), icebergCompatVersion = 2, count = 1)

      // After REORG TABLE command, all the exist files would have ICEBERG_COMPAT_VERSION tag
      runReorgTableForUpgradeUniform(id, 2)
      val finalSnapshot = deltaLog.update()
      assert(finalSnapshot.getProperties("delta.enableIcebergCompatV2") === "true")
      assertAddFileIcebergCompatVersion(finalSnapshot, icebergCompatVersion = 2, count = 2)
    }
  }
}

trait UniversalFormatMiscSuiteBase extends IcebergCompatUtilsBase with UniversalFormatTestHelper {
  test("enforceInvariantsAndDependenciesForCTAS") {
    withTempTableAndDir { case (id, loc) =>
      executeSql(s"CREATE TABLE $id (id INT) USING DELTA LOCATION $loc")
      val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, loc)
      var configurationUnderTest = Map("dummykey1" -> "dummyval1", "dummykey2" -> "dummyval2")
      // The enforce is not lossy. It will do nothing if there is no Universal related key.

      def getUpdatedConfiguration(conf: Map[String, String]): Map[String, String] =
        UniversalFormat.enforceDependenciesInConfiguration(spark, conf, snapshot)

      var updatedConfiguration = getUpdatedConfiguration(configurationUnderTest)
      assert(configurationUnderTest == configurationUnderTest)

      configurationUnderTest = Map(
        "delta.universalFormat.enabledFormats" -> "iceberg",
        "dummykey" -> "dummyvalue"
      )
      val e = intercept[DeltaUnsupportedOperationException] {
        updatedConfiguration = getUpdatedConfiguration(configurationUnderTest)
      }
      assert(e.getErrorClass == "DELTA_UNIVERSAL_FORMAT_VIOLATION")

      for (icv <- allCompatObjects.map(_.version)) {
        configurationUnderTest = Map(
          s"delta.enableIcebergCompatV$icv" -> "true",
          "delta.universalFormat.enabledFormats" -> "iceberg",
          "dummykey" -> "dummyvalue"
        )
        updatedConfiguration = getUpdatedConfiguration(configurationUnderTest)

        assert(updatedConfiguration.size == 5)
        assert(updatedConfiguration("dummykey") == "dummyvalue")
        assert(updatedConfiguration("delta.universalFormat.enabledFormats") == "iceberg")
        assert(updatedConfiguration("delta.columnMapping.mode") == "name")
        assert(updatedConfiguration(s"delta.enableIcebergCompatV$icv") == "true")
        assert(updatedConfiguration("delta.columnMapping.maxColumnId") == "0")

        configurationUnderTest = Map(
          s"delta.enableIcebergCompatV$icv" -> "true",
          "delta.universalFormat.enabledFormats" -> "iceberg",
          "dummykey" -> "dummyvalue",
          "delta.columnMapping.mode" -> "id"
        )
        updatedConfiguration = getUpdatedConfiguration(configurationUnderTest)
        assert(updatedConfiguration.size == 4)
        assert(updatedConfiguration("dummykey") == "dummyvalue")
        assert(updatedConfiguration("delta.columnMapping.mode") == "id")
        assert(updatedConfiguration("delta.universalFormat.enabledFormats") == "iceberg")
        assert(updatedConfiguration(s"delta.enableIcebergCompatV$icv") == "true")
      }
    }
  }

  test("UniForm config validation") {
    Seq("ICEBERG", "iceberg,iceberg", "iceber", "paimon").foreach { invalidConf =>
      withTempTableAndDir { case (id, loc) =>
        val errMsg = intercept[IllegalArgumentException] {
          executeSql(s"""
                 |CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
                 |  'delta.universalFormat.enabledFormats' = '$invalidConf',
                 |  'delta.enableIcebergCompatV1' = 'true',
                 |  'delta.columnMapping.mode' = 'name'
                 |)""".stripMargin)
        }.getMessage
        assert(
          errMsg.contains("Must be a comma-separated list of formats from the list"),
          errMsg
        )
      }
    }
  }

  test("create new UniForm table without manually enabling IcebergCompat - fail") {
    allReaderWriterVersions.foreach { case (r, w) =>
      withTempTableAndDir { case (id, loc) =>
        val e = intercept[DeltaUnsupportedOperationException] {
          executeSql(s"""
                 |CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
                 |  'delta.universalFormat.enabledFormats' = 'iceberg',
                 |  'delta.minReaderVersion' = $r,
                 |  'delta.minWriterVersion' = $w
                 |)""".stripMargin)
        }
        assert(e.getErrorClass == "DELTA_UNIVERSAL_FORMAT_VIOLATION")

        val e1 = intercept[DeltaUnsupportedOperationException] {
          executeSql(s"""
                 |CREATE TABLE $id USING DELTA LOCATION $loc TBLPROPERTIES (
                 |  'delta.universalFormat.enabledFormats' = 'iceberg',
                 |  'delta.minReaderVersion' = $r,
                 |  'delta.minWriterVersion' = $w
                 |) AS SELECT 1""".stripMargin)
        }
        assert(e1.getErrorClass == "DELTA_UNIVERSAL_FORMAT_VIOLATION")
      }
    }
  }

  test("enable UniForm on existing table but IcebergCompat isn't enabled - fail") {
    allReaderWriterVersions.foreach { case (r, w) =>
      withTempTableAndDir { case (id, loc) =>
        executeSql(s"""
               |CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
               |  'delta.minReaderVersion' = $r,
               |  'delta.minWriterVersion' = $w
               |)""".stripMargin)

        val e = intercept[DeltaUnsupportedOperationException] {
          executeSql(s"ALTER TABLE $id SET TBLPROPERTIES " +
            s"('delta.universalFormat.enabledFormats' = 'iceberg')")
        }
        assert(e.getErrorClass === "DELTA_UNIVERSAL_FORMAT_VIOLATION")
      }
    }
  }
}
