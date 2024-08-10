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

package org.apache.spark.sql.delta.uniform

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaUnsupportedOperationException, Snapshot}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.parquet.hadoop.metadata.ParquetMetadata

import org.apache.spark.sql.{DataFrame, QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier

trait IcebergCompatV2EnableUniformByAlterTableSuiteBase extends QueryTest {
  override protected def spark: SparkSession

  /**
   * Assert the invariance between old and new parquet footers,
   * this will check if the number of overlapped parquet footers
   * is the same as expected and extract the newer portion of footers,
   * i.e., the portion of parquet files present in `newParquetFooters`
   * but not in `oldParquetFooters`; or the overlapped portion of footers,
   * as specified by the flag `newerOrOverlapped`.
   *
   * This function is useful when, e.g.,
   * - checking the invariance of parquet footers before and after
   *   ALTER TABLE to enable UniForm, a portion of parquet footers
   *   will stay the same, and the new portion of parquet footers
   *   should be `IcebergCompatV2`.
   * - after running REORG UPGRADE UNIFORM, there may be a portion of
   *   parquet files that do not need to be rewritten, and the number
   *   should be the same as expected.
   *
   * @param oldParquetFooters the old version of parquet footers.
   * @param newParquetFooters the new version of parquet footers.
   * @param expectedNumOfOverlappedParquetFiles the expected number of overlapped parquet footers.
   * @param expectedNumOfAddedParquetFiles the expected number of added portion of parquet footers.
   * @return a pair consists of (overlapped parquet footers, added parquet footers).
   */
  protected def assertInvarianceAndExtractParquetFooters(
      oldParquetFooters: Seq[ParquetMetadata],
      newParquetFooters: Seq[ParquetMetadata],
      expectedNumOfOverlappedParquetFiles: Int,
      expectedNumOfAddedParquetFiles: Int): (Seq[ParquetMetadata], Seq[ParquetMetadata]) = {
    val oldParquetFootersInStr = oldParquetFooters.map { _.toString }
    val newParquetFootersInStr = newParquetFooters.map { _.toString }
    val overlappedParquetFootersInStr = oldParquetFootersInStr.filter { footer =>
      newParquetFootersInStr.contains(footer)
    }
    assert(
      overlappedParquetFootersInStr.length == expectedNumOfOverlappedParquetFiles,
      s"expect number of overlapped parquet footers to be $expectedNumOfOverlappedParquetFiles, " +
        s"but get ${overlappedParquetFootersInStr.length}"
    )
    val addedParquetFootersInStr = newParquetFootersInStr.filter { footer =>
      !oldParquetFootersInStr.contains(footer)
    }
    assert(
      addedParquetFootersInStr.length == expectedNumOfAddedParquetFiles,
      s"expect number of newer parquet footers to be $expectedNumOfAddedParquetFiles, " +
        s"but get ${addedParquetFootersInStr.length}"
    )
    val overlappedParquetFooters = oldParquetFooters.filter { footer =>
      overlappedParquetFootersInStr.contains(footer.toString)
    }
    val addedParquetFooters = newParquetFooters.filter { footer =>
      addedParquetFootersInStr.contains(footer.toString)
    }
    (overlappedParquetFooters, addedParquetFooters)
  }


  /**
   * Assert the properties for old and new parquet footers.
   * Specifically, first check the number of overlapped and added parquet footers
   * to match with the expected numbers;
   * then extract and assert whether each should be considered `IcebergCompatV2`
   * by the expected values.
   *
   * @param oldParquetFooters the old version of parquet footers.
   * @param newParquetFooters the new version of parquet footers.
   * @param expectedNumOfOverlappedParquetFiles the expected number of overlapped parquet footers.
   * @param expectedNumOfAddedParquetFiles the expected number of added parquet footers.
   * @param isOverlappedIcebergCompatV2 whether the overlapped parquet footers is expected
   *                                    to be `IcebergCompatV2`.
   * @param isAddedIcebergCompatV2 whether the added parquet footers is expected to be
   *                               `IcebergCompatV2`.
   */
  protected def assertParquetFootersProperties(
      oldParquetFooters: Seq[ParquetMetadata],
      newParquetFooters: Seq[ParquetMetadata],
      expectedNumOfOverlappedParquetFiles: Int,
      expectedNumOfAddedParquetFiles: Int,
      isOverlappedIcebergCompatV2: Boolean,
      isAddedIcebergCompatV2: Boolean): Unit = {
    val (overlapped, added) = assertInvarianceAndExtractParquetFooters(
      oldParquetFooters = oldParquetFooters,
      newParquetFooters = newParquetFooters,
      expectedNumOfOverlappedParquetFiles = expectedNumOfOverlappedParquetFiles,
      expectedNumOfAddedParquetFiles = expectedNumOfAddedParquetFiles
    )
    assert(isParquetFootersIcebergCompatV2(overlapped) == isOverlappedIcebergCompatV2)
    assert(isParquetFootersIcebergCompatV2(added) == isAddedIcebergCompatV2)
  }

  /** Check if `IcebergCompatV1` is enabled based on the provided snapshot */
  protected def isIcebergCompatV1Enabled(snapshot: Snapshot): Boolean = {
    snapshot
      .getProperties(DeltaConfigs.ICEBERG_COMPAT_V1_ENABLED.key)
      .contains("true")
  }

  /** Check if `IcebergCompatV2` is enabled based on the provided snapshot */
  protected def isIcebergCompatV2Enabled(snapshot: Snapshot): Boolean = {
    snapshot
      .getProperties(DeltaConfigs.ICEBERG_COMPAT_V2_ENABLED.key)
      .contains("true")
  }

  /**
   * Insert three initial rows to the specified table.
   *
   * @param id the table id used for insertion.
   */
  protected def insertInitialRowsIntoTable(id: String): Unit = {
    executeSql(
      s"""
         | INSERT INTO TABLE $id
         | VALUES
         | (1, 'Alex', '2000-01-01'),
         | (1, 'Cat', '2001-01-01'),
         | (2, 'Michael', '2002-10-30')
         |""".stripMargin
    )
  }

  /**
   * Insert two additional rows to the specified table.
   *
   * @param id the table id used for insertion.
   */
  protected def insertAdditionalRowsIntoTable(id: String): Unit = {
    executeSql(
      s"""
         | INSERT INTO TABLE $id
         | VALUES
         | (3, 'Cat', '2003-01-01'),
         | (4, 'Cat', '2004-01-02')
         |""".stripMargin
    )
  }

  /**
   * Create a vanilla delta table with a single partition column.
   *
   * @param id the table id used for creation.
   * @param loc the table location.
   */
  protected def createVanillaDeltaTableWithDV(id: String, loc: String): Unit = {
    executeSql(
      s"""
         | CREATE TABLE $id (id INT, name STRING, date TIMESTAMP)
         | USING DELTA
         | PARTITIONED BY (id)
         | LOCATION $loc
         |""".stripMargin
    )
  }

  /**
   * Create a vanilla delta table with DV disabled and a single partition column.
   *
   * @param id the table id used for creation.
   * @param loc the table location.
   */
  protected def createVanillaDeltaTableWithoutDV(id: String, loc: String): Unit = {
    executeSql(
      s"""
         | CREATE TABLE $id (id INT, name STRING, date TIMESTAMP)
         | USING DELTA
         | PARTITIONED BY (id)
         | LOCATION $loc
         | TBLPROPERTIES (
         |   'delta.enableDeletionVectors' = 'false',
         |   'delta.minReaderVersion' = '2',
         |   'delta.minWriterVersion' = '7'
         | )
         |""".stripMargin
    )
  }

  /**
   * Create an `IcebergCompatV1` uniform table with a single partition column.
   *
   * @param id the table id used for creation.
   * @param loc the table location.
   */
  protected def createIcebergCompatV1Table(id: String, loc: String): Unit = {
    executeSql(
      s"""
         | CREATE TABLE $id (id INT, name STRING, date TIMESTAMP)
         | USING DELTA
         | PARTITIONED BY (id)
         | LOCATION $loc
         | TBLPROPERTIES (
         |   'delta.enableIcebergCompatV1' = 'true',
         |   'delta.universalFormat.enabledFormats' = 'iceberg'
         | )
         |""".stripMargin
    )
  }

  /**
   * Create a delta table with liquid clustering enabled for a single column.
   *
   * @param id the table id used for creation.
   * @param loc the table location.
   */
  protected def createLiquidDeltaTable(id: String, loc: String): Unit = {
    executeSql(
      s"""
         | CREATE TABLE $id (id INT, name STRING, date TIMESTAMP)
         | USING DELTA
         | CLUSTER BY (id)
         | LOCATION $loc
         |""".stripMargin
    )
  }

  /**
   * Create a delta table with nested types and column-mapping enabled.
   *
   * @param id the table id used for creation.
   * @param loc the table location.
   */
  protected def createDeltaTableWithNestedTypesAndColumnMapping(id: String, loc: String): Unit = {
    executeSql(
      s"""
         | CREATE TABLE $id (
         |   id INT,
         |   listOfInt ARRAY<INT>,
         |   listOfList ARRAY<ARRAY<INT>>,
         |   listOfMap ARRAY<MAP<INT, STRING>>,
         |   map MAP<INT, STRING>,
         |   mapOfMap MAP<INT, MAP<INT, STRING>>,
         |   mapOfList MAP<INT, ARRAY<STRING>>,
         |   struct STRUCT<col1: INT, col2: STRING>,
         |   structOfStruct STRUCT<col1: INT, col2: STRUCT<col1: INT, col2: STRING>>,
         |   structOfListAndMap STRUCT<col1: INT, col2: ARRAY<INT>, col3: MAP<INT, STRING>>)
         | USING DELTA
         | LOCATION $loc
         | TBLPROPERTIES (
         |   'delta.columnMapping.mode' = 'name'
         | )
         |""".stripMargin
    )
  }

  /**
   * Insert a single row to the delta table with nested types and column-mapping enabled.
   *
   * @param id the table used for insertion.
   */
  protected def insertRowToDeltaTableWithNestedTypesAndColumnMapping(id: String): Unit = {
    executeSql(
      s"""
         | INSERT INTO TABLE $id VALUES
         | (1,
         |  ARRAY(1, 2, 3),
         |  ARRAY(ARRAY(1, 2), ARRAY(3, 4)),
         |  ARRAY(MAP(1, 'Alex'), MAP(2, 'Michael')),
         |  MAP(1, 'Alex'),
         |  MAP(1, MAP(2, 'Michael')),
         |  MAP(3, ARRAY('Cat', 'Cat')),
         |  STRUCT(2, 'Michael'),
         |  STRUCT(1, STRUCT(2, 'Cat')),
         |  STRUCT(1, ARRAY(4, 5, 6), MAP(7, 'Alex'))
         | )
         |""".stripMargin
    )
  }

  /**
   * Get all parquet footers of data files for the specified table.
   *
   * @param spark the spark session used to get the footers.
   * @param id the table id from which to get all the parquet footers.
   * @return all parquet metadata/footers of the parquet (data) files for this table.
   */
  protected def getParquetFooters(spark: SparkSession, id: String): Seq[ParquetMetadata] = {
    val snapshot = DeltaLog.forTable(spark, new TableIdentifier(id)).update()
    val basePath = snapshot.path.getParent.toString + "/"
    val addFiles: Array[AddFile] = snapshot.allFiles.collect()
    val parquetPaths: Array[String] = addFiles.map { basePath + _.path }
    parquetPaths.map { ParquetIcebergCompatV2Utils.getParquetFooter }
  }

  /**
   * Check whether the current parquet footers are all `IcebergCompatV2`.
   * This will check two properties for each parquet footer,
   * see [[ParquetIcebergCompatV2Utils.isParquetIcebergCompatV2]] for details.
   *
   * @param parquetFooters the parquet footers to be checked.
   * @return whether the footers are considered `IcebergCompatV2`
   */
  protected def isParquetFootersIcebergCompatV2(parquetFooters: Seq[ParquetMetadata]): Boolean = {
    parquetFooters.forall { parquetFooter =>
      ParquetIcebergCompatV2Utils.isParquetIcebergCompatV2(parquetFooter)
    }
  }

  /** The wrapper function to execute sql */
  protected def executeSql(sqlStr: String): DataFrame

  /** The wrapper function to assert the protocol and properties for UniForm Iceberg */
  protected def assertUniFormIcebergProtocolAndProperties(id: String): Unit

  /** The wrapper function to generate a temporary table and directory */
  protected def withTempTableAndDir(f: (String, String) => Unit): Unit

  /**
   * Helper function to enforce the properties that an `IcebergCompatV2`
   * Delta Uniform requires.
   * e.g., disable DV, ensure reader/writer versions, enable column-mapping.
   *
   * @param id the table to be altered.
   */
  protected def enforceDeltaUniformRequireProperties(id: String): Unit = {
    executeSql(
      s"""
         | ALTER TABLE $id
         | SET TBLPROPERTIES (
         |   'delta.columnMapping.mode' = 'name',
         |   'delta.enableDeletionVectors' = 'false',
         |   'delta.minReaderVersion' = '2',
         |   'delta.minWriterVersion' = '7'
         | )
         |""".stripMargin
    )
  }

  /**
   * Enable `IcebergCompatV2` by ALTER TABLE command for the table.
   *
   * @param id the table id used for ALTER TABLE to enable `IcebergCompatV2`.
   */
  protected def enableIcebergCompatV2ByAlterTable(id: String): Unit = {
    executeSql(
      s"""
         | ALTER TABLE $id
         | SET TBLPROPERTIES (
         |   'delta.enableIcebergCompatV2' = 'true',
         |   'delta.universalFormat.enabledFormats' = 'iceberg'
         | )
         |""".stripMargin
    )
  }

  /**
   * The basic test case for enable uniform by ALTER TABLE,
   * this could be used as a prior setup for subsequent tests.
   *
   * @param id the table id.
   * @param loc the table location.
   */
  protected def alterTableToEnableIcebergCompatV2BaseCase(id: String, loc: String): Unit = {
    createVanillaDeltaTableWithDV(id, loc)
    insertInitialRowsIntoTable(id)

    val parquetFooters1 = getParquetFooters(spark, id)

    enforceDeltaUniformRequireProperties(id)
    enableIcebergCompatV2ByAlterTable(id)

    insertAdditionalRowsIntoTable(id)

    val parquetFooters2 = getParquetFooters(spark, id)
    assertParquetFootersProperties(
      oldParquetFooters = parquetFooters1,
      newParquetFooters = parquetFooters2,
      expectedNumOfOverlappedParquetFiles = 2,
      expectedNumOfAddedParquetFiles = 2,
      isOverlappedIcebergCompatV2 = false,
      isAddedIcebergCompatV2 = true
    )

    assertUniFormIcebergProtocolAndProperties(id)
  }

  test("Enable IcebergCompatV2 By ALTER TABLE Base Case") {
    withTempTableAndDir { case (id, loc) =>
      alterTableToEnableIcebergCompatV2BaseCase(id, loc)
    }
  }

  test("Enable IcebergCompatV2 For Vanilla Table By ALTER TABLE With Deletion Vectors Disabled") {
    withTempTableAndDir { case (id, loc) =>
      createVanillaDeltaTableWithoutDV(id, loc)
      insertInitialRowsIntoTable(id)

      executeSql(
        s"""
           | ALTER TABLE $id
           | SET TBLPROPERTIES (
           |   'delta.columnMapping.mode' = 'name'
           | )
           |""".stripMargin
      )

      val parquetFooters1 = getParquetFooters(spark, id)

      // no need to manually disable DV
      enableIcebergCompatV2ByAlterTable(id)

      insertAdditionalRowsIntoTable(id)

      val parquetFooters2 = getParquetFooters(spark, id)
      assertParquetFootersProperties(
        oldParquetFooters = parquetFooters1,
        newParquetFooters = parquetFooters2,
        expectedNumOfOverlappedParquetFiles = 2,
        expectedNumOfAddedParquetFiles = 2,
        isOverlappedIcebergCompatV2 = false,
        isAddedIcebergCompatV2 = true
      )

      assertUniFormIcebergProtocolAndProperties(id)
    }
  }

  test("Enable IcebergCompatV2 By ALTER TABLE With Purging Deletion Vectors") {
    withTempTableAndDir { case (id, loc) =>
      createVanillaDeltaTableWithDV(id, loc)
      insertInitialRowsIntoTable(id)

      executeSql(
        s"""
           | DELETE FROM $id
           | WHERE name = 'Alex'
           |""".stripMargin
      )

      // purge the current table before running ALTER TABLE
      // note: this may be different if `autoOptimize` has been enabled,
      // which will automatically rewrite the parquet file with DV when
      // the above DELETE FROM command is triggered.
      executeSql(
        s"""
           | REORG TABLE $id APPLY (PURGE)
           |""".stripMargin
      )

      val parquetFooters1 = getParquetFooters(spark, id)

      enforceDeltaUniformRequireProperties(id)
      enableIcebergCompatV2ByAlterTable(id)

      insertAdditionalRowsIntoTable(id)

      val parquetFooters2 = getParquetFooters(spark, id)
      assertParquetFootersProperties(
        oldParquetFooters = parquetFooters1,
        newParquetFooters = parquetFooters2,
        expectedNumOfOverlappedParquetFiles = 2,
        expectedNumOfAddedParquetFiles = 2,
        isOverlappedIcebergCompatV2 = false,
        isAddedIcebergCompatV2 = true
      )

      assertUniFormIcebergProtocolAndProperties(id)
    }
  }

  test("REORG UPGRADE UNIFORM Should Rewrite All Parquet Files To Be IcebergCompatV2") {
    withTempTableAndDir { case (id, loc) =>
      alterTableToEnableIcebergCompatV2BaseCase(id, loc)

      // run the REORG UPGRADE UNIFORM command to rewrite the portion of
      // parquet files that are not `IcebergCompatV2`.
      executeSql(
        s"""
           | REORG TABLE $id APPLY
           | (UPGRADE UNIFORM (ICEBERG_COMPAT_VERSION = 2))
           |""".stripMargin
      )

      // now all the parquet files must be `IcebergCompatV2`
      val parquetFooters3 = getParquetFooters(spark, id)
      assert(isParquetFootersIcebergCompatV2(parquetFooters3))

      assertUniFormIcebergProtocolAndProperties(id)
    }
  }

  // TODO: update this test when automatically disable V1 is supported when upgrading
  //       from an existing `IcebergCompatV1` table.
  test("Manually Enable V2 and Disable V1 When Upgrading From an IcebergCompatV1 Table") {
    withTempTableAndDir { case (id, loc) =>
      createIcebergCompatV1Table(id, loc)
      insertInitialRowsIntoTable(id)

      val parquetFooters1 = getParquetFooters(spark, id)

      val snapshot1 = DeltaLog.forTable(spark, new TableIdentifier(id)).update()
      assert(
        isIcebergCompatV1Enabled(snapshot1),
        "`IcebergCompatV1` should be enabled for the current table"
      )

      enforceDeltaUniformRequireProperties(id)

      // enable `IcebergCompatV2` for the `IcebergCompatV1` table.
      // note that `IcebergCompatV1` needs to be disabled *manually* as for now.
      executeSql(
        s"""
           | ALTER TABLE $id
           | SET TBLPROPERTIES (
           |   'delta.enableIcebergCompatV1' = 'false',
           |   'delta.enableIcebergCompatV2' = 'true',
           |   'delta.universalFormat.enabledFormats' = 'iceberg'
           | )
           |""".stripMargin
      )

      insertAdditionalRowsIntoTable(id)

      val parquetFooters2 = getParquetFooters(spark, id)
      assertParquetFootersProperties(
        oldParquetFooters = parquetFooters1,
        newParquetFooters = parquetFooters2,
        expectedNumOfOverlappedParquetFiles = 2,
        expectedNumOfAddedParquetFiles = 2,
        isOverlappedIcebergCompatV2 = true,
        isAddedIcebergCompatV2 = true
      )

      val snapshot2 = DeltaLog.forTable(spark, new TableIdentifier(id)).update()
      assert(
        !isIcebergCompatV1Enabled(snapshot2),
        "`IcebergCompatV1` should be disabled after enabling `IcebergCompatV2` by ALTER TABLE"
      )
      assert(
        isIcebergCompatV2Enabled(snapshot2),
        "`IcebergCompatV2` should be enabled after being enabled by ALTER TABLE"
      )

      assertUniFormIcebergProtocolAndProperties(id)
    }
  }

  test("Enabling V1 and V2 At The Same Time For Vanilla Delta Table Should Fail") {
    withTempTableAndDir { case (id, loc) =>
      createVanillaDeltaTableWithDV(id, loc)
      insertInitialRowsIntoTable(id)

      enforceDeltaUniformRequireProperties(id)

      val ex = intercept[DeltaUnsupportedOperationException](
        executeSql(
          s"""
             | ALTER TABLE $id
             | SET TBLPROPERTIES (
             |   'delta.enableIcebergCompatV1' = 'true',
             |   'delta.enableIcebergCompatV2' = 'true',
             |   'delta.universalFormat.enabledFormats' = 'iceberg'
             | )
             |""".stripMargin
        )
      )
      assertResult(
        "DELTA_ICEBERG_COMPAT_VIOLATION.VERSION_MUTUAL_EXCLUSIVE"
      )(ex.getErrorClass)
    }
  }

  test("Enabling V1 and V2 At The Same Time For IcebergCompatV1 Delta Uniform Table Should Fail") {
    withTempTableAndDir { case (id, loc) =>
      createIcebergCompatV1Table(id, loc)
      insertInitialRowsIntoTable(id)

      enforceDeltaUniformRequireProperties(id)

      val ex = intercept[DeltaUnsupportedOperationException](
        executeSql(
          s"""
             | ALTER TABLE $id
             | SET TBLPROPERTIES (
             |   'delta.enableIcebergCompatV1' = 'true',
             |   'delta.enableIcebergCompatV2' = 'true',
             |   'delta.universalFormat.enabledFormats' = 'iceberg'
             | )
             |""".stripMargin
        )
      )
      assertResult(
        "DELTA_ICEBERG_COMPAT_VIOLATION.VERSION_MUTUAL_EXCLUSIVE"
      )(ex.getErrorClass)
    }
  }

  test("Disable Column-Mapping When Enabling IcebergCompatV2 By ALTER TABLE Should Fail") {
    withTempTableAndDir { case (id, loc) =>
      createVanillaDeltaTableWithDV(id, loc)
      insertInitialRowsIntoTable(id)

      enforceDeltaUniformRequireProperties(id)

      // disable column-mapping when enabling `IcebergCompatV2`
      // delta uniform by ALTER TABLE should fail
      val ex = intercept[DeltaUnsupportedOperationException](
        executeSql(
          s"""
             | ALTER TABLE $id
             | SET TBLPROPERTIES (
             |   'delta.columnMapping.mode' = 'none',
             |   'delta.enableIcebergCompatV2' = 'true',
             |   'delta.universalFormat.enabledFormats' = 'iceberg'
             | )
             |""".stripMargin
        )
      )
      assertResult(
        "DELTA_ICEBERG_COMPAT_VIOLATION.WRONG_REQUIRED_TABLE_PROPERTY"
      )(ex.getErrorClass)
    }
  }

  test("Enable UniForm With LIST, MAP, and Column-Mapping Enabled By ALTER TABLE") {
    withTempTableAndDir { case (id, loc) =>
      createDeltaTableWithNestedTypesAndColumnMapping(id, loc)
      insertRowToDeltaTableWithNestedTypesAndColumnMapping(id)

      val parquetFooters1 = getParquetFooters(spark, id)

      // only DV needs to be disabled here
      executeSql(
        s"""
           | ALTER TABLE $id
           | SET TBLPROPERTIES (
           |   'delta.enableDeletionVectors' = 'false'
           | )
           |""".stripMargin
      )

      enableIcebergCompatV2ByAlterTable(id)

      insertRowToDeltaTableWithNestedTypesAndColumnMapping(id)

      val parquetFooters2 = getParquetFooters(spark, id)
      assertParquetFootersProperties(
        oldParquetFooters = parquetFooters1,
        newParquetFooters = parquetFooters2,
        expectedNumOfAddedParquetFiles = 1,
        expectedNumOfOverlappedParquetFiles = 1,
        isOverlappedIcebergCompatV2 = false,
        isAddedIcebergCompatV2 = true
      )
    }
  }
}
