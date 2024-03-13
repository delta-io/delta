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

package org.apache.spark.sql.delta.columnmapping

import org.apache.spark.sql.delta.DeltaAnalysisException
import org.apache.spark.sql.delta.DeltaColumnMappingUnsupportedException
import org.apache.spark.sql.delta.DeltaConfigs
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.sources.DeltaSQLConf._

import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * Test removing column mapping from a table.
 */
class RemoveColumnMappingSuite
  extends RemoveColumnMappingSuiteUtils
  {

  test("column mapping cannot be removed without the feature flag") {
    withSQLConf(ALLOW_COLUMN_MAPPING_REMOVAL.key -> "false") {
      sql(s"""CREATE TABLE $testTableName
         |USING delta
         |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name')
         |AS SELECT 1 as a
         |""".stripMargin)

      intercept[DeltaColumnMappingUnsupportedException] {
        sql(s"""
             |ALTER TABLE $testTableName
             |SET TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'none')
             |""".stripMargin)
      }
    }
  }

  test("invalid column names") {
    val invalidColName1 = colName("col1")
    val invalidColName2 = colName("col2")
    sql(
      s"""CREATE TABLE $testTableName (a INT, `$invalidColName1` INT, `$invalidColName2` INT)
         |USING delta
         |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
         |""".stripMargin)
    val e = intercept[DeltaAnalysisException] {
      // Try to remove column mapping.
      sql(s"ALTER TABLE $testTableName SET TBLPROPERTIES ('delta.columnMapping.mode' = 'none')")
    }
    assert(e.errorClass
      .contains("DELTA_INVALID_COLUMN_NAMES_WHEN_REMOVING_COLUMN_MAPPING"))
    assert(e.getMessageParametersArray === Array(invalidColName1, invalidColName2))
  }

  test("ALTER TABLE with multiple table properties") {
    sql(
      s"""CREATE TABLE $testTableName (a INT, b INT, c INT)
         |USING delta
         |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
         |""".stripMargin)
    // Remove column mapping and set another property.
    val myProperty = ("acme", "1234")
    sql(s"ALTER TABLE $testTableName SET TBLPROPERTIES " +
      s"('delta.columnMapping.mode' = 'none', '${myProperty._1}' = '${myProperty._2}')")
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName = testTableName))
    assert(deltaLog.update().metadata.configuration.get(myProperty._1).contains(myProperty._2))
  }

  test("ALTER TABLE UNSET column mapping") {
    val propertyToKeep = "acme"
    val propertyToUnset = "acme2"
    sql(
      s"""CREATE TABLE $testTableName (a INT, b INT, c INT)
         |USING delta
         |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name',
         |'$propertyToKeep' = '1234', '$propertyToUnset' = '1234')
         |""".stripMargin)
    sql(s"ALTER TABLE $testTableName UNSET TBLPROPERTIES " +
      s"('delta.columnMapping.mode', '$propertyToKeep')")
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName = testTableName))
    assert(!deltaLog.update()
      .metadata.configuration.contains(DeltaConfigs.COLUMN_MAPPING_MODE.key))
    assert(!deltaLog.update().metadata.configuration.contains(propertyToKeep))
    assert(deltaLog.update().metadata.configuration.contains(propertyToUnset))
  }

  test("ALTER TABLE UNSET column mapping with invalid column names") {
    val invalidColName1 = colName("col1")
    val invalidColName2 = colName("col2")
    val propertyToKeep = "acme"
    val propertyToUnset = "acme2"
    sql(
      s"""CREATE TABLE $testTableName (a INT, `$invalidColName1` INT, `$invalidColName2` INT)
         |USING delta
         |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name',
         |'$propertyToKeep' = '1234', '$propertyToUnset' = '1234')
         |""".stripMargin)
    val e = intercept[DeltaAnalysisException] {
      // Try to remove column mapping.
      sql(s"ALTER TABLE $testTableName UNSET TBLPROPERTIES " +
        s"('delta.columnMapping.mode', '$propertyToKeep')")
    }
    assert(e.errorClass
      .contains("DELTA_INVALID_COLUMN_NAMES_WHEN_REMOVING_COLUMN_MAPPING"))
    assert(e.getMessageParametersArray === Array(invalidColName1, invalidColName2))
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName = testTableName))
    // Column mapping property should stay the same.
    assert(deltaLog.update()
      .metadata.configuration.contains(DeltaConfigs.COLUMN_MAPPING_MODE.key))
    // Both other properties should stay the same.
    assert(deltaLog.update().metadata.configuration.contains(propertyToKeep))
    assert(deltaLog.update().metadata.configuration.contains(propertyToUnset))
  }

  test("remove column mapping from a table") {
    sql(
      s"""CREATE TABLE $testTableName
         |USING delta
         |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name')
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)
    testRemovingColumnMapping()
  }

  test("remove column mapping using unset") {
    sql(
      s"""CREATE TABLE $testTableName
         |USING delta
         |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name')
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)
    testRemovingColumnMapping(unsetTableProperty = true)
  }

  test("remove column mapping from a partitioned table") {
    sql(
      s"""CREATE TABLE $testTableName
         |USING delta
         |PARTITIONED BY (part)
         |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name')
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn, id % 2 as part
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)
    testRemovingColumnMapping()
  }

  test("remove column mapping from a partitioned table with two part columns") {
    sql(
      s"""CREATE TABLE $testTableName
         |USING delta
         |PARTITIONED BY (part1, part2)
         |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name')
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn, id % 2 as part1,
         |id % 3 as part2
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)
    testRemovingColumnMapping()
  }

  test("remove column mapping from a table with only logical names") {
    sql(
      s"""CREATE TABLE $testTableName
         |USING delta
         |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'none')
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)
    // Add column mapping without renaming any columns.
    // That is, the column names in the table should be the same as the logical column names.
    sql(
      s"""ALTER TABLE $testTableName
         |SET TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name',
         'delta.minReaderVersion' = '2',
         'delta.minWriterVersion' = '5'
         |)""".stripMargin)
    testRemovingColumnMapping()
  }

  test("dropped column is added back") {
    sql(
      s"""CREATE TABLE $testTableName
         |USING delta
         |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'none')
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)
    // Add column mapping without renaming any columns.
    // That is, the column names in the table should be the same as the logical column names.
    sql(
      s"""ALTER TABLE $testTableName
         |SET TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name',
         'delta.minReaderVersion' = '2',
         'delta.minWriterVersion' = '5'
         |)""".stripMargin)
    // Drop the second column.
    sql(s"ALTER TABLE $testTableName DROP COLUMN $secondColumn")
    // Remove column mapping, this should rewrite the table to physically remove the dropped column.
    testRemovingColumnMapping()
    // Add the same column back.
    sql(s"ALTER TABLE $testTableName ADD COLUMN $secondColumn BIGINT")
    // Read from the table, ensure none of the original values of secondColumn are present.
    assert(sql(s"SELECT $secondColumn FROM $testTableName WHERE $secondColumn IS NOT NULL").count()
      == 0)
  }
}
