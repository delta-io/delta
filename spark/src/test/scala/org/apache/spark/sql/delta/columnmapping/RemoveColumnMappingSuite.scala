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

import io.delta.tables.DeltaTable

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.schema.DeltaInvariantViolationException
import org.apache.spark.sql.delta.sources.DeltaSQLConf._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * Test removing column mapping from a table.
 */
class RemoveColumnMappingSuite extends RemoveColumnMappingSuiteUtils {

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

  test("table without column mapping enabled") {
    sql(s"""CREATE TABLE $testTableName
           |USING delta
           |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'none')
           |AS SELECT 1 as a
           |""".stripMargin)

    unsetColumnMappingProperty(useUnset = true)
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
      unsetColumnMappingProperty(useUnset = true)
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

  test("remove column mapping from a table with deletion vectors") {
    sql(
      s"""CREATE TABLE $testTableName
         |USING delta
         |TBLPROPERTIES (
         |  '${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name',
         |  '${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key}' = true)
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)
    sql(s"DELETE FROM $testTableName WHERE $logicalColumnName % 2 = 0")
    testRemovingColumnMapping()
  }

  test("remove column mapping from a table with a generated column") {
    // Note: generate expressions are using logical column names and renaming referenced columns
    // is forbidden.
    DeltaTable.create(spark)
      .tableName(testTableName)
      .addColumn(logicalColumnName, "LONG")
      .addColumn(
        DeltaTable.columnBuilder(secondColumn)
          .dataType("LONG")
          .generatedAlwaysAs(s"$logicalColumnName + 1")
          .build())
      .property(DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")
      .execute()
    // Insert data into the table.
    spark.range(totalRows)
      .selectExpr(s"id as $logicalColumnName")
      .writeTo(testTableName)
      .append()
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName = testTableName))
    assert(GeneratedColumn.getGeneratedColumns(deltaLog.update()).head.name == secondColumn)
    testRemovingColumnMapping()
    // Verify the generated column is still there.
    assert(GeneratedColumn.getGeneratedColumns(deltaLog.update()).head.name == secondColumn)
    // Insert more rows.
    spark.range(totalRows)
      .selectExpr(s"id + $totalRows as $logicalColumnName")
      .writeTo(testTableName)
      .append()
    // Verify the generated column values are correct.
    checkAnswer(sql(s"SELECT $logicalColumnName, $secondColumn FROM $testTableName"),
      (0 until totalRows * 2).map(i => Row(i, i + 1)))
  }

  test("column constraints are preserved") {
    // Note: constraints are using logical column names and renaming is forbidden until
    // constraint is dropped.
    sql(
      s"""CREATE TABLE $testTableName
         |USING delta
         |TBLPROPERTIES (
         |  '${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name')
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)
    val constraintName = "secondcolumnaddone"
    val constraintExpr = s"$secondColumn = $logicalColumnName + 1"
    sql(s"ALTER TABLE $testTableName ADD CONSTRAINT " +
      s"$constraintName CHECK ($constraintExpr)")
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName = testTableName))
    assert(deltaLog.update().metadata.configuration(s"delta.constraints.$constraintName") ==
      constraintExpr)
    testRemovingColumnMapping()
    // Verify the constraint is still there.
    assert(deltaLog.update().metadata.configuration(s"delta.constraints.$constraintName") ==
      constraintExpr)
    // Verify the constraint is still enforced.
    intercept[DeltaInvariantViolationException] {
      sql(s"INSERT INTO $testTableName VALUES (0, 0)")
    }
  }

  test("remove column mapping in id mode") {
    sql(
      s"""CREATE TABLE $testTableName
         |USING delta
         |TBLPROPERTIES (
         |  '${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'id')
         |AS SELECT id as $logicalColumnName, id + 1 as $secondColumn
         |  FROM RANGE(0, $totalRows, 1, $numFiles)
         |""".stripMargin)
    testRemovingColumnMapping()
  }
}
