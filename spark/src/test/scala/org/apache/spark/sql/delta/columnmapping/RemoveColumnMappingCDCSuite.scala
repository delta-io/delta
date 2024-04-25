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

import org.apache.spark.sql.delta.DeltaConfigs
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaUnsupportedOperationException

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * Test suite for removing column mapping(CM) from a table with CDC enabled. Test different
 * scenarios with respect to table schema at different points of time. There are a few events we
 * are interested in: UP: enable column mapping DOWN: disable column mapping RE, DROP: rename,
 * drop a column
 *
 * And there are two parameters for reading CDC: START: starting version END: ending version
 *
 * We test all the possible combinations of these events and parameters.
 */
class RemoveColumnMappingCDCSuite extends RemoveColumnMappingSuiteUtils {

  // These two cases will fail because latest schema will be used to read CDC.
  // Table doesn't have column mapping enabled in any of the start, end or latest versions.
  // So it defaults to the non-column mapping behavior.
  runScenario(Start, End, Upgrade, Rename, Downgrade)(ReadCDCIncompatibleDataSchema)
  runScenario(Start, End, Upgrade, Drop, Downgrade)(ReadCDCIncompatibleDataSchema)

  runScenario(Start, End, Upgrade, Rename, Downgrade, Upgrade)(ReadCDCSuccess)
  runScenario(Start, End, Upgrade, Drop, Downgrade, Upgrade)(ReadCDCSuccess)

  // This will use the endVersion schema to read CDC because endVersion has columnampping enabled.
  runScenario(Start, Upgrade, End, Rename, Downgrade)(ReadCDCSuccess)
  runScenario(Start, Upgrade, End, Drop, Downgrade)(ReadCDCSuccess)
  runScenario(Start, Upgrade, End, Rename, Downgrade, Upgrade)(ReadCDCSuccess)
  runScenario(Start, Upgrade, End, Drop, Downgrade, Upgrade)(ReadCDCSuccess)

  // Reading across non-additive schema change.
  runScenario(Start, Upgrade, Rename, End, Downgrade)(ReadCDCIncompatibleSchemaChange)
  runScenario(Start, Upgrade, Drop, End, Downgrade)(ReadCDCIncompatibleSchemaChange)
  runScenario(Start, Upgrade, Rename, End, Downgrade, Upgrade)(ReadCDCIncompatibleSchemaChange)
  runScenario(Start, Upgrade, Drop, End, Downgrade, Upgrade)(ReadCDCIncompatibleSchemaChange)

  runScenario(Start, Upgrade, Rename, Downgrade, End)(ReadCDCIncompatibleSchemaChange)
  runScenario(Start, Upgrade, Drop, Downgrade, End)(ReadCDCIncompatibleSchemaChange)
  runScenario(Start, Upgrade, Rename, Downgrade, End, Upgrade)(ReadCDCIncompatibleSchemaChange)
  runScenario(Start, Upgrade, Drop, Downgrade, End, Upgrade)(ReadCDCIncompatibleSchemaChange)
  runScenario(Start, Upgrade, Rename, Downgrade, Upgrade, End)(ReadCDCIncompatibleSchemaChange)
  runScenario(Start, Upgrade, Drop, Downgrade, Upgrade, End)(ReadCDCIncompatibleSchemaChange)

  runScenario(Upgrade, Start, End, Rename, Downgrade)(ReadCDCSuccess)
  runScenario(Upgrade, Start, End, Drop, Downgrade)(ReadCDCSuccess)
  runScenario(Upgrade, Start, End, Rename, Downgrade, Upgrade)(ReadCDCSuccess)
  runScenario(Upgrade, Start, End, Drop, Downgrade, Upgrade)(ReadCDCSuccess)

  // Reading across non-additive schema change.
  runScenario(Upgrade, Start, Rename, End, Downgrade)(ReadCDCIncompatibleDataSchema)
  runScenario(Upgrade, Start, Drop, End, Downgrade)(ReadCDCIncompatibleDataSchema)
  runScenario(Upgrade, Start, Rename, End, Downgrade, Upgrade)(ReadCDCIncompatibleDataSchema)
  runScenario(Upgrade, Start, Drop, End, Downgrade, Upgrade)(ReadCDCIncompatibleDataSchema)

  // Reading across non-additive schema change.
  runScenario(Upgrade, Start, Rename, Downgrade, End)(ReadCDCIncompatibleSchemaChange)
  runScenario(Upgrade, Start, Drop, Downgrade, End)(ReadCDCIncompatibleSchemaChange)
  runScenario(Upgrade, Start, Rename, Downgrade, End, Upgrade)(ReadCDCIncompatibleSchemaChange)
  runScenario(Upgrade, Start, Drop, Downgrade, End, Upgrade)(ReadCDCIncompatibleSchemaChange)
  runScenario(Upgrade, Start, Rename, Downgrade, Upgrade, End)(ReadCDCIncompatibleSchemaChange)
  runScenario(Upgrade, Start, Drop, Downgrade, Upgrade, End)(ReadCDCIncompatibleDataSchema)

  runScenario(Upgrade, Rename, Start, End, Downgrade)(ReadCDCSuccess)
  runScenario(Upgrade, Drop, Start, End, Downgrade)(ReadCDCSuccess)
  runScenario(Upgrade, Rename, Start, End, Downgrade, Upgrade)(ReadCDCSuccess)
  runScenario(Upgrade, Drop, Start, End, Downgrade, Upgrade)(ReadCDCSuccess)

  // Reading across downgrade.
  runScenario(Upgrade, Rename, Start, Downgrade, End)(ReadCDCIncompatibleDataSchema)
  runScenario(Upgrade, Drop, Start, Downgrade, End)(ReadCDCIncompatibleDataSchema)
  runScenario(Upgrade, Rename, Start, Downgrade, End, Upgrade)(ReadCDCIncompatibleDataSchema)
  runScenario(Upgrade, Drop, Start, Downgrade, End, Upgrade)(ReadCDCIncompatibleDataSchema)
  runScenario(Upgrade, Rename, Start, Downgrade, Upgrade, End)(ReadCDCIncompatibleDataSchema)
  // Schema is readable in this range.
  runScenario(Upgrade, Drop, Start, Downgrade, Upgrade, End)(ReadCDCSuccess)

  runScenario(Upgrade, Rename, Downgrade, Start, End)(ReadCDCSuccess)
  runScenario(Upgrade, Drop, Downgrade, Start, End)(ReadCDCSuccess)
  runScenario(Upgrade, Rename, Downgrade, Start, End, Upgrade)(ReadCDCSuccess)
  runScenario(Upgrade, Drop, Downgrade, Start, End, Upgrade)(ReadCDCSuccess)

  runScenario(Upgrade, Rename, Downgrade, Start, Upgrade, End)(ReadCDCSuccess)
  runScenario(Upgrade, Drop, Downgrade, Start, Upgrade, End)(ReadCDCSuccess)

  runScenario(Upgrade, Rename, Downgrade, Upgrade, Start, End)(ReadCDCSuccess)
  runScenario(Upgrade, Drop, Downgrade, Upgrade, Start, End)(ReadCDCSuccess)

  private def runScenario(operations: Operation*)(readCDC: ReadCDC): Unit = {
    val testName = operations.map { _.toString }.mkString(", ") + " - " + readCDC.toString
    var startVersion = 0L
    var endVersion: Option[Long] = None
    test(testName) {
      createTable()
      operations.foreach {
        case op @ Start =>
          startVersion = deltaLog.update().version
          op.runOperation()
        case op @ End =>
          op.runOperation()
          endVersion = Some(deltaLog.update().version)
        case op => op.runOperation()
      }
      readCDC.runReadCDC(startVersion, endVersion)
    }
  }

  private abstract class Operation {
    def runOperation(): Unit
  }

  private case object Start extends Operation {
    override def runOperation(): Unit = {
      insertMoreRows()
    }
  }

  private case object End extends Operation {
    override def runOperation(): Unit = {
      insertMoreRows()
    }
  }

  private case object Upgrade extends Operation {
    override def runOperation(): Unit = {
      enableColumnMapping()
      insertMoreRows()
    }
  }

  private case object Downgrade extends Operation {
    override def runOperation(): Unit = {
      disableColumnMapping()
      insertMoreRows()
    }
  }

  private case object Rename extends Operation {
    override def runOperation(): Unit = {
      renameColumn()
      insertMoreRows()
    }
  }

  private case object Drop extends Operation {
    override def runOperation(): Unit = {
      dropColumn()
      insertMoreRows()
    }
  }

  private abstract class ReadCDC {
    def runReadCDC(start: Long, end: Option[Long]): Unit
  }

  private case object ReadCDCSuccess extends ReadCDC {
    override def runReadCDC(start: Long, end: Option[Long]): Unit = {
      val changes = getChanges(start, end)
      assert(changes.length > 0, "should have read some changes")
      changes.foreach { row =>
        assert(!row.anyNull, "None of the values should be null")
      }
    }
  }

  private case object ReadCDCIncompatibleSchemaChange extends ReadCDC {
    override def runReadCDC(start: Long, end: Option[Long]): Unit = {
      getCDCAndFailIncompatibleSchemaChange(start, end)
    }
  }

  private case object ReadCDCIncompatibleDataSchema extends ReadCDC {
    override def runReadCDC(start: Long, end: Option[Long]): Unit = {
      getCDCAndFailIncompatibleDataSchema(start, end)
    }
  }

  private val firstColumn = "first_column_name"
  private val thirdColumn = "third_column_name"
  private val renamedThirdColumn = "renamed_third_column_name"

  private def createTable(): Unit = {
    val columnMappingMode = "none"
    sql(s"""CREATE TABLE $testTableName
             |USING delta
             |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = '$columnMappingMode',
             |  '${DeltaConfigs.CHANGE_DATA_FEED.key}' = 'true'
             |)
             |AS SELECT id as $firstColumn, id + 1 as $secondColumn, id + 2 as $thirdColumn
             |  FROM RANGE(0, $totalRows, 1, $numFiles)
             |""".stripMargin)
  }

  private def enableColumnMapping(): Unit = {
    sql(
      s"""ALTER TABLE $testTableName
        SET TBLPROPERTIES (
        '${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5')""")
  }

  private def disableColumnMapping(): Unit = {
    unsetColumnMappingProperty(useUnset = false)
  }

  private def insertMoreRows(): Unit = {
    sql(s"INSERT INTO $testTableName SELECT * FROM $testTableName LIMIT $totalRows")
  }

  private def renameColumn(): Unit = {
    sql(s"ALTER TABLE $testTableName RENAME COLUMN $thirdColumn TO $renamedThirdColumn")
  }

  private def dropColumn(): Unit = {
    sql(s"ALTER TABLE $testTableName DROP COLUMN $thirdColumn")
  }

  private def deltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))

  private def getCDCAndFailIncompatibleSchemaChange(
      startVersion: Long,
      endVersion: Option[Long]) = {
    val e = intercept[DeltaUnsupportedOperationException] {
      getChanges(startVersion, endVersion)
    }
    assert(e.getErrorClass === "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE")
  }

  private def getCDCAndFailIncompatibleDataSchema(
      startVersion: Long,
      endVersion: Option[Long]) = {
    val e = intercept[DeltaUnsupportedOperationException] {
      getChanges(startVersion, endVersion)
    }
    assert(e.getErrorClass === "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA")
  }

  private def getChanges(startVersion: Long, endVersion: Option[Long]): Array[Row] = {
    val endVersionStr = if (endVersion.isDefined) s", ${endVersion.get}" else ""
    sql(s"SELECT * FROM table_changes('$testTableName', $startVersion$endVersionStr)")
      .collect()
  }
}
