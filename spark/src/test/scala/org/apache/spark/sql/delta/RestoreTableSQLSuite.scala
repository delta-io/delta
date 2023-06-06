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


import org.apache.spark.sql.{AnalysisException, DataFrame}

/** Restore tests using the SQL. */
class RestoreTableSQLSuite extends RestoreTableSuiteBase {

  override def restoreTableToVersion(
      tblId: String,
      version: Int,
      isTable: Boolean,
      expectNoOp: Boolean = false): DataFrame = {
    val identifier = if (isTable) {
      tblId
    } else {
      s"delta.`$tblId`"
    }
    spark.sql(s"RESTORE TABLE $identifier VERSION AS OF ${version}")
  }

  override def restoreTableToTimestamp(
      tblId: String,
      timestamp: String,
      isTable: Boolean,
      expectNoOp: Boolean = false): DataFrame = {
    val identifier = if (isTable) {
      tblId
    } else {
      s"delta.`$tblId`"
    }
    spark.sql(s"RESTORE $identifier TO TIMESTAMP AS OF '${timestamp}'")
  }

  test("restoring a table that doesn't exist") {
    val ex = intercept[AnalysisException] {
      sql(s"RESTORE TABLE not_exists VERSION AS OF 0")
    }
    assert(ex.getMessage.contains("Table not found")
      || ex.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND"))
  }

  test("restoring a view") {
    withTempView("tmp") {
      sql("CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM range(10)")
      val ex = intercept[AnalysisException] {
        sql(s"RESTORE tmp TO VERSION AS OF 0")
      }
      assert(ex.getMessage.contains("only supported for Delta tables"))
    }
  }

  test("restoring a view over a Delta table") {
    withTable("delta_table") {
      withView("tmp") {
        sql("CREATE TABLE delta_table USING delta AS SELECT * FROM range(10)")
        sql("CREATE VIEW tmp AS SELECT * FROM delta_table")
        val ex = intercept[AnalysisException] {
          sql(s"RESTORE TABLE tmp VERSION AS OF 0")
        }
        assert(ex.getMessage.contains("only supported for Delta tables"))
      }
    }
  }
}


class RestoreTableSQLNameColumnMappingSuite extends RestoreTableSQLSuite
  with DeltaColumnMappingEnableNameMode {

  import testImplicits._

  override protected def runOnlyTests = Seq(
    "path based table",
    "metastore based table"
  )


  test("restore prior to column mapping upgrade should fail") {
    withTempDir { tempDir =>
      val df1 = Seq(1, 2, 3).toDF("id")
      val df2 = Seq(4, 5, 6).toDF("id")

      def deltaLog: DeltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)

      withColumnMappingConf("none") {
        df1.write.format("delta").save(tempDir.getAbsolutePath)
        require(deltaLog.update().version == 0)

        df2.write.format("delta").mode("append").save(tempDir.getAbsolutePath)
        assert(deltaLog.update().version == 1)
      }

      // upgrade to column mapping mode
      sql(
        s"""
           |ALTER TABLE delta.`$tempDir`
           |SET TBLPROPERTIES (
           |  ${DeltaConfigs.COLUMN_MAPPING_MODE.key} = '$columnMappingModeString',
           |  ${DeltaConfigs.MIN_READER_VERSION.key} = '2',
           |  ${DeltaConfigs.MIN_WRITER_VERSION.key} = '5'
           |)
           |""".stripMargin)

      assert(deltaLog.update().version == 2)

      // try restore back to version 1 before column mapping should fail
      intercept[ColumnMappingUnsupportedException] {
        restoreTableToVersion(tempDir.getAbsolutePath, version = 1, isTable = false)
      }
    }
  }

}

