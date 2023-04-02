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

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.util.Utils

class CloneTableSQLSuite extends CloneTableSuiteBase
  with DeltaColumnMappingTestUtils
{
  // scalastyle:off argcount
  override protected def cloneTable(
      source: String,
      target: String,
      sourceIsTable: Boolean = false,
      targetIsTable: Boolean = false,
      sourceFormat: String = "delta",
      targetLocation: Option[String] = None,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None,
      isCreate: Boolean = true,
      isReplace: Boolean = false,
      tableProperties: Map[String, String] = Map.empty): Unit = {
    val commandSql = CloneTableSQLTestUtils.buildCloneSqlString(
      source, target,
      sourceIsTable, targetIsTable,
      sourceFormat, targetLocation,
      versionAsOf, timestampAsOf,
      isCreate, isReplace, tableProperties)
    sql(commandSql)
  }
  // scalastyle:on argcount

  testAllClones(s"table version as of syntax") { (_, target, isShallow) =>
    val tbl = "source"
    testSyntax(
      tbl,
      target,
      s"CREATE TABLE delta.`$target` ${cloneTypeStr(isShallow)} CLONE $tbl VERSION AS OF 0"
    )
  }

  testAllClones("CREATE OR REPLACE syntax when there is no existing table") {
    (_, clone, isShallow) =>
      val tbl = "source"
      testSyntax(
        tbl,
        clone,
        s"CREATE OR REPLACE TABLE delta.`$clone` ${cloneTypeStr(isShallow)} CLONE $tbl"
      )
  }

  cloneTest("REPLACE cannot be used with IF NOT EXISTS") { (shallow, _) =>
    val tbl = "source"
    intercept[ParseException] {
      testSyntax(tbl, shallow,
        s"CREATE OR REPLACE TABLE IF NOT EXISTS delta.`$shallow` SHALLOW CLONE $tbl")
    }
    intercept[ParseException] {
      testSyntax(tbl, shallow,
        s"REPLACE TABLE IF NOT EXISTS delta.`$shallow` SHALLOW CLONE $tbl")
    }
  }

  testAllClones(
    "IF NOT EXISTS should not go through with CLONE if table exists") { (tblExt, _, isShallow) =>
    val sourceTable = "source"
    val conflictingTable = "conflict"
    withTable(sourceTable, conflictingTable) {
      sql(s"CREATE TABLE $conflictingTable " +
        s"USING PARQUET LOCATION '$tblExt' TBLPROPERTIES ('abc'='def', 'def'='ghi') AS SELECT 1")
      spark.range(5).write.format("delta").saveAsTable(sourceTable)

      sql(s"CREATE TABLE IF NOT EXISTS " +
        s"$conflictingTable ${cloneTypeStr(isShallow)} CLONE $sourceTable")

      checkAnswer(sql(s"SELECT COUNT(*) FROM $conflictingTable"), Row(1))
    }
  }

  testAllClones("IF NOT EXISTS should throw an error if path exists") { (_, target, isShallow) =>
    spark.range(5).write.format("delta").save(target)

    val ex = intercept[AnalysisException] {
      sql(s"CREATE TABLE IF NOT EXISTS " +
        s"delta.`$target` ${cloneTypeStr(isShallow)} CLONE delta.`$target`")
    }

    assert(ex.getMessage.contains("is not empty"))
  }

  cloneTest("Negative test: REPLACE table where there is no existing table") { (shallow, _) =>
    val tbl = "source"
    val ex = intercept[AnalysisException] {
      testSyntax(tbl, shallow, s"REPLACE TABLE delta.`$shallow` SHALLOW CLONE $tbl")
    }

    assert(ex.getMessage.contains("cannot be replaced as it does not exist."))
  }

  cloneTest("cloning a table that doesn't exist") { (tblExt, _) =>
    val ex = intercept[AnalysisException] {
      sql(s"CREATE TABLE delta.`$tblExt` SHALLOW CLONE not_exists")
    }
    assert(ex.getMessage.contains("Table not found"))

    val ex2 = intercept[AnalysisException] {
      sql(s"CREATE TABLE delta.`$tblExt` SHALLOW CLONE not_exists VERSION AS OF 0")
    }
    assert(ex2.getMessage.contains("Table not found"))
  }

  cloneTest("cloning a view") { (tblExt, _) =>
    withTempView("tmp") {
      sql("CREATE OR REPLACE TEMP VIEW tmp AS SELECT * FROM range(10)")
      val ex = intercept[AnalysisException] {
        sql(s"CREATE TABLE delta.`$tblExt` SHALLOW CLONE tmp")
      }
      assert(ex.errorClass === Some("DELTA_CLONE_UNSUPPORTED_SOURCE"))
      assert(ex.getMessage.contains("clone source 'tmp', whose format is View."))
    }
  }

  cloneTest("cloning a view over a Delta table") { (tblExt, _) =>
    withTable("delta_table") {
      withView("tmp") {
        sql("CREATE TABLE delta_table USING delta AS SELECT * FROM range(10)")
        sql("CREATE VIEW tmp AS SELECT * FROM delta_table")
        val ex = intercept[AnalysisException] {
          sql(s"CREATE TABLE delta.`$tblExt` SHALLOW CLONE tmp")
        }
        assert(ex.errorClass === Some("DELTA_CLONE_UNSUPPORTED_SOURCE"))
        assert(
          ex.getMessage.contains("clone source") &&
            ex.getMessage.contains("default.tmp', whose format is View.")
        )
      }
    }
  }

  cloneTest("check metrics returned from shallow clone", TAG_HAS_SHALLOW_CLONE) { (_, _) =>
    val source = "source"
    val target = "target"
    withTable(source, target) {
      spark.range(100).write.format("delta").saveAsTable(source)

      val res = sql(s"CREATE TABLE $target SHALLOW CLONE $source")

      // schema check
      val expectedColumns = Seq(
        "source_table_size",
        "source_num_of_files",
        "num_removed_files",
        "num_copied_files",
        "removed_files_size",
        "copied_files_size"
      )
      assert(expectedColumns == res.columns.toSeq)

      // logic check
      assert(res.count() == 1)
      val returnedMetrics = res.first()
      assert(returnedMetrics.getAs[Long]("source_table_size") != 0L)
      assert(returnedMetrics.getAs[Long]("source_num_of_files") != 0L)
      // Delta-OSS doesn't support copied file metrics
      assert(returnedMetrics.getAs[Long]("num_copied_files") == 0L)
      assert(returnedMetrics.getAs[Long]("copied_files_size") == 0L)
    }
  }

  cloneTest("Negative test: Clone to target path and also have external location") { (deep, ext) =>
    val sourceTable = "source"
    withTable(sourceTable) {
      spark.range(5).write.format("delta").saveAsTable(sourceTable)
      val ex = intercept[IllegalArgumentException] {
        runAndValidateClone(
          sourceTable,
          deep,
          sourceIsTable = true,
          targetLocation = Some(ext))()
      }

      assert(ex.getMessage.contains("Two paths were provided as the CLONE target"))
    }
  }
}


class CloneTableSQLIdColumnMappingSuite
  extends CloneTableSQLSuite
    with CloneTableColumnMappingSuiteBase
    with DeltaColumnMappingEnableIdMode {
}

class CloneTableSQLNameColumnMappingSuite
  extends CloneTableSQLSuite
    with CloneTableColumnMappingNameSuiteBase
    with DeltaColumnMappingEnableNameMode {
}

object CloneTableSQLTestUtils {

  // scalastyle:off argcount
  def buildCloneSqlString(
      source: String,
      target: String,
      sourceIsTable: Boolean = false,
      targetIsTable: Boolean = false,
      sourceFormat: String = "delta",
      targetLocation: Option[String] = None,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None,
      isCreate: Boolean = true,
      isReplace: Boolean = false,
      tableProperties: Map[String, String] = Map.empty): String = {
    val header = if (isCreate && isReplace) {
      "CREATE OR REPLACE"
    } else if (isReplace) {
      "REPLACE"
    } else {
      "CREATE"
    }
    // e.g. CREATE TABLE targetTable
    val createTbl =
      if (targetIsTable) s"$header TABLE $target" else s"$header TABLE delta.`$target`"
    // e.g. CREATE TABLE targetTable SHALLOW CLONE
    val withMethod =
        createTbl + " SHALLOW CLONE "
    // e.g. CREATE TABLE targetTable SHALLOW CLONE delta.`/source/table`
    val withSource = if (sourceIsTable) {
      withMethod + s"$source "
    } else {
      withMethod + s"$sourceFormat.`$source` "
    }
    // e.g. CREATE TABLE targetTable SHALLOW CLONE delta.`/source/table` VERSION AS OF 0
    val withVersion = if (versionAsOf.isDefined) {
      withSource + s"VERSION AS OF ${versionAsOf.get}"
    } else if (timestampAsOf.isDefined) {
      withSource + s"TIMESTAMP AS OF '${timestampAsOf.get}'"
    } else {
      withSource
    }
    // e.g. CREATE TABLE targetTable SHALLOW CLONE delta.`/source/table` VERSION AS OF 0
    //      LOCATION '/desired/target/location'
    val withLocation = if (targetLocation.isDefined) {
      s" $withVersion LOCATION '${targetLocation.get}'"
    } else {
      withVersion
    }
    val withProperties = if (tableProperties.nonEmpty) {
      val props = tableProperties.map(p => s"'${p._1}' = '${p._2}'").mkString(",")
      s" $withLocation TBLPROPERTIES ($props)"
    } else {
      withLocation
    }
    withProperties
  }
  // scalastyle:on argcount
}
