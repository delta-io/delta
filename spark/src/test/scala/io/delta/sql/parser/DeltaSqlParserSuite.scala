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

package io.delta.sql.parser

import io.delta.tables.execution.VacuumTableCommand

import org.apache.spark.sql.delta.CloneTableSQLTestUtils
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.{UnresolvedPathBasedDeltaTable, UnresolvedPathBasedTable}
import org.apache.spark.sql.delta.commands.{DescribeDeltaDetailCommand, OptimizeTableCommand, DeltaReorgTable}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{TableIdentifier, TimeTravel}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedTable}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.{AlterTableDropFeature, CloneTableStatement, RestoreTableStatement}

class DeltaSqlParserSuite extends SparkFunSuite with SQLHelper {

  test("isValidDecimal should recognize a table identifier and not treat them as a decimal") {
    // Setting `delegate` to `null` is fine. The following tests don't need to touch `delegate`.
    val parser = new DeltaSqlParser(null)
    assert(parser.parsePlan("vacuum 123_") ===
      VacuumTableCommand(UnresolvedTable(Seq("123_"), "VACUUM", None), None, false))
    assert(parser.parsePlan("vacuum 1a.123_") ===
      VacuumTableCommand(UnresolvedTable(Seq("1a", "123_"), "VACUUM", None), None, false))
    assert(parser.parsePlan("vacuum a.123A") ===
      VacuumTableCommand(UnresolvedTable(Seq("a", "123A"), "VACUUM", None), None, false))
    assert(parser.parsePlan("vacuum a.123E3_column") ===
      VacuumTableCommand(UnresolvedTable(Seq("a", "123E3_column"), "VACUUM", None), None, false))
    assert(parser.parsePlan("vacuum a.123D_column") ===
      VacuumTableCommand(UnresolvedTable(Seq("a", "123D_column"), "VACUUM", None),
        None, false))
    assert(parser.parsePlan("vacuum a.123BD_column") ===
      VacuumTableCommand(UnresolvedTable(Seq("a", "123BD_column"), "VACUUM", None),
        None, false))

    assert(parser.parsePlan("vacuum delta.`/tmp/table`") ===
      VacuumTableCommand(UnresolvedTable(Seq("delta", "/tmp/table"), "VACUUM", None),
        None, false))

    assert(parser.parsePlan("vacuum \"/tmp/table\"") ===
      VacuumTableCommand(UnresolvedPathBasedDeltaTable("/tmp/table", "VACUUM"), None, false))
  }

  test("Restore command is parsed as expected") {
    val parser = new DeltaSqlParser(null)
    var parsedCmd = parser.parsePlan("RESTORE catalog_foo.db.tbl TO VERSION AS OF 1;")
    assert(parsedCmd ===
      RestoreTableStatement(TimeTravel(
        UnresolvedRelation(Seq("catalog_foo", "db", "tbl")),
        None,
        Some(1),
        Some("sql"))))

    parsedCmd = parser.parsePlan("RESTORE delta.`/tmp` TO VERSION AS OF 1;")
    assert(parsedCmd ===
      RestoreTableStatement(TimeTravel(
        UnresolvedRelation(Seq("delta", "/tmp")),
        None,
        Some(1),
        Some("sql"))))
  }

  test("OPTIMIZE command is parsed as expected") {
    val parser = new DeltaSqlParser(null)
    var parsedCmd = parser.parsePlan("OPTIMIZE tbl")
    assert(parsedCmd ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Nil)(Nil))
    assert(parsedCmd.asInstanceOf[OptimizeTableCommand].child ===
      UnresolvedTable(Seq("tbl"), "OPTIMIZE", None))

    parsedCmd = parser.parsePlan("OPTIMIZE db.tbl")
    assert(parsedCmd ===
      OptimizeTableCommand(None, Some(tblId("tbl", "db")), Nil)(Nil))
    assert(parsedCmd.asInstanceOf[OptimizeTableCommand].child ===
      UnresolvedTable(Seq("db", "tbl"), "OPTIMIZE", None))

    parsedCmd = parser.parsePlan("OPTIMIZE catalog_foo.db.tbl")
    assert(parsedCmd ===
      OptimizeTableCommand(None, Some(tblId("tbl", "db", "catalog_foo")), Nil)(Nil))
    assert(parsedCmd.asInstanceOf[OptimizeTableCommand].child ===
      UnresolvedTable(Seq("catalog_foo", "db", "tbl"), "OPTIMIZE", None))

    assert(parser.parsePlan("OPTIMIZE tbl_${system:spark.testing}") ===
      OptimizeTableCommand(None, Some(tblId("tbl_true")), Nil)(Nil))

    withSQLConf("tbl_var" -> "tbl") {
      assert(parser.parsePlan("OPTIMIZE ${tbl_var}") ===
        OptimizeTableCommand(None, Some(tblId("tbl")), Nil)(Nil))

      assert(parser.parsePlan("OPTIMIZE ${spark:tbl_var}") ===
        OptimizeTableCommand(None, Some(tblId("tbl")), Nil)(Nil))

      assert(parser.parsePlan("OPTIMIZE ${sparkconf:tbl_var}") ===
        OptimizeTableCommand(None, Some(tblId("tbl")), Nil)(Nil))

      assert(parser.parsePlan("OPTIMIZE ${hiveconf:tbl_var}") ===
        OptimizeTableCommand(None, Some(tblId("tbl")), Nil)(Nil))

      assert(parser.parsePlan("OPTIMIZE ${hivevar:tbl_var}") ===
        OptimizeTableCommand(None, Some(tblId("tbl")), Nil)(Nil))
    }

    parsedCmd = parser.parsePlan("OPTIMIZE '/path/to/tbl'")
    assert(parsedCmd ===
      OptimizeTableCommand(Some("/path/to/tbl"), None, Nil)(Nil))
    assert(parsedCmd.asInstanceOf[OptimizeTableCommand].child ===
      UnresolvedPathBasedDeltaTable("/path/to/tbl", "OPTIMIZE"))

    parsedCmd = parser.parsePlan("OPTIMIZE delta.`/path/to/tbl`")
    assert(parsedCmd ===
      OptimizeTableCommand(None, Some(tblId("/path/to/tbl", "delta")), Nil)(Nil))
    assert(parsedCmd.asInstanceOf[OptimizeTableCommand].child ===
      UnresolvedTable(Seq("delta", "/path/to/tbl"), "OPTIMIZE", None))

    assert(parser.parsePlan("OPTIMIZE tbl WHERE part = 1") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Seq("part = 1"))(Nil))

    assert(parser.parsePlan("OPTIMIZE tbl ZORDER BY (col1)") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Nil)
      (Seq(unresolvedAttr("col1"))))

    assert(parser.parsePlan("OPTIMIZE tbl WHERE part = 1 ZORDER BY col1, col2.subcol") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Seq("part = 1"))(
        Seq(unresolvedAttr("col1"), unresolvedAttr("col2", "subcol"))))

    assert(parser.parsePlan("OPTIMIZE tbl WHERE part = 1 ZORDER BY (col1, col2.subcol)") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Seq("part = 1"))(
        Seq(unresolvedAttr("col1"), unresolvedAttr("col2", "subcol"))))
  }

  test("OPTIMIZE command new tokens are non-reserved keywords") {
    // new keywords: OPTIMIZE, ZORDER
    val parser = new DeltaSqlParser(null)

    // Use the new keywords in table name
    assert(parser.parsePlan("OPTIMIZE optimize") ===
      OptimizeTableCommand(None, Some(tblId("optimize")), Nil)(Nil))

    assert(parser.parsePlan("OPTIMIZE zorder") ===
      OptimizeTableCommand(None, Some(tblId("zorder")), Nil)(Nil))

    // Use the new keywords in column name
    assert(parser.parsePlan("OPTIMIZE tbl WHERE zorder = 1 and optimize = 2") ===
      OptimizeTableCommand(None,
        Some(tblId("tbl"))
        , Seq("zorder = 1 and optimize = 2"))(Nil))

    assert(parser.parsePlan("OPTIMIZE tbl ZORDER BY (optimize, zorder)") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Nil)(
        Seq(unresolvedAttr("optimize"), unresolvedAttr("zorder"))))
  }

  test("DESCRIBE DETAIL command is parsed as expected") {
    val parser = new DeltaSqlParser(null)

    // Desc detail on a table
    assert(parser.parsePlan("DESCRIBE DETAIL catalog_foo.db.tbl") ===
      DescribeDeltaDetailCommand(
        UnresolvedTable(Seq("catalog_foo", "db", "tbl"), DescribeDeltaDetailCommand.CMD_NAME, None),
        Map.empty))

    // Desc detail on a raw path
    assert(parser.parsePlan("DESCRIBE DETAIL \"/tmp/table\"") ===
      DescribeDeltaDetailCommand(
        UnresolvedPathBasedTable("/tmp/table", DescribeDeltaDetailCommand.CMD_NAME),
        Map.empty))

    // Desc detail on a delta raw path
    assert(parser.parsePlan("DESCRIBE DETAIL delta.`dummy_raw_path`") ===
      DescribeDeltaDetailCommand(
        UnresolvedTable(Seq("delta", "dummy_raw_path"), DescribeDeltaDetailCommand.CMD_NAME, None),
        Map.empty))
  }

  private def targetPlanForTable(tableParts: String*): UnresolvedTable =
    UnresolvedTable(tableParts.toSeq, "REORG", relationTypeMismatchHint = None)

  test("REORG command is parsed as expected") {
    val parser = new DeltaSqlParser(null)

    assert(parser.parsePlan("REORG TABLE tbl APPLY (PURGE)") ===
      DeltaReorgTable(targetPlanForTable("tbl"))(Nil))

    assert(parser.parsePlan("REORG TABLE tbl_${system:spark.testing} APPLY (PURGE)") ===
      DeltaReorgTable(targetPlanForTable("tbl_true"))(Nil))

    withSQLConf("tbl_var" -> "tbl") {
      assert(parser.parsePlan("REORG TABLE ${tbl_var} APPLY (PURGE)") ===
        DeltaReorgTable(targetPlanForTable("tbl"))(Nil))

      assert(parser.parsePlan("REORG TABLE ${spark:tbl_var} APPLY (PURGE)") ===
        DeltaReorgTable(targetPlanForTable("tbl"))(Nil))

      assert(parser.parsePlan("REORG TABLE ${sparkconf:tbl_var} APPLY (PURGE)") ===
        DeltaReorgTable(targetPlanForTable("tbl"))(Nil))

      assert(parser.parsePlan("REORG TABLE ${hiveconf:tbl_var} APPLY (PURGE)") ===
        DeltaReorgTable(targetPlanForTable("tbl"))(Nil))

      assert(parser.parsePlan("REORG TABLE ${hivevar:tbl_var} APPLY (PURGE)") ===
        DeltaReorgTable(targetPlanForTable("tbl"))(Nil))
    }

    assert(parser.parsePlan("REORG TABLE delta.`/path/to/tbl` APPLY (PURGE)") ===
      DeltaReorgTable(targetPlanForTable("delta", "/path/to/tbl"))(Nil))

    assert(parser.parsePlan("REORG TABLE tbl WHERE part = 1 APPLY (PURGE)") ===
      DeltaReorgTable(targetPlanForTable("tbl"))(Seq("part = 1")))
  }

  test("REORG command new tokens are non-reserved keywords") {
    // new keywords: REORG, APPLY, PURGE
    val parser = new DeltaSqlParser(null)

    // Use the new keywords in table name
    assert(parser.parsePlan("REORG TABLE reorg APPLY (PURGE)") ===
      DeltaReorgTable(targetPlanForTable("reorg"))(Nil))
    assert(parser.parsePlan("REORG TABLE apply APPLY (PURGE)") ===
      DeltaReorgTable(targetPlanForTable("apply"))(Nil))
    assert(parser.parsePlan("REORG TABLE purge APPLY (PURGE)") ===
      DeltaReorgTable(targetPlanForTable("purge"))(Nil))

    // Use the new keywords in column name
    assert(parser.parsePlan(
      "REORG TABLE tbl WHERE reorg = 1 AND apply = 2 AND purge = 3 APPLY (PURGE)") ===
      DeltaReorgTable(targetPlanForTable("tbl"))(Seq("reorg = 1 AND apply =2 AND purge = 3")))
  }

  // scalastyle:off argcount
  private def checkCloneStmt(
      parser: DeltaSqlParser,
      source: String,
      target: String,
      sourceFormat: String = "delta",
      sourceIsTable: Boolean = true,
      sourceIs3LTable: Boolean = false,
      targetIsTable: Boolean = true,
      targetLocation: Option[String] = None,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None,
      isCreate: Boolean = true,
      isReplace: Boolean = false,
      tableProperties: Map[String, String] = Map.empty): Unit = {
    assert {
      parser.parsePlan(CloneTableSQLTestUtils.buildCloneSqlString(
        source,
        target,
        sourceIsTable,
        targetIsTable,
        sourceFormat,
        targetLocation = targetLocation,
        versionAsOf = versionAsOf,
        timestampAsOf = timestampAsOf,
        isCreate = isCreate,
        isReplace = isReplace,
        tableProperties = tableProperties
      )) == {
        val sourceRelation = if (sourceIs3LTable) {
          new UnresolvedRelation(source.split('.'))
        } else {
          UnresolvedRelation(tblId(source, if (sourceIsTable) null else sourceFormat))
        }
        CloneTableStatement(
          if (versionAsOf.isEmpty && timestampAsOf.isEmpty) {
            sourceRelation
          } else {
            TimeTravel(
              sourceRelation,
              timestampAsOf.map(Literal(_)),
              versionAsOf,
              Some("sql"))
          },
          new UnresolvedRelation(target.split('.')),
          ifNotExists = false,
          isReplaceCommand = isReplace,
          isCreateCommand = isCreate,
          tablePropertyOverrides = tableProperties,
          targetLocation = targetLocation
        )
      }
    }
  }
  // scalastyle:on argcount

  test("CLONE command is parsed as expected") {
    val parser = new DeltaSqlParser(null)
    // Standard shallow clone
    checkCloneStmt(parser, source = "t1", target = "t1")
    // Path based source table
    checkCloneStmt(parser, source = "/path/to/t1", target = "t1", sourceIsTable = false)
    // REPLACE
    checkCloneStmt(parser, source = "t1", target = "t1", isCreate = false, isReplace = true)
    // CREATE OR REPLACE
    checkCloneStmt(parser, source = "t1", target = "t1", isCreate = true, isReplace = true)
    // Clone with table properties
    checkCloneStmt(parser, source = "t1", target = "t1", tableProperties = Map("a" -> "a"))
    // Clone with external location
    checkCloneStmt(parser, source = "t1", target = "t1", targetLocation = Some("/new/path"))
    // Clone with time travel
    checkCloneStmt(parser, source = "t1", target = "t1", versionAsOf = Some(1L))
    // Clone with 3L table (only useful for Iceberg table now)
    checkCloneStmt(parser, source = "local.iceberg.table", target = "t1", sourceIs3LTable = true)
    checkCloneStmt(parser, source = "local.iceberg.table", target = "delta.table",
      sourceIs3LTable = true)
    // Custom source format with path
    checkCloneStmt(parser, source = "/path/to/iceberg", target = "t1", sourceFormat = "iceberg",
      sourceIsTable = false)

    // Target table with 3L name
    checkCloneStmt(parser, source = "/path/to/iceberg", target = "a.b.t1", sourceFormat = "iceberg",
      sourceIsTable = false)
    checkCloneStmt(
      parser, source = "spark_catalog.tmp.table", target = "a.b.t1", sourceIs3LTable = true)
    checkCloneStmt(parser, source = "t2", target = "a.b.t1")
  }

  for (truncateHistory <- Seq(true, false))
  test(s"DROP FEATURE command is parsed as expected - truncateHistory: $truncateHistory") {
    val parser = new DeltaSqlParser(null)
    val table = "tbl"
    val featureName = "feature_name"
    val sql = s"ALTER TABLE $table DROP FEATURE $featureName " +
      (if (truncateHistory) "TRUNCATE HISTORY" else "")
    val parsedCmd = parser.parsePlan(sql)
    assert(parsedCmd ===
      AlterTableDropFeature(
        UnresolvedTable(Seq(table), "ALTER TABLE ... DROP FEATURE", None),
        featureName,
        truncateHistory))
  }

  private def unresolvedAttr(colName: String*): UnresolvedAttribute = {
    new UnresolvedAttribute(colName)
  }

  private def tblId(
      tblName: String,
      schema: String = null,
      catalog: String = null): TableIdentifier = {
    if (catalog == null) {
      if (schema == null) new TableIdentifier(tblName)
      else new TableIdentifier(tblName, Some(schema))
    } else {
      assert(schema != null)
      new TableIdentifier(tblName, Some(schema), Some(catalog))
    }
  }
}
