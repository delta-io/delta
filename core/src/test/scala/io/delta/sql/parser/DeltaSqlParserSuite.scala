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
import org.apache.spark.sql.delta.commands.OptimizeTableCommand

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{TableIdentifier, TimeTravel}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.CloneTableStatement

class DeltaSqlParserSuite extends SparkFunSuite with SQLHelper {

  test("isValidDecimal should recognize a table identifier and not treat them as a decimal") {
    // Setting `delegate` to `null` is fine. The following tests don't need to touch `delegate`.
    val parser = new DeltaSqlParser(null)
    assert(parser.parsePlan("vacuum 123_") ===
      VacuumTableCommand(None, Some(TableIdentifier("123_")), None, false))
    assert(parser.parsePlan("vacuum 1a.123_") ===
      VacuumTableCommand(None, Some(TableIdentifier("123_", Some("1a"))), None, false))
    assert(parser.parsePlan("vacuum a.123A") ===
      VacuumTableCommand(None, Some(TableIdentifier("123A", Some("a"))), None, false))
    assert(parser.parsePlan("vacuum a.123E3_column") ===
      VacuumTableCommand(None, Some(TableIdentifier("123E3_column", Some("a"))), None, false))
    assert(parser.parsePlan("vacuum a.123D_column") ===
      VacuumTableCommand(None, Some(TableIdentifier("123D_column", Some("a"))), None, false))
    assert(parser.parsePlan("vacuum a.123BD_column") ===
      VacuumTableCommand(None, Some(TableIdentifier("123BD_column", Some("a"))), None, false))
  }

  test("OPTIMIZE command is parsed as expected") {
    val parser = new DeltaSqlParser(null)
    assert(parser.parsePlan("OPTIMIZE tbl") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Seq.empty, Map.empty)(Seq()))

    assert(parser.parsePlan("OPTIMIZE db.tbl") ===
      OptimizeTableCommand(None, Some(tblId("tbl", "db")), Seq.empty, Map.empty)(Seq()))

    assert(parser.parsePlan("OPTIMIZE tbl_${system:spark.testing}") ===
      OptimizeTableCommand(None, Some(tblId("tbl_true")), Seq.empty, Map.empty)(Seq()))

    withSQLConf("tbl_var" -> "tbl") {
      assert(parser.parsePlan("OPTIMIZE ${tbl_var}") ===
        OptimizeTableCommand(None, Some(tblId("tbl")), Seq.empty, Map.empty)(Seq()))

      assert(parser.parsePlan("OPTIMIZE ${spark:tbl_var}") ===
        OptimizeTableCommand(None, Some(tblId("tbl")), Seq.empty, Map.empty)(Seq()))

      assert(parser.parsePlan("OPTIMIZE ${sparkconf:tbl_var}") ===
        OptimizeTableCommand(None, Some(tblId("tbl")), Seq.empty, Map.empty)(Seq()))

      assert(parser.parsePlan("OPTIMIZE ${hiveconf:tbl_var}") ===
        OptimizeTableCommand(None, Some(tblId("tbl")), Seq.empty, Map.empty)(Seq()))

      assert(parser.parsePlan("OPTIMIZE ${hivevar:tbl_var}") ===
        OptimizeTableCommand(None, Some(tblId("tbl")), Seq.empty, Map.empty)(Seq()))
    }

    assert(parser.parsePlan("OPTIMIZE '/path/to/tbl'") ===
      OptimizeTableCommand(Some("/path/to/tbl"), None, Seq.empty, Map.empty)(Seq()))

    assert(parser.parsePlan("OPTIMIZE delta.`/path/to/tbl`") ===
      OptimizeTableCommand(None, Some(tblId("/path/to/tbl", "delta")), Seq.empty, Map.empty)(Seq()))

    assert(parser.parsePlan("OPTIMIZE tbl WHERE part = 1") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Seq("part = 1"), Map.empty)(Seq()))

    assert(parser.parsePlan("OPTIMIZE tbl ZORDER BY (col1)") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Seq.empty, Map.empty)
      (Seq(unresolvedAttr("col1"))))

    assert(parser.parsePlan("OPTIMIZE tbl WHERE part = 1 ZORDER BY col1, col2.subcol") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Seq("part = 1"), Map.empty)(
        Seq(unresolvedAttr("col1"), unresolvedAttr("col2", "subcol"))))

    assert(parser.parsePlan("OPTIMIZE tbl WHERE part = 1 ZORDER BY (col1, col2.subcol)") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Seq("part = 1"), Map.empty)(
        Seq(unresolvedAttr("col1"), unresolvedAttr("col2", "subcol"))))
  }

  test("OPTIMIZE command new tokens are non-reserved keywords") {
    // new keywords: OPTIMIZE, ZORDER
    val parser = new DeltaSqlParser(null)

    // Use the new keywords in table name
    assert(parser.parsePlan("OPTIMIZE optimize") ===
      OptimizeTableCommand(None, Some(tblId("optimize")), Seq.empty, Map.empty)(Seq()))

    assert(parser.parsePlan("OPTIMIZE zorder") ===
      OptimizeTableCommand(None, Some(tblId("zorder")), Seq.empty, Map.empty)(Seq()))

    // Use the new keywords in column name
    assert(parser.parsePlan("OPTIMIZE tbl WHERE zorder = 1 and optimize = 2") ===
      OptimizeTableCommand(None,
        Some(tblId("tbl"))
        , Seq("zorder = 1 and optimize = 2"), Map.empty)(Seq()))

    assert(parser.parsePlan("OPTIMIZE tbl ZORDER BY (optimize, zorder)") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Seq.empty, Map.empty)(
        Seq(unresolvedAttr("optimize"), unresolvedAttr("zorder"))))
  }

  test("CLONE command is parsed as expected") {
    val parser = new DeltaSqlParser(null)

    // Standard shallow clone
    assert {
      parser.parsePlan(CloneTableSQLTestUtils.buildCloneSqlString(
        "t1",
        "t1",
        sourceIsTable = true,
        targetIsTable = true
      )) == CloneTableStatement(
        UnresolvedRelation(tblId("t1")),
        UnresolvedRelation(tblId("t1")),
        ifNotExists = false,
        isReplaceCommand = false,
        isCreateCommand = true,
        tablePropertyOverrides = Map.empty,
        targetLocation = None
      )
    }

    // Path based source table
    assert {
      parser.parsePlan(CloneTableSQLTestUtils.buildCloneSqlString(
        "/path/to/t1",
        "t1",
        targetIsTable = true
      )) == CloneTableStatement(
        UnresolvedRelation(tblId("/path/to/t1", "delta")),
        UnresolvedRelation(tblId("t1")),
        ifNotExists = false,
        isReplaceCommand = false,
        isCreateCommand = true,
        tablePropertyOverrides = Map.empty,
        targetLocation = None
      )
    }

    // REPLACE
    assert {
      parser.parsePlan(CloneTableSQLTestUtils.buildCloneSqlString(
        "t1",
        "t1",
        sourceIsTable = true,
        targetIsTable = true,
        isCreate = false,
        isReplace = true
      )) == CloneTableStatement(
        UnresolvedRelation(tblId("t1")),
        UnresolvedRelation(tblId("t1")),
        ifNotExists = false,
        isReplaceCommand = true,
        isCreateCommand = false,
        tablePropertyOverrides = Map.empty,
        targetLocation = None
      )
    }

    // CREATE OR REPLACE
    assert {
      parser.parsePlan(CloneTableSQLTestUtils.buildCloneSqlString(
        "t1",
        "t1",
        sourceIsTable = true,
        targetIsTable = true,
        isCreate = true,
        isReplace = true
      )) == CloneTableStatement(
        UnresolvedRelation(tblId("t1")),
        UnresolvedRelation(tblId("t1")),
        ifNotExists = false,
        isReplaceCommand = true,
        isCreateCommand = true,
        tablePropertyOverrides = Map.empty,
        targetLocation = None
      )
    }

    // clone with table properties
    assert {
      parser.parsePlan(CloneTableSQLTestUtils.buildCloneSqlString(
        "t1",
        "t1",
        sourceIsTable = true,
        targetIsTable = true,
        tableProperties = Map("a" -> "a")
      )) == CloneTableStatement(
        UnresolvedRelation(tblId("t1")),
        UnresolvedRelation(tblId("t1")),
        ifNotExists = false,
        isReplaceCommand = false,
        isCreateCommand = true,
        tablePropertyOverrides = Map("a" -> "a"),
        targetLocation = None
      )
    }

    // clone with external location
    assert {
      parser.parsePlan(CloneTableSQLTestUtils.buildCloneSqlString(
        "t1",
        "t1",
        sourceIsTable = true,
        targetIsTable = true,
        targetLocation = Some("/new/path")
      )) == CloneTableStatement(
        UnresolvedRelation(tblId("t1")),
        UnresolvedRelation(tblId("t1")),
        ifNotExists = false,
        isReplaceCommand = false,
        isCreateCommand = true,
        tablePropertyOverrides = Map.empty,
        targetLocation = Some("/new/path")
      )
    }

    // clone with time travel
    assert {
      parser.parsePlan(CloneTableSQLTestUtils.buildCloneSqlString(
        "t1",
        "t1",
        sourceIsTable = true,
        targetIsTable = true,
        versionAsOf = Some(1L)
      )) == CloneTableStatement(
        TimeTravel(
          UnresolvedRelation(tblId("t1")),
          None,
          Some(1L),
          Some("sql")),
        UnresolvedRelation(tblId("t1")),
        ifNotExists = false,
        isReplaceCommand = false,
        isCreateCommand = true,
        tablePropertyOverrides = Map.empty,
        targetLocation = None
      )
    }
  }

  private def unresolvedAttr(colName: String*): UnresolvedAttribute = {
    new UnresolvedAttribute(colName)
  }

  private def tblId(tblName: String, schema: String = null): TableIdentifier = {
    if (schema == null) new TableIdentifier(tblName)
    else new TableIdentifier(tblName, Some(schema))
  }
}
