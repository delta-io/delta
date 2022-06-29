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

import org.apache.spark.sql.delta.commands.OptimizeTableCommand

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

class DeltaSqlParserSuite extends SparkFunSuite {

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
      OptimizeTableCommand(None, Some(tblId("tbl")), None)(Seq()))

    assert(parser.parsePlan("OPTIMIZE db.tbl") ===
      OptimizeTableCommand(None, Some(tblId("tbl", "db")), None)(Seq()))

    assert(parser.parsePlan("OPTIMIZE '/path/to/tbl'") ===
      OptimizeTableCommand(Some("/path/to/tbl"), None, None)(Seq()))

    assert(parser.parsePlan("OPTIMIZE delta.`/path/to/tbl`") ===
      OptimizeTableCommand(None, Some(tblId("/path/to/tbl", "delta")), None)(Seq()))

    assert(parser.parsePlan("OPTIMIZE tbl WHERE part = 1") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Some("part = 1"))(Seq()))

    assert(parser.parsePlan("OPTIMIZE tbl ZORDER BY (col1)") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), None)(Seq(unresolvedAttr("col1"))))

    assert(parser.parsePlan("OPTIMIZE tbl WHERE part = 1 ZORDER BY col1, col2.subcol") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Some("part = 1"))(
        Seq(unresolvedAttr("col1"), unresolvedAttr("col2", "subcol"))))

    assert(parser.parsePlan("OPTIMIZE tbl WHERE part = 1 ZORDER BY (col1, col2.subcol)") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Some("part = 1"))(
        Seq(unresolvedAttr("col1"), unresolvedAttr("col2", "subcol"))))
  }

  test("OPTIMIZE command new tokens are non-reserved keywords") {
    // new keywords: OPTIMIZE, ZORDER
    val parser = new DeltaSqlParser(null)

    // Use the new keywords in table name
    assert(parser.parsePlan("OPTIMIZE optimize") ===
      OptimizeTableCommand(None, Some(tblId("optimize")), None)(Seq()))

    assert(parser.parsePlan("OPTIMIZE zorder") ===
      OptimizeTableCommand(None, Some(tblId("zorder")), None)(Seq()))

    // Use the new keywords in column name
    assert(parser.parsePlan("OPTIMIZE tbl WHERE zorder = 1 and optimize = 2") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), Some("zorder = 1 and optimize = 2"))(Seq()))

    assert(parser.parsePlan("OPTIMIZE tbl ZORDER BY (optimize, zorder)") ===
      OptimizeTableCommand(None, Some(tblId("tbl")), None)(
        Seq(unresolvedAttr("optimize"), unresolvedAttr("zorder"))))
  }

  private def unresolvedAttr(colName: String*): UnresolvedAttribute = {
    new UnresolvedAttribute(colName)
  }

  private def tblId(tblName: String, schema: String = null): TableIdentifier = {
    if (schema == null) new TableIdentifier(tblName)
    else new TableIdentifier(tblName, Some(schema))
  }
}
