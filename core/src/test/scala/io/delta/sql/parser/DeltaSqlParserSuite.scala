/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier

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
}
