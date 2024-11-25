/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.coordinatedcommits

import org.scalatest.funsuite.AnyFunSuite
import io.delta.kernel.TableIdentifier
import java.util.Optional

import scala.collection.JavaConverters._

class TableDescriptorSuite extends AnyFunSuite {

  test("TableDescriptor should throw NullPointerException for null constructor arguments") {
    assertThrows[NullPointerException] {
      new TableDescriptor(null, Optional.empty(), Map.empty[String, String].asJava)
    }
    assertThrows[NullPointerException] {
      new TableDescriptor("/delta/logPath", null, Map.empty[String, String].asJava)
    }
    assertThrows[NullPointerException] {
      new TableDescriptor("/delta/logPath", Optional.empty(), null)
    }
  }

  test("TableDescriptor should return the correct logPath, tableIdOpt, and tableConf") {
    val logPath = "/delta/logPath"
    val tableIdOpt = Optional.of(new TableIdentifier(Array("catalog", "schema"), "table"))
    val tableConf = Map("key1" -> "value1", "key2" -> "value2").asJava

    val tableDescriptor = new TableDescriptor(logPath, tableIdOpt, tableConf)

    assert(tableDescriptor.getLogPath == logPath)
    assert(tableDescriptor.getTableIdentifierOpt == tableIdOpt)
    assert(tableDescriptor.getTableConf == tableConf)
  }

  test("TableDescriptors with the same values should be equal") {
    val logPath = "/delta/logPath"
    val tableIdOpt = Optional.of(new TableIdentifier(Array("catalog", "schema"), "table"))
    val tableConf = Map("key1" -> "value1", "key2" -> "value2").asJava

    val tableDescriptor1 = new TableDescriptor(logPath, tableIdOpt, tableConf)
    val tableDescriptor2 = new TableDescriptor(logPath, tableIdOpt, tableConf)

    assert(tableDescriptor1 == tableDescriptor2)
    assert(tableDescriptor1.hashCode == tableDescriptor2.hashCode)
  }

  test("TableDescriptor with different values should not be equal") {
    val logPath = "/delta/logPath"
    val tableIdOpt = Optional.of(new TableIdentifier(Array("catalog", "schema"), "table"))
    val tableConf1 = Map("key1" -> "value1").asJava
    val tableConf2 = Map("key1" -> "value2").asJava

    val tableDescriptor1 = new TableDescriptor(logPath, tableIdOpt, tableConf1)
    val tableDescriptor2 = new TableDescriptor(logPath, tableIdOpt, tableConf2)

    assert(tableDescriptor1 != tableDescriptor2)
  }

  test("TableDescriptor toString format") {
    val logPath = "/delta/logPath"
    val tableIdOpt = Optional.of(new TableIdentifier(Array("catalog", "schema"), "table"))
    val tableConf = Map("key1" -> "value1").asJava

    val tableDescriptor = new TableDescriptor(logPath, tableIdOpt, tableConf)
    val expectedString = "TableDescriptor{logPath='/delta/logPath', " +
      "tableIdOpt=Optional[TableIdentifier{catalog.schema.table}], " +
      "tableConf={key1=value1}}"
    assert(tableDescriptor.toString == expectedString)
  }
}
