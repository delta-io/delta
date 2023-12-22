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

package org.apache.spark.sql.delta.uniform

import org.apache.spark.sql.Row

abstract class UniFormE2EIcebergSuiteBase extends UniFormE2ETest {

  val testTableName = "delta_table"

  test("Basic Insert") {
    withTable(testTableName) {
      write(
        s"""CREATE TABLE $testTableName (col1 INT) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      write(s"INSERT INTO $testTableName VALUES (123)")
      readAndVerify(testTableName, "col1", "col1", Seq(Row(123)))
    }
  }

  test("CIUD") {
    withTable(testTableName) {
      write(
        s"""CREATE TABLE `$testTableName` (col1 INT) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      write(s"INSERT INTO `$testTableName` VALUES (123),(456),(567),(331)")
      write(s"UPDATE `$testTableName` SET col1 = 191 WHERE col1 = 567")
      write(s"DELETE FROM `$testTableName` WHERE col1 = 456")

      readAndVerify(testTableName, "col1", "col1", Seq(Row(123), Row(191), Row(331)))
    }
  }
}
