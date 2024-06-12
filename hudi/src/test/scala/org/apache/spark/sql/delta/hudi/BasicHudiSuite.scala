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

package org.apache.spark.sql.delta.hudiShaded

tags.DeltaDVIncompatibleSuite
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

@DeltaDVIncompatibleSuite // Uniform and DVs don't get along
class BasicHudiSuite extends QueryTest with SharedSparkSession {

  private var TMP_DIR: String = "tmp"
  private var testTableName: String = "MyTable"
  private var testTablePath: String = "mytablepath"

  test("basic test - managed table created with SQL") {
    sql(
      s"""CREATE TABLE `$testTableName` (col1 INT) USING DELTA
         |LOCATION '$testTablePath'
         |TBLPROPERTIES (
         |  'delta.universalFormat.enabledFormats' = 'hudi',
         |  'delta.enableDeletionVectors' = 'false'
         |)""".stripMargin)
    sql(s"INSERT INTO `$testTableName` VALUES (123)")
  }

  // TODO: test Hudi conversion when deletion vectors are disabled after table is already created
}
