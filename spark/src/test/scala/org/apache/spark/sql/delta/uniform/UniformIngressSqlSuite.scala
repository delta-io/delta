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

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.delta.commands.CreateUniformTableCommand
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

class UniformIngressSqlSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  override protected def sparkConf: SparkConf = super.sparkConf

  /**
   * Helper function to test the sql parsing for create uniform table command.
   *
   * @param sql the original sql string.
   * @param targetTable the uniform table name to create.
   * @param ifNotExists whether this command includes `IF NOT EXISTS`.
   * @param isReplace whether this command is a `REPLACE` command.
   * @param isCreate whether this command is a `CREATE ` command.
   * @param fileFormat the file format for the uniform table, e.g., iceberg.
   * @param metadataPath the path for the metadata, e.g., the s3 bucket path.
   */
  private def parseAndValidate(
      sql: String,
      targetTable: String,
      ifNotExists: Boolean,
      isReplace: Boolean,
      isCreate: Boolean,
      fileFormat: String,
      metadataPath: String): Unit = {
    val target = new UnresolvedRelation(Seq(targetTable))
    // TODO(Zihao): add support for refresh command.
    val result = CreateUniformTableCommand(
      table = target,
      ifNotExists = ifNotExists,
      isReplace = isReplace,
      isCreate = isCreate,
      fileFormat = fileFormat,
      metadataPath = metadataPath
      )
    assert(spark.sessionState.sqlParser.parsePlan(sql) == result)
  }

  test("create uniform table command") {
    val sql1 = "CREATE TABLE ufi_table_1 UNIFORM iceberg METADATA_PATH 'metadata_path_1'"
    parseAndValidate(
      sql = sql1,
      targetTable = "ufi_table_1",
      ifNotExists = false,
      isReplace = false,
      isCreate = true,
      fileFormat = "iceberg",
      metadataPath = "metadata_path_1"
    )

    val sql2 =
      "CREATE TABLE IF NOT EXISTS ufi_table_2 UNIFORM iceberg METADATA_PATH 'metadata_path_2'"
    parseAndValidate(
      sql = sql2,
      targetTable = "ufi_table_2",
      ifNotExists = true,
      isReplace = false,
      isCreate = true,
      fileFormat = "iceberg",
      metadataPath = "metadata_path_2"
    )

    val sql3 =
      "CREATE OR REPLACE TABLE ufi_table_3 UNIFORM iceberg METADATA_PATH 'metadata_path_3'"
    parseAndValidate(
      sql = sql3,
      targetTable = "ufi_table_3",
      ifNotExists = false,
      isReplace = true,
      isCreate = true,
      fileFormat = "iceberg",
      metadataPath = "metadata_path_3"
    )

    val sql4 =
      "REPLACE TABLE ufi_table_4 UNIFORM iceberg METADATA_PATH 'metadata_path_4'"
    parseAndValidate(
      sql = sql4,
      targetTable = "ufi_table_4",
      ifNotExists = false,
      isReplace = true,
      isCreate = false,
      fileFormat = "iceberg",
      metadataPath = "metadata_path_4"
    )
  }
}
