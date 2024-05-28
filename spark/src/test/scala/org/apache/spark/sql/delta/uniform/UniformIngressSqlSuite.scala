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

  test("create uniform table command") {
    val target = new UnresolvedRelation(Seq("uniform_table_1"))
    val result = CreateUniformTableCommand(
      table = target,
      ifNotExists = false,
      isReplace = false,
      isCreate = true,
      fileFormat = "iceberg",
      metadataPath = "metadata_path_1"
    )
    assert(
      spark.sessionState.sqlParser.parsePlan(
        "CREATE TABLE uniform_table_1 UNIFORM iceberg METADATA_PATH 'metadata_path_1'"
       ) == result
    )
  }
}
