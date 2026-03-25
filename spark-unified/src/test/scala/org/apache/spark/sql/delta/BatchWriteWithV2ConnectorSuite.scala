/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, V2ForceTest}

/**
 * Focused write-path regression tests for the Kernel-based V2 connector.
 */
class BatchWriteWithV2ConnectorSuite
  extends QueryTest
  with DeltaSQLCommandTest
  with V2ForceTest {

  test("INSERT INTO partitioned managed table writes through Kernel") {
    val tableName = "partitioned_managed_kernel_write"

    withTable(tableName) {
      spark.sql(
        s"""CREATE TABLE $tableName (
           |  id INT,
           |  region STRING
           |) USING delta
           |PARTITIONED BY (region)""".stripMargin)

      spark.sql(s"INSERT INTO $tableName VALUES (1, 'us'), (2, 'eu'), (3, 'us')")

      checkAnswer(
        spark.table(tableName).orderBy("id"),
        Seq(Row(1, "us"), Row(2, "eu"), Row(3, "us"))
      )
    }
  }
}
