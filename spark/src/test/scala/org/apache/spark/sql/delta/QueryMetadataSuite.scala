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

package org.apache.spark.sql.delta

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

class QueryMetadataSuite extends QueryTest
  with SharedSparkSession
  with DeltaDMLTestUtils
  with DeltaSQLCommandTest {

  import testImplicits._

  test("batch _metadata query") {
    withTable("source") {
      append(Seq((1, 10), (2, 20)).toDF("key1", "value1"), Nil) // test table

      val metadata = spark.read.format("delta").load(tempPath).select("_metadata").collect()

      // TODO verify the metadata response
    }
  }

  test("streaming _metadata query") {
    withTable("source") {
      append(Seq((1, 10), (2, 20)).toDF("key1", "value1"), Nil) // test table

      // TODO breaks
      val metadata = spark.readStream.format("delta").load(tempPath).select("_metadata").collect()

      // TODO verify the metadata response
    }
  }
}
