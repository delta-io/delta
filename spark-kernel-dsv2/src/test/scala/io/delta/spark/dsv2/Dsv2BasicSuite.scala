/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.spark.dsv2

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class Dsv2BasicSuite extends QueryTest with SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.catalog.dsv2", "io.delta.spark.dsv2.catalog.TestCatalog")
  }

  test("loadTableTest") {
    val exception = intercept[Exception] {
      spark.sql("select * from dsv2.test_namespace.test_table")
    }

    assert(exception.isInstanceOf[UnsupportedOperationException])
    assert(exception.getMessage.contains("loadTable method is not implemented"))
  }
}
