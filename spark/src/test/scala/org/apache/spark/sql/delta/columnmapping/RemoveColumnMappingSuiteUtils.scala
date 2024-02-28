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

package org.apache.spark.sql.delta.columnmapping

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.sources.DeltaSQLConf._

import org.apache.spark.sql.QueryTest

/**
 * A base trait for testing removing column mapping.
 * Takes care of setting basic SQL configs and dropping the [[testTableName]] after each test.
 */
trait RemoveColumnMappingSuiteUtils extends QueryTest with DeltaColumnMappingSuiteUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(ALLOW_COLUMN_MAPPING_REMOVAL.key, "true")
  }

  override def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $testTableName")
    super.afterEach()
  }

  protected val testTableName: String = "test_table_" + this.getClass.getSimpleName
}
