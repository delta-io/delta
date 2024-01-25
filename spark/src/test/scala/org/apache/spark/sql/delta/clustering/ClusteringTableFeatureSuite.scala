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

package org.apache.spark.sql.delta.clustering

import org.apache.spark.sql.delta.DeltaAnalysisException
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkFunSuite

class ClusteringTableFeatureSuite extends SparkFunSuite with DeltaSQLCommandTest {

  test("create table without cluster by clause cannot set clustering table properties") {
    withTable("tbl") {
      val e = intercept[DeltaAnalysisException] {
        sql("CREATE TABLE tbl(a INT, b STRING) USING DELTA " +
          "TBLPROPERTIES('delta.feature.clustering' = 'supported')")
      }
      checkError(
        e,
        "DELTA_CREATE_TABLE_SET_CLUSTERING_TABLE_FEATURE_NOT_ALLOWED",
        parameters = Map("tableFeature" -> "clustering")
      )
    }
  }

  test("use alter table set table properties to enable clustering is not allowed.") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(a INT, b STRING) USING DELTA")
      val e = intercept[DeltaAnalysisException] {
        sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.feature.clustering' = 'supported')")
      }
      checkError(
        e,
        "DELTA_ALTER_TABLE_SET_CLUSTERING_TABLE_FEATURE_NOT_ALLOWED",
        parameters = Map("tableFeature" -> "clustering")
      )
    }
  }
}
