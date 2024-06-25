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

package org.apache.spark.sql.delta.skipping.clustering

import org.apache.spark.sql.delta.skipping.ClusteredTableTestUtils
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier

class ClusteringColumnSuite extends ClusteredTableTestUtils with DeltaSQLCommandTest {
  test("ClusteringColumnInfo: validate logical column") {
    val table = "test_table"
    withTable(table) {
      // Create a table with nested dot name column.
      sql(s"CREATE TABLE $table(col0 int, col1 STRUCT<`x.y`: int, z: int>) USING DELTA")
      val col0 = "col0"
      val dotColumnName = "col1.`x.y`"

      val schema = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(table))._2.schema
      val columnInfo0 = ClusteringColumnInfo(schema, ClusteringColumn(schema, col0))
      val columnInfoDot = ClusteringColumnInfo(schema, ClusteringColumn(schema, dotColumnName))

      assert(columnInfo0.logicalName === col0)
      assert(columnInfoDot.logicalName === dotColumnName)
    }
  }

  test("ClusteringColumnInfo: extractLogicalNames") {
    val table = "test_table"
    // Create a table with nested dot name column.
    withClusteredTable(
      table,
      "col0 int, col1 STRUCT<`x.y`: int, z: int>",
      "col0, col1.`x.y`") {
      val col0 = "col0"
      val dotColumnName = "col1.`x.y`"

      val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(table))
      assert(ClusteringColumnInfo.extractLogicalNames(snapshot) == Seq(col0, dotColumnName))
    }
  }

  test("ClusteringColumn: throws correct error when column not found") {
    withTable("tbl") {
      sql("CREATE TABLE tbl (a INT) USING DELTA")
      val schema = spark.table("tbl").schema
      val e = intercept[AnalysisException] {
        ClusteringColumn(schema, "b")
      }
      checkError(
        e,
        "DELTA_COLUMN_NOT_FOUND_IN_SCHEMA",
        parameters = Map(
          "columnName" -> "b",
          "tableSchema" -> schema.treeString))
    }
  }
}
