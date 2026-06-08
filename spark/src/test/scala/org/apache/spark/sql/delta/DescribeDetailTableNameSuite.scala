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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class DescribeDetailTableNameSuite extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  /**
   * Test for issue #4797: DESCRIBE DETAIL returns null table name when accessed via path
   * after creating it with saveAsTable
   */
  test("describe detail should return table name when table created with saveAsTable " +
    "and accessed via path") {
    val tableName = "people"
    withTable(tableName) {
      withTempDir { tempDir =>
        val spark = this.spark
        // scalastyle:off sparkimplicits
        import spark.implicits._
        // scalastyle:on sparkimplicits

        // Create DataFrame and save as Delta table using saveAsTable
        val df = Seq(("Alice", 25), ("Bob", 30)).toDF("name", "age")
        df.write.mode("overwrite").format("delta").saveAsTable(tableName)

        // Get the table location from catalog
        val catalogTable = spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(tableName))
        val tableLocation = catalogTable.location.toString

        // Test 1: DESCRIBE DETAIL using table name should show the table name
        val resultByName = sql(s"DESCRIBE DETAIL $tableName")
        val nameByName = resultByName.select("name").collect().head.getString(0)
        val expectedName = s"spark_catalog.default.$tableName"
        assert(
          nameByName === expectedName,
          s"Expected table name to be '$expectedName', but got '$nameByName'")

        // Test 2: DESCRIBE DETAIL using path should also show the table name (this is the bug)
        val resultByPath = sql(s"DESCRIBE DETAIL '$tableLocation'")
        val nameByPath = resultByPath.select("name").collect().head.getString(0)
        assert(
          nameByPath === expectedName,
          s"Expected table name to be '$expectedName' when accessed by path, but got '$nameByPath'")

        // Test 3: DESCRIBE DETAIL using delta.`path` should also show the table name
        val resultByDeltaPath = sql(s"DESCRIBE DETAIL delta.`$tableLocation`")
        val nameByDeltaPath = resultByDeltaPath.select("name").collect().head.getString(0)
        assert(
          nameByDeltaPath === expectedName,
          s"Expected table name to be '$expectedName' when accessed by delta path," +
          " but got '$nameByDeltaPath'"
          )
      }
    }
  }

  test(
    "describe detail should return table name when table created with external path " +
      "saveAsTable") {
    val tableName = "people_external"
    val expectedName = s"spark_catalog.default.$tableName"
    withTable(tableName) {
      withTempDir { tempDir =>
        val spark = this.spark
        // scalastyle:off sparkimplicits
        import spark.implicits._
        // scalastyle:on sparkimplicits

        // Create DataFrame and save as external Delta table using saveAsTable with path option
        val df = Seq(("Alice", 25), ("Bob", 30)).toDF("name", "age")
        df.write.mode("overwrite").format("delta")
          .option("path", tempDir.getAbsolutePath)
          .saveAsTable(tableName)

        val tableLocation = tempDir.getAbsolutePath

        // Test 1: DESCRIBE DETAIL using table name should show the table name
        val resultByName = sql(s"DESCRIBE DETAIL $tableName")
        val nameByName = resultByName.select("name").collect().head.getString(0)
        assert(
          nameByName === expectedName,
          s"Expected table name to be '$expectedName', but got '$nameByName'")

        // Test 2: DESCRIBE DETAIL using path should also show the table name (this is the bug)
        val resultByPath = sql(s"DESCRIBE DETAIL '$tableLocation'")
        val nameByPath = resultByPath.select("name").collect().head.getString(0)
        assert(
          nameByPath === expectedName,
          s"Expected table name to be '$expectedName' when accessed by path, but got '$nameByPath'")
      }
    }
  }
}
