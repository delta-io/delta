/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package example

import org.apache.spark.sql.SparkSession
import io.delta.tables._

import org.apache.spark.sql.functions._
import org.apache.commons.io.FileUtils
import java.io.File

object QuickstartSQLOnPaths {
  def main(args: Array[String]): Unit = {
    // Create Spark Conf
    val spark = SparkSession
      .builder()
      .appName("QuickstartSQLOnPaths")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    
    val tablePath = new File("/tmp/delta-table")
    if (tablePath.exists()) FileUtils.deleteDirectory(tablePath)

    // Clear up old session
    spark.sql(s"DROP TABLE IF EXISTS newData")

    try {
      // Create a table
      println("Creating a table")
      spark.sql(s"CREATE TABLE delta.`$tablePath`(id LONG) USING delta")
      spark.sql(s"INSERT INTO delta.`$tablePath` VALUES 0, 1, 2, 3, 4")

      // Read table
      println("Reading the table")
      spark.sql(s"SELECT * FROM delta.`$tablePath`").show()

      // Upsert (merge) new data
      println("Upsert new data")
      spark.sql("CREATE TABLE newData(id LONG) USING parquet")
      spark.sql("INSERT INTO newData VALUES 3, 4, 5, 6")
      
      spark.sql(s"""MERGE INTO delta.`$tablePath` data USING newData
          ON data.id = newData.id
          WHEN MATCHED THEN
            UPDATE SET data.id = newData.id
          WHEN NOT MATCHED THEN INSERT *
      """)

      spark.sql(s"SELECT * FROM delta.`$tablePath`").show()

      // Update table data
      println("Overwrite the table")
      spark.sql(s"INSERT OVERWRITE delta.`$tablePath` VALUES 5, 6, 7, 8, 9")
      spark.sql(s"SELECT * FROM delta.`$tablePath`").show()

      // Update every even value by adding 100 to it
      println("Update to the table (add 100 to every even value)")
      spark.sql(s"UPDATE delta.`$tablePath` SET id = (id + 100) WHERE (id % 2 == 0)")
      spark.sql(s"SELECT * FROM delta.`$tablePath`").show()

      // Delete every even value
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE (id % 2 == 0)")
      spark.sql(s"SELECT * FROM delta.`$tablePath`").show()
    } finally {
      // Cleanup
      spark.sql(s"DROP TABLE IF EXISTS newData")
      spark.stop()
    }
  }
}
