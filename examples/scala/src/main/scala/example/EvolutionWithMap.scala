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

package example

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object EvolutionWithMap {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EvolutionWithMap")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._

    val tableName = "insert_map_schema_evolution"

    try {
      // Define initial schema
      val initialSchema = StructType(Seq(
          StructField("key", IntegerType, nullable = false),
          StructField("metrics", MapType(StringType, StructType(Seq(
              StructField("id", IntegerType, nullable = false),
              StructField("value", IntegerType, nullable = false)
          ))))
          ))

      val data = Seq(
      Row(1, Map("event" -> Row(1, 1)))
      )

      val rdd = spark.sparkContext.parallelize(data)

      val initialDf = spark.createDataFrame(rdd, initialSchema)

      initialDf.write
        .option("overwriteSchema", "true")
        .mode("overwrite")
        .format("delta")
        .saveAsTable(s"$tableName")

      // Define the schema with simulteneous change in a StructField name
      // And additional field in a map column
      val evolvedSchema = StructType(Seq(
      StructField("renamed_key", IntegerType, nullable = false),
      StructField("metrics", MapType(StringType, StructType(Seq(
          StructField("id", IntegerType, nullable = false),
          StructField("value", IntegerType, nullable = false),
          StructField("comment", StringType, nullable = true)
      ))))
      ))

      val evolvedData = Seq(
      Row(1, Map("event" -> Row(1, 1, "deprecated")))
      )

      val evolvedRDD = spark.sparkContext.parallelize(evolvedData)

      val modifiedDf = spark.createDataFrame(evolvedRDD, evolvedSchema)

      // The below would fail without schema evolution for map types
      modifiedDf.write
        .mode("append")
        .option("mergeSchema", "true")
        .format("delta")
        .insertInto(s"$tableName")

      spark.sql(s"SELECT * FROM $tableName").show(false)

    } finally {

      // Cleanup
      spark.sql(s"DROP TABLE IF EXISTS $tableName")

      spark.stop()
    }

  }
}
