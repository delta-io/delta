/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import io.delta.tables.DeltaTable

import org.apache.spark.sql.SparkSession

object Variant {

  def main(args: Array[String]): Unit = {
    val tableName = "tbl"

    val spark = SparkSession
      .builder()
      .appName("Variant-Delta")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // Only run this example for Spark versions >= 4.0.0
    if (spark.version.split("\\.").head.toInt < 4) {
      println(s"Skipping Variant.scala since Spark version ${spark.version} is too low")
      return
    }

    // Create and insert variant values.
    try {
      println("Creating and inserting variant values")
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"CREATE TABLE $tableName(v VARIANT) USING DELTA")
      spark.sql(s"INSERT INTO $tableName VALUES (parse_json('1'))")
      spark.sql(s"""INSERT INTO $tableName SELECT parse_json(format_string('{\"k\": %s}', id))
                FROM range(0, 10)""")
      val ids = spark.sql("SELECT variant_get(v, '$.k', 'INT') out " +
                          s"""FROM $tableName WHERE contains(schema_of_variant(v), 'k')
                          ORDER BY out""")
                          .collect().map { r => r.getInt(0) }.toSeq
      val expected = (0 until 10).toSeq
      assert(expected == ids)

      spark.sql(s"DELETE FROM $tableName WHERE variant_get(v, '$$.k', 'INT') = 0")
      val idsWithDelete = spark.sql("SELECT variant_get(v, '$.k', 'INT') out " +
                                    s"""FROM $tableName WHERE contains(schema_of_variant(v), 'k')
                                    ORDER BY out""")
                                    .collect().map { r => r.getInt(0) }.toSeq
      val expectedWithDelete = (1 until 10).toSeq
      assert(idsWithDelete == expectedWithDelete)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
    
    // Convert Parquet table with variant values to Delta.
    try {
      println("Converting a parquet table with variant values to Delta")
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"""CREATE TABLE $tableName USING PARQUET AS (
        SELECT parse_json(format_string('%s', id)) v FROM range(0, 10))""")
      spark.sql(s"CONVERT TO DELTA $tableName")
      val convertToDeltaIds = spark.sql(s"SELECT v::int v FROM $tableName ORDER BY v")
        .collect()
        .map { r => r.getInt(0) }
        .toSeq
      val convertToDeltaExpected = (0 until 10).toSeq
      assert(convertToDeltaIds == convertToDeltaExpected)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

    // DeltaTable create with variant Scala API.
    try {
      println("Creating a delta table with variant type using the DeltaTable API")
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      val table = io.delta.tables.DeltaTable.create()
        .tableName(tableName)
        .addColumn("v", "VARIANT")
        .execute()

      table
        .as("tgt")
        .merge(
          spark.sql("select parse_json(format_string('%s', id)) v from range(0, 10)").as("source"),
          "source.v::int == tgt.v::int"
        )
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
      val insertedVals = spark.sql(s"SELECT v::int v FROM $tableName ORDER BY v")
        .collect()
        .map { r => r.getInt(0) }
        .toSeq
      val expected = (0 until 10).toSeq
      assert(insertedVals == expected)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.stop()
    }
  }
}
