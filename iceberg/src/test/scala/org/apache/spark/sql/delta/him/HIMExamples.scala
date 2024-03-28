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

package org.apache.spark.sql.delta.him

import org.apache.spark.sql.SparkSession

/**
 * This example shows how to start a HIM and create a SparkSession to connect to it.
 */
object HIMExamples {
  def main(args: Array[String]): Unit = {

   val him = new HIM()
   him.start()

   val sparkSession = SparkSession.builder()
     .master("local[*]")
     .appName("DeltaSession")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
     .config("hive.metastore.uris", him.conf().get("hive.metastore.uris"))
     .config("spark.sql.warehouse.dir", him.conf().get("hive.metastore.warehouse.dir"))
     .config("spark.sql.catalogImplementation", "hive")
     .getOrCreate()
   sparkSession.sql("create table t1 (id int) using delta")

   sparkSession.stop()
   sparkSession.close()

   him.stop()
   System.exit(0)
  }
}
