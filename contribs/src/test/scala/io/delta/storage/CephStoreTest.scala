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
package io.delta.storage

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{LocalSparkSession, SparkSession}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.delta.storage.S3SingleDriverLogStore


class CephStoreTest extends SparkFunSuite with LocalSparkSession with SQLHelper {

  test("commit lock flag on Ceph RGW") {
    spark = SparkSession.builder()
      .appName("Quickstart")
      .master("local[*]")
      .config("spark.delta.logStore.class", classOf[S3SingleDriverLogStore].getName)
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("fs.ceph.impl", "io.delta.storage.cephobjectstore.CephStoreSystem")
      .config("spark.hadoop.fs.ceph.username", "<your-ceph-rgw-username>")
      .config("spark.hadoop.fs.ceph.password", "<your-ceph-rgw-secret-key>")
      .config("spark.hadoop.fs.ceph.uri", "<your-ceph-rgw-uri>")
      .config("fs.oss.buffer.dir", "<your-tmp-file-path>")
      .getOrCreate()

    // Try out some basic Delta table operations on Ceph RGW  (in Scala):
    //
    // spark.range(5).write.format("delta")
    // .save("ceph://<your-ceph-rgw-container>/<path-to-delta-table>")
    // spark.read.format("delta")
    // .load("ceph://<your-ceph-rgw-container>/<path-to-delta-table>").show()

    // Update table data
    //
    // spark.range(5, 10).write.format("delta").mode("overwrite")
    // .save("ceph://<your-ceph-rgw-container>/<path-to-delta-table>")
    //  df.show()

    // Read older versions of data using time travel
    //
    // df = spark.read.format("delta").option("versionAsOf", 0)
    // .load("ceph://<your-ceph-rgw-container>/<path-to-delta-table>")
    // df.show()


  }
}
