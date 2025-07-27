package io.delta.dsv2
import java.util.{Optional, UUID}

import io.delta.sql.DeltaSparkSessionExtension

import org.apache.spark.sql.delta.DeltaLog

import org.apache.spark.SparkConf
import org.apache.spark.sql.{functions, QueryTest, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

class BasicDsv2Suite extends QueryTest with SharedSparkSession {

  test("print java version") {
    println("Running test JVM Java version: " + System.getProperty("java.version"))
    assert(true) // dummy assert
  }

  test("test reading using dsv2") {
    val conf = new SparkConf()
      .set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName)
      .set(
        SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      ) // DSV1
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.dsv2", "io.delta.dsv2.catalog.TestCatalog")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
//    sparkSession.sql(
//      s"CREATE OR REPLACE TABLE delta.`/tmp/spark_warehouse/table6`" +
//        s" (id integer) USING DELTA")
    val tablePath = "/home/ada.ma/delta/kernel/kernel-defaults/src/test/" +
      "resources/ada-multi-checkpoint"
    val df = sparkSession.sql(
      s"SELECT * FROM dsv2.delta.`/home/ada.ma/delta/kernel/kernel-defaults/src/test/" +
        "resources/ada-multi-checkpoint`")
    println(s"Total row count: ${df.count()}")
  }

  test("Ada: reading with dsv2") {}
}
