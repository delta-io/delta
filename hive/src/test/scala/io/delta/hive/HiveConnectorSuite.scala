package io.delta.hive

import java.io.File
import java.nio.file.Files

import io.delta.hive.test.HiveTest
import org.apache.spark.SparkConf
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.scalatest.BeforeAndAfterEach

class HiveConnectorSuite extends HiveTest with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    DeltaLog.clearCache()
  }

  override def afterEach(): Unit = {
    DeltaLog.clearCache()
  }

  test("read a non-partitioned table") {
    // Create a Delta table
    val tempPath = Files.createTempDirectory("testdata").toFile
    try {
      val conf = new SparkConf()
      val spark = SparkSession.builder()
        .appName("HiveConnectorSuite")
        .master("local[2]")
        .getOrCreate()
      val testData = (0 until 10).map(x => (x, s"foo${x % 2}")).toSeq
      import spark.implicits._
      testData.toDS.toDF("c1", "c2").write.format("delta").save(tempPath.getCanonicalPath)
      // Clean up resources so that we can use new DeltaLog and SparkSession
      spark.stop()
      DeltaLog.clearCache()
      runQuery(
        s"""
           |create external table deltatesttable(c1 INT, c2 STRING)
           |row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
           |stored as inputformat 'io.delta.hive.DeltaInputFormat'
           |outputformat 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
           |location '${tempPath.getCanonicalPath}'
      """.stripMargin)
      assert(runQuery(
        "select * from deltatesttable").sorted === testData.map(r => s"${r._1}\t${r._2}").sorted)
      runQuery("drop table deltatesttable")
    } finally {
      JavaUtils.deleteRecursively(tempPath)
    }
  }

  test("read a partitioned table") {
    // Create a Delta table
    val tempPath = Files.createTempDirectory("testdata").toFile
    try {
      val conf = new SparkConf()
      val spark = SparkSession.builder()
        .appName("HiveConnectorSuite")
        .master("local[2]")
        .getOrCreate()
      val testData = (0 until 10).map(x => (x, s"foo${x % 2}")).toSeq
      import spark.implicits._
      testData.toDS.toDF("c1", "c2").write.format("delta")
        .partitionBy("c2").save(tempPath.getCanonicalPath)
      // Clean up resources so that we can use new DeltaLog and SparkSession
      spark.stop()
      DeltaLog.clearCache()
      runQuery(
        s"""
           |create external table deltatesttable(c1 INT)
           |partitioned by(c2 STRING) -- TODO Remove this. This should be read from Delta's metadata
           |row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
           |stored as inputformat 'io.delta.hive.DeltaInputFormat'
           |outputformat 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
           |location '${tempPath.getCanonicalPath}'
      """.stripMargin)
      // TODO Remove this. We should discover partitions automatically
      runQuery("msck repair table deltatesttable")
      assert(runQuery(
        "select * from deltatesttable").sorted === testData.map(r => s"${r._1}\t${r._2}").sorted)
      runQuery("drop table deltatesttable")
    } finally {
      JavaUtils.deleteRecursively(tempPath)
    }
  }

  test("read a partitioned table with a partition filter") {
    // Create a Delta table
    val tempPath = Files.createTempDirectory("testdata").toFile
    try {
      val conf = new SparkConf()
      val spark = SparkSession.builder()
        .appName("HiveConnectorSuite")
        .master("local[2]")
        .getOrCreate()
      val testData = (0 until 10).map(x => (x, s"foo${x % 2}")).toSeq
      import spark.implicits._
      testData.toDS.toDF("c1", "c2").write.format("delta")
        .partitionBy("c2").save(tempPath.getCanonicalPath)
      // Clean up resources so that we can use new DeltaLog and SparkSession
      spark.stop()
      DeltaLog.clearCache()
      runQuery(
        s"""
           |create external table deltatesttable(c1 INT)
           |partitioned by(c2 STRING) -- TODO Remove this. This should be read from Delta's metadata
           |row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
           |stored as inputformat 'io.delta.hive.DeltaInputFormat'
           |outputformat 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
           |location '${tempPath.getCanonicalPath}'
      """.stripMargin)
      // TODO Remove this. We should discover partitions automatically
      runQuery("msck repair table deltatesttable")
      // Delete the partition not needed in the below query to verify the partition pruning works
      JavaUtils.deleteRecursively(new File(tempPath, "c2=foo1"))
      assert(tempPath.listFiles.map(_.getName).sorted === Seq("_delta_log", "c2=foo0").sorted)
      assert(runQuery(
        "select * from deltatesttable where c2 = 'foo0'").sorted ===
        testData.filter(_._2 == "foo0").map(r => s"${r._1}\t${r._2}").sorted)
      runQuery("drop table deltatesttable")
    } finally {
      JavaUtils.deleteRecursively(tempPath)
    }
  }
}