package io.delta.dsv2
import java.util.{Optional, UUID}
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{QueryTest, SparkSession, functions}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

class BasicDsv2Suite extends QueryTest {

  // System.setProperty("spark.test.home", "/home/ada.ma/delta/spark-3.5.3-bin-hadoop3")

  System.setProperty("spark.test.home", "/home/ada.ma/spark")

  def sparkConf: SparkConf = {
    new SparkConf()
      // .setMaster("local-cluster[3,2,1024]") // 3 executors, each with 2 cores
      .setMaster("local[6]")
      .set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName)
      .set(
        SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.dsv2", "io.delta.dsv2.catalog.TestCatalog")
  }

  override protected def spark: SparkSession = {
    SparkSession.builder().config(sparkConf).getOrCreate()
  }

  test("reading table with multi part checkpoints using dsv2; only AddFiles (no removeFiles)") {

    println(s">>> spark.test.home = ${System.getProperty("spark.test.home")}")
    println(s">>> Spark version (programmatic): ${org.apache.spark.SPARK_VERSION}")

    spark.range(1).count // this triggers a job and starts executors
    println("Default parallelism = " + spark.sparkContext.defaultParallelism) // 3 executor * 1 core per executor
    val numExecutors = spark.sparkContext.getExecutorMemoryStatus.size - 1
    println("Total executor number: " + numExecutors) // 3 executors
    val cores = Runtime.getRuntime.availableProcessors
    println("Available cores: " + cores) // 32 cores in total


//    val warmupTasks = 6
//    spark.sparkContext.parallelize(1 to warmupTasks * 1000, warmupTasks).map(_ + 1).count()

//    val tablePath = "/home/ada.ma/delta/kernel/kernel-defaults/src/test/" +
//      "resources/ada-multi-checkpoint"
//    val df = spark.sql("SELECT * FROM dsv2.delta.`/home/ada.ma/ada_half_M_rows`")
    val df = spark.sql("SELECT * FROM dsv2.delta.`/home/ada.ma/delta/kernel/kernel-defaults/src/test/resources/ada-multi-checkpoint`")
    // println(s"Total row count: ${df.count()}") // should be 10,000

    for (i <- 1 to 5) {
      val count = df.count()
      println(s"Run $i: Total row count = $count")
    }
    spark.stop()
  }
}
