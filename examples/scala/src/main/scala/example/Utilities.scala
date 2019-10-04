package example

import java.io.File

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.SparkSession

object Utilities {
  def main(args: Array[String]): Unit = {
    // Create a Spark Session with SQL enabled
    val spark = SparkSession
      .builder()
      .appName("...")
      .master("local[4]")
    // config io.delta.sql.DeltaSparkSessionExtension - to enable custom Delta-specific SQL commands
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    // config parallelPartitionDiscovery.parallelism -- control the parallelism for vacuum
      .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "4")
      .getOrCreate()

    // Create a table
    println("Create a parquet table")
    val data = spark.range(0, 5)
    val file = new File("/tmp/parquet-table")
    val path = file.getAbsolutePath
    data.write.format("parquet").save(path)

    // Convert to delta
    println("Convert to Delta")
    DeltaTable.convertToDelta(spark, s"parquet.`$path`")

    // Read table as delta
    var df = spark.read.format("delta").load(path)

    // Read old version of data using time travel
    df = spark.read.format("delta").option("versionAsOf", 0).load(path)
    df.show()

    val deltaTable = DeltaTable.forPath(path)

    // Utility commands
    println("Vacuum the table")
    deltaTable.vacuum()

    println("Describe History for the table")
    deltaTable.history().show()

    // SQL utility commands
    println("SQL Vacuum")
    spark.sql(s"VACUUM '$path' RETAIN 169 HOURS")

    println("SQL Describe History")
    println(spark.sql(s"DESCRIBE HISTORY '$path'").collect())

    // Cleanup
    FileUtils.deleteDirectory(new File(path))
    spark.stop()
  }
}
