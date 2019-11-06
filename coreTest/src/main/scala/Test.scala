package test

import shadedelta.io.delta.tables._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// import shadedelta.org.apache.commons.io.FileUtils

import java.io.File

object Test {
  
  def main(args: Array[String]): Unit = {
    // Create a Spark Session
    val spark = SparkSession
      .builder()
      .appName("Quickstart")
      .master("local[*]")
      .getOrCreate()


    // Create a table
    println("Creating a table")
    val file = new File("/tmp/delta-table")
    val path = file.getCanonicalPath
    var data = spark.range(0, 5)
    data.write.format("delta").mode("overwrite").save(path)

    // Read table
    println("Reading the table")
    val df = spark.read.format("delta").load(path)
    df.show()

    // Cleanup
    // FileUtils.deleteDirectory(file)
    spark.stop()
  }
}
