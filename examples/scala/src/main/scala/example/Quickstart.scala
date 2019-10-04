package example

import org.apache.spark.sql.SparkSession
import io.delta.tables._

import org.apache.spark.sql.functions._
import org.apache.commons.io.FileUtils
import java.io.File

object Quickstart {
  def main(args: Array[String]): Unit = {
    // Create a Spark Session
    val spark = SparkSession
      .builder()
      .appName("...")
      .master("local[4]")
      .getOrCreate()

    // Create a table
    println("Creating a table")
    val file = new File("/tmp/delta-table")
    val path = file.getCanonicalPath
    var data = spark.range(0, 5)
    data.write.format("delta").save(path)

    // Read table
    println("Reading the table")
    val df = spark.read.format("delta").load(path)
    df.show()

    // Upsert (merge) new data
    println("Upsert new data")
    val newData = spark.range(0, 20).toDF
    val deltaTable = DeltaTable.forPath(path)

    deltaTable.as("oldData")
      .merge(
        newData.as("newData"),
        "oldData.id = newData.id")
      .whenMatched
      .update(Map("id" -> col("newData.id")))
      .whenNotMatched
      .insert(Map("id" -> col("newData.id")))
      .execute()

    deltaTable.toDF.show()

    // Update table data
    println("Overwrite the table")
    data = spark.range(5, 10)
    data.write.format("delta").mode("overwrite").save(path)
    deltaTable.toDF.show()

    // Update every even value by adding 100 to it
    println("Update to the table (add 100 to every even value)")
    deltaTable.update(
      condition = expr("id % 2 == 0"),
      set = Map("id" -> expr("id + 100")))
    deltaTable.toDF.show()

    // Delete every even value
    deltaTable.delete(condition = expr("id % 2 == 0"))
    deltaTable.toDF.show()

    // Read old version of the data using time travel
    print("Read old data using time travel")
    val df2 = spark.read.format("delta").option("versionAsOf", 0).load(path)
    df2.show()

    // Cleanup
    FileUtils.deleteDirectory(file)
    spark.stop()
  }
}
