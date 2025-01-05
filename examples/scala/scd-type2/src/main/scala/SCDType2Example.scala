/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.examples

import io.delta.tables._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Example demonstrating how to implement Slowly Changing Dimension (SCD) Type 2
 * using Delta Lake.
 */
object SCDType2Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Delta Lake SCD Type 2 Example")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._

    case class Customer(
      customer_id: Int,
      name: String,
      email: String,
      address: String,
      updated_timestamp: java.sql.Timestamp)

    // Initial data
    val initialCustomers = Seq(
      Customer(
        1,
        "John Doe",
        "john@example.com",
        "123 Main St",
        new java.sql.Timestamp(System.currentTimeMillis())),
      Customer(
        2,
        "Jane Smith",
        "jane@example.com",
        "456 Oak Ave",
        new java.sql.Timestamp(System.currentTimeMillis()))
    ).toDF()

    val deltaPath = "/tmp/delta/customers_dim"
    
    initialCustomers
      .withColumn("valid_from", col("updated_timestamp"))
      .withColumn("valid_to", lit(null).cast("timestamp"))
      .withColumn("is_current", lit(true))
      .write
      .format("delta")
      .mode("overwrite")
      .save(deltaPath)

    performSCDType2Merge(
      createSampleUpdates(),
      deltaPath)

    showCustomerHistory(spark, deltaPath)
  }

  /**
   * Performs SCD Type 2 merge operation on the target Delta table.
   *
   * @param newData DataFrame containing the new/updated records
   * @param targetPath Path to the target Delta table
   */
  def performSCDType2Merge(newData: DataFrame, targetPath: String): Unit = {
    val spark = newData.sparkSession
    val deltaTable = DeltaTable.forPath(spark, targetPath)
    
    val updates = newData
      .withColumn("valid_from", current_timestamp())
      .withColumn("valid_to", lit(null).cast("timestamp"))
      .withColumn("is_current", lit(true))

    // Close current version of records that will be updated
    deltaTable.as("target")
      .merge(
        updates.as("updates"),
        "target.customer_id = updates.customer_id AND target.is_current = true")
      .whenMatched(
        condition = """
          target.name <> updates.name OR 
          target.email <> updates.email OR 
          target.address <> updates.address
        """)
      .updateExpr(
        Map(
          "valid_to" -> "current_timestamp()",
          "is_current" -> "false"))
      .whenNotMatched()
      .insertAll()
      .execute()

    // Insert new versions of updated records
    val updatedRecords = spark.read.format("delta").load(targetPath)
      .where("valid_to = current_timestamp()")
      
    if (!updatedRecords.isEmpty()) {
      updates
        .join(updatedRecords, Seq("customer_id"))
        .select(updates("*"))
        .write
        .format("delta")
        .mode("append")
        .save(targetPath)
    }
  }

  /**
   * Creates sample updates for demonstration.
   */
  def createSampleUpdates(): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._

    Seq(
      Customer(
        1,
        "John Doe",
        "john.doe@example.com",
        "789 Pine St",
        new java.sql.Timestamp(System.currentTimeMillis())),
      Customer(
        3,
        "Alice Brown",
        "alice@example.com",
        "321 Elm St",
        new java.sql.Timestamp(System.currentTimeMillis()))
    ).toDF()
  }

  /**
   * Displays current and historical customer records.
   */
  def showCustomerHistory(spark: SparkSession, deltaPath: String): Unit = {
    println("Current Customer Records:")
    spark.read.format("delta")
      .load(deltaPath)
      .where("is_current = true")
      .orderBy("customer_id")
      .show()

    println("\nFull Customer History:")
    spark.read.format("delta")
      .load(deltaPath)
      .orderBy("customer_id", "valid_from")
      .show()
  }
}
