import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import io.delta.tables._

object SCDType2Example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Delta Lake SCD Type 2 Example")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    import spark.implicits._

    // Create sample customer dimension table
    case class Customer(
      customer_id: Int,
      name: String,
      email: String,
      address: String,
      updated_timestamp: java.sql.Timestamp
    )

    // Initial data
    val initialCustomers = Seq(
      Customer(1, "John Doe", "john@example.com", "123 Main St", new java.sql.Timestamp(System.currentTimeMillis())),
      Customer(2, "Jane Smith", "jane@example.com", "456 Oak Ave", new java.sql.Timestamp(System.currentTimeMillis()))
    ).toDF()

    // Create Delta table
    val deltaPath = "/tmp/delta/customers_dim"
    
    initialCustomers
      .withColumn("valid_from", col("updated_timestamp"))
      .withColumn("valid_to", lit(null).cast("timestamp"))
      .withColumn("is_current", lit(true))
      .write
      .format("delta")
      .mode("overwrite")
      .save(deltaPath)

    // Function to perform SCD Type 2 merge
    def performSCDType2Merge(newData: DataFrame, targetPath: String): Unit = {
      val deltaTable = DeltaTable.forPath(spark, targetPath)
      
      // Prepare updates by adding metadata columns
      val now = current_timestamp()
      val updates = newData
        .withColumn("valid_from", now)
        .withColumn("valid_to", lit(null).cast("timestamp"))
        .withColumn("is_current", lit(true))

      // Perform merge with SCD Type 2 logic
      deltaTable.as("target")
        .merge(
          updates.as("updates"),
          "target.customer_id = updates.customer_id AND target.is_current = true"
        )
        .whenMatched(
          condition = """
            target.name <> updates.name OR 
            target.email <> updates.email OR 
            target.address <> updates.address
          """
        )
        .updateExpr(
          Map(
            "valid_to" -> "current_timestamp()",
            "is_current" -> "false"
          )
        )
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

    // Example usage: Update some customer information
    val updatedCustomers = Seq(
      Customer(1, "John Doe", "john.doe@example.com", "789 Pine St", // Changed email and address
        new java.sql.Timestamp(System.currentTimeMillis())),
      Customer(3, "Alice Brown", "alice@example.com", "321 Elm St", // New customer
        new java.sql.Timestamp(System.currentTimeMillis()))
    ).toDF()

    performSCDType2Merge(updatedCustomers, deltaPath)

    // Query to show how to retrieve current and historical records
    def showCustomerHistory(): Unit = {
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

    showCustomerHistory()
  }
}