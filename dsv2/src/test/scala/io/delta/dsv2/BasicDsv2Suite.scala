package io.delta.dsv2
import  java.util.UUID
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{QueryTest, SparkSession, functions}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

class Dsv2BasicSuite extends QueryTest with SharedSparkSession {

  test("test reading using dsv2") {
    val conf = new SparkConf()
      .set("spark.sql.catalog.dsv2", "io.delta.dsv2.catalog.SimpleUnityCatalog")
      .set("spark.sql.catalog.dsv2.uri", "mask")
      .set("spark.sql.catalog.dsv2.token", "mask")
      .set("spark.sql.catalog.dsv2.warehouse", "managed_iceberg_bugbash_pupr")

    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    println("======== printing result")

    // Get the DataFrame and its schema
    val df = sparkSession.sql("SELECT * FROM dsv2.hxin.ccv2test VERSION AS OF 1")
    val schema = df.schema
    val columnNames = schema.fieldNames
    val rows = df.collect()

    // Calculate column widths for formatting
    val dataValues = rows.map(row =>
      (0 until row.length).map(i => if (row.isNullAt(i)) "null" else row.get(i).toString)
    )
    val columnWidths = columnNames.zipWithIndex.map { case (name, i) =>
      val valueWidth = if (dataValues.isEmpty) 0 else dataValues.map(_(i).length).max
      math.max(name.length, valueWidth) + 2 // add padding
    }

    // Create header and separator
    val header = columnNames.zip(columnWidths)
      .map { case (name, width) => name.padTo(width, ' ') }
      .mkString("|", "|", "|")
    val separator = columnWidths.map(w => "-" * w).mkString("+", "+", "+")

    // Print the formatted table
    println(separator)
    println(header)
    println(separator)

    // Print each row
    rows.foreach { row =>
      val formattedRow = (0 until row.length).map { i =>
        val value = if (row.isNullAt(i)) "null" else row.get(i).toString
        value.padTo(columnWidths(i), ' ')
      }.mkString("|", "|", "|")
      println(formattedRow)
    }
    println(separator)

    // Print total count
    println(s"Total rows: ${rows.length}")
  }

}
