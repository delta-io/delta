/*
 * Copyright 2019 Databricks, Inc.
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

package org.apache.spark.sql.delta

import scala.collection.JavaConverters._

import io.delta.DeltaExtensions
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

class DataFrameWriterV2Suite extends OpenSourceDataFrameWriterV2Suite {


}

// These tests are copied from Apache Spark (minus partition by expressions) and should work exactly
// the same with Delta
trait OpenSourceDataFrameWriterV2Suite
  extends QueryTest
  with SharedSparkSession
  with BeforeAndAfter {

  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.session", classOf[DeltaCatalog].getName)
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.sql.extensions", classOf[DeltaExtensions].getName)
  }

  before {
    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.catalog.listTables("default").foreach { ti =>
      spark.sessionState.catalog.dropTable(ti, ignoreIfNotExists = false, purge = true)
    }
  }

  def catalog: TableCatalog = {
    spark.sessionState.catalogManager.catalog("session").asInstanceOf[DeltaCatalog]
  }

  test("Append: basic append") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING delta")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("session.table_name").append()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    spark.table("source2").writeTo("session.table_name").append()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c"), Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))
  }

  test("Append: by name not position") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING delta")

    checkAnswer(spark.table("table_name"), Seq.empty)

    val exc = intercept[AnalysisException] {
      spark.table("source").withColumnRenamed("data", "d").writeTo("session.table_name").append()
    }

    assert(exc.getMessage.contains("schema mismatch"))

    checkAnswer(
      spark.table("table_name"),
      Seq())
  }

  test("Append: fail if table does not exist") {
    val exc = intercept[NoSuchTableException] {
      spark.table("source").writeTo("session.table_name").append()
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("Overwrite: overwrite by expression: true") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING delta PARTITIONED BY (id)")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("session.table_name").append()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    spark.table("source2").writeTo("session.table_name").overwrite(lit(true))

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))
  }

  test("Overwrite: overwrite by expression: id = 3") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING delta PARTITIONED BY (id)")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("session.table_name").append()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val e = intercept[AnalysisException] {
      spark.table("source2").writeTo("session.table_name").overwrite($"id" === 3)
    }
    assert(e.getMessage.contains("Invalid data would be written to partitions"))

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))
  }

  test("Overwrite: by name not position") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING delta")

    checkAnswer(spark.table("table_name"), Seq.empty)

    val exc = intercept[AnalysisException] {
      spark.table("source").withColumnRenamed("data", "d")
        .writeTo("session.table_name").overwrite(lit(true))
    }

    assert(exc.getMessage.contains("schema mismatch"))

    checkAnswer(
      spark.table("table_name"),
      Seq())
  }

  test("Overwrite: fail if table does not exist") {
    val exc = intercept[NoSuchTableException] {
      spark.table("source").writeTo("session.table_name").overwrite(lit(true))
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("OverwritePartitions: overwrite conflicting partitions") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING delta PARTITIONED BY (id)")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("session.table_name").append()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val e = intercept[AnalysisException] {
      spark.table("source2").withColumn("id", $"id" - 2)
        .writeTo("session.table_name").overwritePartitions()
    }
    assert(e.getMessage.contains("Table default.table_name does not support dynamic overwrite"))

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))
  }

  test("OverwritePartitions: overwrite all rows if not partitioned") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING delta")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("session.table_name").append()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val e = intercept[AnalysisException] {
      spark.table("source2").writeTo("session.table_name").overwritePartitions()
    }
    assert(e.getMessage.contains("Table default.table_name does not support dynamic overwrite"))
  }

  test("OverwritePartitions: by name not position") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING delta")

    checkAnswer(spark.table("table_name"), Seq.empty)

    val e = intercept[AnalysisException] {
      spark.table("source").withColumnRenamed("data", "d")
        .writeTo("session.table_name").overwritePartitions()
    }

    assert(e.getMessage.contains("Table default.table_name does not support dynamic overwrite"))

    checkAnswer(
      spark.table("table_name"),
      Seq())
  }

  test("OverwritePartitions: fail if table does not exist") {
    val exc = intercept[NoSuchTableException] {
      spark.table("source").writeTo("session.table_name").overwritePartitions()
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("Create: basic behavior") {
    spark.table("source").writeTo("session.table_name").using("delta").create()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name === "default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning.isEmpty)
    assert(table.properties.isEmpty)
  }

  test("Create: with using") {
    spark.table("source").writeTo("session.table_name").using("delta").create()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name === "default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning.isEmpty)
    assert(table.properties.isEmpty)
  }

  test("Create: with property") {
    spark.table("source").writeTo("session.table_name")
      .tableProperty("prop", "value").using("delta").create()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array(), "table_name"))
    // scalastyle:off
    println(table)

    assert(table.name === "default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning.isEmpty)
    assert(table.properties === Map("prop" -> "value").asJava)
  }

  test("Create: identity partitioned table") {
    spark.table("source").writeTo("session.table_name").using("delta").partitionedBy($"id").create()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name === "default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(table.properties.isEmpty)
  }

  test("Create: fail if table already exists") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING delta PARTITIONED BY (id)")

    val exc = intercept[TableAlreadyExistsException] {
      spark.table("source").writeTo("session.table_name").using("delta").create()
    }

    assert(exc.getMessage.contains("table_name"))

    val table = catalog.loadTable(Identifier.of(Array(), "table_name"))

    // table should not have been changed
    assert(table.name === "default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(table.properties.isEmpty)
  }

  test("Replace: basic behavior") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING delta PARTITIONED BY (id)")
    spark.sql("INSERT INTO TABLE table_name SELECT * FROM source")

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array(), "table_name"))

    // validate the initial table
    assert(table.name === "default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(table.properties.isEmpty)

    val e = intercept[AnalysisException] {
      spark.table("source2")
        .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
        .writeTo("session.table_name").using("delta").replace()
    }
    assert(e.getMessage.contains("schema mismatch"))

    spark.table("source2")
      .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
      .writeTo("session.table_name").option("mergeSchema", "true").using("delta").replace()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(4L, "d", "even"), Row(5L, "e", "odd"), Row(6L, "f", "even")))

    val replaced = catalog.loadTable(Identifier.of(Array(), "table_name"))

    // validate the replacement table
    assert(replaced.name === "default.table_name")
    assert(replaced.schema === new StructType()
      .add("id", LongType)
      .add("data", StringType)
      .add("even_or_odd", StringType))
    assert(replaced.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(replaced.properties.isEmpty)
  }

  test("Replace: partitioned table") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING delta")
    spark.sql("INSERT INTO TABLE table_name SELECT * FROM source")

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array(), "table_name"))

    // validate the initial table
    assert(table.name === "default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning.isEmpty)
    assert(table.properties.isEmpty)

    val e = intercept[AnalysisException] {
      spark.table("source2")
        .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
        .writeTo("session.table_name").using("delta").partitionedBy($"id").replace()
    }
    assert(e.getMessage.contains("schema mismatch"))

    val e2 = intercept[AnalysisException] {
      spark.table("source2")
        .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
        .writeTo("session.table_name").using("delta")
        .option("mergeSchema", "true").partitionedBy($"id").replace()
    }
    assert(e2.getMessage.contains("schema mismatch"))

    spark.table("source2")
      .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
      .writeTo("session.table_name").using("delta")
      .option("overwriteSchema", "true")
      .partitionedBy($"id")
      .replace()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(4L, "d", "even"), Row(5L, "e", "odd"), Row(6L, "f", "even")))

    val replaced = catalog.loadTable(Identifier.of(Array(), "table_name"))

    // validate the replacement table
    assert(replaced.name === "default.table_name")
    assert(replaced.schema === new StructType()
      .add("id", LongType)
      .add("data", StringType)
      .add("even_or_odd", StringType))
    assert(replaced.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(replaced.properties.isEmpty)
  }

  test("Replace: fail if table does not exist") {
    val exc = intercept[CannotReplaceMissingTableException] {
      spark.table("source").writeTo("session.table_name").using("delta").replace()
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("CreateOrReplace: table does not exist") {
    spark.table("source2").writeTo("session.table_name").using("delta").createOrReplace()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))

    val replaced = catalog.loadTable(Identifier.of(Array(), "table_name"))

    // validate the replacement table
    assert(replaced.name === "default.table_name")
    assert(replaced.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(replaced.partitioning.isEmpty)
    assert(replaced.properties.isEmpty)
  }

  test("CreateOrReplace: table exists") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING delta PARTITIONED BY (id)")
    spark.sql("INSERT INTO TABLE table_name SELECT * FROM source")

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array(), "table_name"))

    // validate the initial table
    assert(table.name === "default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(table.properties.isEmpty)

    val e = intercept[AnalysisException] {
      spark.table("source2")
        .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
        .writeTo("session.table_name").using("delta").createOrReplace()
    }
    assert(e.getMessage.contains("schema mismatch"))

    spark.table("source2")
      .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
      .writeTo("session.table_name").using("delta").option("mergeSchema", "true")
      .createOrReplace()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(4L, "d", "even"), Row(5L, "e", "odd"), Row(6L, "f", "even")))

    val replaced = catalog.loadTable(Identifier.of(Array(), "table_name"))

    // validate the replacement table
    assert(replaced.name === "default.table_name")
    assert(replaced.schema === new StructType()
      .add("id", LongType)
      .add("data", StringType)
      .add("even_or_odd", StringType))
    assert(replaced.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(replaced.properties.isEmpty)
  }

  test("Create: partitioned by years(ts) - not supported") {
    val e = intercept[UnsupportedOperationException] {
      spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("session.table_name")
        .partitionedBy(years($"ts"))
        .using("delta")
        .create()
    }
    assert(e.getMessage.contains("support the partitioning"))
  }

  test("Create: partitioned by months(ts) - not supported") {
    val e = intercept[UnsupportedOperationException] {
      spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("session.table_name")
        .partitionedBy(months($"ts"))
        .using("delta")
        .create()
    }
    assert(e.getMessage.contains("support the partitioning"))
  }

  test("Create: partitioned by days(ts) - not supported") {
    val e = intercept[UnsupportedOperationException] {
      spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("session.table_name")
        .partitionedBy(days($"ts"))
        .using("delta")
        .create()
    }
    assert(e.getMessage.contains("support the partitioning"))
  }

  test("Create: partitioned by hours(ts) - not supported") {
    val e = intercept[UnsupportedOperationException] {
      spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("session.table_name")
        .partitionedBy(hours($"ts"))
        .using("delta")
        .create()
    }
  }

  test("Create: partitioned by bucket(4, id) - not supported") {
    val e = intercept[UnsupportedOperationException] {
      spark.table("source")
        .writeTo("session.table_name")
        .partitionedBy(bucket(4, $"id"))
        .using("delta")
        .create()
    }
    assert(e.getMessage.contains("support the partitioning"))
  }
}
