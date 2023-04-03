/*
 * Copyright (2021) The Delta Lake Project Authors.
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

// scalastyle:off import.ordering.noEmptyLine
import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.actions.{Protocol, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.catalog.{DeltaCatalog, DeltaTableV2}
import org.apache.spark.sql.delta.test.{DeltaColumnMappingSelectedTestMixin, DeltaSQLCommandTest}
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, CreateTableWriter, Dataset, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogV2Util, Identifier, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.util.Utils

// These tests are copied from Apache Spark (minus partition by expressions) and should work exactly
// the same with Delta minus some writer options
trait OpenSourceDataFrameWriterV2Tests
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfter {

  import testImplicits._

  before {
    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")
  }

  after {
    spark.sessionState.catalog.listTables("default").foreach { ti =>
      spark.sessionState.catalog.dropTable(ti, ignoreIfNotExists = false, purge = false)
    }
  }

  def catalog: TableCatalog = {
    spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
  }

  protected def catalogPrefix: String = {
    ""
  }

  protected def getProperties(table: Table): Map[String, String] = {
    table.properties().asScala.toMap
      .filterKeys(!CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(_))
      .filterKeys(!TableFeatureProtocolUtils.isTableProtocolProperty(_))
      .toMap
  }

  test("Append: basic append") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING delta")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("table_name").append()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    spark.table("source2").writeTo("table_name").append()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c"), Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))
  }

  test("Append: by name not position") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING delta")

    checkAnswer(spark.table("table_name"), Seq.empty)

    val exc = intercept[AnalysisException] {
      spark.table("source").withColumnRenamed("data", "d").writeTo("table_name").append()
    }

    assert(exc.getMessage.contains("schema mismatch"))

    checkAnswer(
      spark.table("table_name"),
      Seq())
  }

  test("Append: fail if table does not exist") {
    val exc = intercept[AnalysisException] {
      spark.table("source").writeTo("table_name").append()
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("Overwrite: overwrite by expression: true") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING delta PARTITIONED BY (id)")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("table_name").append()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    spark.table("source2").writeTo("table_name").overwrite(lit(true))

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))
  }

  test("Overwrite: overwrite by expression: id = 3") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING delta PARTITIONED BY (id)")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("table_name").append()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val e = intercept[AnalysisException] {
      spark.table("source2").writeTo("table_name").overwrite($"id" === 3)
    }
    assert(e.getMessage.startsWith("Data written out does not match replaceWhere"))

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))
  }

  test("Overwrite: by name not position") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING delta")

    checkAnswer(spark.table("table_name"), Seq.empty)

    val exc = intercept[AnalysisException] {
      spark.table("source").withColumnRenamed("data", "d")
        .writeTo("table_name").overwrite(lit(true))
    }

    assert(exc.getMessage.contains("schema mismatch"))

    checkAnswer(
      spark.table("table_name"),
      Seq())
  }

  test("Overwrite: fail if table does not exist") {
    val exc = intercept[AnalysisException] {
      spark.table("source").writeTo("table_name").overwrite(lit(true))
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("OverwritePartitions: overwrite conflicting partitions") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING delta PARTITIONED BY (id)")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("table_name").append()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    spark.table("source2").withColumn("id", $"id" - 2)
      .writeTo("table_name").overwritePartitions()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "d"), Row(3L, "e"), Row(4L, "f")))
  }

  test("OverwritePartitions: overwrite all rows if not partitioned") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING delta")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("table_name").append()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    spark.table("source2").writeTo("table_name").overwritePartitions()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))
  }

  test("OverwritePartitions: by name not position") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING delta")

    checkAnswer(spark.table("table_name"), Seq.empty)

    val e = intercept[AnalysisException] {
      spark.table("source").withColumnRenamed("data", "d")
        .writeTo("table_name").overwritePartitions()
    }

    assert(e.getMessage.contains("schema mismatch"))

    checkAnswer(
      spark.table("table_name"),
      Seq())
  }

  test("OverwritePartitions: fail if table does not exist") {
    val exc = intercept[AnalysisException] {
      spark.table("source").writeTo("table_name").overwritePartitions()
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("Create: basic behavior") {
    spark.table("source").writeTo("table_name").using("delta").create()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    assert(table.name === s"${catalogPrefix}default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning.isEmpty)
    assert(getProperties(table).isEmpty)
  }

  test("Create: with using") {
    spark.table("source").writeTo("table_name").using("delta").create()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    assert(table.name === s"${catalogPrefix}default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning.isEmpty)
    assert(getProperties(table).isEmpty)
  }

  test("Create: with property") {
    spark.table("source").writeTo("table_name")
      .tableProperty("prop", "value").using("delta").create()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    assert(table.name === s"${catalogPrefix}default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning.isEmpty)
    assert(getProperties(table) === Map("prop" -> "value"))
  }

  test("Create: identity partitioned table") {
    spark.table("source").writeTo("table_name").using("delta").partitionedBy($"id").create()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    assert(table.name === s"${catalogPrefix}default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(getProperties(table).isEmpty)
  }

  test("Create: fail if table already exists") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING delta PARTITIONED BY (id)")

    val exc = intercept[TableAlreadyExistsException] {
      spark.table("source").writeTo("table_name").using("delta").create()
    }

    assert(exc.getMessage.contains("table_name"))

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    // table should not have been changed
    assert(table.name === s"${catalogPrefix}default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(getProperties(table).isEmpty)
  }

  test("Replace: basic behavior") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING delta PARTITIONED BY (id)")
    spark.sql("INSERT INTO TABLE table_name SELECT * FROM source")

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    // validate the initial table
    assert(table.name === s"${catalogPrefix}default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(getProperties(table).isEmpty)

    spark.table("source2")
      .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
      .writeTo("table_name").using("delta")
      .tableProperty("deLta.aPpeNdonly", "true").replace()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(4L, "d", "even"), Row(5L, "e", "odd"), Row(6L, "f", "even")))

    val replaced = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    // validate the replacement table
    assert(replaced.name === s"${catalogPrefix}default.table_name")
    assert(replaced.schema === new StructType()
      .add("id", LongType)
      .add("data", StringType)
      .add("even_or_odd", StringType))
    assert(replaced.partitioning.isEmpty)
    assert(getProperties(replaced) === Map("delta.appendOnly" -> "true"))
  }

  test("Replace: partitioned table") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING delta")
    spark.sql("INSERT INTO TABLE table_name SELECT * FROM source")

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    // validate the initial table
    assert(table.name === s"${catalogPrefix}default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning.isEmpty)
    assert(getProperties(table).isEmpty)

    spark.table("source2")
      .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
      .writeTo("table_name").using("delta")
      .partitionedBy($"id")
      .replace()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(4L, "d", "even"), Row(5L, "e", "odd"), Row(6L, "f", "even")))

    val replaced = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    // validate the replacement table
    assert(replaced.name === s"${catalogPrefix}default.table_name")
    assert(replaced.schema === new StructType()
      .add("id", LongType)
      .add("data", StringType)
      .add("even_or_odd", StringType))
    assert(replaced.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(getProperties(replaced).isEmpty)
  }

  test("Replace: fail if table does not exist") {
    val exc = intercept[CannotReplaceMissingTableException] {
      spark.table("source").writeTo("table_name").using("delta").replace()
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("CreateOrReplace: table does not exist") {
    spark.table("source2").writeTo("table_name").using("delta").createOrReplace()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))

    val replaced = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    // validate the replacement table
    assert(replaced.name === s"${catalogPrefix}default.table_name")
    assert(replaced.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(replaced.partitioning.isEmpty)
    assert(getProperties(replaced).isEmpty)
  }

  test("CreateOrReplace: table exists") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING delta PARTITIONED BY (id)")
    spark.sql("INSERT INTO TABLE table_name SELECT * FROM source")

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    // validate the initial table
    assert(table.name === s"${catalogPrefix}default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(getProperties(table).isEmpty)

    spark.table("source2")
      .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
      .writeTo("table_name").using("delta").createOrReplace()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(4L, "d", "even"), Row(5L, "e", "odd"), Row(6L, "f", "even")))

    val replaced = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    // validate the replacement table
    assert(replaced.name === s"${catalogPrefix}default.table_name")
    assert(replaced.schema === new StructType()
      .add("id", LongType)
      .add("data", StringType)
      .add("even_or_odd", StringType))
    assert(replaced.partitioning.isEmpty)
    assert(getProperties(replaced).isEmpty)
  }

  test("Create: partitioned by years(ts) - not supported") {
    val e = intercept[AnalysisException] {
      spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("table_name")
        .partitionedBy(years($"ts"))
        .using("delta")
        .create()
    }
    assert(e.getMessage.contains("Partitioning by expressions"))
  }

  test("Create: partitioned by months(ts) - not supported") {
    val e = intercept[AnalysisException] {
      spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("table_name")
        .partitionedBy(months($"ts"))
        .using("delta")
        .create()
    }
    assert(e.getMessage.contains("Partitioning by expressions"))
  }

  test("Create: partitioned by days(ts) - not supported") {
    val e = intercept[AnalysisException] {
      spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("table_name")
        .partitionedBy(days($"ts"))
        .using("delta")
        .create()
    }
    assert(e.getMessage.contains("Partitioning by expressions"))
  }

  test("Create: partitioned by hours(ts) - not supported") {
    val e = intercept[AnalysisException] {
      spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("table_name")
        .partitionedBy(hours($"ts"))
        .using("delta")
        .create()
    }
    assert(e.getMessage.contains("Partitioning by expressions"))
  }

  test("Create: partitioned by bucket(4, id) - not supported") {
    val e = intercept[AnalysisException] {
      spark.table("source")
        .writeTo("table_name")
        .partitionedBy(bucket(4, $"id"))
        .using("delta")
        .create()
    }
    assert(e.getMessage.contains("is not supported for Delta tables"))
  }
}

class DeltaDataFrameWriterV2Suite
  extends OpenSourceDataFrameWriterV2Tests
  with DeltaSQLCommandTest {

  import testImplicits._

  test("Append: basic append by path") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING delta")

    checkAnswer(spark.table("table_name"), Seq.empty)
    val location = catalog.loadTable(Identifier.of(Array("default"), "table_name"))
      .asInstanceOf[DeltaTableV2].path

    spark.table("source").writeTo(s"delta.`$location`").append()

    checkAnswer(
      spark.table(s"delta.`$location`"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))
  }

  test("Create: basic behavior by path") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      spark.table("source").writeTo(s"delta.`$dir`").using("delta").create()

      checkAnswer(
        spark.read.format("delta").load(dir),
        Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

      val table = catalog.loadTable(Identifier.of(Array("delta"), dir))

      assert(table.name === s"delta.`file:$dir`")
      assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
      assert(table.partitioning.isEmpty)
      assert(getProperties(table).isEmpty)
    }
  }

  test("Create: using empty dataframe") {
    spark.table("source").where("false")
      .writeTo("table_name").using("delta")
      .tableProperty("delta.appendOnly", "true")
      .partitionedBy($"id").create()

    checkAnswer(spark.table("table_name"), Seq.empty[Row])

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    assert(table.name === s"${catalogPrefix}default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(getProperties(table) === Map("delta.appendOnly" -> "true"))
  }

  test("Replace: basic behavior using empty df") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING delta PARTITIONED BY (id)")
    spark.sql("INSERT INTO TABLE table_name SELECT * FROM source")

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    // validate the initial table
    assert(table.name === s"${catalogPrefix}default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(getProperties(table).isEmpty)

    spark.table("source2").where("false")
      .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
      .writeTo("table_name").using("delta")
      .tableProperty("deLta.aPpeNdonly", "true").replace()

    checkAnswer(
      spark.table("table_name"),
      Seq.empty[Row])

    val replaced = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    // validate the replacement table
    assert(replaced.name === s"${catalogPrefix}default.table_name")
    assert(replaced.schema === new StructType()
        .add("id", LongType)
        .add("data", StringType)
        .add("even_or_odd", StringType))
    assert(replaced.partitioning.isEmpty)
    assert(getProperties(replaced) === Map("delta.appendOnly" -> "true"))
  }

  test("throw error with createOrReplace and Replace if overwriteSchema=false") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING delta PARTITIONED BY (id)")
    spark.sql("INSERT INTO TABLE table_name SELECT * FROM source")

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    def checkFailure(
        df: Dataset[_],
        errorMsg: String)(
        f: CreateTableWriter[_] => CreateTableWriter[_]): Unit = {
      val e = intercept[IllegalArgumentException] {
        val dfwV2 = df.writeTo("table_name")
          .using("delta")
          .option("overwriteSchema", "false")
        f(dfwV2).replace()
      }
      assert(e.getMessage.contains(errorMsg))

      val e2 = intercept[IllegalArgumentException] {
        val dfwV2 = df.writeTo("table_name")
            .using("delta")
            .option("overwriteSchema", "false")
        f(dfwV2).createOrReplace()
      }
      assert(e2.getMessage.contains(errorMsg))
    }

    // schema changes
    checkFailure(
      spark.table("table_name").withColumn("id2", 'id + 1),
      "overwriteSchema is not allowed when replacing")(a => a.partitionedBy($"id"))

    // partitioning changes
    // did not specify partitioning
    checkFailure(spark.table("table_name"),
      "overwriteSchema is not allowed when replacing")(a => a)

    // different partitioning column
    checkFailure(spark.table("table_name"),
      "overwriteSchema is not allowed when replacing")(a => a.partitionedBy($"data"))

    // different table Properties
    checkFailure(spark.table("table_name"), "overwriteSchema is not allowed when replacing")(a =>
      a.partitionedBy($"id").tableProperty("delta.appendOnly", "true"))
  }

  test("append or overwrite mode should not do implicit casting") {
    val table = "not_implicit_casting"
    withTable(table) {
      spark.sql(s"CREATE TABLE $table(id bigint, p int) USING delta PARTITIONED BY (p)")
      def verifyNotImplicitCasting(f: => Unit): Unit = {
        val e = intercept[AnalysisException](f).getMessage
        assert(e.contains("Failed to merge incompatible data types LongType and IntegerType"))
      }
      verifyNotImplicitCasting {
        Seq(1 -> 1).toDF("id", "p").write.mode("append").format("delta").saveAsTable(table)
      }
      verifyNotImplicitCasting {
        Seq(1 -> 1).toDF("id", "p").write.mode("overwrite").format("delta").saveAsTable(table)
      }
      verifyNotImplicitCasting {
        Seq(1 -> 1).toDF("id", "p").writeTo(table).append()
      }
      verifyNotImplicitCasting {
        Seq(1 -> 1).toDF("id", "p").writeTo(table).overwrite($"p" === 1)
      }
      verifyNotImplicitCasting {
        Seq(1 -> 1).toDF("id", "p").writeTo(table).overwritePartitions()
      }
    }
  }

  test("append or overwrite mode allows missing columns") {
    val table = "allow_missing_columns"
    withTable(table) {
      spark.sql(
        s"CREATE TABLE $table(col1 int, col2 int, col3 int) USING delta PARTITIONED BY (col3)")

      // append
      Seq((0, 10)).toDF("col1", "col3").writeTo(table).append()
      checkAnswer(
        spark.table(table),
        Seq(Row(0, null, 10))
      )

      // overwrite by expression
      Seq((1, 11)).toDF("col1", "col3").writeTo(table).overwrite($"col3" === 11)
      checkAnswer(
        spark.table(table),
        Seq(Row(0, null, 10), Row(1, null, 11))
      )

      // dynamic partition overwrite
      Seq((2, 10)).toDF("col1", "col3").writeTo(table).overwritePartitions()
      checkAnswer(
        spark.table(table),
        Seq(Row(2, null, 10), Row(1, null, 11))
      )
    }

  }
}

trait DeltaDataFrameWriterV2ColumnMappingSuiteBase extends DeltaColumnMappingSelectedTestMixin {
  override protected def runOnlyTests = Seq(
    "Append: basic append",
    "Create: with using",
    "Overwrite: overwrite by expression: true",
    "Replace: partitioned table"
  )
}

class DeltaDataFrameWriterV2IdColumnMappingSuite extends DeltaDataFrameWriterV2Suite
  with DeltaColumnMappingEnableIdMode
  with DeltaDataFrameWriterV2ColumnMappingSuiteBase {

  override protected def getProperties(table: Table): Map[String, String] = {
    // ignore column mapping configurations
    dropColumnMappingConfigurations(super.getProperties(table))
  }

}

class DeltaDataFrameWriterV2NameColumnMappingSuite extends DeltaDataFrameWriterV2Suite
  with DeltaColumnMappingEnableNameMode
  with DeltaDataFrameWriterV2ColumnMappingSuiteBase {

  override protected def getProperties(table: Table): Map[String, String] = {
    // ignore column mapping configurations
    dropColumnMappingConfigurations(super.getProperties(table))
  }

}
