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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}

import java.io.File

trait ShowTablePartitionsSuiteBase extends QueryTest with SharedSparkSession {

  import testImplicits._

  protected def checkResult(
    result: DataFrame,
    expected: Seq[Row],
    columns: Seq[String]): Unit = {
    checkAnswer(
      result.select(columns.head, columns.tail: _*),
      expected
    )
  }

  def withPartitionedDeltaPath(multiParts: Boolean = true)(f: File => Unit): Unit =
    withTempDir { tempDir =>
      val partitionBy = if (multiParts) Seq("partition1", "partition2") else Seq("partition1")
      spark.range(1, 6)
        .withColumn("partition1", col("id") % 3)
        .withColumn("partition2", when(col("partition1") % 2 === 0, lit(0)).cast(StringType))
        .write
        .format("delta")
        .partitionBy(partitionBy: _*)
        .save(tempDir.toString)

      f(tempDir)
    }

  def withPartitionedDeltaTable(
    table: String,
    multiParts: Boolean = true
  )(f: String => Unit): Unit =
    withTable(table) {
      withPartitionedDeltaPath(multiParts) { location =>
        sql(s"CREATE TABLE $table USING DELTA LOCATION '${location.toString}'")
        f(table)
      }
    }

  test("delta table: path") {
    withPartitionedDeltaPath() { path =>
      checkResult(
        sql(s"SHOW PARTITIONS '$path'"),
        Seq(Row(0, "0"), Row(1, null), Row(2, "0")),
        Seq("partition1", "partition2"))
    }
  }

  test("delta table: table identifier") {
    withPartitionedDeltaPath() { path =>
      checkResult(
        sql(s"SHOW PARTITIONS delta.`$path`"),
        Seq(Row(0, "0"), Row(1, null), Row(2, "0")),
        Seq("partition1", "partition2"))
    }
  }

  test("delta table: delta table") {
    withPartitionedDeltaTable("show_partitions") { table =>
      checkResult(
        sql(s"SHOW PARTITIONS $table"),
        Seq(Row(0, "0"), Row(1, null), Row(2, "0")),
        Seq("partition1", "partition2"))
    }
  }

  test("show partition single level of partitioning") {
    withPartitionedDeltaTable("show_partitions", multiParts = false) { table =>
      checkResult(
        sql(s"SHOW PARTITIONS $table"),
        Seq(Row(0), Row(1), Row(2)),
        Seq("partition1"))
    }
  }

  test("show partitions with partition spec") {
    withPartitionedDeltaPath() { path =>
      checkResult(
        sql(s"SHOW PARTITIONS delta.`$path` PARTITION (partition1=2)"),
        Seq(Row(2, "0")),
        Seq("partition1", "partition2"))
    }

    withPartitionedDeltaPath() { path =>
      checkResult(
        sql(s"SHOW PARTITIONS delta.`$path` PARTITION (partition2='0')"),
        Seq(Row(0, "0"), Row(2, "0")),
        Seq("partition1", "partition2"))
    }

    withPartitionedDeltaPath() { path =>
      checkResult(
        sql(s"SHOW PARTITIONS delta.`$path` PARTITION (partition1=2, partition2='0')"),
        Seq(Row(2, "0")),
        Seq("partition1", "partition2"))
    }

    withPartitionedDeltaPath() { path =>
      checkResult(
        sql(s"SHOW PARTITIONS delta.`$path` PARTITION (partition1=1, partition2=null)"),
        Seq(Row(1, null)),
        Seq("partition1", "partition2"))
    }
  }

  test("show partitions with partition spec for non-existent partition") {
    withPartitionedDeltaPath() { path =>
      checkResult(
        sql(s"SHOW PARTITIONS delta.`$path` PARTITION (partition2='NotExist')"),
        Seq.empty[Row],
        Seq("partition1", "partition2"))
    }
  }

  test("show partitions with incorrect partition spec") {
    withPartitionedDeltaPath() { path =>
      val e = intercept[AnalysisException] {
        sql(s"SHOW PARTITIONS delta.`$path` PARTITION (col1=0)")
      }

      assert(e.getMessage.contains(
        s"Non-partitioning column(s) [col1] are specified for SHOW PARTITIONS"))
    }
  }

  test("show partitions of non-partitioned delta table") {
    withTempDir { tempDir =>
      spark
        .range(0, 2)
        .write
        .format("delta")
        .mode("overwrite")
        .save(tempDir.toString)

      val e = intercept[AnalysisException] {
        sql(s"SHOW PARTITIONS delta.`$tempDir`")
      }
      assert(e.getMessage.contains(
        "SHOW PARTITIONS is not allowed on a table that is not partitioned"))
    }
  }

  test("show partitions on permanent view") {
    withPartitionedDeltaTable("show_partitions") { table =>
      val viewName = "show_partition_view"
      withView(viewName) {
        sql(s"CREATE VIEW $viewName AS SELECT * FROM $table")

        val e = intercept[AnalysisException] {
          sql(s"SHOW PARTITIONS $viewName")
        }
        assert(e.getMessage.contains(
          s"$viewName is a view. 'SHOW PARTITIONS' expects a table"))
      }
    }
  }

  test("show partitions on temporary view") {
    withPartitionedDeltaTable("show_partitions") { table =>
      val viewName = "show_partition_temp_view"
      withTempView(viewName) {
        sql(s"CREATE TEMPORARY VIEW $viewName AS SELECT * FROM $table")

        val e = intercept[AnalysisException] {
          sql(s"SHOW PARTITIONS $viewName")
        }
        assert(e.getMessage.contains(
          s"$viewName is a temp view. 'SHOW PARTITIONS' expects a table"))
      }
    }
  }

  test("show partitions detail") {
    withPartitionedDeltaPath(multiParts = false) { path =>
      val expected = DeltaLog.forTable(spark, path)
        .unsafeVolatileSnapshot
        .allFiles
        .withColumn("partition1", col("partitionValues.partition1").cast(LongType))
        .groupBy("partition1")
        .agg(
          struct(
            collect_set(col("path")).as("files"),
            sum("size").as("size"),
            max(col("modificationTime")).as("modificationTime")
          ).as("partition_detail")
        ).collect()

      checkResult(
        sql(s"SHOW PARTITIONS DETAIL delta.`$path`"),
        expected,
        Seq("partition1", "partition_detail"))

      checkResult(
        sql(s"SHOW PARTITIONS DETAIL delta.`$path` PARTITION(partition1=0)"),
        expected.filter(r => r.getAs[Long]("partition1") == 0),
        Seq("partition1", "partition_detail"))
    }
  }

  test("show partitions for non-delta table") {
    withTable("show_partition_parquet") {
      withTempDir { tempPath =>

        sql(s"""CREATE TABLE show_partition_parquet(
           id bigint,
           partition_col int
          ) USING PARQUET PARTITIONED BY (partition_col) LOCATION '$tempPath'""")

        spark
          .range(10)
          .withColumn("partition_col", col("id") % 2)
          .write
          .insertInto("show_partition_parquet")

        checkResult(
          sql(s"SHOW PARTITIONS show_partition_parquet"),
          Seq(Row("partition_col=0"), Row("partition_col=1")),
          Seq("partition"))
      }
    }
  }
}

class ShowTablePartitionsSuite
  extends ShowTablePartitionsSuiteBase with DeltaSQLCommandTest
