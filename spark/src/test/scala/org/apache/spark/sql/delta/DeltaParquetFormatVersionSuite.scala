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

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.ParquetFormatVersion
import org.apache.spark.sql.delta.util.TableParquetVersionOption.writerVersionShortName
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.ParquetProperties.{WriterVersion => ParquetWriterVersion}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetOutputFormat}
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrameWriter, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.delta.test.shims.GridTestShim

class DeltaParquetFormatVersionSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with GridTestShim {
  import testImplicits._

  val v1Format: ParquetFormatVersion = ParquetFormatVersion.V1_0_0
  val v2Format: ParquetFormatVersion = ParquetFormatVersion.V2_12_0
  val v1Writer: ParquetWriterVersion = ParquetWriterVersion.PARQUET_1_0
  val v2Writer: ParquetWriterVersion = ParquetWriterVersion.PARQUET_2_0
  val tablePropertyKey: String = DeltaConfigs.PARQUET_FORMAT_VERSION.key
  val writerOptionKey: String = ParquetOutputFormat.WRITER_VERSION

  private def validateFileParquetVersion(table: String,
    formatVersion: ParquetFormatVersion): Unit = {
    val catalogTable = spark.sessionState.catalog
      .getTableMetadata(TableIdentifier(table))
    val files = DeltaLog
      .forTable(spark, catalogTable)
      .update()
      .allFiles
      .collect()
    assert(files.nonEmpty)

    files.foreach { addFile =>
      val path = ExternalCatalogUtils.unescapePathName(
        s"${catalogTable.location}/${addFile.path}"
      )
      val file = HadoopInputFile.fromPath(new Path(path), new Configuration())
      val reader = ParquetFileReader.open(file)
      try {
        val encodings = reader.getFooter
          .getBlocks
          .asScala
          .flatMap(
            _.getColumns.asScala.flatMap(_.getEncodings.asScala.toSet)
          ).toSet
        formatVersion match {
          case ParquetFormatVersion.V1_0_0 =>
            assert(!encodings.contains(Encoding.DELTA_BINARY_PACKED))
          case ParquetFormatVersion.V2_12_0 =>
            assert(encodings.contains(Encoding.DELTA_BINARY_PACKED))
          case _ =>
            fail(s"Unexpected format version: $formatVersion")
        }
      } finally {
        reader.close()
      }
    }
  }

  private def writeDF: DataFrameWriter[Row] = {
    (1 to 100)
      .map(i => (i, i.toString))
      .toDF("c0", "c1")
      .withColumn("c2", current_timestamp())
      .repartition(10)
      .write
      .format("delta")
  }

  private def getProperties(tableName: String): Map[String, String] =
    sql(s"describe detail $tableName")
      .collect()
      .head
      .getAs[Map[String, String]]("properties")

  def validateProperties(
    table: String,
    includedProps: Map[String, String],
    excludedProps: String*
  ): Unit = {
    val props = getProperties(table)
    includedProps.foreach { case (k, v) => assert(props.get(k).contains(v)) }
    excludedProps.foreach { k => assert(!props.contains(k)) }
  }

  /**
   * Guard for tests that need SPARK-56414 (per-write options overriding session conf in Parquet
   * writes). Databricks Spark always has the fix; OSS Spark needs >= 4.2.
   */
  private def assumeSpark56414Available(): Unit = {
    assume(spark.version >= "4.2",
      "Requires SPARK-56414 (per-write options override session conf in Parquet writes)")
  }

  gridTest("DataFrame options are respected")(Seq(
    (v1Writer, v1Format),
    (v2Writer, v2Format)
  )) { case (writerVersion, formatVersion) =>
    if (formatVersion == v2Format) assumeSpark56414Available()
    withTable("t1") {
      writeDF.option(writerOptionKey, writerVersionShortName(writerVersion)).saveAsTable("t1")
      validateProperties("t1", Map.empty, tablePropertyKey, writerOptionKey)
      validateFileParquetVersion("t1", formatVersion)
    }
  }

  gridTest("CREATE TABLE options are respected")(Seq(
    (v1Writer, v1Format),
    (v2Writer, v2Format)
  )) { case (writerVersion, formatVersion) =>
    withTable("t1") {
      sql(s"""CREATE TABLE t1 (c0 INT, c1 STRING, c2 TIMESTAMP) USING DELTA
      |OPTIONS ('${writerOptionKey}' = '${writerVersionShortName(writerVersion)}')
      """.stripMargin)
      writeDF.mode("append").saveAsTable("t1")
      validateProperties("t1", Map(writerOptionKey -> writerVersionShortName(writerVersion)),
        tablePropertyKey)
      validateFileParquetVersion("t1", formatVersion)
    }
  }

  gridTest("CREATE TABLE TBLPROPERTIES are respected")(Seq(v1Format, v2Format)) { formatVersion =>
    if (formatVersion == v2Format) assumeSpark56414Available()
    withTable("t1") {
      sql(s"""CREATE TABLE t1 (c0 INT, c1 STRING, c2 TIMESTAMP) USING DELTA
      |TBLPROPERTIES ('${tablePropertyKey}' = '${formatVersion.getVersion()}')
      """.stripMargin)
      writeDF.mode("append").saveAsTable("t1")
      validateProperties("t1", Map(tablePropertyKey -> formatVersion.getVersion()),
        writerOptionKey)
      validateFileParquetVersion("t1", formatVersion)
    }
  }

  gridTest("Spark conf is respected")(Seq(
    (v1Writer, v1Format),
    (v2Writer, v2Format)
  )) { case (writerVersion, formatVersion) =>
    withTable("t1") {
      withSQLConf(
        writerOptionKey -> writerVersionShortName(writerVersion)
      ) {
        writeDF.saveAsTable("t1")
        validateProperties("t1", Map.empty, tablePropertyKey, writerOptionKey)
        validateFileParquetVersion("t1", formatVersion)
      }
    }
  }

  gridTest("Table property can be an arbitrary version string")(Seq(
    ("1.1.1", v1Format),
    ("99.99.99", v2Format)
  )) { case (formatVersionStr, formatVersion) =>
    if (formatVersion == v2Format) assumeSpark56414Available()
    withTable("t1") {
      sql(s"""CREATE TABLE t1 (c0 INT, c1 STRING, c2 TIMESTAMP) USING DELTA
      |TBLPROPERTIES ('${tablePropertyKey}' = '${formatVersionStr}')
      """.stripMargin)
      writeDF.mode("append").saveAsTable("t1")
      validateProperties("t1", Map(tablePropertyKey -> formatVersionStr),
        writerOptionKey)
      validateFileParquetVersion("t1", formatVersion)
    }
  }

  test("DataFrame option wins") {
    withTable("t1") {
      sql(s"""CREATE TABLE t1 (c0 INT, c1 STRING, c2 TIMESTAMP) USING DELTA
      |TBLPROPERTIES ('${tablePropertyKey}' = '${v2Format.getVersion()}')
      """.stripMargin)
      validateProperties("t1", Map(tablePropertyKey -> v2Format.getVersion()))
      writeDF.option(writerOptionKey, writerVersionShortName(v1Writer))
        .mode("append").saveAsTable("t1")
      validateFileParquetVersion("t1", v1Format)
    }
  }

  test("CREATE TABLE option wins") {
    withTable("t1") {
      withSQLConf(
        writerOptionKey -> writerVersionShortName(v2Writer)
      ) {
        sql(s"""CREATE TABLE t1 (c0 INT, c1 STRING, c2 TIMESTAMP) USING DELTA
        |OPTIONS ('${writerOptionKey}' = '${writerVersionShortName(v1Writer)}')
        """.stripMargin)
        validateProperties("t1", Map(writerOptionKey -> writerVersionShortName(v1Writer)))
        writeDF.mode("append").saveAsTable("t1")
        validateFileParquetVersion("t1", v1Format)
      }
    }
  }

  test("CREATE TABLE property wins") {
    assumeSpark56414Available()
    withTable("t1") {
      withSQLConf(
        writerOptionKey -> writerVersionShortName(v2Writer)
      ) {
        sql(s"""CREATE TABLE t1 (c0 INT, c1 STRING, c2 TIMESTAMP) USING DELTA
        |TBLPROPERTIES ('${tablePropertyKey}' = '${v1Format.getVersion()}')
        """.stripMargin)
        validateProperties("t1", Map(tablePropertyKey -> v1Format.getVersion()))
        writeDF.mode("append").saveAsTable("t1")
        validateFileParquetVersion("t1", v1Format)
      }
    }
  }

  test("ALTER TABLE can change the Parquet version") {
    withTable("t1") {
      sql(s"""CREATE TABLE t1 (c0 INT, c1 STRING, c2 TIMESTAMP) USING DELTA
      |TBLPROPERTIES ('${tablePropertyKey}' = '${v2Format.getVersion()}')
      """.stripMargin)
      validateProperties("t1", Map(tablePropertyKey -> v2Format.getVersion()))
      sql(s"ALTER TABLE t1 SET TBLPROPERTIES ('${tablePropertyKey}' = '${v1Format.getVersion()}')")
      validateProperties("t1", Map(tablePropertyKey -> v1Format.getVersion()))
      writeDF.mode("append").saveAsTable("t1")
      validateFileParquetVersion("t1", v1Format)
    }
  }
}
