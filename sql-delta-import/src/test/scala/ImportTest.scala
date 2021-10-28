/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.connectors.spark.jdbc

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, BeforeAndAfterEach}

class ImportTest extends AnyFunSuite with BeforeAndAfterAll {

  private def initDataSource (conn: Connection) = {
    conn.prepareStatement("create schema test").executeUpdate()
    conn.prepareStatement(
      """
    create table test.tbl(
      id TINYINT,
      status SMALLINT,
      ts TIMESTAMP,
      title VARCHAR)"""
    ).executeUpdate()
    conn.prepareStatement(
      """
    insert into test.tbl(id, status, ts, title ) VALUES
    (1, 2, parsedatetime('01-02-2021 01:02:21', 'dd-MM-yyyy hh:mm:ss'),'lorem ipsum'),
    (3, 4, parsedatetime('03-04-2021 03:04:21', 'dd-MM-yyyy hh:mm:ss'),'lorem'),
    (5, 6, parsedatetime('05-06-2021 05:06:21', 'dd-MM-yyyy hh:mm:ss'),'ipsum'),
    (7, 8, parsedatetime('07-08-2021 07:08:21', 'dd-MM-yyyy hh:mm:ss'),'Lorem Ipsum')
    """
    ).executeUpdate()
  }

  implicit lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("spark session")
    .config("spark.sql.shuffle.partitions", "10")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()

  val url = "jdbc:h2:mem:testdb;DATABASE_TO_UPPER=FALSE"

  DriverManager.registerDriver(new org.h2.Driver())

  val conn = DriverManager.getConnection(url)
  initDataSource(conn)

  override def afterAll() {
    spark.catalog.clearCache()
    spark.sharedState.cacheManager.clearCache()
    conn.close()
  }

  val chunks = 2

  test("import data into a delta table") {
    spark.sql("DROP TABLE IF EXISTS tbl")
    spark.sql("""
      CREATE TABLE tbl (id INT, status INT, title STRING)
      USING DELTA
      LOCATION "spark-warehouse/tbl"
    """)


    JDBCImport(url, ImportConfig("test.tbl", "tbl", "id", chunks)).run()

    // since we imported data without any optimizations number of
    // read partitions should equal number of chunks used during import
    assert(spark.table("tbl").rdd.getNumPartitions == chunks)

    val imported = spark.sql("select * from tbl")
      .collect()
      .sortBy(a => a.getAs[Int]("id"))

    assert(imported.length == 4)
    assert(imported.map(a => a.getAs[Int]("id")).toSeq == Seq(1, 3, 5, 7))
    assert(imported.map(a => a.getAs[Int]("status")).toSeq == Seq(2, 4, 6, 8))
    assert(imported.map(a => a.getAs[String]("title")).toSeq ==
      Seq("lorem ipsum", "lorem", "ipsum", "Lorem Ipsum"))
  }

  test("transform data before importing it into a delta table") {
    spark.sql("DROP TABLE IF EXISTS tbl2")
    spark.sql("""
      CREATE TABLE tbl2 (id INT, status INT, ts STRING, title STRING)
      USING DELTA
      LOCATION "spark-warehouse/tbl2"
    """)

    val timeStampsToStrings : DataFrame => DataFrame = source => {
      val tsCols = source.schema.fields.filter(_.dataType == DataTypes.TimestampType).map(_.name)
      tsCols.foldLeft(source)((df, name) =>
        df.withColumn(name, from_unixtime(unix_timestamp(col(name)), "yy-MM-dd HH:mm")))
    }

    val transforms = new DataTransforms(Seq(
      a => a.withColumn("title", upper(col("title"))),
      timeStampsToStrings
    ))

    JDBCImport(
      jdbcUrl = url,
      importConfig = ImportConfig("test.tbl", "tbl2", "id", 2),
      dataTransforms = transforms).run()

    val imported = spark.sql("select * from tbl2")
      .collect()
      .sortBy(a => a.getAs[Int]("id"))

    assert(imported.length == 4)
    assert(imported.map(a => a.getAs[String]("title")).toSeq ==
      Seq("LOREM IPSUM", "LOREM", "IPSUM", "LOREM IPSUM"))

    assert(imported.map(a => a.getAs[String]("ts")).toSeq ==
      Seq("21-02-01 01:02", "21-04-03 03:04", "21-06-05 05:06", "21-08-07 07:08"))
  }

}
