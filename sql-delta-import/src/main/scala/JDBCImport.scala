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

package io.delta.connectors.spark.jdbc

import java.util.Properties

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

/**
 * Class that contains JDBC source, read parallelism params and target table name
 *
 *  @param source       - JDBC source table
 *  @param destination  - Delta target database.table
 *  @param splitBy      - column by which to split source data while reading
 *  @param chunks       - to how many chunks split jdbc source data
 */
case class ImportConfig(source: String, destination: String, splitBy: String, chunks: Int) {
  val bounds_sql = s"""
  (select min($splitBy) as lower_bound, max($splitBy) as upper_bound from $source) as bounds
  """
}

/**
 * Class that does reading from JDBC source, transform and writing to Delta table
 *
 *  @param jdbcUrl       - url connecting string for jdbc source
 *  @param importConfig  - case class that contains source read parallelism params and target table
 *  @param jdbcParams    - additional JDBC session params like isolation level, perf tuning,
 *                       net wait params etc...
 *  @param dataTransform - contains function that we should apply to transform our source data
 */
class JDBCImport(jdbcUrl: String,
                 importConfig: ImportConfig,
                 jdbcParams: Map[String, String] = Map(),
                 dataTransform: DataTransforms)
                (implicit val spark: SparkSession) {

  import spark.implicits._

  implicit def mapToProperties(m: Map[String, String]): Properties = {
    val properties = new Properties()
    m.foreach(pair => properties.put(pair._1, pair._2))
    properties
  }

  // list of columns to import is obtained from schema of destination delta table
  private lazy val targetColumns = DeltaTable
    .forName(importConfig.destination)
    .toDF
    .schema
    .fieldNames

  private lazy val sourceDataframe = readJDBCSourceInParallel()
    .select(targetColumns.map(col): _*)

  /**
   * obtains lower and upper bound of source table and uses those values to read in a JDBC dataframe
   * @return a dataframe read from source table
   */
  private def readJDBCSourceInParallel(): DataFrame = {

    val (lower, upper) = spark
      .read
      .jdbc(jdbcUrl, importConfig.bounds_sql, jdbcParams)
      .as[(Option[Long], Option[Long])]
      .take(1)
      .map { case (a, b) => (a.getOrElse(0L), b.getOrElse(0L)) }
      .head

    spark.read.jdbc(
      jdbcUrl,
      importConfig.source,
      importConfig.splitBy,
      lower,
      upper,
      importConfig.chunks,
      jdbcParams)
  }

  private implicit class DataFrameExtensionOps(df: DataFrame) {

    def runTransform(): DataFrame = dataTransform.runTransform(sourceDataframe)

    def writeToDelta(deltaTableToWrite: String): Unit = df
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .insertInto(deltaTableToWrite)
  }

  /**
   * Runs transform against dataframe read from jdbc and writes it to Delta table
   */
  def run(): Unit = {
    sourceDataframe
      .runTransform()
      .writeToDelta(importConfig.destination)
  }
}

object JDBCImport {
  def apply(jdbcUrl: String,
            importConfig: ImportConfig,
            jdbcParams: Map[String, String] = Map(),
            dataTransforms: DataTransforms = new DataTransforms(Seq.empty))
           (implicit spark: SparkSession): JDBCImport = {

    new JDBCImport(jdbcUrl, importConfig, jdbcParams, dataTransforms)
  }
}
