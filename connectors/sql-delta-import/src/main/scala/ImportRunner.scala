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

import org.apache.spark.sql.SparkSession
import org.rogach.scallop.{ScallopConf, ScallopOption}

/**
 * Spark app that wraps functionality of JDBCImport and exposes configuration as command line args
 */
object ImportRunner {

   def main(args: Array[String]): Unit = {
    val config = new ImportRunnerConfig(args)

    implicit val spark = SparkSession
      .builder()
      .appName("sql-delta-import")
      .getOrCreate()

    val importConfig = ImportConfig(
      config.source(),
      config.destination(),
      config.splitBy(),
      config.chunks())

    val transforms = new DataTransforms(Seq.empty)

   JDBCImport(
      jdbcUrl = config.jdbcUrl(),
      importConfig = importConfig,
      dataTransforms = transforms
   ).run
  }
}

class ImportRunnerConfig(arguments: Seq[String]) extends ScallopConf(arguments) {
  val className = "io.delta.connectors.spark.jdbc.ImportRunner"
  val jarName = "sql-delta-import.jar"

  banner("\nOptions:\n")
  footer(
    s"""Usage:
      |spark-submit {spark options} --class $className $jarName OPTIONS
      |""".stripMargin)

  override def mainOptions: Seq[String] = Seq("jdbcUrl", "source", "destination", "splitBy")

  val jdbcUrl: ScallopOption[String] = opt[String](required = true)
  val source: ScallopOption[String] = opt[String](required = true)
  val destination: ScallopOption[String] = opt[String](required = true)
  val splitBy: ScallopOption[String] = opt[String](required = true)
  val chunks: ScallopOption[Int] = opt[Int](default = Some(10))

  verify()
}
