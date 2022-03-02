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
package benchmark

case class TestBenchmarkConf(
    dbName: Option[String] = None,
    benchmarkPath: Option[String] = None) extends BenchmarkConf

object TestBenchmarkConf {
  import scopt.OParser
  private val builder = OParser.builder[TestBenchmarkConf]
  private val argParser = {
    import builder._
    OParser.sequence(
      programName("Test Benchmark"),
      opt[String]("test-param")
        .required()
        .action((x, c) => c) // ignore
        .text("Name of the target database to create with TPC-DS tables in necessary format"),
      opt[String]("benchmark-path")
        .optional()
        .action((x, c) => c.copy(benchmarkPath = Some(x)))
        .text("Cloud path to be used for creating table and generating reports"),
      opt[String]("db-name")
        .optional()
        .action((x, c) => c.copy(dbName = Some(x)))
        .text("Name of the test database to create")
    )
  }

  def parse(args: Array[String]): Option[TestBenchmarkConf] = {
    OParser.parse(argParser, args, TestBenchmarkConf())
  }
}

class TestBenchmark(conf: TestBenchmarkConf) extends Benchmark(conf) {
  def runInternal(): Unit = {
    // Test Spark SQL
    runQuery("SELECT 1 AS X", "sql-test")
    if (conf.benchmarkPath.isEmpty) {
      log("Skipping the delta read / write test as benchmark path has not been provided")
      return
    }

    val dbName = conf.dbName.getOrElse(benchmarkId.replaceAll("-", "_"))
    val dbLocation = conf.dbLocation(dbName)

    // Run database management tests
    runQuery("SHOW DATABASES", "db-list-test")
    runQuery(s"""CREATE DATABASE IF NOT EXISTS $dbName LOCATION "$dbLocation" """, "db-create-test")
    runQuery(s"USE $dbName", "db-use-test")

    // Run table tests
    val tableName = "test"
    runQuery(s"DROP TABLE IF EXISTS $tableName", "table-drop-test")
    runQuery(s"CREATE TABLE $tableName USING delta SELECT 1 AS x", "table-create-test")
    runQuery(s"SELECT * FROM $tableName", "table-query-test")
  }
}

object TestBenchmark {
  def main(args: Array[String]): Unit = {
    println("All command line args = " + args.toSeq)
    TestBenchmarkConf.parse(args).foreach { conf =>
      new TestBenchmark(conf).run()
    }
  }
}
