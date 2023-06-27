/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import java.util.UUID

import org.apache.spark.sql.Row
import org.apache.spark.util.Utils

trait MergeConf extends BenchmarkConf {
  def scaleInGB: Int
  def tableName: String = "web_returns"
  def userDefinedDbName: Option[String]
  def dbName: String = userDefinedDbName.getOrElse(s"merge_sf${scaleInGB}")
  def dbLocation: String = dbLocation(dbName)
}

case class MergeBenchmarkConf(
     scaleInGB: Int = 0,
     userDefinedDbName: Option[String] = None,
     iterations: Int = 3,
     benchmarkPath: Option[String] = None) extends MergeConf {
}

object MergeBenchmarkConf {
  import scopt.OParser
  private val builder = OParser.builder[MergeBenchmarkConf]
  private val argParser = {
    import builder._
    OParser.sequence(
      programName("Merge Benchmark"),
      opt[String]("scale-in-gb")
        .required()
        .valueName("<scale of benchmark in GBs>")
        .action((x, c) => c.copy(scaleInGB = x.toInt))
        .text("Scale factor in GBs of the TPCDS benchmark"),
      opt[String]("benchmark-path")
        .required()
        .valueName("<cloud storage path>")
        .action((x, c) => c.copy(benchmarkPath = Some(x)))
        .text("Cloud path to be used for creating table and generating reports"),
      opt[String]("iterations")
        .optional()
        .valueName("<number of iterations>")
        .action((x, c) => c.copy(iterations = x.toInt))
        .text("Number of times to run the queries"))
  }

  def parse(args: Array[String]): Option[MergeBenchmarkConf] = {
    OParser.parse(argParser, args, MergeBenchmarkConf())
  }
}

class MergeBenchmark(conf: MergeBenchmarkConf) extends Benchmark(conf) {
  /**
   * Runs every merge test case multiple times and records the duration.
   */
  override def runInternal(): Unit = {
    for ((k, v) <- extraConfs) spark.conf.set(k, v)
    spark.sparkContext.setLogLevel("WARN")
    log("All configs:\n\t" + spark.conf.getAll.toSeq.sortBy(_._1).mkString("\n\t"))
    spark.sql(s"USE ${conf.dbName}")

    val targetRowCount = spark.read.table(s"`${conf.dbName}`.`target_${conf.tableName}`").count

    for (iteration <- 1 to conf.iterations) {
      MergeTestCases.testCases.foreach { runMerge(_, targetRowCount, iteration = Some(iteration)) }
    }
    val results = getQueryResults().filter(_.name.startsWith("q"))
    if (results.forall(x => x.errorMsg.isEmpty && x.durationMs.nonEmpty) ) {
      val medianDurationSecPerQuery = results.groupBy(_.name).map { case (q, results) =>
        assert(results.length == conf.iterations)
        val medianMs = Utils.median(results.map(_.durationMs.get), alreadySorted = false)
        (q, medianMs / 1000.0)
      }
      val sumOfMedians = medianDurationSecPerQuery.values.sum
      reportExtraMetric("merge-result-seconds", sumOfMedians)
    }
  }

  /**
   * Merge test runner performing the following steps:
   * - Clone a fresh target table.
   * - Run the merge test case.
   * - Check invariants.
   * - Drop the cloned table.
   */
  protected def runMerge(
      testCase: MergeTestCase,
      targetRowCount: Long,
      iteration: Option[Int] = None,
      printRows: Boolean = false,
      ignoreError: Boolean = true): Seq[Row] = synchronized {
    withCloneTargetTable(testCase.name) { targetTable =>
      val result = super.runQuery(
        testCase.sqlCmd(targetTable),
        testCase.name,
        iteration,
        printRows,
        ignoreError)
      testCase.validate(result, targetRowCount)
      result
    }
  }

  /**
   * Clones the target table before each test case to use a fresh target table and drops the clone
   * afterwards.
   */
  protected def withCloneTargetTable[T](testCaseName: String)(f: String => T): T = {
    val target = s"`${conf.dbName}`.`target_${conf.tableName}`"
    val clonedTableName = s"`${conf.dbName}`.`${conf.tableName}_${generateShortUUID()}`"
    runQuery(s"CREATE TABLE $clonedTableName SHALLOW CLONE $target", s"clone-target-$testCaseName")
    try {
      f(clonedTableName)
    } finally {
      runQuery(s"DROP TABLE IF EXISTS $clonedTableName", s"drop-target-clone-$testCaseName")
    }
  }

  protected def generateShortUUID(): String =
    UUID.randomUUID.toString.replace("-", "_").take(8)
}

object MergeBenchmark {
  def main(args: Array[String]): Unit = {
    MergeBenchmarkConf.parse(args).foreach { conf =>
      new MergeBenchmark(conf).run()
    }
  }
}
