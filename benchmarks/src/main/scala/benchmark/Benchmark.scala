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

import scala.collection.mutable
import java.net.URI
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

import scala.language.postfixOps
import scala.sys.process._
import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import com.fasterxml.jackson.databind.{DeserializationFeature, MapperFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

import org.apache.spark.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col}

trait BenchmarkConf extends Product {
  /** Cloud path where benchmark data is going to be written. */
  def benchmarkPath: Option[String]

  /** Get the database location given the database name and the benchmark path. */
  def dbLocation(dbName: String, suffix: String = ""): String = {
    benchmarkPath.map(p => s"$p/databases/${dbName}_${suffix}").getOrElse {
      throw new IllegalArgumentException("Benchmark path must be specified")
    }
  }

  /** Cloud path where benchmark reports will be uploaded. */
  def reportUploadPath: String = {
    benchmarkPath.map(p => s"$p/reports/").getOrElse {
      throw new IllegalArgumentException("Benchmark path must be specified")
    }
  }
  def jsonReportUploadPath: String = s"$reportUploadPath/json/"
  def csvReportUploadPath: String = s"$reportUploadPath/csv/"

  /** Get the benchmark conf details as a map. */
  def asMap: Map[String, String] = SparkUtils.caseClassToMap(this)
}

@JsonPropertyOrder(alphabetic=true)
case class QueryResult(
    name: String,
    iteration: Option[Int],
    durationMs: Option[Long],
    errorMsg: Option[String])

@JsonPropertyOrder(alphabetic=true)
case class SparkEnvironmentInfo(
    @JsonPropertyOrder(alphabetic=true)
    sparkBuildInfo: Map[String, String],
    @JsonPropertyOrder(alphabetic=true)
    runtimeInfo: Map[String, String],
    @JsonPropertyOrder(alphabetic=true)
    sparkProps: Map[String, String],
    @JsonPropertyOrder(alphabetic=true)
    hadoopProps: Map[String, String],
    @JsonPropertyOrder(alphabetic=true)
    systemProps: Map[String, String],
    @JsonPropertyOrder(alphabetic=true)
    classpathEntries: Map[String, String])

@JsonPropertyOrder(alphabetic=true)
case class BenchmarkReport(
    @JsonPropertyOrder(alphabetic=true)
    benchmarkSpecs: Map[String, String],
    queryResults: Array[QueryResult],
    extraMetrics: Map[String, Double],
    sparkEnvInfo: SparkEnvironmentInfo)

/**
 * Base class for any benchmark with the core functionality of measuring SQL query durations
 * and printing the details as json in a report file.
 */
abstract class Benchmark(private val conf: BenchmarkConf) {

  /* Methods that implementations should override. */

  protected def runInternal(): Unit

  /* Fields and methods that implementations should not have to override */

  final protected lazy val spark = {
    val s = SparkSession.builder()
      .config("spark.ui.proxyBase", "")
      .getOrCreate()
    log("Spark started with configuration:\n" +
      s.conf.getAll.toSeq.sortBy(_._1).map(x => x._1 + ": " + x._2).mkString("\t", "\n\t", "\n"))
    s.sparkContext.setLogLevel("WARN")
    sys.props.update("spark.ui.proxyBase", "")
    s
  }

  private val queryResults = new mutable.ArrayBuffer[QueryResult]
  private val extraMetrics = new mutable.HashMap[String, Double]

  protected def run(): Unit = {
    try {
      log("=" * 80)
      log("=" * 80)
      runInternal()
      log("=" * 80)
    } finally {
      generateReport()
    }
    println(s"SUCCESS")
  }

  protected def runQuery(
      sqlCmd: String,
      queryName: String = "",
      iteration: Option[Int] = None,
      printRows: Boolean = false,
      ignoreError: Boolean = true): DataFrame = synchronized {
    val iterationStr = iteration.map(i => s" - iteration $i").getOrElse("")
    var banner = s"$queryName$iterationStr"
    if (banner.trim.isEmpty) {
      banner = sqlCmd.split("\n")(0).trim + (if (sqlCmd.split("\n").size > 1) "..." else "")
    }
    log("=" * 80)
    log(s"START: $banner")
    log("SQL: " + sqlCmd.replaceAll("\n\\s*", " "))
    spark.sparkContext.setJobGroup(banner, banner, interruptOnCancel = true)
    try {
      val before = System.nanoTime()
      val df = spark.sql(sqlCmd)
      val r = df.collect()
      val after = System.nanoTime()
      if (printRows) df.show(false)
      val durationMs = (after - before) / (1000 * 1000)
      queryResults += QueryResult(queryName, iteration, Some(durationMs), errorMsg = None)
      log(s"END took $durationMs ms: $banner")
      log("=" * 80)
      df
    } catch {
      case NonFatal(e) =>
        log(s"ERROR: $banner\n${e.getMessage}")
        queryResults +=
          QueryResult(queryName, iteration, durationMs = None, errorMsg = Some(e.getMessage))
        if (!ignoreError) throw e else spark.emptyDataFrame
    }
  }


  protected def runFunc(
      queryName: String = "",
      iteration: Option[Int] = None,
      ignoreError: Boolean = true)(f: => Unit): Unit = synchronized {
    val iterationStr = iteration.map(i => s" - iteration $i").getOrElse("")
    var banner = s"$queryName$iterationStr"
    log("=" * 80)
    log(s"START: $banner")
    spark.sparkContext.setJobGroup(banner, banner, interruptOnCancel = true)
    try {
      val before = System.nanoTime()
      f
      val after = System.nanoTime()
      val durationMs = (after - before) / (1000 * 1000)
      queryResults += QueryResult(queryName, iteration, Some(durationMs), errorMsg = None)
      log(s"END took $durationMs ms: $banner")
      log("=" * 80)
    } catch {
      case NonFatal(e) =>
        log(s"ERROR: $banner\n${e.getMessage}")
        queryResults +=
          QueryResult(queryName, iteration, durationMs = None, errorMsg = Some(e.getMessage))
        if (!ignoreError) throw e else spark.emptyDataFrame
    }
  }


  protected def reportExtraMetric(name: String, value: Double): Unit = synchronized {
    extraMetrics += (name -> value)
  }

  protected def getQueryResults(): Array[QueryResult] = synchronized { queryResults.toArray }

  private def generateJSONReport(report: BenchmarkReport): Unit = synchronized {
    import Benchmark._

    val resultJson = toPrettyJson(report)
    val resultFileName =
      if (benchmarkId.trim.isEmpty) "report.json" else s"$benchmarkId-report.json"
    val reportLocalPath = Paths.get(resultFileName).toAbsolutePath()
    Files.write(reportLocalPath, resultJson.getBytes(StandardCharsets.UTF_8))
    println(s"RESULT:\n$resultJson")
    uploadFile(reportLocalPath.toString, conf.jsonReportUploadPath)
  }

  private def generateCSVReport(): Unit = synchronized {
    val csvHeader = "name,iteration,durationMs"
    val csvRows = queryResults.map { r =>
      s"${r.name},${r.iteration.getOrElse(1)},${r.durationMs.getOrElse(-1)}"
    }
    val csvText = (Seq(csvHeader) ++ csvRows).mkString("\n")
    val resultFileName =
      if (benchmarkId.trim.isEmpty) "report.csv" else s"$benchmarkId-report.csv"
    val reportLocalPath = Paths.get(resultFileName).toAbsolutePath()
    Files.write(reportLocalPath, csvText.getBytes(StandardCharsets.UTF_8))
    uploadFile(reportLocalPath.toString, conf.csvReportUploadPath)
  }

  private def generateReport(): Unit = synchronized {
    val report = BenchmarkReport(
      benchmarkSpecs = conf.asMap + ("benchmarkId" -> benchmarkId),
      queryResults = queryResults.toArray,
      extraMetrics = extraMetrics.toMap,
      sparkEnvInfo = SparkUtils.getEnvironmentInfo(spark.sparkContext)
    )
    generateJSONReport(report)
    generateCSVReport()
  }

  private def uploadFile(localPath: String, targetPath: String): Unit = {
    val targetUri = new URI(targetPath)
    val sanitizedTargetPath = targetUri.normalize().toString
    val scheme = new URI(targetPath).getScheme
    try {
      if (scheme.equals("s3")) s"aws s3 cp $localPath $sanitizedTargetPath/" !
      else if (scheme.equals("gs")) s"gsutil cp $localPath $sanitizedTargetPath/" !
      else throw new IllegalArgumentException(String.format("Unsupported scheme %s.", scheme))

      println(s"FILE UPLOAD: Uploaded $localPath to $sanitizedTargetPath")
    } catch {
      case NonFatal(e) =>
        log(s"FILE UPLOAD: Failed to upload $localPath to $sanitizedTargetPath: $e")
    }
  }

  protected def benchmarkId: String =
    sys.env.getOrElse("BENCHMARK_ID", spark.conf.getOption("spark.benchmarkId").getOrElse(""))

  protected def log(str: => String): Unit = {
    println(s"${java.time.LocalDateTime.now} $str")
  }
}

object Benchmark {
  private lazy val mapper = {
    val _mapper = new ObjectMapper with ScalaObjectMapper
    _mapper.setSerializationInclusion(Include.NON_ABSENT)
    _mapper.enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)
    _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _mapper.registerModule(DefaultScalaModule)
    _mapper
  }

  def toJson[T: Manifest](obj: T): String = {
    mapper.writeValueAsString(obj)
  }

  def toPrettyJson[T: Manifest](obj: T): String = {
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj)
  }
}
