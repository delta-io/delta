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

package io.delta.hive.test

import java.io.{Closeable, File}
import java.nio.file.Files
import java.util.{Locale, TimeZone}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaHelper
// scalastyle:off funsuite
import org.scalatest.{BeforeAndAfterAll, FunSuite}

// TODO Yarn is using log4j2. Disable its verbose logs.
trait HiveTest extends FunSuite with BeforeAndAfterAll {
  private val tempPath = Files.createTempDirectory(this.getClass.getSimpleName).toFile

  private var driver: Driver = _
  private var cluster: Closeable = _

  // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
  TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
  // Add Locale setting
  Locale.setDefault(Locale.US)

  override def beforeAll(): Unit = {
    super.beforeAll()
    val warehouseDir = new File(tempPath, "warehouse")
    val metastoreDir = new File(tempPath, "metastore_db")
    val hiveJarsDir = new File(tempPath, "hive_jars")
    val conf = new HiveConf()
    conf.set("hive.execution.engine", engine)
    conf.set("hive.user.install.directory", hiveJarsDir.getCanonicalPath)
    // Disable schema verification and allow schema auto-creation in the
    // Derby database, in case the config for the metastore is set otherwise.
    // Without these settings, starting the client fails with
    // MetaException(message:Version information not found in metastore.)t
    conf.set("hive.metastore.schema.verification", "false")
    conf.set("datanucleus.schema.autoCreateAll", "true")
    // if hive.fetch.task.conversion set to none, "hive.input.format" and "hive.tez.input.format"
    // should be "io.delta.hive.HiveInputFormat".
    conf.set("hive.fetch.task.conversion", "none")
    conf.set("hive.input.format", "io.delta.hive.HiveInputFormat")
    conf.set("hive.tez.input.format", "io.delta.hive.HiveInputFormat")
    conf.set(
      "javax.jdo.option.ConnectionURL",
      s"jdbc:derby:memory:;databaseName=${metastoreDir.getCanonicalPath};create=true")
    conf.set("hive.metastore.warehouse.dir", warehouseDir.getCanonicalPath)
    val fs = FileSystem.getLocal(conf)
    cluster = createCluster(fs.getUri.toString, conf, tempPath)
    setupConfiguration(conf)
    val db = Hive.get(conf)
    SessionState.start(new CliSessionState(conf))
    driver = new Driver(conf)
  }

  def engine: String

  def createCluster(namenode: String, conf: Configuration, tempPath: File): Closeable

  def setupConfiguration(conf: Configuration): Unit = {}

  override def afterAll() {
    if (cluster != null) {
      cluster.close()
    }
    driver.close()
    driver.destroy()
    JavaUtils.deleteRecursively(tempPath)
    // TODO Remove leaked "target/MiniMRCluster-XXX" directories
    super.afterAll()
  }

  def runQuery(query: String): Seq[String] = {
    val response = driver.run(query)
    if (response.getResponseCode != 0) {
      throw new Exception(s"failed to run '$query': ${response.getErrorMessage}")
    }
    val result = new java.util.ArrayList[String]()
    if (driver.getResults(result)) {
      result.asScala
    } else {
      Nil
    }
  }

  /** Run the Hive query and check the result with the expected answer. */
  def checkAnswer[T <: Product](query: String, expected: Seq[T]): Unit = {
    val actualAnswer = runQuery(query).sorted
    val expectedAnswer = expected.map(_.productIterator.mkString("\t")).sorted
    if (actualAnswer != expectedAnswer) {
      fail(
        s"""Answers do not match.
           |Query:
           |
           |$query
           |
           |Expected:
           |
           |${expectedAnswer.mkString("\n")}
           |
           |Actual:
           |
           |${actualAnswer.mkString("\n")}
           |
         """.stripMargin)
    }
  }

  /**
   * Check whether the `filter` is pushed into TableScan's filterExpr field and also verify the
   * answer.
   */
  def checkFilterPushdown[T <: Product](query: String, filter: String, expected: Seq[T]): Unit = {
    // `explain` in Tez doesn't show TableScan's filterExpr field, so we use `explain extended`.
    assert(runQuery(s"explain extended $query").mkString("\n").contains(s"filterExpr: $filter"))
    checkAnswer(query, expected)
  }

  /**
   * Drops table `tableName` after calling `f`.
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        runQuery(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected def withTempDir(f: File => Unit): Unit = {
    val dir = Files.createTempDirectory("hiveondelta").toFile

    try f(dir) finally {
      JavaUtils.deleteRecursively(dir)
    }
  }

  protected def withSparkSession(f: SparkSession => Unit): Unit = {
    val spark = DeltaHelper.spark
    try f(spark) finally {
      // Clean up resources so that we can use new DeltaLog and SparkSession
      spark.stop()
      DeltaLog.clearCache()
    }
  }
}
