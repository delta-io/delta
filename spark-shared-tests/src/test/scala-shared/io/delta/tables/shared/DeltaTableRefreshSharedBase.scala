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

package io.delta.tables.shared

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}

/**
 * Shared base for the repeated table access refresh tests, compiled into both the classic `spark`
 * and the `spark-connect/client` modules (wired via [[Test / unmanagedSourceDirectories]] in
 * build.sbt). It self-types only to [[AnyFunSuite]] and uses only the unified
 * [[org.apache.spark.sql]] API.
 *
 * External writes are simulated by writing commit files directly into the table's `_delta_log` on
 * the local filesystem, which the Connect client and server share.
 *
 * Concrete suites supply [[spark]], [[checkAnswer]], and [[withTable]].
 */
trait DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  protected def spark: SparkSession

  protected def v2EnableMode: String = "NONE"

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit
  protected def withTable(tableNames: String*)(f: => Unit): Unit

  /**
   * Asserts `exception` carries `condition`, tolerating a Connect-wrapped error (the condition may
   * appear in the message rather than as the error condition) and classic error subclasses.
   */
  protected def checkError(exception: SparkThrowable, condition: String): Unit = {
    val cond = exception.getCondition
    if (cond != condition) {
      assert(exception.asInstanceOf[Exception].getMessage.contains(condition),
        s"Expected error condition '$condition' but got '$cond' " +
        s"and message does not contain it either")
    }
  }

  protected def writerSql(sqlText: String): Unit = spark.sql(sqlText)

  protected def createSimpleTable(tableRef: String): Unit =
    spark.sql(s"CREATE TABLE $tableRef (id INT, salary INT) USING delta")

  protected def insertInitialData(tableRef: String): Unit =
    spark.sql(s"INSERT INTO $tableRef VALUES (1, 100)")

  /** Inserts the initial `(1, 100)` row into `tableRef` and asserts the first read. */
  private def seedInitialRow(tableRef: String): Unit = {
    insertInitialData(tableRef)
    checkAnswer(spark.sql(s"SELECT * FROM $tableRef"), Seq(Row(1, 100)))
  }

  /** Runs `body` against a fresh managed catalog table `t` already holding `(1, 100)`. */
  protected def withInitialTable(body: String => Unit): Unit =
    withTable("t") {
      createSimpleTable("t")
      seedInitialRow("t")
      body("t")
    }

  /**
   * Runs `body` against a fresh external catalog table `t` already holding `(1, 100)`, passing the
   * table's storage path so the body can stage external commits there before REFRESH TABLE.
   */
  protected def withExternalTable(body: String => Unit): Unit = {
    val dir = Files.createTempDirectory("refresh-ext").toFile
    try {
      withTable("t") {
        spark.sql(
          s"CREATE TABLE t (id INT, salary INT) USING delta LOCATION '${dir.getAbsolutePath}'")
        seedInitialRow("t")
        body(dir.getAbsolutePath)
      }
    } finally {
      deleteRecursively(dir)
    }
  }

  // External-write simulation: write commit files directly into _delta_log, editing schemas as
  // StructType values recovered from the stored schemaString.

  private val IdRegex = """"id"\s*:\s*"([^"]+)"""".r
  private val SchemaStringRegex = """"schemaString"\s*:\s*"((?:[^"\\]|\\.)*)"""".r

  private def deltaLogDir(path: String): File = new File(path, "_delta_log")

  private def commitJsonFiles(path: String): Array[File] =
    deltaLogDir(path).listFiles().filter(_.getName.endsWith(".json"))

  private def currentVersion(path: String): Long =
    commitJsonFiles(path).map(_.getName.stripSuffix(".json").toLong).max

  private def latestMetaDataLine(path: String): String =
    commitJsonFiles(path).sortBy(_.getName).flatMap { f =>
      new String(Files.readAllBytes(f.toPath), UTF_8).split("\n")
    }.filter(_.contains("\"metaData\"")).lastOption.getOrElse(
      throw new RuntimeException("No metaData action found in commit files"))

  /** The table's current schema, recovered from the stored schemaString. */
  private def currentSchema(path: String): StructType = {
    val escaped = SchemaStringRegex.findFirstMatchIn(latestMetaDataLine(path)).map(_.group(1))
      .getOrElse(throw new RuntimeException("Could not extract schemaString from metadata"))
    DataType.fromJson(escaped.replace("\\\"", "\"")).asInstanceOf[StructType]
  }

  /** Returns the metaData line with its schemaString replaced (id and configuration preserved). */
  private def metaLineWithSchema(metaLine: String, schema: StructType): String = {
    val escaped = schema.json.replace("\"", "\\\"")
    SchemaStringRegex.replaceFirstIn(
      metaLine, java.util.regex.Matcher.quoteReplacement(s""""schemaString":"$escaped""""))
  }

  /** Returns the metaData line with a fresh table id (simulating a recreated table). */
  private def metaLineWithNewId(metaLine: String): String =
    IdRegex.replaceFirstIn(
      metaLine, java.util.regex.Matcher.quoteReplacement(s""""id":"${UUID.randomUUID()}""""))

  private def addFileJson(name: String, size: Long): String =
    s"""{"add":{"path":"$name","partitionValues":{},"size":$size,""" +
    s""""modificationTime":${System.currentTimeMillis()},"dataChange":true}}"""

  private def removeFileJson(name: String): String =
    s"""{"remove":{"path":"$name","deletionTimestamp":${System.currentTimeMillis()},""" +
    s""""dataChange":true}}"""

  /** RemoveFile actions for every data file still active in the table. */
  private def removeActiveFiles(path: String): Seq[String] = {
    val addRegex = """"add":\{[^}]*"path"\s*:\s*"([^"]+)"""".r
    val removeRegex = """"remove":\{[^}]*"path"\s*:\s*"([^"]+)"""".r
    val added = scala.collection.mutable.Set[String]()
    val removed = scala.collection.mutable.Set[String]()
    commitJsonFiles(path).sortBy(_.getName).foreach { f =>
      val content = new String(Files.readAllBytes(f.toPath), UTF_8)
      addRegex.findAllMatchIn(content).foreach(m => added += m.group(1))
      removeRegex.findAllMatchIn(content).foreach(m => removed += m.group(1))
    }
    (added -- removed).toSeq.map(removeFileJson)
  }

  /** Writes a parquet file from a DataFrame into the table dir; returns its AddFile action. */
  private def writeParquetAndGetAddFileAction(path: String, df: DataFrame): String = {
    val tempDir = Files.createTempDirectory("ext-write").toFile
    try {
      df.coalesce(1).write.parquet(s"${tempDir.getAbsolutePath}/out")
      val parquetFile = new File(tempDir, "out").listFiles()
        .filter(_.getName.endsWith(".parquet")).head
      val targetName = s"ext-commit-v${currentVersion(path) + 1}.snappy.parquet"
      Files.copy(parquetFile.toPath, Paths.get(path).resolve(targetName))
      addFileJson(targetName, parquetFile.length())
    } finally {
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(file: File): Unit = {
    Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    file.delete()
  }

  /** Writes a new commit (version + 1) containing the given actions. */
  private def writeCommit(path: String, actions: Seq[String]): Unit =
    Files.write(
      new File(deltaLogDir(path), f"${currentVersion(path) + 1}%020d.json").toPath,
      actions.mkString("\n").getBytes(UTF_8))

  private def idSalaryRowsToDf(rows: Seq[(Int, Int)]): DataFrame = {
    val session = spark
    import session.implicits._
    rows.toDF("id", "salary")
  }

  private def idSalaryNewColumnRowsToDf(rows: Seq[(Int, Int, Int)]): DataFrame = {
    val session = spark
    import session.implicits._
    rows.toDF("id", "salary", "new_column")
  }

  protected def externalDataWrite(path: String, rows: Seq[(Int, Int)]): Unit =
    writeCommit(path, Seq(writeParquetAndGetAddFileAction(path, idSalaryRowsToDf(rows))))

  protected def externalAddColumnAndWrite(path: String, rows: Seq[(Int, Int, Int)]): Unit = {
    val newSchema = currentSchema(path).add("new_column", IntegerType, nullable = true)
    writeCommit(path, Seq(
      metaLineWithSchema(latestMetaDataLine(path), newSchema),
      writeParquetAndGetAddFileAction(path, idSalaryNewColumnRowsToDf(rows))))
  }

  protected def externalDropAndRecreate(path: String): Unit = {
    val recreatedMeta = metaLineWithNewId(latestMetaDataLine(path))
    writeCommit(path, removeActiveFiles(path) :+ recreatedMeta)
  }
}
