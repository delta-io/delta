/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.tables

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.util.UUID

import io.delta.tables.shared.DeltaTableRefreshSharedBase

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.test.DeltaQueryTest
import org.apache.spark.sql.types.{DataType, IntegerType, StructType}

/**
 * Spark Connect variant of the repeated table access refresh tests.
 *
 * Key behavioral differences from classic (local) mode:
 *   - In Connect, Dataset is re-analyzed on each execution, so repeated reads always see the
 *     latest data and schema.
 *
 * These tests document the "OSS Delta (connect)" column from the
 * "Refreshing and pinning tables in Spark" design doc.
 */
trait DeltaTableRefreshConnectTestBase extends DeltaTableRefreshSharedBase {
  self: DeltaQueryTest with RemoteSparkSession =>

  override def isConnect: Boolean = true

  /** Asserts that a SparkThrowable has the expected error condition. */
  protected def checkError(
      exception: SparkThrowable,
      condition: String): Unit = {
    // In Connect, some errors arrive wrapped as INTERNAL_ERROR. Fall back to checking
    // getMessage if getCondition doesn't match.
    val cond = exception.getCondition
    if (cond != condition) {
      assert(exception.asInstanceOf[Exception].getMessage.contains(condition),
        s"Expected error condition '$condition' but got '$cond' " +
        s"and message does not contain it either")
    }
  }

  /**
   * Override in subclasses to use spark.newSession() for writes. In Connect, newSession()
   * creates a new client session that connects to the same server. The server-side DeltaLog
   * is shared (singleton cache per JVM), so writes from either session update the same
   * DeltaLog.currentSnapshot. This means newSession() is NOT a true external writer.
   * We parameterize with it to verify behavior is identical, documenting that the refresh
   * mechanism is driven by the shared DeltaLog, not by session-level state.
   */
  protected def useExternalSession: Boolean = false

  /** Returns a session for performing writes. */
  protected def writerSession: org.apache.spark.sql.SparkSession = {
    if (useExternalSession) spark.newSession() else spark
  }

  /** Execute SQL using the writer session. */
  override protected def writerSql(sqlText: String): Unit = {
    writerSession.sql(sqlText)
  }

  override protected def createSimpleTable(tableName: String): Unit = {
    spark.sql(s"CREATE TABLE $tableName (id INT, salary INT) USING delta")
  }

  override protected def insertInitialData(tableName: String): Unit = {
    spark.sql(s"INSERT INTO $tableName VALUES (1, 100)")
  }

  // ---------------------------------------------------------------------------
  // External write simulation. The Connect client shares the local filesystem with the
  // server, so we write commit files directly into _delta_log, bypassing the server's
  // DeltaLog (its currentSnapshot is NOT updated). This is the Connect equivalent of the
  // classic suite's LogStore based helpers. Schemas are edited as real StructType values
  // (recovered from the stored schemaString) rather than by regex.
  // ---------------------------------------------------------------------------

  private val IdRegex = """"id"\s*:\s*"([^"]+)"""".r
  private val SchemaStringRegex = """"schemaString"\s*:\s*"((?:[^"\\]|\\.)*)"""".r

  /** Extracts the filesystem path from a delta path table reference (delta.`<path>`). */
  private def pathOf(tableRef: String): String =
    tableRef.stripPrefix("delta.`").stripSuffix("`")

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

  /** Writes a parquet file with the given DataFrame into the table dir; returns its AddFile action. */
  private def writeParquet(path: String, df: DataFrame): String = {
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

  private def rows2Df(rows: Seq[(Int, Int)]): DataFrame = {
    val session = spark
    import session.implicits._
    rows.toDF("id", "salary")
  }

  private def rows3Df(rows: Seq[(Int, Int, Int)]): DataFrame = {
    val session = spark
    import session.implicits._
    rows.toDF("id", "salary", "new_column")
  }

  // ---------------------------------------------------------------------------
  // Shared base hook implementations (connect). Error assertions delegate to the
  // substring tolerant checkError above; external writes use the helpers above.
  // ---------------------------------------------------------------------------

  override protected def assertArityMismatchError(f: => Unit): Unit = {
    checkError(
      exception = intercept[AnalysisException] { f },
      condition = "INSERT_COLUMN_ARITY_MISMATCH")
  }

  override protected def assertExternalStrictConflict(f: => Unit): Unit = {
    // Only invoked from classic STRICT branches; Connect never reaches this path because the
    // shared traits gate the call behind !isConnect. Fail loudly if the wiring ever changes
    // rather than silently swallowing the outcome.
    fail("assertExternalStrictConflict should never be invoked on Connect")
  }

  override protected def withRefreshTable(body: String => Unit): Unit = {
    withTempPath { dir => body(s"delta.`${dir.getAbsolutePath}`") }
  }

  override protected def externalDataWrite(tableRef: String, rows: Seq[(Int, Int)]): Unit = {
    val path = pathOf(tableRef)
    writeCommit(path, Seq(writeParquet(path, rows2Df(rows))))
  }

  override protected def externalAddColumnAndWrite(
      tableRef: String, rows: Seq[(Int, Int, Int)]): Unit = {
    val path = pathOf(tableRef)
    val newSchema = currentSchema(path).add("new_column", IntegerType, nullable = true)
    writeCommit(path, Seq(
      metaLineWithSchema(latestMetaDataLine(path), newSchema),
      writeParquet(path, rows3Df(rows))))
  }

  override protected def externalDropAndRecreate(tableRef: String): Unit = {
    val path = pathOf(tableRef)
    val recreatedMeta = metaLineWithNewId(latestMetaDataLine(path))
    writeCommit(path, removeActiveFiles(path) :+ recreatedMeta)
  }
}
