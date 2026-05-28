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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.DeltaQueryTest

/**
 * Spark Connect variant of the table refresh and version pinning tests.
 *
 * Key behavioral differences from classic (local) mode:
 *   - In Connect, Dataset is re-analyzed on each execution, so collect() and show() behave
 *     the same: both always see the latest data and schema.
 *   - Temp views created from Dataset capture the plan, and in Connect temp views with stored
 *     plans behave the same as classic for column-mapping schema changes (they throw
 *     DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS).
 *
 * These tests document the "OSS Delta (connect)" column from the
 * "Refreshing and pinning tables in Spark" design doc.
 */
trait DeltaTableRefreshConnectTestBase { self: DeltaQueryTest with RemoteSparkSession =>

  /**
   * Override in subclasses to use spark.newSession() for writes. In Connect, newSession()
   * creates a new client session that connects to the same server. The server-side DeltaLog
   * is shared (singleton cache per JVM), so writes from either session update the same
   * DeltaLog.currentSnapshot. This means newSession() is NOT a true external writer.
   * We parameterize with it to verify behavior is identical, documenting that the refresh
   * mechanism is driven by the shared DeltaLog, not by session-level state.
   */
  /** Asserts that a SparkThrowable has the expected error condition. */
  protected def checkError(
      exception: SparkThrowable,
      condition: String): Unit = {
    // In Connect, some errors (e.g. Delta's DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS) arrive
    // wrapped as INTERNAL_ERROR. Fall back to checking getMessage if getCondition doesn't match.
    val cond = exception.getCondition
    if (cond != condition) {
      assert(exception.asInstanceOf[Exception].getMessage.contains(condition),
        s"Expected error condition '$condition' but got '$cond' " +
        s"and message does not contain it either")
    }
  }

  /** Spark 4.2+ fixes AMBIGUOUS_COLUMN_OR_FIELD for self-joins without aliases. */
  protected lazy val ambiguousColumnFixed: Boolean = spark.version >= "4.2"

  protected def useExternalSession: Boolean = false

  /** Returns a session for performing writes. */
  protected def writerSession: org.apache.spark.sql.SparkSession = {
    if (useExternalSession) spark.newSession() else spark
  }

  /** Execute SQL using the writer session. */
  protected def writerSql(sqlText: String): Unit = {
    writerSession.sql(sqlText)
  }

  protected def createSimpleTable(tableName: String): Unit = {
    spark.sql(s"CREATE TABLE $tableName (id INT, salary INT) USING delta")
  }

  protected def createColumnMappingTable(tableName: String): Unit = {
    spark.sql(
      s"""CREATE TABLE $tableName (id INT, salary INT) USING delta
         |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
  }

  protected def insertInitialData(tableName: String): Unit = {
    spark.sql(s"INSERT INTO $tableName VALUES (1, 100)")
  }

  /**
   * Simulates a true external write by writing commit files directly to the
   * filesystem using Java NIO, bypassing the server's DeltaLog entirely.
   *
   * This is the Connect equivalent of the classic suite's writeExternalCommit.
   * Since the Connect client shares the same local filesystem as the server,
   * we can write parquet data files and Delta commit JSON files directly.
   * The server's DeltaLog.currentSnapshot is NOT updated.
   *
   * @param tablePath The filesystem path to the Delta table
   * @param data Tuples of (id, salary) to write
   */
  protected def writeExternalCommitViaFilesystem(
      tablePath: String,
      data: Seq[(Int, Int)]): Unit = {
    val deltaLogDir = new File(tablePath, "_delta_log")
    // Find current version by listing commit files
    val commitFiles = deltaLogDir.listFiles().filter(_.getName.endsWith(".json"))
    val currentVersion = commitFiles.map(f =>
      f.getName.stripSuffix(".json").toLong).max

    // Write a parquet file with the data
    val tempDir = Files.createTempDirectory("ext-write").toFile
    try {
      val session = spark
      import session.implicits._
      data.toDF("id", "salary").coalesce(1)
        .write.parquet(s"${tempDir.getAbsolutePath}/out")
      val parquetFile = new File(tempDir, "out").listFiles()
        .filter(_.getName.endsWith(".parquet")).head
      val targetName = s"ext-commit-v${currentVersion + 1}.snappy.parquet"
      Files.copy(
        parquetFile.toPath,
        Paths.get(tablePath).resolve(targetName))

      // Write commit JSON directly to _delta_log
      val addFileJson =
        s"""{"add":{"path":"$targetName","partitionValues":{},"size":${parquetFile.length()},""" +
        s""""modificationTime":${System.currentTimeMillis()},"dataChange":true}}"""
      val commitFile = new File(deltaLogDir,
        f"${currentVersion + 1}%020d.json")
      Files.write(commitFile.toPath, addFileJson.getBytes(StandardCharsets.UTF_8))
    } finally {
      // Clean up temp dir
      tempDir.listFiles().foreach(d =>
        if (d.isDirectory) d.listFiles().foreach(_.delete()))
      tempDir.listFiles().foreach(_.delete())
      tempDir.delete()
    }
  }

  /**
   * Simulates a true external schema change + data write by writing both a
   * Metadata action (with an added column) and an AddFile action directly to
   * the filesystem, bypassing the server's DeltaLog entirely.
   *
   * This is the Connect equivalent of the classic suite's
   * writeExternalCommit(..., newMetadata = Some(...)) for scenario 6d.
   */
  protected def writeExternalSchemaChangeCommitViaFilesystem(
      tablePath: String,
      data: Seq[(Int, Int, Int)]): Unit = {
    val deltaLogDir = new File(tablePath, "_delta_log")
    val commitFiles = deltaLogDir.listFiles().filter(_.getName.endsWith(".json"))
    val currentVersion = commitFiles.map(f =>
      f.getName.stripSuffix(".json").toLong).max

    // Read existing metadata to extract the table id
    val metadataLine = commitFiles.sortBy(_.getName).flatMap { f =>
      new String(Files.readAllBytes(f.toPath), StandardCharsets.UTF_8)
        .split("\n").filter(_.contains("\"metaData\""))
    }.lastOption.getOrElse(
      throw new RuntimeException("No metaData action found in commit files"))
    val idRegex = """"id"\s*:\s*"([^"]+)"""".r
    val tableId = idRegex.findFirstMatchIn(metadataLine).map(_.group(1)).getOrElse(
      throw new RuntimeException("Could not extract table id from metadata"))

    val tempDir = Files.createTempDirectory("ext-schema-write").toFile
    try {
      val session = spark
      import session.implicits._
      data.toDF("id", "salary", "new_column").coalesce(1)
        .write.parquet(s"${tempDir.getAbsolutePath}/out")
      val parquetFile = new File(tempDir, "out").listFiles()
        .filter(_.getName.endsWith(".parquet")).head
      val targetName = s"ext-schema-v${currentVersion + 1}.snappy.parquet"
      Files.copy(
        parquetFile.toPath,
        Paths.get(tablePath).resolve(targetName))

      // New schema with added new_column (escaped for embedding in JSON string)
      val schemaJson =
        """{"type":"struct","fields":[""" +
        """{"name":"id","type":"integer","nullable":true,"metadata":{}},""" +
        """{"name":"salary","type":"integer","nullable":true,"metadata":{}},""" +
        """{"name":"new_column","type":"integer","nullable":true,"metadata":{}}]}"""
      val escapedSchema = schemaJson.replace("\"", "\\\"")

      val metadataActionJson =
        s"""{"metaData":{"id":"$tableId","name":null,"description":null,""" +
        s""""format":{"provider":"parquet","options":{}},""" +
        s""""schemaString":"$escapedSchema",""" +
        s""""partitionColumns":[],"configuration":{},"createdTime":${System.currentTimeMillis()}}}"""

      val addFileJson =
        s"""{"add":{"path":"$targetName","partitionValues":{},"size":${parquetFile.length()},""" +
        s""""modificationTime":${System.currentTimeMillis()},"dataChange":true}}"""

      val commitFile = new File(deltaLogDir,
        f"${currentVersion + 1}%020d.json")
      Files.write(
        commitFile.toPath,
        s"$metadataActionJson\n$addFileJson".getBytes(StandardCharsets.UTF_8))
    } finally {
      tempDir.listFiles().foreach(d =>
        if (d.isDirectory) d.listFiles().foreach(_.delete()))
      tempDir.listFiles().foreach(_.delete())
      tempDir.delete()
    }
  }

  /**
   * Simulates an external DROP and recreate by writing RemoveFile actions for
   * all existing data files and new Metadata with a fresh UUID, bypassing the
   * server's DeltaLog entirely.
   */
  protected def writeExternalDropAndRecreateViaFilesystem(tablePath: String): Unit = {
    val deltaLogDir = new File(tablePath, "_delta_log")
    val commitFiles = deltaLogDir.listFiles().filter(_.getName.endsWith(".json"))
    val currentVersion = commitFiles.map(f =>
      f.getName.stripSuffix(".json").toLong).max

    // Collect all AddFile paths from existing commits
    val addPathRegex = """"add":\{[^}]*"path"\s*:\s*"([^"]+)"""".r
    val removePathRegex = """"remove":\{[^}]*"path"\s*:\s*"([^"]+)"""".r
    val addedPaths = scala.collection.mutable.Set[String]()
    val removedPaths = scala.collection.mutable.Set[String]()
    commitFiles.sortBy(_.getName).foreach { f =>
      val content = new String(Files.readAllBytes(f.toPath), StandardCharsets.UTF_8)
      addPathRegex.findAllMatchIn(content).foreach(m => addedPaths += m.group(1))
      removePathRegex.findAllMatchIn(content).foreach(m => removedPaths += m.group(1))
    }
    val activeFiles = addedPaths -- removedPaths

    // Read existing metadata to get the table structure
    val metadataLine = commitFiles.sortBy(_.getName).flatMap { f =>
      new String(Files.readAllBytes(f.toPath), StandardCharsets.UTF_8)
        .split("\n").filter(_.contains("\"metaData\""))
    }.lastOption.getOrElse(
      throw new RuntimeException("No metaData action found in commit files"))

    // Replace the table id with a fresh UUID to simulate a recreated table
    val newId = java.util.UUID.randomUUID().toString
    val newMetadataLine = metadataLine.replaceFirst(""""id"\s*:\s*"[^"]+"""", s""""id":"$newId"""")

    // Build commit: RemoveFile for each active file + new Metadata
    val removeActions = activeFiles.map { path =>
      s"""{"remove":{"path":"$path","deletionTimestamp":${System.currentTimeMillis()},"dataChange":true}}"""
    }
    val commitContent = (removeActions.toSeq :+ newMetadataLine).mkString("\n")

    val commitFile = new File(deltaLogDir, f"${currentVersion + 1}%020d.json")
    Files.write(commitFile.toPath, commitContent.getBytes(StandardCharsets.UTF_8))
  }

  /**
   * Simulates an external DROP COLUMN by writing a metadata-only commit that
   * removes a column from the schema, bypassing the server's DeltaLog entirely.
   * Works with column mapping tables where field metadata contains
   * delta.columnMapping.id and delta.columnMapping.physicalName.
   */
  protected def writeExternalDropColumnViaFilesystem(
      tablePath: String,
      columnToDrop: String): Unit = {
    val deltaLogDir = new File(tablePath, "_delta_log")
    val commitFiles = deltaLogDir.listFiles().filter(_.getName.endsWith(".json"))
    val currentVersion = commitFiles.map(f =>
      f.getName.stripSuffix(".json").toLong).max

    // Read existing metadata
    val metadataLine = commitFiles.sortBy(_.getName).flatMap { f =>
      new String(Files.readAllBytes(f.toPath), StandardCharsets.UTF_8)
        .split("\n").filter(_.contains("\"metaData\""))
    }.lastOption.getOrElse(
      throw new RuntimeException("No metaData action found in commit files"))

    // Extract and modify the schemaString to remove the column.
    // The schemaString is JSON-escaped inside the metaData JSON.
    // Parse the escaped schema, modify it, and re-escape.
    val schemaStringRegex = """"schemaString"\s*:\s*"((?:[^"\\]|\\.)*)"""".r
    val escapedSchema = schemaStringRegex.findFirstMatchIn(metadataLine).map(_.group(1)).getOrElse(
      throw new RuntimeException("Could not extract schemaString from metadata"))
    val schemaJson = escapedSchema.replace("\\\"", "\"")

    // Remove the field from the schema JSON. Fields with column mapping have nested
    // braces (metadata object inside the field object), so we match two levels of braces.
    val fieldPattern = ("""\{"name":"""" + columnToDrop + """"[^}]*\{[^}]*\}\}""").r
    val newSchemaJson = fieldPattern.replaceFirstIn(schemaJson, "").replace(",,", ",")
      .replace("[,", "[").replace(",]", "]")
    val newEscapedSchema = newSchemaJson.replace("\"", "\\\"")
    val replacement = java.util.regex.Matcher.quoteReplacement(
      s""""schemaString":"$newEscapedSchema"""")
    val newMetadataLine = schemaStringRegex.replaceFirstIn(metadataLine, replacement)

    val commitFile = new File(deltaLogDir, f"${currentVersion + 1}%020d.json")
    Files.write(commitFile.toPath, newMetadataLine.getBytes(StandardCharsets.UTF_8))
  }

  /**
   * Simulates an external DROP and recreate of a column mapping table by writing
   * RemoveFile actions for all data + new Metadata with fresh column mapping IDs,
   * bypassing the server's DeltaLog entirely.
   */
  protected def writeExternalDropAndRecreateColumnMappingViaFilesystem(
      tablePath: String): Unit = {
    val deltaLogDir = new File(tablePath, "_delta_log")
    val commitFiles = deltaLogDir.listFiles().filter(_.getName.endsWith(".json"))
    val currentVersion = commitFiles.map(f =>
      f.getName.stripSuffix(".json").toLong).max

    // Collect active AddFile paths
    val addPathRegex = """"add":\{[^}]*"path"\s*:\s*"([^"]+)"""".r
    val removePathRegex = """"remove":\{[^}]*"path"\s*:\s*"([^"]+)"""".r
    val addedPaths = scala.collection.mutable.Set[String]()
    val removedPaths = scala.collection.mutable.Set[String]()
    commitFiles.sortBy(_.getName).foreach { f =>
      val content = new String(Files.readAllBytes(f.toPath), StandardCharsets.UTF_8)
      addPathRegex.findAllMatchIn(content).foreach(m => addedPaths += m.group(1))
      removePathRegex.findAllMatchIn(content).foreach(m => removedPaths += m.group(1))
    }
    val activeFiles = addedPaths -- removedPaths

    // Read existing metadata
    val metadataLine = commitFiles.sortBy(_.getName).flatMap { f =>
      new String(Files.readAllBytes(f.toPath), StandardCharsets.UTF_8)
        .split("\n").filter(_.contains("\"metaData\""))
    }.lastOption.getOrElse(
      throw new RuntimeException("No metaData action found in commit files"))

    // Replace table id and all column mapping IDs with new values
    val newId = java.util.UUID.randomUUID().toString
    var newMetadataLine = metadataLine.replaceFirst(
      """"id"\s*:\s*"[^"]+"""", s""""id":"$newId"""")
    // Replace all delta.columnMapping.id values with new IDs (100+)
    val colIdRegex = """delta\.columnMapping\.id\\?"?\s*:\s*(\d+)""".r
    var nextId = 100
    newMetadataLine = colIdRegex.replaceAllIn(newMetadataLine, _ => {
      val result = s"delta.columnMapping.id\\\\\":${nextId}"
      nextId += 1
      result
    })
    // Replace all delta.columnMapping.physicalName values
    val colNameRegex = """delta\.columnMapping\.physicalName\\?"?\s*:\s*\\?"?([^"\\,}]+)""".r
    newMetadataLine = colNameRegex.replaceAllIn(newMetadataLine, _ => {
      val uuid = java.util.UUID.randomUUID().toString.take(8)
      s"""delta.columnMapping.physicalName\\\\\":\\\\\"col-recreated-$uuid"""
    })

    val removeActions = activeFiles.map { path =>
      s"""{"remove":{"path":"$path","deletionTimestamp":${System.currentTimeMillis()},"dataChange":true}}"""
    }
    val commitContent = (removeActions.toSeq :+ newMetadataLine).mkString("\n")

    val commitFile = new File(deltaLogDir, f"${currentVersion + 1}%020d.json")
    Files.write(commitFile.toPath, commitContent.getBytes(StandardCharsets.UTF_8))
  }

  /**
   * Simulates an external DROP/ADD of a column by writing a metadata-only commit
   * that replaces the column's column mapping ID and physical name (and optionally
   * its type), bypassing the server's DeltaLog entirely. This triggers
   * DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS because the column mapping ID changed.
   */
  protected def writeExternalReplaceColumnViaFilesystem(
      tablePath: String,
      columnName: String,
      newType: Option[String] = None): Unit = {
    val deltaLogDir = new File(tablePath, "_delta_log")
    val commitFiles = deltaLogDir.listFiles().filter(_.getName.endsWith(".json"))
    val currentVersion = commitFiles.map(f =>
      f.getName.stripSuffix(".json").toLong).max

    val metadataLine = commitFiles.sortBy(_.getName).flatMap { f =>
      new String(Files.readAllBytes(f.toPath), StandardCharsets.UTF_8)
        .split("\n").filter(_.contains("\"metaData\""))
    }.lastOption.getOrElse(
      throw new RuntimeException("No metaData action found in commit files"))

    val schemaStringRegex = """"schemaString"\s*:\s*"((?:[^"\\]|\\.)*)"""".r
    val escapedSchema = schemaStringRegex.findFirstMatchIn(metadataLine).map(_.group(1)).getOrElse(
      throw new RuntimeException("Could not extract schemaString from metadata"))
    val schemaJson = escapedSchema.replace("\\\"", "\"")

    // Find the target field and replace its column mapping ID and physical name
    val uuid = java.util.UUID.randomUUID().toString.take(8)
    // Fields with column mapping have nested braces (metadata object), match two levels.
    val fieldRegex = ("""\{"name":"""" + columnName + """"[^}]*\{[^}]*\}\}""").r
    val fieldMatch = fieldRegex.findFirstIn(schemaJson).getOrElse(
      throw new RuntimeException(s"Could not find field '$columnName' in schema"))
    val typeStr = newType.getOrElse("integer")
    val modifiedField = fieldMatch
      .replaceFirst(""""delta\.columnMapping\.id"\s*:\s*\d+""",
        """"delta.columnMapping.id":999""")
      .replaceFirst(""""delta\.columnMapping\.physicalName"\s*:\s*"[^"]+"""",
        s""""delta.columnMapping.physicalName":"col-replaced-$uuid"""")
    val modifiedFieldWithType = if (newType.isDefined) {
      modifiedField.replaceFirst(""""type"\s*:\s*"[^"]+"""", s""""type":"$typeStr"""")
    } else {
      modifiedField
    }
    val newSchemaJson = schemaJson.replace(fieldMatch, modifiedFieldWithType)

    val newEscapedSchema = newSchemaJson.replace("\"", "\\\"")
    val replacement = java.util.regex.Matcher.quoteReplacement(
      s""""schemaString":"$newEscapedSchema"""")
    val newMetadataLine = schemaStringRegex.replaceFirstIn(metadataLine, replacement)

    val commitFile = new File(deltaLogDir, f"${currentVersion + 1}%020d.json")
    Files.write(commitFile.toPath, newMetadataLine.getBytes(StandardCharsets.UTF_8))
  }
}
