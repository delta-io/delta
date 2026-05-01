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
trait DeltaTableRefreshAndPinningConnectSuiteBase
  extends DeltaQueryTest with RemoteSparkSession {

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

  protected def useExternalSession: Boolean = false

  /**
   * Override in subclasses to set a non-zero staleness limit. The config is set on
   * the server via spark.conf.set at runtime. See the classic suite's class-level
   * scaladoc for why staleness has no observable effect for in-JVM writes.
   */
  protected def stalenessLimitMs: Long = 0L

  private val stalenessConfigKey = "spark.databricks.delta.stalenessLimit"

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (stalenessLimitMs > 0L) {
      spark.conf.set(stalenessConfigKey, s"${stalenessLimitMs}ms")
    }
  }

  override def afterAll(): Unit = {
    try {
      if (stalenessLimitMs > 0L) {
        spark.conf.set(stalenessConfigKey, "0ms")
      }
    } finally {
      super.afterAll()
    }
  }

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

  // ---------------------------------------------------------------------------
  // Section [1]: Temp views with stored plans (Connect behavior)
  // Temp views created from Dataset capture the plan. In Connect, they behave
  // the same as classic for the scenarios tested here.
  // ---------------------------------------------------------------------------

  test("[1.1] connect: temp view picks up session writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        spark.sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1] connect scenario 2: temp view with ADD COLUMN preserves original schema") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // Same as classic: temp view preserves original schema (id, salary) but picks up new data
      checkAnswer(
        spark.sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1] connect scenario 3: temp view after DROP COLUMN throws " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")

      // Same as classic: column mapping schema change throws
      checkError(
        exception = intercept[SparkException] {
          spark.sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"
      )
    }
  }

  test("[1] connect scenario 7: temp view after ALTER COLUMN TYPE INT to BIGINT") {
    withTable("t") {
      spark.sql(
        """CREATE TABLE t (id INT, salary INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.enableTypeWidening' = 'true'
          |)""".stripMargin)
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")

      // Same as classic: type change is detected as incompatible schema change.
      checkError(
        exception = intercept[SparkException] {
          spark.sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"
      )
    }
  }

  test("[1] connect scenario 4: temp view after DROP and recreate table") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      // Same as classic without column mapping: resolves to new empty table
      checkAnswer(spark.sql("SELECT * FROM v"), Seq.empty)
    }
  }

  test("[1] connect scenario 4: temp view after DROP and recreate table " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      // Column IDs changed, so reading the view should fail.
      checkError(
        exception = intercept[SparkException] {
          spark.sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"
      )
    }
  }

  test("[1] connect scenario 5: temp view after DROP/ADD column same name same type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      // Same as classic: column mapping schema change (column IDs changed) throws
      checkError(
        exception = intercept[SparkException] {
          spark.sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"
      )
    }
  }

  test("[1] connect scenario 6: temp view after DROP/ADD column same name different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("id < 999").createOrReplaceTempView("v")
      checkAnswer(spark.sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      // Same as classic: column mapping schema change throws
      checkError(
        exception = intercept[SparkException] {
          spark.sql("SELECT * FROM v").collect()
        },
        condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"
      )
    }
  }

  // ---------------------------------------------------------------------------
  // Section [1] external: Temp views with external modifications (Connect)
  // These test the "Connector w/ cache" behavior from the design doc.
  // With high stalenessLimit, external changes are invisible.
  // ---------------------------------------------------------------------------

  test("[1] connect scenario 1 external: temp view with external data write") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      spark.table(s"delta.`$path`").filter("salary < 999").createOrReplaceTempView("v_1ext")
      checkAnswer(spark.sql("SELECT * FROM v_1ext"), Row(1, 100))

      writeExternalCommitViaFilesystem(path, Seq((2, 200)))

      if (stalenessLimitMs == 0L) {
        checkAnswer(
          spark.sql("SELECT * FROM v_1ext ORDER BY id"),
          Seq(Row(1, 100), Row(2, 200)))
      } else {
        checkAnswer(spark.sql("SELECT * FROM v_1ext"), Row(1, 100))
      }
    }
  }

  test("[1] connect scenario 2 external: temp view with external ADD COLUMN") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      spark.table(s"delta.`$path`").filter("salary < 999").createOrReplaceTempView("v_2ext")
      checkAnswer(spark.sql("SELECT * FROM v_2ext"), Row(1, 100))

      writeExternalSchemaChangeCommitViaFilesystem(path, Seq((2, 200, -1)))

      if (stalenessLimitMs == 0L) {
        // View preserves original schema (id, salary) but picks up new data
        checkAnswer(
          spark.sql("SELECT * FROM v_2ext ORDER BY id"),
          Seq(Row(1, 100), Row(2, 200)))
      } else {
        checkAnswer(spark.sql("SELECT * FROM v_2ext"), Row(1, 100))
      }
    }
  }

  test("[1] connect scenario 3 external: temp view with external DROP COLUMN " +
      "(column mapping)") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(
        s"""CREATE TABLE delta.`$path` (id INT, salary INT) USING delta
           |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      spark.table(s"delta.`$path`").filter("id < 999").createOrReplaceTempView("v_3ext")
      checkAnswer(spark.sql("SELECT * FROM v_3ext"), Row(1, 100))

      writeExternalDropColumnViaFilesystem(path, "salary")

      if (stalenessLimitMs == 0L) {
        checkError(
          exception = intercept[SparkException] {
            spark.sql("SELECT * FROM v_3ext").collect()
          },
          condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
      } else {
        checkAnswer(spark.sql("SELECT * FROM v_3ext"), Row(1, 100))
      }
    }
  }

  test("[1] connect scenario 4 external: temp view after external DROP and recreate " +
      "(column mapping)") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(
        s"""CREATE TABLE delta.`$path` (id INT, salary INT) USING delta
           |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      spark.table(s"delta.`$path`").filter("id < 999").createOrReplaceTempView("v_4ext")
      checkAnswer(spark.sql("SELECT * FROM v_4ext"), Row(1, 100))

      writeExternalDropAndRecreateColumnMappingViaFilesystem(path)

      if (stalenessLimitMs == 0L) {
        checkError(
          exception = intercept[SparkException] {
            spark.sql("SELECT * FROM v_4ext").collect()
          },
          condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
      } else {
        checkAnswer(spark.sql("SELECT * FROM v_4ext"), Row(1, 100))
      }
    }
  }

  test("[1] connect scenario 4 external: temp view after external DROP and recreate " +
      "(no column mapping)") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      spark.table(s"delta.`$path`").filter("salary < 999").createOrReplaceTempView("v_4ext_nc")
      checkAnswer(spark.sql("SELECT * FROM v_4ext_nc"), Row(1, 100))

      writeExternalDropAndRecreateViaFilesystem(path)

      if (stalenessLimitMs == 0L) {
        checkAnswer(spark.sql("SELECT * FROM v_4ext_nc"), Seq.empty)
      } else {
        checkAnswer(spark.sql("SELECT * FROM v_4ext_nc"), Row(1, 100))
      }
    }
  }

  test("[1] connect scenario 5 external: temp view after external DROP/ADD column " +
      "same name same type (column mapping)") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(
        s"""CREATE TABLE delta.`$path` (id INT, salary INT) USING delta
           |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      spark.table(s"delta.`$path`").filter("id < 999").createOrReplaceTempView("v_5ext")
      checkAnswer(spark.sql("SELECT * FROM v_5ext"), Row(1, 100))

      writeExternalReplaceColumnViaFilesystem(path, "salary")

      if (stalenessLimitMs == 0L) {
        checkError(
          exception = intercept[SparkException] {
            spark.sql("SELECT * FROM v_5ext").collect()
          },
          condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
      } else {
        checkAnswer(spark.sql("SELECT * FROM v_5ext"), Row(1, 100))
      }
    }
  }

  test("[1] connect scenario 6 external: temp view after external DROP/ADD column " +
      "same name different type (column mapping)") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(
        s"""CREATE TABLE delta.`$path` (id INT, salary INT) USING delta
           |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      spark.table(s"delta.`$path`").filter("id < 999").createOrReplaceTempView("v_6ext")
      checkAnswer(spark.sql("SELECT * FROM v_6ext"), Row(1, 100))

      writeExternalReplaceColumnViaFilesystem(path, "salary", newType = Some("string"))

      if (stalenessLimitMs == 0L) {
        checkError(
          exception = intercept[SparkException] {
            spark.sql("SELECT * FROM v_6ext").collect()
          },
          condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
      } else {
        checkAnswer(spark.sql("SELECT * FROM v_6ext"), Row(1, 100))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Section [2]: Repeated table access with external changes (Connect)
  // ---------------------------------------------------------------------------

  test("[2] connect scenario 1: repeated access picks up new data") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[2] connect scenario 2: repeated access reflects schema changes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test("[2] connect scenario 3: repeated access after drop and recreate") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      checkAnswer(spark.sql("SELECT * FROM t"), Seq.empty)
    }
  }

  // ---------------------------------------------------------------------------
  // Section [2] external: Repeated table access with external modifications
  // ---------------------------------------------------------------------------

  test("[2] connect scenario 1 external: repeated access picks up external data") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      checkAnswer(spark.sql(s"SELECT * FROM delta.`$path`"), Row(1, 100))

      writeExternalCommitViaFilesystem(path, Seq((2, 200)))

      if (stalenessLimitMs == 0L) {
        checkAnswer(
          spark.sql(s"SELECT * FROM delta.`$path` ORDER BY id"),
          Seq(Row(1, 100), Row(2, 200)))
      } else {
        checkAnswer(spark.sql(s"SELECT * FROM delta.`$path`"), Row(1, 100))
      }
    }
  }

  test("[2] connect scenario 2 external: repeated access reflects external schema change") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      checkAnswer(spark.sql(s"SELECT * FROM delta.`$path`"), Row(1, 100))

      writeExternalSchemaChangeCommitViaFilesystem(path, Seq((2, 200, -1)))

      if (stalenessLimitMs == 0L) {
        checkAnswer(
          spark.sql(s"SELECT * FROM delta.`$path` ORDER BY id"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      } else {
        checkAnswer(spark.sql(s"SELECT * FROM delta.`$path`"), Row(1, 100))
      }
    }
  }

  test("[2] connect scenario 3 external: repeated access after external DROP and recreate") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")

      checkAnswer(spark.sql(s"SELECT * FROM delta.`$path`"), Row(1, 100))

      writeExternalDropAndRecreateViaFilesystem(path)

      if (stalenessLimitMs == 0L) {
        checkAnswer(spark.sql(s"SELECT * FROM delta.`$path`"), Seq.empty)
      } else {
        checkAnswer(spark.sql(s"SELECT * FROM delta.`$path`"), Row(1, 100))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Section [3]: Incrementally constructed queries (Connect)
  // In Connect, Dataset is re-analyzed on each execution, so both df1 and df2
  // resolve to the latest table state (effectively a self-join).
  //
  // Without aliases, the join throws AMBIGUOUS_COLUMN_OR_FIELD because Delta's
  // V1 fallback (FallbackToV1DeltaRelation) loses PLAN_ID_TAG, preventing
  // Connect's plan-based column disambiguation. The aliased duplicates below
  // verify the actual data.
  // ---------------------------------------------------------------------------

  test("[3] connect scenario 1 (no alias): self-join after write throws") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("INSERT INTO t VALUES (2, 200)")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      checkError(
        exception = intercept[AnalysisException] {
          joined.collect()
        },
        condition = "AMBIGUOUS_COLUMN_OR_FIELD"
      )
    }
  }

  test("[3] connect scenario 2 (no alias): self-join after ADD COLUMN throws") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      checkError(
        exception = intercept[AnalysisException] {
          joined.collect()
        },
        condition = "AMBIGUOUS_COLUMN_OR_FIELD"
      )
    }
  }

  test("[3] connect scenario 3 (no alias): self-join after DROP COLUMN throws " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      checkError(
        exception = intercept[AnalysisException] {
          joined.collect()
        },
        condition = "AMBIGUOUS_COLUMN_OR_FIELD"
      )
    }
  }

  test("[3] connect scenario 4 (no alias): self-join after DROP/recreate throws " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      checkError(
        exception = intercept[AnalysisException] {
          joined.collect()
        },
        condition = "AMBIGUOUS_COLUMN_OR_FIELD"
      )
    }
  }

  test("[3] connect scenario 4 (no alias): self-join after DROP/recreate throws " +
      "(no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      checkError(
        exception = intercept[AnalysisException] {
          joined.collect()
        },
        condition = "AMBIGUOUS_COLUMN_OR_FIELD"
      )
    }
  }

  test("[3] connect scenario 5 (no alias): self-join after DROP/ADD same type throws " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      checkError(
        exception = intercept[AnalysisException] {
          joined.collect()
        },
        condition = "AMBIGUOUS_COLUMN_OR_FIELD"
      )
    }
  }

  test("[3] connect scenario 6 (no alias): self-join after DROP/ADD different type throws " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      val df2 = spark.table("t")

      val joined = df1.join(df2, df1("id") === df2("id"))
      checkError(
        exception = intercept[AnalysisException] {
          joined.collect()
        },
        condition = "AMBIGUOUS_COLUMN_OR_FIELD"
      )
    }
  }

  // Section [3] continued: SQL JOIN without column aliases.
  // The SQL equivalent of df1.join(df2, df1("id") === df2("id")) also throws
  // AMBIGUOUS_COLUMN_OR_FIELD because the output has duplicate column names.

  test("[3] connect scenario 1 (SQL JOIN no alias): self-join after write throws") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      writerSql("INSERT INTO t VALUES (2, 200)")

      checkError(
        exception = intercept[AnalysisException] {
          spark.sql("SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id").collect()
        },
        condition = "AMBIGUOUS_COLUMN_OR_FIELD"
      )
    }
  }

  test("[3] connect scenario 2 (SQL JOIN no alias): self-join after ADD COLUMN throws") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      checkError(
        exception = intercept[AnalysisException] {
          spark.sql("SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id").collect()
        },
        condition = "AMBIGUOUS_COLUMN_OR_FIELD"
      )
    }
  }

  test("[3] connect scenario 3 (SQL JOIN no alias): self-join after DROP COLUMN throws " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")

      checkError(
        exception = intercept[AnalysisException] {
          spark.sql("SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id").collect()
        },
        condition = "AMBIGUOUS_COLUMN_OR_FIELD"
      )
    }
  }

  test("[3] connect scenario 4 (SQL JOIN no alias): self-join after DROP/recreate throws " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      checkError(
        exception = intercept[AnalysisException] {
          spark.sql("SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id").collect()
        },
        condition = "AMBIGUOUS_COLUMN_OR_FIELD"
      )
    }
  }

  test("[3] connect scenario 4 (SQL JOIN no alias): self-join after DROP/recreate throws " +
      "(no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      checkError(
        exception = intercept[AnalysisException] {
          spark.sql("SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id").collect()
        },
        condition = "AMBIGUOUS_COLUMN_OR_FIELD"
      )
    }
  }

  test("[3] connect scenario 5 (SQL JOIN no alias): self-join after DROP/ADD same type " +
      "throws (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      checkError(
        exception = intercept[AnalysisException] {
          spark.sql("SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id").collect()
        },
        condition = "AMBIGUOUS_COLUMN_OR_FIELD"
      )
    }
  }

  test("[3] connect scenario 6 (SQL JOIN no alias): self-join after DROP/ADD different " +
      "type throws (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      checkError(
        exception = intercept[AnalysisException] {
          spark.sql("SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id").collect()
        },
        condition = "AMBIGUOUS_COLUMN_OR_FIELD"
      )
    }
  }

  // Section [3] continued: column-renamed duplicates that verify actual data.
  // Delta's V1 fallback loses PLAN_ID_TAG, so we rename columns to disambiguate
  // instead of using DataFrame aliases (.as("t1")) which still fail.

  test("[3] connect scenario 1 (renamed cols): join after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      writerSql("INSERT INTO t VALUES (2, 200)")

      // Rename columns to avoid AMBIGUOUS_COLUMN_OR_FIELD
      val df1 = spark.table("t").toDF("id1", "salary1")
      val df2 = spark.table("t").toDF("id2", "salary2")

      // In Connect, both DataFrames re-analyze to the latest version.
      // Both scans see (1,100),(2,200). Self-join matches each row to itself.
      val joined = df1.join(df2, col("id1") === col("id2"))
      checkAnswer(
        joined.orderBy("id1"),
        Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
    }
  }

  test("[3] connect scenario 2 (renamed cols): join after ADD COLUMN") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // In Connect, both DataFrames re-analyze to the latest version and schema.
      // Both scans see the new schema (id, salary, new_column).
      val df1 = spark.table("t").toDF("id1", "salary1", "new_column1")
      val df2 = spark.table("t").toDF("id2", "salary2", "new_column2")

      val joined = df1.join(df2, col("id1") === col("id2"))
      checkAnswer(
        joined.orderBy("id1"),
        Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))
    }
  }

  test("[3] connect scenario 3 (renamed cols): join after DROP COLUMN (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")

      // In Connect, both DataFrames re-analyze to the latest version and schema.
      // Both scans see only (id) after the column drop.
      val df1 = spark.table("t").toDF("id1")
      val df2 = spark.table("t").toDF("id2")

      val joined = df1.join(df2, col("id1") === col("id2"))
      checkAnswer(
        joined.orderBy("id1"),
        Seq(Row(1, 1)))
    }
  }

  test("[3] connect scenario 4 (renamed cols): join after DROP/recreate (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      val df1 = spark.table("t").toDF("id1", "salary1")
      val df2 = spark.table("t").toDF("id2", "salary2")

      // In Connect, both DataFrames re-analyze to the new empty table.
      val joined = df1.join(df2, col("id1") === col("id2"))
      checkAnswer(joined, Seq.empty)
    }
  }

  test("[3] connect scenario 4 (renamed cols): join after DROP/recreate (no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      val df1 = spark.table("t").toDF("id1", "salary1")
      val df2 = spark.table("t").toDF("id2", "salary2")

      // In Connect, both DataFrames re-analyze to the new empty table.
      val joined = df1.join(df2, col("id1") === col("id2"))
      checkAnswer(joined, Seq.empty)
    }
  }

  test("[3] connect scenario 5 (renamed cols): join after DROP/ADD same type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      // In Connect, both DataFrames re-analyze. The new salary column has no
      // data for existing rows (old salary data is gone).
      val df1 = spark.table("t").toDF("id1", "salary1")
      val df2 = spark.table("t").toDF("id2", "salary2")

      val joined = df1.join(df2, col("id1") === col("id2"))
      checkAnswer(
        joined.orderBy("id1"),
        Seq(Row(1, null, 1, null)))
    }
  }

  test("[3] connect scenario 6 (renamed cols): join after DROP/ADD different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      // In Connect, both DataFrames re-analyze. Salary is now STRING type,
      // old salary data is gone.
      val df1 = spark.table("t").toDF("id1", "salary1")
      val df2 = spark.table("t").toDF("id2", "salary2")

      val joined = df1.join(df2, col("id1") === col("id2"))
      checkAnswer(
        joined.orderBy("id1"),
        Seq(Row(1, null, 1, null)))
    }
  }

  // Section [3] continued: SQL JOIN tests.
  // SQL queries are analyzed in one go on the server, so PLAN_ID_TAG loss
  // does not apply. These verify the design doc's expected data directly.

  test("[3] connect scenario 1 (SQL JOIN): join after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      writerSql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t1.salary AS salary1, " +
          "t2.id AS id2, t2.salary AS salary2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id ORDER BY id1"),
        Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
    }
  }

  test("[3] connect scenario 2 (SQL JOIN): join after ADD COLUMN") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t1.salary AS salary1, t1.new_column AS nc1, " +
          "t2.id AS id2, t2.salary AS salary2, t2.new_column AS nc2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id ORDER BY id1"),
        Seq(Row(1, 100, null, 1, 100, null), Row(2, 200, -1, 2, 200, -1)))
    }
  }

  test("[3] connect scenario 3 (SQL JOIN): join after DROP COLUMN (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")

      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t2.id AS id2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id ORDER BY id1"),
        Seq(Row(1, 1)))
    }
  }

  test("[3] connect scenario 4 (SQL JOIN): join after DROP/recreate (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t1.salary AS salary1, " +
          "t2.id AS id2, t2.salary AS salary2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id"),
        Seq.empty)
    }
  }

  test("[3] connect scenario 4 (SQL JOIN): join after DROP/recreate (no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t1.salary AS salary1, " +
          "t2.id AS id2, t2.salary AS salary2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id"),
        Seq.empty)
    }
  }

  test("[3] connect scenario 5 (SQL JOIN): join after DROP/ADD same type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t1.salary AS salary1, " +
          "t2.id AS id2, t2.salary AS salary2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id ORDER BY id1"),
        Seq(Row(1, null, 1, null)))
    }
  }

  test("[3] connect scenario 6 (SQL JOIN): join after DROP/ADD different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      checkAnswer(
        spark.sql(
          "SELECT t1.id AS id1, t1.salary AS salary1, " +
          "t2.id AS id2, t2.salary AS salary2 " +
          "FROM t t1 JOIN t t2 ON t1.id = t2.id ORDER BY id1"),
        Seq(Row(1, null, 1, null)))
    }
  }

  // ---------------------------------------------------------------------------
  // Section [4]: Version pinning and refresh in Dataset (Connect)
  // In Connect, there is no distinction between show and collect.
  // Both re-analyze and always see the latest data and schema.
  // ---------------------------------------------------------------------------

  test("[4] connect scenario 1: df picks up new data after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      // In Connect, the df is re-analyzed on each execution
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[4] connect scenario 1.2: df.collect on same DataFrame after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      // In Connect, collect() re-analyzes the plan on each execution
      // (unlike classic where it reuses cached QueryExecution).
      // Both show() and collect() see the new data.
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[4] connect scenario 1b: count then collect consistency") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      // In Connect, both count() and collect() re-analyze the plan,
      // so both see the new data. No inconsistency unlike classic mode.
      assert(df.count() == 2)
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[4] connect scenario 2: df picks up ADD COLUMN with new schema") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // In Connect, df is re-analyzed and picks up the new schema
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test("[4] connect scenario 3: df after DROP COLUMN re-analyzes (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")

      // In Connect, df is re-analyzed with the new schema
      checkAnswer(df, Row(1))
    }
  }

  test("[4] connect scenario 4: df after DROP and recreate table (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      // In Connect, df re-analyzes to the new empty table
      checkAnswer(df, Seq.empty)
    }
  }

  test("[4] connect scenario 5: df after DROP/ADD column same name same type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      // In Connect, df re-analyzes. The old salary data is gone.
      checkAnswer(df, Row(1, null))
    }
  }

  test("[4] connect scenario 6: df after DROP/ADD column same name different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")
      writerSql("INSERT INTO t VALUES (2, 'BBB')")

      // In Connect, df re-analyzes with new schema (salary is now STRING).
      // The doc shows both rows with the new STRING schema.
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, null), Row(2, "BBB")))
    }
  }

  // ---------------------------------------------------------------------------
  // Section [5]: CACHE TABLE impact on reads (Connect)
  //
  // Note on external writes: In Connect, all writes go through the same
  // server-side DeltaLog (shared singleton cache). We cannot simulate true
  // external writes (bypassing DeltaLog) from the Connect client. These tests
  // document the same-JVM behavior where all writes update DeltaLog.currentSnapshot,
  // causing Delta's PrepareDeltaScan to discover changes and break the cache.
  //
  // For true external write behavior (cache pinning), see the classic suite's
  // scenarios 6b-6e which use writeExternalCommit via LogStore.
  // ---------------------------------------------------------------------------

  test("[5] connect scenario 1: CACHE TABLE with same-JVM writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      // Same-JVM write updates DeltaLog.currentSnapshot, so Delta's
      // PrepareDeltaScan discovers the change and breaks the cache.
      // Doc says: (1,100) only for true external writes, but same-JVM
      // writes bypass the cache pinning mechanism.
      writerSql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 2: session write then same-JVM external write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      // Session write invalidates cache (via SPARK-55631 refreshCache)
      spark.sql("INSERT INTO t VALUES (2, 200)")

      // Same-JVM "external" write also updates DeltaLog.currentSnapshot
      writerSql("INSERT INTO t VALUES (3, 300)")

      // Both writes visible because both go through same-JVM DeltaLog.
      // Doc says: (1,100),(2,200) only for true external writes.
      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 3: schema change breaks cache") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      // Schema change via same-JVM SQL goes through AlterTableExec which
      // triggers refreshCache (SPARK-55631), invalidating the cache.
      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // Doc says: schema changes break table state pinning.
      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 4: session schema change with external write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      // Session schema change invalidates cache (SPARK-55631)
      spark.sql("ALTER TABLE t ADD COLUMN new_column INT")

      // Same-JVM "external" write
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // Both visible because session ALTER TABLE broke the cache and
      // same-JVM write updated DeltaLog.currentSnapshot.
      // Doc says same for stalenessLimit=0.
      checkAnswer(
        spark.sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      spark.sql("UNCACHE TABLE t")
    }
  }

  test("[5] connect scenario 5: drop and recreate table") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      spark.sql("CACHE TABLE t")

      checkAnswer(spark.sql("SELECT * FROM t"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      // Doc says: empty table after drop and recreate.
      checkAnswer(spark.sql("SELECT * FROM t"), Seq.empty)

      spark.sql("UNCACHE TABLE IF EXISTS t")
    }
  }

  // ---------------------------------------------------------------------------
  // Section [5] continued: True external write simulation via filesystem
  // Uses writeExternalCommitViaFilesystem to write commit files directly to
  // disk, bypassing the server's DeltaLog. This is the Connect equivalent
  // of the classic suite's writeExternalCommit / scenarios 6b-6e.
  // ---------------------------------------------------------------------------

  test("[5] connect scenario 6b: CACHE TABLE pins data against external writes") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")
      spark.sql(s"CACHE TABLE cached_6b AS SELECT * FROM delta.`$path`")

      checkAnswer(spark.sql("SELECT * FROM cached_6b"), Row(1, 100))

      // True external write via filesystem -- server's DeltaLog not updated
      writeExternalCommitViaFilesystem(path, Seq((2, 200)))

      // Doc says: (1,100) only -- CACHE TABLE pins data against external writes.
      // The server's DeltaTableV2.lazy val snapshot is pinned and CacheManager
      // matches the cached plan.
      checkAnswer(spark.sql("SELECT * FROM cached_6b"), Row(1, 100))

      spark.sql("UNCACHE TABLE IF EXISTS cached_6b")

      // After uncaching, fresh query discovers external write (stalenessLimit=0).
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$path` ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[5] connect scenario 6c: session write invalidates, external not visible") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")
      spark.sql(s"CACHE TABLE cached_6c AS SELECT * FROM delta.`$path`")

      checkAnswer(spark.sql("SELECT * FROM cached_6c"), Row(1, 100))

      // Session write invalidates cache
      spark.sql(s"INSERT INTO delta.`$path` VALUES (2, 200)")

      // True external write via filesystem
      writeExternalCommitViaFilesystem(path, Seq((3, 300)))

      // Doc says: (1,100),(2,200) -- session write visible, external not.
      // After session write invalidates the cache, the next query re-analyzes.
      // The server's DeltaLog was updated by the session write but not the
      // external filesystem write.
      checkAnswer(
        spark.sql("SELECT * FROM cached_6c ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      spark.sql("UNCACHE TABLE IF EXISTS cached_6c")

      // After uncaching, fresh query discovers all data including external write.
      // The session INSERT updated server's DeltaLog, and UNCACHE triggers
      // a deltaLog.update() that discovers the external commit.
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$path` ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
    }
  }

  test("[5] connect scenario 6d: external schema change with CACHE") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")
      spark.sql(s"CACHE TABLE cached_6d AS SELECT * FROM delta.`$path`")

      checkAnswer(spark.sql("SELECT * FROM cached_6d"), Row(1, 100))

      // External schema change + data write via filesystem, bypassing
      // the server's DeltaLog entirely (writes both Metadata and AddFile actions)
      writeExternalSchemaChangeCommitViaFilesystem(path, Seq((2, 200, -1)))

      // Schema change breaks plan-shape match in CacheManager.
      // deltaLog.update() (stalenessLimit=0) discovers the new schema,
      // so the cache is effectively invalidated and fresh data is returned.
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$path` ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      spark.sql("UNCACHE TABLE IF EXISTS cached_6d")

      // After uncaching, fresh query sees all data with new schema.
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$path` ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test("[5] connect scenario 6e: session schema change then external write") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(s"CREATE TABLE delta.`$path` (id INT, salary INT) USING delta")
      spark.sql(s"INSERT INTO delta.`$path` VALUES (1, 100)")
      spark.sql(s"CACHE TABLE cached_6e AS SELECT * FROM delta.`$path`")

      checkAnswer(spark.sql("SELECT * FROM cached_6e"), Row(1, 100))

      // Session schema change invalidates cache (SPARK-55631)
      spark.sql(s"ALTER TABLE delta.`$path` ADD COLUMN new_column INT")

      // True external write via filesystem
      writeExternalCommitViaFilesystem(path, Seq((2, 200)))

      // Session schema change broke the cache. Next query re-analyzes.
      // The session's ALTER TABLE is visible (via server's DeltaLog),
      // but the external filesystem write may or may not be visible
      // depending on whether the server's DeltaLog lists new commits.
      // With stalenessLimit=0 (default), it discovers everything.
      checkAnswer(
        spark.sql(s"SELECT id, salary FROM delta.`$path` ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      spark.sql("UNCACHE TABLE IF EXISTS cached_6e")

      // After uncaching, fresh query discovers all data including external write.
      // The session ALTER TABLE updated server's DeltaLog, and UNCACHE triggers
      // a deltaLog.update() that discovers the external commit.
      // new_column is null because the external write only has (id, salary) data.
      checkAnswer(
        spark.sql(s"SELECT * FROM delta.`$path` ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, null)))
    }
  }
}

/** Same-session writes (default). */
class DeltaTableRefreshAndPinningConnectSuite
  extends DeltaTableRefreshAndPinningConnectSuiteBase

/**
 * Writes go through spark.newSession(). In Connect, this creates a new client session
 * to the same server. The server shares a single DeltaLog instance cache, so writes
 * from either session update the same snapshot. Verifies behavior is identical to
 * same-session writes. See trait scaladoc for details.
 */
class DeltaTableRefreshAndPinningConnectExternalSessionSuite
  extends DeltaTableRefreshAndPinningConnectSuiteBase {
  override protected def useExternalSession: Boolean = true
}

/**
 * Sets stalenessLimit to 1 hour on the server. Verifies that behavior is identical
 * to stalenessLimit=0 for in-JVM writes (since writes update DeltaLog.currentSnapshot
 * immediately). External filesystem writes produce different results because the
 * server's cached snapshot is returned without listing the filesystem.
 */
class DeltaTableRefreshAndPinningConnectStaleSuite
  extends DeltaTableRefreshAndPinningConnectSuiteBase {
  override protected def stalenessLimitMs: Long = 3600000L
}

/**
 * Combines external session + high staleness limit. Both parameters have no
 * observable effect for in-JVM writes, so this verifies the combination also
 * produces identical results.
 */
class DeltaTableRefreshAndPinningConnectStaleExternalSessionSuite
  extends DeltaTableRefreshAndPinningConnectSuiteBase {
  override protected def useExternalSession: Boolean = true
  override protected def stalenessLimitMs: Long = 3600000L
}
