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
import org.apache.spark.sql.types.{DataType, IntegerType, MetadataBuilder, StructField, StructType}

/**
 * Shared base for the repeated table access refresh tests.
 *
 * External writes are simulated by writing commit files directly into the table's `_delta_log` on
 * the local filesystem, which the Connect client and server share.
 */
trait DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  protected def spark: SparkSession

  protected def v2EnableMode: String = "NONE"

  /** True in the Connect implementation, false in classic. Branches expectations. */
  def isConnect: Boolean

  /** Spark minor version bucket ("4.0", "4.1", "4.2+") that the asserted behavior keys off. */
  protected def sparkVersionBucket: String = {
    // Parse major and minor numerically so the bucket stays correct once Spark reaches 4.10,
    // where a lexicographic string compare would wrongly rank "4.10" below "4.2".
    val versionParts = spark.version.split('.')
    val major = versionParts(0).toInt
    val minor = versionParts(1).toInt
    if (major > 4 || (major == 4 && minor >= 2)) "4.2+"
    else if (major == 4 && minor >= 1) "4.1"
    else "4.0"
  }

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
        s"Expected error condition '$condition' was not found")
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

  /** Asserts the full table contents (ordered by id) match `expectedRows`. */
  protected def assertFinalTableState(tableRef: String, expectedRows: Seq[Row]): Unit =
    checkAnswer(spark.sql(s"SELECT * FROM $tableRef ORDER BY id"), expectedRows)

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

  /**
   * TBLPROPERTIES clause enabling the requested table features. Column mapping is required before a
   * column can be dropped or re-added; type widening allows an ALTER COLUMN to a wider type.
   */
  private def tableProperties(columnMapping: Boolean, typeWidening: Boolean): String = {
    val props =
      (if (columnMapping) Seq("'delta.columnMapping.mode' = 'name'") else Nil) ++
      (if (typeWidening) Seq("'delta.enableTypeWidening' = 'true'") else Nil)
    if (props.isEmpty) "" else s" TBLPROPERTIES (${props.mkString(", ")})"
  }

  /**
   * Seeds the initial `(1, 100)` row, returning true when the table was seeded. Under STRICT the
   * DSv2 write path rejects writes to column-mapped tables (master's DeltaV2WriteBuilder guard), so
   * such a table can never be seeded; in that case assert the rejection (an
   * UnsupportedOperationException, or its SparkUnsupportedOperationException subclass over Connect)
   * and return false so the caller skips the scenario body.
   */
  private def seedInitialRow(columnMapping: Boolean): Boolean = {
    if (columnMapping && v2EnableMode == "STRICT") {
      val expectedError = "DSv2 writes are not supported on column-mapped Delta tables"
      val seedError =
        try {
          insertInitialData("t")
          None
        } catch {
          case e: UnsupportedOperationException => Option(e.getMessage)
        }
      assert(
        seedError.exists(_.contains(expectedError)),
        s"expected '$expectedError', got: $seedError")
      false
    } else {
      insertInitialData("t")
      true
    }
  }

  /**
   * Runs `body` against a fresh managed catalog table `t` already holding `(1, 100)`, optionally
   * enabling column mapping or type widening. Returns without running `body` when STRICT rejects
   * the seed write to a column-mapped table.
   */
  protected def withInitialTable(
      columnMapping: Boolean = false,
      typeWidening: Boolean = false)(body: String => Unit): Unit =
    withTable("t") {
      spark.sql(
        "CREATE TABLE t (id INT, salary INT) USING delta" +
        tableProperties(columnMapping, typeWidening))
      if (seedInitialRow(columnMapping)) {
        body("t")
      }
    }

  /**
   * Runs `body` against a fresh external catalog table `t` already holding `(1, 100)`, optionally
   * enabling column mapping or type widening, passing the table's storage path so the body can
   * stage external commits there before re-reading. Returns without running `body` when STRICT
   * rejects the seed write to a column-mapped table.
   */
  protected def withExternalTable(
      columnMapping: Boolean = false,
      typeWidening: Boolean = false)(body: String => Unit): Unit = {
    val dir = Files.createTempDirectory("refresh-ext").toFile
    try {
      withTable("t") {
        spark.sql(
          s"CREATE TABLE t (id INT, salary INT) USING delta LOCATION '${dir.getAbsolutePath}'" +
          tableProperties(columnMapping, typeWidening))
        if (seedInitialRow(columnMapping)) {
          body(dir.getAbsolutePath)
        }
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

  private def latestMetadataLine(path: String): String =
    commitJsonFiles(path).sortBy(_.getName).flatMap { f =>
      new String(Files.readAllBytes(f.toPath), UTF_8).split("\n")
    }.filter(_.contains("\"metaData\"")).lastOption.getOrElse(
      throw new RuntimeException("No metaData action found in commit files"))

  /** The table's current schema, recovered from the stored schemaString. */
  private def currentSchema(path: String): StructType = {
    val escaped = SchemaStringRegex.findFirstMatchIn(latestMetadataLine(path)).map(_.group(1))
      .getOrElse(throw new RuntimeException("Could not extract schemaString from metadata"))
    DataType.fromJson(escaped.replace("\\\"", "\"")).asInstanceOf[StructType]
  }

  /** Returns the metadata line with its schemaString replaced (id and configuration preserved). */
  private def metadataLineWithSchema(metadataLine: String, schema: StructType): String = {
    val escaped = schema.json.replace("\"", "\\\"")
    SchemaStringRegex.replaceFirstIn(
      metadataLine, java.util.regex.Matcher.quoteReplacement(s""""schemaString":"$escaped""""))
  }

  /** Returns the metadata line with a fresh table id (simulating a recreated table). */
  private def metadataLineWithNewId(metadataLine: String): String =
    IdRegex.replaceFirstIn(
      metadataLine, java.util.regex.Matcher.quoteReplacement(s""""id":"${UUID.randomUUID()}""""))

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

  /** Appends three column rows externally without a schema change (table is already 3 columns). */
  protected def externalThreeColumnDataWrite(path: String, rows: Seq[(Int, Int, Int)]): Unit =
    writeCommit(path, Seq(writeParquetAndGetAddFileAction(path, idSalaryNewColumnRowsToDf(rows))))

  protected def externalAddColumnAndWrite(path: String, rows: Seq[(Int, Int, Int)]): Unit = {
    val newSchema = currentSchema(path).add("new_column", IntegerType, nullable = true)
    writeCommit(path, Seq(
      metadataLineWithSchema(latestMetadataLine(path), newSchema),
      writeParquetAndGetAddFileAction(path, idSalaryNewColumnRowsToDf(rows))))
  }

  protected def externalDropAndRecreate(path: String): Unit = {
    val recreatedMeta = metadataLineWithNewId(latestMetadataLine(path))
    writeCommit(path, removeActiveFiles(path) :+ recreatedMeta)
  }

  private val MaxColumnIdRegex =
    """"delta\.columnMapping\.maxColumnId"\s*:\s*"(\d+)"""".r

  /** Drops a top level column by rewriting the metadata schema with the column removed. */
  protected def externalDropColumn(path: String, col: String): Unit = {
    val newSchema = StructType(currentSchema(path).filterNot(_.name == col))
    writeCommit(path, Seq(metadataLineWithSchema(latestMetadataLine(path), newSchema)))
  }

  /** Changes a column's type by rewriting the metadata schema (e.g. INT widened to BIGINT). */
  protected def externalChangeColumnType(path: String, col: String, newType: DataType): Unit = {
    val newSchema = StructType(currentSchema(path).map { f =>
      if (f.name == col) f.copy(dataType = newType) else f
    })
    writeCommit(path, Seq(metadataLineWithSchema(latestMetadataLine(path), newSchema)))
  }

  /**
   * Drops `col` then re-adds it with the same name and `newType`. The re-added column gets a fresh
   * column-mapping id and physical name (and bumps `maxColumnId`), mirroring what Delta does for a
   * real DROP then ADD COLUMN on a column-mapping table.
   */
  protected def externalDropAndReAddColumn(path: String, col: String, newType: DataType): Unit = {
    val meta = latestMetadataLine(path)
    val maxId = MaxColumnIdRegex.findFirstMatchIn(meta).map(_.group(1).toInt).getOrElse(
      throw new RuntimeException("Table is not column-mapping enabled"))
    val newId = maxId + 1
    val reAdded = new StructField(
      col,
      newType,
      nullable = true,
      new MetadataBuilder()
        .putLong("delta.columnMapping.id", newId)
        .putString("delta.columnMapping.physicalName", s"col-${UUID.randomUUID()}")
        .build())
    val newSchema = StructType(currentSchema(path).fields.filterNot(_.name == col) :+ reAdded)
    val withSchema = metadataLineWithSchema(meta, newSchema)
    val withBumpedMaxId = MaxColumnIdRegex.replaceFirstIn(
      withSchema,
      java.util.regex.Matcher.quoteReplacement(s""""delta.columnMapping.maxColumnId":"$newId""""))
    writeCommit(path, Seq(withBumpedMaxId))
  }
}
