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

package org.apache.spark.sql.delta

import io.delta.tables.shared.DeltaTableRefreshSharedBase
import org.apache.spark.sql.delta.actions.{AddFile, Metadata => DeltaMetadata}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, MetadataBuilder, StringType, StructField, StructType}
import org.apache.spark.util.Utils

/**
 * Base trait with shared test helpers for the table refresh, version pinning,
 * and schema change detection test suites. Provides table setup helpers,
 * writer session management, and external commit simulation via LogStore.
 *
 * Mixed into category test traits via self-type:
 *   - [[DeltaTempViewRefreshTests]] (Section [1])
 *   - [[DeltaRepeatedAccessRefreshTests]] (Section [2])
 *   - [[DeltaJoinRefreshTests]] (Section [3])
 *   - [[DeltaCacheTableRefreshTests]] (Section [5])
 */
trait DeltaTableRefreshTestBase extends DeltaTableRefreshSharedBase {
  self: QueryTest with SharedSparkSession with DeltaSQLCommandTest =>

  import testImplicits._

  override def isConnect: Boolean = false

  /** Override in subclasses to set the V2 enable mode. */
  override protected def v2EnableMode: String = "NONE"

  /**
   * Override in subclasses to use spark.newSession() for writes. Note that in a single JVM,
   * newSession() shares the same DeltaLog instance cache, so the DeltaLog.currentSnapshot
   * is updated immediately by the writer. This does NOT simulate a true external writer
   * (separate JVM). See suite scaladoc for details.
   */
  protected def useExternalSession: Boolean = false

  /**
   * Returns a session for performing writes. When [[useExternalSession]] is true,
   * returns a new session to simulate external writers.
   */
  protected def writerSession: org.apache.spark.sql.SparkSession = {
    if (useExternalSession) spark.newSession() else spark
  }

  /** Execute SQL using the writer session. */
  override protected def writerSql(sqlText: String): Unit = {
    writerSession.sql(sqlText)
  }

  override protected def createSimpleTable(tableName: String): Unit = {
    sql(s"CREATE TABLE $tableName (id INT, salary INT) USING delta")
  }

  override protected def createColumnMappingTable(tableName: String): Unit = {
    sql(
      s"""CREATE TABLE $tableName (id INT, salary INT) USING delta
         |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
  }

  override protected def createTypeWideningTable(tableName: String): Unit = {
    sql(
      s"""CREATE TABLE $tableName (id INT, salary INT) USING delta
         |TBLPROPERTIES (
         |  'delta.columnMapping.mode' = 'name',
         |  'delta.enableTypeWidening' = 'true'
         |)""".stripMargin)
  }

  override protected def insertInitialData(tableName: String): Unit = {
    sql(s"INSERT INTO $tableName VALUES (1, 100)")
  }

  protected def getTablePath(tableName: String): String = {
    DeltaLog.forTable(spark, TableIdentifier(tableName)).dataPath.toString
  }

  /**
   * Simulates an external write to a named table by writing commit files
   * directly via LogStore, bypassing the DeltaLog commit path entirely.
   *
   * This is the Delta equivalent of Spark's
   * catalog("testcat").loadTable(ident).truncateTable() pattern
   * from DataSourceV2DataFrameSuite (SPARK-54022). It modifies the table
   * at the storage layer without notifying the CacheManager.
   *
   * The session catalog's DeltaTableV2 and its lazy val snapshot are NOT
   * updated. The DeltaLog.currentSnapshot is NOT updated (the commit
   * bypasses DeltaLog.commit()). This means:
   * - CacheManager plan matching still uses the old snapshot -> cache hit
   */
  protected def writeExternalCommit(
      tableName: String,
      data: DataFrame,
      newMetadata: Option[DeltaMetadata] = None): Unit = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
    deltaLog.update()
    val currentVersion = deltaLog.snapshot.version
    val tablePath = deltaLog.dataPath

    val tempDir = Utils.createTempDir()
    try {
      data.coalesce(1).write.parquet(s"${tempDir.getAbsolutePath}/out")
      val parquetFile = new java.io.File(tempDir, "out").listFiles()
        .filter(_.getName.endsWith(".parquet")).head
      val targetName = s"ext-commit-v${currentVersion + 1}.snappy.parquet"
      java.nio.file.Files.copy(
        parquetFile.toPath,
        java.nio.file.Paths.get(tablePath.toUri).resolve(targetName))

      val addFile = AddFile(
        path = targetName,
        partitionValues = Map.empty,
        size = parquetFile.length(),
        modificationTime = System.currentTimeMillis(),
        dataChange = true)

      // Build actions: optional metadata change + data file
      val actions = newMetadata.map(m => Iterator(
        JsonUtils.toJson(m.wrap),
        JsonUtils.toJson(addFile.wrap)
      )).getOrElse(Iterator(
        JsonUtils.toJson(addFile.wrap)
      ))

      // Write commit file directly via LogStore, bypassing DeltaLog.commit()
      deltaLog.store.write(
        FileNames.unsafeDeltaFile(deltaLog.logPath, currentVersion + 1),
        actions,
        overwrite = false,
        deltaLog.newDeltaHadoopConf())
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  /**
   * Writes an external commit containing only a Metadata action (no data file).
   * Used to simulate external schema changes (DROP COLUMN, DROP/ADD column)
   * that bypass DeltaLog.commit().
   */
  protected def writeExternalMetadataOnlyCommit(
      tableName: String,
      newMetadata: DeltaMetadata): Unit = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
    deltaLog.update()
    val currentVersion = deltaLog.snapshot.version
    deltaLog.store.write(
      FileNames.unsafeDeltaFile(deltaLog.logPath, currentVersion + 1),
      Iterator(JsonUtils.toJson(newMetadata.wrap)),
      overwrite = false,
      deltaLog.newDeltaHadoopConf())
  }

  /**
   * Writes an external commit that removes all existing data files and writes
   * new Metadata with a fresh UUID, simulating an external DROP and recreate.
   * When columnMapping is true, all columns get new column mapping IDs and
   * physical names so that the schema change detection triggers
   * DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS.
   */
  protected def writeExternalDropAndRecreateCommit(
      tableName: String,
      columnMapping: Boolean): Unit = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
    deltaLog.update()
    val currentVersion = deltaLog.snapshot.version
    val currentMetadata = deltaLog.snapshot.metadata

    val removeActions = deltaLog.snapshot.allFiles.collect().map(_.remove)

    val newMetadata = if (columnMapping) {
      val newFields = currentMetadata.schema.fields.zipWithIndex.map { case (field, idx) =>
        val newMeta = new MetadataBuilder()
          .withMetadata(field.metadata)
          .putLong(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY, 100L + idx)
          .putString(DeltaColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY,
            s"col-recreated-${java.util.UUID.randomUUID().toString.take(8)}")
          .build()
        field.copy(metadata = newMeta)
      }
      currentMetadata.copy(
        id = java.util.UUID.randomUUID().toString,
        schemaString = StructType(newFields).json,
        configuration = currentMetadata.configuration +
          (DeltaConfigs.COLUMN_MAPPING_MAX_ID.key ->
            (100L + currentMetadata.schema.fields.length).toString))
    } else {
      currentMetadata.copy(id = java.util.UUID.randomUUID().toString)
    }

    val actions = removeActions.map(r => JsonUtils.toJson(r.wrap)).iterator ++
      Iterator(JsonUtils.toJson(newMetadata.wrap))

    deltaLog.store.write(
      FileNames.unsafeDeltaFile(deltaLog.logPath, currentVersion + 1),
      actions,
      overwrite = false,
      deltaLog.newDeltaHadoopConf())
  }

  /** Constructs a StructField with column mapping annotations (ID and physical name). */
  protected def buildColumnMappingField(
      name: String,
      dataType: DataType,
      nullable: Boolean,
      columnId: Long): StructField = {
    val meta = new MetadataBuilder()
      .putLong(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY, columnId)
      .putString(DeltaColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY,
        s"col-ext-${java.util.UUID.randomUUID().toString.take(8)}")
      .build()
    StructField(name, dataType, nullable, meta)
  }

  // ---------------------------------------------------------------------------
  // Shared base hook implementations (classic). Error assertions use Spark's
  // checkError; external writes use the LogStore based helpers above.
  // ---------------------------------------------------------------------------

  override protected def assertSchemaChangeError(f: => Unit): Unit = {
    checkError(
      exception = intercept[DeltaAnalysisException] { f },
      condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS",
      parameters = Map("schemaDiff" -> "(?s).*", "legacyFlagMessage" -> ""),
      matchPVals = true)
  }

  override protected def assertAmbiguousColumnError(f: => Unit): Unit = {
    // Only invoked from connect branches; classic never reaches this path.
    checkError(
      exception = intercept[AnalysisException] { f },
      condition = "AMBIGUOUS_COLUMN_OR_FIELD",
      parameters = Map("name" -> ".*", "n" -> ".*"),
      matchPVals = true)
  }

  override protected def assertArityMismatchError(f: => Unit): Unit = {
    checkError(
      exception = intercept[AnalysisException] { f },
      condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
      parameters = Map("tableName" -> ".*", "tableColumns" -> ".*",
        "dataColumns" -> ".*", "reason" -> ".*"),
      matchPVals = true)
  }

  override protected def assertError(condition: String, messageContains: String)(
      f: => Unit): Unit = {
    // strictConnect is never true for classic; provided for symmetry.
    val exception = intercept[Exception] { f }
    assert(exception.isInstanceOf[org.apache.spark.SparkThrowable],
      s"Expected a SparkThrowable but got ${exception.getClass.getName}: ${exception.getMessage}")
    val throwable = exception.asInstanceOf[org.apache.spark.SparkThrowable]
    assert(throwable.getCondition == condition,
      s"Expected error condition '$condition' but got '${throwable.getCondition}': " +
        s"${exception.getMessage}")
    assert(exception.getMessage.contains(messageContains),
      s"Expected message to contain '$messageContains' but was: ${exception.getMessage}")
  }

  override protected def assertExternalStrictConflict(f: => Unit): Unit = {
    val exception = intercept[java.nio.file.FileAlreadyExistsException] { f }
    assert(exception.getMessage != null)
  }

  override protected def assertPinnedSnapshotMissingError(f: => Unit): Unit = {
    // Only invoked from Connect's strictConnectPinned branches; classic never reaches it.
    fail("assertPinnedSnapshotMissingError should never be invoked on classic")
  }

  override protected def withRefreshTable(body: String => Unit): Unit = {
    withTable("t") { body("t") }
  }

  override protected def externalDataWrite(tableRef: String, rows: Seq[(Int, Int)]): Unit = {
    writeExternalCommit(tableRef, rows.toDF("id", "salary"))
  }

  override protected def externalDataWriteWide(
      tableRef: String, rows: Seq[(Int, Int, Int)]): Unit = {
    writeExternalCommit(tableRef, rows.toDF("id", "salary", "new_column"))
  }

  override protected def externalAddColumnAndWrite(
      tableRef: String, rows: Seq[(Int, Int, Int)]): Unit = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableRef))
    val newSchema = deltaLog.snapshot.metadata.schema
      .add("new_column", IntegerType, nullable = true)
    val newMetadata = deltaLog.snapshot.metadata.copy(schemaString = newSchema.json)
    writeExternalCommit(
      tableRef, rows.toDF("id", "salary", "new_column"), newMetadata = Some(newMetadata))
  }

  override protected def externalDropColumn(tableRef: String, column: String): Unit = {
    val currentMetadata = DeltaLog.forTable(spark, TableIdentifier(tableRef)).snapshot.metadata
    val newSchema = StructType(currentMetadata.schema.fields.filterNot(_.name == column))
    writeExternalMetadataOnlyCommit(tableRef, currentMetadata.copy(schemaString = newSchema.json))
  }

  override protected def externalDropAndRecreate(
      tableRef: String, columnMapping: Boolean): Unit = {
    writeExternalDropAndRecreateCommit(tableRef, columnMapping = columnMapping)
  }

  override protected def externalReplaceColumn(
      tableRef: String, column: String, newType: Option[String]): Unit = {
    val resolvedType: DataType = newType.map(_.toLowerCase(java.util.Locale.ROOT)) match {
      case Some("string") => StringType
      case Some("long") | Some("bigint") => LongType
      case _ => IntegerType
    }
    val currentMetadata = DeltaLog.forTable(spark, TableIdentifier(tableRef)).snapshot.metadata
    val newColumnId = currentMetadata.columnMappingMaxId + 1
    val newFields = currentMetadata.schema.fields.map { field =>
      if (field.name == column) {
        buildColumnMappingField(
          name = column, dataType = resolvedType, nullable = true, columnId = newColumnId)
      } else {
        field
      }
    }
    val newMetadata = currentMetadata.copy(
      schemaString = StructType(newFields).json,
      configuration = currentMetadata.configuration +
        (DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> newColumnId.toString))
    writeExternalMetadataOnlyCommit(tableRef, newMetadata)
  }
}
