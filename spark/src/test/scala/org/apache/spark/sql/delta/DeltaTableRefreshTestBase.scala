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

import org.apache.spark.sql.delta.actions.{AddFile, Metadata => DeltaMetadata}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, MetadataBuilder, StringType, StructField, StructType}
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
 *   - [[DeltaDatasetPinningTests]] (Section [4])
 *   - [[DeltaCacheTableRefreshTests]] (Section [5])
 */
trait DeltaTableRefreshTestBase {
  self: QueryTest with SharedSparkSession with DeltaSQLCommandTest =>

  import testImplicits._

  /** Override in subclasses to set the V2 enable mode. */
  protected def v2EnableMode: String = "NONE"

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
  protected def writerSql(sqlText: String): Unit = {
    writerSession.sql(sqlText)
  }

  protected def createSimpleTable(tableName: String): Unit = {
    sql(s"CREATE TABLE $tableName (id INT, salary INT) USING delta")
  }

  protected def createColumnMappingTable(tableName: String): Unit = {
    sql(
      s"""CREATE TABLE $tableName (id INT, salary INT) USING delta
         |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')""".stripMargin)
  }

  protected def insertInitialData(tableName: String): Unit = {
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
}
