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
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.util.Utils

/**
 * Classic base for the repeated table access refresh tests: table setup helpers and external
 * commit simulation via LogStore. Mixed into [[DeltaRepeatedAccessRefreshTests]] (Section [2]).
 */
trait DeltaTableRefreshTestBase extends DeltaTableRefreshSharedBase {
  self: QueryTest with SharedSparkSession with DeltaSQLCommandTest =>

  import testImplicits._

  /** Override in subclasses to set the V2 enable mode. */
  override protected def v2EnableMode: String = "NONE"

  override protected def writerSql(sqlText: String): Unit = {
    spark.sql(sqlText)
  }

  override protected def createSimpleTable(tableName: String): Unit = {
    sql(s"CREATE TABLE $tableName (id INT, salary INT) USING delta")
  }

  override protected def insertInitialData(tableName: String): Unit = {
    sql(s"INSERT INTO $tableName VALUES (1, 100)")
  }

  /**
   * Simulates an external write to a named table by writing commit files directly via LogStore,
   * bypassing the DeltaLog commit path (and thus the DeltaLog.currentSnapshot / CacheManager).
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
   * Writes an external commit that removes all existing data files and writes
   * new Metadata with a fresh UUID, simulating an external DROP and recreate.
   */
  protected def writeExternalDropAndRecreateCommit(tableName: String): Unit = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
    deltaLog.update()
    val currentVersion = deltaLog.snapshot.version
    val currentMetadata = deltaLog.snapshot.metadata

    val removeActions = deltaLog.snapshot.allFiles.collect().map(_.remove)
    val newMetadata = currentMetadata.copy(id = java.util.UUID.randomUUID().toString)

    val actions = removeActions.map(r => JsonUtils.toJson(r.wrap)).iterator ++
      Iterator(JsonUtils.toJson(newMetadata.wrap))

    deltaLog.store.write(
      FileNames.unsafeDeltaFile(deltaLog.logPath, currentVersion + 1),
      actions,
      overwrite = false,
      deltaLog.newDeltaHadoopConf())
  }

  // ---------------------------------------------------------------------------
  // Shared base hook implementations (classic). The arity assertion uses Spark's
  // checkError; external writes use the LogStore based helpers above.
  // ---------------------------------------------------------------------------

  override protected def assertArityMismatchError(f: => Unit): Unit = {
    checkError(
      exception = intercept[AnalysisException] { f },
      condition = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
      parameters = Map(
        "tableName" -> ".*", "tableColumns" -> ".*", "dataColumns" -> ".*"),
      matchPVals = true)
  }

  override protected def withRefreshTable(body: String => Unit): Unit = {
    withTable("t") { body("t") }
  }

  override protected def externalDataWrite(tableRef: String, rows: Seq[(Int, Int)]): Unit = {
    writeExternalCommit(tableRef, rows.toDF("id", "salary"))
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

  override protected def externalDropAndRecreate(tableRef: String): Unit = {
    writeExternalDropAndRecreateCommit(tableRef)
  }
}
