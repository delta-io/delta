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
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DataType, IntegerType, MetadataBuilder, StringType, StructField, StructType}
import org.apache.spark.util.Utils

/**
 * Tests that document and verify existing Delta behavior for table refresh,
 * version pinning, and schema change detection. These tests cover scenarios from the
 * "Refreshing and pinning tables in Spark" design doc.
 *
 * The suite covers five areas:
 *   1. Temp views with stored plans
 *   2. Repeated table access with external changes
 *   3. Incrementally constructed queries (join of separately analyzed DataFrames)
 *   4. Version pinning and refresh in Dataset (show vs collect)
 *   5. CACHE TABLE impact on reads
 *
 * The base trait is parameterized by:
 *   - V2_ENABLE_MODE (NONE, AUTO) for connector mode coverage
 *   - useExternalSession: when true, writes go through spark.newSession()
 *   - stalenessLimitMs: configures the staleness time limit for async updates
 *
 * Important notes on parameterization:
 *
 * External session (spark.newSession()): In a single JVM, all SparkSessions share the same
 * DeltaLog instance cache (a static Guava Cache on the DeltaLog companion object). When any
 * session commits a write, the DeltaLog.currentSnapshot is updated in place and all other
 * sessions immediately see the new version. This means spark.newSession() is NOT equivalent
 * to a true external writer (separate JVM/cluster). We parameterize with it anyway to verify
 * that the behavior is indeed identical, which documents that Delta's refresh mechanism is
 * driven by the shared DeltaLog, not by Spark's session-level catalog state.
 *
 * Staleness limit (stalenessLimitMs): The delta.stalenessLimit config controls whether
 * deltaLog.update() returns a cached snapshot without doing a filesystem listing. However,
 * since all writes in the same JVM go through the shared DeltaLog and update currentSnapshot
 * as a side effect of committing, the staleness limit has no observable effect in single-JVM
 * tests for normal writes. The snapshot is always already fresh by the time the reader queries
 * it. We parameterize with a high staleness limit to verify that most behaviors are identical,
 * documenting this JVM-level constraint.
 *
 * The exception is Section [5] scenario 6, which writes a commit directly to the filesystem
 * via [[org.apache.spark.sql.delta.storage.LogStore]], bypassing the DeltaLog API entirely.
 * This simulates an external process/cluster and is the only scenario where staleness limit
 * produces observably different results: with staleness = 0 the reader discovers the new
 * commit immediately, while with a high staleness limit the reader returns the cached snapshot.
 */
trait DeltaTableRefreshAndPinningSuiteBase
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  import testImplicits._

  /** Override in subclasses to set the V2 enable mode. */
  protected def v2EnableMode: String = "NONE"

  /**
   * Override in subclasses to use spark.newSession() for writes. Note that in a single JVM,
   * newSession() shares the same DeltaLog instance cache, so the DeltaLog.currentSnapshot
   * is updated immediately by the writer. This does NOT simulate a true external writer
   * (separate JVM). See class scaladoc for details.
   */
  protected def useExternalSession: Boolean = false

  /**
   * Override in subclasses to set a non-zero staleness limit. Note that in a single JVM,
   * writes update DeltaLog.currentSnapshot in place, so the staleness limit has no observable
   * effect: the snapshot is already fresh before the reader queries it. To observe staleness,
   * writes must come from a separate process. See class scaladoc for details.
   */
  protected def stalenessLimitMs: Long = 0L

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaSQLConf.DELTA_ALTER_TABLE_DROP_COLUMN_ENABLED.key, "true")
      .set(DeltaSQLConf.V2_ENABLE_MODE.key, v2EnableMode)
      .set(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key,
        s"${stalenessLimitMs}ms")
  }

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
   * - deltaLog.update(stalenessAcceptable=true) respects stalenessLimit
   */
  protected def writeExternalCommit(
      tableName: String,
      data: DataFrame,
      newMetadata: Option[DeltaMetadata] = None): Unit = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
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
  private def buildColumnMappingField(
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

  /**
   * Blocks the async update in [[SnapshotManagement.update]] to make staleness
   * behavior deterministic on local disk. In production, the foreground thread
   * returns the stale snapshot before the background filesystem listing completes.
   * On local disk the listing is near instant, so the async task can update
   * currentSnapshot before the foreground reads it. Setting asyncUpdateTask to
   * a non completed future prevents a new async task from being submitted
   * (line 1104 guard), guaranteeing the stale snapshot is returned.
   */
  private def withBlockedAsyncUpdate(tableName: String)(body: => Unit): Unit = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
    val savedTask = deltaLog.asyncUpdateTask
    val blockingFuture = new java.util.concurrent.CompletableFuture[Unit]()
    deltaLog.asyncUpdateTask = blockingFuture
    try {
      body
    } finally {
      blockingFuture.complete(())
      deltaLog.asyncUpdateTask = savedTask
    }
  }

  // ---------------------------------------------------------------------------
  // Section [1]: Temp views with stored plans
  // ---------------------------------------------------------------------------

  test("[1.1] temp view picks up writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1] scenario 2: temp view with ADD COLUMN preserves original schema") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // View preserves original schema (id, salary) but picks up new data
      checkAnswer(
        sql("SELECT * FROM v ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[1] scenario 3: temp view with DROP COLUMN throws (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")

      val e = intercept[DeltaAnalysisException] {
        sql("SELECT * FROM v").collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[1] scenario 4: temp view after DROP and recreate table (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      // Column IDs changed, so reading the view should fail
      val e = intercept[DeltaAnalysisException] {
        sql("SELECT * FROM v").collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[1] scenario 4: temp view after DROP and recreate table (no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      // Without column mapping, no column ID check. New table is empty.
      checkAnswer(sql("SELECT * FROM v"), Seq.empty)
    }
  }

  test("[1] scenario 5: temp view after DROP/ADD column same name same type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      val e = intercept[DeltaAnalysisException] {
        sql("SELECT * FROM v").collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[1] scenario 6: temp view after DROP/ADD column same name different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      val e = intercept[DeltaAnalysisException] {
        sql("SELECT * FROM v").collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[1] scenario 7: temp view after ALTER COLUMN TYPE INT to BIGINT") {
    withTable("t") {
      sql(
        """CREATE TABLE t (id INT, salary INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.enableTypeWidening' = 'true'
          |)""".stripMargin)
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writerSql("ALTER TABLE t ALTER COLUMN salary TYPE BIGINT")

      val e = intercept[DeltaAnalysisException] {
        sql("SELECT * FROM v").collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  // ---------------------------------------------------------------------------
  // Section [1] external: Temp views with external modifications
  // These test the "Connector w/ cache" behavior from the design doc.
  // With high stalenessLimit, external changes are invisible because
  // deltaLog.update(stalenessAcceptable=true) returns the cached snapshot
  // without doing a filesystem listing.
  // ---------------------------------------------------------------------------

  test("[1] scenario 1 external: temp view with external data write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writeExternalCommit("t", Seq((2, 200)).toDF("id", "salary"))

      if (stalenessLimitMs == 0L) {
        checkAnswer(
          sql("SELECT * FROM v ORDER BY id"),
          Seq(Row(1, 100), Row(2, 200)))
      } else {
        // In production (HDFS/S3), the background listing takes real time,
        // so the foreground always returns the stale snapshot before the async
        // task completes. On local disk the listing is near instant and the
        // async task can win the race. Block it to simulate production timing.
        withBlockedAsyncUpdate("t") {
          checkAnswer(sql("SELECT * FROM v"), Row(1, 100))
        }
      }
    }
  }

  test("[1] scenario 2 external: temp view with external ADD COLUMN") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t"))
      val currentMetadata = deltaLog.snapshot.metadata
      val newSchema = currentMetadata.schema
        .add("new_column", IntegerType, nullable = true)
      val newMetadata = currentMetadata.copy(schemaString = newSchema.json)

      writeExternalCommit(
        "t",
        Seq((2, 200, -1)).toDF("id", "salary", "new_column"),
        newMetadata = Some(newMetadata))

      if (stalenessLimitMs == 0L) {
        // View preserves original schema (id, salary) but picks up new data
        checkAnswer(
          sql("SELECT * FROM v ORDER BY id"),
          Seq(Row(1, 100), Row(2, 200)))
      } else {
        // In production (HDFS/S3), the background listing takes real time,
        // so the foreground always returns the stale snapshot before the async
        // task completes. On local disk the listing is near instant and the
        // async task can win the race. Block it to simulate production timing.
        withBlockedAsyncUpdate("t") {
          checkAnswer(sql("SELECT * FROM v"), Row(1, 100))
        }
      }
    }
  }

  test("[1] scenario 3 external: temp view with external DROP COLUMN (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t"))
      val currentMetadata = deltaLog.snapshot.metadata
      val newSchema = StructType(
        currentMetadata.schema.fields.filterNot(_.name == "salary"))
      val newMetadata = currentMetadata.copy(schemaString = newSchema.json)

      writeExternalMetadataOnlyCommit("t", newMetadata)

      if (stalenessLimitMs == 0L) {
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql("SELECT * FROM v").collect()
          },
          condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS",
          parameters = Map("schemaDiff" -> "(?s).*", "legacyFlagMessage" -> ""),
          matchPVals = true)
      } else {
        // In production (HDFS/S3), the background listing takes real time,
        // so the foreground always returns the stale snapshot before the async
        // task completes. On local disk the listing is near instant and the
        // async task can win the race. Block it to simulate production timing.
        withBlockedAsyncUpdate("t") {
          checkAnswer(sql("SELECT * FROM v"), Row(1, 100))
        }
      }
    }
  }

  test("[1] scenario 4 external: temp view after external DROP and recreate " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writeExternalDropAndRecreateCommit("t", columnMapping = true)

      if (stalenessLimitMs == 0L) {
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql("SELECT * FROM v").collect()
          },
          condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS",
          parameters = Map("schemaDiff" -> "(?s).*", "legacyFlagMessage" -> ""),
          matchPVals = true)
      } else {
        // In production (HDFS/S3), the background listing takes real time,
        // so the foreground always returns the stale snapshot before the async
        // task completes. On local disk the listing is near instant and the
        // async task can win the race. Block it to simulate production timing.
        withBlockedAsyncUpdate("t") {
          checkAnswer(sql("SELECT * FROM v"), Row(1, 100))
        }
      }
    }
  }

  test("[1] scenario 4 external: temp view after external DROP and recreate " +
      "(no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      writeExternalDropAndRecreateCommit("t", columnMapping = false)

      if (stalenessLimitMs == 0L) {
        // Without column mapping, no column ID check. Existing data is removed.
        checkAnswer(sql("SELECT * FROM v"), Seq.empty)
      } else {
        // In production (HDFS/S3), the background listing takes real time,
        // so the foreground always returns the stale snapshot before the async
        // task completes. On local disk the listing is near instant and the
        // async task can win the race. Block it to simulate production timing.
        withBlockedAsyncUpdate("t") {
          checkAnswer(sql("SELECT * FROM v"), Row(1, 100))
        }
      }
    }
  }

  test("[1] scenario 5 external: temp view after external DROP/ADD column " +
      "same name same type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t"))
      val currentMetadata = deltaLog.snapshot.metadata
      val oldMaxId = currentMetadata.columnMappingMaxId
      val newSalaryId = oldMaxId + 1
      val idField = currentMetadata.schema("id")
      val newSalaryField = buildColumnMappingField(
        name = "salary", dataType = IntegerType, nullable = true, columnId = newSalaryId)
      val newSchema = StructType(Seq(idField, newSalaryField))
      val newMetadata = currentMetadata.copy(
        schemaString = newSchema.json,
        configuration = currentMetadata.configuration +
          (DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> newSalaryId.toString))

      writeExternalMetadataOnlyCommit("t", newMetadata)

      if (stalenessLimitMs == 0L) {
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql("SELECT * FROM v").collect()
          },
          condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS",
          parameters = Map("schemaDiff" -> "(?s).*", "legacyFlagMessage" -> ""),
          matchPVals = true)
      } else {
        // In production (HDFS/S3), the background listing takes real time,
        // so the foreground always returns the stale snapshot before the async
        // task completes. On local disk the listing is near instant and the
        // async task can win the race. Block it to simulate production timing.
        withBlockedAsyncUpdate("t") {
          checkAnswer(sql("SELECT * FROM v"), Row(1, 100))
        }
      }
    }
  }

  test("[1] scenario 6 external: temp view after external DROP/ADD column " +
      "same name different type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      spark.table("t").filter("salary < 999").createOrReplaceTempView("v")
      checkAnswer(sql("SELECT * FROM v"), Row(1, 100))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t"))
      val currentMetadata = deltaLog.snapshot.metadata
      val oldMaxId = currentMetadata.columnMappingMaxId
      val newSalaryId = oldMaxId + 1
      val idField = currentMetadata.schema("id")
      val newSalaryField = buildColumnMappingField(
        name = "salary", dataType = StringType, nullable = true, columnId = newSalaryId)
      val newSchema = StructType(Seq(idField, newSalaryField))
      val newMetadata = currentMetadata.copy(
        schemaString = newSchema.json,
        configuration = currentMetadata.configuration +
          (DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> newSalaryId.toString))

      writeExternalMetadataOnlyCommit("t", newMetadata)

      if (stalenessLimitMs == 0L) {
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql("SELECT * FROM v").collect()
          },
          condition = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS",
          parameters = Map("schemaDiff" -> "(?s).*", "legacyFlagMessage" -> ""),
          matchPVals = true)
      } else {
        // In production (HDFS/S3), the background listing takes real time,
        // so the foreground always returns the stale snapshot before the async
        // task completes. On local disk the listing is near instant and the
        // async task can win the race. Block it to simulate production timing.
        withBlockedAsyncUpdate("t") {
          checkAnswer(sql("SELECT * FROM v"), Row(1, 100))
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Section [2]: Repeated table access with external changes
  // ---------------------------------------------------------------------------

  test("[2] scenario 1: repeated access picks up new data") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[2] scenario 2: repeated access reflects schema changes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test("[2] scenario 3: repeated access after drop and recreate") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      checkAnswer(sql("SELECT * FROM t"), Seq.empty)
    }
  }

  // ---------------------------------------------------------------------------
  // Section [2] external: Repeated table access with external modifications
  // These test the "Connector w/ cache" behavior from the design doc.
  // With high stalenessLimit, external changes are invisible because
  // deltaLog.update(stalenessAcceptable=true) returns the cached snapshot.
  // ---------------------------------------------------------------------------

  test("[2] scenario 1 external: repeated access picks up external data") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      writeExternalCommit("t", Seq((2, 200)).toDF("id", "salary"))

      if (stalenessLimitMs == 0L) {
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, 100), Row(2, 200)))
      } else {
        checkAnswer(sql("SELECT * FROM t"), Row(1, 100))
      }
    }
  }

  test("[2] scenario 2 external: repeated access reflects external schema change") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t"))
      val currentMetadata = deltaLog.snapshot.metadata
      val newSchema = currentMetadata.schema
        .add("new_column", IntegerType, nullable = true)
      val newMetadata = currentMetadata.copy(schemaString = newSchema.json)

      writeExternalCommit(
        "t",
        Seq((2, 200, -1)).toDF("id", "salary", "new_column"),
        newMetadata = Some(newMetadata))

      if (stalenessLimitMs == 0L) {
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      } else {
        checkAnswer(sql("SELECT * FROM t"), Row(1, 100))
      }
    }
  }

  test("[2] scenario 3 external: repeated access after external DROP and recreate") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      writeExternalDropAndRecreateCommit("t", columnMapping = false)

      if (stalenessLimitMs == 0L) {
        checkAnswer(sql("SELECT * FROM t"), Seq.empty)
      } else {
        checkAnswer(sql("SELECT * FROM t"), Row(1, 100))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Section [3]: Incrementally constructed queries
  // ---------------------------------------------------------------------------

  test("[3] scenario 1: join after write uses consistent version") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("INSERT INTO t VALUES (2, 200)")

      val df2 = spark.table("t")

      // PrepareDeltaScan ensures both scans use the same (latest) snapshot
      val joined = df1.join(df2, df1("id") === df2("id"))
      checkAnswer(
        joined.select(df1("id"), df1("salary"), df2("id"), df2("salary")).orderBy(df1("id")),
        Seq(Row(1, 100, 1, 100), Row(2, 200, 2, 200)))
    }
  }

  test("[3] scenario 2: join after ADD COLUMN uses latest data with pinned schema") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      val df2 = spark.table("t")

      // df1 was analyzed with (id, salary), df2 with (id, salary, new_column)
      // Both use the latest version. df1 pins its original schema.
      checkAnswer(
        df1.orderBy("id"),
        Seq(Row(1, 100), Row(2, 200)))

      checkAnswer(
        df2.orderBy("id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      // The join should work since df1 projects only (id, salary)
      val joined = df1.join(df2, df1("id") === df2("id"))
      checkAnswer(
        joined.select(
          df1("id"), df1("salary"),
          df2("id"), df2("salary"), df2("new_column")).orderBy(df1("id")),
        Seq(Row(1, 100, 1, 100, null), Row(2, 200, 2, 200, -1)))
    }
  }

  test("[3] scenario 3: join after DROP COLUMN throws (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")

      val df2 = spark.table("t")

      val e = intercept[DeltaAnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[3] scenario 4: join after DROP and recreate table (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      val df2 = spark.table("t")

      val e = intercept[DeltaAnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[3] scenario 4: join after DROP and recreate table (no column mapping)") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      val df2 = spark.table("t")

      // Without column mapping, no column ID check. New table is empty.
      val joined = df1.join(df2, df1("id") === df2("id"))
      checkAnswer(joined, Seq.empty)
    }
  }

  test("[3] scenario 5: join after DROP/ADD column same name same type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      val df2 = spark.table("t")

      val e = intercept[DeltaAnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  test("[3] scenario 6: join after DROP/ADD column same name different type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df1 = spark.table("t")

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      val df2 = spark.table("t")

      val e = intercept[DeltaAnalysisException] {
        df1.join(df2, df1("id") === df2("id")).collect()
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")
    }
  }

  // ---------------------------------------------------------------------------
  // Section [4]: Version pinning and refresh in Dataset
  // ---------------------------------------------------------------------------

  test("[4] scenario 1.1: df.show picks up new data after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      // Fresh SQL always picks up new data
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[4] scenario 1.2: df.collect on same DataFrame after write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      // First collect triggers QueryExecution
      checkAnswer(df, Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      // collect() on the same df reuses QueryExecution,
      // but TahoeFileIndex.getSnapshot() still calls deltaLog.update()
      // during physical execution. For data-only changes, new data is visible.
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("[4] scenario 1.2b: count then collect inconsistency on same DataFrame") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      // First collect caches QueryExecution
      checkAnswer(df, Row(1, 100))

      writerSql("INSERT INTO t VALUES (2, 200)")

      // count() creates a new QueryExecution, so it sees the new data
      assert(df.count() == 2)

      // collect() on the same df reuses the cached QueryExecution.
      // This is the documented inconsistency from the design doc:
      // count() returns 2 but collect() returns only 1 row because
      // Dataset remembers and reuses QueryExecution for collect but not for
      // show, count, and other actions.
      checkAnswer(df, Row(1, 100))
    }
  }

  test("[4] scenario 2: df.show and collect after ADD COLUMN keeps original schema") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t ADD COLUMN new_column INT")
      writerSql("INSERT INTO t VALUES (2, 200, -1)")

      // df pins original schema (id, salary) but picks up latest version data
      checkAnswer(
        df.orderBy("id"),
        Seq(Row(1, 100), Row(2, 200)))

      // Fresh SQL picks up new schema
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }

  test("[4] scenario 3: df after DROP COLUMN (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")

      // Fresh SQL re-analyzes with the new schema and succeeds.
      // The table now only has column "id".
      checkAnswer(sql("SELECT * FROM t"), Row(1))

      // collect() on the same DataFrame reuses the cached QueryExecution,
      // so it still returns old data with the original (id, salary) schema.
      // Dataset remembers and reuses QueryExecution for collect.
      checkAnswer(df, Row(1, 100))
    }
  }

  test("[4] scenario 4.1: df.show after DROP and recreate table (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta " +
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

      // Fresh SQL re-analyzes and sees the new empty table
      checkAnswer(sql("SELECT * FROM t"), Seq.empty)

      // collect() on the same df references the old table's data files which no longer exist.
      // This results in a runtime error (file not found), not a schema change error.
      intercept[Exception] {
        df.collect()
      }
    }
  }

  test("[4] scenario 5.1: df.show after DROP/ADD column same name same type (column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary INT")

      // Fresh SQL re-analyzes with the new schema (new column IDs).
      // The old salary data is gone since it's a different physical column.
      checkAnswer(sql("SELECT * FROM t"), Row(1, null))

      // collect() on the same DataFrame reuses the cached QueryExecution.
      // The old QueryExecution still references the original physical column,
      // so it returns old data with the original schema.
      checkAnswer(df, Row(1, 100))
    }
  }

  test("[4] scenario 6.1: df.show after DROP/ADD column same name different type " +
      "(column mapping)") {
    withTable("t") {
      createColumnMappingTable("t")
      insertInitialData("t")

      val df = spark.sql("SELECT * FROM t")
      checkAnswer(df, Row(1, 100))

      writerSql("ALTER TABLE t DROP COLUMN salary")
      writerSql("ALTER TABLE t ADD COLUMN salary STRING")

      // Fresh SQL re-analyzes with the new schema (salary is now STRING).
      checkAnswer(sql("SELECT * FROM t"), Row(1, null))

      // collect() on the same DataFrame reuses the cached QueryExecution.
      // The old QueryExecution still references the original physical column,
      // so it returns old data with the original schema.
      checkAnswer(df, Row(1, 100))
    }
  }

  // ---------------------------------------------------------------------------
  // Section [5]: CACHE TABLE impact on reads
  // ---------------------------------------------------------------------------

  test("[5] scenario 1: CACHE TABLE with external writes") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // Write via path to bypass catalog cache invalidation
      val path = getTablePath("t")
      Seq((2, 200)).toDF("id", "salary")
        .write.format("delta").mode("append").save(path)

      // Delta refreshes table versions via PrepareDeltaScan regardless of
      // stalenessLimit. The version change breaks the plan shape match in
      // CacheManager, so the cache entry is not reused and fresh data is returned.
      // This means CACHE TABLE does not truly pin data in Delta.
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      sql("UNCACHE TABLE t")
    }
  }

  test("[5] scenario 2: session write invalidates cache then external write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // Session write invalidates the cache
      sql("INSERT INTO t VALUES (2, 200)")

      // External write via path
      val path = getTablePath("t")
      Seq((3, 300)).toDF("id", "salary")
        .write.format("delta").mode("append").save(path)

      // After a session write invalidates the cache, Delta picks up all data
      // from the log regardless of stalenessLimit.
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))

      sql("UNCACHE TABLE t")
    }
  }

  test("[5] scenario 3: external schema change") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // External schema change via path-based metadata update
      val path = getTablePath("t")
      sql(s"ALTER TABLE delta.`$path` ADD COLUMN new_column INT")
      Seq((2, 200, -1)).toDF("id", "salary", "new_column")
        .write.format("delta").mode("append").save(path)

      // Schema change breaks the plan-shape match in CacheManager,
      // so the cache is effectively invalidated regardless of stalenessLimit.
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      sql("UNCACHE TABLE t")
    }
  }

  test("[5] scenario 4: session schema change with external write") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // Session schema change
      sql("ALTER TABLE t ADD COLUMN new_column INT")

      // External write via path
      val path = getTablePath("t")
      Seq((2, 200, -1)).toDF("id", "salary", "new_column")
        .write.format("delta").mode("append").save(path)

      // Schema change from the session invalidates the cache.
      // Delta picks up all data regardless of stalenessLimit.
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))

      sql("UNCACHE TABLE t")
    }
  }

  test("[5] scenario 5: external drop and recreate table") {
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")

      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      writerSql("DROP TABLE t")
      writerSql("CREATE TABLE t (id INT, salary INT) USING delta")

      // After drop and recreate, the table is empty
      checkAnswer(sql("SELECT * FROM t"), Seq.empty)

      sql("UNCACHE TABLE IF EXISTS t")
    }
  }


  test("[5] scenario 6b: CACHE TABLE pins data against external writes") {
    // Doc scenario 1 with true external write simulation.
    // Uses writeExternalCommit (Delta equivalent of SPARK-54022's
    // catalog.loadTable().truncateTable() pattern).
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")
      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // External write bypassing the session catalog
      writeExternalCommit("t", Seq((2, 200)).toDF("id", "salary"))

      // Doc says: (1,100) only -- cache pins data against external writes
      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      sql("UNCACHE TABLE IF EXISTS t")

      // After uncaching, fresh query calls deltaLog.update() which discovers
      // the external commit when stalenessLimit=0 (forces filesystem listing).
      // With high stalenessLimit, the stale snapshot is returned unchanged.
      if (stalenessLimitMs == 0L) {
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, 100), Row(2, 200)))
      } else {
        checkAnswer(sql("SELECT * FROM t"), Row(1, 100))
      }
    }
  }

  test("[5] scenario 6c: session write invalidates cache, external write not visible") {
    // Doc scenario 2: session INSERT invalidates cache, then external write.
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")
      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // Session write invalidates the cache (via SPARK-55631 refreshCache)
      sql("INSERT INTO t VALUES (2, 200)")

      // External write bypassing the session catalog
      writeExternalCommit("t", Seq((3, 300)).toDF("id", "salary"))

      // Doc says: (1,100),(2,200) -- session write visible, external not
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200)))

      sql("UNCACHE TABLE IF EXISTS t")

      // After uncaching, fresh query discovers all data including external write.
      // The session INSERT updated DeltaLog.currentSnapshot to version 2, and
      // UNCACHE TABLE's table resolution triggers a deltaLog.update() that
      // discovers the external commit at version 3 regardless of stalenessLimit.
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100), Row(2, 200), Row(3, 300)))
    }
  }

  test("[5] scenario 6d: external schema change with CACHE") {
    // Doc scenario 3: external writer adds a column and data.
    // Writes both a Metadata action (schema change) and AddFile via
    // writeExternalCommit, bypassing AlterTableExec.refreshCache (SPARK-55631).
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")
      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // Build new schema with added column
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t"))
      val currentMetadata = deltaLog.snapshot.metadata
      val newSchema = currentMetadata.schema
        .add("new_column", org.apache.spark.sql.types.IntegerType, nullable = true)
      val newMetadata = currentMetadata.copy(schemaString = newSchema.json)

      // External schema change + data write
      writeExternalCommit(
        "t",
        Seq((2, 200, -1)).toDF("id", "salary", "new_column"),
        newMetadata = Some(newMetadata))

      // With stalenessLimit=0: deltaLog.update() lists filesystem, discovers
      //   the schema change. New schema in analyzed plan doesn't match cached
      //   plan -> cache miss -> fresh data visible.
      //   Matches doc: schema changes break table state pinning.
      //
      // With stalenessLimit>0: stale snapshot returned, cache holds.
      if (stalenessLimitMs == 0L) {
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      } else {
        checkAnswer(sql("SELECT * FROM t"), Row(1, 100))
      }

      sql("UNCACHE TABLE IF EXISTS t")

      // After uncaching, fresh query calls deltaLog.update() which discovers
      // the external commit when stalenessLimit=0 (forces filesystem listing).
      // With high stalenessLimit, the stale snapshot is returned unchanged.
      if (stalenessLimitMs == 0L) {
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      } else {
        checkAnswer(sql("SELECT * FROM t"), Row(1, 100))
      }
    }
  }

  test("[5] scenario 6e: session schema change then external write") {
    // Doc scenario 4: session ALTER TABLE invalidates cache (SPARK-55631),
    // then external data write.
    withTable("t") {
      createSimpleTable("t")
      insertInitialData("t")
      sql("CACHE TABLE t")
      checkAnswer(sql("SELECT * FROM t"), Row(1, 100))

      // Session schema change invalidates cache via AlterTableExec.refreshCache
      sql("ALTER TABLE t ADD COLUMN new_column INT")

      // External data write
      writeExternalCommit("t", Seq((2, 200, -1)).toDF("id", "salary", "new_column"))

      // Session schema change breaks cache. Next query re-analyzes.
      // With stalenessLimit=0: listing discovers external write too.
      //   Matches doc: (1,100,null),(2,200,-1)
      // With stalenessLimit>0: external write not discovered.
      //   Session change visible: (1,100,null) only.
      if (stalenessLimitMs == 0L) {
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, 100, null), Row(2, 200, -1)))
      } else {
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, 100, null)))
      }

      sql("UNCACHE TABLE IF EXISTS t")

      // After uncaching, fresh query discovers all data including external write.
      // The session ALTER TABLE updated DeltaLog.currentSnapshot, and
      // UNCACHE TABLE's table resolution triggers a deltaLog.update() that
      // discovers the external commit regardless of stalenessLimit.
      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, 100, null), Row(2, 200, -1)))
    }
  }
}

// ---------------------------------------------------------------------------
// Concrete test suites parameterized by V2 mode, session type, staleness
// ---------------------------------------------------------------------------

// REMOVED: scenarios 7 and staleLog 1-5 tested DeltaLog.update() API staleness
// directly, which is NOT what the design doc's CACHE TABLE section describes.
// The doc's CACHE TABLE scenarios are properly tested by scenarios 6b-6e above,
// which use writeExternalCommit on named tables with CACHE TABLE SQL.

// Concrete test suites parameterized by V2 mode, session type, staleness
// ---------------------------------------------------------------------------

/** V2_ENABLE_MODE = NONE, same-session writes, stalenessLimit = 0. */
class DeltaTableRefreshAndPinningSuite
  extends DeltaTableRefreshAndPinningSuiteBase {
  override protected def v2EnableMode: String = "NONE"
}

/** V2_ENABLE_MODE = AUTO (default), same-session writes, stalenessLimit = 0. */
class DeltaTableRefreshAndPinningAutoModeSuite
  extends DeltaTableRefreshAndPinningSuiteBase {
  override protected def v2EnableMode: String = "AUTO"
}

/**
 * Writes go through spark.newSession(). Verifies that behavior is identical to
 * same-session writes because all sessions in a single JVM share the same DeltaLog
 * instance cache. The DeltaLog.currentSnapshot is updated in place by the writer,
 * so the reader always sees fresh data regardless of which session performed the write.
 */
class DeltaTableRefreshAndPinningExternalSessionSuite
  extends DeltaTableRefreshAndPinningSuiteBase {
  override protected def useExternalSession: Boolean = true
}

/**
 * AUTO mode with external session writes. Combines both parameterization axes to
 * verify the v2 kernel connector also behaves identically with newSession() writes.
 */
class DeltaTableRefreshAndPinningAutoModeExternalSessionSuite
  extends DeltaTableRefreshAndPinningSuiteBase {
  override protected def v2EnableMode: String = "AUTO"
  override protected def useExternalSession: Boolean = true
}

/**
 * Sets stalenessLimit to 1 hour. Verifies that behavior is identical to stalenessLimit=0
 * because in a single JVM, writes update DeltaLog.currentSnapshot as a side effect of
 * committing. By the time the reader calls deltaLog.update(stalenessAcceptable = true),
 * the snapshot is already at the latest version, so the staleness check
 * (isCurrentlyStale returns false, doAsync = true, returns cached snapshot) returns
 * a snapshot that already includes the write. To observe different behavior, the write
 * must come from a separate JVM that commits directly to storage.
 */
class DeltaTableRefreshAndPinningStaleSuite
  extends DeltaTableRefreshAndPinningSuiteBase {
  override protected def stalenessLimitMs: Long = 3600000L
}

/**
 * Combines external session + high staleness limit. Both parameters have no observable
 * effect in a single JVM (see class-level scaladoc), so this suite verifies that the
 * combination also produces identical results.
 */
class DeltaTableRefreshAndPinningStaleExternalSessionSuite
  extends DeltaTableRefreshAndPinningSuiteBase {
  override protected def useExternalSession: Boolean = true
  override protected def stalenessLimitMs: Long = 3600000L
}
