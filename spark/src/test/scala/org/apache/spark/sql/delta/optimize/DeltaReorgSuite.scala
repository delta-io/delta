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

package org.apache.spark.sql.delta.optimize

import org.apache.spark.sql.delta.{DeletionVectorsTestUtils, DeltaColumnMapping, DeltaLog, DeltaUnsupportedOperationException}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.DeltaPurgeOperation
import org.apache.spark.sql.delta.commands.VacuumCommand.generateCandidateFileMap
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.util.DeltaFileOperations
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.logging.log4j.Level
import org.apache.parquet.hadoop.Footer

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.SerializableConfiguration

class DeltaReorgSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaSQLTestUtils
  with DeletionVectorsTestUtils {

  import testImplicits._

  def executePurge(table: String, condition: Option[String] = None): Unit = {
    condition match {
      case Some(cond) => sql(s"REORG TABLE delta.`$table` WHERE $cond APPLY (PURGE)")
      case None => sql(s"REORG TABLE delta.`$table` APPLY (PURGE)")
    }
  }

  test("Purge DVs will combine small files") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5).toDF
    withTempDeltaTable(targetDf) { (_, log) =>
      val path = log.dataPath.toString

      sql(s"DELETE FROM delta.`$path` WHERE id IN (0, 99)")
      assert(log.update().allFiles.filter(_.deletionVector != null).count() === 2)
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> "1073741824") { // 1gb
        executePurge(path)
      }
      val (addFiles, _) = getFileActionsInLastVersion(log)
      assert(addFiles.size === 1, "files should be combined")
      assert(addFiles.forall(_.deletionVector === null))
      checkAnswer(
        sql(s"SELECT * FROM delta.`$path`"),
        (1 to 98).toDF())

      // Verify commit history and operation metrics
      checkOpHistory(
        tablePath = path,
        expOpParams = Map("applyPurge" -> "true", "predicate" -> "[]"),
        numFilesRemoved = 2,
        numFilesAdded = 1)
    }
  }

  test("Purge DVs") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5).toDF()
    withTempDeltaTable(targetDf) { (_, log) =>
      val path = log.dataPath.toString

      sql(s"DELETE FROM delta.`$path` WHERE id IN (0, 99)")
      assert(log.update().allFiles.filter(_.deletionVector != null).count() === 2)

      // First purge
      executePurge(path)
      val (addFiles, _) = getFileActionsInLastVersion(log)
      assert(addFiles.size === 1) // two files are combined
      assert(addFiles.forall(_.deletionVector === null))
      checkAnswer(
        sql(s"SELECT * FROM delta.`$path`"),
        (1 to 98).toDF())

      // Verify commit history and operation metrics
      checkOpHistory(
        tablePath = path,
        expOpParams = Map("applyPurge" -> "true", "predicate" -> "[]"),
        numFilesRemoved = 2,
        numFilesAdded = 1)

      // Second purge is a noop
      val versionBefore = log.update().version
      executePurge(path)
      val versionAfter = log.update().version
      assert(versionBefore === versionAfter)
    }
  }

  test("Purge a non-DV table is a noop") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5).toDF()
    withTempDeltaTable(targetDf, enableDVs = false) { (_, log) =>
      val versionBefore = log.update().version
      executePurge(log.dataPath.toString)
      val versionAfter = log.update().version
      assert(versionBefore === versionAfter)
    }
  }

  test("Purge some partitions of a table with DV") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 1)
      .withColumn("part", col("id") % 4)
      .toDF()
    withTempDeltaTable(targetDf, partitionBy = Seq("part")) { (_, log) =>
      val path = log.dataPath
      // Delete one row from each partition
      sql(s"DELETE FROM delta.`$path` WHERE id IN (48, 49, 50, 51)")
      val (addFiles1, _) = getFileActionsInLastVersion(log)
      assert(addFiles1.size === 4)
      assert(addFiles1.forall(_.deletionVector !== null))
      // PURGE two partitions
      sql(s"REORG TABLE delta.`$path` WHERE part IN (0, 2) APPLY (PURGE)")
      val (addFiles2, _) = getFileActionsInLastVersion(log)
      assert(addFiles2.size === 2)
      assert(addFiles2.forall(_.deletionVector === null))

      // Verify commit history and operation metrics
      checkOpHistory(
        tablePath = path.toString,
        expOpParams = Map("applyPurge" -> "true", "predicate" -> "[\"'part IN (0,2)\"]"),
        numFilesRemoved = 2,
        numFilesAdded = 2)
    }
  }

  private def checkOpHistory(
      tablePath: String,
      expOpParams: Map[String, String],
      numFilesRemoved: Long,
      numFilesAdded: Long): Unit = {
    val (opName, opParams, opMetrics) = DeltaTable.forPath(tablePath)
      .history(1)
      .select("operation", "operationParameters", "operationMetrics")
      .as[(String, Map[String, String], Map[String, String])]
      .head()
    assert(opName === "REORG")
    assert(opParams === expOpParams)
    assert(opMetrics("numAddedFiles").toLong === numFilesAdded)
    assert(opMetrics("numRemovedFiles").toLong === numFilesRemoved)
    // Because each deleted file has a DV associated it which gets rewritten as part of PURGE
    assert(opMetrics("numDeletionVectorsRemoved").toLong === numFilesRemoved)
  }

  /**
   * Get all parquet footers for the input `files`, used only for testing.
   *
   * @param files the sequence of `AddFile` used to read the parquet footers
   *              by the data file path in each `AddFile`.
   * @param log the delta log used to get the configuration and data path.
   * @return the sequence of the corresponding parquet footers, corresponds to
   *         the sequence of `AddFile`.
   */
  private def getParquetFooters(
      files: Seq[AddFile],
      log: DeltaLog,
      parallelism: Int =
        DeltaFileOperations.DEFAULT_PARQUET_FOOTER_READ_PARALLELISM): Seq[Footer] = {
    val serializedConf = new SerializableConfiguration(log.newDeltaHadoopConf())
    val dataPath = new Path(log.dataPath.toString)
    val nameToAddFileMap = generateCandidateFileMap(dataPath, files)
    val fileStatuses = nameToAddFileMap.map { case (absPath, addFile) =>
      new FileStatus(
        /* length */ addFile.size,
        /* isDir */ false,
        /* blockReplication */ 0,
        /* blockSize */ 1,
        /* modificationTime */ addFile.modificationTime,
        new Path(absPath)
      )
    }
    DeltaFileOperations.readParquetFootersInParallel(
      serializedConf.value,
      fileStatuses.toList,
      ignoreCorruptFiles = false,
      parallelism = parallelism
    )
  }

  test("Purge dropped columns of a table without DV") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5)
      .withColumn("id_dropped", col("id") % 4)
      .toDF()
    withTempDeltaTable(targetDf) { (_, log) =>
      val path = log.dataPath.toString

      val (addFiles1, _) = getFileActionsInLastVersion(log)
      assert(addFiles1.size === 5)
      val footers1 = getParquetFooters(addFiles1, log)
      footers1.foreach { footer =>
        val fields = footer.getParquetMetadata.getFileMetaData.getSchema.getFields
        assert(fields.size == 2)
        assert(fields.toArray.map { _.toString }.contains("optional int64 id_dropped"))
      }

      // enable column-mapping first
      sql(
        s"""
           | ALTER TABLE delta.`$path`
           | SET TBLPROPERTIES (
           |   'delta.columnMapping.mode' = 'name'
           | )
           |""".stripMargin
      )
      // drop the extra column by alter table and run REORG PURGE
      sql(
        s"""
           | ALTER TABLE delta.`$path`
           | DROP COLUMN id_dropped
           |""".stripMargin
      )
      executePurge(path)

      val (addFiles2, _) = getFileActionsInLastVersion(log)
      assert(addFiles2.size === 1)
      val footers2 = getParquetFooters(addFiles2, log)
      footers2.foreach { footer =>
        val fields = footer.getParquetMetadata.getFileMetaData.getSchema.getFields
        assert(fields.size == 1)
        assert(!fields.toArray.map { _.toString }.contains("optional int64 id_dropped"))
      }
    }
  }

  test("REORG PURGE footer scan honors batching and parallelism") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5)
      .withColumn("id_dropped", col("id") % 4)
      .toDF()
    withTempDeltaTable(targetDf) { (_, log) =>
      val path = log.dataPath.toString
      sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
      sql(s"ALTER TABLE delta.`$path` DROP COLUMN id_dropped")
      // Files appended after the drop never had `id_dropped`, so only the original files qualify.
      spark.range(100, 160, 1, numPartitions = 3).toDF()
        .write.format("delta").mode("append").save(path)

      val snapshot = log.update()
      val files = snapshot.allFiles.collect().toSeq
      val purge = new DeltaPurgeOperation()

      def select(batchSize: Int, parallelism: Int): Set[String] = {
        withSQLConf(
            DeltaSQLConf.DELTA_REORG_FOOTER_SCAN_BATCH_SIZE.key -> batchSize.toString,
            DeltaSQLConf.DELTA_REORG_FOOTER_SCAN_PARALLELISM.key -> parallelism.toString) {
          purge.filterFilesToReorg(spark, snapshot, files).map(_.path).toSet
        }
      }

      val expected = select(files.size + 5, parallelism = 1)
      assert(expected.nonEmpty && expected.size < files.size,
        s"PURGE should select a strict, non-empty subset of ${files.size} files")
      Seq(0, 1, 2, files.size - 1).foreach { batchSize =>
        Seq(1, 3).foreach { parallelism =>
          assert(select(batchSize, parallelism) === expected,
            s"batchSize=$batchSize parallelism=$parallelism changed selection")
        }
      }

      val footerLogs = new LogAppender("REORG footer scan parallelism", maxEvents = 1000)
      footerLogs.setThreshold(Level.INFO)
      val footerLogger = DeltaFileOperations.getClass.getName.stripSuffix("$")
      withLogAppender(
          footerLogs,
          loggerNames = Seq(footerLogger),
          level = Some(Level.INFO)) {
        select(batchSize = 1, parallelism = 3)
      }
      val messages = footerLogs.loggingEvents.map(_.getMessage.getFormattedMessage)
      val details = messages.mkString(" | ")
      assert(messages.exists(_.contains("with parallelism 3")),
        s"footer reader did not observe parallelism=3 in $details")
    }
  }

  test("REORG footer scan parallelism matches ForkJoinPool bounds") {
    withSQLConf(
        DeltaSQLConf.DELTA_REORG_FOOTER_SCAN_PARALLELISM.key -> Short.MaxValue.toString) {
      assert(spark.sessionState.conf
        .getConf(DeltaSQLConf.DELTA_REORG_FOOTER_SCAN_PARALLELISM) === Short.MaxValue.toInt)
    }

    val error = intercept[IllegalArgumentException] {
      spark.conf.set(
        DeltaSQLConf.DELTA_REORG_FOOTER_SCAN_PARALLELISM.key,
        (Short.MaxValue + 1).toString)
    }
    assert(error.getMessage.contains("must be between 1 and 32767"))
  }

  test("REORG PURGE rejects candidate path collisions across batches") {
    withTempDeltaTable(spark.range(0, 1, 1, numPartitions = 1).toDF()) { (_, log) =>
      val snapshot = log.update()
      val addFile = snapshot.allFiles.as[AddFile].head()
      val duplicateFiles = Seq(addFile, addFile)

      def assertCollision(block: => Unit): Unit = {
        val error = intercept[Exception](block)
        val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).toSeq
        val classes = causes.map(_.getClass).mkString(" -> ")
        assert(causes.exists(_.isInstanceOf[AssertionError]),
          s"expected AssertionError, found $classes")
      }

      withSQLConf("spark.sql.leafNodeDefaultParallelism" -> "1") {
        Seq(0, 1).foreach { batchSize =>
          withSQLConf(
              DeltaSQLConf.DELTA_REORG_FOOTER_SCAN_BATCH_SIZE.key -> batchSize.toString) {
            assertCollision {
              new DeltaPurgeOperation().filterFilesToReorg(spark, snapshot, duplicateFiles)
              ()
            }
          }
        }
      }
    }
  }

  test("REORG footer reads are stable across batch sizes and parallelism") {
    withTempDeltaTable(spark.range(0, 100, 1, numPartitions = 7).toDF()) { (_, log) =>
      val files = log.update().allFiles.collect().toSeq
      assert(files.size === 7)
      Seq(1, 2, files.size, files.size + 5).foreach { batchSize =>
        Seq(1, 4, files.size + 2).foreach { parallelism =>
          files.grouped(batchSize).foreach { batch =>
            val footers = getParquetFooters(batch, log, parallelism)
            assert(footers.map(_.getFile.getName).sorted ===
              batch.map(file => new Path(file.path).getName).sorted,
              s"footers mismatch at batchSize=$batchSize, parallelism=$parallelism")
          }
        }
      }
    }
  }

  test("Columns being renamed should not be purged") {
    val targetDf = spark.range(0, 100, 1, numPartitions = 5)
      .withColumn("id_before_rename", col("id") % 4)
      .withColumn("id_dropped", col("id") % 5)
      .toDF()
    withTempDeltaTable(targetDf) { (_, log) =>
      val path = log.dataPath.toString

      val (addFiles1, _) = getFileActionsInLastVersion(log)
      assert(addFiles1.size === 5)
      val footers1 = getParquetFooters(addFiles1, log)
      footers1.foreach { footer =>
        val fields = footer.getParquetMetadata.getFileMetaData.getSchema.getFields
        assert(fields.size == 3)
        assert(fields.toArray.map { _.toString }.contains("optional int64 id_dropped"))
        assert(fields.toArray.map { _.toString }.contains("optional int64 id_before_rename"))
      }

      // enable column-mapping first
      sql(
        s"""
           | ALTER TABLE delta.`$path`
           | SET TBLPROPERTIES (
           |   'delta.columnMapping.mode' = 'name'
           | )
           |""".stripMargin
      )
      // drop `id_dropped` and rename `id_before_rename` via alter table and run REORG PURGE,
      // this should remove `id_dropped` but keep `id_after_rename` in the parquet files.
      sql(
        s"""
           | ALTER TABLE delta.`$path`
           | DROP COLUMN id_dropped
           |""".stripMargin
      )
      sql(
        s"""
           | ALTER TABLE delta.`$path`
           | RENAME COLUMN id_before_rename TO id_after_rename
           |""".stripMargin
      )
      executePurge(path)

      val tableSchema = log.update().schema
      val tablePhysicalSchema = DeltaColumnMapping.renameColumns(tableSchema)
      val beforeRenameColStr = "StructField(id_before_rename,LongType,true)"
      val afterRenameColStr = "StructField(id_after_rename,LongType,true)"
      assert(tableSchema.fields.length == 2 &&
        tableSchema.map { _.toString }.contains(afterRenameColStr))
      assert(tablePhysicalSchema.fields.length == 2 &&
        tablePhysicalSchema.map { _.toString }.contains(beforeRenameColStr))

      val (addFiles2, _) = getFileActionsInLastVersion(log)
      assert(addFiles2.size === 1)
      val footers2 = getParquetFooters(addFiles2, log)
      footers2.foreach { footer =>
        val fields = footer.getParquetMetadata.getFileMetaData.getSchema.getFields
        assert(fields.size == 2)
        assert(!fields.toArray.map { _.toString }.contains("optional int64 id_dropped = 3"))
        // do note that the actual name for the column will not be
        // changed in parquet file level
        assert(fields.toArray.map { _.toString }.contains("optional int64 id_before_rename = 2"))
      }
    }
  }

  test("reorg on a catalog managed table should fail") {
    withCatalogManagedTable() { tableName =>
      spark.sql(s"INSERT INTO $tableName VALUES (1)")
      val snapshotBefore = getSnapshot(tableName)

      checkError(
        intercept[DeltaUnsupportedOperationException] {
          spark.sql(s"REORG TABLE $tableName APPLY (PURGE)")
        },
        "DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION",
        parameters = Map("operation" -> "DATA_REORGANIZATION")
      )

      val snapshotAfter = getSnapshot(tableName)
      assert(snapshotAfter.version === snapshotBefore.version)
      assert(snapshotAfter.allFiles.collect().toSet === snapshotBefore.allFiles.collect().toSet)
    }
  }
}
