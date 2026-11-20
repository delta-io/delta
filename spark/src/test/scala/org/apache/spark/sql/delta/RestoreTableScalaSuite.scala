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

import java.sql.Date

import scala.concurrent.duration._

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaExcludedTestMixin
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.util.Utils

/** Restore tests using the Scala APIs. */
class RestoreTableScalaSuite extends RestoreTableSuiteBase {

  override def restoreTableToVersion(
      tblId: String,
      version: Int,
      isTable: Boolean,
      expectNoOp: Boolean = false): DataFrame = {
    val deltaTable = if (isTable) {
      io.delta.tables.DeltaTable.forName(spark, tblId)
    } else {
      io.delta.tables.DeltaTable.forPath(spark, tblId)
    }

    deltaTable.restoreToVersion(version)
  }

  override def restoreTableToTimestamp(
      tblId: String,
      timestamp: String,
      isTable: Boolean,
      expectNoOp: Boolean = false): DataFrame = {
    val deltaTable = if (isTable) {
      io.delta.tables.DeltaTable.forName(spark, tblId)
    } else {
      io.delta.tables.DeltaTable.forPath(spark, tblId)
    }

    deltaTable.restoreToTimestamp(timestamp)
  }
}

class RestoreTableScalaWithCatalogOwnedBatch1Suite extends RestoreTableScalaSuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(1)
}

class RestoreTableScalaWithCatalogOwnedBatch2Suite extends RestoreTableScalaSuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(2)
}

class RestoreTableScalaWithCatalogOwnedBatch100Suite extends RestoreTableScalaSuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(100)
}

class RestoreTableScalaDeletionVectorSuite
    extends RestoreTableScalaSuite
    with DeletionVectorsTestUtils
    with DeltaExcludedTestMixin  {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectors(spark.conf)
  }
  override def excluded: Seq[String] = super.excluded ++
    Seq(
      // These tests perform a delete to produce a file to vacuum, but with persistent DVs enabled,
      // we actually just add a DV to the file instead, so there's no unreferenced file for vacuum.
      "restore after vacuum",
      "restore after vacuum - cloned table",
      // These rely on the new-table protocol version to be lower than the latest,
      // but this isn't true for DVs.
      "restore downgrade protocol (allowed=true)",
      "restore downgrade protocol (allowed=false)",
      "restore downgrade protocol with table features (allowed=true)",
      "restore downgrade protocol with table features (allowed=false)",
      "cdf + RESTORE with write amplification reduction",
      "RESTORE doesn't account for session defaults"
    )

  case class RestoreAndCheckArgs(versionToRestore: Int, expectedResult: DataFrame)
  type RestoreAndCheckFunction = RestoreAndCheckArgs => Unit

  /**
   * Tests `testFun` once by restoring to version and once to timestamp.
   *
   * `testFun` is expected to perform setup before executing the `RestoreAndTestFunction` and
   * cleanup afterwards.
   */
  protected def testRestoreByTimestampAndVersion
      (testName: String)
      (testFun: (String, RestoreAndCheckFunction) => Unit): Unit = {
    for (restoreToVersion <- BOOLEAN_DOMAIN) {
      val restoringTo = if (restoreToVersion) "version" else "timestamp"
      test(testName + s" - restoring to $restoringTo") {
        withTempDir{ dir =>
          val path = dir.toString
          val restoreAndCheck: RestoreAndCheckFunction = (args: RestoreAndCheckArgs) => {
            val deltaLog = DeltaLog.forTable(spark, path)
            if (restoreToVersion) {
              restoreTableToVersion(path, args.versionToRestore, isTable = false)
            } else {
              // Set a custom timestamp for the commit
              val desiredDateS = new Date(System.currentTimeMillis() - 3.days.toMillis).toString
              setTimestampToCommitFileAtVersion(
                deltaLog,
                version = args.versionToRestore,
                date = desiredDateS)
              // Set all previous versions to something lower, so we don't error out.
              for (version <- 0 until args.versionToRestore) {
                val previousDateS = new Date(System.currentTimeMillis() - 5.days.toMillis).toString
                setTimestampToCommitFileAtVersion(
                  deltaLog,
                  version = version,
                  date = previousDateS)
              }

              restoreTableToTimestamp(path, desiredDateS, isTable = false)
            }
            checkAnswer(spark.read.format("delta").load(path), args.expectedResult)
          }
          testFun(path, restoreAndCheck)
        }
      }
    }
  }

  testRestoreByTimestampAndVersion(
    "Restoring table with persistent DVs to version without DVs") { (path, restoreAndCheck) =>
    val deltaLog = DeltaLog.forTable(spark, path)
    val df1 = Seq(1, 2, 3, 4, 5).toDF("id")
    val values2 = Seq(6, 7, 8, 9, 10)
    val df2 = values2.toDF("id")

    // Write all values into version 0.
    df1.union(df2).coalesce(1).write.format("delta").save(path) // version 0
    checkAnswer(spark.read.format("delta").load(path), expectedAnswer = df1.union(df2))
    val snapshotV0 = deltaLog.update()
    assert(snapshotV0.version === 0)

    // Delete values 2 so that version 1 is `df1`.
    spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (${values2.mkString(", ")})") // version 1
    assert(getFilesWithDeletionVectors(deltaLog).size > 0)
    checkAnswer(spark.read.format("delta").load(path), expectedAnswer = df1)
    val snapshotV1 = deltaLog.snapshot
    assert(snapshotV1.version === 1)

    restoreAndCheck(RestoreAndCheckArgs(versionToRestore = 0, expectedResult = df1.union(df2)))
    assert(getFilesWithDeletionVectors(deltaLog).size === 0)
  }

  testRestoreByTimestampAndVersion(
    "Restoring table with persistent DVs to version with DVs") { (path, restoreAndCheck) =>
    val deltaLog = DeltaLog.forTable(spark, path)
    val df1 = Seq(1, 2, 3, 4, 5).toDF("id")
    val values2 = Seq(6, 7)
    val df2 = values2.toDF("id")
    val values3 = Seq(8, 9, 10)
    val df3 = values3.toDF("id")

    // Write all values into version 0.
    df1.union(df2).union(df3).coalesce(1).write.format("delta").save(path) // version 0

    // Delete values 2 and 3 in reverse order, so that version 1 is `df1.union(df2)`.
    spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (${values3.mkString(", ")})") // version 1
    assert(getFilesWithDeletionVectors(deltaLog).size > 0)
    checkAnswer(spark.read.format("delta").load(path), expectedAnswer = df1.union(df2))
    spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (${values2.mkString(", ")})") // version 2
    assert(getFilesWithDeletionVectors(deltaLog).size > 0)

    restoreAndCheck(RestoreAndCheckArgs(versionToRestore = 1, expectedResult = df1.union(df2)))
    assert(getFilesWithDeletionVectors(deltaLog).size > 0)
  }

  testRestoreByTimestampAndVersion("Restoring table with persistent DVs to version " +
      "without persistent DVs enabled") { (path, restoreAndCheck) =>
    withSQLConf(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "false",
        // Disable the log clean up. Tests sets the timestamp on commit files to long back
        // in time that triggers the commit file clean up as part of the [[MetadataCleanup]]
        DeltaConfigs.ENABLE_EXPIRED_LOG_CLEANUP.defaultTablePropertyKey -> "false") {
      val deltaLog = DeltaLog.forTable(spark, path)
      val df1 = Seq(1, 2, 3, 4, 5).toDF("id")
      val values2 = Seq(6, 7, 8, 9, 10)
      val df2 = values2.toDF("id")

      // Write all values into version 0.
      df1.union(df2).coalesce(1).write.format("delta").save(path) // version 0
      checkAnswer(spark.read.format("delta").load(path), expectedAnswer = df1.union(df2))
      val snapshotV0 = deltaLog.update()
      assert(snapshotV0.version === 0)
      assert(!DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.fromMetaData(snapshotV0.metadata))

      // Upgrade to us DVs
      spark.sql(s"ALTER TABLE delta.`$path` SET TBLPROPERTIES " +
        s"(${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key} = true)")
      val snapshotV1 = deltaLog.update()
      assert(snapshotV1.version === 1)
      assert(DeletionVectorUtils.deletionVectorsReadable(snapshotV1))
      assert(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.fromMetaData(snapshotV1.metadata))

      // Delete values 2 so that version 1 is `df1`.
      spark.sql(s"DELETE FROM delta.`$path` WHERE id IN (${values2.mkString(", ")})") // version 2
      assert(getFilesWithDeletionVectors(deltaLog).size > 0)
      checkAnswer(spark.read.format("delta").load(path), expectedAnswer = df1)
      val snapshotV2 = deltaLog.update()
      assert(snapshotV2.version === 2)

      // Restore to before the version upgrade. Protocol version should be retained (to make the
      // history readable), but DV creation should be disabled again.
      restoreAndCheck(RestoreAndCheckArgs(versionToRestore = 0, expectedResult = df1.union(df2)))
      val snapshotV3 = deltaLog.update()
      assert(getFilesWithDeletionVectors(deltaLog).size === 0)
      assert(DeletionVectorUtils.deletionVectorsReadable(snapshotV3))
      assert(!DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.fromMetaData(snapshotV3.metadata))
      // Check that we can still read versions that did have DVs.
      checkAnswer(
        spark.read.format("delta").option("versionAsOf", "2").load(path),
        expectedAnswer = df1)
    }
  }
  test("CDF + DV + RESTORE") {
    withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      withTempDir { tempDir =>
        val df0 = Seq(0, 1).toDF("id") // version 0 = [0, 1]
        df0.write.format("delta").save(tempDir.getAbsolutePath)

        val df1 = Seq(2).toDF("id") // version 1: append to df0 = [0, 1, 2]
        df1.write.mode("append").format("delta").save(tempDir.getAbsolutePath)

        val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tempDir.getAbsolutePath)
        deltaTable.delete("id < 1") // version 2: delete (0) = [1, 2]

        deltaTable.updateExpr(
          "id > 1",
          Map("id" -> "4")
        ) // version 3: update 2 --> 4 = [1, 4]

        // version 4: restore to version 2 (delete 4, insert 2) = [1, 2]
        restoreTableToVersion(tempDir.getAbsolutePath, 2, false)
        checkAnswer(
          CDCReader.changesToBatchDF(DeltaLog.forTable(spark, tempDir), 4, 4, spark)
            .drop(CDCReader.CDC_COMMIT_TIMESTAMP),
          Row(4, "delete", 4) :: Row(2, "insert", 4) :: Nil
        )

        // version 5: restore to version 1 (insert 0) = [0, 1, 2]
        restoreTableToVersion(tempDir.getAbsolutePath, 1, false)
        checkAnswer(
          CDCReader.changesToBatchDF(DeltaLog.forTable(spark, tempDir), 5, 5, spark)
            .drop(CDCReader.CDC_COMMIT_TIMESTAMP),
          Row(0, "insert", 5) :: Nil
        )

        // version 6: restore to version 0 (delete 2) = [0, 1]
        restoreTableToVersion(tempDir.getAbsolutePath, 0, false)
        checkAnswer(
          CDCReader.changesToBatchDF(DeltaLog.forTable(spark, tempDir), 6, 6, spark)
            .drop(CDCReader.CDC_COMMIT_TIMESTAMP),
          Row(2, "delete", 6) :: Nil
        )
      }
    }
  }

  BOOLEAN_DOMAIN.foreach { dvAwareFileMetricsEnabled =>
    test(s"DV metrics: same-path file metrics enabled = $dvAwareFileMetricsEnabled") {
      withSQLConf(
          DeltaSQLConf.DELTA_COMMIT_INFO_DATA_CHANGE_WRITE_FOR_NON_AMT_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_DV_AWARE_FILE_METRICS_ENABLED.key ->
            dvAwareFileMetricsEnabled.toString,
          DeltaSQLConf.DELTA_FILE_SIZE_HISTOGRAM_ENABLED.key -> "true") {
        val tableId = "testTable"
        withTable(tableId) {
          spark.range(start = 0L, end = 12L, step = 1, numPartitions = 3).toDF("id")
            .write.format("delta").mode("overwrite").saveAsTable(tableId)
          val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableId))
          val deltaTable = io.delta.tables.DeltaTable.forName(spark, tableId)

          // Each file holds four consecutive IDs. Add a DV to the first two files.
          deltaTable.delete("id IN (3, 6)")
          val v1 = deltaLog.update().version.toInt
          val v1DVs = deltaLog.update().allFiles.collect()
            .filter(_.deletionVector != null).map(_.deletionVector)
          assert(v1DVs.size === 2)
          val v1DVsSizeInBytes = v1DVs.map(_.sizeInBytes).sum

          // Change the first two DVs and add one to the third file. All three files survive.
          deltaTable.delete("id IN (0, 4, 7, 8, 9)")
          val files = deltaLog.update().allFiles.collect()
          assert(files.length === 3)
          val v2DVs = files.filter(_.deletionVector != null).map(_.deletionVector)
          assert(v2DVs.size === 3)
          val v2DVsSizeInBytes = v2DVs.map(_.sizeInBytes).sum

          val expectedCount = if (dvAwareFileMetricsEnabled) 0L else 3L
          val expectedBytes = if (dvAwareFileMetricsEnabled) 0L else files.map(_.size).sum
          val usageLogs = Log4jUsageLogger.track {
            val result = deltaTable.restoreToVersion(v1)
            checkAnswer(result.select("num_removed_files", "num_restored_files",
              "removed_files_size", "restored_files_size"),
              Row(expectedCount, expectedCount, expectedBytes, expectedBytes))
          }
          checkAnswer(deltaTable.toDF.select("id"),
            (0L until 12L).filterNot(Set(3L, 6L)).map(Row(_)))

          // Check Delta history.
          val opMetrics = DeltaMetricsUtils.getLastOperationMetrics(tableId)
          DeltaMetricsUtils.checkOperationMetrics(
            expectedMetrics = Map(
              "numRestoredFiles" -> expectedCount,
              "restoredFilesSize" -> expectedBytes,
              "numRemovedFiles" -> expectedCount,
              "removedFilesSize" -> expectedBytes),
            operationMetrics = opMetrics)
          val commitStats = usageLogs
            .filter(_.tags.get("opType").contains("delta.commit.stats"))
            .map(record => JsonUtils.fromJson[CommitStats](record.blob))
            .last
          assert(commitStats.info.dataChange.contains(true))
          assert(commitStats.numDeletionVectorsAdded ===
            (if (dvAwareFileMetricsEnabled) Some(2L) else None))
          assert(commitStats.numDeletionVectorsRemoved ===
            (if (dvAwareFileMetricsEnabled) Some(3L) else None))
          assert(commitStats.numDeletionVectorsBytesAdded ===
            (if (dvAwareFileMetricsEnabled) Some(v1DVsSizeInBytes.toLong) else None))
          assert(commitStats.numDeletionVectorsBytesRemoved ===
            (if (dvAwareFileMetricsEnabled) Some(v2DVsSizeInBytes.toLong) else None))
          assert(commitStats.numAdd === expectedCount)
          assert(commitStats.numRemove === expectedCount)
          assert(commitStats.bytesNew === expectedBytes)
          assert(commitStats.bytesRemoved === (if (dvAwareFileMetricsEnabled) Some(0L) else None))
          assert(commitStats.addFilesHistogram.get.fileCounts.sum === expectedCount)
          assert(commitStats.removeFilesHistogram.get.fileCounts.sum === expectedCount)
          assert(commitStats.addFilesHistogram.get.totalBytes.sum === expectedBytes)
          assert(commitStats.removeFilesHistogram.get.totalBytes.sum === expectedBytes)
        }
      }
    }
  }

  test("DV metrics: unchanged, DV-updated, restored and removed files in one RESTORE") {
    withSQLConf(DeltaSQLConf.DELTA_DV_AWARE_FILE_METRICS_ENABLED.key -> "true") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        val log = DeltaLog.forTable(spark, path)
        val table = s"delta.`$path`"
        // One file per partition. Partition 0 stays unchanged, partition 1 keeps its file
        // with a different DV, and partitions 2 and 3 appear on only one side of RESTORE.
        spark.range(12).selectExpr("id", "CAST(id DIV 4 AS INT) AS part").coalesce(1)
          .write.format("delta").partitionBy("part").save(path)
        sql(s"DELETE FROM $table WHERE part = 1 AND id = 4")
        val restoreSnapshot = log.update()
        sql(s"DELETE FROM $table WHERE part = 1 AND id = 5")
        sql(s"DELETE FROM $table WHERE part = 2")
        sql(s"INSERT INTO $table VALUES (12, 3)")

        def filesByPartition(snapshot: Snapshot): Map[String, AddFile] = {
          val files = snapshot.allFiles.collect()
          assert(files.length === 3)
          files.map(f => f.partitionValues("part") -> f).toMap
        }
        val before = filesByPartition(restoreSnapshot)
        val current = filesByPartition(log.update())
        assert(before.keySet === Set("0", "1", "2"))
        assert(current.keySet === Set("0", "1", "3"))
        assert(before("0") === current("0"))
        assert(before("1").path === current("1").path)
        val restoredDV = before("1").deletionVector
        val removedDV = current("1").deletionVector
        assert(restoredDV != null && removedDV != null && restoredDV != removedDV)

        val restoredBytes = before("2").size
        val removedBytes = current("3").size
        val usageLogs = Log4jUsageLogger.track {
          val result = sql(s"RESTORE TABLE $table TO VERSION AS OF ${restoreSnapshot.version}")
          checkAnswer(result.select("num_restored_files", "restored_files_size",
            "num_removed_files", "removed_files_size"), Row(1L, restoredBytes, 1L, removedBytes))
        }
        val stats = usageLogs.filter(_.tags.get("opType").contains("delta.commit.stats"))
          .map(record => JsonUtils.fromJson[CommitStats](record.blob)).last
        assert(stats.numAdd === 1L && stats.numRemove === 1L)
        assert(stats.bytesNew === restoredBytes && stats.bytesRemoved.contains(removedBytes))
        assert(stats.numDeletionVectorsAdded.contains(1L))
        assert(stats.numDeletionVectorsRemoved.contains(1L))
        assert(stats.numDeletionVectorsBytesAdded.contains(restoredDV.sizeInBytes.toLong))
        assert(stats.numDeletionVectorsBytesRemoved.contains(removedDV.sizeInBytes.toLong))
        checkAnswer(sql(s"SELECT id FROM $table"), (0L until 12L).filterNot(_ == 4L).map(Row(_)))
      }
    }
  }
}
