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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.functions._

class DeltaAutoOptimizeSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeletionVectorsTestUtils {

  def writeDataToCheckAutoCompact(
      numFiles: Int,
      dataPath: String,
      partitioned: Boolean = false,
      mode: String = "overwrite"): Unit = {
    val df = spark
      .range(50000)
      .withColumn("colA", rand() * 10000000 cast "long")
      .withColumn("colB", rand() * 1000000000 cast "int")
      .withColumn("colC", rand() * 2 cast "int")
      .drop("id")
      .repartition(numFiles)
    if (partitioned) {
      df.write
        .partitionBy("colC")
        .mode(mode)
        .format("delta")
        .save(dataPath)
    } else {
      df.write
        .mode(mode)
        .format("delta")
        .save(dataPath)
    }
  }

  def checkTableVersionAndNumFiles(
      path: String,
      expectedVer: Long,
      expectedNumFiles: Long): Unit = {
    val dt = DeltaLog.forTable(spark, path)
    assert(dt.unsafeVolatileSnapshot.allFiles.count() == expectedNumFiles)
    assert(dt.unsafeVolatileSnapshot.version == expectedVer)
  }

  test("test enabling autoCompact") {
    val tableName = "autoCompactTestTable"
    val tableName2 = s"${tableName}2"
    withTable(tableName, tableName2) {
      withTempDir { dir =>
        val rootPath = dir.getCanonicalPath
        val path = new Path(rootPath, "table1").toString
        var expectedTableVersion = -1
        spark.conf.unset(DeltaSQLConf.AUTO_COMPACT_ENABLED.key)
        writeDataToCheckAutoCompact(100, path)
        // No autoCompact triggered - version should be 0.
        expectedTableVersion += 1
        checkTableVersionAndNumFiles(path, expectedTableVersion, 100)

        // Create table
        spark.sql(s"CREATE TABLE $tableName USING DELTA LOCATION '$path'")
        spark.sql(
          s"ALTER TABLE $tableName SET TBLPROPERTIES (delta.autoOptimize.autoCompact = true)")
        expectedTableVersion += 1 // version increased due to ALTER TABLE

        writeDataToCheckAutoCompact(100, path)
        expectedTableVersion += 2 // autoCompact should be triggered
        checkTableVersionAndNumFiles(path, expectedTableVersion, 1)

        withSQLConf(DeltaSQLConf.AUTO_COMPACT_ENABLED.key -> "false") {
          // Session config should be prior to table properties
          writeDataToCheckAutoCompact(100, path)
          expectedTableVersion += 1 // autoCompact should not be triggered
          checkTableVersionAndNumFiles(path, expectedTableVersion, 100)
        }

        spark.sql(
          s"ALTER TABLE $tableName SET TBLPROPERTIES (delta.autoOptimize.autoCompact = false)")
        expectedTableVersion += 1 // version increased due to SET TBLPROPERTIES

        withSQLConf(DeltaSQLConf.AUTO_COMPACT_ENABLED.key -> "true") {
          // Session config should be prior to table properties
          writeDataToCheckAutoCompact(100, path)
          expectedTableVersion += 2 // autoCompact should be triggered
          checkTableVersionAndNumFiles(path, expectedTableVersion, 1)
        }

        spark.conf.unset(DeltaSQLConf.AUTO_COMPACT_ENABLED.key)

        withSQLConf(
          "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact" -> "true") {
          val path3 = new Path(rootPath, "table3").toString
          writeDataToCheckAutoCompact(100, path3)
          // autoCompact should be triggered for path2.
          checkTableVersionAndNumFiles(path3, 1, 1)
        }
      }
    }
  }

  test("test autoCompact configs") {
    val tableName = "autoCompactTestTable"
    withTable(tableName) {
      withTempDir { dir =>
        val rootPath = dir.getCanonicalPath
        val path = new Path(rootPath, "table1").toString
        var expectedTableVersion = -1
        withSQLConf(DeltaSQLConf.AUTO_COMPACT_ENABLED.key -> "true") {
          writeDataToCheckAutoCompact(100, path, partitioned = true)
          expectedTableVersion += 2 // autoCompact should be triggered
          checkTableVersionAndNumFiles(path, expectedTableVersion, 2)

          withSQLConf(DeltaSQLConf.AUTO_COMPACT_MIN_NUM_FILES.key -> "200") {
            writeDataToCheckAutoCompact(100, path, partitioned = true)
            expectedTableVersion += 1 // autoCompact should not be triggered
            checkTableVersionAndNumFiles(path, expectedTableVersion, 200)
          }

          withSQLConf(DeltaSQLConf.AUTO_COMPACT_MAX_FILE_SIZE.key -> "1") {
            writeDataToCheckAutoCompact(100, path, partitioned = true)
            expectedTableVersion += 1 // autoCompact should not be triggered
            checkTableVersionAndNumFiles(path, expectedTableVersion, 200)
          }

          withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE.key -> "101024",
            DeltaSQLConf.AUTO_COMPACT_MIN_NUM_FILES.key -> "2") {
            val dt = io.delta.tables.DeltaTable.forPath(path)
            dt.optimize().executeCompaction()
            expectedTableVersion += 1 // autoCompact should not be triggered
            checkTableVersionAndNumFiles(path, expectedTableVersion, 8)
          }

          withSQLConf(DeltaSQLConf.AUTO_COMPACT_MIN_NUM_FILES.key -> "100") {
            writeDataToCheckAutoCompact(100, path, partitioned = true)
            expectedTableVersion += 2 // autoCompact should be triggered
            checkTableVersionAndNumFiles(path, expectedTableVersion, 2)
          }
        }
      }
    }
  }

  test("test max compact data size config") {
    withTempDir { dir =>
      val rootPath = dir.getCanonicalPath
      val path = new Path(rootPath, "table1").toString
      var expectedTableVersion = -1
      writeDataToCheckAutoCompact(100, path, partitioned = true)
      expectedTableVersion += 1
      checkTableVersionAndNumFiles(path, expectedTableVersion, 200)
      val dt = io.delta.tables.DeltaTable.forPath(path)
      val dl = DeltaLog.forTable(spark, path)
      val sizeLimit =
        dl.unsafeVolatileSnapshot.allFiles
          .filter(col("path").contains("colC=1"))
          .agg(sum(col("size")))
          .head
          .getLong(0) * 2

      withSQLConf(DeltaSQLConf.AUTO_COMPACT_ENABLED.key -> "true",
        DeltaSQLConf.AUTO_COMPACT_MAX_COMPACT_BYTES.key -> sizeLimit.toString) {
        dt.toDF
          .filter("colC == 1")
          .repartition(50)
          .write
          .format("delta")
          .mode("append")
          .save(path)
        val dl = DeltaLog.forTable(spark, path)
        // version 0: write, 1: append, 2: autoCompact
        assert(dl.unsafeVolatileSnapshot.version == 2)

        {
          val afterAutoCompact =
            dl.unsafeVolatileSnapshot.allFiles.filter(col("path").contains("colC=1")).count
          val beforeAutoCompact = dl
            .getSnapshotAt(dl.unsafeVolatileSnapshot.version - 1)
            .allFiles
            .filter(col("path").contains("colC=1"))
            .count
          assert(beforeAutoCompact == 150)
          assert(afterAutoCompact == 1)
        }

        {
          val afterAutoCompact =
            dl.unsafeVolatileSnapshot.allFiles.filter(col("path").contains("colC=0")).count
          val beforeAutoCompact = dl
            .getSnapshotAt(dl.unsafeVolatileSnapshot.version - 1)
            .allFiles
            .filter(col("path").contains("colC=0"))
            .count
          assert(beforeAutoCompact == 100)
          assert(afterAutoCompact == 100)
        }
      }
    }
  }

  test("test autoCompact.target config") {
    withTempDir { dir =>
      val rootPath = dir.getCanonicalPath
      val path1 = new Path(rootPath, "table1").toString
      val path2 = new Path(rootPath, "table2").toString
      val path3 = new Path(rootPath, "table3").toString
      val path4 = new Path(rootPath, "table4").toString
      val path5 = new Path(rootPath, "table5").toString

      def testAutoCompactTarget(
          path: String,
          target: String,
          expectedColC1Cnt: Long,
          expectedColC2Cnt: Long): Unit = {
        writeDataToCheckAutoCompact(100, path, partitioned = true)
        val dt = io.delta.tables.DeltaTable.forPath(path)

        withSQLConf(
          DeltaSQLConf.AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.AUTO_COMPACT_TARGET.key -> target) {
          dt.toDF
            .filter("colC == 1")
            .repartition(50)
            .write
            .format("delta")
            .mode("append")
            .save(path)

          val dl = DeltaLog.forTable(spark, path)
          // version 0: write, 1: append, 2: autoCompact
          assert(dl.unsafeVolatileSnapshot.version == 2, target)

          {
            val afterAutoCompact =
              dl.unsafeVolatileSnapshot.allFiles.filter(col("path").contains("colC=1")).count
            val beforeAutoCompact = dl
              .getSnapshotAt(dl.unsafeVolatileSnapshot.version - 1)
              .allFiles
              .filter(col("path").contains("colC=1"))
              .count

            assert(beforeAutoCompact == 150)
            assert(afterAutoCompact == expectedColC1Cnt)
          }

          {
            val afterAutoCompact =
              dl.unsafeVolatileSnapshot.allFiles.filter(col("path").contains("colC=0")).count
            val beforeAutoCompact = dl
              .getSnapshotAt(dl.unsafeVolatileSnapshot.version - 1)
              .allFiles
              .filter(col("path").contains("colC=0"))
              .count

            assert(beforeAutoCompact == 100)
            assert(afterAutoCompact == expectedColC2Cnt)
          }
        }
      }
      // Existing files are not optimized; newly added 50 files should be optimized.
      // 100 of colC=0, 101 of colC=1
      testAutoCompactTarget(path1, "commit", 101, 100)
      // Modified partition should be optimized.
      // 100 of colC=0, 1 of colC=1
      testAutoCompactTarget(path2, "partition", 1, 100)

      // table option should compact all partitions
      testAutoCompactTarget(path4, "table", 1, 1)

      withSQLConf(
        DeltaSQLConf.AUTO_COMPACT_ENABLED.key -> "true",
        DeltaSQLConf.AUTO_COMPACT_TARGET.key -> "partition") {
        writeDataToCheckAutoCompact(100, path3)
        // non-partitioned data should work with "partition" option.
        checkTableVersionAndNumFiles(path3, 1, 1)
      }

      withSQLConf(
        "spark.databricks.delta.autoCompact.enabled" -> "true",
        "spark.databricks.delta.autoCompact.target" -> "partition") {
        writeDataToCheckAutoCompact(100, path5)
        // non-partitioned data should work with "partition" option.
        checkTableVersionAndNumFiles(path5, 1, 1)
      }

      val e = intercept[IllegalArgumentException](
        withSQLConf(DeltaSQLConf.AUTO_COMPACT_TARGET.key -> "tabel") {
          writeDataToCheckAutoCompact(10, path3, partitioned = true)
        })
      assert(e.getMessage.contains("should be one of table, commit, partition, but was tabel"))
    }
  }

  test("test autoCompact with DVs") {
    withTempDir { tempDir =>
      val path = tempDir.getAbsolutePath
      withSQLConf(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> "true") {
        // Create 47 files each with 1000 records
        spark.range(start = 0, end = 10000, step = 1, numPartitions = 47)
          .toDF("id")
          .withColumn(colName = "extra", lit("just a random text to fill up the space....."))
          .write.format("delta").mode("append").save(path) // v0

        val deltaLog = DeltaLog.forTable(spark, path)
        val filesV0 = deltaLog.unsafeVolatileSnapshot.allFiles.collect()
        assert(filesV0.size == 47)

        // Default `optimize.maxDeletedRowsRatio` is 0.05.
        // Delete slightly more than threshold ration in two files, less in one of the file
        val file0 = filesV0(1)
        val file1 = filesV0(4)
        val file2 = filesV0(8)
        deleteRows(deltaLog, file0, approxPhyRows = 1000, ratioOfRowsToDelete = 0.06d) // v1
        deleteRows(deltaLog, file1, approxPhyRows = 1000, ratioOfRowsToDelete = 0.06d) // v2
        deleteRows(deltaLog, file2, approxPhyRows = 1000, ratioOfRowsToDelete = 0.01d) // v3

        // Save the data before optimize for comparing it later with optimize
        val data = spark.read.format("delta").load(path)

        withSQLConf(DeltaSQLConf.AUTO_COMPACT_ENABLED.key -> "true",
          DeltaSQLConf.AUTO_COMPACT_TARGET.key -> "table") {
          data.write.format("delta").mode("append").save(path) // v4 and v5
        }
        val appendChanges = deltaLog.getChanges(startVersion = 4).next()._2
        val autoOptimizeChanges = deltaLog.getChanges(startVersion = 5).next()._2

        // We expect the initial files and the ones from the last append to be compacted.
        val expectedRemoveFiles = (filesV0 ++ addedFiles(appendChanges)).map(_.path).toSet

        assert(removedFiles(autoOptimizeChanges).map(_.path).toSet === expectedRemoveFiles)

        assert(addedFiles(autoOptimizeChanges).size == 1) // Expect one new file added

        // Verify the final data after optimization hasn't changed.
        checkAnswer(spark.read.format("delta").load(path), data)
      }
    }
  }

  private def removedFiles(actions: Seq[Action]): Seq[RemoveFile] = {
    actions.filter(_.isInstanceOf[RemoveFile]).map(_.asInstanceOf[RemoveFile])
  }

  private def addedFiles(actions: Seq[Action]): Seq[AddFile] = {
    actions.filter(_.isInstanceOf[AddFile]).map(_.asInstanceOf[AddFile])
  }
}

