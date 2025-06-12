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

package org.apache.spark.sql.delta.clustering

import org.apache.spark.sql.delta.skipping.ClusteredTableTestUtils
import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ClusteredTableClusteringSuite extends SparkFunSuite
  with SharedSparkSession
  with ClusteredTableTestUtils
  with DeltaSQLCommandTest {
  import testImplicits._

  private val table: String = "test_table"

  // Ingest data to create numFiles files with one row in each file.
  private def addFiles(table: String, numFiles: Int): Unit = {
    val df = (1 to numFiles).map(i => (i, i)).toDF("col1", "col2")
    withSQLConf(SQLConf.MAX_RECORDS_PER_FILE.key -> "1") {
      df.write.format("delta").mode("append").saveAsTable(table)
    }
  }

  private def getFiles(table: String): Set[AddFile] = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
    deltaLog.update().allFiles.collect().toSet
  }

  private def assertClustered(files: Set[AddFile]): Unit = {
    assert(files.forall(_.clusteringProvider.contains(ClusteredTableUtils.clusteringProvider)))
  }

  private def assertNotClustered(files: Set[AddFile]): Unit = {
    assert(files.forall(_.clusteringProvider.isEmpty))
  }

  test("optimize clustered table") {
    withSQLConf(SQLConf.MAX_RECORDS_PER_FILE.key -> "2") {
      withClusteredTable(
        table = table,
        schema = "col1 int, col2 int",
        clusterBy = "col1, col2") {
        addFiles(table, numFiles = 4)
        val files0 = getFiles(table)
        assert(files0.size === 4)
        assertNotClustered(files0)

        // Optimize should cluster the data into two 2 files since MAX_RECORDS_PER_FILE is 2.
        runOptimize(table) { metrics =>
          assert(metrics.numFilesRemoved == 4)
          assert(metrics.numFilesAdded == 2)
        }

        val files1 = getFiles(table)
        assert(files1.size == 2)
        assertClustered(files1)
      }
    }
  }

  test("cluster by 1 column") {
    withSQLConf(SQLConf.MAX_RECORDS_PER_FILE.key -> "2") {
      withClusteredTable(
        table = table,
        schema = "col1 int, col2 int",
        clusterBy = "col1") {
        addFiles(table, numFiles = 4)
        val files0 = getFiles(table)
        assert(files0.size === 4)
        assertNotClustered(files0)

        // Optimize should cluster the data into two 2 files since MAX_RECORDS_PER_FILE is 2.
        runOptimize(table) { metrics =>
          assert(metrics.numFilesRemoved == 4)
          assert(metrics.numFilesAdded == 2)
        }

        val files1 = getFiles(table)
        assert(files1.size == 2)
        assertClustered(files1)
      }
    }
  }

  test("optimize clustered table with batching") {
    Seq(("1", 2), ("1g", 1)).foreach { case (batchSize, optimizeCommits) =>
      withClusteredTable(
        table = table,
        schema = "col1 int, col2 int",
        clusterBy = "col1, col2") {
        addFiles(table, numFiles = 4)
        val files0 = getFiles(table)
        assert(files0.size === 4)
        assertNotClustered(files0)

        val totalSize = files0.toSeq.map(_.size).sum
        val halfSize = totalSize / 2

        withSQLConf(
          DeltaSQLConf.DELTA_OPTIMIZE_BATCH_SIZE.key -> batchSize,
          DeltaSQLConf.DELTA_OPTIMIZE_CLUSTERING_MIN_CUBE_SIZE.key -> halfSize.toString,
          DeltaSQLConf.DELTA_OPTIMIZE_CLUSTERING_TARGET_CUBE_SIZE.key -> halfSize.toString) {
          // Optimize should create 2 cubes, which will be in separate batches if the batch size
          // is small enough
          runOptimize(table) { metrics =>
            assert(metrics.numFilesRemoved == 4)
            assert(metrics.numFilesAdded == 2)
          }

          val files1 = getFiles(table)
          assert(files1.size == 2)
          assertClustered(files1)

          val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))

          val commits = deltaLog.history.getHistory(None)
          assert(commits.filter(_.operation == "OPTIMIZE").length == optimizeCommits)
        }
      }
    }
  }

  test("optimize clustered table with batching on an empty table") {
    withClusteredTable(
      table = table,
      schema = "col1 int, col2 int",
      clusterBy = "col1, col2") {
      withSQLConf(DeltaSQLConf.DELTA_OPTIMIZE_BATCH_SIZE.key -> "1g") {
        runOptimize(table) { metrics =>
          assert(metrics.numFilesRemoved == 0)
          assert(metrics.numFilesAdded == 0)
        }
      }
    }
  }
}
