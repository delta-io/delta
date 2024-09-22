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

package org.apache.spark.sql.delta.skipping.clustering

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.skipping.ClusteredTableTestUtilsBase
import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumnInfo, ClusteringFileStats, ClusteringStats}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.zorder.ZCubeInfo

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf

class IncrementalZCubeClusteringSuite extends QueryTest
  with ClusteredTableTestUtilsBase
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

  private def assertClustered(table: String, files: Set[AddFile]): Unit = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
    val clusteringColumns =
      ClusteringColumnInfo.extractLogicalNames(deltaLog.update())
    assert(files.forall(_.clusteringProvider.contains(ClusteredTableUtils.clusteringProvider)))
    assert(files.forall { file =>
      val zCubeInfo = ZCubeInfo.getForFile(file)
      if (zCubeInfo.isEmpty) {
        logError(s"File $file is missing ZCube info.")
        false
      } else {
        zCubeInfo.get.zOrderBy == clusteringColumns
      }
    })
  }

  // The sentinel value to signal skipping size validation in ClusteringStats. This is used for the
  // cases where file size can not be predicated due to compression and encoding.
  private val SKIP_CHECK_SIZE_VALUE: Long = Long.MinValue

  private def validateClusteringMetrics(
      actualMetrics: ClusteringStats, expectedMetrics: ClusteringStats): Unit = {
    var finalActualMetrics = actualMetrics
    if (expectedMetrics.inputZCubeFiles.size == SKIP_CHECK_SIZE_VALUE) {
      val stats = expectedMetrics.inputZCubeFiles
      finalActualMetrics =
        finalActualMetrics.copy(inputZCubeFiles = stats.copy(size = SKIP_CHECK_SIZE_VALUE))
    }
    if (expectedMetrics.inputOtherFiles.size == SKIP_CHECK_SIZE_VALUE) {
      val stats = expectedMetrics.inputOtherFiles
      finalActualMetrics =
        finalActualMetrics.copy(inputOtherFiles = stats.copy(size = SKIP_CHECK_SIZE_VALUE))
    }
    if (expectedMetrics.mergedFiles.size == SKIP_CHECK_SIZE_VALUE) {
      val stats = expectedMetrics.mergedFiles
      finalActualMetrics =
        finalActualMetrics.copy(mergedFiles = stats.copy(size = SKIP_CHECK_SIZE_VALUE))
    }
    assert(expectedMetrics === finalActualMetrics)
  }

  private def getZCubeIds(table: String): Set[String] = {
    val files = getFiles(table)
    files.map(ZCubeInfo.getForFile).collect {
      case Some(ZCubeInfo(id, _)) => id
    }
  }

  test("test incremental clustering") {
    withSQLConf(
      SQLConf.MAX_RECORDS_PER_FILE.key -> "2") {
      withClusteredTable(
        table = table,
        schema = "col1 int, col2 int",
        clusterBy = "col1, col2") {
        addFiles(table, numFiles = 4)
        val files0 = getFiles(table)
        assert(files0.size === 4)

        // Optimize should cluster the data into two 2 files since MAX_RECORDS_PER_FILE is 2.
        runOptimize(table) { metrics =>
          assert(metrics.clusteringStats.nonEmpty)
          validateClusteringMetrics(
            actualMetrics = metrics.clusteringStats.get,
            expectedMetrics = ClusteringStats(
              inputZCubeFiles = ClusteringFileStats(0, SKIP_CHECK_SIZE_VALUE),
              inputOtherFiles = ClusteringFileStats(4, SKIP_CHECK_SIZE_VALUE),
              inputNumZCubes = 0,
              mergedFiles = ClusteringFileStats(4, SKIP_CHECK_SIZE_VALUE),
              numOutputZCubes = 1))

          assert(metrics.numFilesRemoved == 4)
          assert(metrics.numFilesAdded == 2)
        }
        val files1 = getFiles(table)
        assert(files1.size == 2)
        assertClustered(table, files1)
        assert(getZCubeIds(table).size === 1)

        // re-optimize is no-op if there is single ZCUBE in the whole table.
        withSQLConf(
          // Make the current ZCUBE big enough to include all input in a single ZCUBE.
          DeltaSQLConf.DELTA_OPTIMIZE_CLUSTERING_MIN_CUBE_SIZE.key -> Long.MaxValue.toString) {
          runOptimize(table) { metrics =>
            assert(metrics.numFilesRemoved === 0)
          }
        }
        assert(files1 == getFiles(table))

        // Append some new data and only cluster new files.
        addFiles(table, numFiles = 4)
        val files2 = getFiles(table)
        assert(files2.size === 6)

        withSQLConf(
          DeltaSQLConf.DELTA_OPTIMIZE_CLUSTERING_MIN_CUBE_SIZE.key -> 1.toString) {
          runOptimize(table) { metrics =>
            assert(metrics.clusteringStats.nonEmpty)
            validateClusteringMetrics(
              actualMetrics = metrics.clusteringStats.get,
              expectedMetrics = ClusteringStats(
                inputZCubeFiles = ClusteringFileStats(2, SKIP_CHECK_SIZE_VALUE),
                inputOtherFiles = ClusteringFileStats(4, SKIP_CHECK_SIZE_VALUE),
                inputNumZCubes = 1,
                mergedFiles = ClusteringFileStats(6, SKIP_CHECK_SIZE_VALUE),
                numOutputZCubes = 1))

            assert(metrics.numFilesRemoved === 4)
            assert(metrics.numFilesAdded === 2)
          }
        }
        val files3 = getFiles(table)
        assert(files3.intersect(files2) === files1)
        assert(getZCubeIds(table).size === 2)

        // Now there are 2 ZCUBEs, increase ZCUBE size and stable ZCUBEs should be re-clustered.
        withSQLConf(
          DeltaSQLConf.DELTA_OPTIMIZE_CLUSTERING_MIN_CUBE_SIZE.key -> Long.MaxValue.toString) {
          runOptimize(table) { metrics =>
            assert(metrics.clusteringStats.nonEmpty)
            validateClusteringMetrics(
              actualMetrics = metrics.clusteringStats.get,
              expectedMetrics = ClusteringStats(
                inputZCubeFiles = ClusteringFileStats(4, SKIP_CHECK_SIZE_VALUE),
                inputOtherFiles = ClusteringFileStats(0, SKIP_CHECK_SIZE_VALUE),
                inputNumZCubes = 2,
                mergedFiles = ClusteringFileStats(4, SKIP_CHECK_SIZE_VALUE),
                numOutputZCubes = 1))
            assert(metrics.numFilesRemoved === 4)
            // 2 records per file.
            assert(metrics.numFilesAdded === 4)
          }
        }
        val files4 = getFiles(table)
        assertClustered(table, files4)
        assert(getZCubeIds(table).size === 1)
      }
    }
  }

  test("test changing clustering columns") {
    withSQLConf(
      SQLConf.MAX_RECORDS_PER_FILE.key -> "2",
      // Enable update catalog for verifyClusteringColumns.
      DeltaSQLConf.DELTA_UPDATE_CATALOG_ENABLED.key -> "true") {
      withClusteredTable(
        table = table,
        schema = "col1 int, col2 int",
        clusterBy = "col1, col2") {
        addFiles(table, numFiles = 4)
        val files0 = getFiles(table)
        assert(files0.size === 4)
        // Cluster the table into two ZCUBEs.
        runOptimize(table) { metrics =>
          assert(metrics.clusteringStats.nonEmpty)
          validateClusteringMetrics(
            actualMetrics = metrics.clusteringStats.get,
            expectedMetrics = ClusteringStats(
              inputZCubeFiles = ClusteringFileStats(0, SKIP_CHECK_SIZE_VALUE),
              inputOtherFiles = ClusteringFileStats(4, SKIP_CHECK_SIZE_VALUE),
              inputNumZCubes = 0,
              mergedFiles = ClusteringFileStats(4, SKIP_CHECK_SIZE_VALUE),
              numOutputZCubes = 1))

          assert(metrics.numFilesRemoved == 4)
          assert(metrics.numFilesAdded == 2)
        }
        assert(getFiles(table).size == 2)

        addFiles(table, numFiles = 4)
        assert(getFiles(table).size == 6)
        withSQLConf(
          DeltaSQLConf.DELTA_OPTIMIZE_CLUSTERING_MIN_CUBE_SIZE.key -> 1.toString) {
          runOptimize(table) { metrics =>
            assert(metrics.clusteringStats.nonEmpty)
            validateClusteringMetrics(
              actualMetrics = metrics.clusteringStats.get,
              expectedMetrics = ClusteringStats(
                inputZCubeFiles = ClusteringFileStats(2, SKIP_CHECK_SIZE_VALUE),
                inputOtherFiles = ClusteringFileStats(4, SKIP_CHECK_SIZE_VALUE),
                inputNumZCubes = 1,
                mergedFiles = ClusteringFileStats(6, SKIP_CHECK_SIZE_VALUE),
                numOutputZCubes = 1))
            assert(metrics.numFilesRemoved == 4)
            assert(metrics.numFilesAdded == 2)
          }
        }
        val files1 = getFiles(table)
        assert(files1.size === 4)
        assertClustered(table, files1)
        assert(getZCubeIds(table).size == 2)

        sql(s"ALTER TABLE $table CLUSTER BY (col2, col1)")
        verifyClusteringColumns(TableIdentifier(table), Seq("col2", "col1"))
        // Incremental clustering won't touch those clustered files with different clustering
        // columns, so re-clustering should be a no-op.
        withSQLConf(
          DeltaSQLConf.DELTA_OPTIMIZE_CLUSTERING_MIN_CUBE_SIZE.key -> Long.MaxValue.toString) {
          runOptimize(table) { metrics =>
            assert(metrics.clusteringStats.nonEmpty)
            assert(metrics.numFilesRemoved == 0)
          }
        }
        assert(getFiles(table) === files1)

        // Add more files and only new files are clustered.
        addFiles(table, numFiles = 4)
        val files2 = getFiles(table)
        assert(files2.size === 8)
        withSQLConf(
          DeltaSQLConf.DELTA_OPTIMIZE_CLUSTERING_MIN_CUBE_SIZE.key -> Long.MaxValue.toString) {
          runOptimize(table) { metrics =>
            assert(metrics.clusteringStats.nonEmpty)
            validateClusteringMetrics(
              actualMetrics = metrics.clusteringStats.get,
              expectedMetrics = ClusteringStats(
                inputZCubeFiles = ClusteringFileStats(0, SKIP_CHECK_SIZE_VALUE),
                inputOtherFiles = ClusteringFileStats(4, SKIP_CHECK_SIZE_VALUE),
                inputNumZCubes = 0,
                mergedFiles = ClusteringFileStats(4, SKIP_CHECK_SIZE_VALUE),
                numOutputZCubes = 1))
            assert(metrics.numFilesRemoved == 4)
            assert(metrics.numFilesAdded == 2)
          }
        }
        val files3 = getFiles(table)
        assert(files3.size === 6)
        // files1 are files with old clustering columns 'col1'.
        assert(files3.intersect(files2) === files1)
      }
    }
  }
}

