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

import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions}
import org.apache.spark.sql.delta.skipping.ClusteredTableTestUtils
import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta.{ClusteringTableFeature, DeltaAnalysisException, DeltaLog, TableFeature}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ClusteringTableFeatureSuite extends SparkFunSuite
  with SharedSparkSession
  with ClusteredTableTestUtils
  with DeltaSQLCommandTest {
  import testImplicits._

  test("create table without cluster by clause cannot set clustering table properties") {
    withTable("tbl") {
      val e = intercept[DeltaAnalysisException] {
        sql("CREATE TABLE tbl(a INT, b STRING) USING DELTA " +
          "TBLPROPERTIES('delta.feature.clustering' = 'supported')")
      }
      checkError(
        e,
        "DELTA_CREATE_TABLE_SET_CLUSTERING_TABLE_FEATURE_NOT_ALLOWED",
        parameters = Map("tableFeature" -> "clustering"))
    }
  }

  test("use alter table set table properties to enable clustering is not allowed.") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(a INT, b STRING) USING DELTA")
      val e = intercept[DeltaAnalysisException] {
        sql("ALTER TABLE tbl SET TBLPROPERTIES ('delta.feature.clustering' = 'supported')")
      }
      checkError(
        e,
        "DELTA_ALTER_TABLE_SET_CLUSTERING_TABLE_FEATURE_NOT_ALLOWED",
        parameters = Map("tableFeature" -> "clustering"))
    }
  }

  test("alter table cluster by partitioned tables is not allowed.") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(a INT, b STRING) USING DELTA PARTITIONED BY (a)")
      val e1 = intercept[DeltaAnalysisException] {
        sql("ALTER TABLE tbl CLUSTER BY (a)")
      }
      checkError(
        e1,
        "DELTA_ALTER_TABLE_CLUSTER_BY_ON_PARTITIONED_TABLE_NOT_ALLOWED",
        parameters = Map.empty)

      val e2 = intercept[DeltaAnalysisException] {
        sql("ALTER TABLE tbl CLUSTER BY NONE")
      }
      checkError(
        e2,
        "DELTA_ALTER_TABLE_CLUSTER_BY_ON_PARTITIONED_TABLE_NOT_ALLOWED",
        parameters = Map.empty)
    }
  }

   test("alter table cluster by unpartitioned tables is supported.") {
    val table = "tbl"
    withTable(table) {
      sql(s"CREATE TABLE $table (a INT, b STRING) USING DELTA")
      val (_, startingSnapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(table))
      assert(!ClusteredTableUtils.isSupported(startingSnapshot.protocol))
      val clusterByLogs = Log4jUsageLogger.track {
        sql(s"ALTER TABLE $table CLUSTER BY (a)")
      }.filter { e =>
        e.metric == MetricDefinitions.EVENT_TAHOE.name &&
          e.tags.get("opType").contains("delta.ddl.alter.clusterBy")
      }
      assert(clusterByLogs.nonEmpty)
      val clusterByLogJson = JsonUtils.fromJson[Map[String, Any]](clusterByLogs.head.blob)
      assert(!clusterByLogJson("isClusterByNoneSkipped").asInstanceOf[Boolean])
      val (_, finalSnapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(table))
      assert(ClusteredTableUtils.isSupported(finalSnapshot.protocol))
      val dependentFeatures = TableFeature.getDependentFeatures(ClusteringTableFeature)
      dependentFeatures.foreach { feature =>
        assert(finalSnapshot.protocol.isFeatureSupported(feature))
      }

      withSQLConf(SQLConf.MAX_RECORDS_PER_FILE.key -> "2") {
        val df = (1 to 4).map(i => (i, i.toString)).toDF("a", "b")
        withSQLConf(SQLConf.MAX_RECORDS_PER_FILE.key -> "1") {
          df.write.format("delta").mode("append").saveAsTable(table)
        }

        // Optimize should cluster the data into two 2 files since MAX_RECORDS_PER_FILE is 2.
        runOptimize(table) { metrics =>
          assert(metrics.numFilesRemoved == 4)
          assert(metrics.numFilesAdded == 2)
        }

        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
        val files1 = deltaLog.update().allFiles.collect().toSet
        assert(files1.size == 2)
        assert(files1.forall(_.clusteringProvider.contains(ClusteredTableUtils.clusteringProvider)))

        // Check if min-max intervals of 'a' are sorted
        val minMaxIntervals = files1.map { file =>
          val stats = JsonUtils.mapper.readTree(file.stats)
          (stats.get("minValues").get("a").asInt, stats.get("maxValues").get("a").asInt)
        }

        val sortedAsc = minMaxIntervals.sliding(2).forall {
          case Seq((_, maxA1), (minA2, _)) => maxA1.asInstanceOf[Int] < minA2.asInstanceOf[Int]
          case _ => true
        }

        val sortedDesc = minMaxIntervals.sliding(2).forall {
          case Seq((minA1, _), (_, maxA2)) => minA1.asInstanceOf[Int] > maxA2.asInstanceOf[Int]
          case _ => true
        }

        assert(sortedAsc || sortedDesc, "Min-max intervals for column 'a' are not sorted.")
      }
    }
  }
}
