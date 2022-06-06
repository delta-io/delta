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

package org.apache.spark.sql.delta.cdc

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Small end to end tests of workloads using CDC from Delta.
 */
class CDCWorkloadSuite extends QueryTest with SharedSparkSession  with DeltaSQLCommandTest {

  test("replication workload") {
    withSQLConf((DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")) {
      withTempPaths(2) { paths =>
        // Create an empty table at `path` we're going to replicate from, and a replication
        // destination at `replicatedPath`. The destination contains a subset of the final keys,
        // but with out-of-date enrichment data.
        val path = paths.head.getAbsolutePath
        val replicatedPath = paths(1).getAbsolutePath
        spark.range(0).selectExpr("id", "'none' as text").write.format("delta").save(path)
        spark.range(50)
          .selectExpr("id", "'oldEnrichment' as text")
          .filter("id % 4 = 0")
          .write.format("delta").save(replicatedPath)

        // Add data to the replication source in overlapping batches, so we produce both insert and
        // update events.
        for (i <- 0 to 8) {
          withTempView("source") {
            spark.range(i * 5, i * 5 + 10)
              .selectExpr("id", "'newEnrichment' as text")
              .createOrReplaceTempView("source")
            sql(
              s"""MERGE INTO delta.`$path` t USING source s ON s.id = t.id
                 |WHEN MATCHED THEN UPDATE SET *
                 |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
          }
        }

        // Delete some data too.
        sql(s"DELETE FROM delta.`$path` WHERE id < 5")

        for (v <- 0 to 10) {
          withTempView("cdcSource") {
            val changes = spark.read.format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", v)
              .option("endingVersion", v)
              .load(path)
            // Filter out the preimage so the update events only have the final row, as required by
            // our merge API.
            changes.filter("_change_type != 'update_preimage'").createOrReplaceTempView("cdcSource")
            sql(
              s"""MERGE INTO delta.`$replicatedPath` t USING cdcSource s ON s.id = t.id
                 |WHEN MATCHED AND s._change_type = 'update_postimage' OR s._change_type = 'insert'
                 |  THEN UPDATE SET *
                 |WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
                 |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
          }
        }

        // We should have all the rows, all with the new enrichment data from the replication
        // source, except for 0 to 5 which were deleted.
        val expected = spark.range(5, 50).selectExpr("id", "'newEnrichment' as text")
        checkAnswer(spark.read.format("delta").load(replicatedPath), expected)
      }
    }
  }
}
