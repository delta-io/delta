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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier

class UpdateCDCSuite extends UpdateSQLSuite with DeltaColumnMappingTestUtils {
  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")

  test("CDC for unconditional update") {
    append(Seq((1, 1), (2, 2), (3, 3), (4, 4)).toDF("key", "value"))

    checkUpdate(
      condition = None,
      setClauses = "value = -1",
      expectedResults = Row(1, -1) :: Row(2, -1) :: Row(3, -1) :: Row(4, -1) :: Nil)

    val log = DeltaLog.forTable(spark, tempPath)
    val latestVersion = log.unsafeVolatileSnapshot.version
    checkAnswer(
      CDCReader
        .changesToBatchDF(log, latestVersion, latestVersion, spark)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP),
      Row(1, 1, "update_preimage", latestVersion) ::
        Row(1, -1, "update_postimage", latestVersion) ::
        Row(2, 2, "update_preimage", latestVersion) ::
        Row(2, -1, "update_postimage", latestVersion) ::
        Row(3, 3, "update_preimage", latestVersion) ::
        Row(3, -1, "update_postimage", latestVersion) ::
        Row(4, 4, "update_preimage", latestVersion) ::
        Row(4, -1, "update_postimage", latestVersion) ::
        Nil)
  }

  test("CDC for conditional update on all rows") {
    append(Seq((1, 1), (2, 2), (3, 3), (4, 4)).toDF("key", "value"))

    checkUpdate(
      condition = Some("key < 10"),
      setClauses = "value = -1",
      expectedResults = Row(1, -1) :: Row(2, -1) :: Row(3, -1) :: Row(4, -1) :: Nil)

    val log = DeltaLog.forTable(spark, tempPath)
    val latestVersion = log.unsafeVolatileSnapshot.version
    checkAnswer(
      CDCReader
        .changesToBatchDF(log, latestVersion, latestVersion, spark)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP),
      Row(1, 1, "update_preimage", latestVersion) ::
        Row(1, -1, "update_postimage", latestVersion) ::
        Row(2, 2, "update_preimage", latestVersion) ::
        Row(2, -1, "update_postimage", latestVersion) ::
        Row(3, 3, "update_preimage", latestVersion) ::
        Row(3, -1, "update_postimage", latestVersion) ::
        Row(4, 4, "update_preimage", latestVersion) ::
        Row(4, -1, "update_postimage", latestVersion) ::
        Nil)
  }

  test("CDC for point update") {
    append(Seq((1, 1), (2, 2), (3, 3), (4, 4)).toDF("key", "value"))

    checkUpdate(
      condition = Some("key = 1"),
      setClauses = "value = -1",
      expectedResults = Row(1, -1) :: Row(2, 2) :: Row(3, 3) :: Row(4, 4) :: Nil)

    val log = DeltaLog.forTable(spark, tempPath)
    val latestVersion = log.unsafeVolatileSnapshot.version
    checkAnswer(
      CDCReader
        .changesToBatchDF(log, latestVersion, latestVersion, spark)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP),
      Row(1, 1, "update_preimage", latestVersion) ::
        Row(1, -1, "update_postimage", latestVersion) ::
        Nil)
  }

  test("CDC for repeated point update") {
    append(Seq((1, 1), (2, 2), (3, 3), (4, 4)).toDF("key", "value"))

    checkUpdate(
      condition = Some("key = 1"),
      setClauses = "value = -1",
      expectedResults = Row(1, -1) :: Row(2, 2) :: Row(3, 3) :: Row(4, 4) :: Nil)

    val log = DeltaLog.forTable(spark, tempPath)
    val latestVersion1 = log.unsafeVolatileSnapshot.version
    checkAnswer(
      CDCReader
        .changesToBatchDF(log, latestVersion1, latestVersion1, spark)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP),
      Row(1, 1, "update_preimage", latestVersion1) ::
        Row(1, -1, "update_postimage", latestVersion1) ::
        Nil)

    checkUpdate(
      condition = Some("key = 3"),
      setClauses = "value = -3",
      expectedResults = Row(1, -1) :: Row(2, 2) :: Row(3, -3) :: Row(4, 4) :: Nil)

    val latestVersion2 = log.unsafeVolatileSnapshot.version
    checkAnswer(
      CDCReader
        .changesToBatchDF(log, latestVersion1, latestVersion2, spark)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP),
      Row(1, 1, "update_preimage", latestVersion1) ::
        Row(1, -1, "update_postimage", latestVersion1) ::
        Row(3, 3, "update_preimage", latestVersion2) ::
        Row(3, -3, "update_postimage", latestVersion2) ::
        Nil)
  }

  test("CDC for partition-optimized update") {
    append(
      Seq((1, 1, 1), (2, 2, 0), (3, 3, 1), (4, 4, 0)).toDF("key", "value", "part"),
      partitionBy = Seq("part"))

    checkUpdate(
      condition = Some("part = 1"),
      setClauses = "value = -1",
      expectedResults = Row(1, -1) :: Row(2, 2) :: Row(3, -1) :: Row(4, 4) :: Nil)

    val log = DeltaLog.forTable(spark, tempPath)
    val latestVersion = log.unsafeVolatileSnapshot.version
    checkAnswer(
      CDCReader
        .changesToBatchDF(log, latestVersion, latestVersion, spark)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP),
      Row(1, 1, 1, "update_preimage", latestVersion) ::
        Row(1, -1, 1, "update_postimage", latestVersion) ::
        Row(3, 3, 1, "update_preimage", latestVersion) ::
        Row(3, -1, 1, "update_postimage", latestVersion) ::
        Nil)
  }


  test("update a partitioned CDC enabled table to set the partition column to null") {
    val tableName = "part_table_test"
    withTable(tableName) {
      Seq((0, 0, 0), (1, 1, 1), (2, 2, 2))
        .toDF("key", "partition_column", "value")
        .write
        .partitionBy("partition_column")
        .format("delta")
        .saveAsTable(tableName)
      sql(s"INSERT INTO $tableName VALUES (4, 4, 4)")
      sql(s"UPDATE $tableName SET partition_column = null WHERE partition_column = 4")
      checkAnswer(
        CDCReader.changesToBatchDF(
          DeltaLog.forTable(
            spark,
            spark.sessionState.sqlParser.parseTableIdentifier(tableName)
          ), 1, 3, spark)
          .drop(CDCReader.CDC_COMMIT_TIMESTAMP),
        Row(4, 4, 4, "insert", 1) ::
          Row(4, 4, 4, "update_preimage", 2) ::
          Row(4, null, 4, "update_postimage", 2)  :: Nil)
    }
  }
}

