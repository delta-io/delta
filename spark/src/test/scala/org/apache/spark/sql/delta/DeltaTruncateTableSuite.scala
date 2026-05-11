/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import java.sql.Timestamp
import java.util.Locale

import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTestBaseSuite
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType

class DeltaTruncateTableSuite
  extends QueryTest
  with DeltaSQLCommandTest
  with CatalogOwnedTestBaseSuite {

  import DeltaTruncateTableSuite._

  private final val MutableCatalogProperties: Set[String] = Set(
    "delta.lastCommitTimestamp",
    "delta.lastUpdateVersion",
    "transient_lastDdlTime")

  private def deltaLogForTable(tableName: String): DeltaLog =
    DeltaLog.forTable(spark, TableIdentifier(tableName))

  private def activeFileCount(log: DeltaLog): Long = log.update().allFiles.count()

  private def latestHistory(tableName: String): Row =
    sql(s"DESCRIBE HISTORY $tableName LIMIT 1").head()

  private def tableProperties(tableName: String): Map[String, String] = {
    sql(s"SHOW TBLPROPERTIES $tableName")
      .collect()
      .map(row => row.getString(0) -> row.getString(1))
      .toMap
  }

  private def stableTableProperties(properties: Map[String, String]): Map[String, String] = {
    properties -- MutableCatalogProperties
  }

  private def getStringSeq(row: Row, fieldName: String): Seq[String] = {
    row.getAs[scala.collection.Seq[String]](fieldName).toVector
  }

  private def getStringMap(row: Row, fieldName: String): Map[String, String] = {
    row.getAs[scala.collection.Map[String, String]](fieldName).toMap
  }

  private def getDetails(tableName: String): TableDetails = {
    val row = sql(s"DESCRIBE DETAIL $tableName").head()
    val stable = StableTableDetails(
      format = row.getAs[String]("format"),
      id = row.getAs[String]("id"),
      name = row.getAs[String]("name"),
      description = row.getAs[String]("description"),
      location = row.getAs[String]("location"),
      createdAt = row.getAs[Timestamp]("createdAt"),
      partitionColumns = getStringSeq(row, "partitionColumns"),
      clusteringColumns = getStringSeq(row, "clusteringColumns"),
      properties = getStringMap(row, "properties"),
      minReaderVersion = row.getAs[Int]("minReaderVersion"),
      minWriterVersion = row.getAs[Int]("minWriterVersion"),
      tableFeatures = getStringSeq(row, "tableFeatures"))

    TableDetails(
      stable = stable,
      numFiles = row.getAs[Long]("numFiles"),
      sizeInBytes = row.getAs[Long]("sizeInBytes"))
  }

  private def getState(log: DeltaLog): TableState = {
    val snapshot = log.update()
    TableState(
      minReaderVersion = snapshot.protocol.minReaderVersion,
      minWriterVersion = snapshot.protocol.minWriterVersion,
      readerFeatures = snapshot.protocol.readerFeatures.getOrElse(Set.empty),
      writerFeatures = snapshot.protocol.writerFeatures.getOrElse(Set.empty),
      configuration = snapshot.metadata.configuration,
      schema = snapshot.metadata.schema,
      partitionColumns = snapshot.metadata.partitionColumns)
  }

  private def captureSnapshot(log: DeltaLog, tableName: String): TableSnapshot = {
    val state = getState(log)
    val details = getDetails(tableName)
    val props = tableProperties(tableName)
    TableSnapshot(state, details.stable, stableTableProperties(props))
  }

  private def assertSnapshot(
      log: DeltaLog,
      tableName: String,
      expected: TableSnapshot): Unit = {
    val actual = captureSnapshot(log, tableName)
    assert(actual.state == expected.state)
    assert(actual.stableDetails === expected.stableDetails)
    assert(actual.stableProperties === expected.stableProperties)
  }

  Seq(false, true).foreach { partitioned =>
    val desc = if (partitioned) "partitioned" else "unpartitioned"
    test(s"truncate $desc table removes all data and preserves metadata") {
      val tableName = s"truncate_$desc"
      withTable(tableName) {
        val partitionClause = if (partitioned) "PARTITIONED BY (part)" else ""
        sql(
          s"""
             |CREATE TABLE $tableName (id LONG, part LONG)
             |USING delta
             |$partitionClause
             |TBLPROPERTIES ('truncateTestProp' = 'preserved')
             |COMMENT 'metadata survives truncate'
             |""".stripMargin)
        val log = deltaLogForTable(tableName)
        val pathRef = s"delta.`${log.dataPath}`"
        val snapshotBefore = captureSnapshot(log, tableName)

        Seq(tableName, pathRef).foreach { truncateTarget =>
          sql(s"INSERT INTO $tableName VALUES (0, 0), (1, 1), (2, 0), (3, 1)")

          sql(s"TRUNCATE TABLE $truncateTarget")

          checkAnswer(sql(s"SELECT * FROM $tableName"), Nil)
          assert(activeFileCount(log) === 0)

          val history = latestHistory(tableName)
          assert(history.getAs[String]("operation") === "TRUNCATE")

          assertSnapshot(log, tableName, snapshotBefore)
          val detailsAfter = getDetails(tableName)
          assert(detailsAfter.numFiles === 0)
          assert(detailsAfter.sizeInBytes === 0)
        }
      }
    }
  }

  test("truncate empty delta table is a no-op") {
    val tableName = "truncate_empty"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (id LONG) USING delta")

      val log = deltaLogForTable(tableName)
      val pathRef = s"delta.`${log.dataPath}`"
      val versionBeforeTruncate = log.update().version

      val snapshotBefore = captureSnapshot(log, tableName)

      Seq(tableName, pathRef).foreach { truncateTarget =>
        sql(s"TRUNCATE TABLE $truncateTarget")

        assert(log.update().version === versionBeforeTruncate)

        assertSnapshot(log, tableName, snapshotBefore)
      }
    }
  }

  test("truncate is idempotent: second truncate on empty table is a no-op") {
    val tableName = "truncate_idempotent"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (id LONG) USING delta")
      sql(s"INSERT INTO $tableName VALUES (1), (2)")

      val log = deltaLogForTable(tableName)
      val pathRef = s"delta.`${log.dataPath}`"
      val snapshotBefore = captureSnapshot(log, tableName)

      Seq(tableName, pathRef).foreach { truncateTarget =>
        sql(s"INSERT INTO $tableName VALUES (1), (2)")

        sql(s"TRUNCATE TABLE $truncateTarget")
        val versionAfterFirstTruncate = log.update().version
        checkAnswer(sql(s"SELECT * FROM $tableName"), Nil)
        assertSnapshot(log, tableName, snapshotBefore)

        sql(s"TRUNCATE TABLE $truncateTarget")
        assert(log.update().version === versionAfterFirstTruncate)
        checkAnswer(sql(s"SELECT * FROM $tableName"), Nil)
        assertSnapshot(log, tableName, snapshotBefore)
      }
    }
  }

  test("truncate append-only table fails atomically") {
    val tableName = "truncate_append_only"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (id LONG)
           |USING delta
           |TBLPROPERTIES ('delta.appendOnly' = 'true')
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES (1), (2)")

      val log = deltaLogForTable(tableName)
      val pathRef = s"delta.`${log.dataPath}`"
      val versionBeforeTruncate = log.update().version

      val snapshotBefore = captureSnapshot(log, tableName)

      Seq(tableName, pathRef).foreach { truncateTarget =>
        val e = intercept[DeltaUnsupportedOperationException] {
          sql(s"TRUNCATE TABLE $truncateTarget")
        }
        checkError(
          e,
          "DELTA_CANNOT_MODIFY_APPEND_ONLY",
          parameters = Map("table_name" -> "null", "config" -> DeltaConfigs.IS_APPEND_ONLY.key))

        checkAnswer(sql(s"SELECT id FROM $tableName ORDER BY id"), Seq(Row(1), Row(2)))
        assert(log.update().version === versionBeforeTruncate)

        assertSnapshot(log, tableName, snapshotBefore)
      }
    }
  }


  test("truncate with partition spec is rejected and leaves table unchanged") {
    val tableName = "truncate_partition_spec"
    withTable(tableName) {
      spark.range(start = 0, end = 4, step = 1, numPartitions = 2)
        .selectExpr("id", "id % 2 AS part")
        .write.format("delta").partitionBy("part").saveAsTable(tableName)

      val log = deltaLogForTable(tableName)
      val pathRef = s"delta.`${log.dataPath}`"
      val versionBeforeTruncate = log.update().version

      val snapshotBefore = captureSnapshot(log, tableName)

      Seq(tableName, pathRef).foreach { truncateTarget =>
        val e = intercept[Exception] {
          sql(s"TRUNCATE TABLE $truncateTarget PARTITION (part = 1)")
        }
        assert(
          Option(e.getMessage).exists { message =>
            val lowerCaseMessage = message.toLowerCase(Locale.ROOT)
            message.contains("DELTA_TRUNCATE_TABLE_PARTITION_NOT_SUPPORTED") ||
              lowerCaseMessage.contains("truncate") && lowerCaseMessage.contains("partition")
          })

        checkAnswer(
          sql(s"SELECT id, part FROM $tableName ORDER BY id"),
          Seq(Row(0, 0), Row(1, 1), Row(2, 0), Row(3, 1)))
        assert(log.update().version === versionBeforeTruncate)

        assertSnapshot(log, tableName, snapshotBefore)
      }
    }
  }
}

class DeltaTruncateTableWithCatalogOwnedBatch1Suite extends DeltaTruncateTableSuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(1)
}

object DeltaTruncateTableSuite {

  private final case class TableState(
      minReaderVersion: Int,
      minWriterVersion: Int,
      readerFeatures: Set[String],
      writerFeatures: Set[String],
      configuration: Map[String, String],
      schema: StructType,
      partitionColumns: Seq[String])

  private final case class StableTableDetails(
      format: String,
      id: String,
      name: String,
      description: String,
      location: String,
      createdAt: Timestamp,
      partitionColumns: Seq[String],
      clusteringColumns: Seq[String],
      properties: Map[String, String],
      minReaderVersion: Int,
      minWriterVersion: Int,
      tableFeatures: Seq[String])

  private final case class TableDetails(
      stable: StableTableDetails,
      numFiles: Long,
      sizeInBytes: Long)

  private final case class TableSnapshot(
      state: TableState,
      stableDetails: StableTableDetails,
      stableProperties: Map[String, String])
}
