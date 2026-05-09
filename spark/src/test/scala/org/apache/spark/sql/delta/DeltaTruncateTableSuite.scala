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

import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTestBaseSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType

final class DeltaTruncateTableSuite
  extends QueryTest
  with DeltaSQLCommandTest
  with CatalogOwnedTestBaseSuite {

  import DeltaTruncateTableSuite._

  // Make every CREATE TABLE produce a CatalogOwned (CCv2) table by default so the suite exercises
  // the catalog-managed code path that ships with managed UC tables.
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(1)

  // Mirrors UCDeltaTableCreationTest.EXPECTED_MANAGED_TABLE_FEATURES. Stored without the
  // "delta.feature." prefix because Protocol.{reader,writer}Features stores bare feature names.
  private final val ExpectedManagedTableFeatures: Set[String] = Set(
    "appendOnly",
    "catalogManaged",
    "deletionVectors",
    "domainMetadata",
    "inCommitTimestamp",
    "invariants",
    "rowTracking",
    "v2Checkpoint",
    "vacuumProtocolCheck")

  // The subset of UCDeltaTableCreationTest's expected properties that lives in Delta's
  // metadata.configuration (UC server-side properties and protocol versions are checked
  // separately).
  private final val ExpectedManagedTableProperties: Map[String, String] = Map(
    "delta.checkpointPolicy" -> "v2",
    "delta.enableDeletionVectors" -> "true",
    "delta.enableInCommitTimestamps" -> "true",
    "delta.enableRowTracking" -> "true")

  private final val ExpectedManagedTableFeatureProperties: Map[String, String] =
    ExpectedManagedTableFeatures.map(feature => s"delta.feature.$feature" -> "supported").toMap

  private final val ExpectedManagedTablePropertyView: Map[String, String] =
    ExpectedManagedTableProperties ++ ExpectedManagedTableFeatureProperties ++ Map(
      Protocol.MIN_READER_VERSION_PROP -> "3",
      Protocol.MIN_WRITER_VERSION_PROP -> "7")

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

  private def assertManagedTablePropertyView(properties: Map[String, String]): Unit = {
    val missingOrChangedProperties = ExpectedManagedTablePropertyView.filterNot {
      case (key, expected) => properties.get(key).contains(expected)
    }
    assert(missingOrChangedProperties.isEmpty,
      s"Expected managed table properties missing or changed: $missingOrChangedProperties. " +
        s"Actual properties: $properties")
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

    assert(stable.format === "delta")
    assert(stable.minReaderVersion === 3,
      s"Expected detail minReaderVersion=3, got ${stable.minReaderVersion}")
    assert(stable.minWriterVersion === 7,
      s"Expected detail minWriterVersion=7, got ${stable.minWriterVersion}")
    val missingFeatures = ExpectedManagedTableFeatures -- stable.tableFeatures.toSet
    assert(missingFeatures.isEmpty,
      s"Missing expected managed table features from DESCRIBE DETAIL: $missingFeatures. " +
        s"Actual features: ${stable.tableFeatures}")
    ExpectedManagedTableProperties.foreach { case (key, expected) =>
      assert(stable.properties.get(key).contains(expected),
        s"Expected DESCRIBE DETAIL property '$key=$expected' but got " +
          s"'${stable.properties.get(key)}'. Full properties: ${stable.properties}")
    }

    TableDetails(
      stable = stable,
      numFiles = row.getAs[Long]("numFiles"),
      sizeInBytes = row.getAs[Long]("sizeInBytes"))
  }

  /** Captures the current table state and asserts it satisfies the CCv2 managed-table contract. */
  private def getState(log: DeltaLog): TableState = {
    val snapshot = log.update()
    val state = TableState(
      minReaderVersion = snapshot.protocol.minReaderVersion,
      minWriterVersion = snapshot.protocol.minWriterVersion,
      readerFeatures = snapshot.protocol.readerFeatures.getOrElse(Set.empty),
      writerFeatures = snapshot.protocol.writerFeatures.getOrElse(Set.empty),
      configuration = snapshot.metadata.configuration,
      schema = snapshot.metadata.schema,
      partitionColumns = snapshot.metadata.partitionColumns)
    assert(state.minReaderVersion === 3,
      s"Expected minReaderVersion=3, got ${state.minReaderVersion}")
    assert(state.minWriterVersion === 7,
      s"Expected minWriterVersion=7, got ${state.minWriterVersion}")
    val missingFeatures = ExpectedManagedTableFeatures -- state.writerFeatures
    assert(missingFeatures.isEmpty,
      s"Missing expected managed table features: $missingFeatures. " +
        s"Actual writer features: ${state.writerFeatures}")
    ExpectedManagedTableProperties.foreach { case (key, expected) =>
      assert(state.configuration.get(key).contains(expected),
        s"Expected property '$key=$expected' but got '${state.configuration.get(key)}'. " +
          s"Full configuration: ${state.configuration}")
    }
    state
  }

  /** Asserts every field of `actual` matches `expected`, with field-level failure messages. */
  private def assertStateMatches(actual: TableState, expected: TableState): Unit = {
    assert(actual.minReaderVersion === expected.minReaderVersion,
      s"minReaderVersion changed: ${expected.minReaderVersion} -> ${actual.minReaderVersion}")
    assert(actual.minWriterVersion === expected.minWriterVersion,
      s"minWriterVersion changed: ${expected.minWriterVersion} -> ${actual.minWriterVersion}")
    assert(actual.readerFeatures === expected.readerFeatures,
      s"Reader features changed.\nBefore: ${expected.readerFeatures}\n" +
        s"After:  ${actual.readerFeatures}")
    assert(actual.writerFeatures === expected.writerFeatures,
      s"Writer features changed.\nBefore: ${expected.writerFeatures}\n" +
        s"After:  ${actual.writerFeatures}")
    assert(actual.configuration === expected.configuration,
      s"Metadata configuration changed.\nBefore: ${expected.configuration}\n" +
        s"After:  ${actual.configuration}")
    assert(actual.schema === expected.schema,
      s"Schema changed.\nBefore: ${expected.schema.treeString}\n" +
        s"After:  ${actual.schema.treeString}")
    assert(actual.partitionColumns === expected.partitionColumns,
      s"Partition columns changed: ${expected.partitionColumns} -> ${actual.partitionColumns}")
  }

  /**
   * Captures the full preserved table envelope (snapshot state + DESCRIBE DETAIL stable fields +
   * SHOW TBLPROPERTIES stable subset) and asserts the CCv2 managed-table contract from all three
   * angles. Variable fields (numFiles, sizeInBytes, mutable catalog properties) are excluded.
   */
  private def captureSnapshot(log: DeltaLog, tableName: String): TableSnapshot = {
    val state = getState(log)
    val details = getDetails(tableName)
    val props = tableProperties(tableName)
    assertManagedTablePropertyView(props)
    TableSnapshot(state, details.stable, stableTableProperties(props))
  }

  /**
   * Captures the current full snapshot and asserts every preserved attribute matches the
   * baseline, including: protocol versions, reader/writer features, metadata configuration,
   * schema, partition columns, DESCRIBE DETAIL stable fields (id, name, description, location,
   * createdAt, partition/clustering columns, properties, table features), and the SHOW
   * TBLPROPERTIES stable subset (which carries catalog-side properties such as the UC table id).
   */
  private def assertSnapshot(
      log: DeltaLog,
      tableName: String,
      expected: TableSnapshot): Unit = {
    val actual = captureSnapshot(log, tableName)
    assertStateMatches(actual.state, expected.state)
    assert(actual.stableDetails === expected.stableDetails,
      s"DESCRIBE DETAIL stable fields changed.\nBefore: ${expected.stableDetails}\n" +
        s"After:  ${actual.stableDetails}")
    assert(actual.stableProperties === expected.stableProperties,
      s"SHOW TBLPROPERTIES stable subset changed.\nBefore: ${expected.stableProperties}\n" +
        s"After:  ${actual.stableProperties}")
  }

  test("truncate non-empty delta table removes all active files and records TRUNCATE") {
    val tableName = "truncate_non_empty"
    withTable(tableName) {
      spark.range(start = 0, end = 10, step = 1, numPartitions = 4)
        .write.format("delta").saveAsTable(tableName)

      val log = deltaLogForTable(tableName)
      val numFilesBeforeTruncate = activeFileCount(log)
      assert(numFilesBeforeTruncate > 1)

      val snapshotBefore = captureSnapshot(log, tableName)

      sql(s"TRUNCATE TABLE $tableName")

      checkAnswer(sql(s"SELECT * FROM $tableName"), Nil)
      assert(activeFileCount(log) === 0)

      val history = latestHistory(tableName)
      assert(history.getAs[String]("operation") === "TRUNCATE")

      assertSnapshot(log, tableName, snapshotBefore)
      val detailsAfter = getDetails(tableName)
      assert(detailsAfter.numFiles === 0,
        s"Expected numFiles=0 after truncate, got ${detailsAfter.numFiles}")
      assert(detailsAfter.sizeInBytes === 0,
        s"Expected sizeInBytes=0 after truncate, got ${detailsAfter.sizeInBytes}")
    }
  }

  test("truncate empty delta table is a no-op") {
    val tableName = "truncate_empty"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (id LONG) USING delta")

      val log = deltaLogForTable(tableName)
      val versionBeforeTruncate = log.update().version

      val snapshotBefore = captureSnapshot(log, tableName)

      sql(s"TRUNCATE TABLE $tableName")

      checkAnswer(sql(s"SELECT * FROM $tableName"), Nil)
      assert(log.update().version === versionBeforeTruncate)
      assert(activeFileCount(log) === 0)

      assertSnapshot(log, tableName, snapshotBefore)
    }
  }

  test("truncate with idempotency token records SetTransaction even when empty") {
    val tableName = "truncate_idempotent_empty"
    val appId = "truncateIdempotentTestApp"
    val txnAppIdKey = DeltaSQLConf.DELTA_IDEMPOTENT_DML_TXN_APP_ID.key
    val txnVersionKey = DeltaSQLConf.DELTA_IDEMPOTENT_DML_TXN_VERSION.key
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (id LONG) USING delta")

      val log = deltaLogForTable(tableName)
      val versionBeforeTruncate = log.update().version

      val snapshotBefore = captureSnapshot(log, tableName)

      spark.conf.set(txnAppIdKey, appId)
      try {
        // Empty truncate carrying an idempotency token must still commit a
        // SetTransaction so a later replay with the same token is skipped.
        spark.conf.set(txnVersionKey, "1")
        sql(s"TRUNCATE TABLE $tableName")
        assert(log.update().version === versionBeforeTruncate + 1)
        assert(activeFileCount(log) === 0)

        assertSnapshot(log, tableName, snapshotBefore)

        // Insert real data, then replay the truncate with the same token.
        // The replay must be a no-op (data preserved) because the token was
        // already recorded by the empty truncate above.
        spark.conf.unset(txnAppIdKey)
        spark.conf.unset(txnVersionKey)
        sql(s"INSERT INTO $tableName VALUES (1), (2)")
        val versionAfterInsert = log.update().version
        val numFilesAfterInsert = activeFileCount(log)

        val snapshotAfterInsert = captureSnapshot(log, tableName)

        spark.conf.set(txnAppIdKey, appId)
        spark.conf.set(txnVersionKey, "1")
        sql(s"TRUNCATE TABLE $tableName")

        assert(log.update().version === versionAfterInsert)
        assert(activeFileCount(log) === numFilesAfterInsert)
        checkAnswer(sql(s"SELECT id FROM $tableName ORDER BY id"), Seq(Row(1), Row(2)))

        assertSnapshot(log, tableName, snapshotAfterInsert)
      } finally {
        spark.conf.unset(txnAppIdKey)
        spark.conf.unset(txnVersionKey)
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
      val versionBeforeTruncate = log.update().version
      val numFilesBeforeTruncate = activeFileCount(log)

      val snapshotBefore = captureSnapshot(log, tableName)

      val e = intercept[DeltaUnsupportedOperationException] {
        sql(s"TRUNCATE TABLE $tableName")
      }
      checkError(
        e,
        "DELTA_CANNOT_MODIFY_APPEND_ONLY",
        parameters = Map("table_name" -> "null", "config" -> DeltaConfigs.IS_APPEND_ONLY.key))

      checkAnswer(sql(s"SELECT id FROM $tableName ORDER BY id"), Seq(Row(1), Row(2)))
      assert(log.update().version === versionBeforeTruncate)
      assert(activeFileCount(log) === numFilesBeforeTruncate)

      assertSnapshot(log, tableName, snapshotBefore)
    }
  }

  test("truncate partitioned table removes every partition and preserves metadata") {
    val tableName = "truncate_partitioned"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (id LONG, part LONG)
           |USING delta
           |PARTITIONED BY (part)
           |TBLPROPERTIES ('truncateTestProp' = 'preserved')
           |COMMENT 'metadata survives truncate'
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES (0, 0), (1, 1), (2, 0), (3, 1)")

      val log = deltaLogForTable(tableName)
      assert(activeFileCount(log) > 0)

      val snapshotBefore = captureSnapshot(log, tableName)
      val detailsBefore = getDetails(tableName)
      // Sanity-check the table was set up the way this test intends so that the
      // post-truncate "unchanged" assertion is meaningful.
      assert(snapshotBefore.stableDetails.description === "metadata survives truncate")
      assert(snapshotBefore.stableDetails.partitionColumns === Seq("part"))
      assert(snapshotBefore.stableDetails.clusteringColumns === Nil)
      assert(snapshotBefore.stableDetails.properties.get("truncateTestProp").contains("preserved"))
      assert(snapshotBefore.stableProperties.get("truncateTestProp").contains("preserved"))
      assert(detailsBefore.numFiles > 0)
      assert(detailsBefore.sizeInBytes > 0)

      sql(s"TRUNCATE TABLE $tableName")

      checkAnswer(sql(s"SELECT * FROM $tableName"), Nil)
      assert(activeFileCount(log) === 0)

      assertSnapshot(log, tableName, snapshotBefore)
      val detailsAfter = getDetails(tableName)
      assert(detailsAfter.numFiles === 0,
        s"Expected numFiles=0 after truncate, got ${detailsAfter.numFiles}")
      assert(detailsAfter.sizeInBytes === 0,
        s"Expected sizeInBytes=0 after truncate, got ${detailsAfter.sizeInBytes}")
    }
  }

  test("truncate with partition spec is rejected and leaves table unchanged") {
    val tableName = "truncate_partition_spec"
    withTable(tableName) {
      spark.range(start = 0, end = 4, step = 1, numPartitions = 2)
        .selectExpr("id", "id % 2 AS part")
        .write.format("delta").partitionBy("part").saveAsTable(tableName)

      val log = deltaLogForTable(tableName)
      val versionBeforeTruncate = log.update().version
      val numFilesBeforeTruncate = activeFileCount(log)

      val snapshotBefore = captureSnapshot(log, tableName)

      val e = intercept[Exception] {
        sql(s"TRUNCATE TABLE $tableName PARTITION (part = 1)")
      }
      assert(
        Option(e.getMessage).exists { message =>
          val lowerCaseMessage = message.toLowerCase(Locale.ROOT)
          message.contains("DELTA_TRUNCATE_TABLE_PARTITION_NOT_SUPPORTED") ||
            lowerCaseMessage.contains("truncate") && lowerCaseMessage.contains("partition")
        },
        s"Expected partition truncate rejection, got: ${e.getMessage}")

      checkAnswer(
        sql(s"SELECT id, part FROM $tableName ORDER BY id"),
        Seq(Row(0, 0), Row(1, 1), Row(2, 0), Row(3, 1)))
      assert(log.update().version === versionBeforeTruncate)
      assert(activeFileCount(log) === numFilesBeforeTruncate)

      assertSnapshot(log, tableName, snapshotBefore)
    }
  }
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

  /**
   * Combined snapshot of every preserved table attribute, from all three observation angles:
   *   - `state` from the Delta snapshot (protocol versions, reader/writer features,
   *     metadata.configuration, schema, partition columns)
   *   - `stableDetails` from DESCRIBE DETAIL (id, name, description, location, createdAt,
   *     partitionColumns, clusteringColumns, properties, protocol versions, tableFeatures)
   *   - `stableProperties` from SHOW TBLPROPERTIES (catalog-side view), with mutable keys
   *     filtered out (delta.lastCommitTimestamp, delta.lastUpdateVersion, transient_lastDdlTime)
   *
   * `numFiles` / `sizeInBytes` from DESCRIBE DETAIL are intentionally excluded - those
   * legitimately change across a non-empty truncate.
   */
  private final case class TableSnapshot(
      state: TableState,
      stableDetails: StableTableDetails,
      stableProperties: Map[String, String])
}
