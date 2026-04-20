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

package org.apache.spark.sql.delta.uniform

import org.apache.spark.sql.delta.{DeltaLog, UniversalFormat}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests verifying that the distributed Iceberg conversion configuration integrates
 * correctly with Delta table operations. The distributed conversion path is exercised
 * at the unit level in [[DistributedIcebergConversionSuite]]; these tests verify
 * that the configuration flags, table creation, and data operations work correctly
 * when the distributed conversion feature is enabled.
 *
 * Note: In OSS Delta, the IcebergConverter.convertSnapshot is not wired up
 * (the post-commit hook is never registered). Full E2E Iceberg metadata verification
 * requires the proprietary conversion wiring.
 */
class DistributedIcebergConversionE2ESuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  test("tables with distributed conversion config can be created and queried") {
    withSQLConf(
      DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_THRESHOLD.key -> "1"
    ) {
      val tableName = "dist_config_table"
      withTable(tableName) {
        sql(
          s"""CREATE TABLE $tableName (col1 INT) USING DELTA
             |TBLPROPERTIES (
             |  'delta.columnMapping.mode' = 'name',
             |  'delta.enableIcebergCompatV2' = 'true',
             |  'delta.universalFormat.enabledFormats' = 'iceberg',
             |  'delta.enableDeletionVectors' = 'false'
             |)""".stripMargin)
        sql(s"INSERT INTO $tableName VALUES (1), (2), (3), (4), (5)")

        checkAnswer(
          spark.sql(s"SELECT col1 FROM $tableName ORDER BY col1"),
          Seq(Row(1), Row(2), Row(3), Row(4), Row(5)))

        // Verify UniForm is enabled in Delta metadata
        val snapshot = DeltaLog.forTable(spark, TableIdentifier(tableName)).update()
        assert(UniversalFormat.icebergEnabled(snapshot.metadata),
          "UniForm Iceberg should be enabled")
      }
    }
  }

  test("distributed config does not affect Delta data correctness") {
    val distTable = "dist_data"
    val normalTable = "normal_data"

    withTable(distTable, normalTable) {
      // Table with distributed conversion enabled
      withSQLConf(
        DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_THRESHOLD.key -> "1"
      ) {
        sql(
          s"""CREATE TABLE $distTable (id INT, name STRING) USING DELTA
             |TBLPROPERTIES (
             |  'delta.columnMapping.mode' = 'name',
             |  'delta.enableIcebergCompatV2' = 'true',
             |  'delta.universalFormat.enabledFormats' = 'iceberg',
             |  'delta.enableDeletionVectors' = 'false'
             |)""".stripMargin)
        sql(s"INSERT INTO $distTable VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
      }

      // Table without distributed conversion
      withSQLConf(
        DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_ENABLED.key -> "false"
      ) {
        sql(
          s"""CREATE TABLE $normalTable (id INT, name STRING) USING DELTA
             |TBLPROPERTIES (
             |  'delta.columnMapping.mode' = 'name',
             |  'delta.enableIcebergCompatV2' = 'true',
             |  'delta.universalFormat.enabledFormats' = 'iceberg',
             |  'delta.enableDeletionVectors' = 'false'
             |)""".stripMargin)
        sql(s"INSERT INTO $normalTable VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
      }

      // Both should produce identical Delta data
      val expectedRows = Seq(Row(1, "alice"), Row(2, "bob"), Row(3, "carol"))
      checkAnswer(spark.sql(s"SELECT id, name FROM $distTable ORDER BY id"), expectedRows)
      checkAnswer(spark.sql(s"SELECT id, name FROM $normalTable ORDER BY id"), expectedRows)

      // Both should have identical file counts
      val distLog = DeltaLog.forTable(spark, TableIdentifier(distTable))
      val normalLog = DeltaLog.forTable(spark, TableIdentifier(normalTable))
      assert(distLog.update().numOfFiles === normalLog.update().numOfFiles)
    }
  }

  test("partitioned table with distributed config") {
    withSQLConf(
      DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_THRESHOLD.key -> "1"
    ) {
      val tableName = "dist_partitioned"
      withTable(tableName) {
        sql(
          s"""CREATE TABLE $tableName (id INT, category STRING, value DOUBLE) USING DELTA
             |PARTITIONED BY (category)
             |TBLPROPERTIES (
             |  'delta.columnMapping.mode' = 'name',
             |  'delta.enableIcebergCompatV2' = 'true',
             |  'delta.universalFormat.enabledFormats' = 'iceberg',
             |  'delta.enableDeletionVectors' = 'false'
             |)""".stripMargin)
        sql(
          s"""INSERT INTO $tableName VALUES
             |(1, 'A', 10.0), (2, 'B', 20.0), (3, 'A', 30.0),
             |(4, 'C', 40.0), (5, 'B', 50.0)""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT id, category, value FROM $tableName ORDER BY id"),
          Seq(
            Row(1, "A", 10.0), Row(2, "B", 20.0), Row(3, "A", 30.0),
            Row(4, "C", 40.0), Row(5, "B", 50.0)))

        val snapshot = DeltaLog.forTable(spark, TableIdentifier(tableName)).update()
        assert(UniversalFormat.icebergEnabled(snapshot.metadata))
      }
    }
  }

  test("multiple inserts with distributed config") {
    withSQLConf(
      DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_THRESHOLD.key -> "1"
    ) {
      val tableName = "dist_multi"
      withTable(tableName) {
        sql(
          s"""CREATE TABLE $tableName (col1 INT) USING DELTA
             |TBLPROPERTIES (
             |  'delta.columnMapping.mode' = 'name',
             |  'delta.enableIcebergCompatV2' = 'true',
             |  'delta.universalFormat.enabledFormats' = 'iceberg',
             |  'delta.enableDeletionVectors' = 'false'
             |)""".stripMargin)

        sql(s"INSERT INTO $tableName VALUES (1), (2)")
        sql(s"INSERT INTO $tableName VALUES (3), (4)")
        sql(s"INSERT INTO $tableName VALUES (5), (6)")

        checkAnswer(
          spark.sql(s"SELECT col1 FROM $tableName ORDER BY col1"),
          Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(6)))

        val snapshot = DeltaLog.forTable(spark, TableIdentifier(tableName)).update()
        assert(snapshot.numOfFiles >= 3, "Should have at least 3 files from 3 inserts")
      }
    }
  }

  test("CTAS with distributed config") {
    withSQLConf(
      DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_UNIFORM_ICEBERG_DISTRIBUTED_CONVERSION_THRESHOLD.key -> "1"
    ) {
      val tableName = "dist_ctas"
      withTable(tableName, "source_table") {
        sql("CREATE TABLE source_table (col1 INT) USING DELTA")
        sql("INSERT INTO source_table VALUES (10), (20), (30)")
        sql(
          s"""CREATE TABLE $tableName USING DELTA
             |TBLPROPERTIES (
             |  'delta.columnMapping.mode' = 'name',
             |  'delta.enableIcebergCompatV2' = 'true',
             |  'delta.universalFormat.enabledFormats' = 'iceberg',
             |  'delta.enableDeletionVectors' = 'false'
             |) AS SELECT col1 FROM source_table""".stripMargin)

        checkAnswer(
          spark.sql(s"SELECT col1 FROM $tableName ORDER BY col1"),
          Seq(Row(10), Row(20), Row(30)))

        val snapshot = DeltaLog.forTable(spark, TableIdentifier(tableName)).update()
        assert(UniversalFormat.icebergEnabled(snapshot.metadata))
      }
    }
  }
}
