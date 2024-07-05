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

package org.apache.spark.sql.delta.typewidening

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class TypeWideningFeatureCompatibilitySuite
  extends QueryTest
    with DeltaDMLTestUtils
    with TypeWideningTestMixin
    with TypeWideningDropFeatureTestMixin
    with TypeWideningCompatibilityTests
    with TypeWideningColumnMappingTests

/** Tests covering type widening compatibility with other delta features. */
trait TypeWideningCompatibilityTests {
  self: TypeWideningTestMixin with QueryTest with DeltaDMLTestUtils =>

  import testImplicits._

  test("reading CDF with a type change") {
    withSQLConf((DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")) {
      sql(s"CREATE TABLE delta.`$tempPath` (a smallint) USING DELTA")
    }
    append(Seq(1, 2).toDF("a").select($"a".cast(ShortType)))
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE int")
    append(Seq(3, 4).toDF("a"))

    def readCDF(start: Long, end: Long): DataFrame =
      CDCReader
        .changesToBatchDF(deltaLog, start, end, spark)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)
        .drop(CDCReader.CDC_COMMIT_VERSION)

    checkErrorMatchPVals(
      exception = intercept[DeltaUnsupportedOperationException] {
        readCDF(start = 1, end = 1).collect()
      },
      errorClass = "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA",
      parameters = Map(
        "start" -> "1",
        "end" -> "1",
        "readSchema" -> ".*",
        "readVersion" -> "3",
        "incompatibleVersion" -> "1",
        "config" -> ".*defaultSchemaModeForColumnMappingTable"
      )
    )
    checkAnswer(readCDF(start = 3, end = 3), Seq(Row(3, "insert"), Row(4, "insert")))
  }

  test("reading CDF with a type change using read schema from before the change") {
    withSQLConf((DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")) {
      sql(s"CREATE TABLE delta.`$tempPath` (a smallint) USING DELTA")
    }
    append(Seq(1, 2).toDF("a").select($"a".cast(ShortType)))
    val readSchemaSnapshot = deltaLog.update()
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE int")
    append(Seq(3, 4).toDF("a"))

    def readCDF(start: Long, end: Long): DataFrame =
      CDCReader
        .changesToBatchDF(
          deltaLog,
          start,
          end,
          spark,
          readSchemaSnapshot = Some(readSchemaSnapshot)
        )
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)
        .drop(CDCReader.CDC_COMMIT_VERSION)

    checkAnswer(readCDF(start = 1, end = 1), Seq(Row(1, "insert"), Row(2, "insert")))
    checkErrorMatchPVals(
      exception = intercept[DeltaUnsupportedOperationException] {
        readCDF(start = 1, end = 3)
      },
      errorClass = "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE",
      parameters = Map(
        "start" -> "1",
        "end" -> "3",
        "readSchema" -> ".*",
        "readVersion" -> "1",
        "incompatibleVersion" -> "2"
      )
    )
  }

  test("time travel read before type change") {
    sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA")
    append(Seq(1).toDF("a").select($"a".cast(ByteType)))
    sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE smallint")
    append(Seq(2).toDF("a").select($"a".cast(ShortType)))

    val previousVersion = sql(s"SELECT a FROM delta.`$tempPath` VERSION AS OF 1")
    assert(previousVersion.schema("a").dataType === ByteType)
    checkAnswer(previousVersion, Seq(Row(1)))

    val latestVersion = sql(s"SELECT a FROM delta.`$tempPath`")
    assert(latestVersion.schema("a").dataType === ShortType)
    checkAnswer(latestVersion, Seq(Row(1), Row(2)))
  }
}

/** Trait collecting tests covering type widening + column mapping. */
trait TypeWideningColumnMappingTests {
    self: QueryTest
    with TypeWideningTestMixin
    with TypeWideningDropFeatureTestMixin =>

  import testImplicits._

  for (mappingMode <- Seq(IdMapping.name, NameMapping.name)) {
    test(s"change column type and rename it, mappingMode=$mappingMode") {
      withSQLConf((DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey, mappingMode)) {
        sql(s"CREATE TABLE delta.`$tempPath` (a byte) USING DELTA")
      }
      // Add some data and change type of column `a`.
      addSingleFile(Seq(1), ByteType)
      sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN a TYPE smallint")
      addSingleFile(Seq(2), ShortType)
      assert(readDeltaTable(tempPath).schema("a").dataType === ShortType)
      checkAnswer(sql(s"SELECT a FROM delta.`$tempPath`"), Seq(Row(1), Row(2)))

      // Rename column `a` to `a (with reserved characters)`, add more data.
      val newColumnName = "a (with reserved characters)"
      sql(s"ALTER TABLE delta.`$tempPath` RENAME COLUMN a TO `$newColumnName`")
      assert(readDeltaTable(tempPath).schema(newColumnName).dataType === ShortType)
      checkAnswer(
        sql(s"SELECT `$newColumnName` FROM delta.`$tempPath`"), Seq(Row(1), Row(2))
      )
      append(Seq(3).toDF(newColumnName).select(col(newColumnName).cast(ShortType)))
      checkAnswer(
        sql(s"SELECT `$newColumnName` FROM delta.`$tempPath`"), Seq(Row(1), Row(2), Row(3))
      )

      // Change column type again, add more data.
      sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN `$newColumnName` TYPE int")
      assert(
        readDeltaTable(tempPath).schema(newColumnName).dataType === IntegerType)
      append(Seq(4).toDF(newColumnName).select(col(newColumnName).cast(IntegerType)))
      checkAnswer(
        sql(s"SELECT `$newColumnName` FROM delta.`$tempPath`"),
        Seq(Row(1), Row(2), Row(3), Row(4))
      )

      dropTableFeature(
        expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
        // All files except the last one should be rewritten.
        expectedNumFilesRewritten = 3,
        expectedColumnTypes = Map(newColumnName -> IntegerType)
      )
    }

    test(s"dropped column shouldn't cause files to be rewritten, mappingMode=$mappingMode") {
      withSQLConf((DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey, mappingMode)) {
        sql(s"CREATE TABLE delta.`$tempPath` (a byte, b byte) USING DELTA")
      }
      sql(s"INSERT INTO delta.`$tempPath` VALUES (1, 1)")
      sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN b TYPE int")
      sql(s"INSERT INTO delta.`$tempPath` VALUES (2, 2)")
      sql(s"ALTER TABLE delta.`$tempPath` DROP COLUMN b")
      dropTableFeature(
        expectedOutcome = ExpectedOutcome.FAIL_HISTORICAL_VERSION_USES_FEATURE,
        expectedNumFilesRewritten = 0,
        expectedColumnTypes = Map("a" -> ByteType)
      )
    }

    test(s"swap column names and change type, mappingMode=$mappingMode") {
      withSQLConf((DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey, mappingMode)) {
        sql(s"CREATE TABLE delta.`$tempPath` (a byte, b byte) USING DELTA")
      }
      sql(s"INSERT INTO delta.`$tempPath` VALUES (1, 1)")
      sql(s"ALTER TABLE delta.`$tempPath` CHANGE COLUMN b TYPE int")
      sql(s"INSERT INTO delta.`$tempPath` VALUES (2, 2)")
      sql(s"ALTER TABLE delta.`$tempPath` RENAME COLUMN b TO c")
      sql(s"ALTER TABLE delta.`$tempPath` RENAME COLUMN a TO b")
      sql(s"ALTER TABLE delta.`$tempPath` RENAME COLUMN c TO a")
      sql(s"INSERT INTO delta.`$tempPath` VALUES (3, 3)")
      dropTableFeature(
        expectedOutcome = ExpectedOutcome.FAIL_CURRENT_VERSION_USES_FEATURE,
        expectedNumFilesRewritten = 1,
        expectedColumnTypes = Map(
          "a" -> IntegerType,
          "b" -> ByteType
        )
      )
    }
  }
}
