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

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaHiveTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.hive.test.TestHiveSingleton

abstract class HiveConvertToDeltaSuiteBase
  extends ConvertToDeltaHiveTableTests
  with DeltaSQLTestUtils {

  override protected def convertToDelta(
      identifier: String,
      partitionSchema: Option[String] = None, collectStats: Boolean = true): Unit = {
    if (partitionSchema.isEmpty) {
      sql(s"convert to delta $identifier ${collectStatisticsStringOption(collectStats)} ")
    } else {
      val stringSchema = partitionSchema.get
      sql(s"convert to delta $identifier ${collectStatisticsStringOption(collectStats)}" +
        s" partitioned by ($stringSchema) ")
    }
  }

  override protected def verifyExternalCatalogMetadata(tableName: String): Unit = {
    val catalogTable = spark.sessionState.catalog.externalCatalog.getTable("default", tableName)
    // Hive automatically adds some properties
    val cleanProps = catalogTable.properties.filterKeys(_ != "transient_lastDdlTime")
    // We can't alter the schema in the catalog at the moment :(
    assert(cleanProps.isEmpty,
      s"Table properties weren't empty for table $tableName: $cleanProps")
  }

  test("convert with statistics") {
    val tbl = "hive_parquet"
      withTable(tbl) {
        sql(
          s"""
             |CREATE TABLE $tbl (id int, str string)
             |PARTITIONED BY (part string)
             |STORED AS PARQUET
         """.stripMargin)

        sql(s"insert into $tbl VALUES (1, 'a', 1)")

        val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tbl))
        convertToDelta(tbl, Some("part string"), collectStats = true)
        val deltaLog = DeltaLog.forTable(spark, catalogTable)
        val statsDf = deltaLog.unsafeVolatileSnapshot.allFiles
          .select(
            from_json(col("stats"), deltaLog.unsafeVolatileSnapshot.statsSchema).as("stats"))
          .select("stats.*")
        assert(statsDf.filter(col("numRecords").isNull).count == 0)
        val history = io.delta.tables.DeltaTable.forPath(catalogTable.location.getPath).history()
        assert(history.count == 1)
      }
  }

  test("convert without statistics") {
    val tbl = "hive_parquet"
    withTable(tbl) {
      sql(
        s"""
           |CREATE TABLE $tbl (id int, str string)
           |PARTITIONED BY (part string)
           |STORED AS PARQUET
         """.stripMargin)

      sql(s"insert into $tbl VALUES (1, 'a', 1)")

      val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tbl))
      convertToDelta(tbl, Some("part string"), collectStats = false)
      val deltaLog = DeltaLog.forTable(spark, catalogTable)
      val statsDf = deltaLog.unsafeVolatileSnapshot.allFiles
        .select(from_json(col("stats"), deltaLog.unsafeVolatileSnapshot.statsSchema).as("stats"))
        .select("stats.*")
      assert(statsDf.filter(col("numRecords").isNotNull).count == 0)
      val history = io.delta.tables.DeltaTable.forPath(catalogTable.location.getPath).history()
      assert(history.count == 1)

    }
  }

  test("convert a Hive based parquet table") {
    val tbl = "hive_parquet"
    withTable(tbl) {
      sql(
        s"""
           |CREATE TABLE $tbl (id int, str string)
           |PARTITIONED BY (part string)
           |STORED AS PARQUET
         """.stripMargin)

      sql(s"insert into $tbl VALUES (1, 'a', 1)")

      val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tbl))
      assert(catalogTable.provider === Some("hive"))
      assert(catalogTable.storage.serde.exists(_.contains("parquet")))

      convertToDelta(tbl, Some("part string"))

      checkAnswer(
        sql(s"select * from delta.`${getPathForTableName(tbl)}`"),
        Row(1, "a", "1"))

      verifyExternalCatalogMetadata(tbl)
      val updatedTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tbl))
      assert(updatedTable.provider === Some("delta"))
    }
  }

  test("convert a Hive based external parquet table") {
    val tbl = "hive_parquet"
    withTempDir { dir =>
      withTable(tbl) {
        sql(
          s"""
             |CREATE EXTERNAL TABLE $tbl (id int, str string)
             |PARTITIONED BY (part string)
             |STORED AS PARQUET
             |LOCATION '${dir.getCanonicalPath}'
         """.stripMargin)
        sql(s"insert into $tbl VALUES (1, 'a', 1)")

        val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tbl))
        assert(catalogTable.provider === Some("hive"))
        assert(catalogTable.storage.serde.exists(_.contains("parquet")))

        convertToDelta(tbl, Some("part string"))

        checkAnswer(
          sql(s"select * from delta.`${dir.getCanonicalPath}`"),
          Row(1, "a", "1"))

        verifyExternalCatalogMetadata(tbl)
        val updatedTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tbl))
        assert(updatedTable.provider === Some("delta"))
      }
    }
  }

  test("negative case: convert empty partitioned parquet table") {
    val tbl = "hive_parquet"
    withTempDir { dir =>
      withTable(tbl) {
        sql(
          s"""
             |CREATE EXTERNAL TABLE $tbl (id int, str string)
             |PARTITIONED BY (part string)
             |STORED AS PARQUET
             |LOCATION '${dir.getCanonicalPath}'
         """.stripMargin)

        val ae = intercept[AnalysisException] {
          convertToDelta(tbl, Some("part string"))
        }

        assert(ae.getErrorClass == "DELTA_CONVERSION_NO_PARTITION_FOUND")
        assert(ae.getSqlState == "42KD6")
        assert(ae.getMessage.contains(tbl))
      }
    }
  }
}

class HiveConvertToDeltaSuite extends HiveConvertToDeltaSuiteBase with DeltaHiveTest
