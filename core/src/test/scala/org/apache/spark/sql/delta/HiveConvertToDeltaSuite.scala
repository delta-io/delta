/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.test.DeltaHiveTest

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

abstract class HiveConvertToDeltaSuiteBase
  extends ConvertToDeltaHiveTableTests
  with SQLTestUtils {

  override protected def convertToDelta(
      identifier: String,
      partitionSchema: Option[String] = None): Unit = {
    if (partitionSchema.isEmpty) {
      sql(s"convert to delta $identifier")
    } else {
      val stringSchema = partitionSchema.get
      sql(s"convert to delta $identifier partitioned by ($stringSchema) ")
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
}

class HiveConvertToDeltaSuite extends HiveConvertToDeltaSuiteBase with DeltaHiveTest
