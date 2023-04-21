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

package org.apache.spark.sql.delta.rowid

import org.apache.spark.sql.delta.{DeltaLog, RowId}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class RowIdSuite extends QueryTest
    with SharedSparkSession
    with RowIdTestUtils {
  test("Creating a new table with row ID table feature sets row IDs as readable") {
    withRowIdsEnabled(enabled = false) {
      withTable("tbl") {
        spark.range(10).write.format("delta")
          .option(rowIdFeatureName, "supported").saveAsTable("tbl")

        val log = DeltaLog.forTable(spark, TableIdentifier("tbl"))
        assert(RowId.rowIdsEnabled(log.update().protocol, log.update().metadata))
      }
    }
  }

  test("Enabling row IDs on existing table does not set row IDs as readable") {
    withRowIdsEnabled(enabled = false) {
      withTable("tbl") {
        spark.range(10).write.format("delta")
          .saveAsTable("tbl")

        sql(
          s"""
             |ALTER TABLE tbl
             |SET TBLPROPERTIES (
             |'$rowIdFeatureName' = 'supported',
             |'delta.minWriterVersion' = $TABLE_FEATURES_MIN_WRITER_VERSION)""".stripMargin)

        val log = DeltaLog.forTable(spark, TableIdentifier("tbl"))
        assert(RowId.rowIdsSupported(log.update().protocol))
        assert(!RowId.rowIdsEnabled(log.update().protocol, log.update().metadata))
      }
    }
  }
}
