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
package org.apache.spark.sql.delta.deletionvectors

import java.io.File
import java.lang

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.{DeletionVectorsTestUtils, DeltaLog, DeltaTestUtilsForTempViews}
import org.apache.spark.sql.delta.deletionvectors.DeletionVectorsSuite.{table1Path, table2Path, table3Path}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for testing all write commands are disabled on tables with deletion vectors.
 * This is a temporary behavior until we properly implement and test write support on
 * tables with deletion vectors.
 */
class DisableUpdatesToDvEnabledTablesSuite extends QueryTest
    with SharedSparkSession
    with DeletionVectorsTestUtils
    with DeltaSQLCommandTest
    with DeltaTestUtilsForTempViews {

  import io.delta.implicits._

  test("DELETE is blocked on table with DVs") {
    assertDVTableUpdatesAreDisabled(testTablePath = Some(table1WithDVs)) { _ =>
      spark.sql(s"DELETE FROM $table2WithDVs WHERE value in (2, 5, 7)")
    }
  }

  test("MERGE is blocked on table with DVs") {
    assertDVTableUpdatesAreDisabled(testTablePath = Some(table1WithDVs)) { _ =>
      spark.sql(s"MERGE INTO $table1WithDVs t USING (SELECT * FROM $table2WithDVs) s " +
                    s"ON t.value = s.value WHEN MATCHED THEN DELETE ")
    }
  }

  test("UPDATE is blocked on table with DVs") {
    assertDVTableUpdatesAreDisabled(testTablePath = Some(table1WithDVs)) { _ =>
      spark.sql(s"UPDATE $table2WithDVs SET value = 3 WHERE value > 0")
    }
  }

  test("INSERT INTO is blocked on table with DVs") {
    assertDVTableUpdatesAreDisabled(testTablePath = Some(table1WithDVs)) { _ =>
      spark.sql(s"INSERT INTO $table2WithDVs SELECT 200, 2450")
    }
  }

  test("REPLACE OVERWRITE is blocked on table with DVs") {
    assertDVTableUpdatesAreDisabled(testTablePath = Some(table1WithDVs)) { _ =>
      spark.sql(s"INSERT OVERWRITE $table1WithDVs SELECT * FROM $table2WithDVs")
    }
  }

  test("OPTIMIZE is blocked on table with DVs") {
    assertDVTableUpdatesAreDisabled(testTablePath = Some(table1WithDVs)) { _ =>
      spark.sql(s"OPTIMIZE $table2WithDVs")
    }
  }

  test("RESTORE is blocked on table with DVs") {
    assertDVTableUpdatesAreDisabled(testTablePath = Some(table1WithDVs)) { _ =>
      spark.sql(s"RESTORE $table2WithDVs TO VERSION AS OF 0")
    }
  }

  test("VACUUM is blocked on table with DVs") {
    assertDVTableUpdatesAreDisabled(testTablePath = Some(table1WithDVs)) { _ =>
      withSQLConf(DeltaSQLConf.DELTA_VACUUM_RETENTION_CHECK_ENABLED.key -> "false") {
        spark.sql(s"VACUUM $table1WithDVs RETAIN 0 HOURS")
      }
    }
  }

  test("CLONE is blocked on table with DVs") {
    assertDVTableUpdatesAreDisabled(testTablePath = Some(table2WithDVs)) { tablePath =>
      spark.sql(s"CREATE TABLE delta.`$tablePath` SHALLOW CLONE $table2WithDVs")
    }
  }

  test("CREATE TABLE with DVs is blocked") {
    assertDVTableUpdatesAreDisabled(testTablePath = None) { tablePath =>
      withDeletionVectorsEnabled() {
        createTempTable(tablePath)
      }
    }
  }

  test("Enabling DV feature on a table is blocked") {
    assertDVTableUpdatesAreDisabled(testTablePath = None) { tablePath =>
      createTempTable(tablePath)
      enableDeletionVectorsInTable(new Path(tablePath), enable = true)
    }
  }

  def assertDVTableUpdatesAreDisabled(testTablePath: Option[String])(f: String => Unit): Unit = {
    val dataBefore = testTablePath.map(path => spark.sql(s"SELECT * FROM $path"))
    val ex = intercept[UnsupportedOperationException] {
      withTempPath { path =>
        f(path.getAbsolutePath)
      }
    }
    assert(ex.isInstanceOf[UnsupportedOperationException])
    assert(ex.getMessage.contains(
      "Updates to tables with Deletion Vectors feature enabled are not supported in " +
        "this version of Delta Lake."))

    val dataAfter = testTablePath.map(path => spark.sql(s"SELECT * FROM $path"))
    if (testTablePath.isDefined) {
      checkAnswer(dataAfter.get, dataBefore.get)
    }
  }

  private def createTempTable(path: String): Unit = {
    spark.range(end = 100L).toDF("id").coalesce(1)
      .write.format("delta").mode("overwrite").save(path)
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sessionState.conf.setConf(
      DeltaSQLConf.DELTA_ENABLE_BLOCKING_UPDATES_ON_DV_TABLES, false)
  }

  protected override def afterAll(): Unit = {
    spark.sessionState.conf.setConf(
      DeltaSQLConf.DELTA_ENABLE_BLOCKING_UPDATES_ON_DV_TABLES, true)
    super.afterAll()
  }

  private val table2WithDVs = s"delta.`${new File(table2Path).getAbsolutePath}`"
  private val table1WithDVs = s"delta.`${new File(table1Path).getAbsolutePath}`"
}
