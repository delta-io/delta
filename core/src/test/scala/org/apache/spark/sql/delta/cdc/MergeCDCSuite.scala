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
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.tables.{DeltaTable => IODeltaTable}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * Tests for MERGE INTO in CDC output mode. In addition to the ones explicitly defined here, we run
 * all the normal merge tests to verify that CDC writing mode doesn't break existing functionality.
 *
 */
class MergeCDCSuite extends MergeIntoSQLSuite with DeltaColumnMappingTestUtils {
  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")

  /**
   * Utility method for simpler test writing when there's at most clause of each type.
   */
  private def testMergeCdc(name: String)(
      target: => DataFrame,
      source: => DataFrame,
      deleteWhen: String = null,
      update: String = null,
      insert: String = null,
      expectedTableData: => DataFrame = null,
      expectedCdcData: => DataFrame = null,
      expectErrorContains: String = null,
      confs: Seq[(String, String)] = Seq()): Unit = {
    val updateClauses = Option(update).map(u => this.update(set = u)).toSeq
    val insertClauses = Option(insert).map(i => this.insert(values = i)).toSeq
    val deleteClauses = Option(deleteWhen).map(d => this.delete(condition = d)).toSeq
    testMergeCdcUnlimitedClauses(name)(
      target, source, deleteClauses ++ updateClauses ++ insertClauses,
      expectedTableData, expectedCdcData, expectErrorContains, confs)
  }

  private def testMergeCdcUnlimitedClauses(name: String)(
      target: => DataFrame,
      source: => DataFrame,
      clauses: Seq[MergeClause],
      expectedTableData: => DataFrame = null,
      expectedCdcData: => DataFrame = null,
      expectErrorContains: String = null,
      confs: Seq[(String, String)] = Seq()): Unit = {
    test(s"merge CDC - $name") {
      withSQLConf(confs: _*) {
        append(target)
        withTempView("source") {
          source.createOrReplaceTempView("source")

          if (expectErrorContains != null) {
            val ex = intercept[Exception] {
              executeMerge(s"delta.`$tempPath` t", s"source s", "s.key = t.key",
                clauses.toSeq: _*)
            }
            assert(ex.getMessage.contains(expectErrorContains))
          } else {
            executeMerge(s"delta.`$tempPath` t", s"source s", "s.key = t.key",
              clauses.toSeq: _*)
            checkAnswer(
              spark.read.format("delta").load(tempPath),
              expectedTableData)
            // The timestamp is nondeterministic so we drop it when comparing results.
            checkAnswer(
              CDCReader.changesToBatchDF(DeltaLog.forTable(spark, tempPath), 1, 1, spark)
                .drop(CDCReader.CDC_COMMIT_TIMESTAMP),
              expectedCdcData)
          }
        }
      }
    }
  }

  testMergeCdc("insert only")(
    target = ((0, 0) :: (1, 10) :: (3, 30) :: Nil).toDF("key", "n"),
    source = ((1, 1) :: (2, 2)  :: Nil).toDF("key", "n"),
    insert = "*",
    expectedTableData = ((0, 0) :: (1, 10) :: (2, 2) :: (3, 30) :: Nil).toDF(),
    expectedCdcData = ((2, 2, "insert", 1) :: Nil).toDF()
  )

  testMergeCdc("update only")(
    target = ((0, 0) :: (1, 10) :: (3, 30) :: Nil).toDF("key", "n"),
    source = ((1, 1) :: (2, 2)  :: Nil).toDF("key", "n"),
    update = "*",
    expectedTableData = ((0, 0) :: (1, 1) :: (3, 30) :: Nil).toDF(),
    expectedCdcData = ((1, 10, "update_preimage", 1) :: (1, 1, "update_postimage", 1) :: Nil).toDF()
  )

  testMergeCdc("delete only")(
    target = ((0, 0) :: (1, 10) :: (3, 30) :: Nil).toDF("key", "n"),
    source = ((1, 1) :: (2, 2)  :: Nil).toDF("key", "n"),
    deleteWhen = "true",
    expectedTableData = ((0, 0) :: (3, 30) :: Nil).toDF(),
    expectedCdcData = ((1, 10, "delete", 1) :: Nil).toDF()
  )

  testMergeCdc("delete only with duplicate matches")(
    target = ((0, 0) :: (1, 10) :: (3, 30) :: Nil).toDF("key", "n"),
    source = ((1, 1) :: (1, 2) :: (2, 3)  :: Nil).toDF("key", "n"),
    deleteWhen = "true",
    expectErrorContains = "attempted to modify the same\ntarget row"
  )

  testMergeCdc("update + delete + insert together")(
    target = ((0, 0) :: (1, 10) :: (3, 30) :: Nil).toDF("key", "n"),
    source = ((1, 1) :: (2, 2) :: (3, -1) :: Nil).toDF("key", "n"),
    insert = "*",
    update = "*",
    deleteWhen = "s.key = 3",
    expectedTableData = ((0, 0) :: (1, 1) :: (2, 2) :: Nil).toDF(),
    expectedCdcData = (
      (2, 2, "insert", 1) ::
        (1, 10, "update_preimage", 1) :: (1, 1, "update_postimage", 1) ::
        (3, 30, "delete", 1) :: Nil).toDF()
  )

  testMergeCdcUnlimitedClauses("unlimited clauses - conditional final branch")(
    target = ((0, 0) :: (1, 10) :: (3, 30) :: (4, 40) :: (6, 60) :: Nil).toDF("key", "n"),
    source = ((1, 1) :: (2, 2) :: (3, -1) :: (4, 4) :: (5, 0) :: (6, 0) :: Nil).toDF("key", "n"),
    clauses =
      update("*", "s.key = 1") :: update("n = 400", "s.key = 4") ::
      delete("s.key = 3") :: delete("s.key = 6") ::
      insert("*", "s.key = 2") :: insert("(key, n) VALUES (50, 50)", "s.key = 5") :: Nil,
    expectedTableData = ((0, 0) :: (1, 1) :: (2, 2) :: (4, 400) :: (50, 50) :: Nil).toDF(),
    expectedCdcData = (
      (2, 2, "insert", 1) :: (50, 50, "insert", 1) ::
        (1, 10, "update_preimage", 1) :: (1, 1, "update_postimage", 1) ::
        (4, 40, "update_preimage", 1) :: (4, 400, "update_postimage", 1) ::
        (3, 30, "delete", 1) :: (6, 60, "delete", 1) :: Nil).toDF()
  )

  testMergeCdcUnlimitedClauses("unlimited clauses - unconditional final branch")(
    target = ((0, 0) :: (1, 10) :: (3, 30) :: (4, 40) :: (6, 60) :: Nil).toDF("key", "n"),
    source = ((1, 1) :: (2, 2) :: (3, -1) :: (4, 4) :: (5, 0) :: (6, 0) :: Nil).toDF("key", "n"),
    clauses =
      update("*", "s.key = 1") :: update("n = 400", "s.key = 4") ::
        delete("s.key = 3") :: delete(condition = null) ::
        insert("*", "s.key = 2") :: insert("(key, n) VALUES (50, 50)", condition = null) :: Nil,
    expectedTableData = ((0, 0) :: (1, 1) :: (2, 2) :: (4, 400) :: (50, 50) :: Nil).toDF(),
    expectedCdcData = (
      (2, 2, "insert", 1) :: (50, 50, "insert", 1) ::
        (1, 10, "update_preimage", 1) :: (1, 1, "update_postimage", 1) ::
        (4, 40, "update_preimage", 1) :: (4, 400, "update_postimage", 1) ::
        (3, 30, "delete", 1) :: (6, 60, "delete", 1) :: Nil).toDF()
  )

  testMergeCdc("basic schema evolution")(
    target = ((0, 0) :: (1, 10) :: (3, 30) :: Nil).toDF("key", "n"),
    source = ((1, 1, "a") :: (2, 2, "b") :: (3, -1, "c") :: Nil).toDF("key", "n", "text"),
    insert = "*",
    update = "*",
    deleteWhen = "s.key = 3",
    expectedTableData = ((0, 0, null) :: (1, 1, "a") :: (2, 2, "b") :: Nil)
      .asInstanceOf[Seq[(Int, Int, String)]].toDF(),
    expectedCdcData = (
        (1, 10, null, "update_preimage", 1) ::
        (1, 1, "a", "update_postimage", 1) ::
        (2, 2, "b", "insert", 1) ::
        (3, 30, null, "delete", 1) :: Nil)
      .asInstanceOf[List[(Integer, Integer, String, String, Integer)]]
      .toDF("key", "targetVal", "srcVal", "_change_type", "_commit_version"),
    confs = (DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key, "true") :: Nil
  )

  testMergeCdcUnlimitedClauses("all conditions failed for all rows")(
    target = Seq((1, "a"), (2, "b")).toDF("key", "val"),
    source = Seq((1, "t"), (2, "u")).toDF("key", "val"),
    clauses =
      update("t.val = s.val", "s.key = 10") :: insert("*", "s.key = 11") :: Nil,
    expectedTableData =
      Seq((1, "a"), (2, "b")).asInstanceOf[List[(Integer, String)]].toDF("key", "targetVal"),
    expectedCdcData =
      Nil.asInstanceOf[List[(Integer, String, String, Integer)]]
      .toDF("key", "targetVal", "_change_type", "_commit_version")
  )

  testMergeCdcUnlimitedClauses("unlimited clauses schema evolution")(
    // 1 and 2 should be updated from the source, 3 and 4 should be deleted. Only 5 is unchanged
    target = Seq((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")).toDF("key", "targetVal"),
    // 1 and 2 should be updated into the target, 6 and 7 should be inserted. 8 should be ignored
    source = Seq((1, "t"), (2, "u"), (3, "v"), (4, "w"), (6, "x"), (7, "y"), (8, "z"))
      .toDF("key", "srcVal"),
    clauses =
      update("targetVal = srcVal", "s.key = 1") :: update("*", "s.key = 2") ::
        delete("s.key = 3") :: delete("s.key = 4") ::
        insert("(key) VALUES (s.key)", "s.key = 6") :: insert("*", "s.key = 7") :: Nil,
    expectedTableData =
      ((1, "t", null) :: (2, "b", "u") :: (5, "e", null) ::
        (6, null, null) :: (7, null, "y") :: Nil)
        .asInstanceOf[List[(Integer, String, String)]].toDF("key", "targetVal", "srcVal"),
    expectedCdcData = (
        (1, "a", null, "update_preimage", 1) ::
        (1, "t", null, "update_postimage", 1) ::
        (2, "b", null, "update_preimage", 1) ::
        (2, "b", "u", "update_postimage", 1) ::
        (3, "c", null, "delete", 1) ::
        (4, "d", null, "delete", 1) ::
        (6, null, null, "insert", 1) ::
        (7, null, "y", "insert", 1) :: Nil)
      .asInstanceOf[List[(Integer, String, String, String, Integer)]]
      .toDF("key", "targetVal", "srcVal", "_change_type", "_commit_version"),
    confs = (DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key, "true") :: Nil
  )
}
