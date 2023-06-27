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

package benchmark

import org.apache.spark.sql.Row

trait MergeTestCase {
  /**
   * Name of the test case used e.p. in the test results.
   */
  def name: String

  /**
   * The source table configuration to use for the test case. When a test case is defined,
   * [[MergeDataLoad]] will collect all source table configuration and create the source tables
   * required by all tests.
   */
  def sourceTable: MergeSourceTable

  /**
   * The merge command to execute as a SQL string.
   */
  def sqlCmd(targetTable: String): String

  /**
   * Each test case can define invariants to check after the merge command runs to ensure that the
   * benchmark results are valid.
   */
  def validate(mergeStats: Seq[Row], targetRowCount: Long): Unit
}

/**
 * Trait shared by all insert-only merge test cases.
 */
trait InsertOnlyTestCase extends MergeTestCase {
    val filesMatchedFraction: Double
    val rowsNotMatchedFraction: Double

  override def sourceTable: MergeSourceTable = MergeSourceTable(
    filesMatchedFraction,
    rowsMatchedFraction = 0,
    rowsNotMatchedFraction)

  override def validate(mergeStats: Seq[Row], targetRowCount: Long): Unit = {
    assert(mergeStats.length == 1)
    assert(mergeStats.head.getAs[Long]("num_updated_rows") == 0)
    assert(mergeStats.head.getAs[Long]("num_deleted_rows") == 0)
  }
}

/**
 * A merge test case with a single WHEN NOT MATCHED THEN INSERT * clause.
 */
case class SingleInsertOnlyTestCase(
    filesMatchedFraction: Double,
    rowsNotMatchedFraction: Double) extends InsertOnlyTestCase {

  override val name: String = "single_insert_only" +
    s"_filesMatchedFraction_$filesMatchedFraction" +
    s"_rowsNotMatchedFraction_$rowsNotMatchedFraction"


  override def sqlCmd(targetTable: String): String = {
    s"""MERGE INTO $targetTable t
        |USING ${sourceTable.name} s
        |ON t.wr_order_number = s.wr_order_number AND t.wr_item_sk = s.wr_item_sk
        |WHEN NOT MATCHED THEN INSERT *""".stripMargin
   }
}

/**
 * A merge test case with two WHEN NOT MATCHED (AND condition) THEN INSERT * clauses.
 */
case class MultipleInsertOnlyTestCase(
    filesMatchedFraction: Double,
    rowsNotMatchedFraction: Double) extends InsertOnlyTestCase {

  override val name: String = "multiple_insert_only" +
    s"_filesMatchedFraction_$filesMatchedFraction" +
    s"_rowsNotMatchedFraction_$rowsNotMatchedFraction"

  override def sqlCmd(targetTable: String): String = {
    s"""MERGE INTO $targetTable t
        |USING ${sourceTable.name} s
        |ON t.wr_order_number = s.wr_order_number AND t.wr_item_sk = s.wr_item_sk
        |WHEN NOT MATCHED AND s.wr_item_sk % 2 = 0 THEN INSERT *
        |WHEN NOT MATCHED THEN INSERT *""".stripMargin
   }
}

/**
 * A merge test case with a single WHEN MATCHED THEN DELETED clause.
 */
case class DeleteOnlyTestCase(
    filesMatchedFraction: Double,
    rowsMatchedFraction: Double) extends MergeTestCase {

  override val name: String = "delete_only" +
    s"_filesMatchedFraction_$filesMatchedFraction" +
    s"_rowsMatchedFraction_$rowsMatchedFraction"

  override def sourceTable: MergeSourceTable = MergeSourceTable(
    filesMatchedFraction,
    rowsMatchedFraction,
    rowsNotMatchedFraction = 0)

  override def sqlCmd(targetTable: String): String = {
    s"""MERGE INTO $targetTable t
        |USING ${sourceTable.name} s
        |ON t.wr_order_number = s.wr_order_number AND t.wr_item_sk = s.wr_item_sk
        |WHEN MATCHED THEN DELETE""".stripMargin
   }

  override def validate(mergeStats: Seq[Row], targetRowCount: Long): Unit = {
    assert(mergeStats.length == 1)
    assert(mergeStats.head.getAs[Long]("num_updated_rows") == 0)
    assert(mergeStats.head.getAs[Long]("num_inserted_rows") == 0)
  }
}

/**
 * A merge test case with a MATCHED UPDATE and a NOT MATCHED INSERT clause.
 */
case class UpsertTestCase(
    filesMatchedFraction: Double,
    rowsMatchedFraction: Double,
    rowsNotMatchedFraction: Double) extends MergeTestCase {

  override val name: String = "upsert" +
    s"_filesMatchedFraction_$filesMatchedFraction" +
    s"_rowsMatchedFraction_$rowsMatchedFraction" +
    s"_rowsNotMatchedFraction_$rowsNotMatchedFraction"

  override def sourceTable: MergeSourceTable = MergeSourceTable(
    filesMatchedFraction,
    rowsMatchedFraction,
    rowsNotMatchedFraction)

  override def sqlCmd(targetTable: String): String = {
    s"""MERGE INTO $targetTable t
        |USING ${sourceTable.name} s
        |ON t.wr_order_number = s.wr_order_number AND t.wr_item_sk = s.wr_item_sk
        |WHEN MATCHED THEN UPDATE SET *
        |WHEN NOT MATCHED THEN INSERT *""".stripMargin
   }

  override def validate(mergeStats: Seq[Row], targetRowCount: Long): Unit = {
    assert(mergeStats.length == 1)
    assert(mergeStats.head.getAs[Long]("num_deleted_rows") == 0)
  }
}

object MergeTestCases {
  def testCases: Seq[MergeTestCase] =
    insertOnlyTestCases ++
    deleteOnlyTestCases ++
    upsertTestCases

  def insertOnlyTestCases: Seq[MergeTestCase] =
    Seq(0.05, 0.5, 1.0).flatMap { rowsNotMatchedFraction =>
      Seq(
        SingleInsertOnlyTestCase(
          filesMatchedFraction = 0.05,
          rowsNotMatchedFraction),

        MultipleInsertOnlyTestCase(
          filesMatchedFraction = 0.05,
          rowsNotMatchedFraction)
      )
    }

  def deleteOnlyTestCases: Seq[MergeTestCase] = Seq(
    DeleteOnlyTestCase(
      filesMatchedFraction = 0.05,
      rowsMatchedFraction = 0.05))

  def upsertTestCases: Seq[MergeTestCase] = Seq(
    Seq(0.0, 0.01, 0.1).map { rowsMatchedFraction =>
      UpsertTestCase(
        filesMatchedFraction = 0.05,
        rowsMatchedFraction,
        rowsNotMatchedFraction = 0.1)
    },

    Seq(0.5, 0.99, 1.0).map { rowsMatchedFraction =>
      UpsertTestCase(
        filesMatchedFraction = 0.05,
        rowsMatchedFraction,
        rowsNotMatchedFraction = 0.001)
    },

    Seq(
      UpsertTestCase(
        filesMatchedFraction = 0.05,
        rowsMatchedFraction = 0.1,
        rowsNotMatchedFraction = 0.0),

      UpsertTestCase(
        filesMatchedFraction = 0.5,
        rowsMatchedFraction = 0.01,
        rowsNotMatchedFraction = 0.001),

      UpsertTestCase(
        filesMatchedFraction = 1.0,
        rowsMatchedFraction = 0.01,
        rowsNotMatchedFraction = 0.001)
    )
  ).flatten
}
