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
  def name: String
  def sourceTable: MergeSourceTable
  def sqlCmd(targetTable: String): String
  def validate(mergeStats: Seq[Row], targetRowCount: Long): Unit
}

trait InsertOnlyTestCase extends MergeTestCase {
    val fileMatchedFraction: Double
    val rowNotMatchedFraction: Double

  override def sourceTable: MergeSourceTable = MergeSourceTable(
    fileMatchedFraction,
    rowMatchedFraction = 0,
    rowNotMatchedFraction)

  override def validate(mergeStats: Seq[Row], targetRowCount: Long): Unit = {
    assert(mergeStats.length == 1)
    assert(mergeStats.head.getAs[Long]("num_updated_rows") == 0)
    assert(mergeStats.head.getAs[Long]("num_deleted_rows") == 0)
  }
}

case class SingleInsertOnlyTestCase(
    fileMatchedFraction: Double,
    rowNotMatchedFraction: Double) extends InsertOnlyTestCase {

  override val name: String = "single_insert_only" +
    s"_fileMatchedFraction_$fileMatchedFraction" +
    s"_rowNotMatchedFraction_$rowNotMatchedFraction"


  override def sqlCmd(targetTable: String): String = {
    s"""MERGE INTO $targetTable t
        |USING ${sourceTable.name} s
        |ON t.wr_order_number = s.wr_order_number AND t.wr_item_sk = s.wr_item_sk
        |WHEN NOT MATCHED THEN INSERT *""".stripMargin
   }
}

case class MultipleInsertOnlyTestCase(
    fileMatchedFraction: Double,
    rowNotMatchedFraction: Double) extends InsertOnlyTestCase {

  override val name: String = "multiple_insert_only" +
    s"_fileMatchedFraction_$fileMatchedFraction" +
    s"_rowNotMatchedFraction_$rowNotMatchedFraction"

  override def sqlCmd(targetTable: String): String = {
    s"""MERGE INTO $targetTable t
        |USING ${sourceTable.name} s
        |ON t.wr_order_number = s.wr_order_number AND t.wr_item_sk = s.wr_item_sk
        |WHEN NOT MATCHED AND s.wr_item_sk % 2 = 0 THEN INSERT *
        |WHEN NOT MATCHED THEN INSERT *""".stripMargin
   }
}

case class DeleteOnlyTestCase(
    fileMatchedFraction: Double,
    rowMatchedFraction: Double) extends MergeTestCase {

  override val name: String = "delete_only" +
    s"_fileMatchedFraction_$fileMatchedFraction" +
    s"_rowMatchedFraction_$rowMatchedFraction"

  override def sourceTable: MergeSourceTable = MergeSourceTable(
    fileMatchedFraction,
    rowMatchedFraction,
    rowNotMatchedFraction = 0)

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

case class UpsertTestCase(
    fileMatchedFraction: Double,
    rowMatchedFraction: Double,
    rowNotMatchedFraction: Double) extends MergeTestCase {

  override val name: String = "upsert" +
    s"_fileMatchedFraction_$fileMatchedFraction" +
    s"_rowMatchedFraction_$rowMatchedFraction" +
    s"_rowNotMatchedFraction_$rowNotMatchedFraction"

  override def sourceTable: MergeSourceTable = MergeSourceTable(
    fileMatchedFraction,
    rowMatchedFraction,
    rowNotMatchedFraction)

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

  def insertOnlyTestCases: Seq[MergeTestCase] = Seq(
    SingleInsertOnlyTestCase(
      fileMatchedFraction = 0.005,
      rowNotMatchedFraction = 0.005),
    SingleInsertOnlyTestCase(
      fileMatchedFraction = 0.005,
      rowNotMatchedFraction = 0.05),
    SingleInsertOnlyTestCase(
      fileMatchedFraction = 0.005,
      rowNotMatchedFraction = 0.5),
    SingleInsertOnlyTestCase(
      fileMatchedFraction = 0.005,
      rowNotMatchedFraction = 1.0),
    SingleInsertOnlyTestCase(
      fileMatchedFraction = 0.1,
      rowNotMatchedFraction = 0.005),

    MultipleInsertOnlyTestCase(
      fileMatchedFraction = 0.005,
      rowNotMatchedFraction = 0.005),
    MultipleInsertOnlyTestCase(
      fileMatchedFraction = 0.005,
      rowNotMatchedFraction = 0.05),
    MultipleInsertOnlyTestCase(
      fileMatchedFraction = 0.005,
      rowNotMatchedFraction = 0.5),
    MultipleInsertOnlyTestCase(
      fileMatchedFraction = 0.005,
      rowNotMatchedFraction = 1.0),
    MultipleInsertOnlyTestCase(
      fileMatchedFraction = 0.1,
      rowNotMatchedFraction = 0.005))

  def deleteOnlyTestCases: Seq[MergeTestCase] = Seq(
    DeleteOnlyTestCase(
      fileMatchedFraction = 0.005,
      rowMatchedFraction = 0.005))

  def upsertTestCases: Seq[MergeTestCase] = Seq(
    UpsertTestCase(
      fileMatchedFraction = 0.005,
      rowMatchedFraction = 0.005,
      rowNotMatchedFraction = 0.005),

    UpsertTestCase(
      fileMatchedFraction = 0.05,
      rowMatchedFraction = 0.1,
      rowNotMatchedFraction = 0.01),

    UpsertTestCase(
      fileMatchedFraction = 0.01,
      rowMatchedFraction = 0.1,
      rowNotMatchedFraction = 0.01),

    UpsertTestCase(
      fileMatchedFraction = 0.05,
      rowMatchedFraction = 0.0,
      rowNotMatchedFraction = 0.1),

    UpsertTestCase(
      fileMatchedFraction = 0.05,
      rowMatchedFraction = 0.01,
      rowNotMatchedFraction = 0.1),

    UpsertTestCase(
      fileMatchedFraction = 0.05,
      rowMatchedFraction = 0.1,
      rowNotMatchedFraction = 0.0),

    UpsertTestCase(
      fileMatchedFraction = 0.5,
      rowMatchedFraction = 0.01,
      rowNotMatchedFraction = 0.001),

    UpsertTestCase(
      fileMatchedFraction = 1.0,
      rowMatchedFraction = 0.01,
      rowNotMatchedFraction = 0.001),

    UpsertTestCase(
      fileMatchedFraction = 0.01,
      rowMatchedFraction = 0.5,
      rowNotMatchedFraction = 0.001),

    UpsertTestCase(
      fileMatchedFraction = 0.05,
      rowMatchedFraction = 0.5,
      rowNotMatchedFraction = 0.001),

    UpsertTestCase(
      fileMatchedFraction = 0.05,
      rowMatchedFraction = 0.99,
      rowNotMatchedFraction = 0.001),

    UpsertTestCase(
      fileMatchedFraction = 0.05,
      rowMatchedFraction = 1.0,
      rowNotMatchedFraction = 0.001))
}
