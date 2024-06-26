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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql._
import org.apache.iceberg
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.delta.uniform.UniformIngressUtils

class UniformIngressTableSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with ConvertIcebergToDeltaSuiteBase {

  import testImplicits._

  /** Helper method to get the location of the metadata of the specific iceberg table */
  @deprecated
  private def metadataLoc(table: iceberg.Table, metadataVer: Int): String =
    table.location() + s"/metadata/v$metadataVer.metadata.json"

  /**
   * Get the location of metadata version.
   * Please note that the table location is fixed under the current
   * test framework, which will be stored at `tablePath`.
   */
  private def metadataLoc(metadataVer: Int): String =
    tablePath + s"/metadata/v$metadataVer.metadata.json"

  /**
   * Create a new delta UFI table based on the provided `icebergMetadataLoc` and `targetData`,
   * then validate if the `targetData` in the iceberg table could be correctly read by the
   * new delta table.
   *
   * @param icebergMetadataLoc the actual metadata location for the iceberg table.
   * @param targetTableName the name for the delta UFI table.
   * @param targetData the data that has been written to iceberg table, used to validate the read.
   */
  private def checkReadingIcebergAsDelta(
      icebergMetadataLoc: String,
      targetTableName: String,
      targetData: Seq[Row],
      cols: String): Unit = {
    sql(
      s"""
         | CREATE TABLE $targetTableName
         | UNIFORM iceberg
         | METADATA_PATH '$icebergMetadataLoc'
         |""".stripMargin
    )
    val result = sql(s"SELECT $cols from default.$targetTableName")
    checkAnswer(result, targetData)

    val tableIdent = new TableIdentifier(targetTableName)
    val catalogTable = spark.sessionState.catalog.getTableMetadata(tableIdent)

    assert(catalogTable.tableType == CatalogTableType.EXTERNAL)
    assert(UniformIngressUtils.isUniformIngressTable(catalogTable))
    assert(catalogTable.location.toString.contains("/db/table"))

    // TODO: ensure the correct storage path for the metadata location
    // assert(icebergMetadataLoc.contains(catalogTable.storage.locationUri.get.toString))
  }

  /**
   * Refresh an existing delta UFI table based on the provided
   * `icebergMetadataLoc` and `targetData`.
   * Then validate if the `targetData` in the iceberg table could be correctly read by the
   * new delta table.
   *
   * @param icebergMetadataLoc the actual metadata location for the iceberg table.
   * @param targetTableName the name for the delta UFI table.
   * @param targetData the data that has been written to iceberg table, used to validate the read.
   */
  private def checkRefreshingIcebergAsDelta(
      icebergMetadataLoc: String,
      targetTableName: String,
      targetData: Seq[Row],
      cols: String): Unit = {
    sql(
      s"""
         | REFRESH TABLE $targetTableName
         | METADATA_PATH '$icebergMetadataLoc'
         |""".stripMargin
    )
    val result = sql(s"SELECT $cols from default.$targetTableName")
    checkAnswer(result, targetData)

    val tableIdent = new TableIdentifier(targetTableName)
    val catalogTable = spark.sessionState.catalog.getTableMetadata(tableIdent)

    assert(catalogTable.tableType == CatalogTableType.EXTERNAL)
    assert(UniformIngressUtils.isUniformIngressTable(catalogTable))
    assert(catalogTable.location.toString.contains("/db/table"))

    // TODO: same as above, ensure the correct storage path
    // assert(catalogTable.storage.locationUri.get.toString == icebergMetadataLoc)
  }

  test("Create Uniform Ingress Table Test") {
    withTable(table) {
      IcebergTestUtils.createIcebergTable(
        name = table,
        schema = "id bigint, name string",
        partition = Some("id")
      )

      // write two rows to the newly created iceberg table at first
      val df1 = Seq((1, "Alex"), (2, "Michael")).toDF("id", "name")
      IcebergTestUtils.writeIcebergTable(
        name = table,
        rows = df1.collect()
      )

      val deltaTable1 = "ufi_table_1"
      withTable(deltaTable1) {
        // check the CREATE UFI Table command's result
        checkReadingIcebergAsDelta(metadataLoc(2), deltaTable1, df1.collect(), "id, name")

        // write two more rows to the existing iceberg table
        val df2 = Seq((3, "Cat"), (4, "Cat")).toDF("id", "name")
        IcebergTestUtils.writeIcebergTable(
          name = table,
          rows = df2.collect()
        )

        val deltaTable2 = "ufi_table_2"
        withTable(deltaTable2) {
          val targetData = df1.collect() ++ df2.collect()
          checkReadingIcebergAsDelta(metadataLoc(3), deltaTable2, targetData, "id, name")
        }
      }
    }
  }

  test("Refresh Uniform Ingress Table Test") {
    withTable(table) {
      IcebergTestUtils.createIcebergTable(
        name = table,
        schema = "id bigint, name string, age int",
        partition = Some("id")
      )

      // write two rows to the newly created iceberg table at first
      val df1 = Seq((1, "Alex", 23), (2, "Michael", 21)).toDF("id", "name", "age")
      IcebergTestUtils.writeIcebergTable(
        name = table,
        rows = df1.collect()
      )

      val deltaTable1 = "ufi_table_1"
      withTable(deltaTable1) {
        // check the CREATE UFI Table command's result
        checkReadingIcebergAsDelta(metadataLoc(2), deltaTable1, df1.collect(), "id, name, age")

        // write two more rows to the existing iceberg table
        val df2 = Seq((3, "Cat", 33), (4, "Cat", 44)).toDF("id", "name", "age")
        IcebergTestUtils.writeIcebergTable(
          name = table,
          rows = df2.collect()
        )

        // check the UFI table's content after triggering the REFRESH command
        val targetData = df1.collect() ++ df2.collect()
        checkRefreshingIcebergAsDelta(metadataLoc(3), deltaTable1, targetData, "id, name, age")
      }
    }
  }

  test("Refresh Uniform Ingress Table for Deleting/Dropping Column") {
    withTable(table) {
      IcebergTestUtils.createIcebergTable(
        name = table,
        schema = "id bigint, name string, age int",
        partition = Some("id")
      )

      val df1 = Seq((1, "Alex", 23), (2, "Michael", 21)).toDF("id", "name", "age")
      IcebergTestUtils.writeIcebergTable(
        name = table,
        rows = df1.collect()
      )

      val deltaTable1 = "ufi_table_1"
      withTable(deltaTable1) {
        checkReadingIcebergAsDelta(metadataLoc(2), deltaTable1, df1.collect(), "id, name, age")

        // delete the column for `age: int` in the existing iceberg table
        // now the schema is `id: bigint, name: string`
        spark.sql(s"ALTER TABLE $table DROP COLUMN age")

        // the new dataframe should not contain the previous
        // `age: int` column.
        val df2 = Seq((1, "Alex"), (2, "Michael")).toDF("id", "name")
        // check the columns with data after REFRESH command is triggered
        checkRefreshingIcebergAsDelta(metadataLoc(3), deltaTable1, df2.collect(), "id, name")
      }
    }
  }

  /**
   * Dummy override method of convert.
   * This is needed to successfully use the existing test framework.
   */
  override protected def convert(
      tableIdentifier: String,
      partitioning: Option[String] = None,
      collectStats: Boolean = true): Unit = {
    val statement = partitioning.map(p => s" PARTITIONED BY ($p)").getOrElse("")
    spark.sql(s"CONVERT TO DELTA ${tableIdentifier}${statement} " +
      s"${collectStatisticsStringOption(collectStats)}")
  }
}