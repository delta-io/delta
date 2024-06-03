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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.iceberg

class UniformIngressTableSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  import testImplicits._

  /** Helper method to get the location of the metadata of the specific iceberg table */
  private def metadataLoc(table: iceberg.Table, metadataVer: Int): String =
    table.location() + s"/metadata/v$metadataVer.metadata.json"

  test("Create Uniform Ingress Table Test") {
    withTempDir { dir =>
      val icebergTableName = "iceberg_table_1"
      val seq1 = Seq((1, "Alex", 23), (2, "Michael", 21))
      val df1 = seq1.toDF("id", "name", "age")
      val icebergTable = IcebergTestUtils.createIcebergTable(
        spark, dir.getPath, icebergTableName, df1, Seq("id")
      )
      val targetTable = "ufi_table_1"
      withTable(targetTable) {
        spark.sql(
          s"""
            | CREATE TABLE $targetTable
            | UNIFORM iceberg
            | METADATA_PATH '${metadataLoc(icebergTable, 2)}'
            |""".stripMargin)
        val answer = spark.sql(s"SELECT * FROM default.$targetTable")
        checkAnswer(answer, df1.collect())
      }
    }
  }

  test("Refresh Uniform Ingress Table Test") {
    withTempDir { dir =>
      val icebergTableName = "iceberg_table_2"
      val seq1 = Seq((1, "Alex", 23), (2, "Michael", 21))
      val df1 = seq1.toDF("id", "name", "age")
      val icebergTable = IcebergTestUtils.createIcebergTable(
        spark, dir.getPath, icebergTableName, df1, Seq("id")
      )
      val targetTable = "ufi_table_2"
      withTable(targetTable) {
        spark.sql(
          s"""
             | CREATE TABLE $targetTable
             | UNIFORM iceberg
             | METADATA_PATH '${metadataLoc(icebergTable, 2)}'
             |""".stripMargin)
        val answer1 = spark.sql(s"SELECT * FROM default.$targetTable")
        checkAnswer(answer1, df1.collect())

        // write two more rows to the existing iceberg table
        val seq2 = Seq((3, "Cat", 123), (4, "Dog", 456))
        val df2 = seq2.toDF("id", "name", "age")
        IcebergTestUtils.writeIcebergTable(icebergTable, df2)

        spark.sql(
          s"""
             | REFRESH TABLE $targetTable
             | METADATA_PATH '${metadataLoc(icebergTable, 3)}'
             |""".stripMargin
        )
        val answer2 = spark.sql(s"SELECT * FROM default.$targetTable")
        checkAnswer(answer2, df1.collect() ++ df2.collect())
      }
    }
  }
}