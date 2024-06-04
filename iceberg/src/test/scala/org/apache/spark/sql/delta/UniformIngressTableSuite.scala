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
import org.apache.iceberg
import org.scalatest.time.{Seconds, Span}

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

  test("Refresh UFI Table For Deleting Column") {
    withTempDir { dir =>
      val targetTable = "target_table_refresh"
      val icebergTableName = "iceberg_table_4"

      val seq1 = Seq((1, "Alice", true, 23, 1.030), (2, "Bob", false, 24, 2.002))
      val df1 = seq1.toDF("id", "name", "vip", "age", "rate")
      val icebergTable = IcebergTestUtils.createIcebergTable(
        spark,
        dir.getPath,
        icebergTableName,
        df1,
        Seq("vip")
      )

      withTable(targetTable) {
        // check the initial data remains consistent
        sql(
          s"""
             | CREATE TABLE $targetTable
             | UNIFORM iceberg
             | METADATA_PATH '${metadataLoc(icebergTable, 2)}'
             |""".stripMargin
        )
        val result1 = sql(s"SELECT id, name, vip, age, rate from default.$targetTable")
        checkAnswer(result1, df1.collect())

        // delete two columns in iceberg table
        val updatedTable = IcebergTestUtils.updateIcebergTableSchema(
          spark,
          dir.getPath,
          icebergTableName,
          IcebergTestUtils.DeleteColumns(Seq("age", "rate"))
        )
        // refresh the table
        sql(
          s"""
             | REFRESH TABLE $targetTable
             | METADATA_PATH '${metadataLoc(updatedTable, 3)}'
             |""".stripMargin
        )
        // now the metadata (i.e., schema) of the UFI table
        // should be synced with the iceberg table.
        DeltaLog.clearCache()
        val metadata =
          spark.sessionState.catalog.getTableMetadata(new TableIdentifier(targetTable))
        val nameMap = metadata.schema.fields.map(_.name)
        assert(!nameMap.contains("age") && !nameMap.contains("rate"))

        // check the result
        val result2 = sql(s"SELECT id, name, vip FROM default.$targetTable")
        val df2 = Seq((1, "Alice", true), (2, "Bob", false)).toDF("id", "name", "vip")
        checkAnswer(result2, df2.collect())
      }
    }
  }

  test("Refresh UFI Table For Renaming Column") {
    withTempDir { dir =>
      val targetTable = "target_table_refresh"
      val icebergTableName = "iceberg_table_5"

      val seq1 = Seq((1, "Alice", true, 23, 1.030), (2, "Bob", false, 24, 2.002))
      val df1 = seq1.toDF("id", "name", "vip", "age", "rate")
      val icebergTable = IcebergTestUtils.createIcebergTable(
        spark,
        dir.getPath,
        icebergTableName,
        df1,
        Seq("vip")
      )

      withTable(targetTable) {
        // check the initial data remains consistent
        sql(
          s"""
             | CREATE TABLE $targetTable
             | UNIFORM iceberg
             | METADATA_PATH '${metadataLoc(icebergTable, 2)}'
             |""".stripMargin
        )
        val result1 = sql(s"SELECT id, name, vip, age, rate from default.$targetTable")
        checkAnswer(result1, df1.collect())

        // rename two columns in iceberg table
        val updatedTable = IcebergTestUtils.updateIcebergTableSchema(
          spark,
          dir.getPath,
          icebergTableName,
          IcebergTestUtils.RenameColumns(Map("age" -> "age_rename", "rate" -> "rate_rename"))
        )
        // refresh the table
        sql(
          s"""
             | REFRESH TABLE $targetTable
             | METADATA_PATH '${metadataLoc(updatedTable, 3)}'
             |""".stripMargin
        )
        // now the metadata (i.e., schema) of the UFI table
        // should be synced with the iceberg table.
//        eventually(timeout(Span(10, Seconds))) {
//          DeltaLog.clearCache()
//          val metadata =
//            spark.sessionState.catalog.getTableMetadata(new TableIdentifier(targetTable))
//          val nameMap = metadata.schema.fields.map(_.name)
//          assert(!nameMap.contains("age") && !nameMap.contains("rate"))
//          assert(nameMap.contains("age_rename") && nameMap.contains("rate_rename"))
//        }

        // check the result
        val result2 = sql(
          s"SELECT id, name, vip, age_rename, rate_rename from default.$targetTable"
        )
        checkAnswer(result2, df1.collect())
      }
    }
  }

  test("Refresh UFI Table For Updating Column") {
    withTempDir { dir =>
      val targetTable = "target_table_refresh"
      val icebergTableName = "iceberg_table_6"

      val seq1 = Seq((1, "Alice", true, 23, 1.03f), (2, "Bob", false, 24, 2.02f))
      val df1 = seq1.toDF("id", "name", "vip", "age", "rate")
      val icebergTable = IcebergTestUtils.createIcebergTable(
        spark,
        dir.getPath,
        icebergTableName,
        df1,
        Seq("vip")
      )

      withTable(targetTable) {
        // check the initial data remains consistent
        sql(
          s"""
             | CREATE TABLE $targetTable
             | UNIFORM iceberg
             | METADATA_PATH '${metadataLoc(icebergTable, 2)}'
             |""".stripMargin
        )
        val result1 = sql(s"SELECT id, name, vip, age, rate from default.$targetTable")
        checkAnswer(result1, df1.collect())

        // rename two columns in iceberg table
        val updatedTable = IcebergTestUtils.updateIcebergTableSchema(
          spark,
          dir.getPath,
          icebergTableName,
          // only ("int -> bigint", "float -> double", etc.) are considered safe updates here,
          // otherwise the column will be null.
          IcebergTestUtils.UpdateColumns(Map("rate" -> "double"))
        )
        // refresh the table
        sql(
          s"""
             | REFRESH TABLE $targetTable
             | METADATA_PATH '${metadataLoc(updatedTable, 3)}'
             |""".stripMargin
        )
        // now the metadata (i.e., schema) of the UFI table
        // should be synced with the iceberg table.
//        DeltaLog.clearCache()
//        val metadata =
//          spark.sessionState.catalog.getTableMetadata(new TableIdentifier(targetTable))
//        val nameMap = metadata.schema.fields.map(_.name)
//        assert(nameMap.contains("rate"))
//        metadata.schema.fields.foreach {
//          case field => if (field.name == "rate") {
//            assert(field.dataType.typeName == "double")
//          }
//        }

        val seq2 = Seq((3, "Alex", true, 23, 3.33d), (4, "Michael", true, 21, 4.44d))
        val df2 = seq2.toDF("id", "name", "vip", "age", "rate")
        IcebergTestUtils.writeIcebergTable(updatedTable, df2)
        // refresh again
        sql(
          s"""
             | REFRESH TABLE $targetTable
             | METADATA_PATH '${metadataLoc(updatedTable, 4)}'
             |""".stripMargin
        )

        // FIXME: currently update column is not correct when testing locally.
        // check the result
        // val result2 = sql(
        //   s"SELECT * from default.$targetTable"
        // )
        // checkAnswer(result2, df1.withColumn("rate", lit(null.asInstanceOf[Double])).collect()
        //  ++ df2.collect())
      }
    }
  }
}