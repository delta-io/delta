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

package org.apache.spark.sql.delta.uniform

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

abstract class UniFormE2EIcebergSuiteBase extends UniFormE2ETest {

  val testTableName = "delta_table"

  test("Basic Insert") {
    withTable(testTableName) {
      write(
        s"""CREATE TABLE $testTableName (col1 INT) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV1' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      write(s"INSERT INTO $testTableName VALUES (123)")
      readAndVerify(testTableName, "col1", "col1", Seq(Row(123)))
    }
  }

  test("CIUD") {
    withTable(testTableName) {
      write(
        s"""CREATE TABLE `$testTableName` (col1 INT) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV1' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      write(s"INSERT INTO `$testTableName` VALUES (123),(456),(567),(331)")
      write(s"UPDATE `$testTableName` SET col1 = 191 WHERE col1 = 567")
      write(s"DELETE FROM `$testTableName` WHERE col1 = 456")

      readAndVerify(testTableName, "col1", "col1", Seq(Row(123), Row(191), Row(331)))
    }
  }

  test("Nested struct schema test") {
    withTable(testTableName) {
      write(
        s"""CREATE TABLE $testTableName
           | (col1 INT, col2 STRUCT<f1: STRUCT<f2: INT, f3: STRUCT<f4: INT, f5: INT>
           | , f6: INT>, f7: INT>) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV1' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)

      val data = Seq(
        Row(1, Row(Row(2, Row(3, 4), 5), 6))
      )

      val innerStruct3 = StructType(
        StructField("f4", IntegerType) ::
          StructField("f5", IntegerType) :: Nil)

      val innerStruct2 = StructType(
        StructField("f2", IntegerType) ::
          StructField("f3", innerStruct3) ::
          StructField("f6", IntegerType) :: Nil)

      val innerStruct = StructType(
        StructField("f1", innerStruct2) ::
          StructField("f7", IntegerType) :: Nil)

      val schema = StructType(
        StructField("col1", IntegerType) ::
          StructField("col2", innerStruct) :: Nil)

      val tableFullName = tableNameForRead(testTableName)

      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write.format("delta").mode("append")
        .saveAsTable(testTableName)

      val result = read(s"SELECT * FROM $tableFullName")

      assert(result.head === data.head)
    }
  }

  test("Nested struct schema evolution: Add nested struct") {
    withTable(testTableName) {
      write(
        s"""CREATE TABLE $testTableName (
           | col1 INT
           |) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV1' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)

      // Add nested struct
      write(s"ALTER TABLE $testTableName ADD COLUMN col2 STRUCT<f3: INT, f4: STRING>")

      write(
        s"""INSERT INTO $testTableName
           | VALUES (19, struct(25, 'Spark'))
           |""".stripMargin)

      assert(read(s"SELECT * FROM $testTableName").head.toSeq === Array(19, Row(25, "Spark")))
    }
  }

  test("Nested struct schema evolution: Rename, reorder and update type") {
    withTable(testTableName) {
      write(
        s"""CREATE TABLE $testTableName (
           | col1 INT,
           | col2 STRING
           |) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV1' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)

      write(s"INSERT INTO $testTableName VALUES (1, 'foo')")

      // Rename and reorder
      write(s"ALTER TABLE $testTableName RENAME COLUMN col1 TO col3")
      write(s"ALTER TABLE $testTableName ALTER COLUMN col3 AFTER col2")

      assert(read(s"SELECT * FROM $testTableName WHERE col3 == 1").head.toSeq === Array("foo", 1))
    }
  }

  test("Nested struct schema evolution: Add map") {
    withTable(testTableName) {
      write(
        s"""CREATE TABLE $testTableName (
           | col1 INT
           |) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)

      write(s"ALTER TABLE $testTableName ADD COLUMN map_of_ints MAP<int, int>")
      write(s"INSERT INTO $testTableName VALUES (22, map(19, 25))".stripMargin)

      assert(read(s"SELECT * FROM $testTableName").head.toSeq === Array(22, Map(19 -> 25)))
    }
  }

  test("Nested struct schema evolution: Add list") {
    withTable(testTableName) {
      write(
        s"""CREATE TABLE $testTableName (
           | col1 INT
           |) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)

      write(s"ALTER TABLE $testTableName ADD COLUMN ints ARRAY<int>")
      write(s"INSERT INTO $testTableName VALUES (22, ARRAY(19, 25))".stripMargin)

      assert(read(s"SELECT * FROM $testTableName").head.toSeq === Array(22, List(19, 25)))
    }
  }
}
