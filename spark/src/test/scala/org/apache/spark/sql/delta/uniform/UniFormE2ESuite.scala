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

  test("Restore to a specific version") {
    withTable(testTableName) {
      write(
        s"""CREATE TABLE `$testTableName` (col1 INT) USING DELTA
           |TBLPROPERTIES (
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      (1 to 4).foreach{ i =>
        write(s"INSERT INTO `$testTableName` VALUES ($i)")
      }
      readAndVerify(testTableName, "col1", "col1", Seq(Row(1), Row(2), Row(3), Row(4)))

      write(s"RESTORE `$testTableName` TO VERSION AS OF 2")
      readAndVerify(testTableName, "col1", "col1", Seq(Row(1), Row(2)))
    }
  }


  test("Nested struct schema test") {
    withTable(testTableName) {
      write(s"""CREATE TABLE $testTableName
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
}
