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

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaTestUtils.withTimeZone
import org.apache.spark.sql.delta.sources.DeltaSQLConf.UTC_TIMESTAMP_PARTITION_VALUES

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
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

  test("Insert Partitioned Table") {
    val partitionColumns = Array(
      "str STRING",
      "i INTEGER",
      "l LONG",
      "s SHORT",
      "b BYTE",
      "dt DATE",
      "bin BINARY",
      "bool BOOLEAN",
      "ts_ntz TIMESTAMP_NTZ",
      "ts TIMESTAMP")

    val partitionValues: Array[Any] = Array(
      "'some_value'",
      1,
      1234567L,
      1000,
      119,
      "to_date('2016-12-31', 'yyyy-MM-dd')",
      "'asdf'",
      true,
      "TIMESTAMP_NTZ'2021-12-06 05:12:34'",
      "TIMESTAMP'2023-08-18 05:00:00UTC-7'"
    )

    partitionColumns zip partitionValues map {
      partitionColumnsAndValues =>
        val partitionColumnName =
          partitionColumnsAndValues._1.split(" ")(0)
        val tableName = testTableName + "_" + partitionColumnName
        withTable(tableName) {
          write(
            s"""CREATE TABLE $tableName (${partitionColumnsAndValues._1}, col1 INT)
               | USING DELTA
               | PARTITIONED BY ($partitionColumnName)
               | TBLPROPERTIES (
               |  'delta.columnMapping.mode' = 'name',
               |  'delta.enableIcebergCompatV2' = 'true',
               |  'delta.universalFormat.enabledFormats' = 'iceberg'
               |)""".stripMargin)
          write(s"INSERT INTO $tableName VALUES (${partitionColumnsAndValues._2}, 123)")
          val verificationQuery = s"SELECT col1 FROM $tableName " +
            s"where ${partitionColumnName}=${partitionColumnsAndValues._2}"
          // Verify against Delta read and Iceberg read
          checkAnswer(spark.sql(verificationQuery), Seq(Row(123)))
          assert(read(verificationQuery).sameElements(Seq(Row(123))))
        }
    }
  }

  test("Insert Partitioned Table - Multiple Partitions") {
    withTable(testTableName) {
      write(
        s"""CREATE TABLE $testTableName (id int, ts timestamp, col1 INT)
           | USING DELTA
           | PARTITIONED BY (id, ts)
           | TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      write(s"INSERT INTO $testTableName VALUES (1, TIMESTAMP'2023-08-18 05:00:00UTC-7', 123)")
      val verificationQuery = s"SELECT col1 FROM $testTableName " +
        s"where id=1 and ts=TIMESTAMP'2023-08-18 05:00:00UTC-7'"
      // Verify against Delta read and Iceberg read
      checkAnswer(spark.sql(verificationQuery), Seq(Row(123)))
      assert(read(verificationQuery).sameElements(Seq(Row(123))))
    }
  }

  test("Insert Partitioned Table - UTC Adjustment for Non-ISO Timestamp Partition values") {
    withTable(testTableName) {
      withTimeZone("GMT-8") {
        withSQLConf(UTC_TIMESTAMP_PARTITION_VALUES.key -> "false") {
          write(
            s"""CREATE TABLE $testTableName (id int, ts timestamp)
               | USING DELTA
               | PARTITIONED BY (ts)
               | TBLPROPERTIES (
               |  'delta.columnMapping.mode' = 'name',
               |  'delta.enableIcebergCompatV2' = 'true',
               |  'delta.universalFormat.enabledFormats' = 'iceberg'
               |)""".stripMargin)
          write(s"INSERT INTO $testTableName" +
            s" VALUES (1, timestamp'2021-06-30 00:00:00.123456')")

          // Verify partition values in Delta Log
          val deltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))
          val partitionColName = deltaLog.unsafeVolatileMetadata.physicalPartitionColumns.head
          val partitionValues = deltaLog.update().allFiles.head.partitionValues
          assert(partitionValues === Map(partitionColName -> "2021-06-30 00:00:00.123456"))

          // Verify against Delta read and Iceberg read
          val verificationQuery = s"SELECT id FROM $testTableName " +
            s"where ts=TIMESTAMP'2021-06-30 08:00:00.123456UTC'"
          checkAnswer(spark.sql(verificationQuery), Seq(Row(1)))
          checkAnswer(createReaderSparkSession.sql(verificationQuery), Seq(Row(1)))
        }
      }
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

  test("Re-enable test") {
    withTable(testTableName) {
      write(
        s"""CREATE TABLE $testTableName (col1 INT) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV1' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      write(s"INSERT INTO $testTableName VALUES (1)")
      readAndVerify(testTableName, "col1", "col1", Seq(Row(1)))

      write(s"ALTER TABLE `$testTableName` UNSET TBLPROPERTIES " +
        s"('delta.universalFormat.enabledFormats')")
      write(s"""
               | REORG TABLE $testTableName APPLY
               | (UPGRADE UNIFORM (ICEBERG_COMPAT_VERSION = 2))
               |""".stripMargin)
      write(s"INSERT INTO $testTableName VALUES (2)")
      readAndVerify(testTableName, "col1", "col1", Seq(Row(1), Row(2)))
    }
  }
}
