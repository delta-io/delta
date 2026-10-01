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
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.commands.cdc.CDCReader._
import org.apache.spark.sql.delta.sources.DeltaSQLConf._
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.lit

trait DeleteCDCMixin extends DeleteSQLMixin with CDCEnabled {
  protected def testCDCDelete(
      name: String)(
      initialData: => Dataset[_],
      partitionColumns: Seq[String] = Seq.empty,
      deleteCondition: String,
      expectedData: => Dataset[_],
      expectedChangeDataWithoutVersion: => Dataset[_]
    ): Unit = {
    test(s"CDC - $name") {
      withSQLConf(
          ALLOW_EXISTS_SUBQUERY_IN_DELETE.key -> "true",
          (DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")) {
        append(initialData.toDF(), partitionColumns)

        executeDelete(tableSQLIdentifier, deleteCondition)

        checkAnswer(
          readDeltaTableByIdentifier(),
          expectedData.toDF())

        checkAnswer(
          getCDCForLatestOperation(deltaLog, operation = "DELETE"),
          expectedChangeDataWithoutVersion.toDF())
        spark.sql(s"DROP TABLE IF EXISTS $deleteSourceTableName")
      }
    }
  }

  protected val deleteSourceTableName = "__delete_cdc_source_table"
}

trait DeleteCDCTests extends DeleteCDCMixin
  with CDCTestMixin {
  import testImplicits._

  testCDCDelete("unconditional")(
    initialData = spark.range(0, 10, step = 1, numPartitions = 3),
    deleteCondition = "",
    expectedData = spark.range(0),
    expectedChangeDataWithoutVersion = spark.range(10)
      .withColumn(CDC_TYPE_COLUMN_NAME, lit("delete"))
  )

  testCDCDelete("conditional covering all rows")(
    initialData = spark.range(0, 10, step = 1, numPartitions = 3),
    deleteCondition = "id < 100",
    expectedData = spark.range(0),
    expectedChangeDataWithoutVersion = spark.range(10)
      .withColumn(CDC_TYPE_COLUMN_NAME, lit("delete"))
  )

  testCDCDelete("two random rows")(
    initialData = spark.range(0, 10, step = 1, numPartitions = 3),
    deleteCondition = "id = 2 OR id = 8",
    expectedData = Seq(0, 1, 3, 4, 5, 6, 7, 9).toDF(),
    expectedChangeDataWithoutVersion = Seq(2, 8).toDF()
      .withColumn(CDC_TYPE_COLUMN_NAME, lit("delete"))
  )

  testCDCDelete("delete unconditionally - partitioned table")(
    initialData = spark.range(0, 100, step = 1, numPartitions = 10)
      .selectExpr("id % 10 as part", "id"),
    partitionColumns = Seq("part"),
    deleteCondition = "",
    expectedData = Seq.empty[(Long, Long)].toDF("part", "id"),
    expectedChangeDataWithoutVersion =
      spark.range(100)
        .selectExpr("id % 10 as part", "id", "'delete' as _change_type")
  )

  testCDCDelete("delete all rows by condition - partitioned table")(
    initialData = spark.range(0, 100, step = 1, numPartitions = 10)
      .selectExpr("id % 10 as part", "id"),
    partitionColumns = Seq("part"),
    deleteCondition = "id < 1000",
    expectedData = Seq.empty[(Long, Long)].toDF("part", "id"),
    expectedChangeDataWithoutVersion =
      spark.range(100)
        .selectExpr("id % 10 as part", "id", "'delete' as _change_type")
  )

  private val numFiles = 5
  private val numRowsPerFile = 20

  private def testCDCDeleteWithSubquery(
      testNameSuffix: String,
      deleteSourceIdColumn: String)(
      deleteCondition: String,
      expectedData: => Dataset[_],
      expectedChangeDataWithoutVersion: => Dataset[_]): Unit = {
    testCDCDelete("subquery - " + testNameSuffix)(
      initialData = {
        spark.range(0, 100, 1, 10)
          .withColumnRenamed("id", deleteSourceIdColumn)
          .write.format("delta").mode("overwrite")
          .saveAsTable(deleteSourceTableName)
        spark.range(0, numRowsPerFile * numFiles, 1, numFiles)
      },
      deleteCondition = deleteCondition,
      expectedData = expectedData,
      expectedChangeDataWithoutVersion = expectedChangeDataWithoutVersion
    )
  }

  testCDCDeleteWithSubquery("EXISTS - all rows", deleteSourceIdColumn = "src_id")(
    deleteCondition =
      s"EXISTS (SELECT 1 FROM $deleteSourceTableName s WHERE s.src_id = id)",
    expectedData = spark.range(0),
    expectedChangeDataWithoutVersion =
      spark.range(100).selectExpr("id", "'delete' as _change_type"))

  testCDCDeleteWithSubquery("EXISTS - two random rows", deleteSourceIdColumn = "src_id")(
    deleteCondition =
      s"""EXISTS (SELECT 1 FROM $deleteSourceTableName s
         |  WHERE s.src_id = id AND (s.src_id = 33 OR s.src_id = 66))""".stripMargin,
    expectedData = spark.range(100).where("id != 33 AND id != 66"),
    expectedChangeDataWithoutVersion =
      spark.range(100).where("id = 33 OR id = 66")
        .selectExpr("id", "'delete' as _change_type"))

  testCDCDeleteWithSubquery(
    "NOT EXISTS - delete unmatched rows", deleteSourceIdColumn = "src_id")(
    deleteCondition =
      s"""NOT EXISTS (SELECT 1 FROM $deleteSourceTableName s
         |  WHERE s.src_id = id AND s.src_id < 5)""".stripMargin,
    expectedData = spark.range(5),
    expectedChangeDataWithoutVersion =
      spark.range(5, 100).selectExpr("id", "'delete' as _change_type"))

  test("CDC - subquery - EXISTS - no matching rows") {
    withSQLConf(
        ALLOW_EXISTS_SUBQUERY_IN_DELETE.key -> "true",
        DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      spark.range(0, 100, 1, 10)
        .withColumnRenamed("id", "src_id")
        .write.format("delta").mode("overwrite")
        .saveAsTable(deleteSourceTableName)
      append(spark.range(0, numRowsPerFile * numFiles, 1, numFiles).toDF())

      executeDelete(
        tableSQLIdentifier,
        s"""EXISTS (SELECT 1 FROM $deleteSourceTableName s
           |  WHERE s.src_id = id AND s.src_id > 200)""".stripMargin)
      checkAnswer(readDeltaTableByIdentifier(), spark.range(100).toDF())
      spark.sql(s"DROP TABLE IF EXISTS $deleteSourceTableName")
    }
  }

  testCDCDelete("partition-optimized delete")(
    initialData = spark.range(0, 100, step = 1, numPartitions = 10)
      .selectExpr("id % 10 as part", "id"),
    partitionColumns = Seq("part"),
    deleteCondition = "part = 3",
    expectedData =
      spark.range(100).selectExpr("id % 10 as part", "id").where("part != 3"),
    expectedChangeDataWithoutVersion =
      Range(0, 10).map(x => x * 10 + 3).toDF("id")
        .selectExpr("3 as part", "id", "'delete' as _change_type"))


  test("CDC - delete with EXISTS subquery") {
    withSQLConf(
        ALLOW_EXISTS_SUBQUERY_IN_DELETE.key -> "true",
        DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      spark.range(0, 100, 1, 10)
        .withColumnRenamed("id", "source_id")
        .write.format("delta").mode("overwrite")
        .saveAsTable(deleteSourceTableName)
      append(spark.range(0, 10, step = 1, numPartitions = 3)
        .withColumnRenamed("id", "target_id").toDF())
      executeDelete(tableSQLIdentifier,
        s"""EXISTS (SELECT 1 FROM $deleteSourceTableName s
           |  WHERE s.source_id = target_id AND s.source_id < 5)""".stripMargin)
      checkAnswer(
        readDeltaTableByIdentifier(),
        spark.range(5, 10).withColumnRenamed("id", "target_id").toDF())
      checkAnswer(
        getCDCForLatestOperation(deltaLog, operation = "DELETE"),
        spark.range(0, 5).withColumnRenamed("id", "target_id").toDF()
          .withColumn(CDC_TYPE_COLUMN_NAME, lit("delete")))
      spark.sql(s"DROP TABLE IF EXISTS $deleteSourceTableName")
    }
  }

  test("CDC - delete with NOT EXISTS subquery") {
    withSQLConf(
        ALLOW_EXISTS_SUBQUERY_IN_DELETE.key -> "true",
        DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      spark.range(0, 5, 1, 1)
        .withColumnRenamed("id", "source_id")
        .write.format("delta").mode("overwrite")
        .saveAsTable(deleteSourceTableName)
      append(spark.range(0, 10, step = 1, numPartitions = 3)
        .withColumnRenamed("id", "target_id").toDF())
      executeDelete(tableSQLIdentifier,
        s"""NOT EXISTS (SELECT 1 FROM $deleteSourceTableName s
           |  WHERE s.source_id = target_id)""".stripMargin)
      checkAnswer(
        readDeltaTableByIdentifier(),
        spark.range(0, 5).withColumnRenamed("id", "target_id").toDF())
      checkAnswer(
        getCDCForLatestOperation(deltaLog, operation = "DELETE"),
        spark.range(5, 10).withColumnRenamed("id", "target_id").toDF()
          .withColumn(CDC_TYPE_COLUMN_NAME, lit("delete")))
      spark.sql(s"DROP TABLE IF EXISTS $deleteSourceTableName")
    }
  }

  test("CDC - delete with EXISTS subquery matching no rows") {
    withSQLConf(
        ALLOW_EXISTS_SUBQUERY_IN_DELETE.key -> "true",
        DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      spark.range(100, 200, 1, 1)
        .withColumnRenamed("id", "source_id")
        .write.format("delta").mode("overwrite")
        .saveAsTable(deleteSourceTableName)
      append(spark.range(0, 10, step = 1, numPartitions = 2)
        .withColumnRenamed("id", "target_id").toDF())
      executeDelete(tableSQLIdentifier,
        s"""EXISTS (SELECT 1 FROM $deleteSourceTableName s
           |  WHERE s.source_id = target_id)""".stripMargin)
      checkAnswer(
        readDeltaTableByIdentifier(),
        spark.range(10).withColumnRenamed("id", "target_id").toDF())
      spark.sql(s"DROP TABLE IF EXISTS $deleteSourceTableName")
    }
  }
}

trait DeleteCDCTableWithDVsTests extends DeleteCDCMixin
  with CDCTestMixin
  with DeletionVectorsTestUtils {
  import testImplicits._

  private def executeDeleteWithoutAddingDVs(target: String, where: String = null): Unit = {
    super[DeleteCDCMixin].executeDelete(target = target, where = where)
  }

  test("CDC - delete from file with DV with EXISTS subquery") {
    spark.range(0, 10, 1, 10)
      .withColumnRenamed("id", "src_id")
      .write.format("delta").mode("overwrite")
      .saveAsTable(deleteSourceTableName)
    withSQLConf(
        ALLOW_EXISTS_SUBQUERY_IN_DELETE.key -> "true",
        DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      append(spark.range(0, 10, step = 1, numPartitions = 1).toDF())
      addDeletionVectorsToTable(tableSQLIdentifier)
      executeDeleteWithoutAddingDVs(
        tableSQLIdentifier,
        s"""EXISTS (SELECT 1 FROM $deleteSourceTableName s
           |  WHERE s.src_id = id AND s.src_id IN (0, 3))""".stripMargin)
      checkAnswer(
        readDeltaTableByIdentifier(),
        Seq(1, 2, 4, 5, 6, 7, 8, 9).toDF())
      checkAnswer(
        computeCDC(spark, deltaLog, 2, 2).drop(CDC_COMMIT_TIMESTAMP),
        Seq(0, 3).toDF()
          .withColumn(CDC_TYPE_COLUMN_NAME, lit(CDC_TYPE_DELETE_STRING))
          .withColumn(CDC_COMMIT_VERSION, lit(2)))
      spark.sql(s"DROP TABLE IF EXISTS $deleteSourceTableName")
    }
  }

  test("CDC - delete from file with DV with NOT EXISTS subquery") {
    spark.range(0, 5, 1, 5)
      .withColumnRenamed("id", "src_id")
      .write.format("delta").mode("overwrite")
      .saveAsTable(deleteSourceTableName)
    withSQLConf(
        ALLOW_EXISTS_SUBQUERY_IN_DELETE.key -> "true",
        DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      append(spark.range(0, 10, step = 1, numPartitions = 1).toDF())
      addDeletionVectorsToTable(tableSQLIdentifier)
      executeDeleteWithoutAddingDVs(
        tableSQLIdentifier,
        s"""NOT EXISTS (SELECT 1 FROM $deleteSourceTableName s
           |  WHERE s.src_id = id)""".stripMargin)
      checkAnswer(
        readDeltaTableByIdentifier(),
        spark.range(0, 5).toDF())
      checkAnswer(
        computeCDC(spark, deltaLog, 2, 2).drop(CDC_COMMIT_TIMESTAMP),
        spark.range(5, 10).toDF()
          .withColumn(CDC_TYPE_COLUMN_NAME, lit(CDC_TYPE_DELETE_STRING))
          .withColumn(CDC_COMMIT_VERSION, lit(2)))
      spark.sql(s"DROP TABLE IF EXISTS $deleteSourceTableName")
    }
  }

}
