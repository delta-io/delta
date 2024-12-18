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

import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions}
import org.apache.spark.sql.delta.GeneratedAsIdentityType.{GeneratedAlways, GeneratedByDefault}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.commands.merge.MergeStats
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Identity Column test suite for DML operations, including INSERT REPLACE WHERE.
 */
trait IdentityColumnDMLSuiteBase
  extends IdentityColumnTestUtils {

  import testImplicits._

  test("delete") {
    for (generatedAsIdentityType <- GeneratedAsIdentityType.values) {
      val tblName = getRandomTableName
      withIdentityColumnTable(generatedAsIdentityType, tblName) {
        sql(s"INSERT INTO $tblName (value) VALUES (1), (2)")
        val prevMax = sql(s"SELECT MAX(id) FROM $tblName").collect().head.getLong(0)
        sql(s"DELETE FROM $tblName WHERE value = 1")
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $tblName"),
          Row(1L)
        )
        sql(s"DELETE FROM $tblName")
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $tblName"),
          Row(0L)
        )
        sql(s"INSERT INTO $tblName (value) VALUES (1), (2)")
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $tblName where id <= $prevMax"),
          Row(0L)
        )
      }
    }
  }

  test("merge with insert and update") {
    val start = 1L
    val step = 2L
    withSQLConf(
        DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS.key -> "false") {
      val source = s"${getRandomTableName}_src"
      val target = s"${getRandomTableName}_tgt"
      withTable(source, target) {
        var highWaterMark = start - step
        createTable(
          source,
          Seq(
            TestColumnSpec(colName = "value", dataType = IntegerType),
            TestColumnSpec(colName = "value2", dataType = IntegerType)
          )
        )
        sql(
          s"""
             |INSERT INTO $source VALUES (1, 100), (2, 200), (3, 300)
             |""".stripMargin)
        createTableWithIdColAndIntValueCol(
          target, GeneratedAlways, startsWith = Some(start), incrementBy = Some(step))
        sql(
          s"""
             |INSERT INTO $target(value) VALUES (2), (3), (4)
             |""".stripMargin)
        highWaterMark = validateIdentity(target, 3, start, step, 2, 4, highWaterMark)
        val idBeforeMerge1 = sql(s"SELECT id FROM $target WHERE value in (2, 3)").collect()

        sql(
          s"""
             |MERGE INTO $target
             |  USING $source on $target.value = $source.value
             |  WHEN MATCHED THEN UPDATE SET $target.value = $source.value2
             |  WHEN NOT MATCHED THEN INSERT (value) VALUES ($source.value2)
             |""".stripMargin)
        highWaterMark = validateIdentity(target, 4, start, step, 100, 100, highWaterMark)
        // IDENTITY values for updated rows shouldn't change.
        checkAnswer(
          sql(s"SELECT id FROM $target WHERE value in (200, 300)"),
          idBeforeMerge1
        )
        val idBeforeMerge2 =
          sql(s"SELECT id FROM $target WHERE value in (100, 300, 4)").collect()

        sql(s"INSERT OVERWRITE $source VALUES(200, 2000), (4, 400), (5, 500)")
        sql(
          s"""
             |MERGE INTO $target
             |  USING $source on $target.value = $source.value
             |  WHEN MATCHED AND $source.value = 200 THEN DELETE
             |  WHEN MATCHED THEN UPDATE SET $target.value = $source.value2
             |  WHEN NOT MATCHED THEN INSERT (value) VALUES ($source.value2)
             |""".stripMargin)
        highWaterMark = validateIdentity(target, 4, start, step, 500, 500, highWaterMark)
        // IDENTITY values for updated rows shouldn't change.
        checkAnswer(
          sql(s"SELECT id FROM $target WHERE value in (100, 300, 400)"),
          idBeforeMerge2
        )
      }
    }
  }

  test("merge with insert and update and schema evolution") {
    val start = 1L
    val step = 3L

    withSQLConf(
        "spark.databricks.delta.schema.autoMerge.enabled"-> "true",
        DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS.key -> "false") {
      val source = s"${getRandomTableName}_src"
      val target = s"${getRandomTableName}_tgt"
      withTable(source, target) {
        var highWaterMark = start - step
        createTable(
          source,
          Seq(
            TestColumnSpec(colName = "id2", dataType = LongType),
            TestColumnSpec(colName = "value2", dataType = IntegerType)
          )
        )
        sql(s"INSERT INTO $source VALUES (4, 44), (9, 99)")

        createTable(
          target,
          Seq(
            IdentityColumnSpec(
              GeneratedAlways,
              startsWith = Some(start),
              incrementBy = Some(step)
            ),
            TestColumnSpec(colName = "id2", dataType = LongType),
            TestColumnSpec(colName = "value", dataType = IntegerType)
          )
        )
        sql(s"INSERT INTO $target (id2, value) VALUES(1, 1), (4, 4), (7, 7), (10, 10)")
        highWaterMark = validateIdentity(target, 4, start, step, 1, 10, highWaterMark)

        val idBeforeMerge1 = sql(s"SELECT id FROM $target WHERE id2 in (1, 4, 7, 10)")
        sql(
          s"""
             |MERGE INTO $target
             |  USING $source on $target.id2 = $source.id2
             |  WHEN NOT MATCHED THEN INSERT *
             |""".stripMargin)
        checkAnswer(
          sql(s"SELECT id FROM $target WHERE id2 in (1, 4, 7, 10)"),
          idBeforeMerge1
        )
        checkAnswer(
          sql(s"SELECT COUNT(DISTINCT id) == COUNT(*) FROM $target"),
          Row(true)
        )

        val idBeforeMerge2 = sql(s"SELECT id FROM $target WHERE id2 in (1, 4, 7, 9, 10)")
        sql(s"INSERT OVERWRITE $source VALUES(9, 999), (11, 1100)")
        val events = Log4jUsageLogger.track {
          sql(
            s"""
               |MERGE INTO $target
               |  USING $source on $target.id2 = $source.id2
               |  WHEN MATCHED THEN UPDATE SET $target.value = $source.value2
               |  WHEN NOT MATCHED THEN INSERT *
               |""".stripMargin)
        }
        checkAnswer(
          sql(s"SELECT id FROM $target WHERE id2 in (1, 4, 7, 9, 10)"),
          idBeforeMerge2
        )
        checkAnswer(
          sql(s"SELECT COUNT(DISTINCT id) == COUNT(*) FROM $target"),
          Row(true)
        )
        val mergeStats = events.filter { e =>
          e.metric == MetricDefinitions.EVENT_TAHOE.name &&
            e.tags.get("opType").contains("delta.dml.merge.stats")
        }
        assert(mergeStats.size == 1)
      }
    }
  }

  test("MERGE/UPDATE/DELETE which does not INSERT any new data but just touches old rows" +
    " should not change the HIGH WATERMARK") {
    for {
      increment <- Seq(1, -1)
    } {
      withSQLConf(
          DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS.key -> "false") {
        val src = s"${getRandomTableName}_src"
        val tgt = s"${getRandomTableName}_tgt"
        withTable(src, tgt) {
          sql(s"DROP TABLE IF EXISTS $tgt")
          createTable(
            tgt,
            Seq(
              IdentityColumnSpec(
                GeneratedAlways,
                startsWith = Some(0),
                incrementBy = Some(increment)
              ),
              TestColumnSpec(colName = "col1", dataType = IntegerType)
            )
          )

          val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tgt))
          assert(deltaLog.snapshot.version === 0L)

          // INSERT 10 rows where each row is inserted into different file.
          (1 to 10)
            .toDF("col1")
            .repartition(10)
            .write.mode("overwrite").format("delta").saveAsTable(tgt)
          assert(deltaLog.snapshot.version === 1L)
          assert(highWaterMark(deltaLog.snapshot, "id") === increment * 9L)

          // Create src table with only 1 row having value 5.
          Seq(5).toDF("col1").write.saveAsTable(src)
          // The MERGE query will just UPDATE only one file that has one row only.
          sql(
            s"""
               | MERGE INTO $tgt tgt USING $src src
               | ON src.col1 = tgt.col1
               | WHEN MATCHED
               |   THEN UPDATE SET tgt.col1 = 100
               | WHEN NOT MATCHED THEN INSERT (tgt.col1) VALUES (src.col1)
               | """.stripMargin).collect()
          assert(deltaLog.snapshot.version === 2L)
          // The MERGE query shouldn't change the high watermark as it has not INSERTED any new
          // data.
          assert(highWaterMark(deltaLog.snapshot, "id") === increment * 9L)

          // Write 10 more rows to the table using single task and make sure that HIGH WATERMARK is
          // moved 10 units in either direction.
          val newDfToWrite = (11 to 20).toDF("col1").repartition(1)
          newDfToWrite.write.format("delta").mode("append").saveAsTable(tgt)
          assert(highWaterMark(deltaLog.snapshot, "id") === increment * 19L)
          // validate no duplicate identity values
          checkAnswer(sql(s"SELECT COUNT(DISTINCT id) == COUNT(*) FROM $tgt"), Row(true))
        }
      }
    }
  }

  // Helper function to test multiple "WHEN NOT MATCHED THEN INSERT" clauses in a single MERGE with
  // different variations - enable/disable WSCG, vary num partitions in the identity column
  // generation stage.
  private def testMergeWithMultipleWhenNotMatchedClauses(
      numPartitions: Int = 2,
      codegenEnabled: Boolean = true): Unit = {
    val codegenFactoryMode = if (codegenEnabled) {
      CodegenObjectFactoryMode.CODEGEN_ONLY
    } else {
      CodegenObjectFactoryMode.NO_CODEGEN
    }
    withSQLConf(
        SQLConf.CODEGEN_FACTORY_MODE.key -> codegenFactoryMode.toString,
        DeltaSQLConf.MERGE_USE_PERSISTENT_DELETION_VECTORS.key -> "false",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> s"$codegenEnabled") {
      val src = s"${getRandomTableName}_src"
      val tgt = s"${getRandomTableName}_tgt"
      withTable(src, tgt) {
        (1 to 10)
          .map(i => (i, i % 2))
          .toDF("col1", "col2")
          .repartition(numPartitions)
          .write.mode("overwrite")
          .saveAsTable(src)
        sql(s"DROP TABLE IF EXISTS $tgt")
        createTable(
          tgt,
          Seq(
            IdentityColumnSpec(
              GeneratedAlways,
              startsWith = Some(5),
              incrementBy = Some(5),
              colName = "id1"
            ),
            IdentityColumnSpec(
              GeneratedAlways,
              startsWith = Some(0),
              incrementBy = Some(3),
              colName = "id2"
            ),
            TestColumnSpec(colName = "col1", dataType = LongType),
            TestColumnSpec(colName = "col2", dataType = LongType)
          )
        )
        sql(s"INSERT INTO $tgt (col1, col2) VALUES (5, 100), (6, 101)")

        sql(
          s"""
             | MERGE INTO $tgt tgt USING $src src
             | ON src.col1 = tgt.col1
             | WHEN MATCHED AND tgt.col1 == 5
             |   THEN UPDATE SET tgt.col2 = src.col2
             | WHEN NOT MATCHED AND src.col1 % 3 != 0
             |   THEN INSERT (tgt.col1, tgt.col2) VALUES (src.col1, src.col2)
             | WHEN NOT MATCHED AND src.col1 % 3 == 0
             |   THEN INSERT (tgt.col1, tgt.col2) VALUES (src.col1, src.col2)
             | """.stripMargin).collect()

        Seq("id1", "id2").foreach { idCol =>
          checkAnswer(
            sql(s"SELECT COUNT(DISTINCT $idCol) == COUNT(*) FROM $tgt"),
            Row(true))
        }
        assert(sql(s"SELECT * FROM $tgt WHERE id1 % 5 != 0").count() === 0)
        assert(sql(s"SELECT * FROM $tgt WHERE id2 % 3 != 0").count() === 0)

        checkAnswer(
          sql(s"SELECT col1, col2 FROM $tgt"),
          Seq(Row(1, 1), Row(2, 0), Row(3, 1), Row(4, 0), Row(5, 1),
            Row(6, 101), Row(7, 1), Row(8, 0), Row(9, 1), Row(10, 0))
        )
      }
    }
  }

  test(s"MERGE with multiple WHEN NOT MATCHED THEN INSERT clauses") {
    testMergeWithMultipleWhenNotMatchedClauses()
  }

  test(s"MERGE with multiple WHEN NOT MATCHED THEN INSERT clauses + WholeStageCodeGen disabled") {
    testMergeWithMultipleWhenNotMatchedClauses(codegenEnabled = false)
  }

  test(s"MERGE with multiple WHEN NOT MATCHED THEN INSERT clauses + single partition") {
    testMergeWithMultipleWhenNotMatchedClauses(numPartitions = 1)
  }

  private def testReplaceWhereWithCDF(isPartitioned: Boolean): Unit = {
    val start = 1L
    val step = 2L
    withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {
      val table = getRandomTableName
      withTable(table) {
        var highWaterMarkFromData = start - step
        createTable(
          table,
          Seq(
            IdentityColumnSpec(
              GeneratedAlways,
              startsWith = Some(start),
              incrementBy = Some(step)
            ),
            TestColumnSpec(colName = "value", dataType = IntegerType),
            TestColumnSpec(colName = "is_odd", dataType = BooleanType),
            TestColumnSpec(colName = "is_even", dataType = BooleanType)
          ),
          partitionedBy = if (isPartitioned) Seq("is_odd") else Nil
        )
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))

        def highWatermarkFromDeltaLog(): Long = highWaterMark(deltaLog.update(), "id")

        Seq(1, 2, 3, 4).toDF()
          .withColumn("is_odd", $"value" % 2 =!= 0)
          .withColumn("is_even", $"value" % 2 === 0)
          .coalesce(1)
          .write
          .format("delta")
          .mode("append")
          .saveAsTable(table)
        highWaterMarkFromData = validateIdentity(
          table,
          expectedRowCount = 4,
          start = start,
          step = step,
          minValue = 1,
          maxValue = 4,
          oldHighWaterMark = highWaterMarkFromData)
        assert(highWaterMarkFromData === highWatermarkFromDeltaLog())

        val idBeforeReplaceWhere1 = sql(s"SELECT id FROM $table WHERE is_even = true").collect()

        Seq(5, 7).toDF()
          .withColumn("is_odd", $"value" % 2 =!= 0)
          .withColumn("is_even", $"value" % 2 === 0)
          .coalesce(1)
          .write
          .format("delta")
          .mode("overwrite")
          .option(DeltaOptions.REPLACE_WHERE_OPTION, "is_odd = true")
          .saveAsTable(table)

        highWaterMarkFromData = validateIdentity(
          table,
          expectedRowCount = 4,
          start = start,
          step = step,
          minValue = 5,
          maxValue = 7,
          oldHighWaterMark = highWaterMarkFromData)
        assert(highWaterMarkFromData === highWatermarkFromDeltaLog())

        // IDENTITY values for not-updated shouldn't change.
        checkAnswer(
          sql(s"SELECT id FROM $table WHERE is_even = true"),
          idBeforeReplaceWhere1
        )

        // IDENTITY VALUES for inserted change records and new data should be consistent.
        checkAnswer(
          sql(s"SELECT id FROM table_changes('$table', 2, 2) " +
            "WHERE is_odd = true and _change_type = 'insert'"),
          sql(s"SELECT id FROM $table WHERE is_odd = true")
        )

        val idBeforeReplaceWhere2 = sql(s"SELECT id FROM $table WHERE is_odd = true").collect()

        Seq(10, 12).toDF()
          .withColumn("is_odd", $"value" % 2 =!= 0)
          .withColumn("is_even", $"value" % 2 === 0)
          .coalesce(1)
          .write
          .format("delta")
          .mode("overwrite")
          .option(DeltaOptions.REPLACE_WHERE_OPTION, "is_even = true")
          .saveAsTable(table)

        highWaterMarkFromData = validateIdentity(
          table,
          expectedRowCount = 4,
          start = start,
          step = step,
          minValue = 10,
          maxValue = 12,
          oldHighWaterMark = highWaterMarkFromData)
        assert(highWaterMarkFromData === highWatermarkFromDeltaLog())
        // IDENTITY values for not-updated shouldn't change.
        checkAnswer(
          sql(s"SELECT id FROM $table WHERE is_odd = true"),
          idBeforeReplaceWhere2
        )

        // IDENTITY VALUES for inserted change records and data should be consistent.
        checkAnswer(
          sql(s"SELECT id FROM table_changes('$table', 3, 3) " +
            "WHERE is_even = true and _change_type = 'insert'"),
          sql(s"SELECT id FROM $table WHERE is_even = true")
        )

        // ReplaceWhere source data contains an Identity Column will be blocked.
        val e = intercept[AnalysisException] {
          Seq((15, 14), (17, 16)).toDF("id", "value")
            .withColumn("is_odd", $"value" % 2 =!= 0)
            .withColumn("is_even", $"value" % 2 === 0)
            .coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .option(DeltaOptions.REPLACE_WHERE_OPTION, "is_even = true")
            .saveAsTable(table)
        }
        assert(e.getMessage.contains(
          "Providing values for GENERATED ALWAYS AS IDENTITY column id is not supported."))
      }
    }
  }

  Seq(true, false).foreach { isPartitioned =>
    test(s"replaceWhere with CDF enabled [isPartitioned: $isPartitioned]") {
      testReplaceWhereWithCDF(isPartitioned)
    }
  }

  test("CDF for tables with identity column - MERGE") {
    val src = s"${getRandomTableName}_src"
    val tgt = s"${getRandomTableName}_tgt"
    withTable(tgt) {
      generateTableWithIdentityColumn(tgt)
      sql(s"ALTER TABLE $tgt SET TBLPROPERTIES (${DeltaConfigs.CHANGE_DATA_FEED.key}=true)")

      withTempView(src) {
        (1 :: 20 :: 30 :: Nil).toDF("value").createOrReplaceTempView(src)
        sql(
          s"""MERGE INTO $tgt target
             |USING $src src ON target.value = src.value
             |WHEN MATCHED THEN UPDATE SET target.value = src.value
             |WHEN NOT MATCHED THEN INSERT (target.value) VALUES (src.value)""".stripMargin)
      }

      val sortedResult = sql(
        s"""SELECT id, value
           |FROM $tgt
           |ORDER BY id""".stripMargin)
        .as[IdentityColumnTestTableRow]
        .collect()

      assert(sortedResult.length === 8)
      checkGeneratedIdentityValues(
        sortedRows = sortedResult,
        start = 0,
        step = 1,
        expectedLowerBound = 0,
        expectedUpperBound = 20,
        expectedDistinctCount = 8)

      def getIdForValue(value: Int): Long = {
        val rowWithValue = sortedResult.filter(_.value == value.toString)
        assert(
          rowWithValue.length === 1,
          s"Expected 1 row for value $value, found ${rowWithValue.length}")
        rowWithValue.head.id
      }

      // Validate the ids in CDCReader match those in logical data.
      checkAnswer(sql(
        s"""SELECT id, value, ${CDCReader.CDC_TYPE_COLUMN_NAME}
           |FROM table_changes('$tgt', 8)
           |ORDER BY id, value, _change_type""".stripMargin),
        Seq(
          Row(1, 1, CDCReader.CDC_TYPE_UPDATE_POSTIMAGE),
          Row(1, 1, CDCReader.CDC_TYPE_UPDATE_PREIMAGE),
          Row(getIdForValue(value = 20), 20, CDCReader.CDC_TYPE_INSERT),
          Row(getIdForValue(value = 30), 30, CDCReader.CDC_TYPE_INSERT)))
    }
  }

  test("CDF for tables with identity column - UPDATE") {
    val tgt = s"${getRandomTableName}_tgt"
    withTable(tgt) {

      generateTableWithIdentityColumn(tgt)
      sql(s"ALTER TABLE $tgt SET TBLPROPERTIES (${DeltaConfigs.CHANGE_DATA_FEED.key}=true)")

      sql(
        s"""UPDATE $tgt
           |SET value = value + 100
           |WHERE id < 3""".stripMargin)

      checkAnswer(sql(
        s"""SELECT *
           |FROM $tgt
           |ORDER BY id, value""".stripMargin),
        Seq(
          Row(0, 100), Row(1, 101), Row(2, 102),
          Row(3, 3), Row(4, 4), Row(5, 5)))

      checkAnswer(sql(
        s"""SELECT id, value, ${CDCReader.CDC_TYPE_COLUMN_NAME}
           |FROM table_changes('$tgt', 8)
           |ORDER BY id, value, _change_type""".stripMargin),
        Seq(
          Row(0, 0, CDCReader.CDC_TYPE_UPDATE_PREIMAGE),
          Row(0, 100, CDCReader.CDC_TYPE_UPDATE_POSTIMAGE),
          Row(1, 1, CDCReader.CDC_TYPE_UPDATE_PREIMAGE),
          Row(1, 101, CDCReader.CDC_TYPE_UPDATE_POSTIMAGE),
          Row(2, 2, CDCReader.CDC_TYPE_UPDATE_PREIMAGE),
          Row(2, 102, CDCReader.CDC_TYPE_UPDATE_POSTIMAGE)))
    }
  }

  test("UPDATE cannot lead to bad high watermarks") {
    val tblName = getRandomTableName
    withTable(tblName) {
      createTable(
        tblName,
        Seq(
          IdentityColumnSpec(
            GeneratedByDefault,
            startsWith = Some(1),
            incrementBy = Some(1)),
          TestColumnSpec(colName = "value", dataType = IntegerType)
        ),
        partitionedBy = Seq("value"),
        tblProperties = Map(
          DeltaConfigs.CHANGE_DATA_FEED.key -> "true",
          DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key -> "false"
        )
      )
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tblName))

      sql(s"INSERT INTO $tblName(id, value) VALUES (-5, -5), (-3, -3), (-1, -1)")
      val valuesStr = (-999 to -900).map(id => s"($id, -3)").mkString(", ")
      sql(s"INSERT INTO $tblName(id, value) VALUES $valuesStr")
      sql(s"INSERT INTO $tblName(id, value) VALUES (-1, -1)")
      val expectedNumFiles = 5
      assert(deltaLog.update().allFiles.count() === expectedNumFiles)
      assert(getHighWaterMark(deltaLog.update(), colName = "id").isEmpty,
        "High watermark should not be set for user inserted values")

      Seq((-1000L, -3)).toDF("id", "value")
        .write
        .format("delta")
        .mode("overwrite")
        .option(DeltaOptions.REPLACE_WHERE_OPTION, "value = -3 and id <= -987")
        .saveAsTable(tblName)

      assert(getHighWaterMark(deltaLog.update(), colName = "id").isEmpty,
        "High watermark should not be set for user inserted values")

      sql(s"UPDATE $tblName SET value = -3 WHERE id = -1")
      assert(getHighWaterMark(deltaLog.update(), colName = "id").isEmpty,
        "Updates should not update high watermark")
    }
  }
}

class IdentityColumnDMLScalaSuite
  extends IdentityColumnDMLSuiteBase
  with ScalaDDLTestUtils

class IdentityColumnDMLScalaIdColumnMappingSuite
  extends IdentityColumnDMLSuiteBase
  with ScalaDDLTestUtils
  with DeltaColumnMappingEnableIdMode

class IdentityColumnDMLScalaNameColumnMappingSuite
  extends IdentityColumnDMLSuiteBase
  with ScalaDDLTestUtils
  with DeltaColumnMappingEnableNameMode

