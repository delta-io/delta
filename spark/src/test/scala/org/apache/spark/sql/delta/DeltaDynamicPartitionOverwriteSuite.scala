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
import org.apache.spark.sql.delta.DeltaTestUtils.withTimeZone
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{JsonUtils, PartitionUtils}

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, CreateTableWriter, DataFrame, DataFrameWriter, DataFrameWriterV2, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.internal.SQLConf.{PARTITION_OVERWRITE_MODE, PartitionOverwriteMode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampNTZType, TimestampType}

/**
 * Test suite for Dynamic Partition Overwrite.
 *
 * Validates the behavior of DPO in SQL, DF v1 and V2 API and checks both the DF writer options
 * and SQL config. In addition, it validates that DPO config is applied only to selectively
 * overwrite operations and not table replace operations.
 */
class DeltaDynamicPartitionOverwriteSuite
    extends QueryTest
    with DeltaDMLTestUtilsPathBased
    with DeltaSQLCommandTest {
  import testImplicits._

  // Initial data to be inserted on the target data before the DPO operation.
  lazy private val targetData: DataFrame =
    Seq(0, 1).toDF("id")
      .withColumn("value", lit("t"))
      .withColumn("part", col("id"))

  // Source data to be inserted into the target data with the DPO operation.
  lazy private val sourceData: DataFrame =
    Seq(1, 2).toDF("id")
      .withColumn("value", lit("s"))
      .withColumn("part", col("id"))

  // Expected data after the DPO operation.
  lazy private val expectedDpoData: DataFrame = targetData.filter("id != 1").union(sourceData)

  protected var tempTable: String = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    tempTable = s"delta.`$tempPath`"

    // Initialize the target table.
    targetData.write.format("delta").partitionBy("part").saveAsTable(tempTable)
  }

  private def checkAnswer(expectedAnswer: DataFrame): Unit =
    checkAnswer(spark.read.format("delta").load(tempPath), expectedAnswer)

  private def castValue(df: DataFrame, newType: String): DataFrame =
    df.withColumn("value", col("value").cast(newType))

  private case class DPOConf(
    sqlConf: Boolean,
    writerOption: Boolean
  )

  private def dpoTest(testName: String)(testBody: DPOConf => Unit): Unit = {
    // Run test with SQL config.
    test(s"$testName(sqlConf)") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
        testBody(DPOConf(sqlConf = true, writerOption = false))
      }
    }

    // Run test with Writer config.
    test(s"$testName(writerOption)") {
      testBody(DPOConf(sqlConf = false, writerOption = true))
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // DataframeV1
  ///////////////////////////////////////////////////////////////////////////
  private def v1Writer(df: DataFrame, mode: SaveMode, conf: DPOConf): DataFrameWriter[Row] = {
    var writer = df.write.format("delta").mode(mode)
    if (conf.writerOption) {
      writer = writer.option("partitionOverwriteMode", "dynamic")
    }
    writer
  }

  private def v1Write(writer: DataFrameWriter[Row], method: String): Unit = {
    method match {
      case "save" => writer.save(tempPath)
      case "saveAsTable" => writer.saveAsTable(tempTable)
      case "insertInto" => writer.insertInto(tempTable)
    }
  }

  for {
    method <- Seq("save", "saveAsTable", "insertInto")
  } {
    dpoTest(s"DataframeV1($method): Overwrite mode") { conf =>
      // DPO
      v1Write(
        v1Writer(sourceData, SaveMode.Overwrite, conf),
        method
      )
      checkAnswer(expectedDpoData)
    }

    for {
      overwriteSchema <- BOOLEAN_DOMAIN
      mergeSchema <- BOOLEAN_DOMAIN
    } dpoTest(s"DataframeV1($method): Overwrite mode with additive schema, " +
        s"overwriteSchema=$overwriteSchema, mergeSchema=$mergeSchema") { conf =>
      val srcData = sourceData.withColumn("newCol", lit(1))
      val writer = v1Writer(srcData, SaveMode.Overwrite, conf)
        .option("overwriteSchema", overwriteSchema.toString)
        .option("mergeSchema", mergeSchema.toString)

      if (overwriteSchema || mergeSchema) {
        v1Write(writer, method)
        if (overwriteSchema) {
          // No DPO.
          checkAnswer(srcData)
        } else {
          // Schema evolution.
          checkAnswer(targetData.filter("id != 1").withColumn("newCol", lit(null)).union(srcData))
        }
      } else {
        val e = intercept[DeltaAnalysisException] {
          v1Write(writer, method)
        }
        assert(e.getErrorClass == "DELTA_METADATA_MISMATCH")
      }
    }

    for {
      overwriteSchema <- BOOLEAN_DOMAIN
      mergeSchema <- BOOLEAN_DOMAIN
    } dpoTest(s"DataframeV1($method): Overwrite mode with different non-additive schema, " +
        s"overwriteSchema=$overwriteSchema, mergeSchema=$mergeSchema") { conf =>
      val srcData = castValue(sourceData.withColumn("value", col("id")), "double")
      val writer = v1Writer(srcData, SaveMode.Overwrite, conf)
        .option("overwriteSchema", overwriteSchema.toString)
        .option("mergeSchema", mergeSchema.toString)

      if (method == "insertInto") {
        // InsertInto will cast the values to the target schema.
        v1Write(writer, method)
        if (overwriteSchema) {
          checkAnswer(castValue(srcData, "string"))
        } else {
          targetData.filter("id != 1").union(castValue(srcData, "string"))
        }
      } else {
        if (overwriteSchema) {
          v1Write(writer, method)
          checkAnswer(srcData)
        } else {
          val e = intercept[DeltaAnalysisException] {
            v1Write(writer, method)
          }
          assert(e.getErrorClass == "DELTA_FAILED_TO_MERGE_FIELDS")
        }
      }
    }

    dpoTest(s"DataframeV1($method): Overwrite mode with replaceWhere") { conf =>
      withSQLConf(DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED.key -> "false") {
        val writer = v1Writer(sourceData, SaveMode.Overwrite, conf)
          .option("replaceWhere", "id = 99") // always false
        if (conf.writerOption || method == "insertInto") {
          val e = intercept[DeltaIllegalArgumentException] {
            v1Write(writer, method)
          }
          assert(e.getErrorClass == "DELTA_REPLACE_WHERE_WITH_DYNAMIC_PARTITION_OVERWRITE")
        } else {
          v1Write(writer, method)
          checkAnswer(targetData.union(sourceData))
        }
      }
    }

    dpoTest(s"DataframeV1($method): Overwrite mode with replaceWhere and overwriteSchema") { conf =>
      withSQLConf(DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED.key -> "false") {
        val writer = v1Writer(sourceData, SaveMode.Overwrite, conf)
          .option("replaceWhere", "id = 99")
          .option("overwriteSchema", "true")
        if (conf.writerOption || method == "insertInto") {
          val e = intercept[DeltaIllegalArgumentException] {
            v1Write(writer, method)
          }
          assert(e.getErrorClass == "DELTA_REPLACE_WHERE_WITH_DYNAMIC_PARTITION_OVERWRITE")
        } else {
          v1Write(writer, method)
          checkAnswer(targetData.union(sourceData))
        }
      }
    }

    dpoTest(s"DataframeV1($method): Append mode") { conf =>
      v1Write(v1Writer(sourceData, SaveMode.Append, conf), method)
      checkAnswer(targetData.union(sourceData))
    }

    dpoTest(s"DataframeV1($method): Append mode with overwrite schema") { conf =>
      val srcData = castValue(sourceData.withColumn("value", col("id")), "double")
      val writer = v1Writer(srcData, SaveMode.Append, conf)
        .option("overwriteSchema", "true")
      // overwriteSchema is actually ignored in APPEND mode. The type in the source data was
      // changed to double though, the data needs to be cast on write,
      // which only insertInto support.
      if (method == "insertInto") {
        v1Write(writer, method)
        checkAnswer(targetData.union(castValue(srcData, "string")))
      } else {
        val e = intercept[DeltaAnalysisException] {
          v1Write(writer, method)
        }
        assert(e.getErrorClass == "DELTA_FAILED_TO_MERGE_FIELDS")
      }
    }

    dpoTest(s"DataframeV1($method): ErrorIfExists mode ") { conf =>
      val writer = v1Writer(sourceData, SaveMode.ErrorIfExists, conf)
      if (method == "insertInto") {
        v1Write(writer, method)
        checkAnswer(targetData.union(sourceData))
      } else {
        val e = intercept[AnalysisException] {
          v1Write(writer, method)
        }
        if (method == "save") {
          assert(e.getErrorClass == "DELTA_PATH_EXISTS")
        } else {
          assert(e.getErrorClass == "TABLE_OR_VIEW_ALREADY_EXISTS")
        }
      }
    }

    dpoTest(s"DataframeV1($method): Ignore mode") { conf =>
      if (method == "insertInto") {
        v1Write(v1Writer(sourceData, SaveMode.Ignore, conf), method)
        checkAnswer(targetData.union(sourceData))
      } else {
        checkAnswer(targetData)
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // DataframeV2
  ///////////////////////////////////////////////////////////////////////////
  private def v2Writer(df: DataFrame, dpoConf: DPOConf): DataFrameWriterV2[Row] = {
    var writer = df.writeTo(tempTable)
    if (dpoConf.writerOption) {
      writer = writer.option("partitionOverwriteMode", "dynamic")
    }
    writer
  }

  private def v2CreateWriter(df: DataFrame, dpoConf: DPOConf): CreateTableWriter[Row] =
    v2Writer(df, dpoConf).using("delta").partitionedBy(col("part"))

  dpoTest(s"DataframeV2: create") { dpoConf =>
    val e = intercept[AnalysisException] {
      v2CreateWriter(sourceData, dpoConf).create()
    }
    assert(e.getErrorClass == "TABLE_OR_VIEW_ALREADY_EXISTS")
  }


  dpoTest(s"DataframeV2: append") { dpoConf =>
    v2Writer(sourceData, dpoConf).append()
    checkAnswer(targetData.union(sourceData))
  }

  dpoTest(s"DataframeV2: overwritePartitions") { dpoConf =>
    v2Writer(sourceData, dpoConf).overwritePartitions()
    checkAnswer(expectedDpoData)
  }

  for {
    overwriteSchema <- BOOLEAN_DOMAIN
    mergeSchema <- BOOLEAN_DOMAIN
  } dpoTest("DataframeV2: overwritePartitions with different additive schema, " +
    s"overwriteSchema=$overwriteSchema, mergeSchema=$mergeSchema") { dpoConf =>
    val srcData = sourceData.withColumn("newCol", lit(42))
    val writer = v2Writer(srcData, dpoConf)
      .option("overwriteSchema", overwriteSchema.toString)
      .option("mergeSchema", mergeSchema.toString)
    if (overwriteSchema) {
      // No DPO because overwriteSchema + unpartitioned source DF.
      writer.overwritePartitions()
      checkAnswer(srcData)
    } else if (mergeSchema) {
      writer.overwritePartitions()
      targetData.filter("id != 1").withColumn("newCol", lit(null)).union(srcData)
    } else {
      val e = intercept[DeltaAnalysisException] {
        writer.overwritePartitions()
      }
      assert(e.getErrorClass == "DELTA_METADATA_MISMATCH")
    }
  }

  for {
    overwriteSchema <- BOOLEAN_DOMAIN
    mergeSchema <- BOOLEAN_DOMAIN
  } dpoTest("DataframeV2: overwritePartitions with different non-additive schema, " +
      s"overwriteSchema=$overwriteSchema, mergeSchema=$mergeSchema") { dpoConf =>
    val srcData = castValue(sourceData.withColumn("value", col("id")), "double")
    val writer = v2Writer(srcData, dpoConf)
      .option("overwriteSchema", overwriteSchema.toString)
      .option("mergeSchema", mergeSchema.toString)
    writer.overwritePartitions()
    if (overwriteSchema) {
      // No DPO because overwriteSchema + unpartitioned source DF.
      checkAnswer(srcData)
    } else {
      // Now supports implicit casting similar to V1
      checkAnswer(targetData.filter("id != 1").union(castValue(srcData, "string")))
    }
  }

  dpoTest(s"DataframeV2: overwrite") { dpoConf =>
    if (dpoConf.writerOption) {
      val e = intercept[DeltaIllegalArgumentException] {
        v2Writer(sourceData, dpoConf).overwrite($"id" === 99) // always false
      }
      assert(e.getErrorClass == "DELTA_REPLACE_WHERE_WITH_DYNAMIC_PARTITION_OVERWRITE")
    } else {
      withSQLConf(DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED.key -> "false") {
        v2Writer(sourceData, dpoConf).overwrite($"id" === 99) // always false
      }
      checkAnswer(targetData.union(sourceData))
    }
  }

  for {
    overwriteSchema <- BOOLEAN_DOMAIN
    mergeSchema <- BOOLEAN_DOMAIN
  } dpoTest(s"DataframeV2: overwrite with different additive schema, " +
      s"overwriteSchema=$overwriteSchema, mergeSchema=$mergeSchema") { dpoConf =>
    withSQLConf(DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED.key -> "false") {
      val srcData = sourceData.withColumn("newCol", lit(1))
      val writer = v2Writer(srcData, dpoConf)
        .option("overwriteSchema", overwriteSchema.toString)
        .option("mergeSchema", mergeSchema.toString)

      if (mergeSchema) {
        if (dpoConf.writerOption) {
          val e = intercept[DeltaIllegalArgumentException] {
            writer.overwrite($"id" === 99) // always false
          }
          assert(e.getErrorClass == "DELTA_REPLACE_WHERE_WITH_DYNAMIC_PARTITION_OVERWRITE")
        } else {
          writer.overwrite($"id" === 99) // always false
          checkAnswer(targetData.withColumn("newCol", lit(null)).union(srcData))
        }
      } else {
        val e = intercept[DeltaAnalysisException] {
          writer.overwrite($"id" === 99) // always false
        }
        assert(e.getErrorClass == "DELTA_METADATA_MISMATCH")
      }
    }
  }

  for {
    overwriteSchema <- BOOLEAN_DOMAIN
    mergeSchema <- BOOLEAN_DOMAIN
  } dpoTest(s"DataframeV2: overwrite with different non-additive schema, " +
      s"overwriteSchema=$overwriteSchema, mergeSchema=$mergeSchema") { dpoConf =>
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val srcData = castValue(sourceData, "double")
      val writer = v2Writer(srcData, dpoConf)
        .option("overwriteSchema", overwriteSchema.toString)
        .option("mergeSchema", mergeSchema.toString)
      if (overwriteSchema) {
        checkError(
          exception = intercept[DeltaAnalysisException] {
            writer.overwrite($"id" === 99) // always false
          },
          condition = "DELTA_FAILED_TO_MERGE_FIELDS",
          parameters = Map("currentField" -> "value", "updateField" -> "value")
        )
      } else {
        if (dpoConf.writerOption) {
          // With DPO as writer option + replaceWhere, we get conflict error.
          val e = intercept[DeltaIllegalArgumentException] {
            writer.overwrite($"id" === 99) // always false
          }
          assert(e.getErrorClass == "DELTA_REPLACE_WHERE_WITH_DYNAMIC_PARTITION_OVERWRITE")
        } else {
          // With DPO only in SQL config, replaceWhere takes precedence.
          // Since the written data doesn't match the predicate, we get mismatch error
          // when constraint check is enabled.
          val e = intercept[DeltaAnalysisException] {
            writer.overwrite($"id" === 99) // always false
          }
          assert(e.getErrorClass == "DELTA_REPLACE_WHERE_MISMATCH.INVARIANT_VIOLATION")
        }
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // SQL
  ///////////////////////////////////////////////////////////////////////////
  private val tempSourceTable = "source"

  private def dpoSqlTest(name: String)(testBody: => Unit): Unit = {
    test(name) {
      withTable(tempSourceTable) {
        withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
          testBody
        }
      }
    }
  }

  private def insertWithSchemaEvolution(
      table: String,
      values: String,
      overwrite: Boolean = true): Unit = {
    assume(
      DeltaTestUtils.sparkVersionBucket(spark) == "4.2+",
      "INSERT WITH SCHEMA EVOLUTION requires Spark 4.2 or newer")
    val mode = if (overwrite) "OVERWRITE" else "INTO"
    sql(s"INSERT WITH SCHEMA EVOLUTION $mode $table $values")
  }


  dpoSqlTest("SQL: INSERT INTO") {
    sourceData.write
      .format("delta")
      .saveAsTable(tempSourceTable)
    sql(s"INSERT INTO $tempTable SELECT * FROM $tempSourceTable")
    checkAnswer(targetData.union(sourceData))
  }

  dpoSqlTest("SQL: INSERT OVERWRITE") {
    sourceData.write
      .format("delta")
      .saveAsTable(tempSourceTable)
    sql(s"INSERT OVERWRITE $tempTable SELECT * FROM $tempSourceTable")
    checkAnswer(expectedDpoData)
  }

  test("raw string DPO matching replaces an INT partition widened to BIGINT") {
    val table = "dpo_raw_string_integral_widening"
    withTable(table) {
      withSQLConf(
        PARTITION_OVERWRITE_MODE.key -> "dynamic",
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_TYPE_WIDENING_FIX_ENABLED.key -> "true",
        // Cover the supported opt-out from typed parsing, not the default matching path.
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "false") {
        sql(s"""
          |CREATE TABLE $table (id INT, part INT)
          |USING DELTA
          |PARTITIONED BY (part)
          |TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
          |""".stripMargin)
        sql(s"INSERT INTO $table VALUES (10, 1), (20, 2)")
        val records = Log4jUsageLogger.track {
          insertWithSchemaEvolution(table, s"""
            |VALUES (30, CAST(2 AS BIGINT))
            |""".stripMargin)
        }
        val result = sql(s"SELECT id, part FROM $table")
        assert(result.schema("part").dataType == LongType)
        checkAnswer(result, Seq(Row(10, 1L), Row(30, 2L)))
        // Schema-change logging is independent of typed partition parsing.
        val events = records.filter { record =>
          record.tags.get("opType")
            .contains("delta.dynamicPartitionOverwrite.partitionTypeWidening")
        }
        assert(events.size == 1)
        val blob = JsonUtils.fromJson[Map[String, Any]](events.head.blob)
        assert(blob("parsePartitionValues") == false)
      }
    }
  }

  for (partitionTypeWideningFixEnabled <- BOOLEAN_DOMAIN) {
    test("DPO replaces FLOAT partitions with the same value under the current DOUBLE schema, " +
        s"partitionTypeWideningFixEnabled=$partitionTypeWideningFixEnabled") {
      val table = "dpo_float_partition_current_type"
      withTable(table) {
        withSQLConf(
          PARTITION_OVERWRITE_MODE.key -> "dynamic",
          DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_TYPE_WIDENING_FIX_ENABLED.key ->
            partitionTypeWideningFixEnabled.toString,
          DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "true") {
          sql(s"""
            |CREATE TABLE $table (id INT, part FLOAT)
            |USING DELTA
            |PARTITIONED BY (part)
            |TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
            |""".stripMargin)
          sql(s"INSERT INTO $table VALUES (10, CAST(0.1 AS FLOAT)), (20, CAST(0.2 AS FLOAT))")
          val records = Log4jUsageLogger.track {
            insertWithSchemaEvolution(table, s"""
              |VALUES (30, CAST(0.1 AS DOUBLE))
              |""".stripMargin)
          }
          val expected = if (partitionTypeWideningFixEnabled) {
            Seq(Row(20, 0.2d), Row(30, 0.1d))
          } else {
            Seq(Row(10, 0.1d), Row(20, 0.2d), Row(30, 0.1d))
          }
          checkAnswer(sql(s"SELECT id, part FROM $table"), expected)
          assert(records.count { record =>
            record.tags.get("opType")
              .contains("delta.dynamicPartitionOverwrite.partitionTypeWidening")
          } == 1)
        }
      }
    }
  }

  test("DPO preserves FLOAT partitions distinct from a FLOAT-to-DOUBLE cast") {
    val table = "dpo_float_partition_distinct_value"
    withTable(table) {
      withSQLConf(
        PARTITION_OVERWRITE_MODE.key -> "dynamic",
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_TYPE_WIDENING_FIX_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "true") {
        sql(s"""
          |CREATE TABLE $table (id INT, part FLOAT)
          |USING DELTA
          |PARTITIONED BY (part)
          |TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
          |""".stripMargin)
        sql(s"INSERT INTO $table VALUES (10, CAST(0.1 AS FLOAT)), (20, CAST(0.2 AS FLOAT))")
        val records = Log4jUsageLogger.track {
          insertWithSchemaEvolution(table, s"""
            |VALUES (30, CAST(CAST(0.1 AS FLOAT) AS DOUBLE))
            |""".stripMargin)
        }
        checkAnswer(
          sql(s"SELECT id, part FROM $table"),
          Seq(Row(10, 0.1d), Row(20, 0.2d), Row(30, 0.1f.toDouble)))
        // A type change is logged even when the incoming partition is distinct.
        val events = records.filter { record =>
          record.tags.get("opType")
            .contains("delta.dynamicPartitionOverwrite.partitionTypeWidening")
        }
        assert(events.size == 1)
        val blob = JsonUtils.fromJson[Map[String, Any]](events.head.blob)
        assert(blob("hasFilesToRemove") == false)
      }
    }
  }

  for {
    (fromType, toType, partitionValues) <- Seq(
      ("DECIMAL(10,2)", "DECIMAL(12,3)",
        Seq("-99999999.99", "99999999.99", "999999999.999")),
      ("DATE", "TIMESTAMP_NTZ",
        Seq("DATE '2024-01-01'", "DATE '2024-01-02'", "DATE '2024-01-03'")))
    (sessionAnsiEnabled, sessionTimeZone) <- if (toType == "TIMESTAMP_NTZ") {
      Seq(
        (false, "UTC"),
        (true, "UTC"),
        (false, "America/Los_Angeles"),
        (true, "America/Los_Angeles"))
    } else {
      Seq((false, "UTC"))
    }
  } {
    test(s"SQL overwrite replaces existing partition files for $fromType -> $toType, " +
        s"sessionAnsiEnabled=$sessionAnsiEnabled, sessionTimeZone=$sessionTimeZone") {
      val table = "dpo_type_variant"
      withTable(table) {
        withSQLConf(
          SQLConf.ANSI_ENABLED.key -> sessionAnsiEnabled.toString,
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> sessionTimeZone,
          PARTITION_OVERWRITE_MODE.key -> "dynamic",
          DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_TYPE_WIDENING_FIX_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "true") {
          val timestampNtzProperty = if (toType == "TIMESTAMP_NTZ") {
            ", 'delta.feature.timestampNtz' = 'supported'"
          } else {
            ""
          }
          sql(s"""
            |CREATE TABLE $table (id INT, value STRING, part $fromType)
            |USING DELTA
            |PARTITIONED BY (part)
            |TBLPROPERTIES ('delta.enableTypeWidening' = 'true'$timestampNtzProperty)
            |""".stripMargin)

          val initialValues = partitionValues.map(v => s"CAST($v AS $fromType)")
          // Match values as readers interpret the persisted strings under the updated schema.
          val widenedValues = partitionValues.map(v => s"CAST($v AS $toType)")
          sql(s"""
            |INSERT INTO $table
            |VALUES (10, 'old', ${initialValues(0)}), (20, 'old', ${initialValues(1)})
            |""".stripMargin)
          insertWithSchemaEvolution(table, s"""
            |VALUES (30, 'new', ${widenedValues(1)}), (40, 'new', ${widenedValues(2)})
            |""".stripMargin)

          val result = sql(s"SELECT id, value, part FROM $table")
          val expected = sql(s"""
            |SELECT * FROM VALUES
            |  (10, 'old', ${widenedValues(0)}),
            |  (30, 'new', ${widenedValues(1)}),
            |  (40, 'new', ${widenedValues(2)})
            |AS expected(id, value, part)
            |""".stripMargin)
          assert(result.schema("part").dataType == expected.schema("part").dataType)
          checkAnswer(result, expected)
        }
      }
    }
  }

  test("widening two of four partition columns replaces only the matching partition tuple") {
    val table = "dpo_four_partition_columns_widening"
    withTable(table) {
      withSQLConf(
        PARTITION_OVERWRITE_MODE.key -> "dynamic",
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_TYPE_WIDENING_FIX_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "true") {
        sql(s"""
          |CREATE TABLE $table (id INT, p1 INT, p2 DECIMAL(10,2), category STRING, bucket INT)
          |USING DELTA
          |PARTITIONED BY (p1, p2, category, bucket)
          |TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
          |""".stripMargin)
        // Each retained partition differs from the incoming tuple in exactly one component.
        sql(s"""
          |INSERT INTO $table VALUES
          |  (10, 2, 99999999.98, 'a', 1),
          |  (20, 1, 99999999.99, 'a', 1),
          |  (30, 2, 99999999.99, 'a', 1),
          |  (50, 2, 99999999.99, 'b', 1),
          |  (60, 2, 99999999.99, 'a', 2)
          |""".stripMargin)
        insertWithSchemaEvolution(table, s"""
          |VALUES (40, CAST(2 AS BIGINT), CAST(99999999.99 AS DECIMAL(12,3)), 'a', 1)
          |""".stripMargin)

        val result = sql(s"SELECT id, p1, p2, category, bucket FROM $table")
        val expected = sql("""
          |SELECT * FROM VALUES
          |  (10, CAST(2 AS BIGINT), CAST(99999999.98 AS DECIMAL(12,3)), 'a', 1),
          |  (20, CAST(1 AS BIGINT), CAST(99999999.99 AS DECIMAL(12,3)), 'a', 1),
          |  (40, CAST(2 AS BIGINT), CAST(99999999.99 AS DECIMAL(12,3)), 'a', 1),
          |  (50, CAST(2 AS BIGINT), CAST(99999999.99 AS DECIMAL(12,3)), 'b', 1),
          |  (60, CAST(2 AS BIGINT), CAST(99999999.99 AS DECIMAL(12,3)), 'a', 2)
          |AS expected(id, p1, p2, category, bucket)
          |""".stripMargin)
        assert(result.schema("p1").dataType == expected.schema("p1").dataType)
        assert(result.schema("p2").dataType == expected.schema("p2").dataType)
        assert(result.schema("category").dataType == StringType)
        assert(result.schema("bucket").dataType == IntegerType)
        checkAnswer(result, expected)
      }
    }
  }

  test("current-schema DPO matching handles physical partition names and NULLs, " +
      "columnMappingMode=name") {
    val table = "dpo_current_schema_column_mapping"
    withTable(table) {
      withSQLConf(
        PARTITION_OVERWRITE_MODE.key -> "dynamic",
        DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> NameMapping.name,
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_TYPE_WIDENING_FIX_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "true") {
        sql(s"""
          |CREATE TABLE $table (id INT, part INT, category STRING)
          |USING DELTA
          |PARTITIONED BY (part, category)
          |TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
          |""".stripMargin)
        sql(s"""
          |INSERT INTO $table VALUES
          |  (10, 1, 'untouched'), (20, 2, NULL), (30, NULL, 'a')
          |""".stripMargin)
        insertWithSchemaEvolution(table, s"""
          |VALUES
          |  (40, CAST(2 AS BIGINT), CAST(NULL AS STRING)),
          |  (50, CAST(NULL AS BIGINT), 'a')
          |""".stripMargin)

        val result = sql(s"SELECT id, part, category FROM $table")
        assert(result.schema("part").dataType == LongType)
        checkAnswer(
          result,
          Seq(Row(10, 1L, "untouched"), Row(40, 2L, null), Row(50, null, "a")))
      }
    }
  }

  test("widening a NULL partition replaces its existing files") {
    val table = "dpo_null_partition"
    withTable(table) {
      withSQLConf(
        PARTITION_OVERWRITE_MODE.key -> "dynamic",
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_TYPE_WIDENING_FIX_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "true") {
        sql(s"""
          |CREATE TABLE $table (id INT, part INT)
          |USING DELTA
          |PARTITIONED BY (part)
          |TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
          |""".stripMargin)
        sql(s"INSERT INTO $table VALUES (10, 1), (20, NULL)")
        insertWithSchemaEvolution(table, s"""
          |VALUES (30, CAST(NULL AS BIGINT))
          |""".stripMargin)

        val result = sql(s"SELECT id, part FROM $table")
        assert(result.schema("part").dataType == LongType)
        checkAnswer(result, Seq(Row(10, 1L), Row(30, null)))
      }
    }
  }

  for ((partValue, categoryValue, overwrittenId) <- Seq(
      ("2", "'a'", 20),
      ("NULL", "NULL", 50),
      ("2", "NULL", 30),
      ("NULL", "'a'", 40))) {
    test("widening one of two partition columns matches NULL combinations, " +
        s"part=$partValue, category=$categoryValue") {
      val table = "dpo_null_partition_combinations"
      withTable(table) {
        withSQLConf(
          PARTITION_OVERWRITE_MODE.key -> "dynamic",
          DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_TYPE_WIDENING_FIX_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "true") {
          sql(s"""
            |CREATE TABLE $table (id INT, part INT, category STRING)
            |USING DELTA
            |PARTITIONED BY (part, category)
            |TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
            |""".stripMargin)
          sql(s"""
            |INSERT INTO $table VALUES
            |  (10, 1, 'untouched'),
            |  (20, 2, 'a'),
            |  (30, 2, NULL),
            |  (40, NULL, 'a'),
            |  (50, NULL, NULL)
            |""".stripMargin)
          insertWithSchemaEvolution(table, s"""
            |VALUES (60, CAST($partValue AS BIGINT), CAST($categoryValue AS STRING))
            |""".stripMargin)

          val originalRows = Seq(
            Row(10, 1L, "untouched"),
            Row(20, 2L, "a"),
            Row(30, 2L, null),
            Row(40, null, "a"),
            Row(50, null, null))
          val incomingRow = Row(
            60,
            if (partValue == "NULL") null else 2L,
            if (categoryValue == "NULL") null else "a")
          val expected = originalRows.filterNot(_.getInt(0) == overwrittenId) :+ incomingRow
          val result = sql(s"SELECT id, part, category FROM $table")
          assert(result.schema("part").dataType == LongType)
          checkAnswer(result, expected)
        }
      }
    }
  }

  test("VOID incoming NULL replaces an existing NULL INT partition") {
    val table = "dpo_void_incoming_partition"
    withTable(table) {
      withSQLConf(
        PARTITION_OVERWRITE_MODE.key -> "dynamic",
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_TYPE_WIDENING_FIX_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "true") {
        sql(s"""
          |CREATE TABLE $table (id INT, part INT)
          |USING DELTA
          |PARTITIONED BY (part)
          |TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
          |""".stripMargin)
        sql(s"INSERT INTO $table VALUES (10, 1), (20, NULL)")
        insertWithSchemaEvolution(table, """
          |VALUES (30, CAST(NULL AS VOID))
          |""".stripMargin)

        val result = sql(s"SELECT id, part FROM $table")
        assert(result.schema("part").dataType == IntegerType)
        checkAnswer(result, Seq(Row(10, 1), Row(30, null)))
      }
    }
  }

  for (resolveMetadataConflicts <- BOOLEAN_DOMAIN) {
    test("concurrent partition widening conflicts before DPO file matching, " +
        s"resolveMetadataConflicts=$resolveMetadataConflicts") {
      val table = "dpo_concurrent_widening"
      withTable(table) {
        withSQLConf(
          DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_TYPE_WIDENING_FIX_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "true",
          DeltaSQLConf.FEATURE_ENABLEMENT_CONFLICT_RESOLUTION_ENABLED.key ->
            resolveMetadataConflicts.toString) {
          sql(s"""
            |CREATE TABLE $table (id INT, part INT)
            |USING DELTA
            |PARTITIONED BY (part)
            |TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
            |""".stripMargin)
          sql(s"INSERT INTO $table VALUES (10, 2)")
          val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
          val txn = deltaLog.startTransaction()
          val widenedSchema = StructType(txn.metadata.schema.fields.map { field =>
            if (field.name == "part") field.copy(dataType = LongType) else field
          })
          txn.updateMetadata(txn.metadata.copy(schemaString = widenedSchema.json))
          val incomingFile = txn.snapshot.allFiles.collect().head.copy(
            path = "pending-dpo.parquet",
            dataChange = true)
          val filesToRemove = txn.filterFiles(Seq(incomingFile))
          assert(filesToRemove.nonEmpty)

          insertWithSchemaEvolution(table, s"""
            |VALUES (20, CAST(2 AS BIGINT))
            |""".stripMargin, overwrite = false)
          intercept[MetadataChangedException] {
            txn.commit(
              filesToRemove.map(_.remove) :+ incomingFile,
              DeltaOperations.ManualUpdate)
          }
          checkAnswer(sql(s"SELECT id, part FROM $table"), Seq(Row(10, 2L), Row(20, 2L)))
        }
      }
    }
  }

  for (partitionTypeWideningFixEnabled <- BOOLEAN_DOMAIN) {
    test("partition widening logs schema changes independently of the fix flag, " +
        s"partitionTypeWideningFixEnabled=$partitionTypeWideningFixEnabled") {
      val table = "dpo_partition_widening_rollout"
      withTable(table) {
        withSQLConf(
          PARTITION_OVERWRITE_MODE.key -> "dynamic",
          DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_TYPE_WIDENING_FIX_ENABLED.key ->
            partitionTypeWideningFixEnabled.toString,
          DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "true") {
          sql(s"""
            |CREATE TABLE $table (id INT, value STRING, part INT)
            |USING DELTA
            |PARTITIONED BY (part)
            |TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
            |""".stripMargin)
          sql(s"INSERT INTO $table VALUES (10, 'old', 1), (20, 'old', 2)")

          val records = Log4jUsageLogger.track {
            insertWithSchemaEvolution(table, s"""
              |VALUES (30, 'new', CAST(2 AS BIGINT))
              |""".stripMargin)
          }
          val events = records.filter { record =>
            record.metric == MetricDefinitions.EVENT_TAHOE.name &&
              record.tags.get("opType")
                .contains("delta.dynamicPartitionOverwrite.partitionTypeWidening")
          }
          assert(events.size == 1)
          val blob = JsonUtils.fromJson[Map[String, Any]](events.head.blob)
          assert(blob("partitionTypeWideningFixEnabled") == partitionTypeWideningFixEnabled)
          assert(blob("parsePartitionValues") == true)
          assert(blob("hasFilesToRemove") == partitionTypeWideningFixEnabled)
          assert(blob("numIncomingFiles").toString.toInt > 0)
          // The event describes changed types, not specific overlapping partitions.
          assert(blob("changes") == Seq(Map("fromType" -> "INT", "toType" -> "BIGINT")))

          val expected = if (partitionTypeWideningFixEnabled) {
            Seq(Row(10, "old", 1L), Row(30, "new", 2L))
          } else {
            Seq(Row(10, "old", 1L), Row(20, "old", 2L), Row(30, "new", 2L))
          }
          checkAnswer(sql(s"SELECT id, value, part FROM $table"), expected)
        }
      }
    }
  }

  test("widening only a data column leaves DPO unchanged") {
    val table = "dpo_data_column_widening_rollout"
    withTable(table) {
      withSQLConf(
        PARTITION_OVERWRITE_MODE.key -> "dynamic",
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_TYPE_WIDENING_FIX_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "true") {
        sql(s"""
          |CREATE TABLE $table (id INT, value STRING, part INT)
          |USING DELTA
          |PARTITIONED BY (part)
          |TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
          |""".stripMargin)
        sql(s"INSERT INTO $table VALUES (10, 'old', 1), (20, 'old', 2)")

        val records = Log4jUsageLogger.track {
          insertWithSchemaEvolution(table, s"""
            |VALUES (CAST(30 AS BIGINT), 'new', 2)
            |""".stripMargin)
        }
        val events = records.filter { record =>
          record.metric == MetricDefinitions.EVENT_TAHOE.name &&
            record.tags.get("opType")
              .contains("delta.dynamicPartitionOverwrite.partitionTypeWidening")
        }
        assert(events.isEmpty)
        val result = sql(s"SELECT id, value, part FROM $table")
        assert(result.schema("id").dataType == LongType)
        assert(result.schema("part").dataType == IntegerType)
        checkAnswer(result, Seq(Row(10L, "old", 1), Row(30L, "new", 2)))
      }
    }
  }

  dpoSqlTest("SQL: INSERT OVERWRITE with different schema") {
    val srcData = castValue(sourceData.withColumn("value", col("id")), "double")
    srcData.write
      .format("delta")
      .saveAsTable(tempSourceTable)
    sql(s"INSERT OVERWRITE $tempTable SELECT * FROM $tempSourceTable")
    // InsertInto will cast the input.
    checkAnswer(
      targetData.filter("id != 1").union(castValue(srcData, "string")))
  }

  dpoSqlTest("SQL: INSERT INTO REPLACE WHERE") {
    sourceData.write
      .format("delta")
      .saveAsTable(tempSourceTable)
    withSQLConf(DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED.key -> "false") {
      sql(s"INSERT INTO $tempTable REPLACE WHERE id = 99 SELECT * FROM $tempSourceTable")
    }
    checkAnswer(targetData.union(sourceData))
  }

  dpoSqlTest("SQL: CREATE TABLE") {
    sourceData.write
      .format("delta")
      .saveAsTable(tempSourceTable)
    val e = intercept[AnalysisException] {
      sql(
        s"""
           |CREATE TABLE $tempTable
           |USING delta
           |PARTITIONED BY (part)
           |AS SELECT * FROM $tempSourceTable
           |""".stripMargin)
    }
    assert(e.getErrorClass == "TABLE_OR_VIEW_ALREADY_EXISTS")
  }


  for {
    columnMappingMode <- Seq(NoMapping, NameMapping, IdMapping)
    enableANSI <- BOOLEAN_DOMAIN
  } test(
    s"should be able to handle TimestampType partition value in different formats, " +
    s"column mapping = ${columnMappingMode.name}, enableANSI = $enableANSI"
  ) {
    for (enableFix <- BOOLEAN_DOMAIN) {
      withSQLConf(
        PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString,
        DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> columnMappingMode.name,
        SQLConf.ANSI_ENABLED.key -> enableANSI.toString,
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key ->
        enableFix.toString
      ) {
        withTimeZone("UTC") {
          withTempDir { tempDir =>
            val tempPath = tempDir.getCanonicalPath
            val tempTable = s"delta.`$tempPath`"
            withTable(tempTable) {
              sql(s"""
                  |CREATE OR REPLACE TABLE $tempTable (id LONG, value TIMESTAMP)
                  |USING delta
                  |PARTITIONED BY (value)
                  |""".stripMargin)

              // Insert data with non-UTC timestamp partition values initially.
              withSQLConf(DeltaSQLConf.UTC_TIMESTAMP_PARTITION_VALUES.key -> "false") {
                sql(s"""
                  |INSERT INTO $tempTable
                  |VALUES
                  |(0, '2000-01-01 12:00:00'),
                  |(0, '2000-01-01T12:00:00Z'),
                  |(0, '2000-01-01T12:00:00.123456UTC+8'),
                  |(0, '2000-01-01T12:00:00.123456UTC+08:00'),
                  |(0, '1900-01-01 12:00:00')
                  |""".stripMargin)
              }

              // After normalization, there are 3 partition values written in non-UTC format
              val deltaLog = DeltaLog.forTable(spark, tempPath)
              val partitionValuesNonUTC = Set(
                "2000-01-01 12:00:00",
                "2000-01-01 04:00:00.123456",
                "1900-01-01 12:00:00"
              )

              def getFilePartitionValues: Set[String] = {
                deltaLog
                  .update()
                  .allFiles
                  .collect()
                  .flatMap(_.partitionValues.values)
                  .toSet
              }

              assert(getFilePartitionValues == partitionValuesNonUTC)

              // Enable writing partition values in UTC format for DPO
              withSQLConf(DeltaSQLConf.UTC_TIMESTAMP_PARTITION_VALUES.key -> "true") {
                sql(s"INSERT OVERWRITE TABLE $tempTable (SELECT * FROM $tempTable)")
              }

              val result = spark.read.format("delta").load(tempPath).collect()

              // Expected partition values after DPO
              val partitionValuesUTC = Set(
                "2000-01-01T12:00:00.000000Z",
                "2000-01-01T04:00:00.123456Z",
                "1900-01-01T12:00:00.000000Z"
              )

              if (enableFix) {
                // With the fix, DPO correctly identifies and overwrites existing partitions,
                // even if their string representation differs in formats
                assert(result.length == 5)
                assert(getFilePartitionValues == partitionValuesUTC)
              } else {
                // Without the fix, DPO uses raw strings when comparing partition values and
                // treats UTC-formatted and non-UTC-formatted partition values as distinct,
                // leading to duplicated data
                assert(result.length == 10)
                assert(getFilePartitionValues == partitionValuesNonUTC ++ partitionValuesUTC)
              }
            }
          }
        }
      }
    }
  }

  test("partition value parsing for timestamp types throws if the value string is invalid") {
    for {
      enableANSI <- BOOLEAN_DOMAIN
      partColType <- Seq(TimestampType, TimestampNTZType)
    } {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> enableANSI.toString) {
        val partitionSchema = new StructType().add("part", partColType)
        val timezoneId = "UTC"

        def checkThrows(input: String): Unit = {
          val e = intercept[DeltaRuntimeException] {
            PartitionUtils.parsePartitionValues(
              Map("part" -> input),
              partitionSchema,
              timezoneId,
              validatePartitionColumns = true)
          }
          assert(e.getErrorClass == "DELTA_PARTITION_COLUMN_CAST_FAILED")
        }

        // Invalid values - throw an expected exception
        checkThrows("")
        checkThrows("0")
        checkThrows("not-a-timestamp")
        checkThrows("2000-01- 12:00:00")
        checkThrows("2000-01-00 12:00:00")
        checkThrows("2000-01-32 12:00:00")
        checkThrows("2000-01-01T12:00:60Z")
        checkThrows("2000-01-01T12:00:60.1234567Z")
        checkThrows("2000-01-01, 12:00:00 CET")
      }
    }
  }


  for (typeWideningFixEnabled <- BOOLEAN_DOMAIN) {
    test("any errors occurred during partition value parsing should be caught, " +
        s"typeWideningFixEnabled=$typeWideningFixEnabled") {
      import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions}
      withSQLConf(
        PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString,
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_PARSE_PARTITION_VALUES.key -> "true",
        DeltaSQLConf.DELTA_DYNAMIC_PARTITION_OVERWRITE_TYPE_WIDENING_FIX_ENABLED.key ->
          typeWideningFixEnabled.toString,
        DeltaSQLConf.DELTA_FAIL_ON_PARTITION_VALUE_PARSING_ERROR.key -> "false" // let it fallback
      ) {
        withTempDir { tempDir =>
          val tempPath = tempDir.getCanonicalPath
          val tempTable = s"delta.`$tempPath`"
          withTable(tempTable) {
            sql(s"""
              |CREATE OR REPLACE TABLE $tempTable (id INT, partition_col INT)
              |USING delta
              |PARTITIONED BY (partition_col)
              |""".stripMargin)

            // Insert 3 rows
            sql(s"INSERT INTO $tempTable VALUES (0, 0), (1, 1), (2, 2)")

            // Manually create an invalid state by modifying the table's metadata and changing
            // the partition column name to upper case.
            // This should trigger a partition value parsing error.
            val deltaLog = DeltaLog.forTable(spark, tempPath)
            val oldMetadata = deltaLog.update().metadata
            assert(oldMetadata.partitionColumns == Seq("partition_col"))

            val newMetadata = oldMetadata.copy(
              schemaString = oldMetadata.schemaString.replace("partition_col", "PARTITION_COL"),
              partitionColumns = Seq("PARTITION_COL"))
            deltaLog.startTransaction().commit(Seq(newMetadata), DeltaOperations.ManualUpdate)
            assert(deltaLog.update().metadata.partitionColumns == Seq("PARTITION_COL"))

            // Do a DPO, which should fail during partition value parsing, then fallback to old code
            val events = Log4jUsageLogger.track {
              sql(s"INSERT OVERWRITE TABLE $tempTable (SELECT * FROM $tempTable)")
            }

            // Check we have caught the exception and logged an event
            val parsingErrorEvents = events.filter { e =>
              e.metric == MetricDefinitions.EVENT_TAHOE.name &&
                e.tags
                .get("opType")
                .contains("delta.dynamicPartitionOverwrite.partitionValueParsingError.caseMismatch")
            }
            assert(parsingErrorEvents.size == 1)

            // Check that DPO produced the wrong result after it falls back to previous behavior
            checkAnswer(
              sql(s"SELECT id FROM $tempTable ORDER BY id"),
              Seq(Row(0), Row(0), Row(1), Row(1), Row(2), Row(2))
            )
          }
        }
      }
    }
  }
}
