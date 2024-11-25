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

import java.io.File

import scala.collection.mutable.ListBuffer

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.GeneratedAsIdentityType.{GeneratedAlways, GeneratedAsIdentityType, GeneratedByDefault}
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.commons.io.FileUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

/**
 * General test suite for identity columns.
 */
trait IdentityColumnSuiteBase extends IdentityColumnTestUtils {

  import testImplicits._

  test("Don't allow IDENTITY column in the schema if the feature is disabled") {
    val tblName = getRandomTableName
    withSQLConf(DeltaSQLConf.DELTA_IDENTITY_COLUMN_ENABLED.key -> "false") {
      withTable(tblName) {
        val e = intercept[DeltaUnsupportedTableFeatureException] {
          createTableWithIdColAndIntValueCol(
            tblName, GeneratedByDefault, startsWith = None, incrementBy = None)
        }
        val errorMsg = e.getMessage
        assert(errorMsg.contains("requires writer table feature(s) that are unsupported"))
        assert(errorMsg.contains(IdentityColumnsTableFeature.name))
      }
    }
  }

  // Build expected schema of the following table definition for verification:
  // CREATE TABLE tableName (
  //   id BIGINT <keyword> IDENTITY (START WITH <start> INCREMENT BY <step>),
  //   value INT
  // );
  private def expectedSchema(
      generatedAsIdentityType: GeneratedAsIdentityType,
      start: Long = IdentityColumn.defaultStart,
      step: Long = IdentityColumn.defaultStep): StructType = {
    val colFields = new ListBuffer[StructField]

    val allowExplicitInsert = generatedAsIdentityType == GeneratedByDefault
    val builder = new MetadataBuilder()
    builder.putBoolean(DeltaSourceUtils.IDENTITY_INFO_ALLOW_EXPLICIT_INSERT,
      allowExplicitInsert)
    builder.putLong(DeltaSourceUtils.IDENTITY_INFO_START, start)
    builder.putLong(DeltaSourceUtils.IDENTITY_INFO_STEP, step)
    colFields += StructField("id", LongType, true, builder.build())
    colFields += StructField("value", IntegerType)

    StructType(colFields.toSeq)
  }

  test("various configuration") {
    val starts = Seq(
      Long.MinValue,
      Integer.MIN_VALUE.toLong,
      -100L,
      0L,
      1000L,
      Integer.MAX_VALUE.toLong,
      Long.MaxValue
    )
    val steps = Seq(
      Long.MinValue,
      Integer.MIN_VALUE.toLong,
      -100L,
      1000L,
      Integer.MAX_VALUE.toLong,
      Long.MaxValue
    )
    for {
      generatedAsIdentityType <- GeneratedAsIdentityType.values
      startsWith <- starts
      incrementBy <- steps
    } {
      val tblName = getRandomTableName
      withTable(tblName) {
        createTableWithIdColAndIntValueCol(
          tblName, generatedAsIdentityType, Some(startsWith), Some(incrementBy))
        val table = DeltaLog.forTable(spark, TableIdentifier(tblName))
        val actualSchema =
          DeltaColumnMapping.dropColumnMappingMetadata(table.snapshot.metadata.schema)
        assert(actualSchema === expectedSchema(generatedAsIdentityType, startsWith, incrementBy))
      }
    }
  }

  test("default configuration") {
    for {
      generatedAsIdentityType <- GeneratedAsIdentityType.values
      startsWith <- Seq(Some(1L), None)
      incrementBy <- Seq(Some(1L), None)
    } {
      val tblName = getRandomTableName
      withTable(tblName) {
        createTableWithIdColAndIntValueCol(
          tblName, generatedAsIdentityType, startsWith, incrementBy)
        val table = DeltaLog.forTable(spark, TableIdentifier(tblName))
        val actualSchema =
          DeltaColumnMapping.dropColumnMappingMetadata(table.snapshot.metadata.schema)
        assert(actualSchema === expectedSchema(generatedAsIdentityType))
      }
    }
  }

  test("logging") {
    val tblName = getRandomTableName
    withTable(tblName) {
      val eventsDefinition = Log4jUsageLogger.track {
        createTable(
          tblName,
          Seq(
            IdentityColumnSpec(
              GeneratedByDefault,
              startsWith = Some(1),
              incrementBy = Some(1),
              colName = "id1"
            ),
            IdentityColumnSpec(
              GeneratedAlways,
              startsWith = Some(1),
              incrementBy = Some(1),
              colName = "id2"
            ),
            IdentityColumnSpec(
              GeneratedAlways,
              startsWith = Some(1),
              incrementBy = Some(1),
              colName = "id3"
            ),
            TestColumnSpec(colName = "value", dataType = IntegerType)
          )
        )
      }.filter { e =>
        e.tags.get("opType").exists(_ == IdentityColumn.opTypeDefinition)
      }
      assert(eventsDefinition.size == 1)
      assert(JsonUtils.fromJson[Map[String, String]](eventsDefinition.head.blob)
        .get("numIdentityColumns").exists(_ == "3"))

      val eventsWrite = Log4jUsageLogger.track {
        sql(s"INSERT INTO $tblName (id1, value) VALUES (1, 10), (2, 20)")
      }.filter { e =>
        e.tags.get("opType").exists(_ == IdentityColumn.opTypeWrite)
      }
      assert(eventsWrite.size == 1)
      val data = JsonUtils.fromJson[Map[String, String]](eventsWrite.head.blob)
      assert(data.get("numInsertedRows").exists(_ == "2"))
      assert(data.get("generatedIdentityColumnNames").exists(_ == "id2,id3"))
      assert(data.get("generatedIdentityColumnCount").exists(_ == "2"))
      assert(data.get("explicitIdentityColumnNames").exists(_ == "id1"))
      assert(data.get("explicitIdentityColumnCount").exists(_ == "1"))
    }
  }

  test("reading table should not see identity column properties") {
    def verifyNoIdentityColumn(id: Int, f: () => Dataset[_]): Unit = {
      assert(!ColumnWithDefaultExprUtils.hasIdentityColumn(f().schema), s"test $id failed")
    }

    val tblName = getRandomTableName
    withTable(tblName) {
      createTable(
        tblName,
        Seq(
          IdentityColumnSpec(GeneratedByDefault),
          TestColumnSpec(colName = "part", dataType = LongType),
          TestColumnSpec(colName = "value", dataType = StringType)
        ),
        partitionedBy = Seq("part")
      )

      sql(
        s"""
           |INSERT INTO $tblName (part, value) VALUES
           |  (1, "one"),
           |  (2, "two"),
           |  (3, "three")
           |""".stripMargin)
      val path = DeltaLog.forTable(spark, TableIdentifier(tblName)).dataPath.toString

      val commands: Seq[() => Dataset[_]] = Seq(
        () => spark.table(tblName),
        () => sql(s"SELECT * FROM $tblName"),
        () => sql(s"SELECT * FROM delta.`$path`"),
        () => spark.read.format("delta").load(path),
        () => spark.read.format("delta").table(tblName),
        () => spark.readStream.format("delta").load(path),
        () => spark.readStream.format("delta").table(tblName)
      )
      commands.zipWithIndex.foreach {
        case (f, id) => verifyNoIdentityColumn(id, f)
      }
      withTempDir { checkpointDir =>
        val q = spark.readStream.format("delta").table(tblName).writeStream
          .trigger(Trigger.Once)
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .foreachBatch { (df: DataFrame, _: Long) =>
            assert(!ColumnWithDefaultExprUtils.hasIdentityColumn(df.schema))
            ()
          }.start()
        try {
          q.processAllAvailable()
        } finally {
          q.stop()
        }
      }
      withTempDir { outputDir =>
        withTempDir { checkpointDir =>
          val q = spark.readStream.format("delta").table(tblName).writeStream
            .trigger(Trigger.Once)
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("delta")
            .start(outputDir.getCanonicalPath)
          try {
            q.processAllAvailable()
          } finally {
            q.stop()
          }
          val deltaLog = DeltaLog.forTable(spark, outputDir.getCanonicalPath)
          assert(deltaLog.snapshot.version >= 0)
          assert(!ColumnWithDefaultExprUtils.hasIdentityColumn(deltaLog.snapshot.schema))
        }
      }
    }
  }

  private def withWriterVersion5Table(func: String => Unit): Unit = {
    // The table on the following path is created with the following steps:
    // (1) Create a table with IDENTITY column using writer version 6
    //     CREATE TABLE $tblName (
    //       id BIGINT GENERATED BY DEFAULT AS IDENTITY,
    //       part INT,
    //       value STRING
    //     ) USING delta
    //     PARTITIONED BY (part)
    // (2) CTAS from the above table using writer version 5.
    // This will result in a table created using protocol (1, 2) with IDENTITY columns.
    val resourcePath = "src/test/resources/delta/identity_test_written_by_version_5"
    withTempDir { tempDir =>
      // Prepare a table that has the old writer version and identity columns.
      FileUtils.copyDirectory(new File(resourcePath), tempDir)
      val path = tempDir.getCanonicalPath
      val deltaLog = DeltaLog.forTable(spark, path)
      // Verify the table has old writer version and identity columns.
      assert(ColumnWithDefaultExprUtils.hasIdentityColumn(deltaLog.snapshot.schema))
      val writerVersionOnTable = deltaLog.snapshot.protocol.minWriterVersion
      assert(writerVersionOnTable < IdentityColumnsTableFeature.minWriterVersion)
      func(path)
    }
  }

  test("compatibility") {
    withWriterVersion5Table { v5TablePath =>
      // Verify initial data.
      checkAnswer(
        sql(s"SELECT * FROM delta.`$v5TablePath`"),
        Row(1, 1, "one") :: Row(2, 2, "two") :: Row(4, 3, "three") :: Nil
      )
      // Insert new data should generate correct IDENTITY values.
      sql(s"""INSERT INTO delta.`$v5TablePath` VALUES (5, 5, "five")""")
      checkAnswer(
        sql(s"SELECT COUNT(DISTINCT id) FROM delta.`$v5TablePath`"),
        Row(4L)
      )

      val deltaLog = DeltaLog.forTable(spark, v5TablePath)
      val protocolBeforeUpdate = deltaLog.snapshot.protocol

      // ALTER TABLE should drop the IDENTITY columns and keeps the protocol version unchanged.
      sql(s"ALTER TABLE delta.`$v5TablePath` ADD COLUMNS (value2 DOUBLE)")
      deltaLog.update()
      assert(deltaLog.snapshot.protocol == protocolBeforeUpdate)
      assert(!ColumnWithDefaultExprUtils.hasIdentityColumn(deltaLog.snapshot.schema))

      // Specifying a min writer version should not enable IDENTITY column.
      sql(s"ALTER TABLE delta.`$v5TablePath` SET TBLPROPERTIES ('delta.minWriterVersion'='4')")
      deltaLog.update()
      assert(deltaLog.snapshot.protocol == Protocol(1, 4))
      assert(!ColumnWithDefaultExprUtils.hasIdentityColumn(deltaLog.snapshot.schema))
    }
  }

  for {
    generatedAsIdentityType <- GeneratedAsIdentityType.values
  } {
    test(
        "replace table with identity column should upgrade protocol, "
          + s"identityType: $generatedAsIdentityType") {

      val tblName = getRandomTableName
      def getProtocolVersions: (Int, Int) = {
        sql(s"DESC DETAIL $tblName")
          .select("minReaderVersion", "minWriterVersion")
          .as[(Int, Int)]
          .head()
      }

      withTable(tblName) {
        createTable(
          tblName,
          Seq(
            TestColumnSpec(colName = "id", dataType = LongType),
            TestColumnSpec(colName = "value", dataType = IntegerType))
        )
        assert(getProtocolVersions == (1, 2) || getProtocolVersions == (2, 7))
        assert(DeltaLog.forTable(spark, TableIdentifier(tblName)).snapshot.version == 0)

        replaceTable(
          tblName,
          Seq(
            IdentityColumnSpec(
              generatedAsIdentityType,
              startsWith = Some(1),
              incrementBy = Some(1)
            ),
            TestColumnSpec(colName = "value", dataType = IntegerType)
          )
        )
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tblName))
        val protocol = deltaLog.update().protocol
        assert(getProtocolVersions == (1, 7) ||
          protocol.readerAndWriterFeatures.contains(IdentityColumnsTableFeature))
        assert(deltaLog.update().version == 1)
      }
    }
  }

  test("identity value start at boundaries") {
    val starts = Seq(Long.MinValue, Long.MaxValue)
    val steps = Seq(1, 2, -1, -2)
    for {
      generatedAsIdentityType <- GeneratedAsIdentityType.values
      start <- starts
      step <- steps
    } {
      val tblName = getRandomTableName
      withTable(tblName) {
        createTableWithIdColAndIntValueCol(
          tblName, generatedAsIdentityType, Some(start), Some(step))
        val table = DeltaLog.forTable(spark, TableIdentifier(tblName))
        val actualSchema =
          DeltaColumnMapping.dropColumnMappingMetadata(table.snapshot.metadata.schema)
        assert(actualSchema === expectedSchema(generatedAsIdentityType, start, step))
        if ((start < 0L) == (step < 0L)) {
          // test long underflow and overflow
          val ex = intercept[org.apache.spark.SparkException](
            sql(s"INSERT INTO $tblName(value) SELECT 1 UNION ALL SELECT 2")
          )
          assert(ex.getMessage.contains("long overflow"))
        } else {
          sql(s"INSERT INTO $tblName(value) SELECT 1 UNION ALL SELECT 2")
          checkAnswer(sql(s"SELECT COUNT(DISTINCT id) == COUNT(*) FROM $tblName"), Row(true))
          sql(s"INSERT INTO $tblName(value) SELECT 1 UNION ALL SELECT 2")
          checkAnswer(sql(s"SELECT COUNT(DISTINCT id) == COUNT(*) FROM $tblName"), Row(true))
          assert(highWaterMark(table.update(), "id") ===
            (start + (3 * step)))
        }
      }
    }
  }

  test("restore - positive step") {
    val tblName = getRandomTableName
    withTable(tblName) {
      generateTableWithIdentityColumn(tblName)
      sql(s"RESTORE TABLE $tblName TO VERSION AS OF 3")
      sql(s"INSERT INTO $tblName (value) VALUES (6)")
      checkAnswer(
        sql(s"SELECT id, value FROM $tblName ORDER BY value ASC"),
        Seq(Row(0, 0), Row(1, 1), Row(2, 2), Row(6, 6))
      )
    }
  }

  test("restore - negative step") {
    val tblName = getRandomTableName
    withTable(tblName) {
      generateTableWithIdentityColumn(tblName, step = -1)
      sql(s"RESTORE TABLE $tblName TO VERSION AS OF 3")
      sql(s"INSERT INTO $tblName (value) VALUES (6)")
      checkAnswer(
        sql(s"SELECT id, value FROM $tblName ORDER BY value ASC"),
        Seq(Row(0, 0), Row(-1, 1), Row(-2, 2), Row(-6, 6))
      )
    }
  }

  test("restore - on partitioned table") {
      for (generatedAsIdentityType <- GeneratedAsIdentityType.values) {
        val tblName = getRandomTableName
        withTable(tblName) {
          // v0.
          createTable(
            tblName,
            Seq(
              IdentityColumnSpec(generatedAsIdentityType),
              TestColumnSpec(colName = "value", dataType = IntegerType)
            ),
            partitionedBy = Seq("value")
          )
          // v1.
          sql(s"INSERT INTO $tblName (value) VALUES (1), (2)")
          val v1Content = sql(s"SELECT * FROM $tblName").collect()
          // v2.
          sql(s"INSERT INTO $tblName (value) VALUES (3), (4)")
          // v3: RESTORE to v1.
          sql(s"RESTORE TABLE $tblName TO VERSION AS OF 1")
          checkAnswer(
            sql(s"SELECT COUNT(DISTINCT id) FROM $tblName"),
            Row(2L)
          )
          checkAnswer(
            sql(s"SELECT * FROM $tblName"),
            v1Content
          )
          // v4.
          sql(s"INSERT INTO $tblName (value) VALUES (5), (6)")
          checkAnswer(
            sql(s"SELECT COUNT(DISTINCT id) FROM $tblName"),
            Row(4L)
          )
        }
      }
  }

  test("clone") {
      for {
        generatedAsIdentityType <- GeneratedAsIdentityType.values
      } {
        val oldTbl = s"${getRandomTableName}_old"
        val newTbl = s"${getRandomTableName}_new"
        withIdentityColumnTable(generatedAsIdentityType, oldTbl) {
          withTable(newTbl) {
            sql(s"INSERT INTO $oldTbl (value) VALUES (1), (2)")
            val oldSchema = DeltaLog.forTable(spark, TableIdentifier(oldTbl)).snapshot.schema
            sql(
              s"""
                 |CREATE TABLE $newTbl
                 |  SHALLOW CLONE $oldTbl
                 |""".stripMargin)
            val newSchema = DeltaLog.forTable(spark, TableIdentifier(newTbl)).snapshot.schema

            assert(newSchema("id").metadata.getLong(DeltaSourceUtils.IDENTITY_INFO_START) == 1L)
            assert(newSchema("id").metadata.getLong(DeltaSourceUtils.IDENTITY_INFO_STEP) == 1L)
            assert(oldSchema == newSchema)

            sql(s"INSERT INTO $newTbl (value) VALUES (1), (2)")
            checkAnswer(
              sql(s"SELECT COUNT(DISTINCT id) FROM $newTbl"),
              Row(4L)
            )
          }
        }
      }
  }
}

class IdentityColumnScalaSuite
  extends IdentityColumnSuiteBase
  with ScalaDDLTestUtils {

  test("unsupported column type") {
    for (unsupportedType <- unsupportedDataTypes) {
      val tblName = getRandomTableName
      withTable(tblName) {
        val ex = intercept[DeltaUnsupportedOperationException] {
          createTable(
            tblName,
            Seq(
              IdentityColumnSpec(GeneratedAlways, dataType = unsupportedType),
              TestColumnSpec(colName = "value", dataType = StringType)
            )
          )
        }
        assert(ex.getErrorClass === "DELTA_IDENTITY_COLUMNS_UNSUPPORTED_DATA_TYPE")
        assert(ex.getMessage.contains("is not supported for IDENTITY columns"))
      }
    }
  }

  test("unsupported step") {
    for {
      generatedAsIdentityType <- GeneratedAsIdentityType.values
      startsWith <- Seq(Some(1L), None)
    } {
      val tblName = getRandomTableName
      withTable(tblName) {
        val ex = intercept[DeltaAnalysisException] {
          createTableWithIdColAndIntValueCol(
            tblName, generatedAsIdentityType, startsWith, incrementBy = Some(0))
        }
        assert(ex.getErrorClass === "DELTA_IDENTITY_COLUMNS_ILLEGAL_STEP")
        assert(ex.getMessage.contains("step cannot be 0."))
      }
    }
  }

  test("cannot specify generatedAlwaysAs with identity columns") {
    def expectColumnBuilderError(f: => StructField): Unit = {
      val ex = intercept[DeltaAnalysisException] {
        f
      }
      assert(ex.getErrorClass === "DELTA_IDENTITY_COLUMNS_WITH_GENERATED_EXPRESSION")
      ex.getMessage.contains(
        "Identity column cannot be specified with a generated column expression.")
    }
    val generatedColumn = io.delta.tables.DeltaTable.columnBuilder(spark, "id")
      .dataType(LongType)
      .generatedAlwaysAs("id + 1")

    expectColumnBuilderError {
      generatedColumn.generatedAlwaysAsIdentity().build()
    }

    expectColumnBuilderError {
      generatedColumn.generatedByDefaultAsIdentity().build()
    }
  }
}

class IdentityColumnScalaIdColumnMappingSuite
  extends IdentityColumnSuiteBase
  with ScalaDDLTestUtils
  with DeltaColumnMappingEnableIdMode

class IdentityColumnScalaNameColumnMappingSuite
  extends IdentityColumnSuiteBase
  with ScalaDDLTestUtils
  with DeltaColumnMappingEnableNameMode
