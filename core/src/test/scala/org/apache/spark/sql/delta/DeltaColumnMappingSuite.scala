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

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, Metadata, MetadataBuilder, StringType, StructType}

class DeltaColumnMappingSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  import testImplicits._

  private val physicalNames = Map(
    1 -> DeltaColumnMapping.generatePhysicalName,
    2 -> DeltaColumnMapping.generatePhysicalName)

  private def withId(id: Int): Metadata =
    new MetadataBuilder()
      .putLong(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY, id)
      .putString(DeltaColumnMapping.COLUMN_MAPPING_METADATA_PHYSICAL_NAME_KEY, physicalNames(id))
      .build()

  private def withPhysicalMetadata(id: Int, name: String): Metadata =
    new MetadataBuilder()
      .putLong(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY, id)
      .putString(DeltaColumnMapping.COLUMN_MAPPING_METADATA_PHYSICAL_NAME_KEY, physicalNames(id))
      .putString(DeltaColumnMapping.COLUMN_MAPPING_METADATA_DISPLAY_NAME_KEY, name)
      .putLong(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY, id)
      .build()

  private def assertEqual(actual: StructType, expected: StructType): Unit = {
    assert(expected === actual,
      s"""
         |Schema mismatch:
         |
         |expected:
         |${expected.prettyJson}
         |
         |actual:
         |${actual.prettyJson}
         |""".stripMargin)
  }

  private val schema = new StructType()
    .add("a", StringType, true)
    .add("b", IntegerType, true)

  private val schemaWithId = new StructType()
    .add("a", StringType, true, withId(1))
    .add("b", IntegerType, true, withId(2))

  private val physicalSchema = new StructType()
    .add("a", StringType, true, withPhysicalMetadata(1, "a"))
    .add("b", IntegerType, true, withPhysicalMetadata(2, "b"))

  private def dfWithoutIds(spark: SparkSession) =
    spark.createDataFrame(Seq(Row("str1", 1), Row("str2", 2)).asJava, schema)

  private def dfWithIds(spark: SparkSession) =
    spark.createDataFrame(Seq(Row("str1", 1), Row("str2", 2)).asJava, schemaWithId)

  private def propString(props: Map[String, String]) = if (props.isEmpty) {
    ""
  } else {
    props
      .map { case (key, value) => s"'$key' = '$value'" }
      .mkString("TBLPROPERTIES (", ",", ")")
  }

  private def checkProperties(
      tableName: String,
      mode: Option[String] = None,
      readerVersion: Int = 1,
      writerVersion: Int = 2): Unit = {
    val props =
      spark.sql(s"SHOW TBLPROPERTIES $tableName").as[(String, String)].collect().toMap
    assert(props.get("delta.minReaderVersion").map(_.toInt) == Some(readerVersion))
    assert(props.get("delta.minWriterVersion").map(_.toInt) == Some(writerVersion))
    assert(props.get("delta.columnMappingMode") == mode)
  }

  private def createTableWithDeltaTableAPI(
      tableName: String,
      props: Map[String, String] = Map.empty,
      withColumnIds: Boolean = false): Unit = {
    val schemaToUse = if (withColumnIds) schemaWithId else schema
    val builder = io.delta.tables.DeltaTable.createOrReplace(spark)
      .tableName(tableName)
      .addColumn(schemaToUse.fields(0))
      .addColumn(schemaToUse.fields(1))
    props.foreach { case (key, value) =>
      builder.property(key, value)
    }
    builder.execute()
  }

  private def createTableWithSQLCreateOrReplaceAPI(
      tableName: String,
      props: Map[String, String] = Map.empty,
      withColumnIds: Boolean = false): Unit = {
    withTable("source") {
      val dfToWrite = if (withColumnIds) dfWithIds(spark) else dfWithoutIds(spark)
      dfToWrite.write.saveAsTable("source")
      spark.sql(
        s"""
           |CREATE OR REPLACE TABLE $tableName
           |USING DELTA
           |${propString(props)}
           |AS SELECT * FROM source
           |""".stripMargin)
    }
  }

  private def createTableWithSQLAPI(
      tableName: String,
      props: Map[String, String] = Map.empty,
      withColumnIds: Boolean = false,
      isPartitioned: Boolean = false): Unit = {
    withTable("source") {
      val dfToWrite = if (withColumnIds) dfWithIds(spark) else dfWithoutIds(spark)
      dfToWrite.write.saveAsTable("source")
      val partitionStmt = if (isPartitioned) "PARTITIONED BY (a)" else ""
      spark.sql(
        s"""
           |CREATE TABLE $tableName
           |USING DELTA
           |$partitionStmt
           |${propString(props)}
           |AS SELECT * FROM source
           |""".stripMargin)
    }
  }

  private def createTableWithDataFrameAPI(
      tableName: String,
      props: Map[String, String] = Map.empty,
      withColumnIds: Boolean = false): Unit = {
    val sqlConfs = props.map { case (key, value) =>
      "spark.databricks.delta.properties.defaults." + key.stripPrefix("delta.") -> value
    }
    withSQLConf(sqlConfs.toList: _*) {
      val dfToWrite = if (withColumnIds) dfWithIds(spark) else dfWithoutIds(spark)
      dfToWrite.write.format("delta").saveAsTable(tableName)
    }
  }

  private def createTableWithDataFrameWriterV2API(
      tableName: String,
      props: Map[String, String] = Map.empty,
      withColumnIds: Boolean = false): Unit = {
    val dfToWrite = if (withColumnIds) dfWithIds(spark) else dfWithoutIds(spark)
    val writer = dfToWrite.writeTo(tableName).using("delta")
    props.foreach(prop => writer.tableProperty(prop._1, prop._2))
    writer.create()
  }

  private def testCreateTableIdMode(tableName: String)(createFunc: => Unit): Unit = {
    withTable(tableName) {
      createFunc
      checkProperties(tableName,
        readerVersion = 2,
        writerVersion = 5,
        mode = Some("id"))

      assertEqual(spark.table(tableName).schema, schemaWithId)
      assertEqual(
        DeltaLog.forTable(spark, TableIdentifier(tableName)).update().metadata.physicalSchema,
        physicalSchema)
    }
  }

  private def alterTableWithProps(
      tableName: String,
      props: Map[String, String]): Unit =
    spark.sql(
      s"""
       | ALTER TABLE $tableName SET ${propString(props)}
       |""".stripMargin)

  test("create table with id mode auto bumps the version") {
    testCreateTableIdMode("t1") {
      createTableWithDeltaTableAPI(
        "t1", Map("delta.columnMappingMode" -> "id"), withColumnIds = true)
    }

    testCreateTableIdMode("t1") {
      createTableWithSQLAPI(
        "t1", Map("delta.columnMappingMode" -> "id"), withColumnIds = true)
    }

    testCreateTableIdMode("t1") {
      createTableWithDataFrameAPI(
        "t1", Map("delta.columnMappingMode" -> "id"), withColumnIds = true)
    }

    testCreateTableIdMode("t1") {
      createTableWithSQLCreateOrReplaceAPI(
        "t1", Map("delta.columnMappingMode" -> "id"), withColumnIds = true)
    }

    testCreateTableIdMode("t1") {
      createTableWithDataFrameWriterV2API(
        "t1", Map("delta.columnMappingMode" -> "id"), withColumnIds = true)
    }
  }

  test("create table with none mode") {
    withTable("t1") {
      createTableWithSQLAPI(
        "t1", Map("delta.columnMappingMode" -> "none"), withColumnIds = true)

      // Should be still on old protocol
      checkProperties("t1", mode = Some("none"))
      assert(spark.table("t1").schema == schemaWithId)
      assert(
        DeltaLog.forTable(spark, TableIdentifier("t1")).update().metadata.physicalSchema ==
          schemaWithId)
    }
  }

  test("blocked case: id mode but schema has no id") {
    withTable("t1") {
      val e = intercept[IllegalArgumentException] {
        createTableWithSQLAPI("t1", Map("delta.columnMappingMode" -> "id"))
      }
      assert(e.getMessage.contains("Missing column ID in column mapping mode"))
    }
  }

  test("blocked case: id mode but schema has no physical name") {
    withTable("t1") {
      val e = intercept[IllegalArgumentException] {
        val schemaWithNoPhysicalName = StructType(schemaWithId.map { field =>
          val metadata = new MetadataBuilder().withMetadata(field.metadata)
            .remove(DeltaColumnMapping.COLUMN_MAPPING_METADATA_PHYSICAL_NAME_KEY)
            .build()
          field.copy(metadata = metadata)})

        spark.createDataFrame(Seq(Row("str1", 1), Row("str2", 2)).asJava, schemaWithNoPhysicalName)
            .writeTo("t1")
            .using("delta")
            .tableProperty("delta.columnMappingMode", "id")
            .create()
      }
      assert(e.getMessage.contains("Missing physical name in column mapping mode"))
    }
  }

  test("blocked case: create table with name mode") {
    withTable("t1") {
      val e = intercept[UnsupportedOperationException] {
        createTableWithSQLAPI(
          "t1", Map("delta.columnMappingMode" -> "name"))
      }
      assert(e.getMessage.contains("The column mapping mode `name` is not supported"))
    }
  }

  test("blocked case: set mode on old protocol") {
    withTable("t1") {
      createTableWithSQLAPI("t1", withColumnIds = true)
      checkProperties("t1")
      val e = intercept[UnsupportedOperationException] {
        alterTableWithProps("t1", Map("delta.columnMappingMode" -> "id"))
      }
      assert(e.getMessage.contains("does not support the setting of column mapping mode"))
    }
  }

  test("blocked case: change mode on new protocol") {
    withTable("t1") {
      createTableWithSQLAPI("t1",
        Map("delta.columnMappingMode" -> "id"), withColumnIds = true)
      val e = intercept[UnsupportedOperationException] {
        alterTableWithProps("t1", Map("delta.columnMappingMode" -> "none"))
      }
      assert(e.getMessage.contains("Changing column mapping mode"))
    }
  }

  test("blocked case: change schema on new protocol") {
    withTable("t1") {
      createTableWithSQLAPI("t1",
        Map("delta.columnMappingMode" -> "id"), withColumnIds = true)
      val e = intercept[UnsupportedOperationException] {
        spark.sql(
          """
            |ALTER TABLE t1 ALTER COLUMN a AFTER b
            |""".stripMargin
        )
      }

      assert(e.getMessage.contains("Schema changes are not allowed in column mapping mode"))
    }
  }

  test("physical data and partition schema") {
    withTable("t1") {
      createTableWithSQLAPI("t1",
        Map("delta.columnMappingMode" -> "id"), withColumnIds = true)
      val metadata =
        DeltaLog.forTable(spark, TableIdentifier("t1")).update().metadata

      assertEqual(metadata.physicalSchema, physicalSchema)
      assertEqual(
        StructType(metadata.physicalPartitionSchema ++ metadata.physicalDataSchema),
          metadata.physicalSchema)
    }
  }
}
