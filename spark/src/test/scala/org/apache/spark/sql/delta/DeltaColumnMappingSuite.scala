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

// scalastyle:off import.ordering.noEmptyLine
import java.io.File
import java.nio.file.Files

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, AddFile, Metadata => MetadataAction, Protocol, SetTransaction}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.scalatest.GivenWhenThen

import org.apache.spark.sql.{DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
// scalastyle:on import.ordering.noEmptyLine

trait DeltaColumnMappingSuiteUtils
  extends SharedSparkSession
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest {


  protected def supportedModes: Seq[String] = Seq("id", "name")

  protected def colName(name: String) = s"$name with special chars ,;{}()\n\t="

  protected def partitionStmt(partCols: Seq[String]): String = {
    if (partCols.nonEmpty) s"PARTITIONED BY (${partCols.map(name => s"`$name`").mkString(",")})"
    else ""
  }

  protected def propString(props: Map[String, String]) = if (props.isEmpty) ""
    else {
      props
        .map { case (key, value) => s"'$key' = '$value'" }
        .mkString("TBLPROPERTIES (", ",", ")")
    }

  protected def alterTableWithProps(
    tableName: String,
    props: Map[String, String]): Unit =
    spark.sql(
      s"""
         | ALTER TABLE $tableName SET ${propString(props)}
         |""".stripMargin)

  protected def mode(props: Map[String, String]): String =
      props.get(DeltaConfigs.COLUMN_MAPPING_MODE.key).getOrElse("none")

  protected def testColumnMapping(
      testName: String,
      enableSQLConf: Boolean = false,
      modes: Option[Seq[String]] = None)(testCode: String => Unit): Unit = {
    test(testName) {
      modes.getOrElse(supportedModes).foreach { mode => {
        withClue(s"Testing under mode: $mode") {
          if (enableSQLConf) {
            withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> mode) {
              testCode(mode)
            }
          } else {
            testCode(mode)
          }
        }
      }}
    }
  }


}

class DeltaColumnMappingSuite extends QueryTest
  with GivenWhenThen
  with DeltaColumnMappingSuiteUtils {

  import testImplicits._

  protected def withId(id: Long): Metadata =
    new MetadataBuilder()
      .putLong(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY, id)
      .build()

  protected def withPhysicalName(pname: String) =
    new MetadataBuilder()
      .putString(DeltaColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, pname)
      .build()

  protected def withIdAndPhysicalName(id: Long, pname: String): Metadata =
    new MetadataBuilder()
      .putLong(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY, id)
      .putString(DeltaColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, pname)
      .build()

  protected def assertEqual(
      actual: StructType,
      expected: StructType,
      ignorePhysicalName: Boolean = true): Unit = {

    var actualSchema = actual
    var expectedSchema = expected

    val fieldsToRemove = mutable.Set[String]()
    if (ignorePhysicalName) {
      fieldsToRemove.add(DeltaColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)
    }

    def removeFields(metadata: Metadata): Metadata = {
      val metadataBuilder = new MetadataBuilder().withMetadata(metadata)
      fieldsToRemove.foreach { field => {
          if (metadata.contains(field)) {
            metadataBuilder.remove(field)
          }
        }
      }
      metadataBuilder.build()
    }

    // drop fields if needed
    actualSchema = SchemaMergingUtils.transformColumns(actual) { (_, field, _) =>
      field.copy(metadata = removeFields(field.metadata))
    }
    expectedSchema = SchemaMergingUtils.transformColumns(expected) { (_, field, _) =>
      field.copy(metadata = removeFields(field.metadata))
    }

    assert(expectedSchema === actualSchema,
      s"""
         |Schema mismatch:
         |
         |expected:
         |${expectedSchema.prettyJson}
         |
         |actual:
         |${actualSchema.prettyJson}
         |""".stripMargin)

  }

  protected def checkSchema(
      tableName: String,
      expectedSchema: StructType,
      ignorePhysicalName: Boolean = true): Unit = {

    // snapshot schema should have all the expected metadata
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
    assertEqual(deltaLog.update().schema, expectedSchema, ignorePhysicalName)

    // table schema should not have any metadata
    assert(spark.table(tableName).schema ===
      DeltaColumnMapping.dropColumnMappingMetadata(expectedSchema))
  }

  // NOTE:
  // All attached metadata to the following sample inputs, if used in source dataframe,
  // will be CLEARED out after metadata is imported into the target table
  // See ImplicitMetadataOperation.updateMetadata() for how the old metadata is cleared
  protected val schema = new StructType()
    .add("a", StringType, true)
    .add("b", IntegerType, true)

  protected val schemaNested = new StructType()
    .add("a", StringType, true)
    .add("b",
      new StructType()
        .add("c", StringType, true)
        .add("d", IntegerType, true),
      true
    )

  protected val schemaWithId = new StructType()
    .add("a", StringType, true, withId(1))
    .add("b", IntegerType, true, withId(2))

  protected val schemaWithIdRandom = new StructType()
    .add("a", StringType, true, withId(111))
    .add("b", IntegerType, true, withId(222))

  protected val schemaWithIdAndPhysicalNameRandom = new StructType()
    .add("a", StringType, true, withIdAndPhysicalName(111, "asjdklsajdkl"))
    .add("b", IntegerType, true, withIdAndPhysicalName(222, "iotiyoiopio"))

  protected val schemaWithDuplicatingIds = new StructType()
    .add("a", StringType, true, withId(1))
    .add("b", IntegerType, true, withId(2))
    .add("c", IntegerType, true, withId(2))

  protected val schemaWithIdAndDuplicatingPhysicalNames = new StructType()
    .add("a", StringType, true, withIdAndPhysicalName(1, "aaa"))
    .add("b", IntegerType, true, withIdAndPhysicalName(2, "bbb"))
    .add("c", IntegerType, true, withIdAndPhysicalName(3, "bbb"))

  protected val schemaWithDuplicatingPhysicalNames = new StructType()
    .add("a", StringType, true, withPhysicalName("aaa"))
    .add("b", IntegerType, true, withPhysicalName("bbb"))
    .add("c", IntegerType, true, withPhysicalName("bbb"))

  protected val schemaWithDuplicatingPhysicalNamesNested = new StructType()
    .add("b",
      new StructType()
        .add("c", StringType, true, withPhysicalName("dupName"))
        .add("d", IntegerType, true, withPhysicalName("dupName")),
      true,
      withPhysicalName("b")
    )

  protected val schemaWithIdNested = new StructType()
    .add("a", StringType, true, withId(1))
    .add("b",
      new StructType()
        .add("c", StringType, true, withId(3))
        .add("d", IntegerType, true, withId(4)),
      true,
      withId(2)
    )

  protected val schemaWithPhysicalNamesNested = new StructType()
    .add("a", StringType, true, withIdAndPhysicalName(1, "aaa"))
    .add("b",
      // let's call this nested struct 'X'.
      new StructType()
        .add("c", StringType, true, withIdAndPhysicalName(2, "ccc"))
        .add("d", IntegerType, true, withIdAndPhysicalName(3, "ddd"))
        .add("foo.bar",
          new StructType().add("f", LongType, true, withIdAndPhysicalName(4, "fff")),
          true,
          withIdAndPhysicalName(5, "foo.foo.foo.bar.bar.bar")),
      true,
      withIdAndPhysicalName(6, "bbb")
    )
    .add("g",
      // nested struct 'X' (see above) is repeated here.
      new StructType()
        .add("c", StringType, true, withIdAndPhysicalName(7, "ccc"))
        .add("d", IntegerType, true, withIdAndPhysicalName(8, "ddd"))
        .add("foo.bar",
          new StructType().add("f", LongType, true, withIdAndPhysicalName(9, "fff")),
          true,
          withIdAndPhysicalName(10, "foo.foo.foo.bar.bar.bar")),
      true,
      withIdAndPhysicalName(11, "ggg")
    )
    .add("h", IntegerType, true, withIdAndPhysicalName(12, "hhh"))

  protected val schemaWithIdNestedRandom = new StructType()
    .add("a", StringType, true, withId(111))
    .add("b",
      new StructType()
        .add("c", StringType, true, withId(333))
        .add("d", IntegerType, true, withId(444)),
      true,
      withId(222)
    )

  // This schema has both a.b and a . b as physical path for its columns, we would like to make sure
  // it shouldn't trigger the duplicated physical name check
  protected val schemaWithDottedColumnNames = new StructType()
    .add("a.b", StringType, true, withIdAndPhysicalName(1, "a.b"))
    .add("a", new StructType()
      .add("b", StringType, true, withIdAndPhysicalName(3, "b")),
      true, withIdAndPhysicalName(2, "a"))

  protected def dfWithoutIds(spark: SparkSession) =
    spark.createDataFrame(Seq(Row("str1", 1), Row("str2", 2)).asJava, schema)

  protected def dfWithoutIdsNested(spark: SparkSession) =
    spark.createDataFrame(
      Seq(Row("str1", Row("str1.1", 1)), Row("str2", Row("str1.2", 2))).asJava, schemaNested)

  protected def dfWithIds(spark: SparkSession, randomIds: Boolean = false) =
    spark.createDataFrame(Seq(Row("str1", 1), Row("str2", 2)).asJava,
      if (randomIds) schemaWithIdRandom else schemaWithId)

  protected def dfWithIdsNested(spark: SparkSession, randomIds: Boolean = false) =
    spark.createDataFrame(
      Seq(Row("str1", Row("str1.1", 1)), Row("str2", Row("str1.2", 2))).asJava,
      if (randomIds) schemaWithIdNestedRandom else schemaWithIdNested)

  protected def checkProperties(
      tableName: String,
      mode: Option[String] = None,
      readerVersion: Int = 1,
      writerVersion: Int = 2,
      curMaxId: Long = 0): Unit = {
    val props =
      spark.sql(s"SHOW TBLPROPERTIES $tableName").as[(String, String)].collect().toMap
    assert(props.get("delta.minReaderVersion").map(_.toInt) == Some(readerVersion))
    assert(props.get("delta.minWriterVersion").map(_.toInt) == Some(writerVersion))

    assert(props.get(DeltaConfigs.COLUMN_MAPPING_MODE.key) == mode)
    assert(props.get(DeltaConfigs.COLUMN_MAPPING_MAX_ID.key).map(_.toLong).getOrElse(0) == curMaxId)
  }

  protected def createTableWithDeltaTableAPI(
      tableName: String,
      props: Map[String, String] = Map.empty,
      withColumnIds: Boolean = false,
      isPartitioned: Boolean = false): Unit = {
    val schemaToUse = if (withColumnIds) schemaWithId else schema
    val builder = io.delta.tables.DeltaTable.createOrReplace(spark)
      .tableName(tableName)
      .addColumn(schemaToUse.fields(0))
      .addColumn(schemaToUse.fields(1))
    props.foreach { case (key, value) =>
      builder.property(key, value)
    }
    if (isPartitioned) {
      builder.partitionedBy("a")
    }
    builder.execute()
  }

  protected def createTableWithSQLCreateOrReplaceAPI(
      tableName: String,
      props: Map[String, String] = Map.empty,
      withColumnIds: Boolean = false,
      isPartitioned: Boolean = false,
      nested: Boolean = false,
      randomIds: Boolean = false): Unit = {
    withTable("source") {
      val dfToWrite = if (withColumnIds) {
        if (nested) {
          dfWithIdsNested(spark, randomIds)
        } else {
          dfWithIds(spark, randomIds)
        }
      } else {
        if (nested) {
          dfWithoutIdsNested(spark)
        } else {
          dfWithoutIds(spark)
        }
      }
      dfToWrite.write.saveAsTable("source")
      val partitionStmt = if (isPartitioned) "PARTITIONED BY (a)" else ""
      spark.sql(
        s"""
           |CREATE OR REPLACE TABLE $tableName
           |USING DELTA
           |$partitionStmt
           |${propString(props)}
           |AS SELECT * FROM source
           |""".stripMargin)
    }
  }

  protected def createTableWithSQLAPI(
      tableName: String,
      props: Map[String, String] = Map.empty,
      withColumnIds: Boolean = false,
      isPartitioned: Boolean = false,
      nested: Boolean = false,
      randomIds: Boolean = false): Unit = {
    withTable("source") {
      val dfToWrite = if (withColumnIds) {
        if (nested) {
          dfWithIdsNested(spark, randomIds)
        } else {
          dfWithIds(spark, randomIds)
        }
      } else {
        if (nested) {
          dfWithoutIdsNested(spark)
        } else {
          dfWithoutIds(spark)
        }
      }
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

  protected def createTableWithDataFrameAPI(
      tableName: String,
      props: Map[String, String] = Map.empty,
      withColumnIds: Boolean = false,
      isPartitioned: Boolean = false,
      nested: Boolean = false,
      randomIds: Boolean = false): Unit = {
    val sqlConfs = props.map { case (key, value) =>
      "spark.databricks.delta.properties.defaults." + key.stripPrefix("delta.") -> value
    }
    withSQLConf(sqlConfs.toList: _*) {
      val dfToWrite = if (withColumnIds) {
        if (nested) {
          dfWithIdsNested(spark, randomIds)
        } else {
          dfWithIds(spark, randomIds)
        }
      } else {
        if (nested) {
          dfWithoutIdsNested(spark)
        } else {
          dfWithoutIds(spark)
        }
      }
      if (isPartitioned) {
        dfToWrite.write.format("delta").partitionBy("a").saveAsTable(tableName)
      } else {
        dfToWrite.write.format("delta").saveAsTable(tableName)
      }
    }
  }

  protected def createTableWithDataFrameWriterV2API(
      tableName: String,
      props: Map[String, String] = Map.empty,
      withColumnIds: Boolean = false,
      isPartitioned: Boolean = false,
      nested: Boolean = false,
      randomIds: Boolean = false): Unit = {
    val dfToWrite = if (withColumnIds) {
      if (nested) {
        dfWithIdsNested(spark, randomIds)
      } else {
        dfWithIds(spark, randomIds)
      }
    } else {
      if (nested) {
        dfWithoutIdsNested(spark)
      } else {
        dfWithoutIds(spark)
      }
    }
    val writer = dfToWrite.writeTo(tableName).using("delta")
    props.foreach(prop => writer.tableProperty(prop._1, prop._2))
    if (isPartitioned) writer.partitionedBy('a)
    writer.create()
  }

  protected def createStrictSchemaTableWithDeltaTableApi(
      tableName: String,
      schema: StructType,
      props: Map[String, String] = Map.empty,
      isPartitioned: Boolean = false): Unit = {
    val builder = io.delta.tables.DeltaTable.createOrReplace(spark)
      .tableName(tableName)
    builder.addColumns(schema)
    props.foreach(prop => builder.property(prop._1, prop._2))
    if (isPartitioned) builder.partitionedBy("a")
    builder.execute()
  }

  protected def testCreateTableColumnMappingMode(
      tableName: String,
      expectedSchema: StructType,
      ignorePhysicalName: Boolean,
      mode: String,
      createNewTable: Boolean = true,
      tableFeaturesProtocolExpected: Boolean = true)(fn: => Unit): Unit = {
    withTable(tableName) {
        fn
      checkProperties(tableName,
        readerVersion = 2,
        writerVersion = if (tableFeaturesProtocolExpected) 7 else 5,
        mode = Some(mode),
        curMaxId = DeltaColumnMapping.findMaxColumnId(expectedSchema)
      )
      checkSchema(tableName, expectedSchema, ignorePhysicalName)
    }
  }

  test("find max column id in existing columns") {
    assert(DeltaColumnMapping.findMaxColumnId(schemaWithId) == 2)
    assert(DeltaColumnMapping.findMaxColumnId(schemaWithIdNested) == 4)
    assert(DeltaColumnMapping.findMaxColumnId(schemaWithIdRandom) == 222)
    assert(DeltaColumnMapping.findMaxColumnId(schemaWithIdNestedRandom) == 444)
    assert(DeltaColumnMapping.findMaxColumnId(schema) == 0)
    assert(DeltaColumnMapping.findMaxColumnId(new StructType()) == 0)
  }

  test("Enable column mapping with schema change on table with no schema") {
    withTempDir { dir =>
      val tablePath = dir.getCanonicalPath
      Seq((1, "a"), (2, "b")).toDF("id", "name")
        .write.mode("append").format("delta").save(tablePath)
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      val txn = deltaLog.startTransaction()
      txn.commitManually(actions.Metadata()) // Whip the schema out
      val txn2 = deltaLog.startTransaction()
      txn2.commitManually(Protocol(2, 5))
      txn2.updateMetadata(actions.Metadata(
        configuration = Map("delta.columnMapping.mode" -> "name"),
        schemaString = new StructType().add("a", StringType).json))

      // Now ensure that it is not allowed to enable column mapping with schema change
      // on a table with a schema
      Seq((1, "a"), (2, "b")).toDF("id", "name")
        .write.mode("overwrite").format("delta")
        .option("overwriteSchema", "true")
        .save(tablePath)
      val txn3 = deltaLog.startTransaction()
      txn3.commitManually(Protocol(2, 5))
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        txn3.updateMetadata(
          actions.Metadata(
          configuration = Map("delta.columnMapping.mode" -> "name"),
          schemaString = new StructType().add("a", StringType).json))
      }
      val msg = "Schema changes are not allowed during the change of column mapping mode."
      assert(e.getMessage.contains(msg))
    }
  }

  // TODO: repurpose this once we roll out the proper semantics for CM + streaming
  testColumnMapping("isColumnMappingReadCompatible") { mode =>
    // Set up table based on mode and return the initial metadata actions for comparison
    def setupInitialTable(deltaLog: DeltaLog): (MetadataAction, MetadataAction) = {
      val tablePath = deltaLog.dataPath.toString
      if (mode == NameMapping.name) {
        Seq((1, "a"), (2, "b")).toDF("id", "name")
          .write.mode("append").format("delta").save(tablePath)
        // schema: <id, name>
        val m0 = deltaLog.update().metadata

        // add a column
        sql(s"ALTER TABLE delta.`$tablePath` ADD COLUMN (score long)")
        // schema: <id, name, score>
        val m1 = deltaLog.update().metadata

        // column mapping not enabled -> not blocked at all
        assert(DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m1, m0))

        // upgrade to name mode
        alterTableWithProps(s"delta.`$tablePath`", Map(
          DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name",
          DeltaConfigs.MIN_READER_VERSION.key -> "2",
          DeltaConfigs.MIN_WRITER_VERSION.key -> "5"))

        (m0, m1)
      } else {
        // for id mode, just create the table
        withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "id") {
          Seq((1, "a"), (2, "b")).toDF("id", "name")
            .write.mode("append").format("delta").save(tablePath)
        }
        // schema: <id, name>
        val m0 = deltaLog.update().metadata

        // add a column
        sql(s"ALTER TABLE delta.`$tablePath` ADD COLUMN (score long)")
        // schema: <id, name, score>
        val m1 = deltaLog.update().metadata

        // add column shouldn't block
        assert(DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m1, m0))

        (m0, m1)
      }
    }

    withTempDir { dir =>
      val tablePath = dir.getCanonicalPath
      val deltaLog = DeltaLog.forTable(spark, tablePath)

      val (m0, m1) = setupInitialTable(deltaLog)

      // schema: <id, name, score>
      val m2 = deltaLog.update().metadata

      assert(DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m2, m1))
      assert(DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m2, m0))

      // rename column
      sql(s"ALTER TABLE delta.`$tablePath` RENAME COLUMN score TO age")
      // schema: <id, name, age>
      val m3 = deltaLog.update().metadata

      assert(!DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m3, m2))
      assert(!DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m3, m1))
      // But IS read compatible with the initial schema, because the added column should not
      // be blocked by this column mapping check.
      assert(DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m3, m0))

      // drop a column
      sql(s"ALTER TABLE delta.`$tablePath` DROP COLUMN age")
      // schema: <id, name>
      val m4 = deltaLog.update().metadata

      assert(!DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m4, m3))
      assert(!DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m4, m2))
      assert(!DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m4, m1))
      // but IS read compatible with the initial schema, because the added column is dropped
      assert(DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m4, m0))

      // add back the same column
      sql(s"ALTER TABLE delta.`$tablePath` ADD COLUMN (score long)")
      // schema: <id, name, score>
      val m5 = deltaLog.update().metadata

      // It IS read compatible with the previous schema, because the added column should not
      // blocked by this column mapping check.
      assert(DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m5, m4))
      assert(!DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m5, m3))
      assert(!DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m5, m2))
      // But Since the new added column has a different physical name as all previous columns,
      // even it has the same logical name as say, m1.schema, we will still block
      assert(!DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m5, m1))
      // But it IS read compatible with the initial schema, because the added column should not
      // be blocked by this column mapping check.
      assert(DeltaColumnMapping.hasNoColumnMappingSchemaChanges(m5, m0))
    }
  }

  testColumnMapping("create table through raw schema API should " +
    "auto bump the version and retain input metadata") { mode =>

    // provides id only (let Delta generate physical name for me)
    testCreateTableColumnMappingMode(
      "t1", schemaWithIdRandom, ignorePhysicalName = true, mode = mode) {
      createStrictSchemaTableWithDeltaTableApi(
        "t1",
        schemaWithIdRandom,
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode))
    }

    // provides id and physical name (Delta shouldn't rebuild/override)
    // we use random ids as input, which shouldn't be changed too
    testCreateTableColumnMappingMode(
      "t1", schemaWithIdAndPhysicalNameRandom, ignorePhysicalName = false, mode = mode) {
      createStrictSchemaTableWithDeltaTableApi(
        "t1",
        schemaWithIdAndPhysicalNameRandom,
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode))
    }

  }

  testColumnMapping("create table through dataframe should " +
    "auto bumps the version and rebuild schema metadata/drop dataframe metadata") { mode =>
    // existing ids should be dropped/ignored and ids should be regenerated
    // so for tests below even if we are ingesting dfs with random ids
    // we should still expect schema with normal sequential ids
    val expectedSchema = schemaWithId

    testCreateTableColumnMappingMode(
      "t1", expectedSchema, ignorePhysicalName = true, mode = mode) {
      createTableWithSQLAPI(
        "t1",
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode),
        withColumnIds = true,
        randomIds = true)
    }

    testCreateTableColumnMappingMode(
      "t1", expectedSchema, ignorePhysicalName = true, mode = mode) {
      createTableWithDataFrameAPI(
        "t1",
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode),
        withColumnIds = true,
        randomIds = true)
    }

    testCreateTableColumnMappingMode(
      "t1", expectedSchema, ignorePhysicalName = true, mode = mode) {
      createTableWithSQLCreateOrReplaceAPI(
        "t1",
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode),
        withColumnIds = true,
        randomIds = true)
    }

    testCreateTableColumnMappingMode(
      "t1", expectedSchema, ignorePhysicalName = true, mode = mode) {
      createTableWithDataFrameWriterV2API(
        "t1",
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode),
        withColumnIds = true,
        randomIds = true)
    }
  }

  test("create table with none mode") {
    withTable("t1") {
      // column ids will be dropped, having the options here to make sure such happens
      createTableWithSQLAPI(
        "t1",
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "none"),
        withColumnIds = true,
        randomIds = true)

      // Should be still on old protocol, the schema shouldn't have any metadata
      checkProperties(
        "t1",
        mode = Some("none"))

      checkSchema("t1", schema, ignorePhysicalName = false)
    }
  }

  testColumnMapping("update column mapped table invalid max id property is blocked") { mode =>
    withTable("t1") {
      createTableWithSQLAPI(
        "t1",
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode),
        withColumnIds = true
      )

      val log = DeltaLog.forTable(spark, TableIdentifier("t1"))
      // Get rid of max column id prop
      assert {
        intercept[DeltaAnalysisException] {
          log.withNewTransaction { txn =>
            val existingMetadata = log.update().metadata
            txn.commit(existingMetadata.copy(configuration =
              existingMetadata.configuration - DeltaConfigs.COLUMN_MAPPING_MAX_ID.key) :: Nil,
              DeltaOperations.ManualUpdate)
          }
        }.getErrorClass == "DELTA_COLUMN_MAPPING_MAX_COLUMN_ID_NOT_SET"
      }
      // Use an invalid max column id prop
      assert {
        intercept[DeltaAnalysisException] {
          log.withNewTransaction { txn =>
            val existingMetadata = log.update().metadata
            txn.commit(existingMetadata.copy(configuration =
              existingMetadata.configuration ++ Map(
                // '1' is less than the current max
                DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> "1"
              )) :: Nil,
              DeltaOperations.ManualUpdate)
          }
        }.getErrorClass == "DELTA_COLUMN_MAPPING_MAX_COLUMN_ID_NOT_SET_CORRECTLY"
      }
    }
  }

  testColumnMapping(
    "create column mapped table with duplicated id/physical name should error"
  ) { mode =>
    withTable("t1") {
      val e = intercept[ColumnMappingException] {
        createStrictSchemaTableWithDeltaTableApi(
          "t1",
          schemaWithDuplicatingIds,
          Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode))
      }
      assert(
        e.getMessage.contains(
          s"Found duplicated column id `2` in column mapping mode `$mode`"))
      assert(e.getMessage.contains(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY))

      val e2 = intercept[ColumnMappingException] {
        createStrictSchemaTableWithDeltaTableApi(
          "t1",
          schemaWithIdAndDuplicatingPhysicalNames,
          Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode))
      }
      assert(
        e2.getMessage.contains(
          s"Found duplicated physical name `bbb` in column mapping mode `$mode`"))
      assert(e2.getMessage.contains(DeltaColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY))
    }

    // for name mode specific, we would also like to check for name duplication
    if (mode == "name") {
      val e = intercept[ColumnMappingException] {
        createStrictSchemaTableWithDeltaTableApi(
          "t1",
          schemaWithDuplicatingPhysicalNames,
          Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode))
      }
      assert(
        e.getMessage.contains(
          s"Found duplicated physical name `bbb` in column mapping mode `$mode`")
      )

      val e2 = intercept[ColumnMappingException] {
        createStrictSchemaTableWithDeltaTableApi(
          "t1",
          schemaWithDuplicatingPhysicalNamesNested,
          Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode))
      }
      assert(
        e2.getMessage.contains(
          s"Found duplicated physical name `b.dupName` in column mapping mode `$mode`")
      )
    }
  }

  testColumnMapping(
    "create table in column mapping mode without defining ids explicitly"
  ) { mode =>
    withTable("t1") {
      // column ids will be dropped, having the options here to make sure such happens
      createTableWithSQLAPI(
        "t1",
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode),
        withColumnIds = true,
        randomIds = true)
      checkSchema("t1", schemaWithId)
      checkProperties("t1",
        readerVersion = 2,
        writerVersion = 7,
        mode = Some(mode),
        curMaxId = DeltaColumnMapping.findMaxColumnId(schemaWithId)
      )
    }
  }

  testColumnMapping("alter column order in schema on new protocol") { mode =>
    withTable("t1") {
      // column ids will be dropped, having the options here to make sure such happens
      createTableWithSQLAPI("t1",
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode),
        withColumnIds = true,
        nested = true,
        randomIds = true)
      spark.sql(
        """
          |ALTER TABLE t1 ALTER COLUMN a AFTER b
          |""".stripMargin
      )

      checkProperties("t1",
        readerVersion = 2,
        writerVersion = 7,
        mode = Some(mode),
        curMaxId = DeltaColumnMapping.findMaxColumnId(schemaWithIdNested))
      checkSchema(
        "t1",
        schemaWithIdNested.copy(fields = schemaWithIdNested.fields.reverse))
    }
  }

  testColumnMapping("add column in schema on new protocol") { mode =>

    def check(expectedSchema: StructType): Unit = {
      val curMaxId = DeltaColumnMapping.findMaxColumnId(expectedSchema) + 1
      checkSchema("t1", expectedSchema)
      spark.sql(
        """
          |ALTER TABLE t1 ADD COLUMNS (c STRING AFTER b)
          |""".stripMargin
      )

      checkProperties("t1",
        readerVersion = 2,
        writerVersion = 7,
        mode = Some(mode),
        curMaxId = curMaxId)

      checkSchema("t1", expectedSchema.add("c", StringType, true, withId(curMaxId)))

      val curMaxId2 = DeltaColumnMapping.findMaxColumnId(expectedSchema) + 2

      spark.sql(
        """
          |ALTER TABLE t1 ADD COLUMNS (d STRING AFTER c)
          |""".stripMargin
      )
      checkProperties("t1",
        readerVersion = 2,
        writerVersion = 7,
        mode = Some(mode),
        curMaxId = curMaxId2)
      checkSchema("t1",
        expectedSchema
          .add("c", StringType, true, withId(curMaxId))
          .add("d", StringType, true, withId(curMaxId2)))
    }

    withTable("t1") {
      // column ids will be dropped, having the options here to make sure such happens
      createTableWithSQLAPI(
        "t1",
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode), withColumnIds = true, randomIds = true)

      check(schemaWithId)
    }

    withTable("t1") {
      // column ids will NOT be dropped, so future ids should update based on the current max
      createStrictSchemaTableWithDeltaTableApi(
        "t1",
        schemaWithIdRandom,
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode)
      )

      check(schemaWithIdRandom)
    }
  }

  testColumnMapping("add nested column in schema on new protocol") { mode =>
    withTable("t1") {
      // column ids will be dropped, having the options here to make sure such happens
      createTableWithSQLAPI(
        "t1",
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode),
        withColumnIds = true,
        nested = true,
        randomIds = true)

      checkSchema("t1", schemaWithIdNested)

      val curMaxId = DeltaColumnMapping.findMaxColumnId(schemaWithIdNested) + 1

      spark.sql(
        """
          |ALTER TABLE t1 ADD COLUMNS (b.e STRING AFTER d)
          |""".stripMargin
      )

      checkProperties("t1",
        readerVersion = 2,
        writerVersion = 7,
        mode = Some(mode),
        curMaxId = curMaxId)
      checkSchema("t1",
          schemaWithIdNested.merge(
            new StructType().add(
              "b",
              new StructType().add(
                "e", StringType, true, withId(5)),
              true,
              withId(2)
            ))
      )

      val curMaxId2 = DeltaColumnMapping.findMaxColumnId(schemaWithIdNested) + 2
      spark.sql(
        """
          |ALTER TABLE t1 ADD COLUMNS (b.f STRING AFTER e)
          |""".stripMargin
      )
      checkProperties("t1",
        readerVersion = 2,
        writerVersion = 7,
        mode = Some(mode),
        curMaxId = curMaxId2)
      checkSchema("t1",
          schemaWithIdNested.merge(
            new StructType().add(
              "b",
              new StructType().add(
                "e", StringType, true, withId(5)),
              true,
              withId(2)
            )).merge(
          new StructType().add(
              "b",
              new StructType()
                .add("f", StringType, true, withId(6)),
              true,
              withId(2))
        ))

    }
  }

  testColumnMapping("write/merge df to table") { mode =>
    withTable("t1") {
      // column ids will be dropped, having the options here to make sure such happens
      createTableWithDataFrameAPI("t1",
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode), withColumnIds = true, randomIds = true)
      val curMaxId = DeltaColumnMapping.findMaxColumnId(schemaWithId)

      val df1 = dfWithIds(spark)
      df1.write
         .format("delta")
         .mode("append")
         .saveAsTable("t1")

      checkProperties("t1",
        readerVersion = 2,
        writerVersion = 7,
        mode = Some(mode),
        curMaxId = curMaxId)
      checkSchema("t1", schemaWithId)

      val previousSchema = spark.table("t1").schema
      // ingest df with random id should not cause existing schema col id to change
      val df2 = dfWithIds(spark, randomIds = true)
      df2.write
         .format("delta")
         .mode("append")
         .saveAsTable("t1")

      checkProperties("t1",
        readerVersion = 2,
        writerVersion = 7,
        mode = Some(mode),
        curMaxId = curMaxId)

      // with checkPhysicalSchema check
      checkSchema("t1", schemaWithId)

      // compare with before
      assertEqual(spark.table("t1").schema,
        previousSchema, ignorePhysicalName = false)

      val df3 = spark.createDataFrame(
        Seq(Row("str3", 3, "str3.1"), Row("str4", 4, "str4.1")).asJava,
        schemaWithId.add("c", StringType, true, withId(3))
      )
      df3.write
         .option("mergeSchema", "true")
         .format("delta")
         .mode("append")
         .saveAsTable("t1")

      val curMaxId2 = DeltaColumnMapping.findMaxColumnId(schemaWithId) + 1
      checkProperties("t1",
        readerVersion = 2,
        writerVersion = 7,
        mode = Some(mode),
        curMaxId = curMaxId2)
      checkSchema("t1", schemaWithId.add("c", StringType, true, withId(3)))
    }
  }

  testColumnMapping(s"try modifying restricted max id property should fail") { mode =>
    withTable("t1") {
      val e = intercept[UnsupportedOperationException] {
        createTableWithSQLAPI(
          "t1",
          Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode,
              DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> "100"),
          withColumnIds = true,
          nested = true)
      }
      assert(e.getMessage.contains(s"The Delta table configuration " +
        s"${DeltaConfigs.COLUMN_MAPPING_MAX_ID.key} cannot be specified by the user"))
    }

    withTable("t1") {
      createTableWithSQLAPI(
          "t1",
          Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode),
          withColumnIds = true,
          nested = true)

      val e2 = intercept[UnsupportedOperationException] {
        alterTableWithProps("t1", Map(DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> "100"))
      }

      assert(e2.getMessage.contains(s"The Delta table configuration " +
        s"${DeltaConfigs.COLUMN_MAPPING_MAX_ID.key} cannot be specified by the user"))
    }

    withTable("t1") {
      val e = intercept[UnsupportedOperationException] {
        createTableWithDataFrameAPI(
          "t1",
          Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode,
              DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> "100"),
          withColumnIds = true,
          nested = true)
      }
      assert(e.getMessage.contains(s"The Delta table configuration " +
        s"${DeltaConfigs.COLUMN_MAPPING_MAX_ID.key} cannot be specified by the user"))
    }
  }

  testColumnMapping("physical data and partition schema") { mode =>
    withTable("t1") {
      // column ids will be dropped, having the options here to make sure such happens
      createTableWithSQLAPI("t1",
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode),
        withColumnIds = true,
        randomIds = true)

      val metadata = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("t1"))._2.metadata

      assertEqual(metadata.schema, schemaWithId)
      assertEqual(metadata.schema, StructType(metadata.partitionSchema ++ metadata.dataSchema))
    }
  }

  testColumnMapping("block CONVERT TO DELTA") { mode =>
    withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> mode) {
      withTempDir { tablePath =>
        val tempDir = tablePath.getCanonicalPath
        val df1 = Seq(0).toDF("id")
          .withColumn("key1", lit("A1"))
          .withColumn("key2", lit("A2"))

        df1.write
          .partitionBy(Seq("key1"): _*)
          .format("parquet")
          .mode("overwrite")
          .save(tempDir)

        val e = intercept[UnsupportedOperationException] {
          sql(s"convert to delta parquet.`$tempDir` partitioned by (key1 String)")
        }
        assert(e.getMessage.contains(s"cannot be set to `$mode` when using CONVERT TO DELTA"))
      }
    }
  }

  testColumnMapping(
    "column mapping batch scan should detect physical name changes",
    enableSQLConf = true
  ) { _ =>
    withTempDir { dir =>
      spark.range(10).toDF("id")
        .write.format("delta").save(dir.getCanonicalPath)
      // Analysis phase
      val df = spark.read.format("delta").load(dir.getCanonicalPath)
      // Overwrite schema but with same logical schema
      withSQLConf(DeltaSQLConf.REUSE_COLUMN_MAPPING_METADATA_DURING_OVERWRITE.key -> "false") {
        spark.range(10).toDF("id")
          .write.format("delta").option("overwriteSchema", "true").mode("overwrite")
          .save(dir.getCanonicalPath)
      }
      // The previous analyzed DF no longer is able to read the data any more because it generates
      // new physical name for the underlying columns, so we should fail.
      assert {
        intercept[DeltaAnalysisException] {
          df.collect()
        }.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"
      }
      // See we can't read back the same data any more
      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_ON_READ_CHECK_ENABLED.key -> "false") {
        checkAnswer(
          df,
          (0 until 10).map(_ => Row(null))
        )
      }
    }
  }

  protected def testPartitionPath(tableName: String)(createFunc: Boolean => Unit): Unit = {
    withTable(tableName) {
      Seq(true, false).foreach { isPartitioned =>
        spark.sql(s"drop table if exists $tableName")
        createFunc(isPartitioned)
        val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(tableName))
        val prefixLen = DeltaConfigs.RANDOM_PREFIX_LENGTH.fromMetaData(snapshot.metadata)
        Seq(("str3", 3), ("str4", 4)).toDF(schema.fieldNames: _*)
          .write.format("delta").mode("append").saveAsTable(tableName)
        checkAnswer(spark.table(tableName),
          Row("str1", 1) :: Row("str2", 2) :: Row("str3", 3) :: Row("str4", 4) :: Nil)
        // both new table writes and appends should use prefix
        val pattern = s"[A-Za-z0-9]{$prefixLen}/.*part-.*parquet"
        for (file <- snapshot.allFiles.collect()) {
          assert(file.path.matches(pattern))
        }
      }
    }
  }

  // Copied verbatim from the "valid replaceWhere" test in DeltaSuite
  protected def testReplaceWhere(): Unit =
    Seq(true, false).foreach { enabled =>
      withSQLConf(DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED.key -> enabled.toString) {
        Seq(true, false).foreach { partitioned =>
          // Skip when it's not enabled and not partitioned.
          if (enabled || partitioned) {
            withTempDir { dir =>
              val writer = Seq(1, 2, 3, 4).toDF()
                .withColumn("is_odd", $"value" % 2 =!= 0)
                .withColumn("is_even", $"value" % 2 === 0)
                .write
                .format("delta")

              if (partitioned) {
                writer.partitionBy("is_odd").save(dir.toString)
              } else {
                writer.save(dir.toString)
              }

              def data: DataFrame = spark.read.format("delta").load(dir.toString)

              Seq(5, 7).toDF()
                .withColumn("is_odd", $"value" % 2 =!= 0)
                .withColumn("is_even", $"value" % 2 === 0)
                .write
                .format("delta")
                .mode("overwrite")
                .option(DeltaOptions.REPLACE_WHERE_OPTION, "is_odd = true")
                .save(dir.toString)
              checkAnswer(
                data,
                Seq(2, 4, 5, 7).toDF()
                  .withColumn("is_odd", $"value" % 2 =!= 0)
                  .withColumn("is_even", $"value" % 2 === 0))

              // replaceWhere on non-partitioning columns if enabled.
              if (enabled) {
                Seq(6, 8).toDF()
                  .withColumn("is_odd", $"value" % 2 =!= 0)
                  .withColumn("is_even", $"value" % 2 === 0)
                  .write
                  .format("delta")
                  .mode("overwrite")
                  .option(DeltaOptions.REPLACE_WHERE_OPTION, "is_even = true")
                  .save(dir.toString)
                checkAnswer(
                  data,
                  Seq(5, 6, 7, 8).toDF()
                    .withColumn("is_odd", $"value" % 2 =!= 0)
                    .withColumn("is_even", $"value" % 2 === 0))
              }
            }
          }
        }
      }
    }

  testColumnMapping("valid replaceWhere", enableSQLConf = true) { _ =>
    testReplaceWhere()
  }

  protected def verifyUpgradeAndTestSchemaEvolution(tableName: String): Unit = {
    checkProperties(tableName,
      readerVersion = 2,
      writerVersion = 5,
      mode = Some("name"),
      curMaxId = 4)
    checkSchema(tableName, schemaWithIdNested)
    val expectedSchema = new StructType()
      .add("a", StringType, true, withIdAndPhysicalName(1, "a"))
      .add("b",
        new StructType()
          .add("c", StringType, true, withIdAndPhysicalName(3, "c"))
          .add("d", IntegerType, true, withIdAndPhysicalName(4, "d")),
        true,
        withIdAndPhysicalName(2, "b"))

    assertEqual(
      DeltaLog.forTableWithSnapshot(spark, TableIdentifier(tableName))._2.schema,
      expectedSchema,
      ignorePhysicalName = false)

    checkAnswer(spark.table(tableName), dfWithoutIdsNested(spark))

    // test schema evolution
    val newNestedData =
      spark.createDataFrame(
        Seq(Row("str3", Row("str1.3", 3), "new value")).asJava,
        schemaNested.add("e", StringType))
    newNestedData.write.format("delta")
      .option("mergeSchema", "true")
      .mode("append").saveAsTable(tableName)
    checkAnswer(
      spark.table(tableName),
      dfWithoutIdsNested(spark).withColumn("e", lit(null)).union(newNestedData))

    val newTableSchema = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(tableName))._2.schema
    val newPhysicalName = DeltaColumnMapping.getPhysicalName(newTableSchema("e"))

    // physical name of new column should be GUID, not display name
    assert(newPhysicalName.startsWith("col-"))
    assertEqual(
      newTableSchema,
      expectedSchema.add("e", StringType, true, withIdAndPhysicalName(5, newPhysicalName)),
      ignorePhysicalName = false)
  }

  test("change mode on new protocol table") {
    withTable("t1") {
      createTableWithSQLAPI(
        "t1",
        isPartitioned = true,
        nested = true,
        props = Map(
          DeltaConfigs.MIN_READER_VERSION.key -> "2",
          DeltaConfigs.MIN_WRITER_VERSION.key -> "5"))

        alterTableWithProps("t1", Map(
          DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name"))
      verifyUpgradeAndTestSchemaEvolution("t1")
    }
  }

  test("upgrade first and then change mode") {
    withTable("t1") {
      createTableWithSQLAPI("t1", isPartitioned = true, nested = true)
      alterTableWithProps("t1", Map(
        DeltaConfigs.MIN_READER_VERSION.key -> "2",
        DeltaConfigs.MIN_WRITER_VERSION.key -> "5"))

        alterTableWithProps("t1", Map(
          DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name"))
      verifyUpgradeAndTestSchemaEvolution("t1")
    }
  }

  test("upgrade and change mode in one ALTER TABLE cmd") {
    withTable("t1") {
      createTableWithSQLAPI("t1", isPartitioned = true, nested = true)

        alterTableWithProps("t1", Map(
          DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name",
          DeltaConfigs.MIN_READER_VERSION.key -> "2",
          DeltaConfigs.MIN_WRITER_VERSION.key -> "5"))
      verifyUpgradeAndTestSchemaEvolution("t1")
    }
  }

  test("illegal mode changes") {
    val oldModes = Seq("none") ++ supportedModes
    val newModes = Seq("none") ++ supportedModes
    val upgrade = Seq(true, false)
    val removalAllowed = Seq(true, false)
    for(oldMode <- oldModes; newMode <- newModes; ug <- upgrade; ra <- removalAllowed) {
      val oldProps = Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> oldMode)
      val newProps = Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> newMode) ++
        (if (!ug) Map.empty else Map(
          DeltaConfigs.MIN_READER_VERSION.key -> "2",
          DeltaConfigs.MIN_WRITER_VERSION.key -> "5"))
      val isSupportedChange = {
        // No change.
        (oldMode == newMode) ||
          // Downgrade allowed with a flag.
          (ra && oldMode != NoMapping.name && newMode == NoMapping.name) ||
          // Upgrade always allowed.
          (oldMode == NoMapping.name && newMode == NameMapping.name)
      }
      if (!isSupportedChange) {
        Given(s"old mode: $oldMode, new mode: $newMode, upgrade: $ug, removalAllowed: $ra")
        val e = intercept[UnsupportedOperationException] {
          withTable("t1") {
            createTableWithSQLAPI("t1", props = oldProps)
            withSQLConf(DeltaSQLConf.ALLOW_COLUMN_MAPPING_REMOVAL.key ->
              ra.toString) {
              alterTableWithProps("t1", props = newProps)
            }
          }
        }
        assert(e.getMessage.contains("Changing column mapping mode from"))
      }
    }
  }

  test("getPhysicalNameFieldMap") {
    // To keep things simple, we use schema `schemaWithPhysicalNamesNested` such that the
    // physical name is just the logical name repeated three times.

    val actual = DeltaColumnMapping
      .getPhysicalNameFieldMap(schemaWithPhysicalNamesNested)
      .map { case (physicalPath, field) => (physicalPath, field.name) }

    val expected = Map[Seq[String], String](
      Seq("aaa") -> "a",
      Seq("bbb") -> "b",
      Seq("bbb", "ccc") -> "c",
      Seq("bbb", "ddd") -> "d",
      Seq("bbb", "foo.foo.foo.bar.bar.bar") -> "foo.bar",
      Seq("bbb", "foo.foo.foo.bar.bar.bar", "fff") -> "f",
      Seq("ggg") -> "g",
      Seq("ggg", "ccc") -> "c",
      Seq("ggg", "ddd") -> "d",
      Seq("ggg", "foo.foo.foo.bar.bar.bar") -> "foo.bar",
      Seq("ggg", "foo.foo.foo.bar.bar.bar", "fff") -> "f",
      Seq("hhh") -> "h"
    )

    assert(expected === actual,
      s"""
         |The actual physicalName -> logicalName map
         |${actual.mkString("\n")}
         |did not equal the expected map
         |${expected.mkString("\n")}
         |""".stripMargin)
  }

  testColumnMapping("is drop/rename column operation") { mode =>
    import DeltaColumnMapping.{isDropColumnOperation, isRenameColumnOperation}

    withTable("t1") {
      def getMetadata(): MetadataAction = {
        DeltaLog.forTableWithSnapshot(spark, TableIdentifier("t1"))._2.metadata
      }

      createStrictSchemaTableWithDeltaTableApi(
        "t1",
        schemaWithPhysicalNamesNested,
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode)
      )

      // case 1: currentSchema compared with itself
      var currentMetadata = getMetadata()
      var newMetadata = getMetadata()
      assert(
        !isDropColumnOperation(newMetadata, currentMetadata) &&
          !isRenameColumnOperation(newMetadata, currentMetadata)
      )

      // case 2: add a top-level column
      sql("ALTER TABLE t1 ADD COLUMNS (ping INT)")
      currentMetadata = newMetadata
      newMetadata = getMetadata()
      assert(
        !isDropColumnOperation(newMetadata, currentMetadata) &&
          !isRenameColumnOperation(newMetadata, currentMetadata)
      )

      // case 3: add a nested column
      sql("ALTER TABLE t1 ADD COLUMNS (b.`foo.bar`.`my.new;col()` LONG)")
      currentMetadata = newMetadata
      newMetadata = getMetadata()
      assert(
        !isDropColumnOperation(newMetadata, currentMetadata) &&
          !isRenameColumnOperation(newMetadata, currentMetadata)
      )

      // case 4: drop a top-level column
      sql("ALTER TABLE t1 DROP COLUMN (ping)")
      currentMetadata = newMetadata
      newMetadata = getMetadata()
      assert(
        isDropColumnOperation(newMetadata, currentMetadata) &&
          !isRenameColumnOperation(newMetadata, currentMetadata)
      )

      // case 5: drop a nested column
      sql("ALTER TABLE t1 DROP COLUMN (g.`foo.bar`)")
      currentMetadata = newMetadata
      newMetadata = getMetadata()
      assert(
        isDropColumnOperation(newMetadata, currentMetadata) &&
          !isRenameColumnOperation(newMetadata, currentMetadata)
      )

      // case 6: rename a top-level column
      sql("ALTER TABLE t1 RENAME COLUMN a TO pong")
      currentMetadata = newMetadata
      newMetadata = getMetadata()
      assert(
        !isDropColumnOperation(newMetadata, currentMetadata) &&
          isRenameColumnOperation(newMetadata, currentMetadata)
      )

      // case 7: rename a nested column
      sql("ALTER TABLE t1 RENAME COLUMN b.c TO c2")
      currentMetadata = newMetadata
      newMetadata = getMetadata()
      assert(
        !isDropColumnOperation(newMetadata, currentMetadata) &&
          isRenameColumnOperation(newMetadata, currentMetadata)
      )
    }
  }

  Seq(true, false).foreach { cdfEnabled =>
    var shouldBlock = cdfEnabled

    val shouldBlockStr = if (shouldBlock) "should block" else "should not block"

    def checkHelper(
        log: DeltaLog,
        newSchema: StructType,
        action: Action,
        shouldFail: Boolean = shouldBlock): Unit = {
      val txn = log.startTransaction()
      txn.updateMetadata(txn.metadata.copy(schemaString = newSchema.json))

      if (shouldFail) {
        val e = intercept[DeltaUnsupportedOperationException] {
          txn.commit(Seq(action), DeltaOperations.ManualUpdate)
        }.getMessage
        assert(e == "[DELTA_BLOCK_COLUMN_MAPPING_AND_CDC_OPERATION] " +
          "Operation \"Manual Update\" is not allowed when the table has enabled " +
          "change data feed (CDF) and has undergone schema changes using DROP COLUMN or RENAME " +
          "COLUMN.")
      } else {
        txn.commit(Seq(action), DeltaOperations.ManualUpdate)
      }
    }

    val fileActions = Seq(
      AddFile("foo", Map.empty, 1L, 1L, dataChange = true),
      AddFile("foo", Map.empty, 1L, 1L, dataChange = true).remove) ++
      (if (cdfEnabled) AddCDCFile("foo", Map.empty, 1L) :: Nil else Nil)

    testColumnMapping(
      s"CDF and Column Mapping: $shouldBlockStr when CDF=$cdfEnabled",
      enableSQLConf = true) { mode =>

      def createTable(): Unit = {
        createStrictSchemaTableWithDeltaTableApi(
          "t1",
          schemaWithPhysicalNamesNested,
          Map(
            DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode,
            DeltaConfigs.CHANGE_DATA_FEED.key -> cdfEnabled.toString
          )
        )
      }

      Seq("h", "b.`foo.bar`.f").foreach { colName =>

        // case 1: drop column with non-FileAction action should always pass
        withTable("t1") {
          createTable()
          val log = DeltaLog.forTable(spark, TableIdentifier("t1"))
          val droppedColumnSchema = sql("SELECT * FROM t1").drop(colName).schema
          checkHelper(log, droppedColumnSchema, SetTransaction("id", 1, None), shouldFail = false)
        }

        // case 2: rename column with FileAction should fail if $shouldBlock == true
        fileActions.foreach { fileAction =>
          withTable("t1") {
            createTable()
            val log = DeltaLog.forTable(spark, TableIdentifier("t1"))
            withSQLConf(
                DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> mode) {
              withTable("t2") {
                sql("DROP TABLE IF EXISTS t2")
                sql("CREATE TABLE t2 USING DELTA AS SELECT * FROM t1")
                sql(s"ALTER TABLE t2 RENAME COLUMN $colName TO ii")
                val renamedColumnSchema = sql("SELECT * FROM t2").schema
                checkHelper(log, renamedColumnSchema, fileAction)
              }
            }
          }
        }

        // case 3: drop column with FileAction should fail if $shouldBlock == true
        fileActions.foreach { fileAction =>
          {
            withTable("t1") {
              createTable()
              val log = DeltaLog.forTable(spark, TableIdentifier("t1"))
              val droppedColumnSchema = sql("SELECT * FROM t1").drop(colName).schema
              checkHelper(log, droppedColumnSchema, fileAction)
            }
          }
        }
      }
    }
  }

  testColumnMapping("id and name mode should write field_id in parquet schema",
      modes = Some(Seq("name", "id"))) { mode =>
    withTable("t1") {
      createTableWithSQLAPI(
        "t1",
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> mode))
      val (log, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("t1"))
      val files = snapshot.allFiles.collect()
      files.foreach { f =>
        val footer = ParquetFileReader.readFooter(
          log.newDeltaHadoopConf(),
          f.absolutePath(log),
          ParquetMetadataConverter.NO_FILTER)
        footer.getFileMetaData.getSchema.getFields.asScala.foreach(f =>
          // getId.intValue will throw NPE if field id does not exist
          assert(f.getId.intValue >= 0)
        )
      }
    }
  }

  test("should block CM upgrade when commit has FileActions and CDF enabled") {
    Seq(true, false).foreach { cdfEnabled =>
      var shouldBlock = cdfEnabled

      withTable("t1") {
        createTableWithSQLAPI(
          "t1",
          props = Map(DeltaConfigs.CHANGE_DATA_FEED.key -> cdfEnabled.toString))

        val table = DeltaTableV2(spark, TableIdentifier("t1"))
        val currMetadata = table.snapshot.metadata
        val upgradeMetadata = currMetadata.copy(
          configuration = currMetadata.configuration ++ Map(
            DeltaConfigs.MIN_READER_VERSION.key -> "2",
            DeltaConfigs.MIN_WRITER_VERSION.key -> "5",
            DeltaConfigs.COLUMN_MAPPING_MODE.key -> NameMapping.name
          )
        )

        val txn = table.startTransactionWithInitialSnapshot()
        txn.updateMetadata(upgradeMetadata)

        if (shouldBlock) {
          val e = intercept[DeltaUnsupportedOperationException] {
            txn.commit(
              AddFile("foo", Map.empty, 1L, 1L, dataChange = true) :: Nil,
              DeltaOperations.ManualUpdate)
          }.getMessage
          assert(e == "[DELTA_BLOCK_COLUMN_MAPPING_AND_CDC_OPERATION] " +
            "Operation \"Manual Update\" is not allowed when the table has enabled " +
            "change data feed (CDF) and has undergone schema changes using DROP COLUMN or RENAME " +
            "COLUMN.")
        } else {
          txn.commit(
            AddFile("foo", Map.empty, 1L, 1L, dataChange = true) :: Nil,
            DeltaOperations.ManualUpdate)
        }
      }
    }
  }

  test("upgrade with dot column name should not be blocked") {
    testCreateTableColumnMappingMode(
      "t1",
      schemaWithDottedColumnNames,
      false,
      "name",
      createNewTable = false,
      tableFeaturesProtocolExpected = false
    ) {
      sql(s"CREATE TABLE t1 (${schemaWithDottedColumnNames.toDDL}) USING DELTA")
      alterTableWithProps("t1", props = Map(
        DeltaConfigs.COLUMN_MAPPING_MODE.key -> "name",
        DeltaConfigs.MIN_READER_VERSION.key -> "2",
        DeltaConfigs.MIN_WRITER_VERSION.key -> "5"))
    }
  }

  test("explicit id matching") {
    // Explicitly disable field id reading to test id mode reinitialization
    val requiredConfs = Seq(
      SQLConf.PARQUET_FIELD_ID_READ_ENABLED,
      SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED)

    requiredConfs.foreach { conf =>
      withSQLConf(conf.key -> "false") {
        val e = intercept[IllegalArgumentException] {
          withTable("t1") {
            createStrictSchemaTableWithDeltaTableApi(
              "t1",
              schemaWithIdNested,
              Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "id")
            )
            val testData = spark.createDataFrame(
              Seq(Row("str3", Row("str1.3", 3))).asJava, schemaWithIdNested)
            testData.write.format("delta").mode("append").saveAsTable("t1")
          }
        }
        assert(e.getMessage.contains(conf.key))
      }
    }

    // The above configs are enabled by default, so no need to explicitly enable.
    withTable("t1") {
      val testSchema = schemaWithIdNested.add("e", StringType, true, withId(5))
      val testData = spark.createDataFrame(
        Seq(Row("str3", Row("str1.3", 3), "str4")).asJava, testSchema)

      createStrictSchemaTableWithDeltaTableApi(
        "t1",
        testSchema,
        Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> "id")
      )

      testData.write.format("delta").mode("append").saveAsTable("t1")

      def read: DataFrame = spark.read.format("delta").table("t1")
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("t1"))

      def updateFieldIdFor(fieldName: String, newId: Int): Unit = {
        val currentMetadata = deltaLog.update().metadata
        val currentSchema = currentMetadata.schema
        val field = currentSchema(fieldName)
        deltaLog.withNewTransaction { txn =>
          val updated = field.copy(metadata =
            new MetadataBuilder().withMetadata(field.metadata)
              .putLong(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY, newId)
              .putLong(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY, newId)
              .build())
          val newSchema = StructType(Seq(updated) ++ currentSchema.filter(_.name != field.name))
          txn.commit(currentMetadata.copy(
            schemaString = newSchema.json,
            configuration = currentMetadata.configuration ++
              // Just a big id to bypass the check
              Map(DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> "10000")) :: Nil, ManualUpdate)
        }
      }

      // Case 1: manually modify the schema to read a non-existing id
      updateFieldIdFor("a", 100)
      // Reading non-existing id should return null
      checkAnswer(read.select("a"), Row(null) :: Nil)

      // Case 2: manually modify the schema to read another field's id
      // First let's drop e, because Delta detects duplicated field
      sql(s"ALTER TABLE t1 DROP COLUMN e")
      // point to the dropped field <e>'s data
      updateFieldIdFor("a", 5)
      checkAnswer(read.select("a"), Row("str4"))
    }
  }

  test("drop and recreate external Delta table with name column mapping enabled") {
    withTempDir { dir =>
      withTable("t1") {
        val createExternalTblCmd: String =
          s"""
             |CREATE EXTERNAL TABLE t1 (a long)
             |USING DELTA
             |LOCATION '${dir.getCanonicalPath}'
             |TBLPROPERTIES('delta.columnMapping.mode'='name')""".stripMargin
        sql(createExternalTblCmd)
        // Add column and drop the old one to increment max column ID
        sql(s"ALTER TABLE t1 ADD COLUMN (b long)")
        sql(s"ALTER TABLE t1 DROP COLUMN a")
        sql(s"ALTER TABLE t1 RENAME COLUMN b to a")
        val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
        val configBeforeDrop = log.update().metadata.configuration
        assert(configBeforeDrop("delta.columnMapping.maxColumnId") == "2")
        sql(s"DROP TABLE t1")
        sql(createExternalTblCmd)
        // Configuration after recreating the external table should match the config right
        // before initially dropping it.
        assert(log.update().metadata.configuration == configBeforeDrop)
        // Adding another column picks up from the last maxColumnId and increments it
        sql(s"ALTER TABLE t1 ADD COLUMN (c string)")
        assert(log.update().metadata.configuration("delta.columnMapping.maxColumnId") == "3")
      }
    }
  }

  test("replace external Delta table with name column mapping enabled") {
    withTempDir { dir =>
      withTable("t1") {
        val replaceExternalTblCmd: String =
          s"""
             |CREATE OR REPLACE TABLE t1 (a long)
             |USING DELTA
             |LOCATION '${dir.getCanonicalPath}'
             |TBLPROPERTIES('delta.columnMapping.mode'='name')""".stripMargin
        sql(replaceExternalTblCmd)
        // Add column and drop the old one to increment max column ID
        sql(s"ALTER TABLE t1 ADD COLUMN (b long)")
        sql(s"ALTER TABLE t1 DROP COLUMN a")
        sql(s"ALTER TABLE t1 RENAME COLUMN b to a")
        val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
        assert(log.update().metadata.configuration("delta.columnMapping.maxColumnId") == "2")
        sql(replaceExternalTblCmd)
        // Configuration after replacing existing table should be like the table has started new.
        assert(log.update().metadata.configuration("delta.columnMapping.maxColumnId") == "1")
      }
    }
  }

  test("verify internal table properties only if property exists in spec and existing metadata") {
    val withoutMaxColumnId = Map[String, String]("delta.columnMapping.mode" -> "name")
    val maxColumnIdOne = Map[String, String](
      "delta.columnMapping.mode" -> "name",
      "delta.columnMapping.maxColumnId" -> "1"
    )
    val maxColumnIdOneWithOthers = Map[String, String](
      "delta.columnMapping.mode" -> "name",
      "delta.columnMapping.maxColumnId" -> "1",
      "dummy.property" -> "dummy"
    )
    val maxColumnIdTwo = Map[String, String](
      "delta.columnMapping.mode" -> "name",
      "delta.columnMapping.maxColumnId" -> "2"
    )
    // Max column ID is missing in first set of configs. So don't block on verification.
    assert(DeltaColumnMapping.verifyInternalProperties(withoutMaxColumnId, maxColumnIdOne))
    // Max column ID matches.
    assert(DeltaColumnMapping.verifyInternalProperties(maxColumnIdOne, maxColumnIdOneWithOthers))
    // Max column IDs don't match
    assert(!DeltaColumnMapping.verifyInternalProperties(maxColumnIdOne, maxColumnIdTwo))
  }

  testColumnMapping(
    "overwrite a column mapping table should preserve column mapping metadata",
    enableSQLConf = true) { _ =>
    val data = spark.range(10).toDF("id").withColumn("value", lit(1))

    def checkReadability(
        oldDf: DataFrame,
        expected: DataFrame,
        overwrite: () => Unit,
        // Whether the new data files are readable after applying the fix.
        readableWithFix: Boolean = true,
        // Whether the method can read the new data files out of box, regardless of the fix.
        readableOutOfBox: Boolean = false): Unit = {
      // Overwrite
      overwrite()
      if (readableWithFix) {
        // Previous analyzed DF is still readable
        // Apply a .select so the plan cache won't kick in.
        checkAnswer(oldDf.select("id"), expected.select("id").collect())
        withSQLConf(DeltaSQLConf.REUSE_COLUMN_MAPPING_METADATA_DURING_OVERWRITE.key -> "false") {
          // Overwrite again
          overwrite()
          if (readableOutOfBox) {
            checkAnswer(oldDf.select("value"), expected.select("value").collect())
          } else {
            // Without the fix, will fail
            assert {
              intercept[DeltaAnalysisException] {
                oldDf.select("value").collect()
              }.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"
            }
          }
        }
      } else {
        // Not readable, just fail
        assert {
          intercept[DeltaAnalysisException] {
            oldDf.select("value").collect()
          }.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"
        }
      }
    }

    // Readable - overwrite using DF
    val overwriteData1 = spark.range(10, 20).toDF("id").withColumn("value", lit(2))
    withTempDir { dir =>
      data.write.format("delta").save(dir.getCanonicalPath)
      val df = spark.read.format("delta").load(dir.getCanonicalPath)
      checkAnswer(df, data.collect())
      checkReadability(df, overwriteData1, () => {
        overwriteData1.write.mode("overwrite")
          .option("overwriteSchema", "true")
          .format("delta")
          .save(dir.getCanonicalPath)
      })
    }

    // Unreadable - data type changes
    val overwriteIncompatibleDatatType =
      spark.range(10, 20).toDF("id").withColumn("value", lit("name"))
    withTempDir { dir =>
      data.write.format("delta").save(dir.getCanonicalPath)
      val df = spark.read.format("delta").load(dir.getCanonicalPath)
      checkAnswer(df, data.collect())
      checkReadability(df, overwriteIncompatibleDatatType, () => {
        overwriteIncompatibleDatatType.write.mode("overwrite")
          .option("overwriteSchema", "true")
          .format("delta")
          .save(dir.getCanonicalPath)
      }, readableWithFix = false)
    }

    def withTestTable(f: (String, DataFrame) => Unit): Unit = {
      val tableName = s"cm_table"
      withTable(tableName) {
        data.createOrReplaceTempView("src_data")
        spark.sql(s"CREATE TABLE $tableName USING DELTA AS SELECT * FROM src_data")
        val df = spark.read.table(tableName)
        checkAnswer(df, data.collect())

        f(tableName, df)
      }
    }

    withTestTable { (tableName, df) =>
      // "overwrite" using REPLACE won't be covered by this fix because this is logically equivalent
      // to DROP and RECREATE a new table. Therefore this optimization won't kick in.
      overwriteData1.createOrReplaceTempView("overwrite_data")
      checkReadability(df, overwriteData1, () => {
        spark.sql(s"REPLACE TABLE $tableName USING DELTA AS SELECT * FROM overwrite_data")
      }, readableWithFix = false)
    }

    withTestTable { (tableName, df) =>
      // "overwrite" using INSERT OVERWRITE actually works without this fix because it will NOT
      // trigger the overwriteSchema code path. In this case, the pre and post schema are exactly
      // the same, so in fact no schema updates would occur.
      val overwriteData2 = spark.range(20, 30).toDF("id").withColumn("value", lit(2))
      overwriteData2.createOrReplaceTempView("overwrite_data2")
      checkReadability(df, overwriteData2, () => {
        spark.sql(s"INSERT OVERWRITE $tableName SELECT * FROM overwrite_data2")
      }, readableOutOfBox = true)
    }
  }

  test("column mapping upgrade with table features") {
    val testTableName = "columnMappingTestTable"
    withTable(testTableName) {
      val minReaderKey = DeltaConfigs.MIN_READER_VERSION.key
      val minWriterKey = DeltaConfigs.MIN_WRITER_VERSION.key
      sql(
        s"""CREATE TABLE $testTableName
           |USING DELTA
           |TBLPROPERTIES(
           |'${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = 'true'
           |)
           |AS SELECT * FROM RANGE(1)
           |""".stripMargin)

      // [[DeltaColumnMapping.verifyAndUpdateMetadataChange]] should not throw an error. The table
      // does not need to support read table features too.
      val columnMappingMode = DeltaConfigs.COLUMN_MAPPING_MODE.key
      sql(
        s"""ALTER TABLE $testTableName SET TBLPROPERTIES(
           |'$columnMappingMode'='name'
           |)""".stripMargin)

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))
      assert(deltaLog.update().protocol === Protocol(2, 7).withFeatures(Seq(
        AppendOnlyTableFeature,
        InvariantsTableFeature,
        ColumnMappingTableFeature,
        RowTrackingFeature
      )))
    }
  }

  test("DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES exception should include column names") {
    val testTableName = "columnMappingTestTable"
    withTable(testTableName) {
      val invalidColName1 = colName("col1")
      val invalidColName2 = colName("col2")
      // Make sure the error class stays the same for a single and multiple columns.
      testWithInvalidColumns(Seq(invalidColName1))
      testWithInvalidColumns(Seq(invalidColName1, invalidColName2))

      def testWithInvalidColumns(invalidColumns: Seq[String]): Unit = {
        val allColumns = (Seq("a", "b") ++ invalidColumns)
          .mkString("(`", "` int, `", "` int)")
        val e = intercept[DeltaAnalysisException] {
          sql(
            s"""CREATE TABLE $testTableName $allColumns
               |USING DELTA
               |TBLPROPERTIES('${DeltaConfigs.COLUMN_MAPPING_MODE.key}'='none')
               |""".stripMargin)
        }
        val errorClass = "DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES"
        checkError(
          exception = e,
          errorClass = errorClass,
          parameters = DeltaThrowableHelper
            .getParameterNames(errorClass, errorSubClass = null)
            .zip(invalidColumns).toMap
        )
      }
    }
  }

  test("filters pushed down to parquet use physical names") {
    val tableName = "table_name"
    withTable(tableName) {
      // Create a table with column mapping **disabled**
      sql(
        s"""CREATE TABLE $tableName (a INT, b INT)
           |USING DELTA
           |TBLPROPERTIES (
           |  '${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'none',
           |  '${DeltaConfigs.MIN_READER_VERSION.key}' = '2',
           |  '${DeltaConfigs.MIN_WRITER_VERSION.key}' = '5'
           |)
           |""".stripMargin)

      sql(s"INSERT INTO $tableName VALUES (100, 1000)")

      sql(
        s"""ALTER TABLE $tableName
           |SET TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name')
           |""".stripMargin)

      // Confirm that the physical names are equal to the logical names
      val schema = DeltaLog.forTable(spark, TableIdentifier(tableName)).update().schema
      assert(DeltaColumnMapping.getPhysicalName(schema("a")) == "a")
      assert(DeltaColumnMapping.getPhysicalName(schema("b")) == "b")

      // Rename the columns so that the logical name of the second column is equal to the physical
      // name of the first column.
      sql(s"ALTER TABLE $tableName RENAME COLUMN a TO c")
      sql(s"ALTER TABLE $tableName RENAME COLUMN b TO a")

      // Filter the table by the second column. This will return empty results if the filter was
      // (incorrectly) pushed down without translating the logical names to physical names.
      checkAnswer(
        sql(s"SELECT * FROM $tableName WHERE a = 1000"),
        Seq(Row(100, 1000))
      )
    }
  }

  testColumnMapping("stream read from column mapping does not leak metadata") { mode =>
    withTempDir { dir =>
      val (t1, t2, t3) = (
        s"t1_${System.currentTimeMillis()}",
        s"t2_${System.currentTimeMillis()}",
        s"t3_${System.currentTimeMillis()}"
      )
      withTable(t1, t2, t3) {
        // Create source table with column mapping mode and partitioning
        sql(
          s"""CREATE TABLE $t1 (a INT, b STRING)
             |USING DELTA
             |PARTITIONED BY (b)
             |TBLPROPERTIES (
             |  '${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = '$mode',
             |  '${DeltaConfigs.MIN_READER_VERSION.key}' = '2',
             |  '${DeltaConfigs.MIN_WRITER_VERSION.key}' = '5'
             |)
             |""".stripMargin)
        // Insert data into source table
        sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b')")

        // Stream read from source table
        val streamDf = spark.readStream.format("delta").table(t1)
        // Should not contain column mapping metadata
        assert(streamDf.schema.forall(_.metadata.json == "{}"))

        // Create and write to another table
        // The streaming create-table path is what currently leaks the column mapping metadata
        // into the target table. If it was writing to an existing table via DeltaSink, it would not
        // leak because we pruned the column mapping metadata in [[ImplicitMetadataOperations]] when
        // we update the target metadata.
        val q = streamDf.writeStream
          .partitionBy("b")
          .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
          .format("delta")
          .option("checkpointLocation", new File(dir, "_checkpoint1").getCanonicalPath)
          .toTable(t2)
        q.awaitTermination()

        // Check target table Delta log
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(t2))
        assert(deltaLog.update().metadata.schema.forall(_.metadata.json == "{}"))
        assert(deltaLog.update().metadata.columnMappingMode == NoMapping)

        // Check target table data
        checkAnswer(spark.table(t2), Seq(Row(1, "a"), Row(2, "b")))
      }
    }
  }
}
