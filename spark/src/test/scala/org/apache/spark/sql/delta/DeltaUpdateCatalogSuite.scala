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

import scala.util.control.NonFatal

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.hooks.UpdateCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaHiveTest
import com.fasterxml.jackson.core.JsonParseException

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogWithListener
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.util.{ThreadUtils, Utils}

class DeltaUpdateCatalogSuite
  extends DeltaUpdateCatalogSuiteBase
    with DeltaHiveTest {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.DELTA_UPDATE_CATALOG_ENABLED.key, "true")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    cleanupDefaultTable()
  }

  override def afterEach(): Unit = {
    if (!UpdateCatalog.awaitCompletion(10000)) {
      logWarning(s"There are active catalog udpate requests after 10 seconds")
    }
    cleanupDefaultTable()
    super.afterEach()
  }

  /** Remove Hive specific table properties. */
  override protected def filterProperties(properties: Map[String, String]): Map[String, String] = {
    properties.filterKeys(_ != "transient_lastDdlTime").toMap
  }


  test("streaming") {
    withTable(tbl) {
      implicit val _sqlContext = spark.sqlContext
      val stream = MemoryStream[Long]
      val df1 = stream.toDF().toDF("id")

      withTempDir { dir =>
        try {
          val q = df1.writeStream
            .option("checkpointLocation", dir.getCanonicalPath)
            .format("delta")
            .toTable(tbl)

          verifyTableMetadata(expectedSchema = df1.schema.asNullable)

          stream.addData(1, 2, 3)
          q.processAllAvailable()
          q.stop()

          val q2 = df1.withColumn("id2", 'id)
            .writeStream
            .format("delta")
            .option("mergeSchema", "true")
            .option("checkpointLocation", dir.getCanonicalPath)
            .toTable(tbl)

          stream.addData(4, 5, 6)
          q2.processAllAvailable()

          verifyTableMetadataAsync(expectedSchema = df1.schema.asNullable.add("id2", LongType))
        } finally {
          spark.streams.active.foreach(_.stop())
        }
      }
    }
  }

  test("streaming - external location") {
    withTempDir { dir =>
      withTable(tbl) {
        implicit val _sqlContext = spark.sqlContext
        val stream = MemoryStream[Long]
        val df1 = stream.toDF().toDF("id")

        val chk = new File(dir, "chkpoint").getCanonicalPath
        val data = new File(dir, "data").getCanonicalPath
        try {
          val q = df1.writeStream
            .option("checkpointLocation", chk)
            .format("delta")
            .option("path", data)
            .toTable(tbl)

          verifyTableMetadata(expectedSchema = df1.schema.asNullable)

          stream.addData(1, 2, 3)
          q.processAllAvailable()
          q.stop()

          val q2 = df1.withColumn("id2", 'id)
            .writeStream
            .format("delta")
            .option("mergeSchema", "true")
            .option("checkpointLocation", chk)
            .toTable(tbl)

          stream.addData(4, 5, 6)
          q2.processAllAvailable()

          verifyTableMetadataAsync(expectedSchema = df1.schema.add("id2", LongType).asNullable)
        } finally {
          spark.streams.active.foreach(_.stop())
        }
      }
    }
  }

  test("streaming - external table that already exists") {
    withTable(tbl) {
      implicit val _sqlContext = spark.sqlContext
      val stream = MemoryStream[Long]
      val df1 = stream.toDF().toDF("id")

      withTempDir { dir =>
        val chk = new File(dir, "chkpoint").getCanonicalPath
        val data = new File(dir, "data").getCanonicalPath

        spark.range(10).write.format("delta").save(data)
        try {
          val q = df1.writeStream
            .option("checkpointLocation", chk)
            .format("delta")
            .option("path", data)
            .toTable(tbl)

          verifyTableMetadataAsync(expectedSchema = df1.schema.asNullable)

          stream.addData(1, 2, 3)
          q.processAllAvailable()
          q.stop()

          val q2 = df1.withColumn("id2", 'id)
            .writeStream
            .format("delta")
            .option("mergeSchema", "true")
            .option("checkpointLocation", chk)
            .toTable(tbl)

          stream.addData(4, 5, 6)
          q2.processAllAvailable()

          verifyTableMetadataAsync(expectedSchema = df1.schema.add("id2", LongType).asNullable)
        } finally {
          spark.streams.active.foreach(_.stop())
        }
      }
    }
  }

  val MAX_CATALOG_TYPE_DDL_LENGTH: Long =
    DeltaSQLConf.DELTA_UPDATE_CATALOG_LONG_FIELD_TRUNCATION_THRESHOLD.defaultValue.get


  test("convert to delta with partitioning change") {
    withTable(tbl) {
      val df = spark.range(10).withColumn("part", 'id / 2).withColumn("id2", 'id)
      df.writeTo(tbl)
        .partitionedBy('part)
        .using("parquet")
        .create()

      // Partitioning columns go to the end for parquet tables
      val tableSchema =
        new StructType().add("id", LongType).add("id2", LongType).add("part", DoubleType)
      verifyTableMetadata(
        expectedSchema = tableSchema,
        expectedProperties = Map.empty,
        partitioningCols = Seq("part")
      )

      sql(s"CONVERT TO DELTA $tbl PARTITIONED BY (part double)")
      // Information is duplicated for now
      verifyTableMetadata(
        expectedSchema = tableSchema,
        expectedProperties = Map.empty,
        partitioningCols = Seq("part")
      )

      // Remove partitioning of table
      df.writeTo(tbl).using("delta").replace()

      assert(snapshot.metadata.partitionColumns === Nil, "Table is unpartitioned")

      // Hive does not allow for the removal of the partition column once it has
      // been added. Spark keeps the partition columns towards the end if it
      // finds them in Hive. So, for converted tables with partitions,
      // Hive schema != df.schema
      val expectedSchema = tableSchema

      // Schema converts to Delta's format
      verifyTableMetadata(
        expectedSchema = expectedSchema,
        expectedProperties = getBaseProperties(snapshot),
        partitioningCols = Seq("part") // The partitioning information cannot be removed...
      )

      // table is still usable
      checkAnswer(spark.table(tbl), df)

      val df2 = spark.range(10).withColumn("id2", 'id)
      // Gets rid of partition column "part" from the schema
      df2.writeTo(tbl).using("delta").replace()

      val expectedSchema2 = new StructType()
        .add("id", LongType).add("id2", LongType).add("part", DoubleType)
      verifyTableMetadataAsync(
        expectedSchema = expectedSchema2,
        expectedProperties = getBaseProperties(snapshot),
        partitioningCols = Seq("part") // The partitioning information cannot be removed...
      )

      // table is still usable
      checkAnswer(spark.table(tbl), df2)
    }
  }

  test("partitioned table + add column") {
    withTable(tbl) {
      val df = spark.range(10).withColumn("part", 'id / 2).withColumn("id2", 'id)
      df.writeTo(tbl)
        .partitionedBy('part)
        .using("delta")
        .create()

      val tableSchema =
        new StructType().add("id", LongType).add("part", DoubleType).add("id2", LongType)
      verifyTableMetadata(
        expectedSchema = tableSchema,
        expectedProperties = getBaseProperties(snapshot),
        partitioningCols = Seq())

      sql(s"ALTER TABLE $tbl ADD COLUMNS (id3 bigint)")
      verifyTableMetadataAsync(
        expectedSchema = tableSchema.add("id3", LongType),
        expectedProperties = getBaseProperties(snapshot),
        partitioningCols = Seq())
    }
  }

  test("partitioned convert to delta with schema change") {
    withTable(tbl) {
      val df = spark.range(10).withColumn("part", 'id / 2).withColumn("id2", 'id)
      df.writeTo(tbl)
        .partitionedBy('part)
        .using("parquet")
        .create()

      // Partitioning columns go to the end
      val tableSchema =
        new StructType().add("id", LongType).add("id2", LongType).add("part", DoubleType)
      verifyTableMetadata(
        expectedSchema = tableSchema,
        expectedProperties = Map.empty,
        partitioningCols = Seq("part")
      )

      sql(s"CONVERT TO DELTA $tbl PARTITIONED BY (part double)")
      // Information is duplicated for now
      verifyTableMetadata(
        expectedSchema = tableSchema,
        expectedProperties = Map.empty,
        partitioningCols = Seq("part")
      )

      sql(s"ALTER TABLE $tbl ADD COLUMNS (id3 bigint)")

      // Hive does not allow for the removal of the partition column once it has
      // been added. Spark keeps the partition columns towards the end if it
      // finds them in Hive. So, for converted tables with partitions,
      // Hive schema != df.schema
      val expectedSchema = new StructType()
        .add("id", LongType)
        .add("id2", LongType)
        .add("id3", LongType)
        .add("part", DoubleType)

      verifyTableMetadataAsync(
        expectedSchema = expectedSchema,
        partitioningCols = Seq("part")
      )

      // Table is still queryable
      checkAnswer(
        spark.table(tbl),
        // Ordering of columns are different than df due to Hive semantics
        spark.range(10).withColumn("id2", 'id)
          .withColumn("part", 'id / 2)
          .withColumn("id3", lit(null)))
    }
  }


  test("Very long schemas can be stored in the catalog") {
    withTable(tbl) {
      val schema = StructType(Seq.tabulate(1000)(i => StructField(s"col$i", StringType)))
      require(schema.toDDL.length >= MAX_CATALOG_TYPE_DDL_LENGTH,
        s"The length of the schema should be over $MAX_CATALOG_TYPE_DDL_LENGTH " +
          "characters for this test")

      sql(s"CREATE TABLE $tbl (${schema.toDDL}) USING delta")
      verifyTableMetadata(expectedSchema = schema)
    }
  }


  for (truncationThreshold <- Seq(99999, MAX_CATALOG_TYPE_DDL_LENGTH, 4020))
  test(s"Schemas that contain very long fields cannot be stored in the catalog " +
    " when longer than the truncation threshold " +
    s" [DELTA_UPDATE_CATALOG_LONG_FIELD_TRUNCATION_THRESHOLD = $truncationThreshold]") {
    withSQLConf(
        DeltaSQLConf.DELTA_UPDATE_CATALOG_LONG_FIELD_TRUNCATION_THRESHOLD.key ->
            truncationThreshold.toString) {
      withTable(tbl) {
        val schema = new StructType()
          .add("i", StringType)
          .add("struct", StructType(Seq.tabulate(1000)(i => StructField(s"col$i", StringType))))
        require(
          schema.toDDL.length >= 4020,
          s"The length of the schema should be over 4020 " +
            s"characters for this test")

        sql(s"CREATE TABLE $tbl (${schema.toDDL}) USING delta")
        if (truncationThreshold > 4020) {
          verifyTableMetadata(expectedSchema = schema)
        } else {
          verifySchemaInCatalog()
        }
      }
    }
  }

  for (truncationThreshold <- Seq(99999, MAX_CATALOG_TYPE_DDL_LENGTH))
  test(s"Schemas that contain very long fields cannot be stored in the catalog - array" +
      " when longer than the truncation threshold " +
      s" [DELTA_UPDATE_CATALOG_LONG_FIELD_TRUNCATION_THRESHOLD = $truncationThreshold]") {
    withSQLConf(
        DeltaSQLConf.DELTA_UPDATE_CATALOG_LONG_FIELD_TRUNCATION_THRESHOLD.key ->
            truncationThreshold.toString) {
      withTable(tbl) {
        val struct = StructType(Seq.tabulate(1000)(i => StructField(s"col$i", StringType)))
        val schema = new StructType()
          .add("i", StringType)
          .add("array", ArrayType(struct))
        require(schema.toDDL.length >= MAX_CATALOG_TYPE_DDL_LENGTH,
          s"The length of the schema should be over $MAX_CATALOG_TYPE_DDL_LENGTH " +
            s"characters for this test")

        sql(s"CREATE TABLE $tbl (${schema.toDDL}) USING delta")
        if (truncationThreshold == 99999) {
          verifyTableMetadata(expectedSchema = schema)
        } else {
          verifySchemaInCatalog()
        }
      }
    }
  }

  for (truncationThreshold <- Seq(99999, MAX_CATALOG_TYPE_DDL_LENGTH))
  test(s"Schemas that contain very long fields cannot be stored in the catalog - map" +
      " when longer than the truncation threshold " +
      s" [DELTA_UPDATE_CATALOG_LONG_FIELD_TRUNCATION_THRESHOLD = $truncationThreshold]") {
    withSQLConf(
        DeltaSQLConf.DELTA_UPDATE_CATALOG_LONG_FIELD_TRUNCATION_THRESHOLD.key ->
            truncationThreshold.toString) {
      withTable(tbl) {
        val struct = StructType(Seq.tabulate(1000)(i => StructField(s"col$i", StringType)))
        val schema = new StructType()
          .add("i", StringType)
          .add("map", MapType(StringType, struct))
        require(schema.toDDL.length >= MAX_CATALOG_TYPE_DDL_LENGTH,
          s"The length of the schema should be over $MAX_CATALOG_TYPE_DDL_LENGTH " +
            s"characters for this test")

        sql(s"CREATE TABLE $tbl (${schema.toDDL}) USING delta")
        if (truncationThreshold == 99999) {
          verifyTableMetadata(expectedSchema = schema)
        } else {
          verifySchemaInCatalog()
        }
      }
    }
  }

  for (truncationThreshold <- Seq(99999, MAX_CATALOG_TYPE_DDL_LENGTH))
  test(s"Very long nested fields cannot be stored in the catalog - partitioned" +
      " when longer than the truncation threshold " +
      s" [DELTA_UPDATE_CATALOG_LONG_FIELD_TRUNCATION_THRESHOLD = $truncationThreshold]") {
    withSQLConf(
        DeltaSQLConf.DELTA_UPDATE_CATALOG_LONG_FIELD_TRUNCATION_THRESHOLD.key ->
            truncationThreshold.toString) {
      withTable(tbl) {
        val schema = new StructType()
          .add("i", StringType)
          .add("part", StringType)
          .add("struct", StructType(Seq.tabulate(1000)(i => StructField(s"col$i", StringType))))
        require(
          schema.toDDL.length >= MAX_CATALOG_TYPE_DDL_LENGTH,
          "The length of the schema should be over 4000 characters for this test")

        sql(s"CREATE TABLE $tbl (${schema.toDDL}) USING delta PARTITIONED BY (part)")
        if (truncationThreshold == 99999) {
          verifyTableMetadata(expectedSchema = schema)
        } else {
          verifySchemaInCatalog()
        }
      }
    }
  }

  test("Very long schemas can be stored in the catalog - partitioned") {
    withTable(tbl) {
      val schema = StructType(Seq.tabulate(1000)(i => StructField(s"col$i", StringType)))
        .add("part", StringType)
      require(schema.toDDL.length >= MAX_CATALOG_TYPE_DDL_LENGTH,
        "The length of the schema should be over 4000 characters for this test")

      sql(s"CREATE TABLE $tbl (${schema.toDDL}) USING delta PARTITIONED BY (part)")
      verifyTableMetadata(expectedSchema = schema)
    }
  }


  // scalastyle:off nonascii
  test("Schema containing non-latin characters cannot be stored - top-level") {
    withTable(tbl) {
      val schema = new StructType().add("今天", "string")
      sql(s"CREATE TABLE $tbl (${schema.toDDL}) USING delta")
      verifySchemaInCatalog(expectedErrorMessage = UpdateCatalog.NON_LATIN_CHARS_ERROR)
    }
  }

  test("Schema containing non-latin characters cannot be stored - struct") {
    withTable(tbl) {
      val schema = new StructType().add("struct", new StructType().add("今天", "string"))
      sql(s"CREATE TABLE $tbl (${schema.toDDL}) USING delta")
      verifySchemaInCatalog(expectedErrorMessage = UpdateCatalog.NON_LATIN_CHARS_ERROR)
    }
  }

  test("Schema containing non-latin characters cannot be stored - array") {
    withTable(tbl) {
      val schema = new StructType()
        .add("i", StringType)
        .add("array", ArrayType(new StructType().add("今天", "string")))

      sql(s"CREATE TABLE $tbl (${schema.toDDL}) USING delta")
      verifySchemaInCatalog(expectedErrorMessage = UpdateCatalog.NON_LATIN_CHARS_ERROR)
    }
  }

  test("Schema containing non-latin characters cannot be stored - map") {
    withTable(tbl) {
      val schema = new StructType()
        .add("i", StringType)
        .add("map", MapType(StringType, new StructType().add("今天", "string")))

      sql(s"CREATE TABLE $tbl (${schema.toDDL}) USING delta")
      verifySchemaInCatalog(expectedErrorMessage = UpdateCatalog.NON_LATIN_CHARS_ERROR)
    }
  }
  // scalastyle:on nonascii

  /**
   * Verifies that the schema stored in the catalog explicitly is empty, however the getTablesByName
   * method still correctly returns the actual schema.
   */
  private def verifySchemaInCatalog(
      table: String = tbl,
      catalogPartitionCols: Seq[String] = Nil,
      expectedErrorMessage: String = UpdateCatalog.LONG_SCHEMA_ERROR): Unit = {
    val cat = spark.sessionState.catalog.externalCatalog.getTable("default", table)
    assert(cat.schema.isEmpty, s"Schema wasn't empty")
    assert(cat.partitionColumnNames === catalogPartitionCols)
    getBaseProperties(snapshot).foreach { case (k, v) =>
      assert(cat.properties.get(k) === Some(v),
        s"Properties didn't match for table: $table. Expected: ${getBaseProperties(snapshot)}, " +
        s"Got: ${cat.properties}")
    }
    assert(cat.properties(UpdateCatalog.ERROR_KEY) === expectedErrorMessage)

    // Make sure table is readable
    checkAnswer(spark.table(table), Nil)
  }

  def testAddRemoveProperties(): Unit = {
    withTable(tbl) {
      val df = spark.range(10).toDF("id")
      df.writeTo(tbl)
        .using("delta")
        .create()

      var initialProperties: Map[String, String] = Map.empty
      val logs = Log4jUsageLogger.track {
        sql(s"ALTER TABLE $tbl SET TBLPROPERTIES(some.key = 1, another.key = 2)")

        initialProperties = getBaseProperties(snapshot)
        verifyTableMetadataAsync(
          expectedSchema = df.schema.asNullable,
          expectedProperties = Map("some.key" -> "1", "another.key" -> "2") ++
            initialProperties
        )
      }
      val updateLogged = logs.filter(_.metric == "tahoeEvent")
        .filter(_.tags.get("opType").exists(_.startsWith("delta.catalog.update.properties")))
      assert(updateLogged.nonEmpty, "Ensure that the schema update in the MetaStore is logged")

      // The UpdateCatalog hook only checks if new properties have been
      // added. If properties have been removed only, no metadata update will be triggered.
      val logs2 = Log4jUsageLogger.track {
        sql(s"ALTER TABLE $tbl UNSET TBLPROPERTIES(another.key)")
        verifyTableMetadataAsync(
          expectedSchema = df.schema.asNullable,
          expectedProperties = Map("some.key" -> "1", "another.key" -> "2") ++
            initialProperties
        )
      }
      val updateLogged2 = logs2.filter(_.metric == "tahoeEvent")
        .filter(_.tags.get("opType").exists(_.startsWith("delta.catalog.update.properties")))
      assert(updateLogged2.size == 0, "Ensure that the schema update in the MetaStore is logged")

      // Adding a new property will trigger an update
      val logs3 = Log4jUsageLogger.track {
        sql(s"ALTER TABLE $tbl SET TBLPROPERTIES(a.third.key = 3)")
        verifyTableMetadataAsync(
          expectedSchema = df.schema.asNullable,
          expectedProperties = Map("some.key" -> "1", "a.third.key" -> "3") ++
            getBaseProperties(snapshot)
        )
      }
      val updateLogged3 = logs3.filter(_.metric == "tahoeEvent")
        .filter(_.tags.get("opType").exists(_.startsWith("delta.catalog.update.properties")))
      assert(updateLogged3.nonEmpty, "Ensure that the schema update in the MetaStore is logged")
    }
  }

  test("add and remove properties") {
    testAddRemoveProperties()
  }

  test("alter table commands update the catalog") {
    runAlterTableTests { (tableName, expectedSchema) =>
      verifyTableMetadataAsync(
        expectedSchema = expectedSchema,
        // The ALTER TABLE statements in runAlterTableTests create table version 7.
        // However, version 7 is created by dropping a CHECK constraint, which currently
        // *does not* trigger a catalog update. For Hive tables, only *adding* properties
        // causes a catalog update, not *removing*. Hence, the metadata in the catalog should
        // still be at version 6.
        expectedProperties = getBaseProperties(snapshotAt(6)) ++
          Map("some" -> "thing", "delta.constraints.id_3" -> "id3 > 10"),
        table = tableName
      )
    }
  }
}
