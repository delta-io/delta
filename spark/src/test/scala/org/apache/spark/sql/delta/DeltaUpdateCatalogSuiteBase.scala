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

import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.hooks.UpdateCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.scalatest.time.SpanSugar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.util.{ThreadUtils, Utils}

abstract class DeltaUpdateCatalogSuiteBase
  extends QueryTest
    with DeltaSQLTestUtils
    with SpanSugar {

  protected val tbl = "delta_table"

  import testImplicits._

  protected def cleanupDefaultTable(): Unit = disableUpdates {
    spark.sql(s"DROP TABLE IF EXISTS $tbl")
    val path = spark.sessionState.catalog.defaultTablePath(TableIdentifier(tbl))
    try Utils.deleteRecursively(new File(path)) catch {
      case NonFatal(e) => // do nothing
    }
  }

  /** Turns off the storing of metadata (schema + properties) in the catalog. */
  protected def disableUpdates(f: => Unit): Unit = {
    withSQLConf(DeltaSQLConf.DELTA_UPDATE_CATALOG_ENABLED.key -> "false") {
      f
    }
  }

  protected def deltaLog: DeltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))
  protected def snapshot: Snapshot = deltaLog.unsafeVolatileSnapshot
  protected def snapshotAt(v: Long): Snapshot = deltaLog.getSnapshotAt(v)

  protected def getBaseProperties(snapshot: Snapshot): Map[String, String] = {
    Map(
      DeltaConfigs.METASTORE_LAST_UPDATE_VERSION -> snapshot.version.toString,
      DeltaConfigs.METASTORE_LAST_COMMIT_TIMESTAMP -> snapshot.timestamp.toString,
      DeltaConfigs.MIN_READER_VERSION.key -> snapshot.protocol.minReaderVersion.toString,
      DeltaConfigs.MIN_WRITER_VERSION.key -> snapshot.protocol.minWriterVersion.toString) ++
      snapshot.protocol.readerAndWriterFeatureNames.map { name =>
        s"${TableFeatureProtocolUtils.FEATURE_PROP_PREFIX}$name" ->
          TableFeatureProtocolUtils.FEATURE_PROP_SUPPORTED
      } ++ snapshot.metadata.configuration.get("delta.enableDeletionVectors")
        .map("delta.enableDeletionVectors" -> _).toMap
  }

  /**
   * Verifies that the table metadata in the catalog are eventually up-to-date. Updates to the
   * catalog are generally asynchronous, except explicit DDL operations, e.g. CREATE/REPLACE.
   */
  protected def verifyTableMetadataAsync(
      expectedSchema: StructType,
      expectedProperties: Map[String, String] = getBaseProperties(snapshot),
      table: String = tbl,
      partitioningCols: Seq[String] = Nil): Unit = {
    // We unfortunately need an eventually, because the updates can be async
    eventually(timeout(10.seconds)) {
      verifyTableMetadata(expectedSchema, expectedProperties, table, partitioningCols)
    }
    // Ensure that no other threads will later revert us back to the state we just checked
    if (!UpdateCatalog.awaitCompletion(10000)) {
      logWarning(s"There are active catalog udpate requests after 10 seconds")
    }
  }

  protected def filterProperties(properties: Map[String, String]): Map[String, String]

  /** Verifies that the table metadata in the catalog are up-to-date. */
  protected def verifyTableMetadata(
      expectedSchema: StructType,
      expectedProperties: Map[String, String] = getBaseProperties(snapshot),
      table: String = tbl,
      partitioningCols: Seq[String] = Nil): Unit = {
    DeltaLog.clearCache()
    val cat = spark.sessionState.catalog.externalCatalog.getTable("default", table)
    assert(cat.schema === expectedSchema, s"Schema didn't match for table: $table")
    assert(cat.partitionColumnNames === partitioningCols)
    assert(filterProperties(cat.properties) === expectedProperties,
      s"Properties didn't match for table: $table")

    val tables = spark.sessionState.catalog.getTablesByName(Seq(TableIdentifier(table)))

    assert(tables.head.schema === expectedSchema)
    assert(tables.head.partitionColumnNames === partitioningCols)
    assert(filterProperties(tables.head.properties) === expectedProperties)
  }


  test("mergeSchema") {
    withTable(tbl) {
      val df = spark.range(10).withColumn("part", 'id / 2)
      df.writeTo(tbl).using("delta").create()

      verifyTableMetadata(expectedSchema = df.schema.asNullable)

      val df2 = spark.range(10).withColumn("part", 'id / 2).withColumn("id2", 'id)
      df2.writeTo(tbl)
        .option("mergeSchema", "true")
        .append()

      verifyTableMetadataAsync(expectedSchema = df2.schema.asNullable)
    }
  }

  test("mergeSchema - nested data types") {
    withTable(tbl) {
      val df = spark.range(10).withColumn("part", 'id / 2)
        .withColumn("str", struct('id.cast("int") as "int"))
      df.writeTo(tbl).using("delta").create()

      verifyTableMetadata(expectedSchema = df.schema.asNullable)

      val df2 = spark.range(10).withColumn("part", 'id / 2)
        .withColumn("str", struct('id as "id2", 'id.cast("int") as "int"))
      df2.writeTo(tbl)
        .option("mergeSchema", "true")
        .append()

      val schema = new StructType()
        .add("id", LongType)
        .add("part", DoubleType)
        .add("str", new StructType()
          .add("int", IntegerType)
          .add("id2", LongType)) // New columns go to the end
      verifyTableMetadataAsync(expectedSchema = schema)
    }
  }


  test("merge") {
    val tmp = "tmpView"
    withDeltaTable { df =>
      withTempView(tmp) {
        withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
          df.withColumn("id2", 'id).createOrReplaceTempView(tmp)
          sql(
            s"""MERGE INTO $tbl t
               |USING $tmp s
               |ON t.id = s.id
               |WHEN NOT MATCHED THEN INSERT *
             """.stripMargin)

          verifyTableMetadataAsync(df.withColumn("id2", 'id).schema.asNullable)
        }
      }
    }
  }

  test("creating and replacing a table puts the schema and table properties in the metastore") {
    withTable(tbl) {
      val df = spark.range(10).withColumn("part", 'id / 2).withColumn("id2", 'id)
      df.writeTo(tbl)
        .tableProperty("delta.checkpointInterval", "5")
        .tableProperty("some", "thing")
        .partitionedBy('part)
        .using("delta")
        .create()

      verifyTableMetadata(
        expectedSchema = df.schema.asNullable,
        expectedProperties = getBaseProperties(snapshot) ++ Map(
          "delta.checkpointInterval" -> "5",
          "some" -> "thing")
      )

      val df2 = spark.range(10).withColumn("part", 'id / 2)
      df2.writeTo(tbl)
        .tableProperty("other", "thing")
        .using("delta")
        .replace()

      verifyTableMetadata(
        expectedSchema = df2.schema.asNullable,
        expectedProperties = getBaseProperties(snapshot) ++ Map("other" -> "thing")
      )
    }
  }

  test("creating table in metastore over existing path") {
    withTempDir { dir =>
      withTable(tbl) {
        val df = spark.range(10).withColumn("part", 'id % 2).withColumn("id2", 'id)
        df.write.format("delta").partitionBy("part").save(dir.getCanonicalPath)

        sql(s"CREATE TABLE $tbl USING delta LOCATION '${dir.getCanonicalPath}'")
        verifyTableMetadata(df.schema.asNullable)
      }
    }
  }

  test("replacing non-Delta table") {
    withTable(tbl) {
      val df = spark.range(10).withColumn("part", 'id / 2).withColumn("id2", 'id)
      df.writeTo(tbl)
        .tableProperty("delta.checkpointInterval", "5")
        .tableProperty("some", "thing")
        .partitionedBy('part)
        .using("parquet")
        .create()

      val e = intercept[AnalysisException] {
        df.writeTo(tbl).using("delta").replace()
      }

      assert(e.getMessage.contains("not a Delta table"))
    }
  }

  test("alter table add columns") {
    withDeltaTable { df =>
      sql(s"ALTER TABLE $tbl ADD COLUMNS (id2 bigint)")
      verifyTableMetadataAsync(df.withColumn("id2", 'id).schema.asNullable)
    }
  }

  protected def runAlterTableTests(f: (String, StructType) => Unit): Unit = {
    // We set the default minWriterVersion to the version required to ADD/DROP CHECK constraints
    // to prevent an automatic protocol  upgrade  (i.e. an implicit property change) when adding
    // the CHECK constraint below.
    withSQLConf(
      "spark.databricks.delta.properties.defaults.minReaderVersion" -> "1",
      "spark.databricks.delta.properties.defaults.minWriterVersion" -> "3") {
      withDeltaTable { _ =>
        sql(s"ALTER TABLE $tbl SET TBLPROPERTIES ('some' = 'thing', 'other' = 'thing')")
        sql(s"ALTER TABLE $tbl UNSET TBLPROPERTIES ('other')")
        sql(s"ALTER TABLE $tbl ADD COLUMNS (id2 bigint, id3 bigint)")
        sql(s"ALTER TABLE $tbl CHANGE COLUMN id2 id2 bigint FIRST")
        sql(s"ALTER TABLE $tbl REPLACE COLUMNS (id3 bigint, id2 bigint, id bigint)")
        sql(s"ALTER TABLE $tbl ADD CONSTRAINT id_3 CHECK (id3 > 10)")
        sql(s"ALTER TABLE $tbl DROP CONSTRAINT id_3")

        val expectedSchema = StructType(Seq(
          StructField("id3", LongType, true),
          StructField("id2", LongType, true),
          StructField("id", LongType, true))
        )

        f(tbl, expectedSchema)
      }
    }
  }

  /**
   * Creates a table with the name `tbl` and executes a function that takes a representative
   * DataFrame with the schema of the table. Performs cleanup of the table afterwards.
   */
  protected def withDeltaTable(f: DataFrame => Unit): Unit = {
    // Turn off async updates so that we don't update the catalog during table cleanup
    disableUpdates {
      withTable(tbl) {
        withSQLConf(DeltaSQLConf.DELTA_UPDATE_CATALOG_ENABLED.key -> "true") {
          sql(s"CREATE TABLE $tbl (id bigint) USING delta")
          val df = spark.range(10)
          verifyTableMetadata(df.schema.asNullable)

          f(df.toDF())
        }
      }
    }
  }

  test("skip update when flag is not set") {
    withDeltaTable(df => {
      withSQLConf(DeltaSQLConf.DELTA_UPDATE_CATALOG_ENABLED.key -> "false") {
        val propertiesAtV1 = getBaseProperties(snapshot)
        sql(s"ALTER TABLE $tbl SET TBLPROPERTIES(some.key = 1)")
        verifyTableMetadataAsync(
          expectedSchema = df.schema.asNullable,
          expectedProperties = propertiesAtV1)
      }
    })
  }


  test(s"REORG TABLE does not perform catalog update") {
    val tableName = "myTargetTable"
    withDeltaTable { df =>
      sql(s"REORG TABLE $tbl APPLY (PURGE)")
      verifyTableMetadataAsync(df.schema.asNullable)
    }
  }
}
