/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.delta.DeltaConfigs.CHECKPOINT_INTERVAL
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.delta.test.HiveDeltaSQLCommandTest
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, MapType, StringType, StructField, StructType}

class HiveDeltaCatalogDDLSuite  extends QueryTest with SharedSparkSession
            with SQLTestUtils with HiveDeltaSQLCommandTest with DeltaAlterTableTestBase {

    override protected def createTable(schema: String,
                                       tblProperties: Map[String, String]): String = {
        val props = tblProperties.map { case (key, value) =>
            s"'$key' = '$value'"
        }.mkString(", ")
        val propsString = if (tblProperties.isEmpty) "" else s" TBLPROPERTIES ($props)"
        sql(s"CREATE TABLE delta_test ($schema) USING delta$propsString")
        "delta_test"
    }

    override protected def createTable(df: DataFrame, partitionedBy: Seq[String]): String = {
        df.write.partitionBy(partitionedBy: _*).format("delta").saveAsTable("delta_test")
        "delta_test"
    }

    override protected def dropTable(identifier: String): Unit = {
        sql(s"DROP TABLE IF EXISTS $identifier")
    }

    override protected def getDeltaLog(identifier: String): DeltaLog = {
        DeltaLog.forTable(spark, TableIdentifier(identifier))
    }

    import testImplicits._

    private def getDeltaLog(table: CatalogTable): DeltaLog = {
        getDeltaLog(new Path(table.storage.locationUri.get))
    }

    protected def getDeltaLog(path: Path): DeltaLog = {
        DeltaLog.forTable(spark, path)
    }

    test("Create hive delta table") {
        withTempDir { tempDir =>
            withTable("delta_test") {
                val catalog = spark.sessionState.catalog
                sql("CREATE TABLE delta_test(a LONG, b String) USING delta " +
                  s"OPTIONS (path='${tempDir.getCanonicalPath}')")
                val table = catalog.getTableMetadata(TableIdentifier("delta_test"))
                assert(table.tableType == CatalogTableType.EXTERNAL)
                assert(table.provider.contains("hive"))
                assert(table.properties("storage_handler") === "io.delta.hive.DeltaStorageHandler")

                // Query the data and the metadata directly via the DeltaLog
                val deltaLog = getDeltaLog(table)

                assert(deltaLog.snapshot.schema == new StructType()
                  .add("a", "long")
                  .add("b", "string")
                )
                assert(deltaLog.snapshot.metadata.partitionColumns.isEmpty)

                val externalTable = catalog.externalCatalog.getTable("default", "delta_test")
                assert(externalTable.schema == deltaLog.snapshot.schema)
                assert(externalTable.partitionColumnNames.isEmpty)

                sql("INSERT INTO delta_test SELECT 1, 'a'")
                checkAnswer(
                    sql("SELECT * FROM delta_test"),
                    Seq(Row(1L, "a")))
            }
        }
    }

    test("Create hive delta table with partionning") {
        withTempDir { tempDir =>
            withTable("delta_test") {
                sql("CREATE TABLE delta_test(a LONG, b String) USING delta PARTITIONED BY (a)" +
                  s"OPTIONS (path='${tempDir.getCanonicalPath}')")
                val catalog = spark.sessionState.catalog
                val table = catalog.getTableMetadata(TableIdentifier("delta_test"))
                assert(table.tableType == CatalogTableType.EXTERNAL)
                assert(table.provider.contains("hive"))
                assert(table.properties("storage_handler") === "io.delta.hive.DeltaStorageHandler")

                // Query the data and the metadata directly via the DeltaLog
                val deltaLog = getDeltaLog(table)

                assert(deltaLog.snapshot.schema == new StructType()
                  .add("a", "long")
                  .add("b", "string")
                )
                assert(deltaLog.snapshot.metadata.partitionSchema ==
                  new StructType().add("a", "long"))

                val externalTable = catalog.externalCatalog.getTable("default", "delta_test")
                assert(externalTable.schema == deltaLog.snapshot.schema)
                assert(externalTable.partitionColumnNames.isEmpty)

                sql("INSERT INTO delta_test SELECT 1, 'a'")
                checkAnswer(
                    sql("SELECT * FROM delta_test"),
                    Seq(Row(1L, "a")))
            }
        }
    }

    test("Create hive delta table with using location") {
        withTempDir { dir =>
            withTable("delta_test") {
                val path = dir.getCanonicalPath()

                val df = Seq(
                    (1, "IT", "Alice"),
                    (2, "CS", "Bob"),
                    (3, "IT", "Carol")).toDF("id", "dept", "name")
                df.write.format("delta").partitionBy("name", "dept").save(path)

                sql(s"CREATE TABLE delta_test USING delta LOCATION '$path'")
                val catalog = spark.sessionState.catalog
                val table = catalog.getTableMetadata(TableIdentifier("delta_test"))
                assert(table.tableType == CatalogTableType.EXTERNAL)
                assert(table.provider.contains("hive"))
                assert(table.properties("storage_handler") === "io.delta.hive.DeltaStorageHandler")

                val deltaLog = getDeltaLog(table)

                assert(deltaLog.snapshot.schema == new StructType()
                  .add("id", "int")
                  .add("dept", "string")
                  .add("name", "string")
                )
                assert(deltaLog.snapshot.metadata.partitionSchema == new StructType()
                  .add("name", "string")
                  .add("dept", "string")
                )

                val externalTable = catalog.externalCatalog.getTable("default", "delta_test")
                assert(externalTable.schema == deltaLog.snapshot.schema)
                assert(externalTable.partitionColumnNames.isEmpty)
            }
        }
    }

    test("Create hive delta table with usins save as table") {
        withTempDir { dir =>
            withTable("delta_test") {
                val df = Seq(
                    (1, "IT", "Alice"),
                    (2, "CS", "Bob"),
                    (3, "IT", "Carol")).toDF("id", "dept", "name")
                df.write.format("delta").partitionBy("name", "dept").saveAsTable("delta_test")
                val catalog = spark.sessionState.catalog
                val table = catalog.getTableMetadata(TableIdentifier("delta_test"))
                assert(table.tableType == CatalogTableType.MANAGED)
                assert(table.provider.contains("hive"))
                assert(table.properties("storage_handler") === "io.delta.hive.DeltaStorageHandler")

                val deltaLog = getDeltaLog(table)

                assert(deltaLog.snapshot.schema == new StructType()
                  .add("id", "int")
                  .add("dept", "string")
                  .add("name", "string")
                )
                assert(deltaLog.snapshot.metadata.partitionSchema == new StructType()
                  .add("name", "string")
                  .add("dept", "string")
                )

                val externalTable = catalog.externalCatalog.getTable("default", "delta_test")
                assert(externalTable.schema == deltaLog.snapshot.schema)
                assert(externalTable.partitionColumnNames.isEmpty)
            }
        }
    }

    ddlTest("Alter table add column") {
        withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>

            sql("ALTER TABLE delta_test add column (v3 long)")

            val deltaLog = getDeltaLog(tableName)
            assert(deltaLog.snapshot.schema == new StructType()
              .add("v1", "integer").add("v2", "string").add("v3", "long"))

            val catalog = spark.sessionState.catalog
            val externalAlterTable = catalog.externalCatalog.getTable("default", tableName)
            assert(externalAlterTable.schema == deltaLog.snapshot.schema)
        }
    }

    ddlTest("CHANGE COLUMN - add a comment") {
        withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>

            sql(s"ALTER TABLE $tableName CHANGE COLUMN v1 v1 integer COMMENT 'a comment'")

            val deltaLog = getDeltaLog(tableName)
            assert(deltaLog.snapshot.schema == new StructType()
              .add("v1", "integer", true, "a comment").add("v2", "string"))

            val catalog = spark.sessionState.catalog
            val externalAlterTable = catalog.externalCatalog.getTable("default", tableName)
            assert(externalAlterTable.schema == deltaLog.snapshot.schema)
        }
    }

    test("CHANGE COLUMN - move to first") {
        val df = Seq((1, "a", true), (2, "b", false)).toDF("v1", "v2", "v3")
        withDeltaTable(df, Seq("v2", "v3")) { tableName =>

            sql(s"ALTER TABLE $tableName CHANGE COLUMN v3 v3 boolean FIRST")

            val deltaLog = getDeltaLog(tableName)
            assert(deltaLog.snapshot.schema == new StructType()
              .add("v3", "boolean").add("v1", "integer").add("v2", "string"))

            checkDatasetUnorderly(
                spark.table(tableName).as[(Boolean, Int, String)],
                (true, 1, "a"), (false, 2, "b"))

            val catalog = spark.sessionState.catalog
            val externalAlterTable = catalog.externalCatalog.getTable("default", tableName)
            assert(externalAlterTable.schema == deltaLog.snapshot.schema)
        }
    }

    ddlTest("Merge schema by dataframe") {
        withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>
            val catalog = spark.sessionState.catalog
            val data = List(
                Row(1, "aa", Seq(11.0, 12.0), Map("k1" -> 11, "k12" -> 12), Row(13, "v11"))
            )
            val schema: StructType = StructType(
                  StructField(name = "v1", IntegerType, nullable = true) ::
                  StructField(name = "v2", StringType, nullable = true) ::
                  StructField(name = "array", ArrayType(DoubleType, containsNull = true),
                      nullable = true) ::
                  StructField(name = "map", MapType(StringType, IntegerType,
                      valueContainsNull = true), nullable = true) ::
                  StructField(name = "struct", StructType(
                        StructField(name = "int", IntegerType, nullable = true) ::
                        StructField(name = "string", StringType, nullable = true) :: Nil
                  ), nullable = true
                  ) :: Nil
            )

            spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
              .write.format("delta").option("mergeSchema", "true").mode("append")
              .saveAsTable(tableName)

            assert(getDeltaLog(tableName).snapshot.schema == schema)

            assert(catalog.externalCatalog.getTable("default", tableName).schema == schema)

            val schema2: StructType = StructType(
                  StructField(name = "v1", IntegerType, nullable = true) ::
                  StructField(name = "v2", StringType, nullable = true) ::
                  StructField(name = "array", ArrayType(DoubleType, containsNull = true),
                      nullable = true) ::
                  StructField(name = "map", MapType(StringType, IntegerType,
                      valueContainsNull = true), nullable = true) ::
                  StructField(name = "struct", StructType(
                        StructField(name = "int", IntegerType, nullable = true) ::
                        StructField(name = "string", StringType, nullable = true) ::
                        StructField(name = "long", LongType, nullable = true) :: Nil
                  ), nullable = true
                  ) :: Nil
            )
            val data2 = List(
                Row(1, "aa", Seq(11.0, 12.0), Map("k1" -> 11, "k12" -> 12), Row(13, "v11", 2L))
            )

            spark.createDataFrame(spark.sparkContext.parallelize(data2), schema2)
              .write.format("delta").option("mergeSchema", "true").mode("append")
              .saveAsTable(tableName)
            assert(getDeltaLog(tableName).snapshot.schema == schema2)

            assert(catalog.externalCatalog.getTable("default", tableName).schema == schema2)
        }
    }

    ddlTest("Merge schema with merge by sql") {
        withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>
            sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
            Seq((3, "a", 1.0)).toDF("v1", "v2", "v3").createOrReplaceTempView("updateTable")
            sql(
                s"""MERGE INTO $tableName as s USING updateTable as u on s.v1 = u.v1
                   | WHEN NOT MATCHED THEN INSERT *
                   |""".stripMargin
            )


            val deltaLog = getDeltaLog(tableName)
            assert(deltaLog.snapshot.schema == new StructType()
              .add("v1", "integer")
              .add("v2", "string")
              .add("v3", "double")
            )

            val catalog = spark.sessionState.catalog
            val externalAlterTable = catalog.externalCatalog.getTable("default", tableName)
            assert(externalAlterTable.schema == deltaLog.snapshot.schema)
        }
    }

    ddlTest("Merge schema with merge by dataframe") {
        withDeltaTable(Seq((1, "a"), (2, "b")).toDF("v1", "v2")) { tableName =>
            sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")
            io.delta.tables.DeltaTable.forName(tableName).as("s").merge(
                Seq((3, "a", 1.0)).toDF("v1", "v2", "v3").as("u"),
                "s.v1 = u.v1"
            ).whenNotMatched().insertAll().execute()


            val deltaLog = getDeltaLog(tableName)
            assert(deltaLog.snapshot.schema == new StructType()
              .add("v1", "integer")
              .add("v2", "string")
              .add("v3", "double")
            )

            val catalog = spark.sessionState.catalog
            val externalAlterTable = catalog.externalCatalog.getTable("default", tableName)
            assert(externalAlterTable.schema == deltaLog.snapshot.schema)
        }
    }

    ddlTest("SET/UNSET TBLPROPERTIES - simple") {
        withDeltaTable("v1 int, v2 string") { tableName =>

            sql(s"""
                   |ALTER TABLE $tableName
                   |SET TBLPROPERTIES (
                   |  'delta.logRetentionDuration' = '2 weeks',
                   |  'delta.checkpointInterval' = '20',
                   |  'key' = 'value'
                   |)""".stripMargin)

            val deltaLog = getDeltaLog(tableName)
            val snapshot1 = deltaLog.update()
            assert(snapshot1.metadata.configuration == Map(
                "storage_handler" -> "io.delta.hive.DeltaStorageHandler",
                "delta.logRetentionDuration" -> "2 weeks",
                "delta.checkpointInterval" -> "20",
                "key" -> "value"))
            assert(deltaLog.deltaRetentionMillis == 2 * 7 * 24 * 60 * 60 * 1000)
            assert(deltaLog.checkpointInterval == 20)

            val catalog = spark.sessionState.catalog
            val table = catalog.getTableMetadata(TableIdentifier("delta_test"))
            assert(table.tableType == CatalogTableType.MANAGED)
            assert(table.provider.contains("hive"))
            assert(table.properties === snapshot1.metadata.configuration)

            sql(s"ALTER TABLE $tableName UNSET TBLPROPERTIES ('delta.checkpointInterval', 'key')")

            val snapshot2 = deltaLog.update()
            assert(snapshot2.metadata.configuration == Map(
                "storage_handler" -> "io.delta.hive.DeltaStorageHandler",
                "delta.logRetentionDuration" -> "2 weeks"))
            assert(deltaLog.deltaRetentionMillis == 2 * 7 * 24 * 60 * 60 * 1000)
            assert(deltaLog.checkpointInterval ==
              CHECKPOINT_INTERVAL.fromString(CHECKPOINT_INTERVAL.defaultValue))
            val tableUnset = catalog.getTableMetadata(TableIdentifier("delta_test"))
            assert(tableUnset.tableType == CatalogTableType.MANAGED)
            assert(tableUnset.provider.contains("hive"))
            assert(tableUnset.properties === snapshot2.metadata.configuration)
        }
    }

}
