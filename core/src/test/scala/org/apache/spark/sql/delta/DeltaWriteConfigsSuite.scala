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

import java.util.Locale

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

/**
 * This test suite tests all (or nearly-all) combinations of ways to write configs to a delta table.
 *
 * At a high level, it tests the following matrix of conditions:
 *
 * - DataFrameWriter or DataStreamWriter or DataFrameWriterV2 or DeltaTableBuilder or SQL API
 * X
 * - option is / is not prefixed with 'delta'
 * X
 * - using table name or table path
 * X
 * - CREATE or REPLACE or CREATE OR REPLACE (table already exists) OR CREATE OR REPLACE (table
 *   doesn't already exist)
 *
 * At the end of the test suite, it prints out summary tables all of the cases above.
 */
class DeltaWriteConfigsSuite extends QueryTest
  with SharedSparkSession  with DeltaSQLCommandTest {

  val config_no_prefix = "randomPrefixLength" // default 2, we set it to 3
  val config_no_prefix_2 = "logRetentionDuration" // default interval 30 days, we set it to 60 days

  val config_prefix = "delta.randomizeFilePrefixes" // default is false, we set it to true
  val config_prefix_2 = "delta.checkpointInterval" // default is 10, we set it to 20

  override def afterAll(): Unit = {
    import testImplicits._
    // scalastyle:off println

    println("DataFrameWriter Test Output")
    dfw_output.toSeq
      .toDF("Output Location", "Output Mode", s"Contains No-Prefix Option",
        "Contains Prefix-Option")
      .show(100, false)

    println("DataStreamWriter Test Output")
    dsw_output.toSeq
      .toDF("Output Location", "Output Mode", s"Contains No-Prefix Option",
        "Contains Prefix-Option")
      .show(100, false)

    println("DataFrameWriterV2 Test Output")
    dfw_v2_output.toSeq
      .toDF("Output Location", "Output Mode", s"Contains No-Prefix Option",
        "Contains Prefix-Option")
      .show(100, false)

    println("DeltaTableBuilder Test Output")
    dtb_output.toSeq
      .toDF("Output Location", "Output Mode", s"Contains No-Prefix Option",
        "Contains Prefix-Option", "ERROR")
      .show(100, false)

    println("SQL Test Output")
    sql_output.toSeq
      .toDF("Output Location", "Config Input", s"SQL Operation", "AS SELECT",
        "Contains OPTION no-prefix", "Contains OPTION prefix", "Contains TBLPROPERTIES no-prefix",
        "Contains TBLPROPERTIES prefix")
      .show(100, false)

    // scalastyle:on println
    super.afterAll()
  }

  private val dfw_output = new ListBuffer[(String, String, Boolean, Boolean)]
  private val dsw_output = new ListBuffer[(String, String, Boolean, Boolean)]
  private val dfw_v2_output = new ListBuffer[(String, String, Boolean, Boolean)]
  private val dtb_output = new ListBuffer[(String, String, Boolean, Boolean, Boolean)]
  private val sql_output =
    new ListBuffer[(String, String, String, Boolean, Boolean, Boolean, Boolean, Boolean)]

  /*
  DataFrameWriter Test Output
  +---------------+-----------+-------------------------+----------------------+
  |Output Location|Output Mode|Contains No-Prefix Option|Contains Prefix-Option|
  +---------------+-----------+-------------------------+----------------------+
  |path           |create     |false                    |false                 |
  |path           |overwrite  |false                    |false                 |
  |path           |append     |false                    |false                 |
  |table          |create     |false                    |true                  |
  |table          |overwrite  |false                    |true                  |
  |table          |append     |false                    |true                  |
  +---------------+-----------+-------------------------+----------------------+
  */
  Seq("path", "table").foreach { outputLoc =>
    Seq("create", "overwrite", "append").foreach { outputMode =>
      val testName = s"DataFrameWriter - outputLoc=$outputLoc & mode=$outputMode"
      test(testName) {
        withTempDir { dir =>
          withTable("tbl") {
            var data = spark.range(10).write.format("delta")
              .option(config_no_prefix, "3")
              .option(config_prefix, "true")

            if (outputMode != "create") {
              data = data.mode(outputMode)
            }

            val log = outputLoc match {
              case "path" =>
                data.save(dir.getCanonicalPath)
                DeltaLog.forTable(spark, dir)
              case "table" =>
                data.saveAsTable("tbl")
                DeltaLog.forTable(spark, TableIdentifier("tbl"))
            }

            val config = log.snapshot.metadata.configuration
            val answer_no_prefix = config.contains(config_no_prefix)
            val answer_prefix = config.contains(config_prefix)

            assert(!answer_no_prefix)
            assert(answer_prefix == (outputLoc == "table"))

            dfw_output += ((outputLoc, outputMode, answer_no_prefix, answer_prefix))
          }
        }
      }
    }
  }

  /*
  DataStreamWriter Test Output
  +---------------+-----------+-------------------------+----------------------+
  |Output Location|Output Mode|Contains No-Prefix Option|Contains Prefix-Option|
  +---------------+-----------+-------------------------+----------------------+
  |path           |create     |false                    |false                 |
  |path           |append     |false                    |false                 |
  |path           |complete   |false                    |false                 |
  |table          |create     |false                    |false                 |
  |table          |append     |false                    |false                 |
  |table          |complete   |false                    |false                 |
  +---------------+-----------+-------------------------+----------------------+
  */
  // Data source DeltaDataSource does not support Update output mode
  Seq("path", "table").foreach { outputLoc =>
    Seq("create", "append", "complete").foreach { outputMode =>
      val testName = s"DataStreamWriter - outputLoc=$outputLoc & outputMode=$outputMode"
      test(testName) {
        withTempDir { dir =>
          withTempDir { checkpointDir =>
            withTable("src", "tbl") {
              spark.range(10).write.format("delta").saveAsTable("src")

              var data = spark.readStream.format("delta").table("src")

              // Needed to resolve error: Complete output mode not supported when there are no
              // streaming aggregations on streaming DataFrames/Datasets
              if (outputMode == "complete") {
                data = data.groupBy().count()
              }

              var stream = data.writeStream
                .format("delta")
                .option("checkpointLocation", checkpointDir.getCanonicalPath)
                .option(config_no_prefix, "3")
                .option(config_prefix, "true")

              if (outputMode != "create") {
                stream = stream.outputMode(outputMode)
              }

              val log = outputLoc match {
                case "path" =>
                  stream.start(dir.getCanonicalPath).stop()
                  DeltaLog.forTable(spark, dir)
                case "table" =>
                  stream.toTable("tbl").stop()
                  DeltaLog.forTable(spark, TableIdentifier("tbl"))
              }

              val config = log.snapshot.metadata.configuration
              val answer_no_prefix = config.contains(config_no_prefix)
              val answer_prefix = config.contains(config_prefix)

              assert(!answer_no_prefix)
              assert(!answer_prefix)

              dsw_output += ((outputLoc, outputMode, answer_no_prefix, answer_prefix))
            }
          }
        }
      }
    }
  }

  /*
  DataFrameWriterV2 Test Output
  +---------------+--------------+-------------------------+----------------------+
  |Output Location|Output Mode   |Contains No-Prefix Option|Contains Prefix-Option|
  +---------------+--------------+-------------------------+----------------------+
  |path           |create        |false                    |true                  |
  |path           |replace       |false                    |true                  |
  |path           |c_or_r_create |false                    |true                  |
  |path           |c_or_r_replace|false                    |true                  |
  |table          |create        |false                    |true                  |
  |table          |replace       |false                    |true                  |
  |table          |c_or_r_create |false                    |true                  |
  |table          |c_or_r_replace|false                    |true                  |
  +---------------+--------------+-------------------------+----------------------+
  */
  Seq("path", "table").foreach { outputLoc =>
    Seq("create", "replace", "c_or_r_create", "c_or_r_replace").foreach { outputMode =>
      val testName = s"DataFrameWriterV2 - outputLoc=$outputLoc & outputMode=$outputMode"
      test(testName) {
        withTempDir { dir =>
          withTable("tbl") {
            val table = outputLoc match {
              case "path" => s"delta.`${dir.getCanonicalPath}`"
              case "table" => "tbl"
            }

            val data = spark.range(10).writeTo(table).using("delta")
              .option(config_no_prefix, "3")
              .option(config_prefix, "true")

            if (outputMode.contains("replace")) {
              spark.range(100).writeTo(table).using("delta").create()
            }

            outputMode match {
              case "create" => data.create()
              case "replace" => data.replace()
              case "c_or_r_create" | "c_or_r_replace" => data.createOrReplace()
            }

            val log = outputLoc match {
              case "path" => DeltaLog.forTable(spark, dir)
              case "table" => DeltaLog.forTable(spark, TableIdentifier("tbl"))
            }

            val config = log.snapshot.metadata.configuration
            val answer_no_prefix = config.contains(config_no_prefix)
            val answer_prefix = config.contains(config_prefix)

            assert(!answer_no_prefix)
            assert(answer_prefix)

            dfw_v2_output += ((outputLoc, outputMode, answer_no_prefix, answer_prefix))
          }
        }
      }
    }
  }

  /*
  DeltaTableBuilder Test Output
  +---------------+--------------+-------------------------+----------------------+-----+
  |Output Location|Output Mode   |Contains No-Prefix Option|Contains Prefix-Option|ERROR|
  +---------------+--------------+-------------------------+----------------------+-----+
  |path           |create        |false                    |true                  |false|
  |path           |replace       |false                    |true                  |false|
  |path           |c_or_r_create |false                    |true                  |false|
  |path           |c_or_r_replace|false                    |false                 |true |
  |table          |create        |false                    |true                  |false|
  |table          |replace       |false                    |true                  |false|
  |table          |c_or_r_create |false                    |true                  |false|
  |table          |c_or_r_replace|false                    |true                  |false|
  +---------------+--------------+-------------------------+----------------------+-----+
  */
  Seq("path", "table").foreach { outputLoc =>
    Seq("create", "replace", "c_or_r_create", "c_or_r_replace").foreach { outputMode =>
      val testName = s"DeltaTableBuilder - outputLoc=$outputLoc & outputMode=$outputMode"
      test(testName) {
        withTempDir { dir =>
          withTable("tbl") {

            if (outputMode.contains("replace")) {
              outputLoc match {
                case "path" =>
                  io.delta.tables.DeltaTable.create()
                    .addColumn("bar", StringType).location(dir.getCanonicalPath).execute()
                case "table" =>
                  io.delta.tables.DeltaTable.create()
                    .addColumn("bar", StringType).tableName("tbl").execute()
              }
            }

            var tblBuilder = outputMode match {
              case "create" =>
                io.delta.tables.DeltaTable.create()
              case "replace" =>
                io.delta.tables.DeltaTable.replace()
              case "c_or_r_create" | "c_or_r_replace" =>
                io.delta.tables.DeltaTable.createOrReplace()
            }

            tblBuilder.addColumn("foo", StringType)
            tblBuilder = tblBuilder.property(config_no_prefix, "3")
            tblBuilder = tblBuilder.property(config_prefix, "true")

            val log = (outputLoc, outputMode) match {
              case ("path", "c_or_r_replace") =>
                intercept[DeltaAnalysisException] {
                  tblBuilder.location(dir.getCanonicalPath).execute()
                }
                null
              case ("path", _) =>
                tblBuilder.location(dir.getCanonicalPath).execute()
                DeltaLog.forTable(spark, dir)
              case ("table", _) =>
                tblBuilder.tableName("tbl").execute()
                DeltaLog.forTable(spark, TableIdentifier("tbl"))
            }

            log match {
              case null =>
                // CREATE OR REPLACE seems broken when using path and the table already exists
                // with a different schema.
                // DeltaAnalysisException: The specified schema does not match the existing schema
                // ...
                // Specified schema is missing field(s): bar
                // Specified schema has additional field(s): foo
                assert(outputLoc == "path" && outputMode == "c_or_r_replace")
                dtb_output += ((outputLoc, outputMode, false, false, true))
              case _ =>
                val config = log.snapshot.metadata.configuration
                val answer_no_prefix = config.contains(config_no_prefix)
                val answer_prefix = config.contains(config_prefix)

                assert(!answer_no_prefix)
                assert(answer_prefix)

                dtb_output += ((outputLoc, outputMode, answer_no_prefix, answer_prefix, false))
            }
          }
        }
      }
    }
  }

  // scalastyle:off line.size.limit
  /*
  SQL Test Output
  +---------------+-------------------------+--------------+---------+-------------------------+----------------------+--------------------------------+-----------------------------+
  |Output Location|Config Input             |SQL Operation |AS SELECT|Contains OPTION no-prefix|Contains OPTION prefix|Contains TBLPROPERTIES no-prefix|Contains TBLPROPERTIES prefix|
  +---------------+-------------------------+--------------+---------+-------------------------+----------------------+--------------------------------+-----------------------------+
  |path           |options                  |create        |true     |false                    |true                  |false                           |false                        |
  |path           |options                  |create        |false    |true                     |true                  |false                           |false                        |
  |path           |options                  |replace       |true     |false                    |true                  |false                           |false                        |
  |path           |options                  |replace       |false    |false                    |true                  |false                           |false                        |
  |path           |options                  |c_or_r_create |true     |false                    |true                  |false                           |false                        |
  |path           |options                  |c_or_r_create |false    |false                    |true                  |false                           |false                        |
  |path           |options                  |c_or_r_replace|true     |false                    |true                  |false                           |false                        |
  |path           |options                  |c_or_r_replace|false    |false                    |true                  |false                           |false                        |
  |path           |tblproperties            |create        |true     |false                    |false                 |true                            |true                         |
  |path           |tblproperties            |create        |false    |false                    |false                 |true                            |true                         |
  |path           |tblproperties            |replace       |true     |false                    |false                 |true                            |true                         |
  |path           |tblproperties            |replace       |false    |false                    |false                 |true                            |true                         |
  |path           |tblproperties            |c_or_r_create |true     |false                    |false                 |true                            |true                         |
  |path           |tblproperties            |c_or_r_create |false    |false                    |false                 |true                            |true                         |
  |path           |tblproperties            |c_or_r_replace|true     |false                    |false                 |true                            |true                         |
  |path           |tblproperties            |c_or_r_replace|false    |false                    |false                 |true                            |true                         |
  |path           |options_and_tblproperties|create        |true     |false                    |true                  |true                            |true                         |
  |path           |options_and_tblproperties|create        |false    |true                     |true                  |true                            |true                         |
  |path           |options_and_tblproperties|replace       |true     |false                    |true                  |true                            |true                         |
  |path           |options_and_tblproperties|replace       |false    |false                    |true                  |true                            |true                         |
  |path           |options_and_tblproperties|c_or_r_create |true     |false                    |true                  |true                            |true                         |
  |path           |options_and_tblproperties|c_or_r_create |false    |false                    |true                  |true                            |true                         |
  |path           |options_and_tblproperties|c_or_r_replace|true     |false                    |true                  |true                            |true                         |
  |path           |options_and_tblproperties|c_or_r_replace|false    |false                    |true                  |true                            |true                         |
  |table          |options                  |create        |true     |false                    |true                  |false                           |false                        |
  |table          |options                  |create        |false    |true                     |true                  |false                           |false                        |
  |table          |options                  |replace       |true     |false                    |true                  |false                           |false                        |
  |table          |options                  |replace       |false    |false                    |true                  |false                           |false                        |
  |table          |options                  |c_or_r_create |true     |false                    |true                  |false                           |false                        |
  |table          |options                  |c_or_r_create |false    |false                    |true                  |false                           |false                        |
  |table          |options                  |c_or_r_replace|true     |false                    |true                  |false                           |false                        |
  |table          |options                  |c_or_r_replace|false    |false                    |true                  |false                           |false                        |
  |table          |tblproperties            |create        |true     |false                    |false                 |true                            |true                         |
  |table          |tblproperties            |create        |false    |false                    |false                 |true                            |true                         |
  |table          |tblproperties            |replace       |true     |false                    |false                 |true                            |true                         |
  |table          |tblproperties            |replace       |false    |false                    |false                 |true                            |true                         |
  |table          |tblproperties            |c_or_r_create |true     |false                    |false                 |true                            |true                         |
  |table          |tblproperties            |c_or_r_create |false    |false                    |false                 |true                            |true                         |
  |table          |tblproperties            |c_or_r_replace|true     |false                    |false                 |true                            |true                         |
  |table          |tblproperties            |c_or_r_replace|false    |false                    |false                 |true                            |true                         |
  |table          |options_and_tblproperties|create        |true     |false                    |true                  |true                            |true                         |
  |table          |options_and_tblproperties|create        |false    |true                     |true                  |true                            |true                         |
  |table          |options_and_tblproperties|replace       |true     |false                    |true                  |true                            |true                         |
  |table          |options_and_tblproperties|replace       |false    |false                    |true                  |true                            |true                         |
  |table          |options_and_tblproperties|c_or_r_create |true     |false                    |true                  |true                            |true                         |
  |table          |options_and_tblproperties|c_or_r_create |false    |false                    |true                  |true                            |true                         |
  |table          |options_and_tblproperties|c_or_r_replace|true     |false                    |true                  |true                            |true                         |
  |table          |options_and_tblproperties|c_or_r_replace|false    |false                    |true                  |true                            |true                         |
  +---------------+-------------------------+--------------+---------+-------------------------+----------------------+--------------------------------+-----------------------------+
  */
  // scalastyle:on line.size.limit
  Seq("path", "table").foreach { outputLoc =>
    Seq("options", "tblproperties", "options_and_tblproperties").foreach { configInput =>
      Seq("create", "replace", "c_or_r_create", "c_or_r_replace").foreach { sqlOp =>
        Seq(true, false).foreach { useAsSelectStmt =>
          val testName = s"SQL - outputLoc=$outputLoc & configInput=$configInput & sqlOp=$sqlOp" +
            s" & useAsSelectStmt=$useAsSelectStmt"

          test(testName) {
            withTempDir { dir =>
              withTable("tbl", "other") {
                if (sqlOp.contains("replace")) {
                  var stmt = "CREATE TABLE tbl (ID INT) USING DELTA"
                  if (outputLoc == "path") {
                    stmt = stmt + s" LOCATION '${dir.getCanonicalPath}'"
                  }
                  sql(stmt)
                }

                val sqlOpStr = sqlOp match {
                  case "c_or_r_create" | "c_or_r_replace" => "CREATE OR REPLACE"
                  case _ => sqlOp.toUpperCase(Locale.ROOT)
                }

                val schemaStr = if (useAsSelectStmt) "" else "(id INT) "
                var stmt = sqlOpStr + " TABLE tbl " + schemaStr + "USING DELTA\n"

                if (configInput.contains("options")) {
                  stmt = stmt + s"OPTIONS('$config_no_prefix'=3,'$config_prefix'=true)\n"
                }
                if (outputLoc == "path") {
                  stmt = stmt + s"LOCATION '${dir.getCanonicalPath}'\n"
                }
                if (configInput.contains("tblproperties")) {
                  stmt = stmt + s"TBLPROPERTIES('$config_no_prefix_2'='interval 60 days'," +
                    s"'$config_prefix_2'=20)\n"
                }
                if (useAsSelectStmt) {
                  sql("CREATE TABLE other (id INT) USING DELTA")
                  stmt = stmt + "AS SELECT * FROM other\n"
                }

                // scalastyle:off println
                println(stmt)
                // scalastyle:on println
                sql(stmt)
                sql("SHOW TBLPROPERTIES tbl").show(truncate = false)

                val config = sql("SHOW TBLPROPERTIES tbl").collect().map { row =>
                  val key = row.getString(0)
                  val value = row.getString(1)
                  (key, value)
                }.toMap

                val option_no_prefix = config.contains(config_no_prefix)
                val option_prefix = config.contains(config_prefix)
                val tblproperties_no_prefix = config.contains(config_no_prefix_2)
                val tblproperties_prefix = config.contains(config_prefix_2)

                sql_output += ((outputLoc, configInput, sqlOp, useAsSelectStmt, option_no_prefix,
                  option_prefix, tblproperties_no_prefix, tblproperties_prefix))
              }
            }
          }
        }
      }
    }
  }

}
