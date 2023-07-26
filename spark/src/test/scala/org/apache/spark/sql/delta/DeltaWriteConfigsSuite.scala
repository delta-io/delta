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
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

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
 * doesn't already exist)
 *
 * At the end of the test suite, it prints out summary tables all of the cases above.
 */
class DeltaWriteConfigsSuite extends QueryTest
  with SharedSparkSession  with DeltaSQLCommandTest {

  val config_no_prefix = "dataSkippingNumIndexedCols"
  val config_no_prefix_value = "33"

  val config_prefix = "delta.deletedFileRetentionDuration"
  val config_prefix_value = "interval 2 weeks"

  val config_no_prefix_2 = "logRetentionDuration"
  val config_no_prefix_2_value = "interval 60 days"

  val config_prefix_2 = "delta.checkpointInterval"
  val config_prefix_2_value = "20"

  override def afterAll(): Unit = {
    import testImplicits._
    // scalastyle:off println

    println("DataFrameWriter Test Output")
    dfw_output.toSeq
      .toDF("Output Location", "Output Mode", s"Contains No-Prefix Option",
        "Contains Prefix-Option", "Config")
      .show(100, false)

    println("DataStreamWriter Test Output")
    dsw_output.toSeq
      .toDF("Output Location", "Output Mode", s"Contains No-Prefix Option",
        "Contains Prefix-Option", "Config")
      .show(100, false)

    println("DataFrameWriterV2 Test Output")
    dfw_v2_output.toSeq
      .toDF("Output Location", "Output Mode", s"Contains No-Prefix Option",
        "Contains Prefix-Option", "Config")
      .show(100, false)

    println("DeltaTableBuilder Test Output")
    dtb_output.toSeq
      .toDF("Output Location", "Output Mode", s"Contains No-Prefix Option (lowercase)",
        s"Contains No-Prefix Option", "Contains Prefix-Option", "ERROR", "Config")
      .show(100, false)

    println("SQL Test Output")
    sql_output.toSeq
      .toDF("Output Location", "Config Input", s"SQL Operation", "AS SELECT",
        "Contains OPTION no-prefix", "Contains OPTION prefix", "Contains TBLPROPERTIES no-prefix",
        "Contains TBLPROPERTIES prefix", "Config")
      .show(100, false)

    // scalastyle:on println
    super.afterAll()
  }


  private val dfw_output = new ListBuffer[DeltaFrameStreamAPITestOutput]
  private val dsw_output = new ListBuffer[DeltaFrameStreamAPITestOutput]
  private val dfw_v2_output = new ListBuffer[DeltaFrameStreamAPITestOutput]
  private val dtb_output = new ListBuffer[DeltaTableBuilderAPITestOutput]
  private val sql_output = new ListBuffer[SQLAPIOutput]

  // scalastyle:off line.size.limit
  /*
  DataFrameWriter Test Output
  +---------------+-----------+-------------------------+----------------------+------------------------------------------------------+
  |Output Location|Output Mode|Contains No-Prefix Option|Contains Prefix-Option|Config                                                |
  +---------------+-----------+-------------------------+----------------------+------------------------------------------------------+
  |path           |create     |false                    |true                  |delta.deletedFileRetentionDuration -> interval 2 weeks|
  |path           |overwrite  |false                    |true                  |delta.deletedFileRetentionDuration -> interval 2 weeks|
  |path           |append     |false                    |true                  |delta.deletedFileRetentionDuration -> interval 2 weeks|
  |table          |create     |false                    |true                  |delta.deletedFileRetentionDuration -> interval 2 weeks|
  |table          |overwrite  |false                    |true                  |delta.deletedFileRetentionDuration -> interval 2 weeks|
  |table          |append     |false                    |true                  |delta.deletedFileRetentionDuration -> interval 2 weeks|
  +---------------+-----------+-------------------------+----------------------+------------------------------------------------------+
  */
  // scalastyle:on line.size.limit
  Seq("path", "table").foreach { outputLoc =>
    Seq("create", "overwrite", "append").foreach { outputMode =>
      val testName = s"DataFrameWriter - outputLoc=$outputLoc & mode=$outputMode"
      test(testName) {
        withTempDir { dir =>
          withTable("tbl") {
            var data = spark.range(10).write.format("delta")
              .option(config_no_prefix, config_no_prefix_value)
              .option(config_prefix, config_prefix_value)

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
            assert(answer_prefix)
            assert(config.size == 1)

            dfw_output += DeltaFrameStreamAPITestOutput(
              outputLocation = outputLoc,
              outputMode = outputMode,
              containsNoPrefixOption = answer_no_prefix,
              containsPrefixOption = answer_prefix,
              config = config.mkString(",")
            )

          }
        }
      }
    }
  }

  // scalastyle:off line.size.limit
  /*
  DataStreamWriter Test Output
  +---------------+-----------+-------------------------+----------------------+------+
  |Output Location|Output Mode|Contains No-Prefix Option|Contains Prefix-Option|Config|
  +---------------+-----------+-------------------------+----------------------+------+
  |path           |create     |false                    |false                 |      |
  |path           |append     |false                    |false                 |      |
  |path           |complete   |false                    |false                 |      |
  |table          |create     |false                    |false                 |      |
  |table          |append     |false                    |false                 |      |
  |table          |complete   |false                    |false                 |      |
  +---------------+-----------+-------------------------+----------------------+------+
  */
  // scalastyle:on line.size.limit
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
                .option(config_no_prefix, config_no_prefix_value)
                .option(config_prefix, config_prefix_value)

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

              assert(config.isEmpty)
              assert(!answer_no_prefix)
              assert(!answer_prefix)

              dsw_output += DeltaFrameStreamAPITestOutput(
                outputLocation = outputLoc,
                outputMode = outputMode,
                containsNoPrefixOption = answer_no_prefix,
                containsPrefixOption = answer_prefix,
                config = config.mkString(",")
              )

            }
          }
        }
      }
    }
  }

  // scalastyle:off line.size.limit
  /*
  DataFrameWriterV2 Test Output
  +---------------+--------------+-------------------------+----------------------+------------------------------------------------------+
  |Output Location|Output Mode   |Contains No-Prefix Option|Contains Prefix-Option|Config                                                |
  +---------------+--------------+-------------------------+----------------------+------------------------------------------------------+
  |path           |create        |false                    |true                  |delta.deletedFileRetentionDuration -> interval 2 weeks|
  |path           |replace       |false                    |true                  |delta.deletedFileRetentionDuration -> interval 2 weeks|
  |path           |c_or_r_create |false                    |true                  |delta.deletedFileRetentionDuration -> interval 2 weeks|
  |path           |c_or_r_replace|false                    |true                  |delta.deletedFileRetentionDuration -> interval 2 weeks|
  |table          |create        |false                    |true                  |delta.deletedFileRetentionDuration -> interval 2 weeks|
  |table          |replace       |false                    |true                  |delta.deletedFileRetentionDuration -> interval 2 weeks|
  |table          |c_or_r_create |false                    |true                  |delta.deletedFileRetentionDuration -> interval 2 weeks|
  |table          |c_or_r_replace|false                    |true                  |delta.deletedFileRetentionDuration -> interval 2 weeks|
  +---------------+--------------+-------------------------+----------------------+------------------------------------------------------+
  */
  // scalastyle:on line.size.limit
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
              .option(config_no_prefix, config_no_prefix_value)
              .option(config_prefix, config_prefix_value)

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
            assert(config.size == 1)

            dfw_v2_output += DeltaFrameStreamAPITestOutput(
              outputLocation = outputLoc,
              outputMode = outputMode,
              containsNoPrefixOption = answer_no_prefix,
              containsPrefixOption = answer_prefix,
              config = config.mkString(",")
            )

          }
        }
      }
    }
  }

  // scalastyle:off line.size.limit
  /*
  DeltaTableBuilder Test Output
  +---------------+--------------+-------------------------------------+-------------------------+----------------------+-----+---------------------------------------------------------------------------------------+
  |Output Location|Output Mode   |Contains No-Prefix Option (lowercase)|Contains No-Prefix Option|Contains Prefix-Option|ERROR|Config                                                                                 |
  +---------------+--------------+-------------------------------------+-------------------------+----------------------+-----+---------------------------------------------------------------------------------------+
  |path           |create        |true                                 |false                    |true                  |false|delta.deletedFileRetentionDuration -> interval 2 weeks,dataSkippingNumIndexedCols -> 33|
  |path           |replace       |true                                 |false                    |true                  |false|delta.deletedFileRetentionDuration -> interval 2 weeks,dataSkippingNumIndexedCols -> 33|
  |path           |c_or_r_create |true                                 |false                    |true                  |false|delta.deletedFileRetentionDuration -> interval 2 weeks,dataSkippingNumIndexedCols -> 33|
  |path           |c_or_r_replace|false                                |false                    |false                 |true |                                                                                       |
  |table          |create        |true                                 |false                    |true                  |false|dataSkippingNumIndexedCols -> 33,delta.deletedFileRetentionDuration -> interval 2 weeks|
  |table          |replace       |true                                 |false                    |true                  |false|delta.deletedFileRetentionDuration -> interval 2 weeks,dataSkippingNumIndexedCols -> 33|
  |table          |c_or_r_create |true                                 |false                    |true                  |false|delta.deletedFileRetentionDuration -> interval 2 weeks,dataSkippingNumIndexedCols -> 33|
  |table          |c_or_r_replace|true                                 |false                    |true                  |false|delta.deletedFileRetentionDuration -> interval 2 weeks,dataSkippingNumIndexedCols -> 33|
  +---------------+--------------+-------------------------------------+-------------------------+----------------------+-----+---------------------------------------------------------------------------------------+
  */
  // scalastyle:on line.size.limit
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
            tblBuilder = tblBuilder.property(config_no_prefix, config_no_prefix_value)
            tblBuilder = tblBuilder.property(config_prefix, config_prefix_value)

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
                dtb_output += DeltaTableBuilderAPITestOutput(
                  outputLocation = outputLoc,
                  outputMode = outputMode,
                  containsNoPrefixOptionLowerCase = false,
                  containsNoPrefixOption = false,
                  containsPrefixOption = false,
                  error = true,
                  config = ""
                )
              case _ =>
                val config = log.snapshot.metadata.configuration

                val answer_no_prefix_lowercase =
                  config.contains(config_no_prefix.toLowerCase(Locale.ROOT))
                val answer_no_prefix = config.contains(config_no_prefix)
                val answer_prefix = config.contains(config_prefix)

                assert(!answer_no_prefix_lowercase)
                assert(answer_no_prefix)
                assert(answer_prefix)
                assert(config.size == 2)

                dtb_output += DeltaTableBuilderAPITestOutput(
                  outputLocation = outputLoc,
                  outputMode = outputMode,
                  containsNoPrefixOptionLowerCase = answer_no_prefix_lowercase,
                  containsNoPrefixOption = answer_no_prefix,
                  containsPrefixOption = answer_prefix,
                  error = false,
                  config = config.mkString(",")
                )
            }
          }
        }
      }
    }
  }

  // scalastyle:off line.size.limit
  /*
  SQL Test Output
  +---------------+-------------------------+--------------+---------+-------------------------+----------------------+--------------------------------+-----------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  |Output Location|Config Input             |SQL Operation |AS SELECT|Contains OPTION no-prefix|Contains OPTION prefix|Contains TBLPROPERTIES no-prefix|Contains TBLPROPERTIES prefix|Config                                                                                                                                                                                                                                                               |
  +---------------+-------------------------+--------------+---------+-------------------------+----------------------+--------------------------------+-----------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  |path           |options                  |create        |true     |false                    |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks                                                                                                                                                                                                               |
  |path           |options                  |create        |false    |true                     |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks,dataSkippingNumIndexedCols -> 33,option.delta.deletedFileRetentionDuration -> interval 2 weeks,option.dataSkippingNumIndexedCols -> 33                                                                        |
  |path           |options                  |replace       |true     |false                    |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks                                                                                                                                                                                                               |
  |path           |options                  |replace       |false    |false                    |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks                                                                                                                                                                                                               |
  |path           |options                  |c_or_r_create |true     |false                    |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks                                                                                                                                                                                                               |
  |path           |options                  |c_or_r_create |false    |false                    |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks                                                                                                                                                                                                               |
  |path           |options                  |c_or_r_replace|true     |false                    |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks                                                                                                                                                                                                               |
  |path           |options                  |c_or_r_replace|false    |false                    |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks                                                                                                                                                                                                               |
  |path           |tblproperties            |create        |true     |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |path           |tblproperties            |create        |false    |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |path           |tblproperties            |replace       |true     |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |path           |tblproperties            |replace       |false    |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |path           |tblproperties            |c_or_r_create |true     |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |path           |tblproperties            |c_or_r_create |false    |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |path           |tblproperties            |c_or_r_replace|true     |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |path           |tblproperties            |c_or_r_replace|false    |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |path           |options_and_tblproperties|create        |true     |false                    |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                       |
  |path           |options_and_tblproperties|create        |false    |true                     |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,dataSkippingNumIndexedCols -> 33,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20,option.delta.deletedFileRetentionDuration -> interval 2 weeks,option.dataSkippingNumIndexedCols -> 33|
  |path           |options_and_tblproperties|replace       |true     |false                    |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                       |
  |path           |options_and_tblproperties|replace       |false    |false                    |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                       |
  |path           |options_and_tblproperties|c_or_r_create |true     |false                    |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                       |
  |path           |options_and_tblproperties|c_or_r_create |false    |false                    |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                       |
  |path           |options_and_tblproperties|c_or_r_replace|true     |false                    |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                       |
  |path           |options_and_tblproperties|c_or_r_replace|false    |false                    |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                       |
  |table          |options                  |create        |true     |false                    |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks                                                                                                                                                                                                               |
  |table          |options                  |create        |false    |true                     |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks,dataSkippingNumIndexedCols -> 33,option.delta.deletedFileRetentionDuration -> interval 2 weeks,option.dataSkippingNumIndexedCols -> 33                                                                        |
  |table          |options                  |replace       |true     |false                    |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks                                                                                                                                                                                                               |
  |table          |options                  |replace       |false    |false                    |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks                                                                                                                                                                                                               |
  |table          |options                  |c_or_r_create |true     |false                    |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks                                                                                                                                                                                                               |
  |table          |options                  |c_or_r_create |false    |false                    |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks                                                                                                                                                                                                               |
  |table          |options                  |c_or_r_replace|true     |false                    |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks                                                                                                                                                                                                               |
  |table          |options                  |c_or_r_replace|false    |false                    |true                  |N/A                             |N/A                          |delta.deletedFileRetentionDuration -> interval 2 weeks                                                                                                                                                                                                               |
  |table          |tblproperties            |create        |true     |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |table          |tblproperties            |create        |false    |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |table          |tblproperties            |replace       |true     |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |table          |tblproperties            |replace       |false    |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |table          |tblproperties            |c_or_r_create |true     |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |table          |tblproperties            |c_or_r_create |false    |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |table          |tblproperties            |c_or_r_replace|true     |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |table          |tblproperties            |c_or_r_replace|false    |N/A                      |N/A                   |true                            |true                         |logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                                                                              |
  |table          |options_and_tblproperties|create        |true     |false                    |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                       |
  |table          |options_and_tblproperties|create        |false    |true                     |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,dataSkippingNumIndexedCols -> 33,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20,option.delta.deletedFileRetentionDuration -> interval 2 weeks,option.dataSkippingNumIndexedCols -> 33|
  |table          |options_and_tblproperties|replace       |true     |false                    |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                       |
  |table          |options_and_tblproperties|replace       |false    |false                    |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                       |
  |table          |options_and_tblproperties|c_or_r_create |true     |false                    |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                       |
  |table          |options_and_tblproperties|c_or_r_create |false    |false                    |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                       |
  |table          |options_and_tblproperties|c_or_r_replace|true     |false                    |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                       |
  |table          |options_and_tblproperties|c_or_r_replace|false    |false                    |true                  |true                            |true                         |delta.deletedFileRetentionDuration -> interval 2 weeks,logRetentionDuration -> interval 60 days,delta.checkpointInterval -> 20                                                                                                                                       |
  +---------------+-------------------------+--------------+---------+-------------------------+----------------------+--------------------------------+-----------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
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
                  stmt = stmt + s"OPTIONS(" +
                    s"'$config_no_prefix'=$config_no_prefix_value," +
                    s"'$config_prefix'='$config_prefix_value')\n"
                }
                if (outputLoc == "path") {
                  stmt = stmt + s"LOCATION '${dir.getCanonicalPath}'\n"
                }
                if (configInput.contains("tblproperties")) {
                  stmt = stmt + s"TBLPROPERTIES(" +
                    s"'$config_no_prefix_2'='$config_no_prefix_2_value'," +
                    s"'$config_prefix_2'=$config_prefix_2_value)\n"
                }
                if (useAsSelectStmt) {
                  sql("CREATE TABLE other (id INT) USING DELTA")
                  stmt = stmt + "AS SELECT * FROM other\n"
                }

                // scalastyle:off println
                println(stmt)
                // scalastyle:on println

                sql(stmt)

                val log = DeltaLog.forTable(spark, TableIdentifier("tbl"))
                val config = log.snapshot.metadata.configuration

                val option_was_set = configInput.contains("options")
                val tblproperties_was_set = configInput.contains("tblproperties")

                val option_no_prefix = config.contains(config_no_prefix)
                val option_prefix = config.contains(config_prefix)
                val tblproperties_no_prefix = config.contains(config_no_prefix_2)
                val tblproperties_prefix = config.contains(config_prefix_2)

                var expectedSize = 0
                if (option_was_set) {
                  assert(option_prefix)
                  expectedSize += 1
                  if (sqlOp == "create" && !useAsSelectStmt) {
                    assert(option_no_prefix)
                    assert(config.contains(s"option.$config_prefix"))
                    assert(config.contains(s"option.$config_no_prefix"))
                    expectedSize += 3
                  }
                }
                if (tblproperties_was_set) {
                  assert(tblproperties_prefix)
                  assert(tblproperties_no_prefix)
                  expectedSize += 2
                }

                assert(config.size == expectedSize)

                sql_output += SQLAPIOutput(
                  outputLoc,
                  configInput,
                  sqlOp,
                  useAsSelectStmt,
                  if (option_was_set) option_no_prefix.toString else "N/A",
                  if (option_was_set) option_prefix.toString else "N/A",
                  if (tblproperties_was_set) tblproperties_no_prefix.toString else "N/A",
                  if (tblproperties_was_set) tblproperties_prefix.toString else "N/A",
                  config.mkString(",")
                )
              }
            }
          }
        }
      }
    }
  }
}

// Need to be outside to be stable references for Spark to generate the case classes
case class DeltaFrameStreamAPITestOutput(
    outputLocation: String,
    outputMode: String,
    containsNoPrefixOption: Boolean,
    containsPrefixOption: Boolean,
    config: String)

case class DeltaTableBuilderAPITestOutput(
    outputLocation: String,
    outputMode: String,
    containsNoPrefixOptionLowerCase: Boolean,
    containsNoPrefixOption: Boolean,
    containsPrefixOption: Boolean,
    error: Boolean,
    config: String)

case class SQLAPIOutput(
    outputLocation: String,
    confiInput: String,
    sqlOperation: String,
    asSelect: Boolean,
    containsOptionNoPrefix: String,
    containsOptionPrefix: String,
    containsTblPropertiesNoPrefix: String,
    containsTblPropertiesPrefix: String,
    config: String)
