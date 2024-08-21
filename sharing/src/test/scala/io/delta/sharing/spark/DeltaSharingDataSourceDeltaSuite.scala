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

package io.delta.sharing.spark

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.delta.sharing.DeltaSharingTestSparkUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{
  DateType,
  IntegerType,
  LongType,
  StringType,
  StructType,
  TimestampNTZType,
  TimestampType
}

trait DeltaSharingDataSourceDeltaSuiteBase
    extends QueryTest
    with DeltaSQLCommandTest
    with DeltaSharingTestSparkUtils
    with DeltaSharingDataSourceDeltaTestUtils {

  override def beforeEach(): Unit = {
    spark.sessionState.conf.setConfString(
      "spark.delta.sharing.jsonPredicateV2Hints.enabled",
      "false"
    )
  }

  /**
   * metadata tests
   */
  test("failed to getMetadata") {
    withTempDir { tempDir =>
      val sharedTableName = "shared_table_broken_json"

      def test(tablePath: String, tableFullName: String): Unit = {
        DeltaSharingUtils.overrideIteratorBlock[String](
          blockId = TestClientForDeltaFormatSharing.getBlockId(sharedTableName, "getMetadata"),
          values = Seq("bad protocol string", "bad metadata string").toIterator
        )
        DeltaSharingUtils.overrideSingleBlock[Long](
          blockId = TestClientForDeltaFormatSharing.getBlockId(sharedTableName, "getTableVersion"),
          value = 1
        )
        // JsonParseException on "bad protocol string"
        val exception = intercept[com.fasterxml.jackson.core.JsonParseException] {
          spark.read.format("deltaSharing").option("responseFormat", "delta").load(tablePath).schema
        }
        assert(exception.getMessage.contains("Unrecognized token 'bad'"))

        // table_with_broken_protocol
        // able to parse as a DeltaSharingSingleAction, but it's an addFile, not metadata.
        DeltaSharingUtils.overrideIteratorBlock[String](
          blockId = TestClientForDeltaFormatSharing.getBlockId(sharedTableName, "getMetadata"),
          // scalastyle:off line.size.limit
          values = Seq(
            """{"add": {"path":"random","id":"random","partitionValues":{},"size":1,"motificationTime":1,"dataChange":false}}"""
          ).toIterator
        )
        val exception2 = intercept[IllegalStateException] {
          spark.read.format("deltaSharing").option("responseFormat", "delta").load(tablePath).schema
        }
        assert(
          exception2.getMessage
            .contains(s"Failed to get Protocol for $tableFullName")
        )

        // table_with_broken_metadata
        // able to parse as a DeltaSharingSingleAction, but it's an addFile, not metadata.
        DeltaSharingUtils.overrideIteratorBlock[String](
          blockId = TestClientForDeltaFormatSharing.getBlockId(sharedTableName, "getMetadata"),
          values = Seq(
            """{"protocol":{"minReaderVersion":1}}"""
          ).toIterator
        )
        val exception3 = intercept[IllegalStateException] {
          spark.read.format("deltaSharing").option("responseFormat", "delta").load(tablePath).schema
        }
        assert(
          exception3.getMessage
            .contains(s"Failed to get Metadata for $tableFullName")
        )
      }

      withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
        val profileFile = prepareProfileFile(tempDir)
        val tableFullName = s"share1.default.$sharedTableName"
        test(s"${profileFile.getCanonicalPath}#$tableFullName", tableFullName)
      }
    }
  }

  def assertLimit(tableName: String, expectedLimit: Seq[Long]): Unit = {
    assert(expectedLimit ==
      TestClientForDeltaFormatSharing.limits.filter(_._1.contains(tableName)).map(_._2))
  }

  def assertRequestedFormat(tableName: String, expectedFormat: Seq[String]): Unit = {
    assert(expectedFormat ==
      TestClientForDeltaFormatSharing.requestedFormat.filter(_._1.contains(tableName)).map(_._2))
  }

  def assertJsonPredicateHints(tableName: String, expectedHints: Seq[String]): Unit = {
    assert(expectedHints ==
      TestClientForDeltaFormatSharing.jsonPredicateHints.filter(_._1.contains(tableName)).map(_._2)
    )
  }
  /**
   * snapshot queries
   */
  test("DeltaSharingDataSource able to read simple data") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_simple"
      withTable(deltaTableName) {
        createTable(deltaTableName)
        sql(
          s"INSERT INTO $deltaTableName" +
          """ VALUES (1, "one", "2023-01-01", "2023-01-01 00:00:00"),
              |(2, "two", "2023-02-02", "2023-02-02 00:00:00")""".stripMargin
        )

        val sharedTableName = "shared_table_simple"
        prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        val expectedSchema: StructType = new StructType()
          .add("c1", IntegerType)
          .add("c2", StringType)
          .add("c3", DateType)
          .add("c4", TimestampType)
        val expected = Seq(
          Row(1, "one", sqlDate("2023-01-01"), sqlTimestamp("2023-01-01 00:00:00")),
          Row(2, "two", sqlDate("2023-02-02"), sqlTimestamp("2023-02-02 00:00:00"))
        )

        Seq(true, false).foreach { skippingEnabled =>
          Seq(true, false).foreach { sharingConfig =>
            Seq(true, false).foreach { deltaConfig =>
              val sharedTableName = s"shared_table_simple_" +
                s"${skippingEnabled}_${sharingConfig}_$deltaConfig"
              prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName)
              prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName, limitHint = Some(1))
              prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

              def test(tablePath: String, tableName: String): Unit = {
                assert(
                  expectedSchema == spark.read
                    .format("deltaSharing")
                    .option("responseFormat", "delta")
                    .load(tablePath)
                    .schema
                )
                val df =
                  spark.read.format("deltaSharing").option("responseFormat", "delta").load(tablePath)
                  checkAnswer(df, expected)
                assert(df.count() > 0)
                assertLimit(tableName, Seq.empty[Long])
                val limitDf = spark.read
                  .format("deltaSharing")
                  .option("responseFormat", "delta")
                  .load(tablePath)
                  .limit(1)
                assert(limitDf.collect().size == 1)
                assertLimit(tableName, Some(1L).filter(_ => skippingEnabled && sharingConfig && deltaConfig).toSeq)
              }

              val limitPushdownConfigs = Map(
                "spark.delta.sharing.limitPushdown.enabled" -> sharingConfig.toString,
                DeltaSQLConf.DELTA_LIMIT_PUSHDOWN_ENABLED.key -> deltaConfig.toString,
                DeltaSQLConf.DELTA_STATS_SKIPPING.key -> skippingEnabled.toString
              )
              withSQLConf((limitPushdownConfigs ++ getDeltaSharingClassesSQLConf).toSeq: _*) {
                val profileFile = prepareProfileFile(tempDir)
                val tableName = s"share1.default.$sharedTableName"
                test(s"${profileFile.getCanonicalPath}#$tableName", tableName)
              }
            }
          }
        }
      }
    }
  }

  test("DeltaSharingDataSource able to read data with changes") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_change"

      def test(tablePath: String, expectedCount: Int, expectedSchema: StructType): Unit = {
        assert(
          expectedSchema == spark.read
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .load(tablePath)
            .schema
        )

        val deltaDf = spark.read.format("delta").table(deltaTableName)
        val sharingDf =
          spark.read.format("deltaSharing").option("responseFormat", "delta").load(tablePath)
        checkAnswer(deltaDf, sharingDf)
        assert(sharingDf.count() == expectedCount)
      }

      withTable(deltaTableName) {
        val sharedTableName = "shared_table_change"
        createTable(deltaTableName)

        // test 1: insert 2 rows
        sql(
          s"INSERT INTO $deltaTableName" +
            """ VALUES (1, "one", "2023-01-01", "2023-01-01 00:00:00"),
              |(2, "two", "2023-02-02", "2023-02-02 00:00:00")""".stripMargin
        )
        prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
        val expectedSchema: StructType = new StructType()
          .add("c1", IntegerType)
          .add("c2", StringType)
          .add("c3", DateType)
          .add("c4", TimestampType)
        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          val tableName = s"share1.default.$sharedTableName"
          test(s"${profileFile.getCanonicalPath}#$tableName", 2, expectedSchema)
        }

        // test 2: insert 2 more rows, and rename a column
        spark.sql(
          s"""ALTER TABLE $deltaTableName SET TBLPROPERTIES('delta.minReaderVersion' = '2',
             |'delta.minWriterVersion' = '5',
             |'delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = true)""".stripMargin
        )
        sql(
          s"INSERT INTO $deltaTableName" +
            """ VALUES (3, "three", "2023-03-03", "2023-03-03 00:00:00"),
              |(4, "four", "2023-04-04", "2023-04-04 00:00:00")""".stripMargin
        )
        sql(s"""ALTER TABLE $deltaTableName RENAME COLUMN c3 TO c3rename""")
        prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
        val expectedNewSchema: StructType = new StructType()
          .add("c1", IntegerType)
          .add("c2", StringType)
          .add("c3rename", DateType)
          .add("c4", TimestampType)
        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          val tableName = s"share1.default.$sharedTableName"
          test(s"${profileFile.getCanonicalPath}#$tableName", 4, expectedNewSchema)
        }

        // test 3: delete 1 row
        sql(s"DELETE FROM $deltaTableName WHERE c1 = 2")
        prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          val tableName = s"share1.default.$sharedTableName"
          test(s"${profileFile.getCanonicalPath}#$tableName", 3, expectedNewSchema)
        }
      }
    }
  }

  test("DeltaSharingDataSource able to auto resolve responseFormat") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_auto"
      withTable(deltaTableName) {
        createSimpleTable(deltaTableName, enableCdf = false)
        sql(
          s"""INSERT INTO $deltaTableName VALUES (1, "one"), (2, "one")""".stripMargin
        )
        sql(
          s"""INSERT INTO $deltaTableName VALUES (1, "two"), (2, "two")""".stripMargin
        )

        val expectedSchema: StructType = new StructType()
          .add("c1", IntegerType)
          .add("c2", StringType)

        def testAutoResolve(tablePath: String, tableName: String, expectedFormat: String): Unit = {
          assert(
            expectedSchema == spark.read
              .format("deltaSharing")
              .load(tablePath)
              .schema
          )

          val deltaDf = spark.read.format("delta").table(deltaTableName)
          val sharingDf = spark.read.format("deltaSharing").load(tablePath)
          checkAnswer(deltaDf, sharingDf)
          assert(sharingDf.count() > 0)
          assertLimit(tableName, Seq.empty[Long])
          assertRequestedFormat(tableName, Seq(expectedFormat))

          val limitDf = spark.read
            .format("deltaSharing")
            .load(tablePath)
            .limit(1)
          assert(limitDf.collect().size == 1)
          assertLimit(tableName, Seq(1L))

          val deltaDfV1 = spark.read.format("delta").option("versionAsOf", 1).table(deltaTableName)
          val sharingDfV1 =
            spark.read.format("deltaSharing").option("versionAsOf", 1).load(tablePath)
          checkAnswer(deltaDfV1, sharingDfV1)
          assert(sharingDfV1.count() > 0)
          assertRequestedFormat(tableName, Seq(expectedFormat))
        }

        // Test for delta format response
        val sharedDeltaTable = "shared_delta_table"
        prepareMockedClientAndFileSystemResult(deltaTableName, sharedDeltaTable)
        prepareMockedClientAndFileSystemResult(deltaTableName, sharedDeltaTable, limitHint = Some(1))
        prepareMockedClientAndFileSystemResult(
          deltaTableName,
          sharedDeltaTable,
          versionAsOf = Some(1)
        )
        prepareMockedClientGetTableVersion(deltaTableName, sharedDeltaTable)

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          testAutoResolve(
            s"${profileFile.getCanonicalPath}#share1.default.$sharedDeltaTable",
            s"share1.default.$sharedDeltaTable",
            "delta"
          )
        }

        // Test for parquet format response
        val sharedParquetTable = "shared_parquet_table"
        prepareMockedClientAndFileSystemResultForParquet(
          deltaTableName,
          sharedParquetTable
        )
        prepareMockedClientAndFileSystemResultForParquet(
          deltaTableName,
          sharedParquetTable,
          limitHint = Some(1)
        )
        prepareMockedClientAndFileSystemResultForParquet(
          deltaTableName,
          sharedParquetTable,
          versionAsOf = Some(1)
        )
        prepareMockedClientGetTableVersion(deltaTableName, sharedParquetTable)

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          testAutoResolve(
            s"${profileFile.getCanonicalPath}#share1.default.$sharedParquetTable",
            s"share1.default.$sharedParquetTable",
            "parquet"
          )
        }
      }
    }
  }

  test("DeltaSharingDataSource able to read data with filters and select") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_filters"
      withTable(deltaTableName) {
        createSimpleTable(deltaTableName, enableCdf = false)
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "first"), (2, "first")""")
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "second"), (2, "second")""")
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "third"), (2, "third")""")

        Seq("c1", "c2", "c1c2").foreach { filterColumn =>
          val sharedTableName = s"shared_table_filters_$filterColumn"
          prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName)
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

          spark.sessionState.conf.setConfString(
            "spark.delta.sharing.jsonPredicateV2Hints.enabled",
            "true"
          )

          // The files returned from delta sharing client are the same for these queries.
          // This is to test the filters are passed correctly to TahoeLogFileIndex for the local delta
          // log.
          def testFiltersAndSelect(tablePath: String, tableName: String): Unit = {
            // select
            var expected = Seq(Row(1), Row(1), Row(1), Row(2), Row(2), Row(2))
            var df = spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .load(tablePath)
              .select("c1")
            checkAnswer(df, expected)
            assertJsonPredicateHints(tableName, Seq.empty[String])

            expected = Seq(
              Row("first"),
              Row("first"),
              Row("second"),
              Row("second"),
              Row("third"),
              Row("third")
            )
            df = spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .load(tablePath)
              .select("c2")
            checkAnswer(df, expected)
            assertJsonPredicateHints(tableName, Seq.empty[String])

            // filter
            var expectedJson = ""
            if (filterColumn == "c1c2") {
              expected = Seq(Row(1, "first"), Row(1, "second"), Row(1, "third"), Row(2, "second"))
              df = spark.read
                .format("deltaSharing")
                .option("responseFormat", "delta")
                .load(tablePath)
                .filter(col("c1") === 1 || col("c2") === "second")
              checkAnswer(df, expected)
              expectedJson =
                """{"op":"or","children":[
                  |  {"op":"equal","children":[
                  |    {"op":"column","name":"c1","valueType":"int"},
                  |    {"op":"literal","value":"1","valueType":"int"}]},
                  |  {"op":"equal","children":[
                  |    {"op":"column","name":"c2","valueType":"string"},
                  |    {"op":"literal","value":"second","valueType":"string"}]}
                  |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
              assertJsonPredicateHints(tableName, Seq(expectedJson))
            } else if (filterColumn == "c1") {
              expected = Seq(Row(1, "first"), Row(1, "second"), Row(1, "third"))
              df = spark.read
                .format("deltaSharing")
                .option("responseFormat", "delta")
                .load(tablePath)
                .filter(col("c1") === 1)
              checkAnswer(df, expected)
              expectedJson =
                """{"op":"and","children":[
                  |  {"op":"not","children":[
                  |    {"op":"isNull","children":[
                  |      {"op":"column","name":"c1","valueType":"int"}]}]},
                  |  {"op":"equal","children":[
                  |    {"op":"column","name":"c1","valueType":"int"},
                  |    {"op":"literal","value":"1","valueType":"int"}]}
                  |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
              assertJsonPredicateHints(tableName, Seq(expectedJson))
            } else {
              assert(filterColumn == "c2")
              expected = Seq(Row(1, "second"), Row(2, "second"))
              expectedJson =
                """{"op":"and","children":[
                  |  {"op":"not","children":[
                  |    {"op":"isNull","children":[
                  |      {"op":"column","name":"c2","valueType":"string"}]}]},
                  |  {"op":"equal","children":[
                  |    {"op":"column","name":"c2","valueType":"string"},
                  |    {"op":"literal","value":"second","valueType":"string"}]}
                  |]}""".stripMargin.replaceAll("\n", "").replaceAll(" ", "")
              df = spark.read
                .format("deltaSharing")
                .option("responseFormat", "delta")
                .load(tablePath)
                .filter(col("c2") === "second")
              checkAnswer(df, expected)
              assertJsonPredicateHints(tableName, Seq(expectedJson))

              // filters + select as well
              expected = Seq(Row(1), Row(2))
              df = spark.read
                .format("deltaSharing")
                .option("responseFormat", "delta")
                .load(tablePath)
                .filter(col("c2") === "second")
                .select("c1")
              checkAnswer(df, expected)
              assertJsonPredicateHints(tableName, Seq(expectedJson))
            }
          }

          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            val profileFile = prepareProfileFile(tempDir)
            testFiltersAndSelect(
              s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName",
              s"share1.default.$sharedTableName"
            )
          }
        }
      }
    }
  }

  test("DeltaSharingDataSource able to read data with different filters") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_diff_filter"
      withTable(deltaTableName) {
        createSimpleTable(deltaTableName, enableCdf = false)
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "first"), (2, "first")""")
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "second"), (2, "second")""")
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "third"), (2, "third")""")

        val sharedTableName = s"shared_table_filters_diff_filter"
        prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName, limitHint = Some(2))
        prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        spark.sessionState.conf.setConfString(
          "spark.delta.sharing.jsonPredicateV2Hints.enabled",
          "true"
        )

        // The files returned from delta sharing client are the same for these queries.
        // This is to test the filters are passed correctly to TahoeLogFileIndex for the local delta
        // log.
        def testDiffFilter(tablePath: String, tableName: String): Unit = {
          val df = spark.read
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .load(tablePath)

          // limit
          assert(df.limit(2).count() == 2)
          // full
          val expectedFull = Seq(
            Row(1, "first"), Row(1, "second"), Row(1, "third"),
            Row(2, "first"), Row(2, "second"), Row(2, "third")
          )
          checkAnswer(df, expectedFull)
        }

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          testDiffFilter(
            s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName",
            s"share1.default.$sharedTableName"
          )
        }
      }
    }
  }

  test("DeltaSharingDataSource able to read data for time travel queries") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_time_travel"
      withTable(deltaTableName) {
        createTable(deltaTableName)

        sql(
          s"INSERT INTO $deltaTableName" +
          """ VALUES (1, "one", "2023-01-01", "2023-01-01 00:00:00")""".stripMargin
        )
        sql(
          s"INSERT INTO $deltaTableName" +
          """ VALUES (2, "two", "2023-02-02", "2023-02-02 00:00:00")""".stripMargin
        )
        sql(
          s"INSERT INTO $deltaTableName" +
          """ VALUES (3, "three", "2023-03-03", "2023-03-03 00:00:00")""".stripMargin
        )

        val sharedTableNameV1 = "shared_table_v1"
        prepareMockedClientAndFileSystemResult(
          deltaTable = deltaTableName,
          sharedTable = sharedTableNameV1,
          versionAsOf = Some(1L)
        )

        def testVersionAsOf1(tablePath: String): Unit = {
          val dfV1 = spark.read
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .option("versionAsOf", 1)
            .load(tablePath)
          val expectedV1 = Seq(
            Row(1, "one", sqlDate("2023-01-01"), sqlTimestamp("2023-01-01 00:00:00"))
          )
            checkAnswer(dfV1, expectedV1)
        }
        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          testVersionAsOf1(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableNameV1")
        }

        // using different table name because spark caches the content read from a file, i.e.,
        // the delta log from 0.json.
        // TODO: figure out how to get a per query id and use it in getCustomTablePath to
        //  differentiate the same table used in different queries.
        // TODO: Also check if it's possible to disable the file cache.
        val sharedTableNameV3 = "shared_table_v3"
        prepareMockedClientAndFileSystemResult(
          deltaTable = deltaTableName,
          sharedTable = sharedTableNameV3,
          versionAsOf = Some(3L)
        )

        def testVersionAsOf3(tablePath: String): Unit = {
          val dfV3 = spark.read
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .option("versionAsOf", 3)
            .load(tablePath)
          val expectedV3 = Seq(
            Row(1, "one", sqlDate("2023-01-01"), sqlTimestamp("2023-01-01 00:00:00")),
            Row(2, "two", sqlDate("2023-02-02"), sqlTimestamp("2023-02-02 00:00:00")),
            Row(3, "three", sqlDate("2023-03-03"), sqlTimestamp("2023-03-03 00:00:00"))
          )
            checkAnswer(dfV3, expectedV3)
        }
        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          testVersionAsOf3(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableNameV3")
        }

        val sharedTableNameTs = "shared_table_ts"
        // Given the result of delta sharing rpc is mocked, the actual value of the timestampStr
        // can be any thing that's valid for DeltaSharingOptions, and formattedTimestamp is the
        // parsed result and will be sent in the delta sharing rpc.
        val timestampStr = "2023-01-01 00:00:00"
        val formattedTimestamp = "2023-01-01T08:00:00Z"

        prepareMockedClientGetTableVersion(deltaTableName, sharedTableNameTs)
        prepareMockedClientAndFileSystemResult(
          deltaTable = deltaTableName,
          sharedTable = sharedTableNameTs,
          versionAsOf = None,
          timestampAsOf = Some(formattedTimestamp)
        )

        def testTimestampQuery(tablePath: String): Unit = {
          val dfTs = spark.read
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .option("timestampAsOf", timestampStr)
            .load(tablePath)
          val expectedTs = Seq(
            Row(1, "one", sqlDate("2023-01-01"), sqlTimestamp("2023-01-01 00:00:00")),
            Row(2, "two", sqlDate("2023-02-02"), sqlTimestamp("2023-02-02 00:00:00")),
            Row(3, "three", sqlDate("2023-03-03"), sqlTimestamp("2023-03-03 00:00:00"))
          )
            checkAnswer(dfTs, expectedTs)
        }
        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          testTimestampQuery(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableNameTs")
        }
      }
    }
  }

  test("DeltaSharingDataSource able to read data with more entries") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_more"
      withTable(deltaTableName) {
        createSimpleTable(deltaTableName, enableCdf = false)
        // The table operations take about 6~10 seconds.
        for (i <- 0 to 9) {
          val iteration = s"iteration $i"
          val valuesBuilder = Seq.newBuilder[String]
          for (j <- 0 to 49) {
            valuesBuilder += s"""(${i * 10 + j}, "$iteration")"""
          }
          sql(s"INSERT INTO $deltaTableName VALUES ${valuesBuilder.result().mkString(",")}")
        }

        val sharedTableName = "shared_table_more"
        prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        val expectedSchema: StructType = new StructType()
          .add("c1", IntegerType)
          .add("c2", StringType)
        val expected = spark.read.format("delta").table(deltaTableName)

        def test(tablePath: String): Unit = {
          assert(
            expectedSchema == spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .load(tablePath)
              .schema
          )
          val df =
            spark.read.format("deltaSharing").option("responseFormat", "delta").load(tablePath)
          checkAnswer(df, expected)
        }

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          test(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
        }
      }
    }
  }

  test("DeltaSharingDataSource able to read data with join on the same table") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_join"
      withTable(deltaTableName) {
        createSimpleTable(deltaTableName, enableCdf = false)
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "first"), (2, "first")""")
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "second"), (2, "second")""")
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "third"), (2, "third")""")

        val sharedTableName = "shared_table_join"
        prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
        prepareMockedClientAndFileSystemResult(
          deltaTableName,
          sharedTableName,
          versionAsOf = Some(1L)
        )

        def testJoin(tablePath: String): Unit = {
          // Query the same latest version
          val deltaDfLatest = spark.read.format("delta").table(deltaTableName)
          val deltaDfV1 = spark.read.format("delta").option("versionAsOf", 1).table(deltaTableName)
          val sharingDfLatest =
            spark.read.format("deltaSharing").option("responseFormat", "delta").load(tablePath)
          val sharingDfV1 =
            spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("versionAsOf", 1)
              .load(tablePath)

          var deltaDfJoined = deltaDfLatest.join(deltaDfLatest, "c1")
          var sharingDfJoined = sharingDfLatest.join(sharingDfLatest, "c1")
          // CheckAnswer ensures that delta sharing produces the same result as delta.
          // The check on the size is used to double check that a valid dataframe is generated.
          checkAnswer(deltaDfJoined, sharingDfJoined)
          assert(sharingDfJoined.count() > 0)

          // Query the same versionAsOf
          deltaDfJoined = deltaDfV1.join(deltaDfV1, "c1")
          sharingDfJoined = sharingDfV1.join(sharingDfV1, "c1")
          checkAnswer(deltaDfJoined, sharingDfJoined)
          assert(sharingDfJoined.count() > 0)

          // Query with different versions
          deltaDfJoined = deltaDfLatest.join(deltaDfV1, "c1")
          sharingDfJoined = sharingDfLatest.join(sharingDfV1, "c1")
          checkAnswer(deltaDfJoined, sharingDfJoined)
          // Size is 6 because for each of the 6 rows in latest, there is 1 row with the same c1
          // value in v1.
          assert(sharingDfJoined.count() > 0)
        }

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          testJoin(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
        }
      }
    }
  }

  test("DeltaSharingDataSource able to read empty data") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_empty"
      withTable(deltaTableName) {
        createSimpleTable(deltaTableName, enableCdf = true)
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "first"), (2, "first")""")
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "second"), (2, "second")""")
        sql(s"DELETE FROM $deltaTableName WHERE c1 <= 2")
        // This command is just to create an empty table version at version 4.
        spark.sql(s"ALTER TABLE $deltaTableName SET TBLPROPERTIES('delta.minReaderVersion' = 1)")

        val sharedTableName = "shared_table_empty"
        prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        def testEmpty(tablePath: String): Unit = {
          val deltaDf = spark.read.format("delta").table(deltaTableName)
          val sharingDf =
            spark.read.format("deltaSharing").option("responseFormat", "delta").load(tablePath)
          checkAnswer(deltaDf, sharingDf)
          assert(sharingDf.count() == 0)

          val deltaCdfDf = spark.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", 4)
            .table(deltaTableName)
          val sharingCdfDf = spark.read
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", 4)
            .load(tablePath)
          checkAnswer(deltaCdfDf, sharingCdfDf)
          assert(sharingCdfDf.count() == 0)
        }

        // There's only metadata change but not actual files in version 4.
        prepareMockedClientAndFileSystemResultForCdf(
          deltaTableName,
          sharedTableName,
          startingVersion = 4
        )

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          testEmpty(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
        }
      }
    }
  }

  /**
   * cdf queries
   */
  test("DeltaSharingDataSource able to read data for simple cdf query") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_cdf"
      withTable(deltaTableName) {
        sql(s"""
               |CREATE TABLE $deltaTableName (c1 INT, c2 STRING) USING DELTA PARTITIONED BY (c2)
               |TBLPROPERTIES (delta.enableChangeDataFeed = true)
               |""".stripMargin)
        // 2 inserts in version 1, 1 with c1=2
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "one"), (2, "two")""")
        // 1 insert in version 2, 0 with c1=2
        sql(s"""INSERT INTO $deltaTableName VALUES (3, "two")""")
        // 0 operations in version 3
        sql(s"""OPTIMIZE $deltaTableName""")
        // 2 updates in version 4, 2 with c1=2
        sql(s"""UPDATE $deltaTableName SET c2="new two" where c1=2""")
        // 1 delete in version 5, 1 with c1=2
        sql(s"""DELETE FROM $deltaTableName WHERE c1 = 2""")

        val sharedTableName = "shard_table_cdf"
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        Seq(0, 1, 2, 3, 4, 5).foreach { startingVersion =>
          val ts = getTimeStampForVersion(deltaTableName, startingVersion)
          val startingTimestamp = DateTimeUtils.toJavaTimestamp(ts * 1000).toInstant.toString
          prepareMockedClientAndFileSystemResultForCdf(
            deltaTableName,
            sharedTableName,
            startingVersion,
            Some(startingTimestamp)
          )

          def test(tablePath: String): Unit = {
            val expectedSchema: StructType = new StructType()
              .add("c1", IntegerType)
              .add("c2", StringType)
              .add("_change_type", StringType)
              .add("_commit_version", LongType)
              .add("_commit_timestamp", TimestampType)
            val schema = spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", startingVersion)
              .load(tablePath)
              .schema
            assert(expectedSchema == schema)

            val expected = spark.read
              .format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", startingVersion)
              .table(deltaTableName)
            val df = spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", startingVersion)
              .load(tablePath)
            checkAnswer(df, expected)
            assert(df.count() > 0)
          }

          def testFiltersAndSelect(tablePath: String): Unit = {
            val expectedSchema: StructType = new StructType()
              .add("c2", StringType)
              .add("_change_type", StringType)
              .add("_commit_version", LongType)
            val schema = spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", startingVersion)
              .load(tablePath)
              .select("c2", "_change_type", "_commit_version")
              .schema
            assert(expectedSchema == schema)

            val expected = spark.read
              .format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", startingVersion)
              .table(deltaTableName)
              .select("c2", "_change_type", "_commit_version")
            val dfVersion = spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", startingVersion)
              .load(tablePath)
              .select("c2", "_change_type", "_commit_version")
            checkAnswer(dfVersion, expected)
            val dfTime = spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingTimestamp", startingTimestamp)
              .load(tablePath)
              .select("c2", "_change_type", "_commit_version")
            checkAnswer(dfTime, expected)
            assert(dfTime.count() > 0)

            val expectedFiltered = spark.read
              .format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", startingVersion)
              .table(deltaTableName)
              .select("c2", "_change_type", "_commit_version")
              .filter(col("c1") === 2)
            val dfFiltered = spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", startingVersion)
              .load(tablePath)
              .select("c2", "_change_type", "_commit_version")
              .filter(col("c1") === 2)
            checkAnswer(dfFiltered, expectedFiltered)
            assert(dfFiltered.count() > 0)
          }

          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            val profileFile = prepareProfileFile(tempDir)
            test(profileFile.getCanonicalPath + s"#share1.default.$sharedTableName")
            testFiltersAndSelect(
              profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"
            )
          }
        }

        // test join on the same table in cdf query
        def testJoin(tablePath: String): Unit = {
          val deltaV0 = spark.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", 0)
            .table(deltaTableName)
          val deltaV3 = spark.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", 3)
            .table(deltaTableName)
          val sharingV0 = spark.read
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", 0)
            .load(tablePath)
          val sharingV3 = spark.read
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", 3)
            .load(tablePath)

          def testJoinedDf(
              deltaLeft: DataFrame,
              deltaRight: DataFrame,
              sharingLeft: DataFrame,
              sharingRight: DataFrame,
              expectedSize: Int): Unit = {
            val deltaJoined = deltaLeft.join(deltaRight, usingColumns = Seq("c1", "c2"))
            val sharingJoined = sharingLeft.join(sharingRight, usingColumns = Seq("c1", "c2"))
            checkAnswer(deltaJoined, sharingJoined)
            assert(sharingJoined.count() > 0)
          }
          testJoinedDf(deltaV0, deltaV0, sharingV0, sharingV0, 10)
          testJoinedDf(deltaV3, deltaV3, sharingV3, sharingV3, 5)
          testJoinedDf(deltaV0, deltaV3, sharingV0, sharingV3, 6)
        }

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          testJoin(profileFile.getCanonicalPath + s"#share1.default.$sharedTableName")
        }
      }
    }
  }

  test("DeltaSharingDataSource able to read data for cdf query with more entries") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_cdf_more"
      withTable(deltaTableName) {
        createSimpleTable(deltaTableName, enableCdf = true)
        // The table operations take about 20~30 seconds.
        for (i <- 0 to 9) {
          val iteration = s"iteration $i"
          val valuesBuilder = Seq.newBuilder[String]
          for (j <- 0 to 49) {
            valuesBuilder += s"""(${i * 10 + j}, "$iteration")"""
          }
          sql(s"INSERT INTO $deltaTableName VALUES ${valuesBuilder.result().mkString(",")}")
          sql(s"""UPDATE $deltaTableName SET c1 = c1 + 100 where c2 = "${iteration}"""")
          sql(s"""DELETE FROM $deltaTableName where c2 = "${iteration}"""")
        }

        val sharedTableName = "shard_table_cdf_more"
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
        Seq(0, 10, 20, 30).foreach { startingVersion =>
          prepareMockedClientAndFileSystemResultForCdf(
            deltaTableName,
            sharedTableName,
            startingVersion
          )

          val expected = spark.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", startingVersion)
            .table(deltaTableName)

          def test(tablePath: String): Unit = {
            val df = spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", startingVersion)
              .load(tablePath)
            checkAnswer(df, expected)
            assert(df.count() > 0)
          }

          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            val profileFile = prepareProfileFile(tempDir)
            test(profileFile.getCanonicalPath + s"#share1.default.$sharedTableName")
          }
        }
      }
    }
  }

  test("DeltaSharingDataSource able to read data with special chars") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_special"
      withTable(deltaTableName) {
        // scalastyle:off nonascii
        sql(s"""CREATE TABLE $deltaTableName (`第一列` INT, c2 STRING)
               |USING DELTA PARTITIONED BY (c2)
               |""".stripMargin)
        // The table operations take about 6~10 seconds.
        for (i <- 0 to 99) {
          val iteration = s"iteration $i"
          val valuesBuilder = Seq.newBuilder[String]
          for (j <- 0 to 99) {
            valuesBuilder += s"""(${i * 10 + j}, "$iteration")"""
          }
          sql(s"INSERT INTO $deltaTableName VALUES ${valuesBuilder.result().mkString(",")}")
        }

        val sharedTableName = "shared_table_more"
        prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        val expectedSchema: StructType = new StructType()
          .add("第一列", IntegerType)
          .add("c2", StringType)
        // scalastyle:on nonascii
        val expected = spark.read.format("delta").table(deltaTableName)

        def test(tablePath: String): Unit = {
          assert(
            expectedSchema == spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .load(tablePath)
              .schema
          )
          val df =
            spark.read.format("deltaSharing").option("responseFormat", "delta").load(tablePath)
          checkAnswer(df, expected)
        }

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          test(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
        }
      }
    }
  }

  test("DeltaSharingDataSource able to read cdf with special chars") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_cdf_special"
      withTable(deltaTableName) {
        // scalastyle:off nonascii
        sql(s"""CREATE TABLE $deltaTableName (`第一列` INT, c2 STRING)
               |USING DELTA PARTITIONED BY (c2)
               |TBLPROPERTIES (delta.enableChangeDataFeed = true)
               |""".stripMargin)
        // The table operations take about 20~30 seconds.
        for (i <- 0 to 9) {
          val iteration = s"iteration $i"
          val valuesBuilder = Seq.newBuilder[String]
          for (j <- 0 to 49) {
            valuesBuilder += s"""(${i * 10 + j}, "$iteration")"""
          }
          sql(s"INSERT INTO $deltaTableName VALUES ${valuesBuilder.result().mkString(",")}")
          sql(s"""UPDATE $deltaTableName SET `第一列` = `第一列` + 100 where c2 = "${iteration}"""")
          // scalastyle:on nonascii
          sql(s"""DELETE FROM $deltaTableName where c2 = "${iteration}"""")
        }

        val sharedTableName = "shard_table_cdf_special"
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
        Seq(0, 10, 20, 30).foreach { startingVersion =>
          prepareMockedClientAndFileSystemResultForCdf(
            deltaTableName,
            sharedTableName,
            startingVersion
          )

          val expected = spark.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", startingVersion)
            .table(deltaTableName)

          def test(tablePath: String): Unit = {
            val df = spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", startingVersion)
              .load(tablePath)
            checkAnswer(df, expected)
            assert(df.count() > 0)
          }

          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            val profileFile = prepareProfileFile(tempDir)
            test(profileFile.getCanonicalPath + s"#share1.default.$sharedTableName")
          }
        }
      }
    }
  }

  /**
   * deletion vector tests
   */
  test("DeltaSharingDataSource able to read data for dv table") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_dv"
      withTable(deltaTableName) {
        spark
          .range(start = 0, end = 100)
          .withColumn("partition", col("id").divide(10).cast("int"))
          .write
          .partitionBy("partition")
          .format("delta")
          .saveAsTable(deltaTableName)
        spark
          .range(start = 100, end = 200)
          .withColumn("partition", col("id").mod(100).divide(10).cast("int"))
          .write
          .mode("append")
          .partitionBy("partition")
          .format("delta")
          .saveAsTable(deltaTableName)
        spark.sql(
          s"ALTER TABLE $deltaTableName SET TBLPROPERTIES('delta.enableDeletionVectors' = true)"
        )

        // Delete 2 rows per partition.
        sql(s"""DELETE FROM $deltaTableName where mod(id, 10) < 2""")
        // Delete 1 more row per partition.
        sql(s"""DELETE FROM $deltaTableName where mod(id, 10) = 3""")
        // Delete 1 more row per partition.
        sql(s"""DELETE FROM $deltaTableName where mod(id, 10) = 6""")

        Seq(true, false).foreach { skippingEnabled =>
          val sharedTableName = s"shared_table_dv_$skippingEnabled"
          prepareMockedClientAndFileSystemResult(
            deltaTable = deltaTableName,
            sharedTable = sharedTableName,
            assertMultipleDvsInOneFile = true
          )
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

          def testReadDVTable(tablePath: String): Unit = {
            val expectedSchema: StructType = new StructType()
              .add("id", LongType)
              .add("partition", IntegerType)
            assert(
              expectedSchema == spark.read
                .format("deltaSharing")
                .option("responseFormat", "delta")
                .load(tablePath)
                .schema
            )

            val sharingDf =
              spark.read.format("deltaSharing").option("responseFormat", "delta").load(tablePath)
            val deltaDf = spark.read.format("delta").table(deltaTableName)
            val filteredSharingDf =
              spark.read
                .format("deltaSharing")
                .option("responseFormat", "delta")
                .load(tablePath)
                .filter(col("id").mod(10) > 5)
            val filteredDeltaDf =
              spark.read
                .format("delta")
                .table(deltaTableName)
                .filter(col("id").mod(10) > 5)

            if (!skippingEnabled) {
              def assertError(dataFrame: DataFrame): Unit = {
                val ex = intercept[IllegalArgumentException] {
                  dataFrame.collect()
                }
                assert(ex.getMessage contains
                  "Cannot work with a non-pinned table snapshot of the TahoeFileIndex")
              }
              assertError(sharingDf)
              assertError(filteredDeltaDf)
            } else {
            checkAnswer(sharingDf, deltaDf)
            assert(sharingDf.count() > 0)
            checkAnswer(filteredSharingDf, filteredDeltaDf)
            assert(filteredSharingDf.count() > 0)
            }
          }

          val additionalConfigs = Map(
            DeltaSQLConf.DELTA_STATS_SKIPPING.key -> skippingEnabled.toString
          )
          withSQLConf((additionalConfigs ++ getDeltaSharingClassesSQLConf).toSeq: _*) {
            val profileFile = prepareProfileFile(tempDir)
            testReadDVTable(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
          }
        }
      }
    }
  }

  test("DeltaSharingDataSource able to read data for dv and cdf") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_dv_cdf"
      withTable(deltaTableName) {
        createDVTableWithCdf(deltaTableName)
        // version 1: 20 inserts
        spark
          .range(start = 0, end = 20)
          .select(col("id").cast("int").as("c1"))
          .withColumn("partition", col("c1").divide(10).cast("int"))
          .write
          .mode("append")
          .format("delta")
          .saveAsTable(deltaTableName)
        // version 2: 20 inserts
        spark
          .range(start = 100, end = 120)
          .select(col("id").cast("int").as("c1"))
          .withColumn("partition", col("c1").mod(100).divide(10).cast("int"))
          .write
          .mode("append")
          .format("delta")
          .saveAsTable(deltaTableName)
        // version 3: 20 updates
        sql(s"""UPDATE $deltaTableName SET c1=c1+5 where partition=0""")
        // This deletes will create one DV file used by AddFile from both version 1 and version 2.
        // version 4: 14 deletes
        sql(s"""DELETE FROM $deltaTableName WHERE mod(c1, 100)<=10""")

        val sharedTableName = "shard_table_dv_cdf"
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        Seq(0, 1, 2, 3, 4).foreach { startingVersion =>
          prepareMockedClientAndFileSystemResultForCdf(
            deltaTableName,
            sharedTableName,
            startingVersion,
            assertMultipleDvsInOneFile = true
          )

          def testReadDVCdf(tablePath: String): Unit = {
            val schema = spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", startingVersion)
              .load(tablePath)
              .schema
            val expectedSchema: StructType = new StructType()
              .add("c1", IntegerType)
              .add("partition", IntegerType)
              .add("_change_type", StringType)
              .add("_commit_version", LongType)
              .add("_commit_timestamp", TimestampType)
            assert(expectedSchema == schema)

            val deltaDf = spark.read
              .format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", startingVersion)
              .table(deltaTableName)
            val sharingDf = spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", startingVersion)
              .load(tablePath)
            checkAnswer(sharingDf, deltaDf)
            assert(sharingDf.count() > 0)
          }

          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            val profileFile = prepareProfileFile(tempDir)
            testReadDVCdf(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
          }
        }
      }
    }
  }

  test("DeltaSharingDataSource able to read data for inline dv") {
    import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArrayFormat
    Seq(RoaringBitmapArrayFormat.Portable, RoaringBitmapArrayFormat.Native).foreach { format =>
      withTempDir { tempDir =>
        val deltaTableName = s"delta_table_inline_dv_$format"
        withTable(deltaTableName) {
          createDVTableWithCdf(deltaTableName)
          // Use divide 10 to set partition column to 0 for all values, then use repartition to
          // ensure the 5 values are written in one file.
          spark
            .range(start = 0, end = 5)
            .select(col("id").cast("int").as("c1"))
            .withColumn("partition", col("c1").divide(10).cast("int"))
            .repartition(1)
            .write
            .mode("append")
            .format("delta")
            .saveAsTable(deltaTableName)

          val sharedTableName = s"shared_table_inline_dv_$format"
          prepareMockedClientAndFileSystemResult(
            deltaTable = deltaTableName,
            sharedTable = sharedTableName,
            inlineDvFormat = Some(format)
          )
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
          prepareMockedClientAndFileSystemResultForCdf(
            deltaTableName,
            sharedTableName,
            startingVersion = 1,
            inlineDvFormat = Some(format)
          )

          def testReadInlineDVCdf(tablePath: String): Unit = {
            val deltaDf = spark.read
              .format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", 1)
              .table(deltaTableName)
              .filter(col("c1") > 1)
            val sharingDf = spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", 1)
              .load(tablePath)
            checkAnswer(sharingDf, deltaDf)
            assert(sharingDf.count() > 0)
          }

          def testReadInlineDV(tablePath: String): Unit = {
            val expectedSchema: StructType = new StructType()
              .add("c1", IntegerType)
              .add("partition", IntegerType)
            assert(
              expectedSchema == spark.read
                .format("deltaSharing")
                .option("responseFormat", "delta")
                .load(tablePath)
                .schema
            )

            val sharingDf =
              spark.read.format("deltaSharing").option("responseFormat", "delta").load(tablePath)
            val expectedDf = Seq(Row(1, 0), Row(3, 0), Row(4, 0))
            checkAnswer(sharingDf, expectedDf)

            val filteredSharingDf =
              spark.read
                .format("deltaSharing")
                .option("responseFormat", "delta")
                .load(tablePath)
                .filter(col("c1") < 4)
            val expectedFilteredDf = Seq(Row(1, 0), Row(3, 0))
            checkAnswer(filteredSharingDf, expectedFilteredDf)
          }

          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            val profileFile = prepareProfileFile(tempDir)
            testReadInlineDV(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
            testReadInlineDVCdf(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
          }
        }
      }
    }
  }

  test("DeltaSharingDataSource able to read timestampNTZ table") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_timestampNTZ"
      withTable(deltaTableName) {
        sql(s"CREATE TABLE $deltaTableName(c1 TIMESTAMP_NTZ) USING DELTA")
        sql(s"""INSERT INTO $deltaTableName VALUES ('2022-01-02 03:04:05.123456')""")

        val sharedTableName = "shared_table_timestampNTZ"
        prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        def testReadTimestampNTZ(tablePath: String): Unit = {
          val expectedSchema: StructType = new StructType()
            .add("c1", TimestampNTZType)
          assert(
            expectedSchema == spark.read
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .load(tablePath)
              .schema
          )
          val sharingDf =
            spark.read.format("deltaSharing").option("responseFormat", "delta").load(tablePath)
          val deltaDf = spark.read.format("delta").table(deltaTableName)
          checkAnswer(sharingDf, deltaDf)
          assert(sharingDf.count() > 0)
        }

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          testReadTimestampNTZ(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
        }
      }
    }
  }
}

class DeltaSharingDataSourceDeltaSuite extends DeltaSharingDataSourceDeltaSuiteBase {}
