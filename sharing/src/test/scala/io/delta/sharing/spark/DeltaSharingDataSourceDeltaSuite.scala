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
  TimestampType
}

trait DeltaSharingDataSourceDeltaSuiteBase
    extends QueryTest
    with DeltaSQLCommandTest
    with DeltaSharingTestSparkUtils
    with DeltaSharingDataSourceDeltaTestUtils {

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

  def assertLimit(tablePath: String, expectedLimit: Seq[Long]): Unit = {
    assert(expectedLimit ==
      TestClientForDeltaFormatSharing.limits.filter(_._1.contains(tablePath)).map(_._2))
  }

  def assertRequestedFormat(tablePath: String, expectedFormat: Seq[String]): Unit = {
    assert(expectedFormat ==
      TestClientForDeltaFormatSharing.requestedFormat.filter(_._1.contains(tablePath)).map(_._2))
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

        val expectedSchema: StructType = new StructType()
          .add("c1", IntegerType)
          .add("c2", StringType)
          .add("c3", DateType)
          .add("c4", TimestampType)
        val expected = Seq(
          Row(1, "one", sqlDate("2023-01-01"), sqlTimestamp("2023-01-01 00:00:00")),
          Row(2, "two", sqlDate("2023-02-02"), sqlTimestamp("2023-02-02 00:00:00"))
        )

        Seq(true, false).foreach { enableLimitPushdown =>
          val sharedTableName = s"shared_table_simple_$enableLimitPushdown"
          prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName)
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
            assertLimit(tableName, Some(1L).filter(_ => enableLimitPushdown).toSeq)
          }

          val limitPushdownConfig = Map(
            "spark.delta.sharing.limitPushdown.enabled" -> enableLimitPushdown.toString
          )
          withSQLConf((limitPushdownConfig ++ getDeltaSharingClassesSQLConf).toSeq: _*) {
            val profileFile = prepareProfileFile(tempDir)
            val tableName = s"share1.default.$sharedTableName"
            test(s"${profileFile.getCanonicalPath}#$tableName", tableName)
          }
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

        val sharedTableName = "shared_table_filters"
        prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        // The files returned from delta sharing client are the same for these queries.
        // This is to test the filters are passed correctly to TahoeLogFileIndex for the local delta
        // log.
        def testFiltersAndSelect(tablePath: String): Unit = {
          var expected = Seq(Row(1, "first"), Row(1, "second"), Row(1, "third"), Row(2, "second"))
          var df = spark.read
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .load(tablePath)
            .filter(col("c1") === 1 || col("c2") === "second")
          checkAnswer(df, expected)

          expected = Seq(Row(1, "first"), Row(1, "second"), Row(1, "third"))
          df = spark.read
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .load(tablePath)
            .filter(col("c1") === 1)
          checkAnswer(df, expected)

          expected = Seq(Row(1, "second"), Row(2, "second"))
          df = spark.read
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .load(tablePath)
            .filter(col("c2") === "second")
          checkAnswer(df, expected)

          // with select as well
          expected = Seq(Row(1), Row(1), Row(1), Row(2), Row(2), Row(2))
          df = spark.read
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .load(tablePath)
            .select("c1")
          checkAnswer(df, expected)

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

          expected = Seq(Row(1), Row(2))
          df = spark.read
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .load(tablePath)
            .filter(col("c2") === "second")
            .select("c1")
          checkAnswer(df, expected)
        }

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val profileFile = prepareProfileFile(tempDir)
          testFiltersAndSelect(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
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
          // linzhou
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
}

class DeltaSharingDataSourceDeltaSuite extends DeltaSharingDataSourceDeltaSuiteBase {}
