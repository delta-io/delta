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

import org.apache.spark.sql.delta.DeltaConfigs
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.delta.sharing.DeltaSharingTestSparkUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

// Unit tests to verify that type widening works with delta sharing.
class DeltaSharingDataSourceTypeWideningSuite
    extends QueryTest
    with DeltaSQLCommandTest
    with DeltaSharingTestSparkUtils
    with DeltaSharingDataSourceDeltaTestUtils {

  import testImplicits._

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaConfigs.ENABLE_TYPE_WIDENING.defaultTablePropertyKey, true.toString)
  }

  /** Sets up delta sharing mocks to read a table and validates results. */
  private def testReadingDeltaShare(
      tableName: String,
      versionAsOf: Option[Long],
      responseFormat: String,
      filter: Option[Column] = None,
      expectedSchema: StructType,
      expectedJsonPredicate: Seq[String] = Seq.empty,
      expectedResult: DataFrame): Unit = {
    withTempDir { tempDir =>
      val sharedTableName =
        if (responseFormat == DeltaSharingOptions.RESPONSE_FORMAT_DELTA) {
          tableName + "shared_delta_table"
        } else {
          // The mock test client expects the table name to contain 'shared_parquet_table' for
          // parquet format sharing.
          tableName + "shared_parquet_table"
        }
      prepareMockedClientMetadata(tableName, sharedTableName)
      prepareMockedClientGetTableVersion(tableName, sharedTableName, versionAsOf)
      if (responseFormat == DeltaSharingOptions.RESPONSE_FORMAT_DELTA) {
        prepareMockedClientAndFileSystemResult(tableName, sharedTableName, versionAsOf)
      } else {
        assert(responseFormat == DeltaSharingOptions.RESPONSE_FORMAT_PARQUET)
        prepareMockedClientAndFileSystemResultForParquet(tableName, sharedTableName, versionAsOf)
      }

      var reader = spark.read
        .format("deltaSharing")
        .option("responseFormat", responseFormat)
      versionAsOf.foreach { version =>
        reader = reader.option("versionAsOf", version)
      }

      TestClientForDeltaFormatSharing.jsonPredicateHints.clear()
      withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
        val profileFile = prepareProfileFile(tempDir)
        var result = reader
          .load(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
        filter.foreach { f =>
          result = result.filter(f)
        }
        assert(result.schema === expectedSchema)
        checkAnswer(result, expectedResult)
        assert(getJsonPredicateHints(tableName) === expectedJsonPredicate)
      }
    }
  }

  /** Fetches JSON predicates passed to the test client when reading a table. */
  private def getJsonPredicateHints(tableName: String): Seq[String] = {
    TestClientForDeltaFormatSharing
      .jsonPredicateHints
      .filterKeys(_.contains(tableName))
      .values
      .toSeq
  }

  /** Creates a table and applies a type change to it. */
  private def withTestTable(testBody: String => Unit): Unit = {
    val deltaTableName = "type_widening"
    withTable(deltaTableName) {
      sql(s"CREATE TABLE $deltaTableName (value SMALLINT) USING DELTA")
      sql(s"INSERT INTO $deltaTableName VALUES (1), (2)")
      sql(s"ALTER TABLE $deltaTableName CHANGE COLUMN value TYPE INT")
      sql(s"INSERT INTO $deltaTableName VALUES (3), (${Int.MaxValue})")
      sql(s"INSERT INTO $deltaTableName VALUES (4), (5)")
      testBody(deltaTableName)
    }
  }

  /** Short-hand for the type widening metadata for column `value` for the test table above. */
  private val typeWideningMetadata: Metadata =
    new MetadataBuilder()
      .putMetadataArray(
        "delta.typeChanges", Array(
          new MetadataBuilder()
            .putLong("tableVersion", 2)
            .putString("fromType", "short")
            .putString("toType", "integer")
            .build()))
      .build()

  for (responseFormat <- Seq(DeltaSharingOptions.RESPONSE_FORMAT_DELTA,
    DeltaSharingOptions.RESPONSE_FORMAT_PARQUET)) {
    test(s"Delta sharing with type widening, responseFormat=$responseFormat") {
      withTestTable { tableName =>
        testReadingDeltaShare(
          tableName,
          versionAsOf = None,
          responseFormat,
          expectedSchema = new StructType()
            .add("value", IntegerType, nullable = true, metadata = typeWideningMetadata),
          expectedResult = Seq(1, 2, 3, Int.MaxValue, 4, 5).toDF("value"))
      }
    }

    test(s"Delta sharing with type widening, time travel, responseFormat=$responseFormat") {
      withTestTable { tableName =>
        testReadingDeltaShare(
          tableName,
          versionAsOf = Some(3),
          responseFormat = DeltaSharingOptions.RESPONSE_FORMAT_DELTA,
          expectedSchema = new StructType()
            .add("value", IntegerType, nullable = true, metadata = typeWideningMetadata),
          expectedResult = Seq(1, 2, 3, Int.MaxValue).toDF("value"))

        testReadingDeltaShare(
          tableName,
          versionAsOf = Some(2),
          responseFormat = DeltaSharingOptions.RESPONSE_FORMAT_DELTA,
          expectedSchema = new StructType()
            .add("value", IntegerType, nullable = true, metadata = typeWideningMetadata),
          expectedResult = Seq(1, 2).toDF("value"))

        testReadingDeltaShare(
          tableName,
          versionAsOf = Some(1),
          responseFormat = DeltaSharingOptions.RESPONSE_FORMAT_DELTA,
          expectedSchema = new StructType()
            .add("value", ShortType),
          expectedResult = Seq(1, 2).toDF("value"))
      }
    }

    test(s"jsonPredicateHints on non-partition column after type widening, " +
      s"responseFormat=$responseFormat") {
      withTestTable { tableName =>
        testReadingDeltaShare(
          tableName,
          versionAsOf = None,
          responseFormat,
          filter = Some(col("value") === Int.MaxValue),
          expectedSchema = new StructType()
            .add("value", IntegerType, nullable = true, metadata = typeWideningMetadata),
          expectedResult = Seq(Int.MaxValue).toDF("value"),
          expectedJsonPredicate = Seq(
            """
              |{"op":"and","children":[
              |  {"op":"not","children":[
              |    {"op":"isNull","children":[
              |      {"op":"column","name":"value","valueType":"int"}]}]},
              |  {"op":"equal","children":[
              |    {"op":"column","name":"value","valueType":"int"},
              |    {"op":"literal","value":"2147483647","valueType":"int"}]}]}
            """.stripMargin.replaceAll("\n", "").replaceAll(" ", ""))
        )
      }
    }

    test(s"jsonPredicateHints on partition column after type widening, " +
      s"responseFormat=$responseFormat") {
      val deltaTableName = "type_widening_partitioned"
      withTable(deltaTableName) {
        sql(
          s"""
             |CREATE TABLE $deltaTableName (part SMALLINT, value SMALLINT)
             |USING DELTA
             |PARTITIONED BY (part)
           """.stripMargin
        )
        sql(s"INSERT INTO $deltaTableName VALUES (1, 1), (2, 2)")
        sql(s"ALTER TABLE $deltaTableName CHANGE COLUMN part TYPE INT")
        sql(s"INSERT INTO $deltaTableName VALUES (3, 3), (${Int.MaxValue}, 4)")

        testReadingDeltaShare(
          deltaTableName,
          versionAsOf = None,
          responseFormat,
          filter = Some(col("part") === Int.MaxValue),
          expectedSchema = new StructType()
            .add("part", IntegerType, nullable = true, metadata = typeWideningMetadata)
            .add("value", ShortType),
          expectedResult = Seq((Int.MaxValue, 4)).toDF("part", "value"),
          expectedJsonPredicate = Seq(
            """
              |{"op":"and","children":[
              |  {"op":"not","children":[
              |    {"op":"isNull","children":[
              |      {"op":"column","name":"part","valueType":"int"}]}]},
              |  {"op":"equal","children":[
              |    {"op":"column","name":"part","valueType":"int"},
              |    {"op":"literal","value":"2147483647","valueType":"int"}]}]}
            """.stripMargin.replaceAll("\n", "").replaceAll(" ", ""))
        )
      }
    }
  }
}
