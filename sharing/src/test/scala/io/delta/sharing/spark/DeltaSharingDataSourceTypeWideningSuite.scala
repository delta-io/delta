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
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.delta.sharing.DeltaSharingTestSparkUtils
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
      expectedSchema: StructType,
      expectedResult: DataFrame): Unit = {
    withTempDir { tempDir =>
      val sharedTableName =
        if (responseFormat == DeltaSharingOptions.RESPONSE_FORMAT_DELTA) {
          "type_widening_shared_delta_table"
        } else {
          "type_widening_shared_parquet_table"
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

      withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
        val profileFile = prepareProfileFile(tempDir)
        val result = reader.load(s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
        assert(result.schema === expectedSchema)
        checkAnswer(result, expectedResult)
      }
    }
  }

  /** Creates a table and applies a type change to it. */
  private def withTestTable(testBody: String => Unit): Unit = {
    val deltaTableName = "type_widening_table"
    withTable(deltaTableName) {
      sql(s"CREATE TABLE $deltaTableName (value SMALLINT) USING DELTA")
      sql(s"INSERT INTO $deltaTableName VALUES (1), (2)")
      sql(s"ALTER TABLE $deltaTableName CHANGE COLUMN value TYPE INT")
      sql(s"INSERT INTO $deltaTableName VALUES (3), (${Int.MaxValue})")
      sql(s"INSERT INTO $deltaTableName VALUES (4), (5)")
      testBody(deltaTableName)
    }
  }

  for (responseFormat <- Seq(
    DeltaSharingOptions.RESPONSE_FORMAT_DELTA,
    DeltaSharingOptions.RESPONSE_FORMAT_PARQUET
  )) {
    // Type widening metadata for column `value` for the test table above.
    // The server strips the metadata in Parquet format sharing, this is ok since it's not used on
    // the read path anyway.
    val typeWideningMetadata: Metadata =
      if (responseFormat == DeltaSharingOptions.RESPONSE_FORMAT_DELTA) {
        new MetadataBuilder()
          .putMetadataArray(
            "delta.typeChanges", Array(
              new MetadataBuilder()
                .putLong("tableVersion", 2)
                .putString("fromType", "short")
                .putString("toType", "integer")
                .build()))
          .build()
      } else {
        Metadata.empty
      }

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
          responseFormat,
          expectedSchema = new StructType()
            .add("value", IntegerType, nullable = true, metadata = typeWideningMetadata),
          expectedResult = Seq(1, 2, 3, Int.MaxValue).toDF("value"))

        testReadingDeltaShare(
          tableName,
          versionAsOf = Some(2),
          responseFormat,
          expectedSchema = new StructType()
            .add("value", IntegerType, nullable = true, metadata = typeWideningMetadata),
          expectedResult = Seq(1, 2).toDF("value"))

        testReadingDeltaShare(
          tableName,
          versionAsOf = Some(1),
          responseFormat,
          expectedSchema = new StructType()
            .add("value", ShortType),
          expectedResult = Seq(1, 2).toDF("value"))
      }
    }
  }
}
