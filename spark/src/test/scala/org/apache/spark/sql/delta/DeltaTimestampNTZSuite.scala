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

import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DeltaTimestampNTZSuite extends QueryTest
  with SharedSparkSession  with DeltaSQLCommandTest {

  private def getProtocolForTable(table: String): Protocol = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
    deltaLog.unsafeVolatileSnapshot.protocol
  }

  test("create a new table with TIMESTAMP_NTZ, higher protocol and feature should be picked.") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(c1 STRING, c2 TIMESTAMP, c3 TIMESTAMP_NTZ) USING DELTA")
      sql(
        """INSERT INTO tbl VALUES
          |('foo','2022-01-02 03:04:05.123456','2022-01-02 03:04:05.123456')""".stripMargin)
      assert(spark.table("tbl").head == Row(
        "foo",
        new Timestamp(2022 - 1900, 0, 2, 3, 4, 5, 123456000),
        LocalDateTime.of(2022, 1, 2, 3, 4, 5, 123456000)))
      assert(getProtocolForTable("tbl") ==
        TimestampNTZTableFeature.minProtocolVersion.withFeature(TimestampNTZTableFeature)
      )
    }
  }

  test("creating a table without TIMESTAMP_NTZ should use the usual minimum protocol") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(c1 STRING, c2 TIMESTAMP, c3 TIMESTAMP) USING DELTA")
      assert(getProtocolForTable("tbl") == Protocol(1, 2))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("tbl"))
      assert(
        !deltaLog.unsafeVolatileSnapshot.protocol.isFeatureSupported(TimestampNTZTableFeature),
        s"Table tbl contains TimestampNTZFeature descriptor when its not supposed to"
      )
    }
  }

  test("add a new column using TIMESTAMP_NTZ should upgrade to the correct protocol versions") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(c1 STRING, c2 TIMESTAMP) USING delta")
      assert(getProtocolForTable("tbl") == Protocol(1, 2))

      // Should throw error
      val e = intercept[SparkThrowable] {
        sql("ALTER TABLE tbl ADD COLUMN c3 TIMESTAMP_NTZ")
      }

      // add table feature
      sql(s"ALTER TABLE tbl " +
          s"SET TBLPROPERTIES('delta.feature.timestampNtz' = 'supported')")

      sql("ALTER TABLE tbl ADD COLUMN c3 TIMESTAMP_NTZ")


      sql(
        """INSERT INTO tbl VALUES
          |('foo','2022-01-02 03:04:05.123456','2022-01-02 03:04:05.123456')""".stripMargin)
      assert(spark.table("tbl").head == Row(
        "foo",
        new Timestamp(2022 - 1900, 0, 2, 3, 4, 5, 123456000),
        LocalDateTime.of(2022, 1, 2, 3, 4, 5, 123456000)))

      assert(getProtocolForTable("tbl") ==
        TimestampNTZTableFeature.minProtocolVersion
          .withFeature(TimestampNTZTableFeature)
          .withFeature(InvariantsTableFeature)
          .withFeature(AppendOnlyTableFeature)
      )
    }
  }

  test("use TIMESTAMP_NTZ in a partition column") {
    withTable("delta_test") {
      sql(
        """CREATE TABLE delta_test(c1 STRING, c2 TIMESTAMP, c3 TIMESTAMP_NTZ)
          |USING delta
          |PARTITIONED BY (c3)""".stripMargin)
      sql(
        """INSERT INTO delta_test VALUES
          |('foo','2022-01-02 03:04:05.123456','2022-01-02 03:04:05.123456')""".stripMargin)
      assert(spark.table("delta_test").head == Row(
        "foo",
        new Timestamp(2022 - 1900, 0, 2, 3, 4, 5, 123456000),
        LocalDateTime.of(2022, 1, 2, 3, 4, 5, 123456000)))
      assert(getProtocolForTable("delta_test") ==
        TimestampNTZTableFeature.minProtocolVersion.withFeature(TimestampNTZTableFeature)
      )
    }
  }

  test("min/max stats collection should not apply on TIMESTAMP_NTZ") {
    withTable("delta_test") {
      sql("CREATE TABLE delta_test(c1 STRING, c2 TIMESTAMP, c3 TIMESTAMP_NTZ) USING delta")
      val statsSchema = DeltaLog.forTable(spark, TableIdentifier("delta_test")).snapshot.statsSchema
      assert(statsSchema("minValues").dataType == StructType.fromDDL("c1 STRING, c2 TIMESTAMP"))
      assert(statsSchema("maxValues").dataType == StructType.fromDDL("c1 STRING, c2 TIMESTAMP"))
    }
  }
}
