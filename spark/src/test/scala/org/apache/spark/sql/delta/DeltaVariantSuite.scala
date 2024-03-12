/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.actions.{Protocol, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.types.StructType

class DeltaVariantSuite
    extends QueryTest
    with DeltaSQLCommandTest {

  private def getProtocolForTable(table: String): Protocol = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
    deltaLog.unsafeVolatileSnapshot.protocol
  }

  test("create a new table with Variant, higher protocol and feature should be picked.") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, v VARIANT) USING DELTA")
      sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")
      // TODO(r.chen): Enable once `parse_json` is properly implemented in OSS Spark.
      // assert(spark.table("tbl").selectExpr("v::int").head == Row(99))
      assert(
        getProtocolForTable("tbl") ==
        VariantTypeTableFeature.minProtocolVersion.withFeature(VariantTypeTableFeature)
      )
    }
  }

  test("creating a table without Variant should use the usual minimum protocol") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING, i INTEGER) USING DELTA")
      assert(getProtocolForTable("tbl") == Protocol(1, 2))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("tbl"))
      assert(
        !deltaLog.unsafeVolatileSnapshot.protocol.isFeatureSupported(VariantTypeTableFeature),
        s"Table tbl contains VariantTypeFeature descriptor when its not supposed to"
      )
    }
  }

  test("add a new Variant column should upgrade to the correct protocol versions") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(s STRING) USING delta")
      assert(getProtocolForTable("tbl") == Protocol(1, 2))

      // Should throw error
      val e = intercept[SparkThrowable] {
        sql("ALTER TABLE tbl ADD COLUMN v VARIANT")
      }
      // capture the existing protocol here.
      // we will check the error message later in this test as we need to compare the
      // expected schema and protocol
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("tbl"))
      val currentProtocol = deltaLog.unsafeVolatileSnapshot.protocol
      val currentFeatures = currentProtocol.implicitlyAndExplicitlySupportedFeatures
        .map(_.name)
        .toSeq
        .sorted
        .mkString(", ")

      // add table feature
      sql(
        s"ALTER TABLE tbl " +
        s"SET TBLPROPERTIES('delta.feature.variantType-dev' = 'supported')"
      )

      sql("ALTER TABLE tbl ADD COLUMN v VARIANT")

      // check previously thrown error message
      checkError(
        e,
        errorClass = "DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT",
        parameters = Map(
          "unsupportedFeatures" -> VariantTypeTableFeature.name,
          "supportedFeatures" -> currentFeatures
        )
      )

      sql("INSERT INTO tbl (SELECT 'foo', parse_json(cast(id + 99 as string)) FROM range(1))")
      // TODO(r.chen): Enable once `parse_json` is properly implemented in OSS Spark.
      // assert(spark.table("tbl").selectExpr("v::int").head == Row(99))

      assert(
        getProtocolForTable("tbl") ==
        VariantTypeTableFeature.minProtocolVersion
          .withFeature(VariantTypeTableFeature)
          .withFeature(InvariantsTableFeature)
          .withFeature(AppendOnlyTableFeature)
      )
    }
  }
}
