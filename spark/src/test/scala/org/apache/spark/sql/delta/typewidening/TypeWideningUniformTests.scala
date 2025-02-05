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

package org.apache.spark.sql.delta.typewidening

import org.apache.spark.sql.delta.{DeltaInsertIntoTest, DeltaUnsupportedOperationException, IcebergCompat, IcebergCompatV1, IcebergCompatV2}
import org.apache.spark.sql.delta.DeltaErrors.toSQLType
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.scalatest.GivenWhenThen

import org.apache.spark.sql.{QueryTest, SaveMode}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, DateType, DecimalType}

/** Trait collecting tests covering type widening + Uniform Iceberg compatibility. */
trait TypeWideningUniformTests extends QueryTest
  with TypeWideningTestMixin
  with TypeWideningTestCases
  with DeltaInsertIntoTest
  with GivenWhenThen {

  // Iceberg supports all base type changes eligible for widening during schema evolution except
  // for date -> timestampNtz and decimal scale changes.
  private val icebergSupportedTestCases = supportedTestCases.filter {
    case SupportedTypeEvolutionTestCase(_ : DateType, _, _, _) => false
    case SupportedTypeEvolutionTestCase(from: DecimalType, to: DecimalType, _, _) =>
      from.scale == to.scale
    case _ => true
  }

  // Unsupported type changes are all base changes that aren't supported above and all changes that
  // are not eligible for schema evolution: int -> double, int -> decimal
  private val icebergUnsupportedTestCases =
    supportedTestCases.diff(icebergSupportedTestCases) ++ alterTableOnlySupportedTestCases

  /** Helper to enable Uniform with Iceberg compatibility on the given table. */
  private def enableIcebergUniform(tableName: String, compat: IcebergCompat): Unit =
    sql(
      s"""
         |ALTER TABLE $tableName SET TBLPROPERTIES (
         |  'delta.columnMapping.mode' = 'name',
         |  '${compat.config.key}' = 'true',
         |  'delta.universalFormat.enabledFormats' = 'iceberg'
         |)
       """.stripMargin)

  /** Helper to check that the given function violates Uniform compatibility with type widening. */
  private def checkIcebergCompatViolation(
      compat: IcebergCompat,
      fromType: DataType,
      toType: DataType)(f: => Unit): Unit = {
    Given(s"iceberg compat ${compat.getClass.getSimpleName}")
    checkError(
      exception = intercept[DeltaUnsupportedOperationException] {
        f
      },
      "DELTA_ICEBERG_COMPAT_VIOLATION.UNSUPPORTED_TYPE_WIDENING",
      parameters = Map(
        "version" -> compat.version.toString,
        "prevType" -> toSQLType(fromType),
        "newType" -> toSQLType(toType),
        "fieldPath" -> "a"
      )
    )
  }

  test("apply supported type change then enable uniform") {
    for (testCase <- icebergSupportedTestCases) {
      Given(s"changing ${testCase.fromType.sql} -> ${testCase.toType.sql}")
      val tableName = "type_widening_uniform_supported_table"
      withTable(tableName) {
        sql(s"CREATE TABLE $tableName (a ${testCase.fromType.sql}) USING DELTA")
        sql(s"ALTER TABLE $tableName CHANGE COLUMN a TYPE ${testCase.toType.sql}")
        enableIcebergUniform(tableName, IcebergCompatV2)
      }
    }
  }

  test("apply unsupported type change then enable uniform") {
    for (testCase <- icebergUnsupportedTestCases) {
      val tableName = "type_widening_uniform_unsupported_table"
      withTable(tableName) {
        Given(s"changing ${testCase.fromType.sql} -> ${testCase.toType.sql}")
        sql(s"CREATE TABLE $tableName (a ${testCase.fromType.sql}) USING DELTA")
        sql(s"ALTER TABLE $tableName CHANGE COLUMN a TYPE ${testCase.toType.sql}")
        checkIcebergCompatViolation(IcebergCompatV1, testCase.fromType, testCase.toType) {
          enableIcebergUniform(tableName, IcebergCompatV1)
        }
        checkIcebergCompatViolation(IcebergCompatV2, testCase.fromType, testCase.toType) {
          enableIcebergUniform(tableName, IcebergCompatV2)
        }
      }
    }
  }

  test("enable uniform then apply supported type change - ALTER TABLE") {
    for (testCase <- icebergSupportedTestCases) {
      Given(s"changing ${testCase.fromType.sql} -> ${testCase.toType.sql}")
      val tableName = "type_widening_uniform_manual_supported_table"
      withTable(tableName) {
        sql(s"CREATE TABLE $tableName (a ${testCase.fromType.sql}) USING DELTA")
        enableIcebergUniform(tableName, IcebergCompatV2)
        sql(s"ALTER TABLE $tableName CHANGE COLUMN a TYPE ${testCase.toType.sql}")
      }
    }
  }

  test("enable uniform then apply unsupported type change - ALTER TABLE") {
    for (testCase <- icebergUnsupportedTestCases) {
      val tableName = "type_widening_uniform_manual_unsupported_table"
      withTable(tableName) {
        Given(s"changing ${testCase.fromType.sql} -> ${testCase.toType.sql}")
        sql(s"CREATE TABLE $tableName (a ${testCase.fromType.sql}) USING DELTA")
        enableIcebergUniform(tableName, IcebergCompatV2)
        checkIcebergCompatViolation(IcebergCompatV2, testCase.fromType, testCase.toType) {
          sql(s"ALTER TABLE $tableName CHANGE COLUMN a TYPE ${testCase.toType.sql}")
        }
      }
    }
  }


  test("enable uniform then apply supported type change - MERGE") {
    for (testCase <- icebergSupportedTestCases) {
      Given(s"changing ${testCase.fromType.sql} -> ${testCase.toType.sql}")
      withTable("source", "target") {
        testCase.initialValuesDF.write.format("delta").saveAsTable("target")
        testCase.additionalValuesDF.write.format("delta").saveAsTable("source")
        enableIcebergUniform("target", IcebergCompatV2)
        withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
          sql(
            s"""
               |MERGE INTO target
               |USING source
               |ON 0 = 1
               |WHEN NOT MATCHED THEN INSERT *
           """.stripMargin)
        }
        val result = sql(s"SELECT * FROM target")
        assert(result.schema("value").dataType === testCase.toType)
        checkAnswer(result, testCase.expectedResult)
      }
    }
  }

  test("enable uniform then apply unsupported type change - MERGE") {
    for (testCase <- icebergUnsupportedTestCases) {
      Given(s"changing ${testCase.fromType.sql} -> ${testCase.toType.sql}")
      withTable("source", "target") {
        testCase.initialValuesDF.write.format("delta").saveAsTable("target")
        // Here we use a source for MERGE that contains the same data that is already present in the
        // target, except that it uses a wider type. Since Uniform is enabled and Iceberg doesn't
        // support the given type change, we will keep the existing narrower type and downcast
        // values. `testCase.additionalValueDF` contains values that would overflow, which would
        // just fail, hence why we use `testCase.initialValueDF` instead.
        testCase.initialValuesDF
          .select(col("value").cast(testCase.toType))
          .write
          .format("delta")
          .saveAsTable("source")
        enableIcebergUniform("target", IcebergCompatV2)
        withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
          sql(
            s"""
               |MERGE INTO target
               |USING source
               |ON 0 = 1
               |WHEN NOT MATCHED THEN INSERT *
          """.stripMargin)
        }
        val result = sql(s"SELECT * FROM target")
        val expected = testCase.initialValuesDF.union(testCase.initialValuesDF)
        assert(result.schema("value").dataType === testCase.fromType)
        checkAnswer(result, expected)
      }
    }
  }


  for (insert <- Set(
      // Cover only a subset of all INSERTs. There's little value in testing all of them and it
      // quickly gets expensive.
      SQLInsertByPosition(SaveMode.Append),
      SQLInsertByName(SaveMode.Append),
      DFv1InsertInto(SaveMode.Append),
      StreamingInsert)) {
    test(s"enable uniform then apply supported type change - ${insert.name}") {
      for (testCase <- icebergSupportedTestCases) {
        Given(s"changing ${testCase.fromType.sql} -> ${testCase.toType.sql}")
        withTable("source", "target") {
          testCase.initialValuesDF.write.format("delta").saveAsTable("target")
          testCase.additionalValuesDF.write.format("delta").saveAsTable("source")
          enableIcebergUniform("target", IcebergCompatV2)
          withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
            insert.runInsert(columns = Seq("value"), whereCol = "value", whereValue = 1)
          }
          val result = sql(s"SELECT * FROM target")
          assert(result.schema("value").dataType === testCase.toType)
          checkAnswer(result, testCase.expectedResult)
        }
      }
    }

    test(s"enable uniform then apply unsupported type change - ${insert.name}") {
      for (testCase <- icebergUnsupportedTestCases) {
        Given(s"changing ${testCase.fromType.sql} -> ${testCase.toType.sql}")
        withTable("source", "target") {
          testCase.initialValuesDF.write.format("delta").saveAsTable("target")
          // Here we use a source for INSERT that contains the same data that is already present in
          // the target, except that it uses a wider type. Since Uniform is enabled and Iceberg
          // doesn't support the given type change, we will keep the existing narrower type and
          // downcast values. `testCase.additionalValueDF` contains values that would overflow,
          // which would just fail, hence why we use `testCase.initialValueDF` instead.
          testCase.initialValuesDF
            .select(col("value").cast(testCase.toType))
            .write
            .format("delta")
            .saveAsTable("source")
          enableIcebergUniform("target", IcebergCompatV2)
          withSQLConf(
            DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true",
            DeltaSQLConf.DELTA_STREAMING_SINK_ALLOW_IMPLICIT_CASTS.key -> "true"
          ) {
            insert.runInsert(columns = Seq("value"), whereCol = "value", whereValue = 1)
          }
          val result = sql(s"SELECT * FROM target")
          val expected = testCase.initialValuesDF.union(testCase.initialValuesDF)
          assert(result.schema("value").dataType === testCase.fromType)
          checkAnswer(result, expected)
        }
      }
    }
  }
}
