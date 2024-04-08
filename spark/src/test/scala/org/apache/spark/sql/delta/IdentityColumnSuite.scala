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

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.delta.GeneratedAsIdentityType.{GeneratedAlways, GeneratedAsIdentityType, GeneratedByDefault}
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._


/**
 * General test suite for identity columns.
 */
trait IdentityColumnSuiteBase extends IdentityColumnTestUtils {

  import testImplicits._
  protected val tblName = "identity_test"

  test("Don't allow IDENTITY column in the schema if the feature is disabled") {
    withSQLConf(DeltaSQLConf.DELTA_IDENTITY_COLUMN_ENABLED.key -> "false") {
      withTable(tblName) {
        val e = intercept[DeltaUnsupportedTableFeatureException] {
          createTableWithIdColAndIntValueCol(
            tblName, GeneratedByDefault, startsWith = None, incrementBy = None)
        }
        val errorMsg = e.getMessage
        assert(errorMsg.contains("requires writer table feature(s) that are unsupported"))
        assert(errorMsg.contains(IdentityColumnsTableFeature.name))
      }
    }
  }

  // Build expected schema of the following table definition for verification:
  // CREATE TABLE tableName (
  //   id BIGINT <keyword> IDENTITY (START WITH <start> INCREMENT BY <step>),
  //   value INT
  // );
  private def expectedSchema(
      generatedAsIdentityType: GeneratedAsIdentityType,
      start: Long = IdentityColumn.defaultStart,
      step: Long = IdentityColumn.defaultStep): StructType = {
    val colFields = new ListBuffer[StructField]

    val allowExplicitInsert = generatedAsIdentityType == GeneratedByDefault
    val builder = new MetadataBuilder()
    builder.putBoolean(DeltaSourceUtils.IDENTITY_INFO_ALLOW_EXPLICIT_INSERT,
      allowExplicitInsert)
    builder.putLong(DeltaSourceUtils.IDENTITY_INFO_START, start)
    builder.putLong(DeltaSourceUtils.IDENTITY_INFO_STEP, step)
    colFields += StructField("id", LongType, true, builder.build())

    colFields += StructField("value", IntegerType)

    StructType(colFields)
  }

  test("various configuration") {
    val starts = Seq(
      Long.MinValue,
      Integer.MIN_VALUE.toLong,
      -100L,
      0L,
      1000L,
      Integer.MAX_VALUE.toLong,
      Long.MaxValue
    )
    val steps = Seq(
      Long.MinValue,
      Integer.MIN_VALUE.toLong,
      -100L,
      1000L,
      Integer.MAX_VALUE.toLong,
      Long.MaxValue
    )
    withSQLConf(DeltaSQLConf.DELTA_IDENTITY_COLUMN_ENABLED.key -> "true") {
      for {
        generatedAsIdentityType <- GeneratedAsIdentityType.values
        startsWith <- starts
        incrementBy <- steps
      } {
        withTable(tblName) {
          createTableWithIdColAndIntValueCol(
            tblName, generatedAsIdentityType, Some(startsWith), Some(incrementBy))
          val table = DeltaLog.forTable(spark, TableIdentifier(tblName))
          val actualSchema =
            DeltaColumnMapping.dropColumnMappingMetadata(table.snapshot.metadata.schema)
          assert(actualSchema === expectedSchema(generatedAsIdentityType, startsWith, incrementBy))
        }
      }
    }
  }

  test("default configuration") {
    withSQLConf(DeltaSQLConf.DELTA_IDENTITY_COLUMN_ENABLED.key -> "true") {
      for {
        generatedAsIdentityType <- GeneratedAsIdentityType.values
        startsWith <- Seq(Some(1L), None)
        incrementBy <- Seq(Some(1L), None)
      } {
        withTable(tblName) {
          createTableWithIdColAndIntValueCol(
            tblName, generatedAsIdentityType, startsWith, incrementBy)
          val table = DeltaLog.forTable(spark, TableIdentifier(tblName))
          val actualSchema =
            DeltaColumnMapping.dropColumnMappingMetadata(table.snapshot.metadata.schema)
          assert(actualSchema === expectedSchema(generatedAsIdentityType))
        }
      }
    }
  }

}

class IdentityColumnScalaSuite
  extends IdentityColumnSuiteBase
  with ScalaDDLTestUtils

class IdentityColumnScalaIdColumnMappingSuite
  extends IdentityColumnSuiteBase
  with ScalaDDLTestUtils
  with DeltaColumnMappingEnableIdMode

class IdentityColumnScalaNameColumnMappingSuite
  extends IdentityColumnSuiteBase
  with ScalaDDLTestUtils
  with DeltaColumnMappingEnableNameMode
