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

package org.apache.spark.sql.delta.util

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.parquet.schema.MessageTypeParser

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BinaryType, StringType}

/**
 * Single-JVM guard for [[DeltaFileOperations.buildParquetToSparkSchemaConverter]]: it must work
 * with no active or default `SparkSession` (the condition on executors). The end-to-end
 * distributed reproduction lives in `ConvertToDeltaDistributedSuite`.
 */
class DeltaFileOperationsParquetConverterSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  /** Run `f` with no active or default SparkSession, restoring them afterwards. */
  private def withoutAnyActiveSession[T](f: => T): T = {
    val prevActive = SparkSession.getActiveSession
    val prevDefault = SparkSession.getDefaultSession
    try {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      f
    } finally {
      prevActive.foreach(SparkSession.setActiveSession)
      prevDefault.foreach(SparkSession.setDefaultSession)
    }
  }

  test("buildParquetToSparkSchemaConverter works without an active SparkSession") {
    // A plain (unannotated) parquet binary: assumeBinaryIsString decides String vs Binary.
    val parquetSchema = MessageTypeParser.parseMessageType(
      "message root { required binary b; }")

    withoutAnyActiveSession {
      val asString = DeltaFileOperations.buildParquetToSparkSchemaConverter(
        Map(SQLConf.PARQUET_BINARY_AS_STRING.key -> "true"))
      assert(asString.convert(parquetSchema).head.dataType === StringType)

      val asBinary = DeltaFileOperations.buildParquetToSparkSchemaConverter(
        Map(SQLConf.PARQUET_BINARY_AS_STRING.key -> "false"))
      assert(asBinary.convert(parquetSchema).head.dataType === BinaryType)
    }
  }
}
