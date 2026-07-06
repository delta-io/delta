/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.streaming.StreamTest

/**
 * Streaming regression tests for row-tracking metadata and CDC combination scenarios.
 *
 * The base suite runs through the DSv1 connector; DeltaV2SourceRowTrackingSuite
 * re-runs the same tests through the DSv2 connector via V2ForceTest.
 */
trait DeltaSourceRowTrackingSuiteBase extends StreamTest
  with DeltaSourceSuiteBase
  with DeltaColumnMappingTestUtils {

  import testImplicits._

  test("_metadata.row_id is not available in streaming") {
    // DSv1 streaming (DeltaSource / StreamingRelation) does not expose _metadata.row_id;
    // the relation schema only contains user data columns.
    withTempDir { inputDir =>
      val path = inputDir.getCanonicalPath
      Seq((1L, "Alice"), (2L, "Bob")).toDF("id", "name")
        .write.format("delta")
        .option(DeltaConfigs.ROW_TRACKING_ENABLED.key, "true")
        .save(path)

      val ex = intercept[Exception] {
        val df = loadStreamWithOptions(path, Map.empty)
          .selectExpr("id", "_metadata.row_id as row_id")
        testStream(df)(ProcessAllAvailable())
      }
      assert(
        ex.getMessage.toLowerCase(Locale.ROOT).contains("row_id"),
        s"Expected error about row_id not available in streaming, got: ${ex.getMessage}"
      )
    }
  }

  test("CDC stream on row-tracking table works when _metadata not selected") {
    // The protocol prohibition fires only when row-tracking metadata is actually requested.
    // A CDC stream that reads only data columns must work even if the table has row tracking.
    withTempDir { inputDir =>
      val path = inputDir.getCanonicalPath
      Seq((1L, "Alice"), (2L, "Bob")).toDF("id", "name")
        .write.format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .option("delta.enableRowTracking", "true")
        .save(path)

      val df = loadStreamWithOptions(
            path, Map("readChangeFeed" -> "true", "startingVersion" -> "0"))
        .select("id", "name")

      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer((1L, "Alice"), (2L, "Bob"))
      )
    }
  }

  test("CDC stream on row-tracking column-mapped table rejects _metadata.row_id") {
    // The RT-protocol rejection fires before column-mapping translation; column mapping
    // being enabled must not bypass the guard.
    withTempDir { inputDir =>
      val path = inputDir.getCanonicalPath
      // Enabling CM + RT via df.write triggers DELTA_BLOCK_COLUMN_MAPPING_AND_CDC_OPERATION
      // when CDC is also set (CM setup is treated as a rename). Use DDL + INSERT instead.
      sql(s"CREATE TABLE delta.`$path` (id LONG, user_name STRING) USING delta " +
        s"TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'name', " +
        s"'delta.enableChangeDataFeed' = 'true', " +
        s"'${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = 'true')")
      Seq((1L, "Alice"), (2L, "Bob")).toDF("id", "user_name")
        .write.format("delta").mode("append").save(path)

      val ex = intercept[Exception] {
        val df = loadStreamWithOptions(
              path, Map("readChangeFeed" -> "true", "startingVersion" -> "0"))
          .selectExpr("id", "_metadata.row_id")
        testStream(df)(ProcessAllAvailable())
      }
      assert(
        ex.getMessage.toLowerCase(Locale.ROOT).contains("row_id"),
        s"Expected error mentioning row_id under CDC + CM, got: ${ex.getMessage}"
      )
    }
  }
}

class DeltaSourceRowTrackingSuite
  extends DeltaSourceRowTrackingSuiteBase
  with DeltaSQLCommandTest
