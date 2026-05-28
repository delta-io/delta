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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.StreamTest

/**
 * Streaming regression tests for row-tracking metadata projection.
 *
 * Each test verifies that _metadata.row_id / _metadata.row_commit_version can be projected
 * in a streaming query, covering the interaction with DV filtering, partition columns in the
 * middle of the DDL schema, and column mapping.
 *
 * The base suite runs through the DSv1 connector; DeltaV2SourceRowTrackingStreamingSuite
 * re-runs the same tests through the DSv2 connector via V2ForceTest.
 */
trait DeltaSourceRowTrackingStreamingSuiteBase extends StreamTest
  with DeltaSourceSuiteBase
  with DeltaColumnMappingTestUtils {

  import testImplicits._

  test("_metadata.row_id projection in streaming matches batch") {
    withTempDir { inputDir =>
      val path = inputDir.getCanonicalPath
      // Single-file writes so row_id assignment is deterministic: rows in insertion order.
      Seq((1L, "Alice"), (2L, "Bob")).toDF("id", "name")
        .coalesce(1)
        .write.format("delta")
        .option(DeltaConfigs.ROW_TRACKING_ENABLED.key, "true")
        .save(path)
      Seq((3L, "Charlie")).toDF("id", "name")
        .coalesce(1)
        .write.format("delta").mode("append").save(path)

      val df = loadStreamWithOptions(path, Map.empty)
        .selectExpr("id", "_metadata.row_id as row_id")

      // First commit -> row_ids 0, 1; second commit -> row_id 2.
      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer((1L, 0L), (2L, 1L), (3L, 2L))
      )
    }
  }

  test("_metadata.row_commit_version projection in streaming matches batch") {
    withTempDir { inputDir =>
      val path = inputDir.getCanonicalPath
      Seq((1L, "Alice"), (2L, "Bob")).toDF("id", "name")
        .write.format("delta")
        .option(DeltaConfigs.ROW_TRACKING_ENABLED.key, "true")
        .save(path)
      Seq((3L, "Charlie")).toDF("id", "name")
        .write.format("delta").mode("append").save(path)

      val df = loadStreamWithOptions(path, Map.empty)
        .selectExpr("id", "_metadata.row_commit_version as rcv")

      // Version 0 = first INSERT, version 1 = second INSERT.
      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer((1L, 0L), (2L, 0L), (3L, 1L))
      )
    }
  }

  test("_metadata.row_id preserved through deletion vector filtering") {
    withTempDir { inputDir =>
      val path = inputDir.getCanonicalPath
      // 1000 rows in a single file, inserted in order -> row_id[i] == id[i].
      spark.range(1000)
        .selectExpr("id", "cast(id as string) as name")
        .coalesce(1)
        .write.format("delta")
        .option(DeltaConfigs.ROW_TRACKING_ENABLED.key, "true")
        .option(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key, "true")
        .save(path)
      // Low-selectivity predicate -> DV path instead of file rewrite.
      sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0")

      val df = loadStreamWithOptions(path, Map.empty)
        .selectExpr("id", "_metadata.row_id as row_id")

      // Survivors: odd ids 1..999, each with row_id == id.
      val expected = (1 to 999 by 2).map(i => Row(i.toLong, i.toLong))
      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer(expected: _*)
      )
    }
  }

  test("_metadata.row_id with partition column in middle of DDL schema") {
    withTempDir { inputDir =>
      val path = inputDir.getCanonicalPath
      // DDL order: id, part (partition), name.  part sits in position 1.
      Seq((1L, "a", "Alice"), (2L, "b", "Bob")).toDF("id", "part", "name")
        .write.format("delta")
        .option(DeltaConfigs.ROW_TRACKING_ENABLED.key, "true")
        .partitionBy("part")
        .save(path)

      val df = loadStreamWithOptions(path, Map.empty)
        .selectExpr("id", "part", "_metadata.row_id as row_id")

      // Each partition is a separate file; row_ids are per-file (0 within each partition).
      // Both rows will have row_id = 0 since each is alone in its partition file.
      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer((1L, "a", 0L), (2L, "b", 0L))
      )
    }
  }

  test("_metadata.row_id with column mapping name mode") {
    withTempDir { inputDir =>
      val path = inputDir.getCanonicalPath
      Seq((1L, "Alice"), (2L, "Bob")).toDF("id", "user_name")
        .coalesce(1)
        .write.format("delta")
        .option(DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")
        .option(DeltaConfigs.ROW_TRACKING_ENABLED.key, "true")
        .save(path)

      val df = loadStreamWithOptions(path, Map.empty)
        .selectExpr("id", "user_name", "_metadata.row_id as row_id")

      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer((1L, "Alice", 0L), (2L, "Bob", 1L))
      )
    }
  }

  test("_metadata.row_id with partition and column mapping") {
    withTempDir { inputDir =>
      val path = inputDir.getCanonicalPath
      // DDL order: id, region (partition), score - partition in position 1.
      Seq((1L, "eu", 9.5), (2L, "us", 8.0), (3L, "eu", 7.5))
        .toDF("id", "region", "score")
        .write.format("delta")
        .option(DeltaConfigs.COLUMN_MAPPING_MODE.key, "name")
        .option(DeltaConfigs.ROW_TRACKING_ENABLED.key, "true")
        .partitionBy("region")
        .save(path)

      val df = loadStreamWithOptions(path, Map.empty)
        .selectExpr("id", "region", "score", "_metadata.row_id as row_id")

      // Within each partition: eu has ids 1, 3 (row_ids 0, 1); us has id 2 (row_id 0).
      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer(
          (1L, "eu", 9.5, 0L),
          (2L, "us", 8.0, 0L),
          (3L, "eu", 7.5, 1L)
        )
      )
    }
  }
}

class DeltaSourceRowTrackingStreamingSuite
  extends DeltaSourceRowTrackingStreamingSuiteBase
  with DeltaSQLCommandTest
