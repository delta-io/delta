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

import java.io.File

import org.apache.spark.sql.types._

/**
 * Suite covering type widening and type changes in Delta streaming sources.
 */
class DeltaTypeWideningStreamingSourceSuite extends DeltaTypeWideningStreamingSourceTests

trait DeltaTypeWideningStreamingSourceTests
  extends DeltaStreamTest
  with DeltaTypeWideningTestMixin {

  import testImplicits._

  test("delta source - widening type change then restore back") {
    withTempDir { dir =>
      sql(s"CREATE TABLE delta.`$dir` (a byte) USING DELTA")
      val checkpointDir = new File(dir, "sink_checkpoint")

      testStream(readStreamFromPath(dir))(
        StartStream(checkpointLocation = checkpointDir.toString),
        Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (1)") },
        Execute { _ => sql(s"ALTER TABLE delta.`$dir`ALTER COLUMN a TYPE long") },
        Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (2)") },
        // Widening a column type requires restarting the stream so that the new, wider schema is
        // used to process the batch.
        ExpectSchemaChangeFailure(readSchema = "a byte", dataSchema = "a long", isRetryable = true)
      )

      val widenedSchema = new StructType()
        .add("a", LongType, nullable = true,
          metadata = typeWideningMetadata(version = 2, ByteType, LongType))

      testStream(readStreamFromPath(dir, options = Map("ignoreDeletes" -> "true")))(
        StartStream(checkpointLocation = checkpointDir.toString),
        CheckSchema(dir, widenedSchema),
        CheckAnswer(2),
        // Restore will narrow the type back. This doesn't require restarting the stream as the
        // wider schema can still be used to process the batch.
        Execute { _ => sql(s"RESTORE delta.`$dir` VERSION AS OF 1") },
        Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (3)") },
        CheckSchema(dir, StructType.fromDDL("a byte")),
        CheckAnswer(2, 3)
      )
    }
  }

  test("delta source - narrowing type change then restore back") {
    withTempDir { dir =>
      sql(s"CREATE TABLE delta.`$dir` (a decimal(14, 2)) USING DELTA")
      val checkpointDir = new File(dir, "sink_checkpoint")

      testStream(readStreamFromPath(dir, options = Map("ignoreDeletes" -> "true")))(
        StartStream(checkpointLocation = checkpointDir.toString),
        OverwriteTable(dir, StructType.fromDDL("a decimal(6, 1)")),
        Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (1.1)") },
        // Narrowing a column type doesn't require restarting the stream as the initial wider schema
        // can still be used to process the batch.
        CheckSchema(dir, StructType.fromDDL("a decimal(6, 1)")),
        CheckAnswer(1.1),
        // Restore will widen the type back. This doesn't require restarting the stream either as
        // the initial wider schema can still be used to process the batch.
        Execute { _ => sql(s"RESTORE delta.`$dir` VERSION AS OF 0") },
        Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (2.2)") },
        CheckSchema(dir, StructType.fromDDL("a decimal(14, 2)")),
        CheckAnswer(1.1, 2.2)
      )
    }
  }

  test("delta source - arbitrary type changes are not supported") {
    withTempDir { dir =>
      sql(s"CREATE TABLE delta.`$dir` (a byte) USING DELTA")
      val checkpointDir = new File(dir, "sink_checkpoint")

      testStream(readStreamFromPath(dir))(
        StartStream(checkpointLocation = checkpointDir.toString),
        Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES (1)") },
        OverwriteTable(dir, StructType.fromDDL("a string")),
        Execute { _ => sql(s"INSERT INTO delta.`$dir` VALUES ('two')") },
        ExpectSchemaChangeFailure(
          readSchema = "a byte", dataSchema = "a string", isRetryable = false)
      )

      // Try restarting the stream even though the error is not retryable and it will fail again.
      testStream(readStreamFromPath(dir))(
        StartStream(checkpointLocation = checkpointDir.toString),
        ExpectSchemaChangeFailure(
          readSchema = "a string", dataSchema = "a byte", isRetryable = false)
      )
    }
  }
}
