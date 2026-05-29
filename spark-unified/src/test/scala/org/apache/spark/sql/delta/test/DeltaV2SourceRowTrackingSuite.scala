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

package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.DeltaSourceRowTrackingSuiteBase

/**
 * Runs DeltaSourceRowTrackingStreamingSuite through the DSv2 connector
 * (V2_ENABLE_MODE=STRICT, routes via DeltaCatalog -> DeltaTableV2).
 */
class DeltaV2SourceRowTrackingSuite
  extends DeltaSourceRowTrackingSuiteBase
  with V2ForceTest {

  override protected def useDsv2: Boolean = true

  override protected lazy val shouldPassTests: Set[String] = Set(
    "CDC stream on row-tracking table works when _metadata not selected"
  )

  override protected lazy val shouldFailTests: Set[String] = Set(
    // TODO: Requires Spark PR apache/spark#56133, which adds pruneColumns() to
    // MicroBatchExecution for SupportsPushDownRequiredColumns sources. Without it
    // _metadata is not added to requiredDataSchema, causing AIOOBE at executor
    // runtime when codegen reads position N from an N-column batch.
    "_metadata.row_id projection in streaming matches batch",
    "_metadata.row_commit_version projection in streaming matches batch",
    "_metadata.row_id with partition column in middle of DDL schema",
    "_metadata.row_id with column mapping name mode",
    "_metadata.row_id with partition and column mapping",

    // TODO: V2ForceTest STRICT mode routes DELETE through the DSv2 write path which
    // does not support deletes. The DELETE needed to create deletion vectors fails with
    // "Table does not support deletes". Rewrite to create DVs via executeInV1Mode.
    "_metadata.row_id preserved through deletion vector filtering",

    // TODO: V2 kernel rejects INSERT to a row-tracking table without 'numRecords'
    // statistics (KernelException). The CREATE TABLE + INSERT setup fails before the
    // streaming assertion is reached. Fix by wrapping the INSERT in executeInV1Mode.
    "CDC stream on row-tracking column-mapped table rejects _metadata.row_id"
  )
}
