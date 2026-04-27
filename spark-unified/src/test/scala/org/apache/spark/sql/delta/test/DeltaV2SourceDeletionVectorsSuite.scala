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

import org.apache.spark.sql.delta.DeltaSourceDeletionVectorsSuite

/**
 * Test suite that runs DeltaSourceDeletionVectorsSuite using the V2 connector
 * (V2_ENABLE_MODE=STRICT).
 */
class DeltaV2SourceDeletionVectorsSuite
  extends DeltaSourceDeletionVectorsSuite with V2ForceTest {

  override protected def useDsv2: Boolean = true

  override protected def executeDml(sqlText: String): Unit = executeInV1Mode(sqlText)

  override protected def shouldPassTests: Set[String] = Set(
    "allow to delete files before starting a streaming query",
    "allow to delete files before staring a streaming query without checkpoint",
    "multiple deletion vectors per file with initial snapshot",
    "deleting files fails query if ignoreDeletes = false",
    "deleting files when ignoreChanges = true doesn't fail the query",
    "allow to delete files after staring a streaming query when ignoreDeletes is true",
    "allow to delete files after staring a streaming query when ignoreFileDeletion is true",
    "updating the source table causes failure when ignoreChanges = false - using DELETE",
    "allow to update the source table when ignoreChanges = true - using DELETE",
    "updating source table when ignoreDeletes = true fails the query - using DELETE",
    "subsequent DML commands are processed correctly in a batch - DELETE->DELETE - List()",
    "subsequent DML commands are processed correctly in a batch - DELETE->DELETE" +
      " - List((ignoreDeletes,true))",
    "subsequent DML commands are processed correctly in a batch - DELETE->DELETE" +
      " - List((skipChangeCommits,true))",
    "subsequent DML commands are processed correctly in a batch - DELETE->DELETE" +
      " - List((ignoreChanges,true))",
    "subsequent DML commands are processed correctly in a batch - INSERT->DELETE - List()",
    "subsequent DML commands are processed correctly in a batch - INSERT->DELETE" +
      " - List((ignoreDeletes,true))",
    "subsequent DML commands are processed correctly in a batch - INSERT->DELETE" +
      " - List((skipChangeCommits,true))",
    "subsequent DML commands are processed correctly in a batch - INSERT->DELETE" +
      " - List((ignoreChanges,true))",
    "multiple deletion vectors per file - List((ignoreChanges,true))",
    "multiple deletion vectors per file - List((ignoreFileDeletion,true))",
    "streaming read with nulls and deletion vectors does not NPE",
    "streaming read with variant column and deletion vectors does not ClassCastException"
  )

  override protected def shouldFailTests: Set[String] = Set.empty
}
