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

package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.{DeltaSourceDeletionVectorTests, DeltaSourceSuiteBase, PersistentDVEnabled}

/**
 * Test suite that runs DeltaSourceDeletionVectorTests using the V2 connector.
 */
class DeltaSourceV2DeletionVectorsSuite extends DeltaSourceSuiteBase
  with DeltaSQLCommandTest
  with DeltaSourceDeletionVectorTests
  with PersistentDVEnabled
  with V2ForceTest {

  override protected def useDsv2: Boolean = true

  private lazy val shouldPassTests = Set(
    "allow to delete files before starting a streaming query",
    "allow to delete files before staring a streaming query without checkpoint",
    "multiple deletion vectors per file with initial snapshot"
  )

  private lazy val shouldFailTests = Set(
    // These tests use ignoreDeletes/ignoreChanges options not yet supported in V2
    "deleting files fails query if ignoreDeletes = false",
    "allow to delete files after staring a streaming query when ignoreFileDeletion is true",
    "allow to delete files after staring a streaming query when ignoreDeletes is true",
    "updating the source table causes failure when ignoreChanges = false - using DELETE",
    "allow to update the source table when ignoreChanges = true - using DELETE",
    "deleting files when ignoreChanges = true doesn't fail the query",
    "updating source table when ignoreDeletes = true fails the query - using DELETE",
    "subsequent DML commands are processed correctly in a batch - DELETE->DELETE - List()",
    "subsequent DML commands are processed correctly in a batch - DELETE->DELETE" +
      " - List((ignoreDeletes,true))",
    "subsequent DML commands are processed correctly in a batch - DELETE->DELETE" +
      " - List((ignoreChanges,true))",
    "subsequent DML commands are processed correctly in a batch - DELETE->DELETE" +
      " - List((skipChangeCommits,true))",
    "subsequent DML commands are processed correctly in a batch - INSERT->DELETE - List()",
    "subsequent DML commands are processed correctly in a batch - INSERT->DELETE" +
      " - List((ignoreDeletes,true))",
    "subsequent DML commands are processed correctly in a batch - INSERT->DELETE" +
      " - List((ignoreChanges,true))",
    "subsequent DML commands are processed correctly in a batch - INSERT->DELETE" +
      " - List((skipChangeCommits,true))",
    "multiple deletion vectors per file - List((ignoreFileDeletion,true))",
    "multiple deletion vectors per file - List((ignoreChanges,true))"
  )

  override protected def shouldFail(testName: String): Boolean = {
    val inPassList = shouldPassTests.contains(testName)
    val inFailList = shouldFailTests.contains(testName)

    assert(inPassList || inFailList, s"Test '$testName' not in shouldPassTests or shouldFailTests")
    assert(!(inPassList && inFailList),
      s"Test '$testName' in both shouldPassTests and shouldFailTests")

    inFailList
  }
}
