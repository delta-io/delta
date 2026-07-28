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

import org.apache.spark.sql.delta.DeltaTimeTravelSuite

/**
 * Runs the DSv1 [[DeltaTimeTravelSuite]] under the STRICT V2 connector.
 *
 * Table setup is rerouted to the V1 connector via [[DeltaTimeTravelSuite.setupSql]] /
 * [[DeltaTimeTravelSuite.runSetup]], so the time travel read itself is what runs under V2.
 * [[V2ForceTest]] also ensures that no test is silently executing a V1 file-source scan.
 */
class DeltaTimeTravelV2Suite extends DeltaTimeTravelSuite with V2ForceTest {

  override protected def assertNoV1Fallback: Boolean = true

  override protected def setupSql(sqlText: String): Unit = executeInV1Mode(sqlText)

  override protected def runSetup[T](f: => T): T = inV1Mode(f)

  override protected def shouldPassTests: Set[String] = Set(
    // History-manager and log-deletion internals: operate on DeltaLog directly.
    "getCommits should monotonize timestamps",
    "describe history timestamps are adjusted according to file timestamp",
    "should filter only delta files when computing earliest version",
    "resolving commits should return commit before timestamp",
    "BufferingLogDeletionIterator: iterator behavior",
    "BufferingLogDeletionIterator: early exit while handling adjusted timestamps due to timestamp",
    "BufferingLogDeletionIterator: early exit while handling adjusted timestamps due to version",
    "BufferingLogDeletionIterator: multiple adjusted timestamps",
    // Input validation that never engages the Delta connector.
    "[SPARK-45383] Time travel on a non-existing table should throw AnalysisException",
    "can't provide both version and timestamp in DataFrameReader",
    "don't time travel a valid non-delta path with @ syntax",
    // Catalog reads.
    "SPARK-41154: Correct relation caching for queries with time travel spec",
    "timestamp as of expression for table in database",
    "Dataframe-based time travel works with different timestamp precisions"
  )

  override protected def shouldFailTests: Set[String] = Set(
    // Path-based time travel not supported yet.
    "don't time travel a valid delta path with @ syntax",
    "scans on different versions of same table are executed correctly",
    // Path-based schema/partition-evolution reads.
    "time travel with schema changes - should instantiate old schema",
    "time travel with partition changes - should instantiate old schema",
    "as of timestamp in between commits should use commit before timestamp",
    "as of timestamp on exact timestamp",
    "as of timestamp on invalid timestamp",
    "as of exact timestamp after last commit should fail",
    "as of with versions",
    "time travelling with adjusted timestamps",
    // Catalog reads cross-checked against a path-based V1 oracle.
    "time travel support in SQL",
    // Write / maintenance surfaces not supported.
    "Block time travel beyond deletedFileRetention",
    "Block CDC beyond deletedFileRetention",
    "Block restore table beyond deletedFileRetention",
    "Block clone table beyond deletedFileRetention"
  )
}
