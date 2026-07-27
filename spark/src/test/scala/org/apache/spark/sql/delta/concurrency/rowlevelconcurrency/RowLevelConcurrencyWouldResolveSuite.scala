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

package org.apache.spark.sql.delta.concurrency.rowlevelconcurrency

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.RowLevelConcurrency.SkipReason
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for [[RowLevelConcurrency.wouldResolve]] -- the detect-only
 * action-shape analysis used by the conflict-checker phase. These tests
 * exercise only synthetic action sequences and do not require a SparkSession or a
 * Delta table.
 */
class RowLevelConcurrencyWouldResolveSuite extends SparkFunSuite {

  // ---------- helpers ----------

  private def addFile(path: String): AddFile =
    AddFile(
      path = path,
      partitionValues = Map.empty,
      size = 1L,
      modificationTime = 1L,
      dataChange = true,
      stats = "{\"numRecords\": 100}")

  private def removeFile(path: String): RemoveFile =
    RemoveFile(
      path = path,
      deletionTimestamp = Some(1L),
      dataChange = true)

  // ---------- candidate detection ----------

  test("wouldResolve: single DV-vs-DV same-path candidate is detected") {
    val path = "f1.parquet"

    val loserActions: Seq[Action] = Seq(removeFile(path), addFile(path))
    val winnerAdded = Seq(addFile(path))
    val winnerRemoved = Seq(removeFile(path))

    val summary = RowLevelConcurrency.wouldResolve(
      loserActions, winnerAdded, winnerRemoved)

    assert(summary.candidateFileCount == 1)
    assert(summary.candidatePaths == Seq(path))
    assert(summary.skipReasons.isEmpty)
  }

  test("wouldResolve: multiple disjoint same-path candidates are detected") {
    val loserActions: Seq[Action] = (1 to 5).flatMap { i =>
      Seq(removeFile(s"f$i"), addFile(s"f$i"))
    }
    val winnerAdded = (1 to 5).map { i => addFile(s"f$i") }
    val winnerRemoved = (1 to 5).map { i => removeFile(s"f$i") }

    val summary = RowLevelConcurrency.wouldResolve(
      loserActions, winnerAdded, winnerRemoved)

    assert(summary.candidateFileCount == 5)
    assert(summary.candidatePaths.size == 5)
    assert(summary.skipReasons.isEmpty)
  }

  // ---------- skip reasons ----------

  test("wouldResolve: winner is CoW (path rewritten) -- WinnerCowOrCompacted") {
    val loserActions: Seq[Action] = Seq(removeFile("f1"), addFile("f1"))
    // Winner removed f1 but added a different path (CoW shape).
    val winnerAdded = Seq(addFile("f1_rewritten"))
    val winnerRemoved = Seq(removeFile("f1"))

    val summary = RowLevelConcurrency.wouldResolve(
      loserActions, winnerAdded, winnerRemoved)

    assert(summary.candidateFileCount == 0)
    assert(summary.skipReasons(SkipReason.WinnerCowOrCompacted) == 1)
  }

  test("wouldResolve: winner produced multiple AddFiles for same path -- WinnerNotDvOnly") {
    val loserActions: Seq[Action] = Seq(removeFile("f1"), addFile("f1"))
    // Pathological: winner produced two AddFiles for the same path.
    val winnerAdded = Seq(addFile("f1"), addFile("f1"))
    val winnerRemoved = Seq(removeFile("f1"))

    val summary = RowLevelConcurrency.wouldResolve(
      loserActions, winnerAdded, winnerRemoved)

    assert(summary.candidateFileCount == 0)
    assert(summary.skipReasons(SkipReason.WinnerNotDvOnly) == 1)
  }

  test("wouldResolve: loser removed path but did not re-add it -- LoserNoSamePathAdd") {
    val loserActions: Seq[Action] = Seq(removeFile("f1"))  // No matching AddFile
    val winnerAdded = Seq(addFile("f1"))
    val winnerRemoved = Seq(removeFile("f1"))

    val summary = RowLevelConcurrency.wouldResolve(
      loserActions, winnerAdded, winnerRemoved)

    assert(summary.candidateFileCount == 0)
    assert(summary.skipReasons(SkipReason.LoserNoSamePathAdd) == 1)
  }

  test("wouldResolve: no shared paths returns empty summary") {
    val loserActions: Seq[Action] = Seq(removeFile("f1"), addFile("f1"))
    val winnerAdded = Seq(addFile("f2"))
    val winnerRemoved = Seq(removeFile("f2"))

    val summary = RowLevelConcurrency.wouldResolve(
      loserActions, winnerAdded, winnerRemoved)

    assert(summary.candidateFileCount == 0)
    assert(summary.candidatePaths.isEmpty)
    assert(summary.skipReasons.isEmpty)
  }

  // ---------- mixed cases ----------

  test("wouldResolve: mixed candidates and skips are recorded separately") {
    val loserActions: Seq[Action] = Seq(
      removeFile("f1"), addFile("f1"),  // Candidate
      removeFile("f2"), addFile("f2"),  // Will be skipped (winner CoW)
      removeFile("f3"))                  // Will be skipped (loser no add)
    val winnerAdded = Seq(addFile("f1"), addFile("f2_rewritten"), addFile("f3"))
    val winnerRemoved = Seq(removeFile("f1"), removeFile("f2"), removeFile("f3"))

    val summary = RowLevelConcurrency.wouldResolve(
      loserActions, winnerAdded, winnerRemoved)

    assert(summary.candidateFileCount == 1)
    assert(summary.candidatePaths == Seq("f1"))
    assert(summary.skipReasons(SkipReason.WinnerCowOrCompacted) == 1)
    assert(summary.skipReasons(SkipReason.LoserNoSamePathAdd) == 1)
  }

  // ---------- log cap ----------

  test("wouldResolve: candidatePaths cap prevents log blowup") {
    // 50 candidate files > 16 cap
    val n = 50
    val loserActions: Seq[Action] = (1 to n).flatMap { i =>
      Seq(removeFile(s"f$i"), addFile(s"f$i"))
    }
    val winnerAdded = (1 to n).map { i => addFile(s"f$i") }
    val winnerRemoved = (1 to n).map { i => removeFile(s"f$i") }

    val summary = RowLevelConcurrency.wouldResolve(
      loserActions, winnerAdded, winnerRemoved)

    assert(summary.candidateFileCount == n)
    assert(summary.candidatePaths.size <= 16)
  }
}
